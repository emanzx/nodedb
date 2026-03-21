//! Per-session token bucket rate limiter for sync mutations.
//!
//! Each sync session gets an independent rate limiter that controls
//! how many mutations per second a client can push. This prevents
//! a single misbehaving or compromised client from overwhelming the
//! WAL/Raft commit path.
//!
//! Uses a classic token bucket algorithm:
//! - Tokens replenish at `rate_per_sec` tokens/second
//! - Bucket holds at most `burst` tokens
//! - Each mutation consumes 1 token
//! - When empty, mutations are rejected with `RateLimited` compensation hint

use std::time::Instant;

/// Per-session rate limiter configuration.
#[derive(Debug, Clone)]
pub struct RateLimitConfig {
    /// Maximum sustained mutations per second.
    pub rate_per_sec: f64,
    /// Maximum burst size (tokens).
    pub burst: u32,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            rate_per_sec: 100.0,
            burst: 200,
        }
    }
}

/// Token bucket rate limiter for a single sync session.
pub struct SyncRateLimiter {
    /// Current token count (fractional for sub-second precision).
    tokens: f64,
    /// Maximum tokens (burst capacity).
    max_tokens: f64,
    /// Tokens added per second.
    rate: f64,
    /// Last time tokens were replenished.
    last_refill: Instant,
    /// Total mutations allowed.
    total_allowed: u64,
    /// Total mutations throttled.
    total_throttled: u64,
}

impl SyncRateLimiter {
    pub fn new(config: &RateLimitConfig) -> Self {
        Self {
            tokens: config.burst as f64,
            max_tokens: config.burst as f64,
            rate: config.rate_per_sec,
            last_refill: Instant::now(),
            total_allowed: 0,
            total_throttled: 0,
        }
    }

    /// Try to consume one token for a mutation.
    ///
    /// Returns `Ok(())` if allowed, or `Err(retry_after_ms)` with the
    /// estimated milliseconds until a token becomes available.
    pub fn try_acquire(&mut self) -> Result<(), u64> {
        self.refill();

        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            self.total_allowed += 1;
            Ok(())
        } else {
            self.total_throttled += 1;
            // Calculate how long until 1 token is available.
            let deficit = 1.0 - self.tokens;
            let wait_secs = deficit / self.rate;
            let wait_ms = (wait_secs * 1000.0).ceil() as u64;
            Err(wait_ms.max(1))
        }
    }

    /// Refill tokens based on elapsed time.
    fn refill(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        if elapsed > 0.0 {
            self.tokens = (self.tokens + elapsed * self.rate).min(self.max_tokens);
            self.last_refill = now;
        }
    }

    /// Total mutations allowed by this limiter.
    pub fn total_allowed(&self) -> u64 {
        self.total_allowed
    }

    /// Total mutations throttled by this limiter.
    pub fn total_throttled(&self) -> u64 {
        self.total_throttled
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn allows_up_to_burst() {
        let config = RateLimitConfig {
            rate_per_sec: 10.0,
            burst: 5,
        };
        let mut limiter = SyncRateLimiter::new(&config);

        // Should allow exactly `burst` mutations immediately.
        for _ in 0..5 {
            assert!(limiter.try_acquire().is_ok());
        }
        // 6th should fail.
        let err = limiter.try_acquire().unwrap_err();
        assert!(err >= 1); // retry_after_ms > 0
        assert_eq!(limiter.total_allowed(), 5);
        assert_eq!(limiter.total_throttled(), 1);
    }

    #[test]
    fn refills_over_time() {
        let config = RateLimitConfig {
            rate_per_sec: 1000.0, // Fast refill for test.
            burst: 5,
        };
        let mut limiter = SyncRateLimiter::new(&config);

        // Drain all tokens.
        for _ in 0..5 {
            limiter.try_acquire().ok();
        }
        assert!(limiter.try_acquire().is_err());

        // Wait for refill (at 1000/s, 10ms = ~10 tokens, capped at burst=5).
        thread::sleep(Duration::from_millis(10));

        // Should have tokens again.
        assert!(limiter.try_acquire().is_ok());
    }

    #[test]
    fn retry_after_is_reasonable() {
        let config = RateLimitConfig {
            rate_per_sec: 10.0,
            burst: 1,
        };
        let mut limiter = SyncRateLimiter::new(&config);
        limiter.try_acquire().ok(); // Consume the 1 token.

        let retry_ms = limiter.try_acquire().unwrap_err();
        // At 10/s, one token takes ~100ms. Allow some slack.
        assert!((1..=200).contains(&retry_ms), "retry_ms={retry_ms}");
    }

    #[test]
    fn zero_rate_blocks_everything_after_burst() {
        let config = RateLimitConfig {
            rate_per_sec: 0.0,
            burst: 2,
        };
        let mut limiter = SyncRateLimiter::new(&config);
        assert!(limiter.try_acquire().is_ok());
        assert!(limiter.try_acquire().is_ok());
        // With rate=0, no refill ever happens.
        assert!(limiter.try_acquire().is_err());
    }
}
