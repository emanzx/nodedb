# Security Policy

## Supported Versions

NodeDB is in pre-1.0 development. Security fixes are issued only against the
latest released minor version. Once 1.0 ships, this policy will be updated to
cover an LTS window.

| Version | Supported          |
| ------- | ------------------ |
| 0.1.x   | :white_check_mark: |
| < 0.1   | :x:                |

## Reporting a Vulnerability

**Do not open a public GitHub issue for security reports.**

Report vulnerabilities privately via GitHub's **Security Advisories** workflow:

> [Report a vulnerability](https://github.com/NodeDB-Lab/nodedb/security/advisories/new)

Or navigate to the repository's **Security** tab → **Advisories** →
**Report a vulnerability**. This opens a private channel between you and the
maintainers; the report is not visible to the public until an advisory is
published.

Please include:

- A description of the vulnerability and its impact.
- Reproduction steps, including affected version (`nodedb --version`) and
  configuration (deployment mode: Origin cloud, Origin local, or Lite).
- Any proof-of-concept code, log excerpts, or stack traces.
- Whether the issue is already public or under coordinated disclosure
  elsewhere (e.g. RustSec, a GitHub Security Advisory on a dependency).

You should receive an acknowledgement within **3 business days**. We aim to
provide an initial assessment (severity, scope, mitigation timeline) within
**10 business days**.

## Disclosure Policy

We follow coordinated disclosure:

1. The reporter and the NodeDB maintainers agree on an embargo window
   (typically 90 days, shorter for actively-exploited issues).
2. A fix is prepared on a private branch, reviewed, and merged.
3. A patch release is cut and announced via:
   - GitHub release notes on the affected tag
   - A RustSec advisory, once the affected NodeDB crates are published to
     crates.io
4. The reporter is credited in the advisory unless they prefer to remain
   anonymous.

## Scope

In-scope components:

- The `nodedb` server binary and all crates under this repository.
- The published `nodedb-*` crates on crates.io.
- The on-disk WAL, segment, and snapshot formats.
- The pgwire, HTTP, and native MessagePack protocols.
- The CRDT sync protocol used between Origin and Lite.

Out of scope:

- Third-party dependencies — please report upstream and link the advisory.
- Denial-of-service via expected resource exhaustion (e.g. issuing very
  large queries on an unconfigured deployment). Configure memory governors,
  query timeouts, and per-tenant budgets per the operator guide.
- Issues in development tooling (`scripts/`, benchmarks, examples) unless
  they affect the shipped binary.

## Hardening Defaults

Production deployments should:

- Enable TLS on the pgwire listener (`pgwire.tls.cert` / `pgwire.tls.key`).
- Set a non-trust authentication method (`pgwire.auth = "scram-sha-256"`).
- Enable WAL encryption (`wal.encryption = "aes-256-gcm"`) on untrusted
  storage.
- Configure per-tenant memory and IO budgets via `nodedb-mem` governors.
- Restrict the cluster QUIC listener to a private network.
