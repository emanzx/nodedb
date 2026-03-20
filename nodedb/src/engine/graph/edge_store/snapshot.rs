use super::store::{EDGES, EdgeStore, REVERSE_EDGES, redb_err};

impl EdgeStore {
    /// Export all forward edges as key-value pairs (for snapshot transfer).
    pub fn export_edges(&self) -> crate::Result<Vec<(String, Vec<u8>)>> {
        let txn = self.db.begin_read().map_err(|e| redb_err("read txn", e))?;
        let table = txn
            .open_table(EDGES)
            .map_err(|e| redb_err("open edges", e))?;
        let mut pairs = Vec::new();
        for entry in table
            .range::<&str>(..)
            .map_err(|e| redb_err("iter edges", e))?
        {
            let entry = entry.map_err(|e| redb_err("read edge", e))?;
            pairs.push((entry.0.value().to_string(), entry.1.value().to_vec()));
        }
        Ok(pairs)
    }

    /// Export all reverse edges as key-value pairs (for snapshot transfer).
    pub fn export_reverse_edges(&self) -> crate::Result<Vec<(String, Vec<u8>)>> {
        let txn = self.db.begin_read().map_err(|e| redb_err("read txn", e))?;
        let table = txn
            .open_table(REVERSE_EDGES)
            .map_err(|e| redb_err("open rev edges", e))?;
        let mut pairs = Vec::new();
        for entry in table
            .range::<&str>(..)
            .map_err(|e| redb_err("iter rev edges", e))?
        {
            let entry = entry.map_err(|e| redb_err("read rev edge", e))?;
            pairs.push((entry.0.value().to_string(), entry.1.value().to_vec()));
        }
        Ok(pairs)
    }

    /// Import edges from a snapshot (bulk insert into both forward and reverse tables).
    pub fn import_edges(
        &self,
        edges: &[(String, Vec<u8>)],
        reverse: &[(String, Vec<u8>)],
    ) -> crate::Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("write txn", e))?;
        {
            let mut table = txn
                .open_table(EDGES)
                .map_err(|e| redb_err("open edges", e))?;
            for (key, value) in edges {
                table
                    .insert(key.as_str(), value.as_slice())
                    .map_err(|e| redb_err("insert edge", e))?;
            }
        }
        {
            let mut table = txn
                .open_table(REVERSE_EDGES)
                .map_err(|e| redb_err("open rev edges", e))?;
            for (key, value) in reverse {
                table
                    .insert(key.as_str(), value.as_slice())
                    .map_err(|e| redb_err("insert rev edge", e))?;
            }
        }
        txn.commit().map_err(|e| redb_err("commit", e))?;
        Ok(())
    }
}
