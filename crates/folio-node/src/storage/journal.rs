use async_trait::async_trait;
use folio_core::error::{FolioError, Result};
use folio_core::protocol::Entry;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use tokio::fs::{self, OpenOptions};
use tokio::io::AsyncWriteExt;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoredEntry {
    pub entry: Entry,
    pub appended_at_ms: u64,
}

#[async_trait]
pub trait Journal: Send + Sync {
    async fn append(&self, entry: &StoredEntry) -> Result<()>;
}

#[derive(Debug)]
pub struct FileJournal {
    path: PathBuf,
}

impl FileJournal {
    pub async fn new(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }
        Ok(Self { path })
    }
}

#[async_trait]
impl Journal for FileJournal {
    async fn append(&self, entry: &StoredEntry) -> Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .await?;
        let bytes =
            bincode::serialize(entry).map_err(|e| FolioError::Serialization(e.to_string()))?;
        let len = (bytes.len() as u32).to_le_bytes();
        file.write_all(&len).await?;
        file.write_all(&bytes).await?;
        file.flush().await?;
        file.sync_data().await?;
        Ok(())
    }
}
