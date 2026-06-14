//! LAC (Last Add Confirmed) tracker — FR-CS-03.

use folio_core::protocol::AppendAck;

#[derive(Debug, Clone)]
pub struct LacTracker {
    lac: u64,
}

impl LacTracker {
    pub fn new(initial: u64) -> Self {
        Self { lac: initial }
    }

    pub fn current(&self) -> u64 {
        self.lac
    }

    pub fn advance(&mut self, acks: &[AppendAck], entry_id: u64) -> u64 {
        let max_reported = acks.iter().map(|a| a.local_lac).max().unwrap_or(0);
        self.lac = self.lac.max(max_reported).max(entry_id);
        self.lac
    }
}
