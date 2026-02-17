/// Tracks per-run and cumulative statistics for the watchdog.
/// This is a lightweight in-memory tracker; the authoritative cumulative data
/// lives in the database (space_saved_log, transcode_history).
#[derive(Debug, Clone, Default)]
pub struct RunStats {
    pub files_inspected: u64,
    pub files_queued: u64,
    pub files_transcoded: u64,
    pub transcode_failures: u64,
    pub space_saved_bytes: i64,
}

#[allow(dead_code)]
impl RunStats {
    pub fn reset(&mut self) {
        *self = Self::default();
    }
}

#[derive(Debug, Clone, Default)]
#[allow(dead_code)]
pub struct CumulativeStats {
    pub files_inspected: u64,
    pub files_transcoded: u64,
    pub transcode_failures: u64,
    pub space_saved_bytes: i64,
}

#[allow(dead_code)]
impl CumulativeStats {
    /// Merge a completed run's stats into the cumulative totals.
    pub fn merge_run(&mut self, run: &RunStats) {
        self.files_inspected += run.files_inspected;
        self.files_transcoded += run.files_transcoded;
        self.transcode_failures += run.transcode_failures;
        self.space_saved_bytes += run.space_saved_bytes;
    }
}
