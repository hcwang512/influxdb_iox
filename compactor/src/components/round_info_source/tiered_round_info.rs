use std::sync::{Arc, Mutex};
use std::fmt::{Debug, Display, Formatter};
use async_trait::async_trait;
use data_types::{CompactionLevel, ParquetFile};
use crate::{Components, DynError, PartitionInfo, RoundInfo};
use crate::components::round_info_source::RoundInfoSource;
use crate::round_info::{CompactRange, CompactType};

#[derive(Debug)]
pub struct TieredRoundInfo {
    pub(crate) max_file_size: usize,
    pub(crate) max_file_size_to_group: usize,
}

impl Display for TieredRoundInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

#[async_trait]
impl RoundInfoSource for TieredRoundInfo {
    async fn calculate(&self, components: Arc<Components>, last_round_info: Option<Arc<RoundInfo>>, partition_info: &PartitionInfo, concurrency_limit: usize, files: Vec<ParquetFile>) -> Result<(Arc<RoundInfo>, bool), DynError> {
        let l2s: Vec<ParquetFile> = files.into_iter().
            filter(|f| f.compaction_level == CompactionLevel::Final).collect();
        let mut chains = Vec::with_capacity(10);
        let mut chain = Vec::with_capacity(l2s.len());
        let mut current_sum = 0;
        let mut ranges = Vec::new();

        for f in l2s.into_iter() {
            if current_sum + f.file_size_bytes < self.max_file_size as i64 {
                current_sum += f.file_size_bytes;
                chain.push(f);
            } else {
                if chain.len() > 1 {
                    chains.push(chain);
                }
                chain = Vec::new();
                current_sum = 0;
                chain.push(f);
            }
        }
        for chain in chains {
            ranges.push(CompactRange{
                op: Some(CompactType::TargetLevel {target_level: CompactionLevel::Final, max_total_file_size_to_group: self.max_file_size_to_group }),
                min: chain[0].min_time.get(),
                max: chain.last().unwrap().max_time.get(),
                cap: 0,
                has_l0s: false,
                files_for_now: Mutex::new(Some(chain)),
                branches: Mutex::new(None),
                files_for_later: Mutex::new(None),
            })
        }

        Ok((Arc::new(RoundInfo{
            ranges,
            l2_files_for_later: Mutex::new(None),
        }), false))
    }

}
