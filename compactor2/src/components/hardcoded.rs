//! Current hardcoded component setup.
//!
//! TODO: Make this a runtime-config.

use std::sync::Arc;

use data_types::CompactionLevel;
use object_store::memory::InMemory;

use crate::{
    components::{
        namespaces_source::catalog::CatalogNamespacesSource,
        tables_source::catalog::CatalogTablesSource,
    },
    config::Config,
    error::ErrorKind,
};

use super::{
    commit::{
        catalog::CatalogCommit, logging::LoggingCommitWrapper, metrics::MetricsCommitWrapper,
        mock::MockCommit, Commit,
    },
    df_plan_exec::dedicated::DedicatedDataFusionPlanExec,
    df_planner::planner_v1::V1DataFusionPlanner,
    divide_initial::single_branch::SingleBranchDivideInitial,
    file_filter::{and::AndFileFilter, level_range::LevelRangeFileFilter},
    files_filter::{chain::FilesFilterChain, per_file::PerFileFilesFilter},
    parquet_file_sink::{
        dedicated::DedicatedExecParquetFileSinkWrapper, logging::LoggingParquetFileSinkWrapper,
        object_store::ObjectStoreParquetFileSink,
    },
    partition_done_sink::{
        catalog::CatalogPartitionDoneSink, error_kind::ErrorKindPartitionDoneSinkWrapper,
        logging::LoggingPartitionDoneSinkWrapper, metrics::MetricsPartitionDoneSinkWrapper,
        mock::MockPartitionDoneSink, PartitionDoneSink,
    },
    partition_files_source::catalog::CatalogPartitionFilesSource,
    partition_filter::{
        and::AndPartitionFilter, by_id::ByIdPartitionFilter, has_files::HasFilesPartitionFilter,
        has_matching_file::HasMatchingFilePartitionFilter, logging::LoggingPartitionFilterWrapper,
        metrics::MetricsPartitionFilterWrapper, never_skipped::NeverSkippedPartitionFilter,
        PartitionFilter,
    },
    partitions_source::{
        catalog::CatalogPartitionsSource, logging::LoggingPartitionsSourceWrapper,
        metrics::MetricsPartitionsSourceWrapper,
        randomize_order::RandomizeOrderPartitionsSourcesWrapper,
    },
    round_split::all_now::AllNowRoundSplit,
    scratchpad::{ignore_writes_object_store::IgnoreWrites, prod::ProdScratchpadGen},
    skipped_compactions_source::catalog::CatalogSkippedCompactionsSource,
    Components,
};

/// Get hardcoded components.
pub fn hardcoded_components(config: &Config) -> Arc<Components> {
    // TODO: partitions source: Implementing ID-based sharding / hash-partitioning so we can run multiple compactors in
    //       parallel. This should be a wrapper around the existing partions source.

    let mut partition_filters: Vec<Arc<dyn PartitionFilter>> = vec![];
    if let Some(ids) = &config.partition_filter {
        // filter as early as possible, so we don't need any catalog lookups for the filtered partitions
        partition_filters.push(Arc::new(ByIdPartitionFilter::new(ids.clone())));
    }
    if !config.ignore_partition_skip_marker {
        partition_filters.push(Arc::new(NeverSkippedPartitionFilter::new(
            CatalogSkippedCompactionsSource::new(
                config.backoff_config.clone(),
                Arc::clone(&config.catalog),
            ),
        )));
    }
    partition_filters.push(Arc::new(HasMatchingFilePartitionFilter::new(
        LevelRangeFileFilter::new(CompactionLevel::Initial..=CompactionLevel::Initial),
    )));
    partition_filters.push(Arc::new(HasFilesPartitionFilter::new()));

    let partition_done_sink: Arc<dyn PartitionDoneSink> = if config.shadow_mode {
        Arc::new(MockPartitionDoneSink::new())
    } else {
        Arc::new(CatalogPartitionDoneSink::new(
            config.backoff_config.clone(),
            Arc::clone(&config.catalog),
        ))
    };

    let commit: Arc<dyn Commit> = if config.shadow_mode {
        Arc::new(MockCommit::new())
    } else {
        Arc::new(CatalogCommit::new(
            config.backoff_config.clone(),
            Arc::clone(&config.catalog),
        ))
    };

    let scratchpad_store_output = if config.shadow_mode {
        Arc::new(IgnoreWrites::new(Arc::new(InMemory::new())))
    } else {
        Arc::clone(config.parquet_store_real.object_store())
    };

    Arc::new(Components {
        partitions_source: Arc::new(LoggingPartitionsSourceWrapper::new(
            MetricsPartitionsSourceWrapper::new(
                RandomizeOrderPartitionsSourcesWrapper::new(
                    CatalogPartitionsSource::new(
                        config.backoff_config.clone(),
                        Arc::clone(&config.catalog),
                        config.partition_threshold,
                        Arc::clone(&config.time_provider),
                    ),
                    1234,
                ),
                &config.metric_registry,
            ),
        )),
        partition_files_source: Arc::new(CatalogPartitionFilesSource::new(
            config.backoff_config.clone(),
            Arc::clone(&config.catalog),
        )),
        files_filter: Arc::new(FilesFilterChain::new(vec![Arc::new(
            PerFileFilesFilter::new(AndFileFilter::new(vec![Arc::new(
                LevelRangeFileFilter::new(
                    CompactionLevel::Initial..=CompactionLevel::FileNonOverlapped,
                ),
            )])),
        )])),
        partition_filter: Arc::new(LoggingPartitionFilterWrapper::new(
            MetricsPartitionFilterWrapper::new(
                AndPartitionFilter::new(partition_filters),
                &config.metric_registry,
            ),
        )),
        partition_done_sink: Arc::new(LoggingPartitionDoneSinkWrapper::new(
            MetricsPartitionDoneSinkWrapper::new(
                ErrorKindPartitionDoneSinkWrapper::new(
                    partition_done_sink,
                    ErrorKind::variants()
                        .iter()
                        .filter(|kind| {
                            // use explicit match statement so we never forget to add new variants
                            match kind {
                                ErrorKind::OutOfMemory
                                | ErrorKind::Timeout
                                | ErrorKind::Unknown => true,
                                ErrorKind::ObjectStore => false,
                            }
                        })
                        .copied()
                        .collect(),
                ),
                &config.metric_registry,
            ),
        )),
        commit: Arc::new(LoggingCommitWrapper::new(MetricsCommitWrapper::new(
            commit,
            &config.metric_registry,
        ))),
        namespaces_source: Arc::new(CatalogNamespacesSource::new(
            config.backoff_config.clone(),
            Arc::clone(&config.catalog),
        )),
        tables_source: Arc::new(CatalogTablesSource::new(
            config.backoff_config.clone(),
            Arc::clone(&config.catalog),
        )),
        df_planner: Arc::new(V1DataFusionPlanner::new(
            config.parquet_store_scratchpad.clone(),
            Arc::clone(&config.exec),
            config.max_desired_file_size_bytes,
            config.percentage_max_file_size,
            config.split_percentage,
        )),
        df_plan_exec: Arc::new(DedicatedDataFusionPlanExec::new(Arc::clone(&config.exec))),
        parquet_file_sink: Arc::new(LoggingParquetFileSinkWrapper::new(
            DedicatedExecParquetFileSinkWrapper::new(
                ObjectStoreParquetFileSink::new(
                    config.shard_id,
                    config.parquet_store_scratchpad.clone(),
                    Arc::clone(&config.time_provider),
                ),
                Arc::clone(&config.exec),
            ),
        )),
        round_split: Arc::new(AllNowRoundSplit::new()),
        divide_initial: Arc::new(SingleBranchDivideInitial::new()),
        scratchpad_gen: Arc::new(ProdScratchpadGen::new(
            config.partition_scratchpad_concurrency,
            config.backoff_config.clone(),
            Arc::clone(config.parquet_store_real.object_store()),
            Arc::clone(config.parquet_store_scratchpad.object_store()),
            scratchpad_store_output,
        )),
    })
}