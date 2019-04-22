# RocksDB default options change log
## Unreleased
* delayed_write_rate takes the rate given by rate_limiter if not specified.

## 5.2
* Change the default of delayed slowdown value to 16MB/s and further increase the L0 stop condition to 36 files.

## 5.0 (11/17/2016)
* Options::allow_concurrent_memtable_write and Options::enable_write_thread_adaptive_yield are now true by default
* Options.level0_stop_writes_trigger default value changes from 24 to 32.

## 4.8.0 (5/2/2016)
* options.max_open_files changes from 5000 to -1. It improves performance, but users need to set file descriptor limit to be large enough and watch memory usage for index and bloom filters.
* options.base_background_compactions changes from max_background_compactions to 1. When users set higher max_background_compactions but the write throughput is not high, the writes are less spiky to disks.
* options.wal_recovery_mode changes from kTolerateCorruptedTailRecords to kPointInTimeRecovery. Avoid some false positive when file system or hardware reorder the writes for file data and metadata.

## 4.7.0 (4/8/2016)
* options.write_buffer_size changes from 4MB to 64MB.
* options.target_file_size_base changes from 2MB to 64MB.
* options.max_bytes_for_level_base changes from 10MB to 256MB.
* options.soft_pending_compaction_bytes_limit changes from 0 (disabled) to 64GB.
* options.hard_pending_compaction_bytes_limit changes from 0 (disabled) to 256GB.
* table_cache_numshardbits changes from 4 to 6.
* max_file_opening_threads changes from 1 to 16.
