// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Advanced Column Family Options which are mutable
 *
 * Taken from include/rocksdb/advanced_options.h
 * and MutableCFOptions in util/cf_options.h
 */
public interface AdvancedMutableColumnFamilyOptionsInterface
    <T extends AdvancedMutableColumnFamilyOptionsInterface> {

  /**
   * The maximum number of write buffers that are built up in memory.
   * The default is 2, so that when 1 write buffer is being flushed to
   * storage, new writes can continue to the other write buffer.
   * Default: 2
   *
   * @param maxWriteBufferNumber maximum number of write buffers.
   * @return the instance of the current options.
   */
  T setMaxWriteBufferNumber(
      int maxWriteBufferNumber);

  /**
   * Returns maximum number of write buffers.
   *
   * @return maximum number of write buffers.
   * @see #setMaxWriteBufferNumber(int)
   */
  int maxWriteBufferNumber();

  /**
   * Number of locks used for inplace update
   * Default: 10000, if inplace_update_support = true, else 0.
   *
   * @param inplaceUpdateNumLocks the number of locks used for
   *     inplace updates.
   * @return the reference to the current options.
   * @throws java.lang.IllegalArgumentException thrown on 32-Bit platforms
   *     while overflowing the underlying platform specific value.
   */
  T setInplaceUpdateNumLocks(
      long inplaceUpdateNumLocks);

  /**
   * Number of locks used for inplace update
   * Default: 10000, if inplace_update_support = true, else 0.
   *
   * @return the number of locks used for inplace update.
   */
  long inplaceUpdateNumLocks();

  /**
   * if prefix_extractor is set and memtable_prefix_bloom_size_ratio is not 0,
   * create prefix bloom for memtable with the size of
   * write_buffer_size * memtable_prefix_bloom_size_ratio.
   * If it is larger than 0.25, it is santinized to 0.25.
   *
   * Default: 0 (disable)
   *
   * @param memtablePrefixBloomSizeRatio The ratio
   * @return the reference to the current options.
   */
  T setMemtablePrefixBloomSizeRatio(
      double memtablePrefixBloomSizeRatio);

  /**
   * if prefix_extractor is set and memtable_prefix_bloom_size_ratio is not 0,
   * create prefix bloom for memtable with the size of
   * write_buffer_size * memtable_prefix_bloom_size_ratio.
   * If it is larger than 0.25, it is santinized to 0.25.
   *
   * Default: 0 (disable)
   *
   * @return the ratio
   */
  double memtablePrefixBloomSizeRatio();

  /**
   * Page size for huge page TLB for bloom in memtable. If &le; 0, not allocate
   * from huge page TLB but from malloc.
   * Need to reserve huge pages for it to be allocated. For example:
   *     sysctl -w vm.nr_hugepages=20
   * See linux doc Documentation/vm/hugetlbpage.txt
   *
   * @param memtableHugePageSize The page size of the huge
   *     page tlb
   * @return the reference to the current options.
   */
  T setMemtableHugePageSize(
      long memtableHugePageSize);

  /**
   * Page size for huge page TLB for bloom in memtable. If &le; 0, not allocate
   * from huge page TLB but from malloc.
   * Need to reserve huge pages for it to be allocated. For example:
   *     sysctl -w vm.nr_hugepages=20
   * See linux doc Documentation/vm/hugetlbpage.txt
   *
   * @return The page size of the huge page tlb
   */
  long memtableHugePageSize();

  /**
   * The size of one block in arena memory allocation.
   * If &le; 0, a proper value is automatically calculated (usually 1/10 of
   * writer_buffer_size).
   *
   * There are two additional restriction of the specified size:
   * (1) size should be in the range of [4096, 2 &lt;&lt; 30] and
   * (2) be the multiple of the CPU word (which helps with the memory
   * alignment).
   *
   * We'll automatically check and adjust the size number to make sure it
   * conforms to the restrictions.
   * Default: 0
   *
   * @param arenaBlockSize the size of an arena block
   * @return the reference to the current options.
   * @throws java.lang.IllegalArgumentException thrown on 32-Bit platforms
   *   while overflowing the underlying platform specific value.
   */
  T setArenaBlockSize(long arenaBlockSize);

  /**
   * The size of one block in arena memory allocation.
   * If &le; 0, a proper value is automatically calculated (usually 1/10 of
   * writer_buffer_size).
   *
   * There are two additional restriction of the specified size:
   * (1) size should be in the range of [4096, 2 &lt;&lt; 30] and
   * (2) be the multiple of the CPU word (which helps with the memory
   * alignment).
   *
   * We'll automatically check and adjust the size number to make sure it
   * conforms to the restrictions.
   * Default: 0
   *
   * @return the size of an arena block
   */
  long arenaBlockSize();

  /**
   * Soft limit on number of level-0 files. We start slowing down writes at this
   * point. A value &lt; 0 means that no writing slow down will be triggered by
   * number of files in level-0.
   *
   * @param level0SlowdownWritesTrigger The soft limit on the number of
   *   level-0 files
   * @return the reference to the current options.
   */
  T setLevel0SlowdownWritesTrigger(
      int level0SlowdownWritesTrigger);

  /**
   * Soft limit on number of level-0 files. We start slowing down writes at this
   * point. A value &lt; 0 means that no writing slow down will be triggered by
   * number of files in level-0.
   *
   * @return The soft limit on the number of
   *   level-0 files
   */
  int level0SlowdownWritesTrigger();

  /**
   * Maximum number of level-0 files.  We stop writes at this point.
   *
   * @param level0StopWritesTrigger The maximum number of level-0 files
   * @return the reference to the current options.
   */
  T setLevel0StopWritesTrigger(
      int level0StopWritesTrigger);

  /**
   * Maximum number of level-0 files.  We stop writes at this point.
   *
   * @return The maximum number of level-0 files
   */
  int level0StopWritesTrigger();

  /**
   * The target file size for compaction.
   * This targetFileSizeBase determines a level-1 file size.
   * Target file size for level L can be calculated by
   * targetFileSizeBase * (targetFileSizeMultiplier ^ (L-1))
   * For example, if targetFileSizeBase is 2MB and
   * target_file_size_multiplier is 10, then each file on level-1 will
   * be 2MB, and each file on level 2 will be 20MB,
   * and each file on level-3 will be 200MB.
   * by default targetFileSizeBase is 2MB.
   *
   * @param targetFileSizeBase the target size of a level-0 file.
   * @return the reference to the current options.
   *
   * @see #setTargetFileSizeMultiplier(int)
   */
  T setTargetFileSizeBase(
      long targetFileSizeBase);

  /**
   * The target file size for compaction.
   * This targetFileSizeBase determines a level-1 file size.
   * Target file size for level L can be calculated by
   * targetFileSizeBase * (targetFileSizeMultiplier ^ (L-1))
   * For example, if targetFileSizeBase is 2MB and
   * target_file_size_multiplier is 10, then each file on level-1 will
   * be 2MB, and each file on level 2 will be 20MB,
   * and each file on level-3 will be 200MB.
   * by default targetFileSizeBase is 2MB.
   *
   * @return the target size of a level-0 file.
   *
   * @see #targetFileSizeMultiplier()
   */
  long targetFileSizeBase();

  /**
   * targetFileSizeMultiplier defines the size ratio between a
   * level-L file and level-(L+1) file.
   * By default target_file_size_multiplier is 1, meaning
   * files in different levels have the same target.
   *
   * @param multiplier the size ratio between a level-(L+1) file
   *     and level-L file.
   * @return the reference to the current options.
   */
  T setTargetFileSizeMultiplier(
      int multiplier);

  /**
   * targetFileSizeMultiplier defines the size ratio between a
   * level-(L+1) file and level-L file.
   * By default targetFileSizeMultiplier is 1, meaning
   * files in different levels have the same target.
   *
   * @return the size ratio between a level-(L+1) file and level-L file.
   */
  int targetFileSizeMultiplier();

  /**
   * The ratio between the total size of level-(L+1) files and the total
   * size of level-L files for all L.
   * DEFAULT: 10
   *
   * @param multiplier the ratio between the total size of level-(L+1)
   *     files and the total size of level-L files for all L.
   * @return the reference to the current options.
   *
   * See {@link MutableColumnFamilyOptionsInterface#setMaxBytesForLevelBase(long)}
   */
  T setMaxBytesForLevelMultiplier(double multiplier);

  /**
   * The ratio between the total size of level-(L+1) files and the total
   * size of level-L files for all L.
   * DEFAULT: 10
   *
   * @return the ratio between the total size of level-(L+1) files and
   *     the total size of level-L files for all L.
   *
   * See {@link MutableColumnFamilyOptionsInterface#maxBytesForLevelBase()}
   */
  double maxBytesForLevelMultiplier();

  /**
   * Different max-size multipliers for different levels.
   * These are multiplied by max_bytes_for_level_multiplier to arrive
   * at the max-size of each level.
   *
   * Default: 1
   *
   * @param maxBytesForLevelMultiplierAdditional The max-size multipliers
   *   for each level
   * @return the reference to the current options.
   */
  T setMaxBytesForLevelMultiplierAdditional(
      int[] maxBytesForLevelMultiplierAdditional);

  /**
   * Different max-size multipliers for different levels.
   * These are multiplied by max_bytes_for_level_multiplier to arrive
   * at the max-size of each level.
   *
   * Default: 1
   *
   * @return The max-size multipliers for each level
   */
  int[] maxBytesForLevelMultiplierAdditional();

  /**
   * All writes will be slowed down to at least delayed_write_rate if estimated
   * bytes needed to be compaction exceed this threshold.
   *
   * Default: 64GB
   *
   * @param softPendingCompactionBytesLimit The soft limit to impose on
   *   compaction
   * @return the reference to the current options.
   */
  T setSoftPendingCompactionBytesLimit(
      long softPendingCompactionBytesLimit);

  /**
   * All writes will be slowed down to at least delayed_write_rate if estimated
   * bytes needed to be compaction exceed this threshold.
   *
   * Default: 64GB
   *
   * @return The soft limit to impose on compaction
   */
  long softPendingCompactionBytesLimit();

  /**
   * All writes are stopped if estimated bytes needed to be compaction exceed
   * this threshold.
   *
   * Default: 256GB
   *
   * @param hardPendingCompactionBytesLimit The hard limit to impose on
   *   compaction
   * @return the reference to the current options.
   */
  T setHardPendingCompactionBytesLimit(
      long hardPendingCompactionBytesLimit);

  /**
   * All writes are stopped if estimated bytes needed to be compaction exceed
   * this threshold.
   *
   * Default: 256GB
   *
   * @return The hard limit to impose on compaction
   */
  long hardPendingCompactionBytesLimit();

  /**
   * An iteration-&gt;Next() sequentially skips over keys with the same
   * user-key unless this option is set. This number specifies the number
   * of keys (with the same userkey) that will be sequentially
   * skipped before a reseek is issued.
   * Default: 8
   *
   * @param maxSequentialSkipInIterations the number of keys could
   *     be skipped in a iteration.
   * @return the reference to the current options.
   */
  T setMaxSequentialSkipInIterations(
      long maxSequentialSkipInIterations);

  /**
   * An iteration-&gt;Next() sequentially skips over keys with the same
   * user-key unless this option is set. This number specifies the number
   * of keys (with the same userkey) that will be sequentially
   * skipped before a reseek is issued.
   * Default: 8
   *
   * @return the number of keys could be skipped in a iteration.
   */
  long maxSequentialSkipInIterations();

  /**
   * Maximum number of successive merge operations on a key in the memtable.
   *
   * When a merge operation is added to the memtable and the maximum number of
   * successive merges is reached, the value of the key will be calculated and
   * inserted into the memtable instead of the merge operation. This will
   * ensure that there are never more than max_successive_merges merge
   * operations in the memtable.
   *
   * Default: 0 (disabled)
   *
   * @param maxSuccessiveMerges the maximum number of successive merges.
   * @return the reference to the current options.
   * @throws java.lang.IllegalArgumentException thrown on 32-Bit platforms
   *   while overflowing the underlying platform specific value.
   */
  T setMaxSuccessiveMerges(
      long maxSuccessiveMerges);

  /**
   * Maximum number of successive merge operations on a key in the memtable.
   *
   * When a merge operation is added to the memtable and the maximum number of
   * successive merges is reached, the value of the key will be calculated and
   * inserted into the memtable instead of the merge operation. This will
   * ensure that there are never more than max_successive_merges merge
   * operations in the memtable.
   *
   * Default: 0 (disabled)
   *
   * @return the maximum number of successive merges.
   */
  long maxSuccessiveMerges();

  /**
   * After writing every SST file, reopen it and read all the keys.
   *
   * Default: false
   *
   * @param paranoidFileChecks true to enable paranoid file checks
   * @return the reference to the current options.
   */
  T setParanoidFileChecks(
      boolean paranoidFileChecks);

  /**
   * After writing every SST file, reopen it and read all the keys.
   *
   * Default: false
   *
   * @return true if paranoid file checks are enabled
   */
  boolean paranoidFileChecks();

  /**
   * Measure IO stats in compactions and flushes, if true.
   *
   * Default: false
   *
   * @param reportBgIoStats true to enable reporting
   * @return the reference to the current options.
   */
  T setReportBgIoStats(
      boolean reportBgIoStats);

  /**
   * Determine whether IO stats in compactions and flushes are being measured
   *
   * @return true if reporting is enabled
   */
  boolean reportBgIoStats();

  /**
   * Non-bottom-level files older than TTL will go through the compaction
   * process. This needs {@link MutableDBOptionsInterface#maxOpenFiles()} to be
   * set to -1.
   *
   * Enabled only for level compaction for now.
   *
   * Default: 0 (disabled)
   *
   * Dynamically changeable through
   * {@link RocksDB#setOptions(ColumnFamilyHandle, MutableColumnFamilyOptions)}.
   *
   * @param ttl the time-to-live.
   *
   * @return the reference to the current options.
   */
  T setTtl(final long ttl);

  /**
   * Get the TTL for Non-bottom-level files that will go through the compaction
   * process.
   *
   * See {@link #setTtl(long)}.
   *
   * @return the time-to-live.
   */
  long ttl();
}
