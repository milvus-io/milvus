// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.util.List;

/**
 * Advanced Column Family Options which are not
 * mutable (i.e. present in {@link AdvancedMutableColumnFamilyOptionsInterface}
 *
 * Taken from include/rocksdb/advanced_options.h
 */
public interface AdvancedColumnFamilyOptionsInterface
    <T extends AdvancedColumnFamilyOptionsInterface> {

  /**
   * The minimum number of write buffers that will be merged together
   * before writing to storage.  If set to 1, then
   * all write buffers are flushed to L0 as individual files and this increases
   * read amplification because a get request has to check in all of these
   * files. Also, an in-memory merge may result in writing lesser
   * data to storage if there are duplicate records in each of these
   * individual write buffers.  Default: 1
   *
   * @param minWriteBufferNumberToMerge the minimum number of write buffers
   *     that will be merged together.
   * @return the reference to the current options.
   */
  T setMinWriteBufferNumberToMerge(
      int minWriteBufferNumberToMerge);

  /**
   * The minimum number of write buffers that will be merged together
   * before writing to storage.  If set to 1, then
   * all write buffers are flushed to L0 as individual files and this increases
   * read amplification because a get request has to check in all of these
   * files. Also, an in-memory merge may result in writing lesser
   * data to storage if there are duplicate records in each of these
   * individual write buffers.  Default: 1
   *
   * @return the minimum number of write buffers that will be merged together.
   */
  int minWriteBufferNumberToMerge();

  /**
   * The total maximum number of write buffers to maintain in memory including
   * copies of buffers that have already been flushed.  Unlike
   * {@link AdvancedMutableColumnFamilyOptionsInterface#maxWriteBufferNumber()},
   * this parameter does not affect flushing.
   * This controls the minimum amount of write history that will be available
   * in memory for conflict checking when Transactions are used.
   *
   * When using an OptimisticTransactionDB:
   * If this value is too low, some transactions may fail at commit time due
   * to not being able to determine whether there were any write conflicts.
   *
   * When using a TransactionDB:
   * If Transaction::SetSnapshot is used, TransactionDB will read either
   * in-memory write buffers or SST files to do write-conflict checking.
   * Increasing this value can reduce the number of reads to SST files
   * done for conflict detection.
   *
   * Setting this value to 0 will cause write buffers to be freed immediately
   * after they are flushed.
   * If this value is set to -1,
   * {@link AdvancedMutableColumnFamilyOptionsInterface#maxWriteBufferNumber()}
   * will be used.
   *
   * Default:
   * If using a TransactionDB/OptimisticTransactionDB, the default value will
   * be set to the value of
   * {@link AdvancedMutableColumnFamilyOptionsInterface#maxWriteBufferNumber()}
   * if it is not explicitly set by the user. Otherwise, the default is 0.
   *
   * @param maxWriteBufferNumberToMaintain The maximum number of write
   *     buffers to maintain
   *
   * @return the reference to the current options.
   */
  T setMaxWriteBufferNumberToMaintain(
      int maxWriteBufferNumberToMaintain);

  /**
   * The total maximum number of write buffers to maintain in memory including
   * copies of buffers that have already been flushed.
   *
   * @return maxWriteBufferNumberToMaintain The maximum number of write buffers
   *     to maintain
   */
  int maxWriteBufferNumberToMaintain();

  /**
   * Allows thread-safe inplace updates.
   * If inplace_callback function is not set,
   *   Put(key, new_value) will update inplace the existing_value iff
   *   * key exists in current memtable
   *   * new sizeof(new_value) &le; sizeof(existing_value)
   *   * existing_value for that key is a put i.e. kTypeValue
   * If inplace_callback function is set, check doc for inplace_callback.
   * Default: false.
   *
   * @param inplaceUpdateSupport true if thread-safe inplace updates
   *     are allowed.
   * @return the reference to the current options.
   */
  T setInplaceUpdateSupport(
      boolean inplaceUpdateSupport);

  /**
   * Allows thread-safe inplace updates.
   * If inplace_callback function is not set,
   *   Put(key, new_value) will update inplace the existing_value iff
   *   * key exists in current memtable
   *   * new sizeof(new_value) &le; sizeof(existing_value)
   *   * existing_value for that key is a put i.e. kTypeValue
   * If inplace_callback function is set, check doc for inplace_callback.
   * Default: false.
   *
   * @return true if thread-safe inplace updates are allowed.
   */
  boolean inplaceUpdateSupport();

  /**
   * Control locality of bloom filter probes to improve cache miss rate.
   * This option only applies to memtable prefix bloom and plaintable
   * prefix bloom. It essentially limits the max number of cache lines each
   * bloom filter check can touch.
   * This optimization is turned off when set to 0. The number should never
   * be greater than number of probes. This option can boost performance
   * for in-memory workload but should use with care since it can cause
   * higher false positive rate.
   * Default: 0
   *
   * @param bloomLocality the level of locality of bloom-filter probes.
   * @return the reference to the current options.
   */
  T setBloomLocality(int bloomLocality);

  /**
   * Control locality of bloom filter probes to improve cache miss rate.
   * This option only applies to memtable prefix bloom and plaintable
   * prefix bloom. It essentially limits the max number of cache lines each
   * bloom filter check can touch.
   * This optimization is turned off when set to 0. The number should never
   * be greater than number of probes. This option can boost performance
   * for in-memory workload but should use with care since it can cause
   * higher false positive rate.
   * Default: 0
   *
   * @return the level of locality of bloom-filter probes.
   * @see #setBloomLocality(int)
   */
  int bloomLocality();

  /**
   * <p>Different levels can have different compression
   * policies. There are cases where most lower levels
   * would like to use quick compression algorithms while
   * the higher levels (which have more data) use
   * compression algorithms that have better compression
   * but could be slower. This array, if non-empty, should
   * have an entry for each level of the database;
   * these override the value specified in the previous
   * field 'compression'.</p>
   *
   * <strong>NOTICE</strong>
   * <p>If {@code level_compaction_dynamic_level_bytes=true},
   * {@code compression_per_level[0]} still determines {@code L0},
   * but other elements of the array are based on base level
   * (the level {@code L0} files are merged to), and may not
   * match the level users see from info log for metadata.
   * </p>
   * <p>If {@code L0} files are merged to {@code level - n},
   * then, for {@code i&gt;0}, {@code compression_per_level[i]}
   * determines compaction type for level {@code n+i-1}.</p>
   *
   * <strong>Example</strong>
   * <p>For example, if we have 5 levels, and we determine to
   * merge {@code L0} data to {@code L4} (which means {@code L1..L3}
   * will be empty), then the new files go to {@code L4} uses
   * compression type {@code compression_per_level[1]}.</p>
   *
   * <p>If now {@code L0} is merged to {@code L2}. Data goes to
   * {@code L2} will be compressed according to
   * {@code compression_per_level[1]}, {@code L3} using
   * {@code compression_per_level[2]}and {@code L4} using
   * {@code compression_per_level[3]}. Compaction for each
   * level can change when data grows.</p>
   *
   * <p><strong>Default:</strong> empty</p>
   *
   * @param compressionLevels list of
   *     {@link org.rocksdb.CompressionType} instances.
   *
   * @return the reference to the current options.
   */
  T setCompressionPerLevel(
      List<CompressionType> compressionLevels);

  /**
   * <p>Return the currently set {@link org.rocksdb.CompressionType}
   * per instances.</p>
   *
   * <p>See: {@link #setCompressionPerLevel(java.util.List)}</p>
   *
   * @return list of {@link org.rocksdb.CompressionType}
   *     instances.
   */
  List<CompressionType> compressionPerLevel();

  /**
   * Set the number of levels for this database
   * If level-styled compaction is used, then this number determines
   * the total number of levels.
   *
   * @param numLevels the number of levels.
   * @return the reference to the current options.
   */
  T setNumLevels(int numLevels);

  /**
   * If level-styled compaction is used, then this number determines
   * the total number of levels.
   *
   * @return the number of levels.
   */
  int numLevels();

  /**
   * <p>If {@code true}, RocksDB will pick target size of each level
   * dynamically. We will pick a base level b &gt;= 1. L0 will be
   * directly merged into level b, instead of always into level 1.
   * Level 1 to b-1 need to be empty. We try to pick b and its target
   * size so that</p>
   *
   * <ol>
   * <li>target size is in the range of
   *   (max_bytes_for_level_base / max_bytes_for_level_multiplier,
   *    max_bytes_for_level_base]</li>
   * <li>target size of the last level (level num_levels-1) equals to extra size
   *    of the level.</li>
   * </ol>
   *
   * <p>At the same time max_bytes_for_level_multiplier and
   * max_bytes_for_level_multiplier_additional are still satisfied.</p>
   *
   * <p>With this option on, from an empty DB, we make last level the base
   * level, which means merging L0 data into the last level, until it exceeds
   * max_bytes_for_level_base. And then we make the second last level to be
   * base level, to start to merge L0 data to second last level, with its
   * target size to be {@code 1/max_bytes_for_level_multiplier} of the last
   * levels extra size. After the data accumulates more so that we need to
   * move the base level to the third last one, and so on.</p>
   *
   * <h2>Example</h2>
   * <p>For example, assume {@code max_bytes_for_level_multiplier=10},
   * {@code num_levels=6}, and {@code max_bytes_for_level_base=10MB}.</p>
   *
   * <p>Target sizes of level 1 to 5 starts with:</p>
   * {@code [- - - - 10MB]}
   * <p>with base level is level. Target sizes of level 1 to 4 are not applicable
   * because they will not be used.
   * Until the size of Level 5 grows to more than 10MB, say 11MB, we make
   * base target to level 4 and now the targets looks like:</p>
   * {@code [- - - 1.1MB 11MB]}
   * <p>While data are accumulated, size targets are tuned based on actual data
   * of level 5. When level 5 has 50MB of data, the target is like:</p>
   * {@code [- - - 5MB 50MB]}
   * <p>Until level 5's actual size is more than 100MB, say 101MB. Now if we
   * keep level 4 to be the base level, its target size needs to be 10.1MB,
   * which doesn't satisfy the target size range. So now we make level 3
   * the target size and the target sizes of the levels look like:</p>
   * {@code [- - 1.01MB 10.1MB 101MB]}
   * <p>In the same way, while level 5 further grows, all levels' targets grow,
   * like</p>
   * {@code [- - 5MB 50MB 500MB]}
   * <p>Until level 5 exceeds 1000MB and becomes 1001MB, we make level 2 the
   * base level and make levels' target sizes like this:</p>
   * {@code [- 1.001MB 10.01MB 100.1MB 1001MB]}
   * <p>and go on...</p>
   *
   * <p>By doing it, we give {@code max_bytes_for_level_multiplier} a priority
   * against {@code max_bytes_for_level_base}, for a more predictable LSM tree
   * shape. It is useful to limit worse case space amplification.</p>
   *
   * <p>{@code max_bytes_for_level_multiplier_additional} is ignored with
   * this flag on.</p>
   *
   * <p>Turning this feature on or off for an existing DB can cause unexpected
   * LSM tree structure so it's not recommended.</p>
   *
   * <p><strong>Caution</strong>: this option is experimental</p>
   *
   * <p>Default: false</p>
   *
   * @param enableLevelCompactionDynamicLevelBytes boolean value indicating
   *     if {@code LevelCompactionDynamicLevelBytes} shall be enabled.
   * @return the reference to the current options.
   */
  @Experimental("Turning this feature on or off for an existing DB can cause" +
      "unexpected LSM tree structure so it's not recommended")
  T setLevelCompactionDynamicLevelBytes(
      boolean enableLevelCompactionDynamicLevelBytes);

  /**
   * <p>Return if {@code LevelCompactionDynamicLevelBytes} is enabled.
   * </p>
   *
   * <p>For further information see
   * {@link #setLevelCompactionDynamicLevelBytes(boolean)}</p>
   *
   * @return boolean value indicating if
   *    {@code levelCompactionDynamicLevelBytes} is enabled.
   */
  @Experimental("Caution: this option is experimental")
  boolean levelCompactionDynamicLevelBytes();

  /**
   * Maximum size of each compaction (not guarantee)
   *
   * @param maxCompactionBytes the compaction size limit
   * @return the reference to the current options.
   */
  T setMaxCompactionBytes(
      long maxCompactionBytes);

  /**
   * Control maximum size of each compaction (not guaranteed)
   *
   * @return compaction size threshold
   */
  long maxCompactionBytes();

  /**
   * Set compaction style for DB.
   *
   * Default: LEVEL.
   *
   * @param compactionStyle Compaction style.
   * @return the reference to the current options.
   */
  ColumnFamilyOptionsInterface setCompactionStyle(
      CompactionStyle compactionStyle);

  /**
   * Compaction style for DB.
   *
   * @return Compaction style.
   */
  CompactionStyle compactionStyle();

  /**
   * If level {@link #compactionStyle()} == {@link CompactionStyle#LEVEL},
   * for each level, which files are prioritized to be picked to compact.
   *
   * Default: {@link CompactionPriority#ByCompensatedSize}
   *
   * @param compactionPriority The compaction priority
   *
   * @return the reference to the current options.
   */
  T setCompactionPriority(
      CompactionPriority compactionPriority);

  /**
   * Get the Compaction priority if level compaction
   * is used for all levels
   *
   * @return The compaction priority
   */
  CompactionPriority compactionPriority();

  /**
   * Set the options needed to support Universal Style compactions
   *
   * @param compactionOptionsUniversal The Universal Style compaction options
   *
   * @return the reference to the current options.
   */
  T setCompactionOptionsUniversal(
      CompactionOptionsUniversal compactionOptionsUniversal);

  /**
   * The options needed to support Universal Style compactions
   *
   * @return The Universal Style compaction options
   */
  CompactionOptionsUniversal compactionOptionsUniversal();

  /**
   * The options for FIFO compaction style
   *
   * @param compactionOptionsFIFO The FIFO compaction options
   *
   * @return the reference to the current options.
   */
  T setCompactionOptionsFIFO(
      CompactionOptionsFIFO compactionOptionsFIFO);

  /**
   * The options for FIFO compaction style
   *
   * @return The FIFO compaction options
   */
  CompactionOptionsFIFO compactionOptionsFIFO();

  /**
   * <p>This flag specifies that the implementation should optimize the filters
   * mainly for cases where keys are found rather than also optimize for keys
   * missed. This would be used in cases where the application knows that
   * there are very few misses or the performance in the case of misses is not
   * important.</p>
   *
   * <p>For now, this flag allows us to not store filters for the last level i.e
   * the largest level which contains data of the LSM store. For keys which
   * are hits, the filters in this level are not useful because we will search
   * for the data anyway.</p>
   *
   * <p><strong>NOTE</strong>: the filters in other levels are still useful
   * even for key hit because they tell us whether to look in that level or go
   * to the higher level.</p>
   *
   * <p>Default: false<p>
   *
   * @param optimizeFiltersForHits boolean value indicating if this flag is set.
   * @return the reference to the current options.
   */
  T setOptimizeFiltersForHits(
      boolean optimizeFiltersForHits);

  /**
   * <p>Returns the current state of the {@code optimize_filters_for_hits}
   * setting.</p>
   *
   * @return boolean value indicating if the flag
   *     {@code optimize_filters_for_hits} was set.
   */
  boolean optimizeFiltersForHits();

  /**
   * In debug mode, RocksDB run consistency checks on the LSM every time the LSM
   * change (Flush, Compaction, AddFile). These checks are disabled in release
   * mode, use this option to enable them in release mode as well.
   *
   * Default: false
   *
   * @param forceConsistencyChecks true to force consistency checks
   *
   * @return the reference to the current options.
   */
  T setForceConsistencyChecks(
      boolean forceConsistencyChecks);

  /**
   * In debug mode, RocksDB run consistency checks on the LSM every time the LSM
   * change (Flush, Compaction, AddFile). These checks are disabled in release
   * mode.
   *
   * @return true if consistency checks are enforced
   */
  boolean forceConsistencyChecks();
}
