// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

public interface MutableColumnFamilyOptionsInterface
    <T extends MutableColumnFamilyOptionsInterface>
        extends AdvancedMutableColumnFamilyOptionsInterface<T> {

  /**
   * Amount of data to build up in memory (backed by an unsorted log
   * on disk) before converting to a sorted on-disk file.
   *
   * Larger values increase performance, especially during bulk loads.
   * Up to {@code max_write_buffer_number} write buffers may be held in memory
   * at the same time, so you may wish to adjust this parameter
   * to control memory usage.
   *
   * Also, a larger write buffer will result in a longer recovery time
   * the next time the database is opened.
   *
   * Default: 4MB
   * @param writeBufferSize the size of write buffer.
   * @return the instance of the current object.
   * @throws java.lang.IllegalArgumentException thrown on 32-Bit platforms
   *   while overflowing the underlying platform specific value.
   */
  MutableColumnFamilyOptionsInterface setWriteBufferSize(long writeBufferSize);

  /**
   * Return size of write buffer size.
   *
   * @return size of write buffer.
   * @see #setWriteBufferSize(long)
   */
  long writeBufferSize();

  /**
   * Disable automatic compactions. Manual compactions can still
   * be issued on this column family
   *
   * @param disableAutoCompactions true if auto-compactions are disabled.
   * @return the reference to the current option.
   */
  MutableColumnFamilyOptionsInterface setDisableAutoCompactions(
      boolean disableAutoCompactions);

  /**
   * Disable automatic compactions. Manual compactions can still
   * be issued on this column family
   *
   * @return true if auto-compactions are disabled.
   */
  boolean disableAutoCompactions();

  /**
   * Number of files to trigger level-0 compaction. A value &lt; 0 means that
   * level-0 compaction will not be triggered by number of files at all.
   *
   * Default: 4
   *
   * @param level0FileNumCompactionTrigger The number of files to trigger
   *   level-0 compaction
   * @return the reference to the current option.
   */
  MutableColumnFamilyOptionsInterface setLevel0FileNumCompactionTrigger(
      int level0FileNumCompactionTrigger);

  /**
   * Number of files to trigger level-0 compaction. A value &lt; 0 means that
   * level-0 compaction will not be triggered by number of files at all.
   *
   * Default: 4
   *
   * @return The number of files to trigger
   */
  int level0FileNumCompactionTrigger();

  /**
   * We try to limit number of bytes in one compaction to be lower than this
   * threshold. But it's not guaranteed.
   * Value 0 will be sanitized.
   *
   * @param maxCompactionBytes max bytes in a compaction
   * @return the reference to the current option.
   * @see #maxCompactionBytes()
   */
  MutableColumnFamilyOptionsInterface setMaxCompactionBytes(final long maxCompactionBytes);

  /**
   * We try to limit number of bytes in one compaction to be lower than this
   * threshold. But it's not guaranteed.
   * Value 0 will be sanitized.
   *
   * @return the maximum number of bytes in for a compaction.
   * @see #setMaxCompactionBytes(long)
   */
  long maxCompactionBytes();

  /**
   * The upper-bound of the total size of level-1 files in bytes.
   * Maximum number of bytes for level L can be calculated as
   * (maxBytesForLevelBase) * (maxBytesForLevelMultiplier ^ (L-1))
   * For example, if maxBytesForLevelBase is 20MB, and if
   * max_bytes_for_level_multiplier is 10, total data size for level-1
   * will be 20MB, total file size for level-2 will be 200MB,
   * and total file size for level-3 will be 2GB.
   * by default 'maxBytesForLevelBase' is 10MB.
   *
   * @param maxBytesForLevelBase maximum bytes for level base.
   *
   * @return the reference to the current option.
   *
   * See {@link AdvancedMutableColumnFamilyOptionsInterface#setMaxBytesForLevelMultiplier(double)}
   */
  T setMaxBytesForLevelBase(
      long maxBytesForLevelBase);

  /**
   * The upper-bound of the total size of level-1 files in bytes.
   * Maximum number of bytes for level L can be calculated as
   * (maxBytesForLevelBase) * (maxBytesForLevelMultiplier ^ (L-1))
   * For example, if maxBytesForLevelBase is 20MB, and if
   * max_bytes_for_level_multiplier is 10, total data size for level-1
   * will be 20MB, total file size for level-2 will be 200MB,
   * and total file size for level-3 will be 2GB.
   * by default 'maxBytesForLevelBase' is 10MB.
   *
   * @return the upper-bound of the total size of level-1 files
   *     in bytes.
   *
   * See {@link AdvancedMutableColumnFamilyOptionsInterface#maxBytesForLevelMultiplier()}
   */
  long maxBytesForLevelBase();

  /**
   * Compress blocks using the specified compression algorithm.  This
   * parameter can be changed dynamically.
   *
   * Default: SNAPPY_COMPRESSION, which gives lightweight but fast compression.
   *
   * @param compressionType Compression Type.
   * @return the reference to the current option.
   */
  T setCompressionType(
          CompressionType compressionType);

  /**
   * Compress blocks using the specified compression algorithm.  This
   * parameter can be changed dynamically.
   *
   * Default: SNAPPY_COMPRESSION, which gives lightweight but fast compression.
   *
   * @return Compression type.
   */
  CompressionType compressionType();
}
