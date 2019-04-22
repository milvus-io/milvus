// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

public class CompactionJobStats extends RocksObject {

  public CompactionJobStats() {
    super(newCompactionJobStats());
  }

  /**
   * Private as called from JNI C++
   */
  CompactionJobStats(final long nativeHandle) {
    super(nativeHandle);
  }

  /**
   * Reset the stats.
   */
  public void reset() {
    reset(nativeHandle_);
  }

  /**
   * Aggregate the CompactionJobStats from another instance with this one.
   *
   * @param compactionJobStats another instance of stats.
   */
  public void add(final CompactionJobStats compactionJobStats) {
    add(nativeHandle_, compactionJobStats.nativeHandle_);
  }

  /**
   * Get the elapsed time in micro of this compaction.
   *
   * @return the elapsed time in micro of this compaction.
   */
  public long elapsedMicros() {
    return elapsedMicros(nativeHandle_);
  }

  /**
   * Get the number of compaction input records.
   *
   * @return the number of compaction input records.
   */
  public long numInputRecords() {
    return numInputRecords(nativeHandle_);
  }

  /**
   * Get the number of compaction input files.
   *
   * @return the number of compaction input files.
   */
  public long numInputFiles() {
    return numInputFiles(nativeHandle_);
  }

  /**
   * Get the number of compaction input files at the output level.
   *
   * @return the number of compaction input files at the output level.
   */
  public long numInputFilesAtOutputLevel() {
    return numInputFilesAtOutputLevel(nativeHandle_);
  }

  /**
   * Get the number of compaction output records.
   *
   * @return the number of compaction output records.
   */
  public long numOutputRecords() {
    return numOutputRecords(nativeHandle_);
  }

  /**
   * Get the number of compaction output files.
   *
   * @return the number of compaction output files.
   */
  public long numOutputFiles() {
    return numOutputFiles(nativeHandle_);
  }

  /**
   * Determine if the compaction is a manual compaction.
   *
   * @return true if the compaction is a manual compaction, false otherwise.
   */
  public boolean isManualCompaction() {
    return isManualCompaction(nativeHandle_);
  }

  /**
   * Get the size of the compaction input in bytes.
   *
   * @return the size of the compaction input in bytes.
   */
  public long totalInputBytes() {
    return totalInputBytes(nativeHandle_);
  }

  /**
   * Get the size of the compaction output in bytes.
   *
   * @return the size of the compaction output in bytes.
   */
  public long totalOutputBytes() {
    return totalOutputBytes(nativeHandle_);
  }

  /**
   * Get the number of records being replaced by newer record associated
   * with same key.
   *
   * This could be a new value or a deletion entry for that key so this field
   * sums up all updated and deleted keys.
   *
   * @return the number of records being replaced by newer record associated
   *     with same key.
   */
  public long numRecordsReplaced() {
    return numRecordsReplaced(nativeHandle_);
  }

  /**
   * Get the sum of the uncompressed input keys in bytes.
   *
   * @return the sum of the uncompressed input keys in bytes.
   */
  public long totalInputRawKeyBytes() {
    return totalInputRawKeyBytes(nativeHandle_);
  }

  /**
   * Get the sum of the uncompressed input values in bytes.
   *
   * @return the sum of the uncompressed input values in bytes.
   */
  public long totalInputRawValueBytes() {
    return totalInputRawValueBytes(nativeHandle_);
  }

  /**
   * Get the number of deletion entries before compaction.
   *
   * Deletion entries can disappear after compaction because they expired.
   *
   * @return the number of deletion entries before compaction.
   */
  public long numInputDeletionRecords() {
    return numInputDeletionRecords(nativeHandle_);
  }

  /**
   * Get the number of deletion records that were found obsolete and discarded
   * because it is not possible to delete any more keys with this entry.
   * (i.e. all possible deletions resulting from it have been completed)
   *
   * @return the number of deletion records that were found obsolete and
   *     discarded.
   */
  public long numExpiredDeletionRecords() {
    return numExpiredDeletionRecords(nativeHandle_);
  }

  /**
   * Get the number of corrupt keys (ParseInternalKey returned false when
   * applied to the key) encountered and written out.
   *
   * @return the number of corrupt keys.
   */
  public long numCorruptKeys() {
    return numCorruptKeys(nativeHandle_);
  }

  /**
   * Get the Time spent on file's Append() call.
   *
   * Only populated if {@link ColumnFamilyOptions#reportBgIoStats()} is set.
   *
   * @return the Time spent on file's Append() call.
   */
  public long fileWriteNanos() {
    return fileWriteNanos(nativeHandle_);
  }

  /**
   * Get the Time spent on sync file range.
   *
   * Only populated if {@link ColumnFamilyOptions#reportBgIoStats()} is set.
   *
   * @return the Time spent on sync file range.
   */
  public long fileRangeSyncNanos() {
    return fileRangeSyncNanos(nativeHandle_);
  }

  /**
   * Get the Time spent on file fsync.
   *
   * Only populated if {@link ColumnFamilyOptions#reportBgIoStats()} is set.
   *
   * @return the Time spent on file fsync.
   */
  public long fileFsyncNanos() {
    return fileFsyncNanos(nativeHandle_);
  }

  /**
   * Get the Time spent on preparing file write (falocate, etc)
   *
   * Only populated if {@link ColumnFamilyOptions#reportBgIoStats()} is set.
   *
   * @return the Time spent on preparing file write (falocate, etc).
   */
  public long filePrepareWriteNanos() {
    return filePrepareWriteNanos(nativeHandle_);
  }

  /**
   * Get the smallest output key prefix.
   *
   * @return the smallest output key prefix.
   */
  public byte[] smallestOutputKeyPrefix() {
    return smallestOutputKeyPrefix(nativeHandle_);
  }

  /**
   * Get the largest output key prefix.
   *
   * @return the smallest output key prefix.
   */
  public byte[] largestOutputKeyPrefix() {
    return largestOutputKeyPrefix(nativeHandle_);
  }

  /**
   * Get the number of single-deletes which do not meet a put.
   *
   * @return number of single-deletes which do not meet a put.
   */
  @Experimental("Performance optimization for a very specific workload")
  public long numSingleDelFallthru() {
    return numSingleDelFallthru(nativeHandle_);
  }

  /**
   * Get the number of single-deletes which meet something other than a put.
   *
   * @return the number of single-deletes which meet something other than a put.
   */
  @Experimental("Performance optimization for a very specific workload")
  public long numSingleDelMismatch() {
    return numSingleDelMismatch(nativeHandle_);
  }

  private static native long newCompactionJobStats();
  @Override protected native void disposeInternal(final long handle);


  private static native void reset(final long handle);
  private static native void add(final long handle,
      final long compactionJobStatsHandle);
  private static native long elapsedMicros(final long handle);
  private static native long numInputRecords(final long handle);
  private static native long numInputFiles(final long handle);
  private static native long numInputFilesAtOutputLevel(final long handle);
  private static native long numOutputRecords(final long handle);
  private static native long numOutputFiles(final long handle);
  private static native boolean isManualCompaction(final long handle);
  private static native long totalInputBytes(final long handle);
  private static native long totalOutputBytes(final long handle);
  private static native long numRecordsReplaced(final long handle);
  private static native long totalInputRawKeyBytes(final long handle);
  private static native long totalInputRawValueBytes(final long handle);
  private static native long numInputDeletionRecords(final long handle);
  private static native long numExpiredDeletionRecords(final long handle);
  private static native long numCorruptKeys(final long handle);
  private static native long fileWriteNanos(final long handle);
  private static native long fileRangeSyncNanos(final long handle);
  private static native long fileFsyncNanos(final long handle);
  private static native long filePrepareWriteNanos(final long handle);
  private static native byte[] smallestOutputKeyPrefix(final long handle);
  private static native byte[] largestOutputKeyPrefix(final long handle);
  private static native long numSingleDelFallthru(final long handle);
  private static native long numSingleDelMismatch(final long handle);
}
