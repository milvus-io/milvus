// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class CompactionJobInfo extends RocksObject {

  public CompactionJobInfo() {
    super(newCompactionJobInfo());
  }

  /**
   * Private as called from JNI C++
   */
  private CompactionJobInfo(final long nativeHandle) {
    super(nativeHandle);
  }

  /**
   * Get the name of the column family where the compaction happened.
   *
   * @return the name of the column family
   */
  public byte[] columnFamilyName() {
    return columnFamilyName(nativeHandle_);
  }

  /**
   * Get the status indicating whether the compaction was successful or not.
   *
   * @return the status
   */
  public Status status() {
    return status(nativeHandle_);
  }

  /**
   * Get the id of the thread that completed this compaction job.
   *
   * @return the id of the thread
   */
  public long threadId() {
    return threadId(nativeHandle_);
  }

  /**
   * Get the job id, which is unique in the same thread.
   *
   * @return the id of the thread
   */
  public int jobId() {
    return jobId(nativeHandle_);
  }

  /**
   * Get the smallest input level of the compaction.
   *
   * @return the input level
   */
  public int baseInputLevel() {
    return baseInputLevel(nativeHandle_);
  }

  /**
   * Get the output level of the compaction.
   *
   * @return the output level
   */
  public int outputLevel() {
    return outputLevel(nativeHandle_);
  }

  /**
   * Get the names of the compaction input files.
   *
   * @return the names of the input files.
   */
  public List<String> inputFiles() {
    return Arrays.asList(inputFiles(nativeHandle_));
  }

  /**
   * Get the names of the compaction output files.
   *
   * @return the names of the output files.
   */
  public List<String> outputFiles() {
    return Arrays.asList(outputFiles(nativeHandle_));
  }

  /**
   * Get the table properties for the input and output tables.
   *
   * The map is keyed by values from {@link #inputFiles()} and
   *     {@link #outputFiles()}.
   *
   * @return the table properties
   */
  public Map<String, TableProperties> tableProperties() {
    return tableProperties(nativeHandle_);
  }

  /**
   * Get the Reason for running the compaction.
   *
   * @return the reason.
   */
  public CompactionReason compactionReason() {
    return CompactionReason.fromValue(compactionReason(nativeHandle_));
  }

  //
  /**
   * Get the compression algorithm used for output files.
   *
   * @return the compression algorithm
   */
  public CompressionType compression() {
    return CompressionType.getCompressionType(compression(nativeHandle_));
  }

  /**
   * Get detailed information about this compaction.
   *
   * @return the detailed information, or null if not available.
   */
  public /* @Nullable */ CompactionJobStats stats() {
    final long statsHandle = stats(nativeHandle_);
    if (statsHandle == 0) {
      return null;
    }

    return new CompactionJobStats(statsHandle);
  }


  private static native long newCompactionJobInfo();
  @Override protected native void disposeInternal(final long handle);

  private static native byte[] columnFamilyName(final long handle);
  private static native Status status(final long handle);
  private static native long threadId(final long handle);
  private static native int jobId(final long handle);
  private static native int baseInputLevel(final long handle);
  private static native int outputLevel(final long handle);
  private static native String[] inputFiles(final long handle);
  private static native String[] outputFiles(final long handle);
  private static native Map<String, TableProperties> tableProperties(
      final long handle);
  private static native byte compactionReason(final long handle);
  private static native byte compression(final long handle);
  private static native long stats(final long handle);
}
