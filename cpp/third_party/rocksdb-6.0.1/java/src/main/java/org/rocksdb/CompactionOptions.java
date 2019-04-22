// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.util.List;

/**
 * CompactionOptions are used in
 * {@link RocksDB#compactFiles(CompactionOptions, ColumnFamilyHandle, List, int, int, CompactionJobInfo)}
 * calls.
 */
public class CompactionOptions extends RocksObject {

  public CompactionOptions() {
    super(newCompactionOptions());
  }

  /**
   * Get the compaction output compression type.
   *
   * See {@link #setCompression(CompressionType)}.
   *
   * @return the compression type.
   */
  public CompressionType compression() {
    return CompressionType.getCompressionType(
        compression(nativeHandle_));
  }

  /**
   * Set the compaction output compression type.
   *
   * Default: snappy
   *
   * If set to {@link CompressionType#DISABLE_COMPRESSION_OPTION},
   * RocksDB will choose compression type according to the
   * {@link ColumnFamilyOptions#compressionType()}, taking into account
   * the output level if {@link ColumnFamilyOptions#compressionPerLevel()}
   * is specified.
   *
   * @param compression the compression type to use for compaction output.
   *
   * @return the instance of the current Options.
   */
  public CompactionOptions setCompression(final CompressionType compression) {
    setCompression(nativeHandle_, compression.getValue());
    return this;
  }

  /**
   * Get the compaction output file size limit.
   *
   * See {@link #setOutputFileSizeLimit(long)}.
   *
   * @return the file size limit.
   */
  public long outputFileSizeLimit() {
    return outputFileSizeLimit(nativeHandle_);
  }

  /**
   * Compaction will create files of size {@link #outputFileSizeLimit()}.
   *
   * Default: 2^64-1, which means that compaction will create a single file
   *
   * @param outputFileSizeLimit the size limit
   *
   * @return the instance of the current Options.
   */
  public CompactionOptions setOutputFileSizeLimit(
      final long outputFileSizeLimit) {
    setOutputFileSizeLimit(nativeHandle_, outputFileSizeLimit);
    return this;
  }

  /**
   * Get the maximum number of threads that will concurrently perform a
   * compaction job.
   *
   * @return the maximum number of threads.
   */
  public int maxSubcompactions() {
    return maxSubcompactions(nativeHandle_);
  }

  /**
   * This value represents the maximum number of threads that will
   * concurrently perform a compaction job by breaking it into multiple,
   * smaller ones that are run simultaneously.
   *
   * Default: 0 (i.e. no subcompactions)
   *
   * If &gt; 0, it will replace the option in
   * {@link DBOptions#maxSubcompactions()} for this compaction.
   *
   * @param maxSubcompactions The maximum number of threads that will
   *     concurrently perform a compaction job
   *
   * @return the instance of the current Options.
   */
  public CompactionOptions setMaxSubcompactions(final int maxSubcompactions) {
    setMaxSubcompactions(nativeHandle_, maxSubcompactions);
    return this;
  }

  private static native long newCompactionOptions();
  @Override protected final native void disposeInternal(final long handle);

  private static native byte compression(final long handle);
  private static native void setCompression(final long handle,
      final byte compressionTypeValue);
  private static native long outputFileSizeLimit(final long handle);
  private static native void setOutputFileSizeLimit(final long handle,
      final long outputFileSizeLimit);
  private static native int maxSubcompactions(final long handle);
  private static native void setMaxSubcompactions(final long handle,
      final int maxSubcompactions);
}
