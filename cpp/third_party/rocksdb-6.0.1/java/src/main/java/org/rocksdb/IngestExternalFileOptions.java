package org.rocksdb;
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

import java.util.List;

/**
 * IngestExternalFileOptions is used by
 * {@link RocksDB#ingestExternalFile(ColumnFamilyHandle, List, IngestExternalFileOptions)}.
 */
public class IngestExternalFileOptions extends RocksObject {

  public IngestExternalFileOptions() {
    super(newIngestExternalFileOptions());
  }

  /**
   * @param moveFiles {@link #setMoveFiles(boolean)}
   * @param snapshotConsistency {@link #setSnapshotConsistency(boolean)}
   * @param allowGlobalSeqNo {@link #setAllowGlobalSeqNo(boolean)}
   * @param allowBlockingFlush {@link #setAllowBlockingFlush(boolean)}
   */
  public IngestExternalFileOptions(final boolean moveFiles,
      final boolean snapshotConsistency, final boolean allowGlobalSeqNo,
      final boolean allowBlockingFlush) {
    super(newIngestExternalFileOptions(moveFiles, snapshotConsistency,
        allowGlobalSeqNo, allowBlockingFlush));
  }

  /**
   * Can be set to true to move the files instead of copying them.
   *
   * @return true if files will be moved
   */
  public boolean moveFiles() {
    return moveFiles(nativeHandle_);
  }

  /**
   * Can be set to true to move the files instead of copying them.
   *
   * @param moveFiles true if files should be moved instead of copied
   *
   * @return the reference to the current IngestExternalFileOptions.
   */
  public IngestExternalFileOptions setMoveFiles(final boolean moveFiles) {
    setMoveFiles(nativeHandle_, moveFiles);
    return this;
  }

  /**
   * If set to false, an ingested file keys could appear in existing snapshots
   * that where created before the file was ingested.
   *
   * @return true if snapshot consistency is assured
   */
  public boolean snapshotConsistency() {
    return snapshotConsistency(nativeHandle_);
  }

  /**
   * If set to false, an ingested file keys could appear in existing snapshots
   * that where created before the file was ingested.
   *
   * @param snapshotConsistency true if snapshot consistency is required
   *
   * @return the reference to the current IngestExternalFileOptions.
   */
  public IngestExternalFileOptions setSnapshotConsistency(
      final boolean snapshotConsistency) {
    setSnapshotConsistency(nativeHandle_, snapshotConsistency);
    return this;
  }

  /**
   * If set to false, {@link RocksDB#ingestExternalFile(ColumnFamilyHandle, List, IngestExternalFileOptions)}
   * will fail if the file key range overlaps with existing keys or tombstones in the DB.
   *
   * @return true if global seq numbers are assured
   */
  public boolean allowGlobalSeqNo() {
    return allowGlobalSeqNo(nativeHandle_);
  }

  /**
   * If set to false, {@link RocksDB#ingestExternalFile(ColumnFamilyHandle, List, IngestExternalFileOptions)}
   * will fail if the file key range overlaps with existing keys or tombstones in the DB.
   *
   * @param allowGlobalSeqNo true if global seq numbers are required
   *
   * @return the reference to the current IngestExternalFileOptions.
   */
  public IngestExternalFileOptions setAllowGlobalSeqNo(
      final boolean allowGlobalSeqNo) {
    setAllowGlobalSeqNo(nativeHandle_, allowGlobalSeqNo);
    return this;
  }

  /**
   * If set to false and the file key range overlaps with the memtable key range
   * (memtable flush required), IngestExternalFile will fail.
   *
   * @return true if blocking flushes may occur
   */
  public boolean allowBlockingFlush() {
    return allowBlockingFlush(nativeHandle_);
  }

  /**
   * If set to false and the file key range overlaps with the memtable key range
   * (memtable flush required), IngestExternalFile will fail.
   *
   * @param allowBlockingFlush true if blocking flushes are allowed
   *
   * @return the reference to the current IngestExternalFileOptions.
   */
  public IngestExternalFileOptions setAllowBlockingFlush(
      final boolean allowBlockingFlush) {
    setAllowBlockingFlush(nativeHandle_, allowBlockingFlush);
    return this;
  }

  /**
   * Returns true if duplicate keys in the file being ingested are
   * to be skipped rather than overwriting existing data under that key.
   *
   * @return true if duplicate keys in the file being ingested are to be
   *     skipped, false otherwise.
   */
  public boolean ingestBehind() {
    return ingestBehind(nativeHandle_);
  }

  /**
   * Set to true if you would like duplicate keys in the file being ingested
   * to be skipped rather than overwriting existing data under that key.
   *
   * Usecase: back-fill of some historical data in the database without
   * over-writing existing newer version of data.
   *
   * This option could only be used if the DB has been running
   * with DBOptions#allowIngestBehind() == true since the dawn of time.
   *
   * All files will be ingested at the bottommost level with seqno=0.
   *
   * Default: false
   *
   * @param ingestBehind true if you would like duplicate keys in the file being
   *     ingested to be skipped.
   *
   * @return the reference to the current IngestExternalFileOptions.
   */
  public IngestExternalFileOptions setIngestBehind(final boolean ingestBehind) {
    setIngestBehind(nativeHandle_, ingestBehind);
    return this;
  }

  /**
   * Returns true write if the global_seqno is written to a given offset
   * in the external SST file for backward compatibility.
   *
   * See {@link #setWriteGlobalSeqno(boolean)}.
   *
   * @return true if the global_seqno is written to a given offset,
   *     false otherwise.
   */
  public boolean writeGlobalSeqno() {
    return writeGlobalSeqno(nativeHandle_);
  }

  /**
   * Set to true if you would like to write the global_seqno to a given offset
   * in the external SST file for backward compatibility.
   *
   * Older versions of RocksDB write the global_seqno to a given offset within
   * the ingested SST files, and new versions of RocksDB do not.
   *
   * If you ingest an external SST using new version of RocksDB and would like
   * to be able to downgrade to an older version of RocksDB, you should set
   * {@link #writeGlobalSeqno()} to true.
   *
   * If your service is just starting to use the new RocksDB, we recommend that
   * you set this option to false, which brings two benefits:
   *    1. No extra random write for global_seqno during ingestion.
   *    2. Without writing external SST file, it's possible to do checksum.
   *
   * We have a plan to set this option to false by default in the future.
   *
   * Default: true
   *
   * @param writeGlobalSeqno true to write the gloal_seqno to a given offset,
   *     false otherwise
   *
   * @return the reference to the current IngestExternalFileOptions.
   */
  public IngestExternalFileOptions setWriteGlobalSeqno(
      final boolean writeGlobalSeqno) {
    setWriteGlobalSeqno(nativeHandle_, writeGlobalSeqno);
    return this;
  }

  private native static long newIngestExternalFileOptions();
  private native static long newIngestExternalFileOptions(
      final boolean moveFiles, final boolean snapshotConsistency,
      final boolean allowGlobalSeqNo, final boolean allowBlockingFlush);
  @Override protected final native void disposeInternal(final long handle);

  private native boolean moveFiles(final long handle);
  private native void setMoveFiles(final long handle, final boolean move_files);
  private native boolean snapshotConsistency(final long handle);
  private native void setSnapshotConsistency(final long handle,
      final boolean snapshotConsistency);
  private native boolean allowGlobalSeqNo(final long handle);
  private native void setAllowGlobalSeqNo(final long handle,
      final boolean allowGloablSeqNo);
  private native boolean allowBlockingFlush(final long handle);
  private native void setAllowBlockingFlush(final long handle,
      final boolean allowBlockingFlush);
  private native boolean ingestBehind(final long handle);
  private native void setIngestBehind(final long handle,
      final boolean ingestBehind);
  private native boolean writeGlobalSeqno(final long handle);
  private native void setWriteGlobalSeqno(final long handle,
      final boolean writeGlobalSeqNo);
}
