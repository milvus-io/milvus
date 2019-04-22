// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Provides Checkpoint functionality. Checkpoints
 * provide persistent snapshots of RocksDB databases.
 */
public class Checkpoint extends RocksObject {

  /**
   * Creates a Checkpoint object to be used for creating open-able
   * snapshots.
   *
   * @param db {@link RocksDB} instance.
   * @return a Checkpoint instance.
   *
   * @throws java.lang.IllegalArgumentException if {@link RocksDB}
   *     instance is null.
   * @throws java.lang.IllegalStateException if {@link RocksDB}
   *     instance is not initialized.
   */
  public static Checkpoint create(final RocksDB db) {
    if (db == null) {
      throw new IllegalArgumentException(
          "RocksDB instance shall not be null.");
    } else if (!db.isOwningHandle()) {
      throw new IllegalStateException(
          "RocksDB instance must be initialized.");
    }
    Checkpoint checkpoint = new Checkpoint(db);
    return checkpoint;
  }

  /**
   * <p>Builds an open-able snapshot of RocksDB on the same disk, which
   * accepts an output directory on the same disk, and under the directory
   * (1) hard-linked SST files pointing to existing live SST files
   * (2) a copied manifest files and other files</p>
   *
   * @param checkpointPath path to the folder where the snapshot is going
   *     to be stored.
   * @throws RocksDBException thrown if an error occurs within the native
   *     part of the library.
   */
  public void createCheckpoint(final String checkpointPath)
      throws RocksDBException {
    createCheckpoint(nativeHandle_, checkpointPath);
  }

  private Checkpoint(final RocksDB db) {
    super(newCheckpoint(db.nativeHandle_));
    this.db_ = db;
  }

  private final RocksDB db_;

  private static native long newCheckpoint(long dbHandle);
  @Override protected final native void disposeInternal(final long handle);

  private native void createCheckpoint(long handle, String checkpointPath)
      throws RocksDBException;
}
