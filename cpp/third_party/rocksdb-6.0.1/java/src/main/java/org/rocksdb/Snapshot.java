// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Snapshot of database
 */
public class Snapshot extends RocksObject {
  Snapshot(final long nativeHandle) {
    super(nativeHandle);

    // The pointer to the snapshot is always released
    // by the database instance.
    disOwnNativeHandle();
  }

  /**
   * Return the associated sequence number;
   *
   * @return the associated sequence number of
   *     this snapshot.
   */
  public long getSequenceNumber() {
    return getSequenceNumber(nativeHandle_);
  }

  @Override
  protected final void disposeInternal(final long handle) {
    /**
     * Nothing to release, we never own the pointer for a
     * Snapshot. The pointer
     * to the snapshot is released by the database
     * instance.
     */
  }

  private native long getSequenceNumber(long handle);
}
