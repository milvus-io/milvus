// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.util.Arrays;
import java.util.Objects;

/**
 * ColumnFamilyHandle class to hold handles to underlying rocksdb
 * ColumnFamily Pointers.
 */
public class ColumnFamilyHandle extends RocksObject {
  ColumnFamilyHandle(final RocksDB rocksDB,
      final long nativeHandle) {
    super(nativeHandle);
    // rocksDB must point to a valid RocksDB instance;
    assert(rocksDB != null);
    // ColumnFamilyHandle must hold a reference to the related RocksDB instance
    // to guarantee that while a GC cycle starts ColumnFamilyHandle instances
    // are freed prior to RocksDB instances.
    this.rocksDB_ = rocksDB;
  }

  /**
   * Gets the name of the Column Family.
   *
   * @return The name of the Column Family.
   *
   * @throws RocksDBException if an error occurs whilst retrieving the name.
   */
  public byte[] getName() throws RocksDBException {
    return getName(nativeHandle_);
  }

  /**
   * Gets the ID of the Column Family.
   *
   * @return the ID of the Column Family.
   */
  public int getID() {
    return getID(nativeHandle_);
  }

  /**
   * Gets the up-to-date descriptor of the column family
   * associated with this handle. Since it fills "*desc" with the up-to-date
   * information, this call might internally lock and release DB mutex to
   * access the up-to-date CF options. In addition, all the pointer-typed
   * options cannot be referenced any longer than the original options exist.
   *
   * Note that this function is not supported in RocksDBLite.
   *
   * @return the up-to-date descriptor.
   *
   * @throws RocksDBException if an error occurs whilst retrieving the
   *     descriptor.
   */
  public ColumnFamilyDescriptor getDescriptor() throws RocksDBException {
    assert(isOwningHandle());
    return getDescriptor(nativeHandle_);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final ColumnFamilyHandle that = (ColumnFamilyHandle) o;
    try {
      return rocksDB_.nativeHandle_ == that.rocksDB_.nativeHandle_ &&
          getID() == that.getID() &&
          Arrays.equals(getName(), that.getName());
    } catch (RocksDBException e) {
      throw new RuntimeException("Cannot compare column family handles", e);
    }
  }

  @Override
  public int hashCode() {
    try {
      return Objects.hash(getName(), getID(), rocksDB_.nativeHandle_);
    } catch (RocksDBException e) {
      throw new RuntimeException("Cannot calculate hash code of column family handle", e);
    }
  }

  /**
   * <p>Deletes underlying C++ iterator pointer.</p>
   *
   * <p>Note: the underlying handle can only be safely deleted if the RocksDB
   * instance related to a certain ColumnFamilyHandle is still valid and
   * initialized. Therefore {@code disposeInternal()} checks if the RocksDB is
   * initialized before freeing the native handle.</p>
   */
  @Override
  protected void disposeInternal() {
    if(rocksDB_.isOwningHandle()) {
      disposeInternal(nativeHandle_);
    }
  }

  private native byte[] getName(final long handle) throws RocksDBException;
  private native int getID(final long handle);
  private native ColumnFamilyDescriptor getDescriptor(final long handle) throws RocksDBException;
  @Override protected final native void disposeInternal(final long handle);

  private final RocksDB rocksDB_;
}
