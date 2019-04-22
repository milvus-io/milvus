// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Filters are stored in rocksdb and are consulted automatically
 * by rocksdb to decide whether or not to read some
 * information from disk. In many cases, a filter can cut down the
 * number of disk seeks form a handful to a single disk seek per
 * DB::Get() call.
 */
//TODO(AR) should be renamed FilterPolicy
public abstract class Filter extends RocksObject {

  protected Filter(final long nativeHandle) {
    super(nativeHandle);
  }

  /**
   * Deletes underlying C++ filter pointer.
   *
   * Note that this function should be called only after all
   * RocksDB instances referencing the filter are closed.
   * Otherwise an undefined behavior will occur.
   */
  @Override
  protected void disposeInternal() {
    disposeInternal(nativeHandle_);
  }

  @Override
  protected final native void disposeInternal(final long handle);
}
