// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * <p>A RocksEnv is an interface used by the rocksdb implementation to access
 * operating system functionality like the filesystem etc.</p>
 *
 * <p>All Env implementations are safe for concurrent access from
 * multiple threads without any external synchronization.</p>
 */
public class RocksEnv extends Env {

  /**
   * <p>Package-private constructor that uses the specified native handle
   * to construct a RocksEnv.</p>
   *
   * <p>Note that the ownership of the input handle
   * belongs to the caller, and the newly created RocksEnv will not take
   * the ownership of the input handle.  As a result, calling
   * {@code dispose()} of the created RocksEnv will be no-op.</p>
   */
  RocksEnv(final long handle) {
    super(handle);
  }

  @Override
  protected native final void disposeInternal(final long handle);
}
