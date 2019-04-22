// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Persistent cache for caching IO pages on a persistent medium. The
 * cache is specifically designed for persistent read cache.
 */
public class PersistentCache extends RocksObject {

  public PersistentCache(final Env env, final String path, final long size,
      final Logger logger, final boolean optimizedForNvm)
      throws RocksDBException {
    super(newPersistentCache(env.nativeHandle_, path, size,
        logger.nativeHandle_, optimizedForNvm));
  }

  private native static long newPersistentCache(final long envHandle,
    final String path, final long size, final long loggerHandle,
    final boolean optimizedForNvm) throws RocksDBException;

  @Override protected final native void disposeInternal(final long handle);
}
