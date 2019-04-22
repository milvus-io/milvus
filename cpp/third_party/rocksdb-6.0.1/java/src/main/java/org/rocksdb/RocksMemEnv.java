// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Memory environment.
 */
//TODO(AR) rename to MemEnv
public class RocksMemEnv extends Env {

  /**
   * <p>Creates a new environment that stores its data
   * in memory and delegates all non-file-storage tasks to
   * {@code baseEnv}.</p>
   *
   * <p>The caller must delete the result when it is
   * no longer needed.</p>
   *
   * @param baseEnv the base environment,
   *     must remain live while the result is in use.
   */
  public RocksMemEnv(final Env baseEnv) {
    super(createMemEnv(baseEnv.nativeHandle_));
  }

  /**
   * @deprecated Use {@link #RocksMemEnv(Env)}.
   */
  @Deprecated
  public RocksMemEnv() {
    this(Env.getDefault());
  }

  private static native long createMemEnv(final long baseEnvHandle);
  @Override protected final native void disposeInternal(final long handle);
}
