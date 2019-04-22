// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Timed environment.
 */
public class TimedEnv extends Env {

  /**
   * <p>Creates a new environment that measures function call times for
   * filesystem operations, reporting results to variables in PerfContext.</p>
   *
   *
   * <p>The caller must delete the result when it is
   * no longer needed.</p>
   *
   * @param baseEnv the base environment,
   *     must remain live while the result is in use.
   */
  public TimedEnv(final Env baseEnv) {
    super(createTimedEnv(baseEnv.nativeHandle_));
  }

  private static native long createTimedEnv(final long baseEnvHandle);
  @Override protected final native void disposeInternal(final long handle);
}
