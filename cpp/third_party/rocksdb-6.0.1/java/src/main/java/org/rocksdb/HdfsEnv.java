// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * HDFS environment.
 */
public class HdfsEnv extends Env {

  /**
   <p>Creates a new environment that is used for HDFS environment.</p>
   *
   * <p>The caller must delete the result when it is
   * no longer needed.</p>
   *
   * @param fsName the HDFS as a string in the form "hdfs://hostname:port/"
   */
  public HdfsEnv(final String fsName) {
    super(createHdfsEnv(fsName));
  }

  private static native long createHdfsEnv(final String fsName);
  @Override protected final native void disposeInternal(final long handle);
}
