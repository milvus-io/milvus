// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import org.junit.Test;


public class CompressionTypesTest {
  @Test
  public void getCompressionType() {
    for (final CompressionType compressionType : CompressionType.values()) {
      String libraryName = compressionType.getLibraryName();
      compressionType.equals(CompressionType.getCompressionType(
          libraryName));
    }
  }
}
