// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb.util;

public class SizeUnit {
  public static final long KB = 1024L;
  public static final long MB = KB * KB;
  public static final long GB = KB * MB;
  public static final long TB = KB * GB;
  public static final long PB = KB * TB;

  private SizeUnit() {}
}
