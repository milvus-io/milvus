// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Just a Java wrapper around EmptyValueCompactionFilter implemented in C++
 */
public class RemoveEmptyValueCompactionFilter
    extends AbstractCompactionFilter<Slice> {
  public RemoveEmptyValueCompactionFilter() {
    super(createNewRemoveEmptyValueCompactionFilter0());
  }

  private native static long createNewRemoveEmptyValueCompactionFilter0();
}
