// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Base class for comparators which will receive
 * byte[] based access via org.rocksdb.Slice in their
 * compare method implementation.
 *
 * byte[] based slices perform better when small keys
 * are involved. When using larger keys consider
 * using @see org.rocksdb.DirectComparator
 */
public abstract class Comparator extends AbstractComparator<Slice> {

  public Comparator(final ComparatorOptions copt) {
    super(copt);
  }

  @Override
  protected long initializeNative(final long... nativeParameterHandles) {
    return createNewComparator0(nativeParameterHandles[0]);
  }

  @Override
  final ComparatorType getComparatorType() {
    return ComparatorType.JAVA_COMPARATOR;
  }

  private native long createNewComparator0(final long comparatorOptionsHandle);
}
