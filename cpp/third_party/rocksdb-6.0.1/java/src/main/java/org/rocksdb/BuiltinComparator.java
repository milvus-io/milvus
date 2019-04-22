// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Builtin RocksDB comparators
 *
 * <ol>
 *   <li>BYTEWISE_COMPARATOR - Sorts all keys in ascending bytewise
 *   order.</li>
 *   <li>REVERSE_BYTEWISE_COMPARATOR - Sorts all keys in descending bytewise
 *   order</li>
 * </ol>
 */
public enum BuiltinComparator {
  BYTEWISE_COMPARATOR, REVERSE_BYTEWISE_COMPARATOR
}
