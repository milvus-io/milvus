// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;


interface TransactionalOptions extends AutoCloseable {

  /**
   * True indicates snapshots will be set, just like if
   * {@link Transaction#setSnapshot()} had been called
   *
   * @return whether a snapshot will be set
   */
  boolean isSetSnapshot();

  /**
   * Setting the setSnapshot to true is the same as calling
   * {@link Transaction#setSnapshot()}.
   *
   * Default: false
   *
   * @param <T> The type of transactional options.
   * @param setSnapshot Whether to set a snapshot
   *
   * @return this TransactionalOptions instance
   */
  <T extends TransactionalOptions> T setSetSnapshot(final boolean setSnapshot);
}
