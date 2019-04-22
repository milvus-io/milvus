// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
package org.rocksdb;

/**
 * A CompactionFilter allows an application to modify/delete a key-value at
 * the time of compaction.
 *
 * At present we just permit an overriding Java class to wrap a C++
 * implementation
 */
public abstract class AbstractCompactionFilter<T extends AbstractSlice<?>>
    extends RocksObject {

  public static class Context {
    private final boolean fullCompaction;
    private final boolean manualCompaction;

    public Context(final boolean fullCompaction, final boolean manualCompaction) {
      this.fullCompaction = fullCompaction;
      this.manualCompaction = manualCompaction;
    }

    /**
     * Does this compaction run include all data files
     *
     * @return true if this is a full compaction run
     */
    public boolean isFullCompaction() {
      return fullCompaction;
    }

    /**
     * Is this compaction requested by the client,
     * or is it occurring as an automatic compaction process
     *
     * @return true if the compaction was initiated by the client
     */
    public boolean isManualCompaction() {
      return manualCompaction;
    }
  }

  protected AbstractCompactionFilter(final long nativeHandle) {
    super(nativeHandle);
  }

  /**
   * Deletes underlying C++ compaction pointer.
   *
   * Note that this function should be called only after all
   * RocksDB instances referencing the compaction filter are closed.
   * Otherwise an undefined behavior will occur.
   */
  @Override
  protected final native void disposeInternal(final long handle);
}
