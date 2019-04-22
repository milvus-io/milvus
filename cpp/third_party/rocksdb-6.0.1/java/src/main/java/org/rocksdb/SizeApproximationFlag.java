package org.rocksdb;

import java.util.List;

/**
 * Flags for
 * {@link RocksDB#getApproximateSizes(ColumnFamilyHandle, List, SizeApproximationFlag...)}
 * that specify whether memtable stats should be included,
 * or file stats approximation or both.
 */
public enum SizeApproximationFlag {
  NONE((byte)0x0),
  INCLUDE_MEMTABLES((byte)0x1),
  INCLUDE_FILES((byte)0x2);

  private final byte value;

  SizeApproximationFlag(final byte value) {
    this.value = value;
  }

  /**
   * Get the internal byte representation.
   *
   * @return the internal representation.
   */
  byte getValue() {
    return value;
  }
}
