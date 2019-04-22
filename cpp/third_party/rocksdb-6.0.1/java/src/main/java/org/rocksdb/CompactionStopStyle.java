package org.rocksdb;

/**
 * Algorithm used to make a compaction request stop picking new files
 * into a single compaction run
 */
public enum CompactionStopStyle {

  /**
   * Pick files of similar size
   */
  CompactionStopStyleSimilarSize((byte)0x0),

  /**
   * Total size of picked files &gt; next file
   */
  CompactionStopStyleTotalSize((byte)0x1);


  private final byte value;

  CompactionStopStyle(final byte value) {
    this.value = value;
  }

  /**
   * Returns the byte value of the enumerations value
   *
   * @return byte representation
   */
  public byte getValue() {
    return value;
  }

  /**
   * Get CompactionStopStyle by byte value.
   *
   * @param value byte representation of CompactionStopStyle.
   *
   * @return {@link org.rocksdb.CompactionStopStyle} instance or null.
   * @throws java.lang.IllegalArgumentException if an invalid
   *     value is provided.
   */
  public static CompactionStopStyle getCompactionStopStyle(final byte value) {
    for (final CompactionStopStyle compactionStopStyle :
        CompactionStopStyle.values()) {
      if (compactionStopStyle.getValue() == value){
        return compactionStopStyle;
      }
    }
    throw new IllegalArgumentException(
        "Illegal value provided for CompactionStopStyle.");
  }
}
