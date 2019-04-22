package org.rocksdb;

/**
 * RocksDB log levels.
 */
public enum InfoLogLevel {
  DEBUG_LEVEL((byte)0),
  INFO_LEVEL((byte)1),
  WARN_LEVEL((byte)2),
  ERROR_LEVEL((byte)3),
  FATAL_LEVEL((byte)4),
  HEADER_LEVEL((byte)5),
  NUM_INFO_LOG_LEVELS((byte)6);

  private final byte value_;

  private InfoLogLevel(final byte value) {
    value_ = value;
  }

  /**
   * Returns the byte value of the enumerations value
   *
   * @return byte representation
   */
  public byte getValue() {
    return value_;
  }

  /**
   * Get InfoLogLevel by byte value.
   *
   * @param value byte representation of InfoLogLevel.
   *
   * @return {@link org.rocksdb.InfoLogLevel} instance.
   * @throws java.lang.IllegalArgumentException if an invalid
   *     value is provided.
   */
  public static InfoLogLevel getInfoLogLevel(final byte value) {
    for (final InfoLogLevel infoLogLevel : InfoLogLevel.values()) {
      if (infoLogLevel.getValue() == value){
        return infoLogLevel;
      }
    }
    throw new IllegalArgumentException(
        "Illegal value provided for InfoLogLevel.");
  }
}
