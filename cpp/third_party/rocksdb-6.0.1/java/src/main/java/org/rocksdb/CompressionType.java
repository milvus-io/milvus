// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Enum CompressionType
 *
 * <p>DB contents are stored in a set of blocks, each of which holds a
 * sequence of key,value pairs. Each block may be compressed before
 * being stored in a file. The following enum describes which
 * compression method (if any) is used to compress a block.</p>
 */
public enum CompressionType {

  NO_COMPRESSION((byte) 0x0, null),
  SNAPPY_COMPRESSION((byte) 0x1, "snappy"),
  ZLIB_COMPRESSION((byte) 0x2, "z"),
  BZLIB2_COMPRESSION((byte) 0x3, "bzip2"),
  LZ4_COMPRESSION((byte) 0x4, "lz4"),
  LZ4HC_COMPRESSION((byte) 0x5, "lz4hc"),
  XPRESS_COMPRESSION((byte) 0x6, "xpress"),
  ZSTD_COMPRESSION((byte)0x7, "zstd"),
  DISABLE_COMPRESSION_OPTION((byte)0x7F, null);

  /**
   * <p>Get the CompressionType enumeration value by
   * passing the library name to this method.</p>
   *
   * <p>If library cannot be found the enumeration
   * value {@code NO_COMPRESSION} will be returned.</p>
   *
   * @param libraryName compression library name.
   *
   * @return CompressionType instance.
   */
  public static CompressionType getCompressionType(String libraryName) {
    if (libraryName != null) {
      for (CompressionType compressionType : CompressionType.values()) {
        if (compressionType.getLibraryName() != null &&
            compressionType.getLibraryName().equals(libraryName)) {
          return compressionType;
        }
      }
    }
    return CompressionType.NO_COMPRESSION;
  }

  /**
   * <p>Get the CompressionType enumeration value by
   * passing the byte identifier to this method.</p>
   *
   * @param byteIdentifier of CompressionType.
   *
   * @return CompressionType instance.
   *
   * @throws IllegalArgumentException If CompressionType cannot be found for the
   *   provided byteIdentifier
   */
  public static CompressionType getCompressionType(byte byteIdentifier) {
    for (final CompressionType compressionType : CompressionType.values()) {
      if (compressionType.getValue() == byteIdentifier) {
        return compressionType;
      }
    }

    throw new IllegalArgumentException(
        "Illegal value provided for CompressionType.");
  }

  /**
   * <p>Returns the byte value of the enumerations value.</p>
   *
   * @return byte representation
   */
  public byte getValue() {
    return value_;
  }

  /**
   * <p>Returns the library name of the compression type
   * identified by the enumeration value.</p>
   *
   * @return library name
   */
  public String getLibraryName() {
    return libraryName_;
  }

  CompressionType(final byte value, final String libraryName) {
    value_ = value;
    libraryName_ = libraryName;
  }

  private final byte value_;
  private final String libraryName_;
}
