// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Options for Compression
 */
public class CompressionOptions extends RocksObject {

  public CompressionOptions() {
    super(newCompressionOptions());
  }

  public CompressionOptions setWindowBits(final int windowBits) {
    setWindowBits(nativeHandle_, windowBits);
    return this;
  }

  public int windowBits() {
    return windowBits(nativeHandle_);
  }

  public CompressionOptions setLevel(final int level) {
    setLevel(nativeHandle_, level);
    return this;
  }

  public int level() {
    return level(nativeHandle_);
  }

  public CompressionOptions setStrategy(final int strategy) {
    setStrategy(nativeHandle_, strategy);
    return this;
  }

  public int strategy() {
    return strategy(nativeHandle_);
  }

  /**
   * Maximum size of dictionary used to prime the compression library. Currently
   * this dictionary will be constructed by sampling the first output file in a
   * subcompaction when the target level is bottommost. This dictionary will be
   * loaded into the compression library before compressing/uncompressing each
   * data block of subsequent files in the subcompaction. Effectively, this
   * improves compression ratios when there are repetitions across data blocks.
   *
   * A value of 0 indicates the feature is disabled.
   *
   * Default: 0.
   *
   * @param maxDictBytes Maximum bytes to use for the dictionary
   *
   * @return the reference to the current options
   */
  public CompressionOptions setMaxDictBytes(final int maxDictBytes) {
    setMaxDictBytes(nativeHandle_, maxDictBytes);
    return this;
  }

  /**
   * Maximum size of dictionary used to prime the compression library.
   *
   * @return The maximum bytes to use for the dictionary
   */
  public int maxDictBytes() {
    return maxDictBytes(nativeHandle_);
  }

  /**
   * Maximum size of training data passed to zstd's dictionary trainer. Using
   * zstd's dictionary trainer can achieve even better compression ratio
   * improvements than using {@link #setMaxDictBytes(int)} alone.
   *
   * The training data will be used to generate a dictionary
   * of {@link #maxDictBytes()}.
   *
   * Default: 0.
   *
   * @param zstdMaxTrainBytes Maximum bytes to use for training ZStd.
   *
   * @return the reference to the current options
   */
  public CompressionOptions setZStdMaxTrainBytes(final int zstdMaxTrainBytes) {
    setZstdMaxTrainBytes(nativeHandle_, zstdMaxTrainBytes);
    return this;
  }

  /**
   * Maximum size of training data passed to zstd's dictionary trainer.
   *
   * @return Maximum bytes to use for training ZStd
   */
  public int zstdMaxTrainBytes() {
    return zstdMaxTrainBytes(nativeHandle_);
  }

  /**
   * When the compression options are set by the user, it will be set to "true".
   * For bottommost_compression_opts, to enable it, user must set enabled=true.
   * Otherwise, bottommost compression will use compression_opts as default
   * compression options.
   *
   * For compression_opts, if compression_opts.enabled=false, it is still
   * used as compression options for compression process.
   *
   * Default: false.
   *
   * @param enabled true to use these compression options
   *     for the bottommost_compression_opts, false otherwise
   *
   * @return the reference to the current options
   */
  public CompressionOptions setEnabled(final boolean enabled) {
    setEnabled(nativeHandle_, enabled);
    return this;
  }

  /**
   * Determine whether these compression options
   * are used for the bottommost_compression_opts.
   *
   * @return true if these compression options are used
   *     for the bottommost_compression_opts, false otherwise
   */
  public boolean enabled() {
    return enabled(nativeHandle_);
  }


  private native static long newCompressionOptions();
  @Override protected final native void disposeInternal(final long handle);

  private native void setWindowBits(final long handle, final int windowBits);
  private native int windowBits(final long handle);
  private native void setLevel(final long handle, final int level);
  private native int level(final long handle);
  private native void setStrategy(final long handle, final int strategy);
  private native int strategy(final long handle);
  private native void setMaxDictBytes(final long handle, final int maxDictBytes);
  private native int maxDictBytes(final long handle);
  private native void setZstdMaxTrainBytes(final long handle,
      final int zstdMaxTrainBytes);
  private native int zstdMaxTrainBytes(final long handle);
  private native void setEnabled(final long handle, final boolean enabled);
  private native boolean enabled(final long handle);
}
