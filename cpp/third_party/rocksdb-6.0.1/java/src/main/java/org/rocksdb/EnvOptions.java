// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Options while opening a file to read/write
 */
public class EnvOptions extends RocksObject {
  static {
    RocksDB.loadLibrary();
  }

  /**
   * Construct with default Options
   */
  public EnvOptions() {
    super(newEnvOptions());
  }

  /**
   * Construct from {@link DBOptions}.
   *
   * @param dbOptions the database options.
   */
  public EnvOptions(final DBOptions dbOptions) {
    super(newEnvOptions(dbOptions.nativeHandle_));
  }

  /**
   * Enable/Disable memory mapped reads.
   *
   * Default: false
   *
   * @param useMmapReads true to enable memory mapped reads, false to disable.
   *
   * @return the reference to these options.
   */
  public EnvOptions setUseMmapReads(final boolean useMmapReads) {
    setUseMmapReads(nativeHandle_, useMmapReads);
    return this;
  }

  /**
   * Determine if memory mapped reads are in-use.
   *
   * @return true if memory mapped reads are in-use, false otherwise.
   */
  public boolean useMmapReads() {
    assert(isOwningHandle());
    return useMmapReads(nativeHandle_);
  }

  /**
   * Enable/Disable memory mapped Writes.
   *
   * Default: true
   *
   * @param useMmapWrites true to enable memory mapped writes, false to disable.
   *
   * @return the reference to these options.
   */
  public EnvOptions setUseMmapWrites(final boolean useMmapWrites) {
    setUseMmapWrites(nativeHandle_, useMmapWrites);
    return this;
  }

  /**
   * Determine if memory mapped writes are in-use.
   *
   * @return true if memory mapped writes are in-use, false otherwise.
   */
  public boolean useMmapWrites() {
    assert(isOwningHandle());
    return useMmapWrites(nativeHandle_);
  }

  /**
   * Enable/Disable direct reads, i.e. {@code O_DIRECT}.
   *
   * Default: false
   *
   * @param useDirectReads true to enable direct reads, false to disable.
   *
   * @return the reference to these options.
   */
  public EnvOptions setUseDirectReads(final boolean useDirectReads) {
    setUseDirectReads(nativeHandle_, useDirectReads);
    return this;
  }

  /**
   * Determine if direct reads are in-use.
   *
   * @return true if direct reads are in-use, false otherwise.
   */
  public boolean useDirectReads() {
    assert(isOwningHandle());
    return useDirectReads(nativeHandle_);
  }

  /**
   * Enable/Disable direct writes, i.e. {@code O_DIRECT}.
   *
   * Default: false
   *
   * @param useDirectWrites true to enable direct writes, false to disable.
   *
   * @return the reference to these options.
   */
  public EnvOptions setUseDirectWrites(final boolean useDirectWrites) {
    setUseDirectWrites(nativeHandle_, useDirectWrites);
    return this;
  }

  /**
   * Determine if direct writes are in-use.
   *
   * @return true if direct writes are in-use, false otherwise.
   */
  public boolean useDirectWrites() {
    assert(isOwningHandle());
    return useDirectWrites(nativeHandle_);
  }

  /**
   * Enable/Disable fallocate calls.
   *
   * Default: true
   *
   * If false, {@code fallocate()} calls are bypassed.
   *
   * @param allowFallocate true to enable fallocate calls, false to disable.
   *
   * @return the reference to these options.
   */
  public EnvOptions setAllowFallocate(final boolean allowFallocate) {
    setAllowFallocate(nativeHandle_, allowFallocate);
    return this;
  }

  /**
   * Determine if fallocate calls are used.
   *
   * @return true if fallocate calls are used, false otherwise.
   */
  public boolean allowFallocate() {
    assert(isOwningHandle());
    return allowFallocate(nativeHandle_);
  }

  /**
   * Enable/Disable the {@code FD_CLOEXEC} bit when opening file descriptors.
   *
   * Default: true
   *
   * @param setFdCloexec true to enable the {@code FB_CLOEXEC} bit,
   *     false to disable.
   *
   * @return the reference to these options.
   */
  public EnvOptions setSetFdCloexec(final boolean setFdCloexec) {
    setSetFdCloexec(nativeHandle_, setFdCloexec);
    return this;
  }

  /**
   * Determine i fthe {@code FD_CLOEXEC} bit is set when opening file
   * descriptors.
   *
   * @return true if the {@code FB_CLOEXEC} bit is enabled, false otherwise.
   */
  public boolean setFdCloexec() {
    assert(isOwningHandle());
    return setFdCloexec(nativeHandle_);
  }

  /**
   * Allows OS to incrementally sync files to disk while they are being
   * written, in the background. Issue one request for every
   * {@code bytesPerSync} written.
   *
   * Default: 0
   *
   * @param bytesPerSync 0 to disable, otherwise the number of bytes.
   *
   * @return the reference to these options.
   */
  public EnvOptions setBytesPerSync(final long bytesPerSync) {
    setBytesPerSync(nativeHandle_, bytesPerSync);
    return this;
  }

  /**
   * Get the number of incremental bytes per sync written in the background.
   *
   * @return 0 if disabled, otherwise the number of bytes.
   */
  public long bytesPerSync() {
    assert(isOwningHandle());
    return bytesPerSync(nativeHandle_);
  }

  /**
   * If true, we will preallocate the file with {@code FALLOC_FL_KEEP_SIZE}
   * flag, which means that file size won't change as part of preallocation.
   * If false, preallocation will also change the file size. This option will
   * improve the performance in workloads where you sync the data on every
   * write. By default, we set it to true for MANIFEST writes and false for
   * WAL writes
   *
   * @param fallocateWithKeepSize true to preallocate, false otherwise.
   *
   * @return the reference to these options.
   */
  public EnvOptions setFallocateWithKeepSize(
      final boolean fallocateWithKeepSize) {
    setFallocateWithKeepSize(nativeHandle_, fallocateWithKeepSize);
    return this;
  }

  /**
   * Determine if file is preallocated.
   *
   * @return true if the file is preallocated, false otherwise.
   */
  public boolean fallocateWithKeepSize() {
    assert(isOwningHandle());
    return fallocateWithKeepSize(nativeHandle_);
  }

  /**
   * See {@link DBOptions#setCompactionReadaheadSize(long)}.
   *
   * @param compactionReadaheadSize the compaction read-ahead size.
   *
   * @return the reference to these options.
   */
  public EnvOptions setCompactionReadaheadSize(
      final long compactionReadaheadSize) {
    setCompactionReadaheadSize(nativeHandle_, compactionReadaheadSize);
    return this;
  }

  /**
   * See {@link DBOptions#compactionReadaheadSize()}.
   *
   * @return the compaction read-ahead size.
   */
  public long compactionReadaheadSize() {
    assert(isOwningHandle());
    return compactionReadaheadSize(nativeHandle_);
  }

  /**
   * See {@link DBOptions#setRandomAccessMaxBufferSize(long)}.
   *
   * @param randomAccessMaxBufferSize the max buffer size for random access.
   *
   * @return the reference to these options.
   */
  public EnvOptions setRandomAccessMaxBufferSize(
      final long randomAccessMaxBufferSize) {
    setRandomAccessMaxBufferSize(nativeHandle_, randomAccessMaxBufferSize);
    return this;
  }

  /**
   * See {@link DBOptions#randomAccessMaxBufferSize()}.
   *
   * @return the max buffer size for random access.
   */
  public long randomAccessMaxBufferSize() {
    assert(isOwningHandle());
    return randomAccessMaxBufferSize(nativeHandle_);
  }

  /**
   * See {@link DBOptions#setWritableFileMaxBufferSize(long)}.
   *
   * @param writableFileMaxBufferSize the max buffer size.
   *
   * @return the reference to these options.
   */
  public EnvOptions setWritableFileMaxBufferSize(
      final long writableFileMaxBufferSize) {
    setWritableFileMaxBufferSize(nativeHandle_, writableFileMaxBufferSize);
    return this;
  }

  /**
   * See {@link DBOptions#writableFileMaxBufferSize()}.
   *
   * @return the max buffer size.
   */
  public long writableFileMaxBufferSize() {
    assert(isOwningHandle());
    return writableFileMaxBufferSize(nativeHandle_);
  }

  /**
   * Set the write rate limiter for flush and compaction.
   *
   * @param rateLimiter the rate limiter.
   *
   * @return the reference to these options.
   */
  public EnvOptions setRateLimiter(final RateLimiter rateLimiter) {
    this.rateLimiter = rateLimiter;
    setRateLimiter(nativeHandle_, rateLimiter.nativeHandle_);
    return this;
  }

  /**
   * Get the write rate limiter for flush and compaction.
   *
   * @return the rate limiter.
   */
  public RateLimiter rateLimiter() {
    assert(isOwningHandle());
    return rateLimiter;
  }

  private native static long newEnvOptions();
  private native static long newEnvOptions(final long dboptions_handle);
  @Override protected final native void disposeInternal(final long handle);

  private native void setUseMmapReads(final long handle,
      final boolean useMmapReads);
  private native boolean useMmapReads(final long handle);
  private native void setUseMmapWrites(final long handle,
      final boolean useMmapWrites);
  private native boolean useMmapWrites(final long handle);
  private native void setUseDirectReads(final long handle,
      final boolean useDirectReads);
  private native boolean useDirectReads(final long handle);
  private native void setUseDirectWrites(final long handle,
      final boolean useDirectWrites);
  private native boolean useDirectWrites(final long handle);
  private native void setAllowFallocate(final long handle,
      final boolean allowFallocate);
  private native boolean allowFallocate(final long handle);
  private native void setSetFdCloexec(final long handle,
      final boolean setFdCloexec);
  private native boolean setFdCloexec(final long handle);
  private native void setBytesPerSync(final long handle,
      final long bytesPerSync);
  private native long bytesPerSync(final long handle);
  private native void setFallocateWithKeepSize(
      final long handle, final boolean fallocateWithKeepSize);
  private native boolean fallocateWithKeepSize(final long handle);
  private native void setCompactionReadaheadSize(
      final long handle, final long compactionReadaheadSize);
  private native long compactionReadaheadSize(final long handle);
  private native void setRandomAccessMaxBufferSize(
      final long handle, final long randomAccessMaxBufferSize);
  private native long randomAccessMaxBufferSize(final long handle);
  private native void setWritableFileMaxBufferSize(
      final long handle, final long writableFileMaxBufferSize);
  private native long writableFileMaxBufferSize(final long handle);
  private native void setRateLimiter(final long handle,
      final long rateLimiterHandle);
  private RateLimiter rateLimiter;
}
