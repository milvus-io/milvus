// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.util.Map;

/**
 * SstFileManager is used to track SST files in the DB and control their
 * deletion rate.
 *
 * All SstFileManager public functions are thread-safe.
 *
 * SstFileManager is not extensible.
 */
//@ThreadSafe
public final class SstFileManager extends RocksObject {

  public static final long RATE_BYTES_PER_SEC_DEFAULT = 0;
  public static final boolean DELETE_EXISTING_TRASH_DEFAULT = true;
  public static final double MAX_TRASH_DB_RATION_DEFAULT = 0.25;
  public static final long BYTES_MAX_DELETE_CHUNK_DEFAULT = 64 * 1024 * 1024;

  /**
   * Create a new SstFileManager that can be shared among multiple RocksDB
   * instances to track SST file and control there deletion rate.
   *
   * @param env the environment.
   *
   * @throws RocksDBException thrown if error happens in underlying native library.
   */
  public SstFileManager(final Env env) throws RocksDBException {
    this(env, null);
  }

  /**
   * Create a new SstFileManager that can be shared among multiple RocksDB
   * instances to track SST file and control there deletion rate.
   *
   * @param env the environment.
   * @param logger if not null, the logger will be used to log errors.
   *
   * @throws RocksDBException thrown if error happens in underlying native library.
   */
  public SstFileManager(final Env env, /*@Nullable*/  final Logger logger)
      throws RocksDBException {
    this(env, logger, RATE_BYTES_PER_SEC_DEFAULT);
  }

  /**
   * Create a new SstFileManager that can be shared among multiple RocksDB
   * instances to track SST file and control there deletion rate.
   *
   * @param env the environment.
   * @param logger if not null, the logger will be used to log errors.
   *
   * == Deletion rate limiting specific arguments ==
   * @param rateBytesPerSec how many bytes should be deleted per second, If
   *     this value is set to 1024 (1 Kb / sec) and we deleted a file of size
   *     4 Kb in 1 second, we will wait for another 3 seconds before we delete
   *     other files, Set to 0 to disable deletion rate limiting.
   *
   * @throws RocksDBException thrown if error happens in underlying native library.
   */
  public SstFileManager(final Env env, /*@Nullable*/  final Logger logger,
      final long rateBytesPerSec) throws RocksDBException {
    this(env, logger, rateBytesPerSec, MAX_TRASH_DB_RATION_DEFAULT);
  }

  /**
   * Create a new SstFileManager that can be shared among multiple RocksDB
   * instances to track SST file and control there deletion rate.
   *
   * @param env the environment.
   * @param logger if not null, the logger will be used to log errors.
   *
   * == Deletion rate limiting specific arguments ==
   * @param rateBytesPerSec how many bytes should be deleted per second, If
   *     this value is set to 1024 (1 Kb / sec) and we deleted a file of size
   *     4 Kb in 1 second, we will wait for another 3 seconds before we delete
   *     other files, Set to 0 to disable deletion rate limiting.
   * @param maxTrashDbRatio if the trash size constitutes for more than this
   *     fraction of the total DB size we will start deleting new files passed
   *     to DeleteScheduler immediately.
   *
   *  @throws RocksDBException thrown if error happens in underlying native library.
   */
  public SstFileManager(final Env env, /*@Nullable*/ final Logger logger,
      final long rateBytesPerSec, final double maxTrashDbRatio)
      throws RocksDBException {
    this(env, logger, rateBytesPerSec, maxTrashDbRatio,
        BYTES_MAX_DELETE_CHUNK_DEFAULT);
  }

  /**
   * Create a new SstFileManager that can be shared among multiple RocksDB
   * instances to track SST file and control there deletion rate.
   *
   * @param env the environment.
   * @param logger if not null, the logger will be used to log errors.
   *
   * == Deletion rate limiting specific arguments ==
   * @param rateBytesPerSec how many bytes should be deleted per second, If
   *     this value is set to 1024 (1 Kb / sec) and we deleted a file of size
   *     4 Kb in 1 second, we will wait for another 3 seconds before we delete
   *     other files, Set to 0 to disable deletion rate limiting.
   * @param maxTrashDbRatio if the trash size constitutes for more than this
   *     fraction of the total DB size we will start deleting new files passed
   *     to DeleteScheduler immediately.
   * @param bytesMaxDeleteChunk if a single file is larger than delete chunk,
   *     ftruncate the file by this size each time, rather than dropping the whole
   *     file. 0 means to always delete the whole file.
   *
   * @throws RocksDBException thrown if error happens in underlying native library.
   */
  public SstFileManager(final Env env, /*@Nullable*/final Logger logger,
      final long rateBytesPerSec, final double maxTrashDbRatio,
      final long bytesMaxDeleteChunk) throws RocksDBException {
    super(newSstFileManager(env.nativeHandle_,
        logger != null ? logger.nativeHandle_ : 0,
        rateBytesPerSec, maxTrashDbRatio, bytesMaxDeleteChunk));
  }


  /**
   * Update the maximum allowed space that should be used by RocksDB, if
   * the total size of the SST files exceeds {@code maxAllowedSpace}, writes to
   * RocksDB will fail.
   *
   * Setting {@code maxAllowedSpace} to 0 will disable this feature;
   * maximum allowed space will be infinite (Default value).
   *
   * @param maxAllowedSpace the maximum allowed space that should be used by
   *     RocksDB.
   */
  public void setMaxAllowedSpaceUsage(final long maxAllowedSpace) {
    setMaxAllowedSpaceUsage(nativeHandle_, maxAllowedSpace);
  }

  /**
   * Set the amount of buffer room each compaction should be able to leave.
   * In other words, at its maximum disk space consumption, the compaction
   * should still leave {@code compactionBufferSize} available on the disk so
   * that other background functions may continue, such as logging and flushing.
   *
   * @param compactionBufferSize the amount of buffer room each compaction
   *     should be able to leave.
   */
  public void setCompactionBufferSize(final long compactionBufferSize) {
    setCompactionBufferSize(nativeHandle_, compactionBufferSize);
  }

  /**
   * Determines if the total size of SST files exceeded the maximum allowed
   * space usage.
   *
   * @return true when the maximum allows space usage has been exceeded.
   */
  public boolean isMaxAllowedSpaceReached() {
    return isMaxAllowedSpaceReached(nativeHandle_);
  }

  /**
   * Determines if the total size of SST files as well as estimated size
   * of ongoing compactions exceeds the maximums allowed space usage.
   *
   * @return true when the total size of SST files as well as estimated size
   * of ongoing compactions exceeds the maximums allowed space usage.
   */
  public boolean isMaxAllowedSpaceReachedIncludingCompactions() {
    return isMaxAllowedSpaceReachedIncludingCompactions(nativeHandle_);
  }

  /**
   * Get the total size of all tracked files.
   *
   * @return the total size of all tracked files.
   */
  public long getTotalSize() {
    return getTotalSize(nativeHandle_);
  }

  /**
   * Gets all tracked files and their corresponding sizes.
   *
   * @return a map containing all tracked files and there corresponding sizes.
   */
  public Map<String, Long> getTrackedFiles() {
    return getTrackedFiles(nativeHandle_);
  }

  /**
   * Gets the delete rate limit.
   *
   * @return the delete rate limit (in bytes per second).
   */
  public long getDeleteRateBytesPerSecond() {
    return getDeleteRateBytesPerSecond(nativeHandle_);
  }

  /**
   * Set the delete rate limit.
   *
   * Zero means disable delete rate limiting and delete files immediately.
   *
   * @param deleteRate the delete rate limit (in bytes per second).
   */
  public void setDeleteRateBytesPerSecond(final long deleteRate) {
    setDeleteRateBytesPerSecond(nativeHandle_, deleteRate);
  }

  /**
   * Get the trash/DB size ratio where new files will be deleted immediately.
   *
   * @return the trash/DB size ratio.
   */
  public double getMaxTrashDBRatio() {
    return getMaxTrashDBRatio(nativeHandle_);
  }

  /**
   * Set the trash/DB size ratio where new files will be deleted immediately.
   *
   * @param ratio the trash/DB size ratio.
   */
  public void setMaxTrashDBRatio(final double ratio) {
    setMaxTrashDBRatio(nativeHandle_, ratio);
  }

  private native static long newSstFileManager(final long handle,
      final long logger_handle, final long rateBytesPerSec,
      final double maxTrashDbRatio, final long bytesMaxDeleteChunk)
      throws RocksDBException;
  private native void setMaxAllowedSpaceUsage(final long handle,
      final long maxAllowedSpace);
  private native void setCompactionBufferSize(final long handle,
      final long compactionBufferSize);
  private native boolean isMaxAllowedSpaceReached(final long handle);
  private native boolean isMaxAllowedSpaceReachedIncludingCompactions(
      final long handle);
  private native long getTotalSize(final long handle);
  private native Map<String, Long> getTrackedFiles(final long handle);
  private native long getDeleteRateBytesPerSecond(final long handle);
  private native void setDeleteRateBytesPerSecond(final long handle,
        final long deleteRate);
  private native double getMaxTrashDBRatio(final long handle);
  private native void setMaxTrashDBRatio(final long handle, final double ratio);
  @Override protected final native void disposeInternal(final long handle);
}
