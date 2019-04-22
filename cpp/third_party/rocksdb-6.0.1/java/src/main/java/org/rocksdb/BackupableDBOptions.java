// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.io.File;

/**
 * <p>BackupableDBOptions to control the behavior of a backupable database.
 * It will be used during the creation of a {@link org.rocksdb.BackupEngine}.
 * </p>
 * <p>Note that dispose() must be called before an Options instance
 * become out-of-scope to release the allocated memory in c++.</p>
 *
 * @see org.rocksdb.BackupEngine
 */
public class BackupableDBOptions extends RocksObject {

  private Env backupEnv = null;
  private Logger infoLog = null;
  private RateLimiter backupRateLimiter = null;
  private RateLimiter restoreRateLimiter = null;

  /**
   * <p>BackupableDBOptions constructor.</p>
   *
   * @param path Where to keep the backup files. Has to be different than db
   *   name. Best to set this to {@code db name_ + "/backups"}
   * @throws java.lang.IllegalArgumentException if illegal path is used.
   */
  public BackupableDBOptions(final String path) {
    super(newBackupableDBOptions(ensureWritableFile(path)));
  }

  private static String ensureWritableFile(final String path) {
    final File backupPath = path == null ? null : new File(path);
    if (backupPath == null || !backupPath.isDirectory() ||
        !backupPath.canWrite()) {
      throw new IllegalArgumentException("Illegal path provided.");
    } else {
      return path;
    }
  }

  /**
   * <p>Returns the path to the BackupableDB directory.</p>
   *
   * @return the path to the BackupableDB directory.
   */
  public String backupDir() {
    assert(isOwningHandle());
    return backupDir(nativeHandle_);
  }

  /**
   * Backup Env object. It will be used for backup file I/O. If it's
   * null, backups will be written out using DBs Env. Otherwise
   * backup's I/O will be performed using this object.
   *
   * If you want to have backups on HDFS, use HDFS Env here!
   *
   * Default: null
   *
   * @param env The environment to use
   * @return instance of current BackupableDBOptions.
   */
  public BackupableDBOptions setBackupEnv(final Env env) {
    assert(isOwningHandle());
    setBackupEnv(nativeHandle_, env.nativeHandle_);
    this.backupEnv = env;
    return this;
  }

  /**
   * Backup Env object. It will be used for backup file I/O. If it's
   * null, backups will be written out using DBs Env. Otherwise
   * backup's I/O will be performed using this object.
   *
   * If you want to have backups on HDFS, use HDFS Env here!
   *
   * Default: null
   *
   * @return The environment in use
   */
  public Env backupEnv() {
    return this.backupEnv;
  }

  /**
   * <p>Share table files between backups.</p>
   *
   * @param shareTableFiles If {@code share_table_files == true}, backup will
   *   assume that table files with same name have the same contents. This
   *   enables incremental backups and avoids unnecessary data copies. If
   *   {@code share_table_files == false}, each backup will be on its own and
   *   will not share any data with other backups.
   *
   * <p>Default: true</p>
   *
   * @return instance of current BackupableDBOptions.
   */
  public BackupableDBOptions setShareTableFiles(final boolean shareTableFiles) {
    assert(isOwningHandle());
    setShareTableFiles(nativeHandle_, shareTableFiles);
    return this;
  }

  /**
   * <p>Share table files between backups.</p>
   *
   * @return boolean value indicating if SST files will be shared between
   *     backups.
   */
  public boolean shareTableFiles() {
    assert(isOwningHandle());
    return shareTableFiles(nativeHandle_);
  }

  /**
   * Set the logger to use for Backup info and error messages
   *
   * @param logger The logger to use for the backup
   * @return instance of current BackupableDBOptions.
   */
  public BackupableDBOptions setInfoLog(final Logger logger) {
    assert(isOwningHandle());
    setInfoLog(nativeHandle_, logger.nativeHandle_);
    this.infoLog = logger;
    return this;
  }

  /**
   * Set the logger to use for Backup info and error messages
   *
   * Default: null
   *
   * @return The logger in use for the backup
   */
  public Logger infoLog() {
    return this.infoLog;
  }

  /**
   * <p>Set synchronous backups.</p>
   *
   * @param sync If {@code sync == true}, we can guarantee you'll get consistent
   *   backup even on a machine crash/reboot. Backup process is slower with sync
   *   enabled. If {@code sync == false}, we don't guarantee anything on machine
   *   reboot. However, chances are some of the backups are consistent.
   *
   * <p>Default: true</p>
   *
   * @return instance of current BackupableDBOptions.
   */
  public BackupableDBOptions setSync(final boolean sync) {
    assert(isOwningHandle());
    setSync(nativeHandle_, sync);
    return this;
  }

  /**
   * <p>Are synchronous backups activated.</p>
   *
   * @return boolean value if synchronous backups are configured.
   */
  public boolean sync() {
    assert(isOwningHandle());
    return sync(nativeHandle_);
  }

  /**
   * <p>Set if old data will be destroyed.</p>
   *
   * @param destroyOldData If true, it will delete whatever backups there are
   *   already.
   *
   * <p>Default: false</p>
   *
   * @return instance of current BackupableDBOptions.
   */
  public BackupableDBOptions setDestroyOldData(final boolean destroyOldData) {
    assert(isOwningHandle());
    setDestroyOldData(nativeHandle_, destroyOldData);
    return this;
  }

  /**
   * <p>Returns if old data will be destroyed will performing new backups.</p>
   *
   * @return boolean value indicating if old data will be destroyed.
   */
  public boolean destroyOldData() {
    assert(isOwningHandle());
    return destroyOldData(nativeHandle_);
  }

  /**
   * <p>Set if log files shall be persisted.</p>
   *
   * @param backupLogFiles If false, we won't backup log files. This option can
   *   be useful for backing up in-memory databases where log file are
   *   persisted, but table files are in memory.
   *
   * <p>Default: true</p>
   *
   * @return instance of current BackupableDBOptions.
   */
  public BackupableDBOptions setBackupLogFiles(final boolean backupLogFiles) {
    assert(isOwningHandle());
    setBackupLogFiles(nativeHandle_, backupLogFiles);
    return this;
  }

  /**
   * <p>Return information if log files shall be persisted.</p>
   *
   * @return boolean value indicating if log files will be persisted.
   */
  public boolean backupLogFiles() {
    assert(isOwningHandle());
    return backupLogFiles(nativeHandle_);
  }

  /**
   * <p>Set backup rate limit.</p>
   *
   * @param backupRateLimit Max bytes that can be transferred in a second during
   *   backup. If 0 or negative, then go as fast as you can.
   *
   * <p>Default: 0</p>
   *
   * @return instance of current BackupableDBOptions.
   */
  public BackupableDBOptions setBackupRateLimit(long backupRateLimit) {
    assert(isOwningHandle());
    backupRateLimit = (backupRateLimit <= 0) ? 0 : backupRateLimit;
    setBackupRateLimit(nativeHandle_, backupRateLimit);
    return this;
  }

  /**
   * <p>Return backup rate limit which described the max bytes that can be
   * transferred in a second during backup.</p>
   *
   * @return numerical value describing the backup transfer limit in bytes per
   *   second.
   */
  public long backupRateLimit() {
    assert(isOwningHandle());
    return backupRateLimit(nativeHandle_);
  }

  /**
   * Backup rate limiter. Used to control transfer speed for backup. If this is
   * not null, {@link #backupRateLimit()} is ignored.
   *
   * Default: null
   *
   * @param backupRateLimiter The rate limiter to use for the backup
   * @return instance of current BackupableDBOptions.
   */
  public BackupableDBOptions setBackupRateLimiter(final RateLimiter backupRateLimiter) {
    assert(isOwningHandle());
    setBackupRateLimiter(nativeHandle_, backupRateLimiter.nativeHandle_);
    this.backupRateLimiter = backupRateLimiter;
    return this;
  }

  /**
   * Backup rate limiter. Used to control transfer speed for backup. If this is
   * not null, {@link #backupRateLimit()} is ignored.
   *
   * Default: null
   *
   * @return The rate limiter in use for the backup
   */
  public RateLimiter backupRateLimiter() {
    assert(isOwningHandle());
    return this.backupRateLimiter;
  }

  /**
   * <p>Set restore rate limit.</p>
   *
   * @param restoreRateLimit Max bytes that can be transferred in a second
   *   during restore. If 0 or negative, then go as fast as you can.
   *
   * <p>Default: 0</p>
   *
   * @return instance of current BackupableDBOptions.
   */
  public BackupableDBOptions setRestoreRateLimit(long restoreRateLimit) {
    assert(isOwningHandle());
    restoreRateLimit = (restoreRateLimit <= 0) ? 0 : restoreRateLimit;
    setRestoreRateLimit(nativeHandle_, restoreRateLimit);
    return this;
  }

  /**
   * <p>Return restore rate limit which described the max bytes that can be
   * transferred in a second during restore.</p>
   *
   * @return numerical value describing the restore transfer limit in bytes per
   *   second.
   */
  public long restoreRateLimit() {
    assert(isOwningHandle());
    return restoreRateLimit(nativeHandle_);
  }

  /**
   * Restore rate limiter. Used to control transfer speed during restore. If
   * this is not null, {@link #restoreRateLimit()} is ignored.
   *
   * Default: null
   *
   * @param restoreRateLimiter The rate limiter to use during restore
   * @return instance of current BackupableDBOptions.
   */
  public BackupableDBOptions setRestoreRateLimiter(final RateLimiter restoreRateLimiter) {
    assert(isOwningHandle());
    setRestoreRateLimiter(nativeHandle_, restoreRateLimiter.nativeHandle_);
    this.restoreRateLimiter = restoreRateLimiter;
    return this;
  }

  /**
   * Restore rate limiter. Used to control transfer speed during restore. If
   * this is not null, {@link #restoreRateLimit()} is ignored.
   *
   * Default: null
   *
   * @return The rate limiter in use during restore
   */
  public RateLimiter restoreRateLimiter() {
    assert(isOwningHandle());
    return this.restoreRateLimiter;
  }

  /**
   * <p>Only used if share_table_files is set to true. If true, will consider
   * that backups can come from different databases, hence a sst is not uniquely
   * identified by its name, but by the triple (file name, crc32, file length)
   * </p>
   *
   * @param shareFilesWithChecksum boolean value indicating if SST files are
   *   stored using the triple (file name, crc32, file length) and not its name.
   *
   * <p>Note: this is an experimental option, and you'll need to set it manually
   * turn it on only if you know what you're doing*</p>
   *
   * <p>Default: false</p>
   *
   * @return instance of current BackupableDBOptions.
   */
  public BackupableDBOptions setShareFilesWithChecksum(
      final boolean shareFilesWithChecksum) {
    assert(isOwningHandle());
    setShareFilesWithChecksum(nativeHandle_, shareFilesWithChecksum);
    return this;
  }

  /**
   * <p>Return of share files with checksum is active.</p>
   *
   * @return boolean value indicating if share files with checksum
   *     is active.
   */
  public boolean shareFilesWithChecksum() {
    assert(isOwningHandle());
    return shareFilesWithChecksum(nativeHandle_);
  }

  /**
   * Up to this many background threads will copy files for
   * {@link BackupEngine#createNewBackup(RocksDB, boolean)} and
   * {@link BackupEngine#restoreDbFromBackup(int, String, String, RestoreOptions)}
   *
   * Default: 1
   *
   * @param maxBackgroundOperations The maximum number of background threads
   * @return instance of current BackupableDBOptions.
   */
  public BackupableDBOptions setMaxBackgroundOperations(
      final int maxBackgroundOperations) {
    assert(isOwningHandle());
    setMaxBackgroundOperations(nativeHandle_, maxBackgroundOperations);
    return this;
  }

  /**
   * Up to this many background threads will copy files for
   * {@link BackupEngine#createNewBackup(RocksDB, boolean)} and
   * {@link BackupEngine#restoreDbFromBackup(int, String, String, RestoreOptions)}
   *
   * Default: 1
   *
   * @return The maximum number of background threads
   */
  public int maxBackgroundOperations() {
    assert(isOwningHandle());
    return maxBackgroundOperations(nativeHandle_);
  }

  /**
   * During backup user can get callback every time next
   * {@link #callbackTriggerIntervalSize()} bytes being copied.
   *
   * Default: 4194304
   *
   * @param callbackTriggerIntervalSize The interval size for the
   *     callback trigger
   * @return instance of current BackupableDBOptions.
   */
  public BackupableDBOptions setCallbackTriggerIntervalSize(
      final long callbackTriggerIntervalSize) {
    assert(isOwningHandle());
    setCallbackTriggerIntervalSize(nativeHandle_, callbackTriggerIntervalSize);
    return this;
  }

  /**
   * During backup user can get callback every time next
   * {@link #callbackTriggerIntervalSize()} bytes being copied.
   *
   * Default: 4194304
   *
   * @return The interval size for the callback trigger
   */
  public long callbackTriggerIntervalSize() {
    assert(isOwningHandle());
    return callbackTriggerIntervalSize(nativeHandle_);
  }

  private native static long newBackupableDBOptions(final String path);
  private native String backupDir(long handle);
  private native void setBackupEnv(final long handle, final long envHandle);
  private native void setShareTableFiles(long handle, boolean flag);
  private native boolean shareTableFiles(long handle);
  private native void setInfoLog(final long handle, final long infoLogHandle);
  private native void setSync(long handle, boolean flag);
  private native boolean sync(long handle);
  private native void setDestroyOldData(long handle, boolean flag);
  private native boolean destroyOldData(long handle);
  private native void setBackupLogFiles(long handle, boolean flag);
  private native boolean backupLogFiles(long handle);
  private native void setBackupRateLimit(long handle, long rateLimit);
  private native long backupRateLimit(long handle);
  private native void setBackupRateLimiter(long handle, long rateLimiterHandle);
  private native void setRestoreRateLimit(long handle, long rateLimit);
  private native long restoreRateLimit(long handle);
  private native void setRestoreRateLimiter(final long handle,
      final long rateLimiterHandle);
  private native void setShareFilesWithChecksum(long handle, boolean flag);
  private native boolean shareFilesWithChecksum(long handle);
  private native void setMaxBackgroundOperations(final long handle,
      final int maxBackgroundOperations);
  private native int maxBackgroundOperations(final long handle);
  private native void setCallbackTriggerIntervalSize(final long handle,
      long callbackTriggerIntervalSize);
  private native long callbackTriggerIntervalSize(final long handle);
  @Override protected final native void disposeInternal(final long handle);
}
