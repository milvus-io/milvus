// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
package org.rocksdb;

import java.util.List;

/**
 * BackupEngine allows you to backup
 * and restore the database
 *
 * Be aware, that `new BackupEngine` takes time proportional to the amount
 * of backups. So if you have a slow filesystem to backup (like HDFS)
 * and you have a lot of backups then restoring can take some time.
 * That's why we recommend to limit the number of backups.
 * Also we recommend to keep BackupEngine alive and not to recreate it every
 * time you need to do a backup.
 */
public class BackupEngine extends RocksObject implements AutoCloseable {

  protected BackupEngine(final long nativeHandle) {
    super(nativeHandle);
  }

  /**
   * Opens a new Backup Engine
   *
   * @param env The environment that the backup engine should operate within
   * @param options Any options for the backup engine
   *
   * @return A new BackupEngine instance
   * @throws RocksDBException thrown if the backup engine could not be opened
   */
  public static BackupEngine open(final Env env,
      final BackupableDBOptions options) throws RocksDBException {
    return new BackupEngine(open(env.nativeHandle_, options.nativeHandle_));
  }

  /**
   * Captures the state of the database in the latest backup
   *
   * Just a convenience for {@link #createNewBackup(RocksDB, boolean)} with
   * the flushBeforeBackup parameter set to false
   *
   * @param db The database to backup
   *
   * Note - This method is not thread safe
   *
   * @throws RocksDBException thrown if a new backup could not be created
   */
  public void createNewBackup(final RocksDB db) throws RocksDBException {
    createNewBackup(db, false);
  }

  /**
   * Captures the state of the database in the latest backup
   *
   * @param db The database to backup
   * @param flushBeforeBackup When true, the Backup Engine will first issue a
   *                          memtable flush and only then copy the DB files to
   *                          the backup directory. Doing so will prevent log
   *                          files from being copied to the backup directory
   *                          (since flush will delete them).
   *                          When false, the Backup Engine will not issue a
   *                          flush before starting the backup. In that case,
   *                          the backup will also include log files
   *                          corresponding to live memtables. The backup will
   *                          always be consistent with the current state of the
   *                          database regardless of the flushBeforeBackup
   *                          parameter.
   *
   * Note - This method is not thread safe
   *
   * @throws RocksDBException thrown if a new backup could not be created
   */
  public void createNewBackup(
      final RocksDB db, final boolean flushBeforeBackup)
      throws RocksDBException {
    assert (isOwningHandle());
    createNewBackup(nativeHandle_, db.nativeHandle_, flushBeforeBackup);
  }

  /**
   * Captures the state of the database in the latest backup along with
   * application specific metadata.
   *
   * @param db The database to backup
   * @param metadata Application metadata
   * @param flushBeforeBackup When true, the Backup Engine will first issue a
   *                          memtable flush and only then copy the DB files to
   *                          the backup directory. Doing so will prevent log
   *                          files from being copied to the backup directory
   *                          (since flush will delete them).
   *                          When false, the Backup Engine will not issue a
   *                          flush before starting the backup. In that case,
   *                          the backup will also include log files
   *                          corresponding to live memtables. The backup will
   *                          always be consistent with the current state of the
   *                          database regardless of the flushBeforeBackup
   *                          parameter.
   *
   * Note - This method is not thread safe
   *
   * @throws RocksDBException thrown if a new backup could not be created
   */
  public void createNewBackupWithMetadata(final RocksDB db, final String metadata,
      final boolean flushBeforeBackup) throws RocksDBException {
    assert (isOwningHandle());
    createNewBackupWithMetadata(nativeHandle_, db.nativeHandle_, metadata, flushBeforeBackup);
  }

  /**
   * Gets information about the available
   * backups
   *
   * @return A list of information about each available backup
   */
  public List<BackupInfo> getBackupInfo() {
    assert (isOwningHandle());
    return getBackupInfo(nativeHandle_);
  }

  /**
   * <p>Returns a list of corrupted backup ids. If there
   * is no corrupted backup the method will return an
   * empty list.</p>
   *
   * @return array of backup ids as int ids.
   */
  public int[] getCorruptedBackups() {
    assert(isOwningHandle());
    return getCorruptedBackups(nativeHandle_);
  }

  /**
   * <p>Will delete all the files we don't need anymore. It will
   * do the full scan of the files/ directory and delete all the
   * files that are not referenced.</p>
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void garbageCollect() throws RocksDBException {
    assert(isOwningHandle());
    garbageCollect(nativeHandle_);
  }

  /**
   * Deletes old backups, keeping just the latest numBackupsToKeep
   *
   * @param numBackupsToKeep The latest n backups to keep
   *
   * @throws RocksDBException thrown if the old backups could not be deleted
   */
  public void purgeOldBackups(
      final int numBackupsToKeep) throws RocksDBException {
    assert (isOwningHandle());
    purgeOldBackups(nativeHandle_, numBackupsToKeep);
  }

  /**
   * Deletes a backup
   *
   * @param backupId The id of the backup to delete
   *
   * @throws RocksDBException thrown if the backup could not be deleted
   */
  public void deleteBackup(final int backupId) throws RocksDBException {
    assert (isOwningHandle());
    deleteBackup(nativeHandle_, backupId);
  }

  /**
   * Restore the database from a backup
   *
   * IMPORTANT: if options.share_table_files == true and you restore the DB
   * from some backup that is not the latest, and you start creating new
   * backups from the new DB, they will probably fail!
   *
   * Example: Let's say you have backups 1, 2, 3, 4, 5 and you restore 3.
   * If you add new data to the DB and try creating a new backup now, the
   * database will diverge from backups 4 and 5 and the new backup will fail.
   * If you want to create new backup, you will first have to delete backups 4
   * and 5.
   *
   * @param backupId The id of the backup to restore
   * @param dbDir The directory to restore the backup to, i.e. where your
   *              database is
   * @param walDir The location of the log files for your database,
   *               often the same as dbDir
   * @param restoreOptions Options for controlling the restore
   *
   * @throws RocksDBException thrown if the database could not be restored
   */
  public void restoreDbFromBackup(
      final int backupId, final String dbDir, final String walDir,
      final RestoreOptions restoreOptions) throws RocksDBException {
    assert (isOwningHandle());
    restoreDbFromBackup(nativeHandle_, backupId, dbDir, walDir,
        restoreOptions.nativeHandle_);
  }

  /**
   * Restore the database from the latest backup
   *
   * @param dbDir The directory to restore the backup to, i.e. where your
   *              database is
   * @param walDir The location of the log files for your database, often the
   *               same as dbDir
   * @param restoreOptions Options for controlling the restore
   *
   * @throws RocksDBException thrown if the database could not be restored
   */
  public void restoreDbFromLatestBackup(
      final String dbDir, final String walDir,
      final RestoreOptions restoreOptions) throws RocksDBException {
    assert (isOwningHandle());
    restoreDbFromLatestBackup(nativeHandle_, dbDir, walDir,
        restoreOptions.nativeHandle_);
  }

  private native static long open(final long env,
      final long backupableDbOptions) throws RocksDBException;

  private native void createNewBackup(final long handle, final long dbHandle,
      final boolean flushBeforeBackup) throws RocksDBException;

  private native void createNewBackupWithMetadata(final long handle, final long dbHandle,
      final String metadata, final boolean flushBeforeBackup) throws RocksDBException;

  private native List<BackupInfo> getBackupInfo(final long handle);

  private native int[] getCorruptedBackups(final long handle);

  private native void garbageCollect(final long handle) throws RocksDBException;

  private native void purgeOldBackups(final long handle,
      final int numBackupsToKeep) throws RocksDBException;

  private native void deleteBackup(final long handle, final int backupId)
      throws RocksDBException;

  private native void restoreDbFromBackup(final long handle, final int backupId,
      final String dbDir, final String walDir, final long restoreOptionsHandle)
      throws RocksDBException;

  private native void restoreDbFromLatestBackup(final long handle,
      final String dbDir, final String walDir, final long restoreOptionsHandle)
      throws RocksDBException;

  @Override protected final native void disposeInternal(final long handle);
}
