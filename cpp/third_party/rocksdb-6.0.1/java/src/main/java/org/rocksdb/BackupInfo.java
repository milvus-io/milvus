// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
package org.rocksdb;

/**
 * Instances of this class describe a Backup made by
 * {@link org.rocksdb.BackupEngine}.
 */
public class BackupInfo {

  /**
   * Package private constructor used to create instances
   * of BackupInfo by {@link org.rocksdb.BackupEngine}
   *
   * @param backupId id of backup
   * @param timestamp timestamp of backup
   * @param size size of backup
   * @param numberFiles number of files related to this backup.
   */
  BackupInfo(final int backupId, final long timestamp, final long size, final int numberFiles,
      final String app_metadata) {
    backupId_ = backupId;
    timestamp_ = timestamp;
    size_ = size;
    numberFiles_ = numberFiles;
    app_metadata_ = app_metadata;
  }

  /**
   *
   * @return the backup id.
   */
  public int backupId() {
    return backupId_;
  }

  /**
   *
   * @return the timestamp of the backup.
   */
  public long timestamp() {
    return timestamp_;
  }

  /**
   *
   * @return the size of the backup
   */
  public long size() {
    return size_;
  }

  /**
   *
   * @return the number of files of this backup.
   */
  public int numberFiles() {
    return numberFiles_;
  }

  /**
   *
   * @return the associated application metadata, or null
   */
  public String appMetadata() {
    return app_metadata_;
  }

  private int backupId_;
  private long timestamp_;
  private long size_;
  private int numberFiles_;
  private String app_metadata_;
}
