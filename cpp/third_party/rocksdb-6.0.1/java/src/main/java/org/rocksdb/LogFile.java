// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

public class LogFile {
  private final String pathName;
  private final long logNumber;
  private final WalFileType type;
  private final long startSequence;
  private final long sizeFileBytes;

  /**
   * Called from JNI C++
   */
  private LogFile(final String pathName, final long logNumber,
      final byte walFileTypeValue, final long startSequence,
      final long sizeFileBytes) {
    this.pathName = pathName;
    this.logNumber = logNumber;
    this.type = WalFileType.fromValue(walFileTypeValue);
    this.startSequence = startSequence;
    this.sizeFileBytes = sizeFileBytes;
  }

  /**
   * Returns log file's pathname relative to the main db dir
   * Eg. For a live-log-file = /000003.log
   * For an archived-log-file = /archive/000003.log
   *
   * @return log file's pathname
   */
  public String pathName() {
    return pathName;
  }

  /**
   * Primary identifier for log file.
   * This is directly proportional to creation time of the log file
   *
   * @return the log number
   */
  public long logNumber() {
    return logNumber;
  }

  /**
   * Log file can be either alive or archived.
   *
   * @return the type of the log file.
   */
  public WalFileType type() {
    return type;
  }

  /**
   * Starting sequence number of writebatch written in this log file.
   *
   * @return the stating sequence number
   */
  public long startSequence() {
    return startSequence;
  }

  /**
   * Size of log file on disk in Bytes.
   *
   * @return size of log file
   */
  public long sizeFileBytes() {
    return sizeFileBytes;
  }
}
