// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * The metadata that describes a SST file.
 */
public class SstFileMetaData {
  private final String fileName;
  private final String path;
  private final long size;
  private final long smallestSeqno;
  private final long largestSeqno;
  private final byte[] smallestKey;
  private final byte[] largestKey;
  private final long numReadsSampled;
  private final boolean beingCompacted;
  private final long numEntries;
  private final long numDeletions;

  /**
   * Called from JNI C++
   */
  protected SstFileMetaData(
      final String fileName,
      final String path,
      final long size,
      final long smallestSeqno,
      final long largestSeqno,
      final byte[] smallestKey,
      final byte[] largestKey,
      final long numReadsSampled,
      final boolean beingCompacted,
      final long numEntries,
      final long numDeletions) {
    this.fileName = fileName;
    this.path = path;
    this.size = size;
    this.smallestSeqno = smallestSeqno;
    this.largestSeqno = largestSeqno;
    this.smallestKey = smallestKey;
    this.largestKey = largestKey;
    this.numReadsSampled = numReadsSampled;
    this.beingCompacted = beingCompacted;
    this.numEntries = numEntries;
    this.numDeletions = numDeletions;
  }

  /**
   * Get the name of the file.
   *
   * @return the name of the file.
   */
  public String fileName() {
    return fileName;
  }

  /**
   * Get the full path where the file locates.
   *
   * @return the full path
   */
  public String path() {
    return path;
  }

  /**
   * Get the file size in bytes.
   *
   * @return file size
   */
  public long size() {
    return size;
  }

  /**
   * Get the smallest sequence number in file.
   *
   * @return the smallest sequence number
   */
  public long smallestSeqno() {
    return smallestSeqno;
  }

  /**
   * Get the largest sequence number in file.
   *
   * @return the largest sequence number
   */
  public long largestSeqno() {
    return largestSeqno;
  }

  /**
   * Get the smallest user defined key in the file.
   *
   * @return the smallest user defined key
   */
  public byte[] smallestKey() {
    return smallestKey;
  }

  /**
   * Get the largest user defined key in the file.
   *
   * @return the largest user defined key
   */
  public byte[] largestKey() {
    return largestKey;
  }

  /**
   * Get the number of times the file has been read.
   *
   * @return the number of times the file has been read
   */
  public long numReadsSampled() {
    return numReadsSampled;
  }

  /**
   * Returns true if the file is currently being compacted.
   *
   * @return true if the file is currently being compacted, false otherwise.
   */
  public boolean beingCompacted() {
    return beingCompacted;
  }

  /**
   * Get the number of entries.
   *
   * @return the number of entries.
   */
  public long numEntries() {
    return numEntries;
  }

  /**
   * Get the number of deletions.
   *
   * @return the number of deletions.
   */
  public long numDeletions() {
    return numDeletions;
  }
}
