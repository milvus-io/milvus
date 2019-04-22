// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import org.junit.ClassRule;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class CompactionJobInfoTest {

  @ClassRule
  public static final RocksMemoryResource rocksMemoryResource =
      new RocksMemoryResource();

  @Test
  public void columnFamilyName() {
    try (final CompactionJobInfo compactionJobInfo = new CompactionJobInfo()) {
      assertThat(compactionJobInfo.columnFamilyName())
          .isEmpty();
    }
  }

  @Test
  public void status() {
    try (final CompactionJobInfo compactionJobInfo = new CompactionJobInfo()) {
      assertThat(compactionJobInfo.status().getCode())
          .isEqualTo(Status.Code.Ok);
    }
  }

  @Test
  public void threadId() {
    try (final CompactionJobInfo compactionJobInfo = new CompactionJobInfo()) {
      assertThat(compactionJobInfo.threadId())
          .isEqualTo(0);
    }
  }

  @Test
  public void jobId() {
    try (final CompactionJobInfo compactionJobInfo = new CompactionJobInfo()) {
      assertThat(compactionJobInfo.jobId())
          .isEqualTo(0);
    }
  }

  @Test
  public void baseInputLevel() {
    try (final CompactionJobInfo compactionJobInfo = new CompactionJobInfo()) {
      assertThat(compactionJobInfo.baseInputLevel())
          .isEqualTo(0);
    }
  }

  @Test
  public void outputLevel() {
    try (final CompactionJobInfo compactionJobInfo = new CompactionJobInfo()) {
      assertThat(compactionJobInfo.outputLevel())
          .isEqualTo(0);
    }
  }

  @Test
  public void inputFiles() {
    try (final CompactionJobInfo compactionJobInfo = new CompactionJobInfo()) {
      assertThat(compactionJobInfo.inputFiles())
          .isEmpty();
    }
  }

  @Test
  public void outputFiles() {
    try (final CompactionJobInfo compactionJobInfo = new CompactionJobInfo()) {
      assertThat(compactionJobInfo.outputFiles())
          .isEmpty();
    }
  }

  @Test
  public void tableProperties() {
    try (final CompactionJobInfo compactionJobInfo = new CompactionJobInfo()) {
      assertThat(compactionJobInfo.tableProperties())
          .isEmpty();
    }
  }

  @Test
  public void compactionReason() {
    try (final CompactionJobInfo compactionJobInfo = new CompactionJobInfo()) {
      assertThat(compactionJobInfo.compactionReason())
          .isEqualTo(CompactionReason.kUnknown);
    }
  }

  @Test
  public void compression() {
    try (final CompactionJobInfo compactionJobInfo = new CompactionJobInfo()) {
      assertThat(compactionJobInfo.compression())
          .isEqualTo(CompressionType.NO_COMPRESSION);
    }
  }

  @Test
  public void stats() {
    try (final CompactionJobInfo compactionJobInfo = new CompactionJobInfo()) {
      assertThat(compactionJobInfo.stats())
          .isNotNull();
    }
  }
}
