// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import org.junit.Test;
import org.rocksdb.CompactRangeOptions.BottommostLevelCompaction;

import static org.assertj.core.api.Assertions.assertThat;

public class CompactRangeOptionsTest {

  static {
    RocksDB.loadLibrary();
  }

  @Test
  public void exclusiveManualCompaction() {
    CompactRangeOptions opt = new CompactRangeOptions();
    boolean value = false;
    opt.setExclusiveManualCompaction(value);
    assertThat(opt.exclusiveManualCompaction()).isEqualTo(value);
    value = true;
    opt.setExclusiveManualCompaction(value);
    assertThat(opt.exclusiveManualCompaction()).isEqualTo(value);
  }

  @Test
  public void bottommostLevelCompaction() {
    CompactRangeOptions opt = new CompactRangeOptions();
    BottommostLevelCompaction value = BottommostLevelCompaction.kSkip;
    opt.setBottommostLevelCompaction(value);
    assertThat(opt.bottommostLevelCompaction()).isEqualTo(value);
    value = BottommostLevelCompaction.kForce;
    opt.setBottommostLevelCompaction(value);
    assertThat(opt.bottommostLevelCompaction()).isEqualTo(value);
    value = BottommostLevelCompaction.kIfHaveCompactionFilter;
    opt.setBottommostLevelCompaction(value);
    assertThat(opt.bottommostLevelCompaction()).isEqualTo(value);
  }

  @Test
  public void changeLevel() {
    CompactRangeOptions opt = new CompactRangeOptions();
    boolean value = false;
    opt.setChangeLevel(value);
    assertThat(opt.changeLevel()).isEqualTo(value);
    value = true;
    opt.setChangeLevel(value);
    assertThat(opt.changeLevel()).isEqualTo(value);
  }

  @Test
  public void targetLevel() {
    CompactRangeOptions opt = new CompactRangeOptions();
    int value = 2;
    opt.setTargetLevel(value);
    assertThat(opt.targetLevel()).isEqualTo(value);
    value = 3;
    opt.setTargetLevel(value);
    assertThat(opt.targetLevel()).isEqualTo(value);
  }

  @Test
  public void targetPathId() {
    CompactRangeOptions opt = new CompactRangeOptions();
    int value = 2;
    opt.setTargetPathId(value);
    assertThat(opt.targetPathId()).isEqualTo(value);
    value = 3;
    opt.setTargetPathId(value);
    assertThat(opt.targetPathId()).isEqualTo(value);
  }

  @Test
  public void allowWriteStall() {
    CompactRangeOptions opt = new CompactRangeOptions();
    boolean value = false;
    opt.setAllowWriteStall(value);
    assertThat(opt.allowWriteStall()).isEqualTo(value);
    value = true;
    opt.setAllowWriteStall(value);
    assertThat(opt.allowWriteStall()).isEqualTo(value);
  }

  @Test
  public void maxSubcompactions() {
    CompactRangeOptions opt = new CompactRangeOptions();
    int value = 2;
    opt.setMaxSubcompactions(value);
    assertThat(opt.maxSubcompactions()).isEqualTo(value);
    value = 3;
    opt.setMaxSubcompactions(value);
    assertThat(opt.maxSubcompactions()).isEqualTo(value);
  }
}
