// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import org.junit.ClassRule;
import org.junit.Test;

import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

public class WriteOptionsTest {

  @ClassRule
  public static final RocksMemoryResource rocksMemoryResource =
      new RocksMemoryResource();

  public static final Random rand = PlatformRandomHelper.
          getPlatformSpecificRandomFactory();

  @Test
  public void writeOptions() {
    try (final WriteOptions writeOptions = new WriteOptions()) {

      writeOptions.setSync(true);
      assertThat(writeOptions.sync()).isTrue();
      writeOptions.setSync(false);
      assertThat(writeOptions.sync()).isFalse();

      writeOptions.setDisableWAL(true);
      assertThat(writeOptions.disableWAL()).isTrue();
      writeOptions.setDisableWAL(false);
      assertThat(writeOptions.disableWAL()).isFalse();


      writeOptions.setIgnoreMissingColumnFamilies(true);
      assertThat(writeOptions.ignoreMissingColumnFamilies()).isTrue();
      writeOptions.setIgnoreMissingColumnFamilies(false);
      assertThat(writeOptions.ignoreMissingColumnFamilies()).isFalse();

      writeOptions.setNoSlowdown(true);
      assertThat(writeOptions.noSlowdown()).isTrue();
      writeOptions.setNoSlowdown(false);
      assertThat(writeOptions.noSlowdown()).isFalse();

      writeOptions.setLowPri(true);
      assertThat(writeOptions.lowPri()).isTrue();
      writeOptions.setLowPri(false);
      assertThat(writeOptions.lowPri()).isFalse();
    }
  }

  @Test
  public void copyConstructor() {
    WriteOptions origOpts = new WriteOptions();
    origOpts.setDisableWAL(rand.nextBoolean());
    origOpts.setIgnoreMissingColumnFamilies(rand.nextBoolean());
    origOpts.setSync(rand.nextBoolean());
    WriteOptions copyOpts = new WriteOptions(origOpts);
    assertThat(origOpts.disableWAL()).isEqualTo(copyOpts.disableWAL());
    assertThat(origOpts.ignoreMissingColumnFamilies()).isEqualTo(
            copyOpts.ignoreMissingColumnFamilies());
    assertThat(origOpts.sync()).isEqualTo(copyOpts.sync());
  }

}
