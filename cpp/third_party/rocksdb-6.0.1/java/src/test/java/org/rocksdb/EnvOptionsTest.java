// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import org.junit.ClassRule;
import org.junit.Test;

import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

public class EnvOptionsTest {
  @ClassRule
  public static final RocksMemoryResource rocksMemoryResource = new RocksMemoryResource();

  public static final Random rand = PlatformRandomHelper.getPlatformSpecificRandomFactory();

  @Test
  public void dbOptionsConstructor() {
    final long compactionReadaheadSize = 4 * 1024 * 1024;
    try (final DBOptions dbOptions = new DBOptions()
        .setCompactionReadaheadSize(compactionReadaheadSize)) {
      try (final EnvOptions envOptions = new EnvOptions(dbOptions)) {
        assertThat(envOptions.compactionReadaheadSize())
            .isEqualTo(compactionReadaheadSize);
      }
    }
  }

  @Test
  public void useMmapReads() {
    try (final EnvOptions envOptions = new EnvOptions()) {
      final boolean boolValue = rand.nextBoolean();
      envOptions.setUseMmapReads(boolValue);
      assertThat(envOptions.useMmapReads()).isEqualTo(boolValue);
    }
  }

  @Test
  public void useMmapWrites() {
    try (final EnvOptions envOptions = new EnvOptions()) {
      final boolean boolValue = rand.nextBoolean();
      envOptions.setUseMmapWrites(boolValue);
      assertThat(envOptions.useMmapWrites()).isEqualTo(boolValue);
    }
  }

  @Test
  public void useDirectReads() {
    try (final EnvOptions envOptions = new EnvOptions()) {
      final boolean boolValue = rand.nextBoolean();
      envOptions.setUseDirectReads(boolValue);
      assertThat(envOptions.useDirectReads()).isEqualTo(boolValue);
    }
  }

  @Test
  public void useDirectWrites() {
    try (final EnvOptions envOptions = new EnvOptions()) {
      final boolean boolValue = rand.nextBoolean();
      envOptions.setUseDirectWrites(boolValue);
      assertThat(envOptions.useDirectWrites()).isEqualTo(boolValue);
    }
  }

  @Test
  public void allowFallocate() {
    try (final EnvOptions envOptions = new EnvOptions()) {
      final boolean boolValue = rand.nextBoolean();
      envOptions.setAllowFallocate(boolValue);
      assertThat(envOptions.allowFallocate()).isEqualTo(boolValue);
    }
  }

  @Test
  public void setFdCloexecs() {
    try (final EnvOptions envOptions = new EnvOptions()) {
      final boolean boolValue = rand.nextBoolean();
      envOptions.setSetFdCloexec(boolValue);
      assertThat(envOptions.setFdCloexec()).isEqualTo(boolValue);
    }
  }

  @Test
  public void bytesPerSync() {
    try (final EnvOptions envOptions = new EnvOptions()) {
      final long longValue = rand.nextLong();
      envOptions.setBytesPerSync(longValue);
      assertThat(envOptions.bytesPerSync()).isEqualTo(longValue);
    }
  }

  @Test
  public void fallocateWithKeepSize() {
    try (final EnvOptions envOptions = new EnvOptions()) {
      final boolean boolValue = rand.nextBoolean();
      envOptions.setFallocateWithKeepSize(boolValue);
      assertThat(envOptions.fallocateWithKeepSize()).isEqualTo(boolValue);
    }
  }

  @Test
  public void compactionReadaheadSize() {
    try (final EnvOptions envOptions = new EnvOptions()) {
      final int intValue = rand.nextInt();
      envOptions.setCompactionReadaheadSize(intValue);
      assertThat(envOptions.compactionReadaheadSize()).isEqualTo(intValue);
    }
  }

  @Test
  public void randomAccessMaxBufferSize() {
    try (final EnvOptions envOptions = new EnvOptions()) {
      final int intValue = rand.nextInt();
      envOptions.setRandomAccessMaxBufferSize(intValue);
      assertThat(envOptions.randomAccessMaxBufferSize()).isEqualTo(intValue);
    }
  }

  @Test
  public void writableFileMaxBufferSize() {
    try (final EnvOptions envOptions = new EnvOptions()) {
      final int intValue = rand.nextInt();
      envOptions.setWritableFileMaxBufferSize(intValue);
      assertThat(envOptions.writableFileMaxBufferSize()).isEqualTo(intValue);
    }
  }

  @Test
  public void rateLimiter() {
    try (final EnvOptions envOptions = new EnvOptions();
      final RateLimiter rateLimiter1 = new RateLimiter(1000, 100 * 1000, 1)) {
      envOptions.setRateLimiter(rateLimiter1);
      assertThat(envOptions.rateLimiter()).isEqualTo(rateLimiter1);

      try(final RateLimiter rateLimiter2 = new RateLimiter(1000)) {
        envOptions.setRateLimiter(rateLimiter2);
        assertThat(envOptions.rateLimiter()).isEqualTo(rateLimiter2);
      }
    }
  }
}
