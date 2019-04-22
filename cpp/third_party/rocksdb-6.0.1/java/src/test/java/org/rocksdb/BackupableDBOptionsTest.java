// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Random;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class BackupableDBOptionsTest {

  private final static String ARBITRARY_PATH =
      System.getProperty("java.io.tmpdir");

  @ClassRule
  public static final RocksMemoryResource rocksMemoryResource =
      new RocksMemoryResource();

  @Rule
  public ExpectedException exception = ExpectedException.none();

  public static final Random rand = PlatformRandomHelper.
      getPlatformSpecificRandomFactory();

  @Test
  public void backupDir() {
    try (final BackupableDBOptions backupableDBOptions =
             new BackupableDBOptions(ARBITRARY_PATH)) {
      assertThat(backupableDBOptions.backupDir()).
          isEqualTo(ARBITRARY_PATH);
    }
  }

  @Test
  public void env() {
    try (final BackupableDBOptions backupableDBOptions =
             new BackupableDBOptions(ARBITRARY_PATH)) {
      assertThat(backupableDBOptions.backupEnv()).
          isNull();

      try(final Env env = new RocksMemEnv(Env.getDefault())) {
        backupableDBOptions.setBackupEnv(env);
        assertThat(backupableDBOptions.backupEnv())
            .isEqualTo(env);
      }
    }
  }

  @Test
  public void shareTableFiles() {
    try (final BackupableDBOptions backupableDBOptions =
             new BackupableDBOptions(ARBITRARY_PATH)) {
      final boolean value = rand.nextBoolean();
      backupableDBOptions.setShareTableFiles(value);
      assertThat(backupableDBOptions.shareTableFiles()).
          isEqualTo(value);
    }
  }

  @Test
  public void infoLog() {
    try (final BackupableDBOptions backupableDBOptions =
             new BackupableDBOptions(ARBITRARY_PATH)) {
      assertThat(backupableDBOptions.infoLog()).
          isNull();

      try(final Options options = new Options();
          final Logger logger = new Logger(options){
            @Override
            protected void log(InfoLogLevel infoLogLevel, String logMsg) {

            }
          }) {
        backupableDBOptions.setInfoLog(logger);
        assertThat(backupableDBOptions.infoLog())
            .isEqualTo(logger);
      }
    }
  }

  @Test
  public void sync() {
    try (final BackupableDBOptions backupableDBOptions =
             new BackupableDBOptions(ARBITRARY_PATH)) {
      final boolean value = rand.nextBoolean();
      backupableDBOptions.setSync(value);
      assertThat(backupableDBOptions.sync()).isEqualTo(value);
    }
  }

  @Test
  public void destroyOldData() {
    try (final BackupableDBOptions backupableDBOptions =
             new BackupableDBOptions(ARBITRARY_PATH);) {
      final boolean value = rand.nextBoolean();
      backupableDBOptions.setDestroyOldData(value);
      assertThat(backupableDBOptions.destroyOldData()).
          isEqualTo(value);
    }
  }

  @Test
  public void backupLogFiles() {
    try (final BackupableDBOptions backupableDBOptions =
             new BackupableDBOptions(ARBITRARY_PATH)) {
      final boolean value = rand.nextBoolean();
      backupableDBOptions.setBackupLogFiles(value);
      assertThat(backupableDBOptions.backupLogFiles()).
          isEqualTo(value);
    }
  }

  @Test
  public void backupRateLimit() {
    try (final BackupableDBOptions backupableDBOptions =
             new BackupableDBOptions(ARBITRARY_PATH)) {
      final long value = Math.abs(rand.nextLong());
      backupableDBOptions.setBackupRateLimit(value);
      assertThat(backupableDBOptions.backupRateLimit()).
          isEqualTo(value);
      // negative will be mapped to 0
      backupableDBOptions.setBackupRateLimit(-1);
      assertThat(backupableDBOptions.backupRateLimit()).
          isEqualTo(0);
    }
  }

  @Test
  public void backupRateLimiter() {
    try (final BackupableDBOptions backupableDBOptions =
             new BackupableDBOptions(ARBITRARY_PATH)) {
      assertThat(backupableDBOptions.backupEnv()).
          isNull();

      try(final RateLimiter backupRateLimiter =
              new RateLimiter(999)) {
        backupableDBOptions.setBackupRateLimiter(backupRateLimiter);
        assertThat(backupableDBOptions.backupRateLimiter())
            .isEqualTo(backupRateLimiter);
      }
    }
  }

  @Test
  public void restoreRateLimit() {
    try (final BackupableDBOptions backupableDBOptions =
             new BackupableDBOptions(ARBITRARY_PATH)) {
      final long value = Math.abs(rand.nextLong());
      backupableDBOptions.setRestoreRateLimit(value);
      assertThat(backupableDBOptions.restoreRateLimit()).
          isEqualTo(value);
      // negative will be mapped to 0
      backupableDBOptions.setRestoreRateLimit(-1);
      assertThat(backupableDBOptions.restoreRateLimit()).
          isEqualTo(0);
    }
  }

  @Test
  public void restoreRateLimiter() {
    try (final BackupableDBOptions backupableDBOptions =
             new BackupableDBOptions(ARBITRARY_PATH)) {
      assertThat(backupableDBOptions.backupEnv()).
          isNull();

      try(final RateLimiter restoreRateLimiter =
              new RateLimiter(911)) {
        backupableDBOptions.setRestoreRateLimiter(restoreRateLimiter);
        assertThat(backupableDBOptions.restoreRateLimiter())
            .isEqualTo(restoreRateLimiter);
      }
    }
  }

  @Test
  public void shareFilesWithChecksum() {
    try (final BackupableDBOptions backupableDBOptions =
             new BackupableDBOptions(ARBITRARY_PATH)) {
      boolean value = rand.nextBoolean();
      backupableDBOptions.setShareFilesWithChecksum(value);
      assertThat(backupableDBOptions.shareFilesWithChecksum()).
          isEqualTo(value);
    }
  }

  @Test
  public void maxBackgroundOperations() {
    try (final BackupableDBOptions backupableDBOptions =
             new BackupableDBOptions(ARBITRARY_PATH)) {
      final int value = rand.nextInt();
      backupableDBOptions.setMaxBackgroundOperations(value);
      assertThat(backupableDBOptions.maxBackgroundOperations()).
          isEqualTo(value);
    }
  }

  @Test
  public void callbackTriggerIntervalSize() {
    try (final BackupableDBOptions backupableDBOptions =
             new BackupableDBOptions(ARBITRARY_PATH)) {
      final long value = rand.nextLong();
      backupableDBOptions.setCallbackTriggerIntervalSize(value);
      assertThat(backupableDBOptions.callbackTriggerIntervalSize()).
          isEqualTo(value);
    }
  }

  @Test
  public void failBackupDirIsNull() {
    exception.expect(IllegalArgumentException.class);
    try (final BackupableDBOptions opts = new BackupableDBOptions(null)) {
      //no-op
    }
  }

  @Test
  public void failBackupDirIfDisposed() {
    try (final BackupableDBOptions options =
             setupUninitializedBackupableDBOptions(exception)) {
      options.backupDir();
    }
  }

  @Test
  public void failSetShareTableFilesIfDisposed() {
    try (final BackupableDBOptions options =
             setupUninitializedBackupableDBOptions(exception)) {
      options.setShareTableFiles(true);
    }
  }

  @Test
  public void failShareTableFilesIfDisposed() {
    try (BackupableDBOptions options =
             setupUninitializedBackupableDBOptions(exception)) {
      options.shareTableFiles();
    }
  }

  @Test
  public void failSetSyncIfDisposed() {
    try (final BackupableDBOptions options =
             setupUninitializedBackupableDBOptions(exception)) {
      options.setSync(true);
    }
  }

  @Test
  public void failSyncIfDisposed() {
    try (final BackupableDBOptions options =
             setupUninitializedBackupableDBOptions(exception)) {
      options.sync();
    }
  }

  @Test
  public void failSetDestroyOldDataIfDisposed() {
    try (final BackupableDBOptions options =
             setupUninitializedBackupableDBOptions(exception)) {
      options.setDestroyOldData(true);
    }
  }

  @Test
  public void failDestroyOldDataIfDisposed() {
    try (final BackupableDBOptions options =
             setupUninitializedBackupableDBOptions(exception)) {
      options.destroyOldData();
    }
  }

  @Test
  public void failSetBackupLogFilesIfDisposed() {
    try (final BackupableDBOptions options =
             setupUninitializedBackupableDBOptions(exception)) {
      options.setBackupLogFiles(true);
    }
  }

  @Test
  public void failBackupLogFilesIfDisposed() {
    try (final BackupableDBOptions options =
             setupUninitializedBackupableDBOptions(exception)) {
      options.backupLogFiles();
    }
  }

  @Test
  public void failSetBackupRateLimitIfDisposed() {
    try (final BackupableDBOptions options =
             setupUninitializedBackupableDBOptions(exception)) {
      options.setBackupRateLimit(1);
    }
  }

  @Test
  public void failBackupRateLimitIfDisposed() {
    try (final BackupableDBOptions options =
             setupUninitializedBackupableDBOptions(exception)) {
      options.backupRateLimit();
    }
  }

  @Test
  public void failSetRestoreRateLimitIfDisposed() {
    try (final BackupableDBOptions options =
             setupUninitializedBackupableDBOptions(exception)) {
      options.setRestoreRateLimit(1);
    }
  }

  @Test
  public void failRestoreRateLimitIfDisposed() {
    try (final BackupableDBOptions options =
             setupUninitializedBackupableDBOptions(exception)) {
      options.restoreRateLimit();
    }
  }

  @Test
  public void failSetShareFilesWithChecksumIfDisposed() {
    try (final BackupableDBOptions options =
             setupUninitializedBackupableDBOptions(exception)) {
      options.setShareFilesWithChecksum(true);
    }
  }

  @Test
  public void failShareFilesWithChecksumIfDisposed() {
    try (final BackupableDBOptions options =
             setupUninitializedBackupableDBOptions(exception)) {
      options.shareFilesWithChecksum();
    }
  }

  private BackupableDBOptions setupUninitializedBackupableDBOptions(
      ExpectedException exception) {
    final BackupableDBOptions backupableDBOptions =
        new BackupableDBOptions(ARBITRARY_PATH);
    backupableDBOptions.close();
    exception.expect(AssertionError.class);
    return backupableDBOptions;
  }
}
