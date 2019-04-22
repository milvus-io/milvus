// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class MutableDBOptions extends AbstractMutableOptions {

  /**
   * User must use builder pattern, or parser.
   *
   * @param keys the keys
   * @param values the values
   *
   * See {@link #builder()} and {@link #parse(String)}.
   */
  private MutableDBOptions(final String[] keys, final String[] values) {
    super(keys, values);
  }

  /**
   * Creates a builder which allows you
   * to set MutableDBOptions in a fluent
   * manner
   *
   * @return A builder for MutableDBOptions
   */
  public static MutableDBOptionsBuilder builder() {
    return new MutableDBOptionsBuilder();
  }

  /**
   * Parses a String representation of MutableDBOptions
   *
   * The format is: key1=value1;key2=value2;key3=value3 etc
   *
   * For int[] values, each int should be separated by a comma, e.g.
   *
   * key1=value1;intArrayKey1=1,2,3
   *
   * @param str The string representation of the mutable db options
   *
   * @return A builder for the mutable db options
   */
  public static MutableDBOptionsBuilder parse(final String str) {
    Objects.requireNonNull(str);

    final MutableDBOptionsBuilder builder =
        new MutableDBOptionsBuilder();

    final String[] options = str.trim().split(KEY_VALUE_PAIR_SEPARATOR);
    for(final String option : options) {
      final int equalsOffset = option.indexOf(KEY_VALUE_SEPARATOR);
      if(equalsOffset <= 0) {
        throw new IllegalArgumentException(
            "options string has an invalid key=value pair");
      }

      final String key = option.substring(0, equalsOffset);
      if(key.isEmpty()) {
        throw new IllegalArgumentException("options string is invalid");
      }

      final String value = option.substring(equalsOffset + 1);
      if(value.isEmpty()) {
        throw new IllegalArgumentException("options string is invalid");
      }

      builder.fromString(key, value);
    }

    return builder;
  }

  private interface MutableDBOptionKey extends MutableOptionKey {}

  public enum DBOption implements MutableDBOptionKey {
    max_background_jobs(ValueType.INT),
    base_background_compactions(ValueType.INT),
    max_background_compactions(ValueType.INT),
    avoid_flush_during_shutdown(ValueType.BOOLEAN),
    writable_file_max_buffer_size(ValueType.LONG),
    delayed_write_rate(ValueType.LONG),
    max_total_wal_size(ValueType.LONG),
    delete_obsolete_files_period_micros(ValueType.LONG),
    stats_dump_period_sec(ValueType.INT),
    max_open_files(ValueType.INT),
    bytes_per_sync(ValueType.LONG),
    wal_bytes_per_sync(ValueType.LONG),
    compaction_readahead_size(ValueType.LONG);

    private final ValueType valueType;
    DBOption(final ValueType valueType) {
      this.valueType = valueType;
    }

    @Override
    public ValueType getValueType() {
      return valueType;
    }
  }

  public static class MutableDBOptionsBuilder
      extends AbstractMutableOptionsBuilder<MutableDBOptions, MutableDBOptionsBuilder, MutableDBOptionKey>
      implements MutableDBOptionsInterface<MutableDBOptionsBuilder> {

    private final static Map<String, MutableDBOptionKey> ALL_KEYS_LOOKUP = new HashMap<>();
    static {
      for(final MutableDBOptionKey key : DBOption.values()) {
        ALL_KEYS_LOOKUP.put(key.name(), key);
      }
    }

    private MutableDBOptionsBuilder() {
      super();
    }

    @Override
    protected MutableDBOptionsBuilder self() {
      return this;
    }

    @Override
    protected Map<String, MutableDBOptionKey> allKeys() {
      return ALL_KEYS_LOOKUP;
    }

    @Override
    protected MutableDBOptions build(final String[] keys,
        final String[] values) {
      return new MutableDBOptions(keys, values);
    }

    @Override
    public MutableDBOptionsBuilder setMaxBackgroundJobs(
        final int maxBackgroundJobs) {
      return setInt(DBOption.max_background_jobs, maxBackgroundJobs);
    }

    @Override
    public int maxBackgroundJobs() {
      return getInt(DBOption.max_background_jobs);
    }

    @Override
    public void setBaseBackgroundCompactions(
        final int baseBackgroundCompactions) {
      setInt(DBOption.base_background_compactions,
          baseBackgroundCompactions);
    }

    @Override
    public int baseBackgroundCompactions() {
      return getInt(DBOption.base_background_compactions);
    }

    @Override
    public MutableDBOptionsBuilder setMaxBackgroundCompactions(
        final int maxBackgroundCompactions) {
      return setInt(DBOption.max_background_compactions,
          maxBackgroundCompactions);
    }

    @Override
    public int maxBackgroundCompactions() {
      return getInt(DBOption.max_background_compactions);
    }

    @Override
    public MutableDBOptionsBuilder setAvoidFlushDuringShutdown(
        final boolean avoidFlushDuringShutdown) {
      return setBoolean(DBOption.avoid_flush_during_shutdown,
          avoidFlushDuringShutdown);
    }

    @Override
    public boolean avoidFlushDuringShutdown() {
      return getBoolean(DBOption.avoid_flush_during_shutdown);
    }

    @Override
    public MutableDBOptionsBuilder setWritableFileMaxBufferSize(
        final long writableFileMaxBufferSize) {
      return setLong(DBOption.writable_file_max_buffer_size,
          writableFileMaxBufferSize);
    }

    @Override
    public long writableFileMaxBufferSize() {
      return getLong(DBOption.writable_file_max_buffer_size);
    }

    @Override
    public MutableDBOptionsBuilder setDelayedWriteRate(
        final long delayedWriteRate) {
      return setLong(DBOption.delayed_write_rate,
          delayedWriteRate);
    }

    @Override
    public long delayedWriteRate() {
      return getLong(DBOption.delayed_write_rate);
    }

    @Override
    public MutableDBOptionsBuilder setMaxTotalWalSize(
        final long maxTotalWalSize) {
      return setLong(DBOption.max_total_wal_size, maxTotalWalSize);
    }

    @Override
    public long maxTotalWalSize() {
      return getLong(DBOption.max_total_wal_size);
    }

    @Override
    public MutableDBOptionsBuilder setDeleteObsoleteFilesPeriodMicros(
        final long micros) {
      return setLong(DBOption.delete_obsolete_files_period_micros, micros);
    }

    @Override
    public long deleteObsoleteFilesPeriodMicros() {
      return getLong(DBOption.delete_obsolete_files_period_micros);
    }

    @Override
    public MutableDBOptionsBuilder setStatsDumpPeriodSec(
        final int statsDumpPeriodSec) {
      return setInt(DBOption.stats_dump_period_sec, statsDumpPeriodSec);
    }

    @Override
    public int statsDumpPeriodSec() {
      return getInt(DBOption.stats_dump_period_sec);
    }

    @Override
    public MutableDBOptionsBuilder setMaxOpenFiles(final int maxOpenFiles) {
      return setInt(DBOption.max_open_files, maxOpenFiles);
    }

    @Override
    public int maxOpenFiles() {
      return getInt(DBOption.max_open_files);
    }

    @Override
    public MutableDBOptionsBuilder setBytesPerSync(final long bytesPerSync) {
      return setLong(DBOption.bytes_per_sync, bytesPerSync);
    }

    @Override
    public long bytesPerSync() {
      return getLong(DBOption.bytes_per_sync);
    }

    @Override
    public MutableDBOptionsBuilder setWalBytesPerSync(
        final long walBytesPerSync) {
      return setLong(DBOption.wal_bytes_per_sync, walBytesPerSync);
    }

    @Override
    public long walBytesPerSync() {
      return getLong(DBOption.wal_bytes_per_sync);
    }

    @Override
    public MutableDBOptionsBuilder setCompactionReadaheadSize(
        final long compactionReadaheadSize) {
      return setLong(DBOption.compaction_readahead_size,
          compactionReadaheadSize);
    }

    @Override
    public long compactionReadaheadSize() {
      return getLong(DBOption.compaction_readahead_size);
    }
  }
}
