// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.util.*;

public class MutableColumnFamilyOptions
    extends AbstractMutableOptions {

  /**
   * User must use builder pattern, or parser.
   *
   * @param keys the keys
   * @param values the values
   *
   * See {@link #builder()} and {@link #parse(String)}.
   */
  private MutableColumnFamilyOptions(final String[] keys,
      final String[] values) {
    super(keys, values);
  }

  /**
   * Creates a builder which allows you
   * to set MutableColumnFamilyOptions in a fluent
   * manner
   *
   * @return A builder for MutableColumnFamilyOptions
   */
  public static MutableColumnFamilyOptionsBuilder builder() {
    return new MutableColumnFamilyOptionsBuilder();
  }

  /**
   * Parses a String representation of MutableColumnFamilyOptions
   *
   * The format is: key1=value1;key2=value2;key3=value3 etc
   *
   * For int[] values, each int should be separated by a comma, e.g.
   *
   * key1=value1;intArrayKey1=1,2,3
   *
   * @param str The string representation of the mutable column family options
   *
   * @return A builder for the mutable column family options
   */
  public static MutableColumnFamilyOptionsBuilder parse(final String str) {
    Objects.requireNonNull(str);

    final MutableColumnFamilyOptionsBuilder builder =
        new MutableColumnFamilyOptionsBuilder();

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

  private interface MutableColumnFamilyOptionKey extends MutableOptionKey {}

  public enum MemtableOption implements MutableColumnFamilyOptionKey {
    write_buffer_size(ValueType.LONG),
    arena_block_size(ValueType.LONG),
    memtable_prefix_bloom_size_ratio(ValueType.DOUBLE),
    @Deprecated memtable_prefix_bloom_bits(ValueType.INT),
    @Deprecated memtable_prefix_bloom_probes(ValueType.INT),
    memtable_huge_page_size(ValueType.LONG),
    max_successive_merges(ValueType.LONG),
    @Deprecated filter_deletes(ValueType.BOOLEAN),
    max_write_buffer_number(ValueType.INT),
    inplace_update_num_locks(ValueType.LONG);

    private final ValueType valueType;
    MemtableOption(final ValueType valueType) {
      this.valueType = valueType;
    }

    @Override
    public ValueType getValueType() {
      return valueType;
    }
  }

  public enum CompactionOption implements MutableColumnFamilyOptionKey {
    disable_auto_compactions(ValueType.BOOLEAN),
    @Deprecated soft_rate_limit(ValueType.DOUBLE),
    soft_pending_compaction_bytes_limit(ValueType.LONG),
    @Deprecated hard_rate_limit(ValueType.DOUBLE),
    hard_pending_compaction_bytes_limit(ValueType.LONG),
    level0_file_num_compaction_trigger(ValueType.INT),
    level0_slowdown_writes_trigger(ValueType.INT),
    level0_stop_writes_trigger(ValueType.INT),
    max_compaction_bytes(ValueType.LONG),
    target_file_size_base(ValueType.LONG),
    target_file_size_multiplier(ValueType.INT),
    max_bytes_for_level_base(ValueType.LONG),
    max_bytes_for_level_multiplier(ValueType.INT),
    max_bytes_for_level_multiplier_additional(ValueType.INT_ARRAY),
    ttl(ValueType.LONG);

    private final ValueType valueType;
    CompactionOption(final ValueType valueType) {
      this.valueType = valueType;
    }

    @Override
    public ValueType getValueType() {
      return valueType;
    }
  }

  public enum MiscOption implements MutableColumnFamilyOptionKey {
    max_sequential_skip_in_iterations(ValueType.LONG),
    paranoid_file_checks(ValueType.BOOLEAN),
    report_bg_io_stats(ValueType.BOOLEAN),
    compression_type(ValueType.ENUM);

    private final ValueType valueType;
    MiscOption(final ValueType valueType) {
      this.valueType = valueType;
    }

    @Override
    public ValueType getValueType() {
      return valueType;
    }
  }

  public static class MutableColumnFamilyOptionsBuilder
      extends AbstractMutableOptionsBuilder<MutableColumnFamilyOptions, MutableColumnFamilyOptionsBuilder, MutableColumnFamilyOptionKey>
      implements MutableColumnFamilyOptionsInterface<MutableColumnFamilyOptionsBuilder> {

    private final static Map<String, MutableColumnFamilyOptionKey> ALL_KEYS_LOOKUP = new HashMap<>();
    static {
      for(final MutableColumnFamilyOptionKey key : MemtableOption.values()) {
        ALL_KEYS_LOOKUP.put(key.name(), key);
      }

      for(final MutableColumnFamilyOptionKey key : CompactionOption.values()) {
        ALL_KEYS_LOOKUP.put(key.name(), key);
      }

      for(final MutableColumnFamilyOptionKey key : MiscOption.values()) {
        ALL_KEYS_LOOKUP.put(key.name(), key);
      }
    }

    private MutableColumnFamilyOptionsBuilder() {
      super();
    }

    @Override
    protected MutableColumnFamilyOptionsBuilder self() {
      return this;
    }

    @Override
    protected Map<String, MutableColumnFamilyOptionKey> allKeys() {
      return ALL_KEYS_LOOKUP;
    }

    @Override
    protected MutableColumnFamilyOptions build(final String[] keys,
        final String[] values) {
      return new MutableColumnFamilyOptions(keys, values);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setWriteBufferSize(
        final long writeBufferSize) {
      return setLong(MemtableOption.write_buffer_size, writeBufferSize);
    }

    @Override
    public long writeBufferSize() {
      return getLong(MemtableOption.write_buffer_size);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setArenaBlockSize(
        final long arenaBlockSize) {
      return setLong(MemtableOption.arena_block_size, arenaBlockSize);
    }

    @Override
    public long arenaBlockSize() {
      return getLong(MemtableOption.arena_block_size);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setMemtablePrefixBloomSizeRatio(
        final double memtablePrefixBloomSizeRatio) {
      return setDouble(MemtableOption.memtable_prefix_bloom_size_ratio,
          memtablePrefixBloomSizeRatio);
    }

    @Override
    public double memtablePrefixBloomSizeRatio() {
      return getDouble(MemtableOption.memtable_prefix_bloom_size_ratio);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setMemtableHugePageSize(
        final long memtableHugePageSize) {
      return setLong(MemtableOption.memtable_huge_page_size,
          memtableHugePageSize);
    }

    @Override
    public long memtableHugePageSize() {
      return getLong(MemtableOption.memtable_huge_page_size);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setMaxSuccessiveMerges(
        final long maxSuccessiveMerges) {
      return setLong(MemtableOption.max_successive_merges, maxSuccessiveMerges);
    }

    @Override
    public long maxSuccessiveMerges() {
      return getLong(MemtableOption.max_successive_merges);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setMaxWriteBufferNumber(
        final int maxWriteBufferNumber) {
      return setInt(MemtableOption.max_write_buffer_number,
          maxWriteBufferNumber);
    }

    @Override
    public int maxWriteBufferNumber() {
      return getInt(MemtableOption.max_write_buffer_number);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setInplaceUpdateNumLocks(
        final long inplaceUpdateNumLocks) {
      return setLong(MemtableOption.inplace_update_num_locks,
          inplaceUpdateNumLocks);
    }

    @Override
    public long inplaceUpdateNumLocks() {
      return getLong(MemtableOption.inplace_update_num_locks);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setDisableAutoCompactions(
        final boolean disableAutoCompactions) {
      return setBoolean(CompactionOption.disable_auto_compactions,
          disableAutoCompactions);
    }

    @Override
    public boolean disableAutoCompactions() {
      return getBoolean(CompactionOption.disable_auto_compactions);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setSoftPendingCompactionBytesLimit(
        final long softPendingCompactionBytesLimit) {
      return setLong(CompactionOption.soft_pending_compaction_bytes_limit,
          softPendingCompactionBytesLimit);
    }

    @Override
    public long softPendingCompactionBytesLimit() {
      return getLong(CompactionOption.soft_pending_compaction_bytes_limit);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setHardPendingCompactionBytesLimit(
        final long hardPendingCompactionBytesLimit) {
      return setLong(CompactionOption.hard_pending_compaction_bytes_limit,
          hardPendingCompactionBytesLimit);
    }

    @Override
    public long hardPendingCompactionBytesLimit() {
      return getLong(CompactionOption.hard_pending_compaction_bytes_limit);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setLevel0FileNumCompactionTrigger(
        final int level0FileNumCompactionTrigger) {
      return setInt(CompactionOption.level0_file_num_compaction_trigger,
          level0FileNumCompactionTrigger);
    }

    @Override
    public int level0FileNumCompactionTrigger() {
      return getInt(CompactionOption.level0_file_num_compaction_trigger);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setLevel0SlowdownWritesTrigger(
        final int level0SlowdownWritesTrigger) {
      return setInt(CompactionOption.level0_slowdown_writes_trigger,
          level0SlowdownWritesTrigger);
    }

    @Override
    public int level0SlowdownWritesTrigger() {
      return getInt(CompactionOption.level0_slowdown_writes_trigger);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setLevel0StopWritesTrigger(
        final int level0StopWritesTrigger) {
      return setInt(CompactionOption.level0_stop_writes_trigger,
          level0StopWritesTrigger);
    }

    @Override
    public int level0StopWritesTrigger() {
      return getInt(CompactionOption.level0_stop_writes_trigger);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setMaxCompactionBytes(final long maxCompactionBytes) {
      return setLong(CompactionOption.max_compaction_bytes, maxCompactionBytes);
    }

    @Override
    public long maxCompactionBytes() {
      return getLong(CompactionOption.max_compaction_bytes);
    }


    @Override
    public MutableColumnFamilyOptionsBuilder setTargetFileSizeBase(
        final long targetFileSizeBase) {
      return setLong(CompactionOption.target_file_size_base,
          targetFileSizeBase);
    }

    @Override
    public long targetFileSizeBase() {
      return getLong(CompactionOption.target_file_size_base);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setTargetFileSizeMultiplier(
        final int targetFileSizeMultiplier) {
      return setInt(CompactionOption.target_file_size_multiplier,
          targetFileSizeMultiplier);
    }

    @Override
    public int targetFileSizeMultiplier() {
      return getInt(CompactionOption.target_file_size_multiplier);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setMaxBytesForLevelBase(
        final long maxBytesForLevelBase) {
      return setLong(CompactionOption.max_bytes_for_level_base,
          maxBytesForLevelBase);
    }

    @Override
    public long maxBytesForLevelBase() {
      return getLong(CompactionOption.max_bytes_for_level_base);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setMaxBytesForLevelMultiplier(
        final double maxBytesForLevelMultiplier) {
      return setDouble(CompactionOption.max_bytes_for_level_multiplier, maxBytesForLevelMultiplier);
    }

    @Override
    public double maxBytesForLevelMultiplier() {
      return getDouble(CompactionOption.max_bytes_for_level_multiplier);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setMaxBytesForLevelMultiplierAdditional(
        final int[] maxBytesForLevelMultiplierAdditional) {
      return setIntArray(
          CompactionOption.max_bytes_for_level_multiplier_additional,
          maxBytesForLevelMultiplierAdditional);
    }

    @Override
    public int[] maxBytesForLevelMultiplierAdditional() {
      return getIntArray(
          CompactionOption.max_bytes_for_level_multiplier_additional);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setMaxSequentialSkipInIterations(
        final long maxSequentialSkipInIterations) {
      return setLong(MiscOption.max_sequential_skip_in_iterations,
          maxSequentialSkipInIterations);
    }

    @Override
    public long maxSequentialSkipInIterations() {
      return getLong(MiscOption.max_sequential_skip_in_iterations);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setParanoidFileChecks(
        final boolean paranoidFileChecks) {
      return setBoolean(MiscOption.paranoid_file_checks, paranoidFileChecks);
    }

    @Override
    public boolean paranoidFileChecks() {
      return getBoolean(MiscOption.paranoid_file_checks);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setCompressionType(
        final CompressionType compressionType) {
      return setEnum(MiscOption.compression_type, compressionType);
    }

    @Override
    public CompressionType compressionType() {
      return (CompressionType)getEnum(MiscOption.compression_type);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setReportBgIoStats(
        final boolean reportBgIoStats) {
      return setBoolean(MiscOption.report_bg_io_stats, reportBgIoStats);
    }

    @Override
    public boolean reportBgIoStats() {
      return getBoolean(MiscOption.report_bg_io_stats);
    }

    @Override
    public MutableColumnFamilyOptionsBuilder setTtl(final long ttl) {
      return setLong(CompactionOption.ttl, ttl);
    }

    @Override
    public long ttl() {
      return getLong(CompactionOption.ttl);
    }
  }
}
