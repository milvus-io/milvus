// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * ColumnFamilyOptions to control the behavior of a database.  It will be used
 * during the creation of a {@link org.rocksdb.RocksDB} (i.e., RocksDB.open()).
 *
 * If {@link #dispose()} function is not called, then it will be GC'd
 * automatically and native resources will be released as part of the process.
 */
public class ColumnFamilyOptions extends RocksObject
    implements ColumnFamilyOptionsInterface<ColumnFamilyOptions>,
    MutableColumnFamilyOptionsInterface<ColumnFamilyOptions> {
  static {
    RocksDB.loadLibrary();
  }

  /**
   * Construct ColumnFamilyOptions.
   *
   * This constructor will create (by allocating a block of memory)
   * an {@code rocksdb::ColumnFamilyOptions} in the c++ side.
   */
  public ColumnFamilyOptions() {
    super(newColumnFamilyOptions());
  }

  /**
   * Copy constructor for ColumnFamilyOptions.
   *
   * NOTE: This does a shallow copy, which means comparator, merge_operator, compaction_filter,
   * compaction_filter_factory and other pointers will be cloned!
   *
   * @param other The ColumnFamilyOptions to copy.
   */
  public ColumnFamilyOptions(ColumnFamilyOptions other) {
    super(copyColumnFamilyOptions(other.nativeHandle_));
    this.memTableConfig_ = other.memTableConfig_;
    this.tableFormatConfig_ = other.tableFormatConfig_;
    this.comparator_ = other.comparator_;
    this.compactionFilter_ = other.compactionFilter_;
    this.compactionFilterFactory_ = other.compactionFilterFactory_;
    this.compactionOptionsUniversal_ = other.compactionOptionsUniversal_;
    this.compactionOptionsFIFO_ = other.compactionOptionsFIFO_;
    this.bottommostCompressionOptions_ = other.bottommostCompressionOptions_;
    this.compressionOptions_ = other.compressionOptions_;
  }

  /**
   * Constructor from Options
   *
   * @param options The options.
   */
  public ColumnFamilyOptions(final Options options) {
    super(newColumnFamilyOptionsFromOptions(options.nativeHandle_));
  }

  /**
   * <p>Constructor to be used by
   * {@link #getColumnFamilyOptionsFromProps(java.util.Properties)},
   * {@link ColumnFamilyDescriptor#columnFamilyOptions()}
   * and also called via JNI.</p>
   *
   * @param handle native handle to ColumnFamilyOptions instance.
   */
  ColumnFamilyOptions(final long handle) {
    super(handle);
  }

  /**
   * <p>Method to get a options instance by using pre-configured
   * property values. If one or many values are undefined in
   * the context of RocksDB the method will return a null
   * value.</p>
   *
   * <p><strong>Note</strong>: Property keys can be derived from
   * getter methods within the options class. Example: the method
   * {@code writeBufferSize()} has a property key:
   * {@code write_buffer_size}.</p>
   *
   * @param properties {@link java.util.Properties} instance.
   *
   * @return {@link org.rocksdb.ColumnFamilyOptions instance}
   *     or null.
   *
   * @throws java.lang.IllegalArgumentException if null or empty
   *     {@link Properties} instance is passed to the method call.
   */
  public static ColumnFamilyOptions getColumnFamilyOptionsFromProps(
      final Properties properties) {
    if (properties == null || properties.size() == 0) {
      throw new IllegalArgumentException(
          "Properties value must contain at least one value.");
    }
    ColumnFamilyOptions columnFamilyOptions = null;
    StringBuilder stringBuilder = new StringBuilder();
    for (final String name : properties.stringPropertyNames()){
      stringBuilder.append(name);
      stringBuilder.append("=");
      stringBuilder.append(properties.getProperty(name));
      stringBuilder.append(";");
    }
    long handle = getColumnFamilyOptionsFromProps(
        stringBuilder.toString());
    if (handle != 0){
      columnFamilyOptions = new ColumnFamilyOptions(handle);
    }
    return columnFamilyOptions;
  }

  @Override
  public ColumnFamilyOptions optimizeForSmallDb() {
    optimizeForSmallDb(nativeHandle_);
    return this;
  }

  @Override
  public ColumnFamilyOptions optimizeForPointLookup(
      final long blockCacheSizeMb) {
    optimizeForPointLookup(nativeHandle_,
        blockCacheSizeMb);
    return this;
  }

  @Override
  public ColumnFamilyOptions optimizeLevelStyleCompaction() {
    optimizeLevelStyleCompaction(nativeHandle_,
        DEFAULT_COMPACTION_MEMTABLE_MEMORY_BUDGET);
    return this;
  }

  @Override
  public ColumnFamilyOptions optimizeLevelStyleCompaction(
      final long memtableMemoryBudget) {
    optimizeLevelStyleCompaction(nativeHandle_,
        memtableMemoryBudget);
    return this;
  }

  @Override
  public ColumnFamilyOptions optimizeUniversalStyleCompaction() {
    optimizeUniversalStyleCompaction(nativeHandle_,
        DEFAULT_COMPACTION_MEMTABLE_MEMORY_BUDGET);
    return this;
  }

  @Override
  public ColumnFamilyOptions optimizeUniversalStyleCompaction(
      final long memtableMemoryBudget) {
    optimizeUniversalStyleCompaction(nativeHandle_,
        memtableMemoryBudget);
    return this;
  }

  @Override
  public ColumnFamilyOptions setComparator(
      final BuiltinComparator builtinComparator) {
    assert(isOwningHandle());
    setComparatorHandle(nativeHandle_, builtinComparator.ordinal());
    return this;
  }

  @Override
  public ColumnFamilyOptions setComparator(
      final AbstractComparator<? extends AbstractSlice<?>> comparator) {
    assert (isOwningHandle());
    setComparatorHandle(nativeHandle_, comparator.nativeHandle_,
            comparator.getComparatorType().getValue());
    comparator_ = comparator;
    return this;
  }

  @Override
  public ColumnFamilyOptions setMergeOperatorName(final String name) {
    assert (isOwningHandle());
    if (name == null) {
      throw new IllegalArgumentException(
          "Merge operator name must not be null.");
    }
    setMergeOperatorName(nativeHandle_, name);
    return this;
  }

  @Override
  public ColumnFamilyOptions setMergeOperator(
      final MergeOperator mergeOperator) {
    setMergeOperator(nativeHandle_, mergeOperator.nativeHandle_);
    return this;
  }

  @Override
  public ColumnFamilyOptions setCompactionFilter(
        final AbstractCompactionFilter<? extends AbstractSlice<?>>
            compactionFilter) {
    setCompactionFilterHandle(nativeHandle_, compactionFilter.nativeHandle_);
    compactionFilter_ = compactionFilter;
    return this;
  }

  @Override
  public AbstractCompactionFilter<? extends AbstractSlice<?>> compactionFilter() {
    assert (isOwningHandle());
    return compactionFilter_;
  }

  @Override
  public ColumnFamilyOptions setCompactionFilterFactory(final AbstractCompactionFilterFactory<? extends AbstractCompactionFilter<?>> compactionFilterFactory) {
    assert (isOwningHandle());
    setCompactionFilterFactoryHandle(nativeHandle_, compactionFilterFactory.nativeHandle_);
    compactionFilterFactory_ = compactionFilterFactory;
    return this;
  }

  @Override
  public AbstractCompactionFilterFactory<? extends AbstractCompactionFilter<?>> compactionFilterFactory() {
    assert (isOwningHandle());
    return compactionFilterFactory_;
  }

  @Override
  public ColumnFamilyOptions setWriteBufferSize(final long writeBufferSize) {
    assert(isOwningHandle());
    setWriteBufferSize(nativeHandle_, writeBufferSize);
    return this;
  }

  @Override
  public long writeBufferSize()  {
    assert(isOwningHandle());
    return writeBufferSize(nativeHandle_);
  }

  @Override
  public ColumnFamilyOptions setMaxWriteBufferNumber(
      final int maxWriteBufferNumber) {
    assert(isOwningHandle());
    setMaxWriteBufferNumber(nativeHandle_, maxWriteBufferNumber);
    return this;
  }

  @Override
  public int maxWriteBufferNumber() {
    assert(isOwningHandle());
    return maxWriteBufferNumber(nativeHandle_);
  }

  @Override
  public ColumnFamilyOptions setMinWriteBufferNumberToMerge(
      final int minWriteBufferNumberToMerge) {
    setMinWriteBufferNumberToMerge(nativeHandle_, minWriteBufferNumberToMerge);
    return this;
  }

  @Override
  public int minWriteBufferNumberToMerge() {
    return minWriteBufferNumberToMerge(nativeHandle_);
  }

  @Override
  public ColumnFamilyOptions useFixedLengthPrefixExtractor(final int n) {
    assert(isOwningHandle());
    useFixedLengthPrefixExtractor(nativeHandle_, n);
    return this;
  }

  @Override
  public ColumnFamilyOptions useCappedPrefixExtractor(final int n) {
    assert(isOwningHandle());
    useCappedPrefixExtractor(nativeHandle_, n);
    return this;
  }

  @Override
  public ColumnFamilyOptions setCompressionType(
      final CompressionType compressionType) {
    setCompressionType(nativeHandle_, compressionType.getValue());
    return this;
  }

  @Override
  public CompressionType compressionType() {
    return CompressionType.getCompressionType(compressionType(nativeHandle_));
  }

  @Override
  public ColumnFamilyOptions setCompressionPerLevel(
      final List<CompressionType> compressionLevels) {
    final byte[] byteCompressionTypes = new byte[
        compressionLevels.size()];
    for (int i = 0; i < compressionLevels.size(); i++) {
      byteCompressionTypes[i] = compressionLevels.get(i).getValue();
    }
    setCompressionPerLevel(nativeHandle_, byteCompressionTypes);
    return this;
  }

  @Override
  public List<CompressionType> compressionPerLevel() {
    final byte[] byteCompressionTypes =
        compressionPerLevel(nativeHandle_);
    final List<CompressionType> compressionLevels = new ArrayList<>();
    for (final Byte byteCompressionType : byteCompressionTypes) {
      compressionLevels.add(CompressionType.getCompressionType(
          byteCompressionType));
    }
    return compressionLevels;
  }

  @Override
  public ColumnFamilyOptions setBottommostCompressionType(
      final CompressionType bottommostCompressionType) {
    setBottommostCompressionType(nativeHandle_,
        bottommostCompressionType.getValue());
    return this;
  }

  @Override
  public CompressionType bottommostCompressionType() {
    return CompressionType.getCompressionType(
        bottommostCompressionType(nativeHandle_));
  }

  @Override
  public ColumnFamilyOptions setBottommostCompressionOptions(
      final CompressionOptions bottommostCompressionOptions) {
    setBottommostCompressionOptions(nativeHandle_,
        bottommostCompressionOptions.nativeHandle_);
    this.bottommostCompressionOptions_ = bottommostCompressionOptions;
    return this;
  }

  @Override
  public CompressionOptions bottommostCompressionOptions() {
    return this.bottommostCompressionOptions_;
  }

  @Override
  public ColumnFamilyOptions setCompressionOptions(
      final CompressionOptions compressionOptions) {
    setCompressionOptions(nativeHandle_, compressionOptions.nativeHandle_);
    this.compressionOptions_ = compressionOptions;
    return this;
  }

  @Override
  public CompressionOptions compressionOptions() {
    return this.compressionOptions_;
  }

  @Override
  public ColumnFamilyOptions setNumLevels(final int numLevels) {
    setNumLevels(nativeHandle_, numLevels);
    return this;
  }

  @Override
  public int numLevels() {
    return numLevels(nativeHandle_);
  }

  @Override
  public ColumnFamilyOptions setLevelZeroFileNumCompactionTrigger(
      final int numFiles) {
    setLevelZeroFileNumCompactionTrigger(
        nativeHandle_, numFiles);
    return this;
  }

  @Override
  public int levelZeroFileNumCompactionTrigger() {
    return levelZeroFileNumCompactionTrigger(nativeHandle_);
  }

  @Override
  public ColumnFamilyOptions setLevelZeroSlowdownWritesTrigger(
      final int numFiles) {
    setLevelZeroSlowdownWritesTrigger(nativeHandle_, numFiles);
    return this;
  }

  @Override
  public int levelZeroSlowdownWritesTrigger() {
    return levelZeroSlowdownWritesTrigger(nativeHandle_);
  }

  @Override
  public ColumnFamilyOptions setLevelZeroStopWritesTrigger(final int numFiles) {
    setLevelZeroStopWritesTrigger(nativeHandle_, numFiles);
    return this;
  }

  @Override
  public int levelZeroStopWritesTrigger() {
    return levelZeroStopWritesTrigger(nativeHandle_);
  }

  @Override
  public ColumnFamilyOptions setTargetFileSizeBase(
      final long targetFileSizeBase) {
    setTargetFileSizeBase(nativeHandle_, targetFileSizeBase);
    return this;
  }

  @Override
  public long targetFileSizeBase() {
    return targetFileSizeBase(nativeHandle_);
  }

  @Override
  public ColumnFamilyOptions setTargetFileSizeMultiplier(
      final int multiplier) {
    setTargetFileSizeMultiplier(nativeHandle_, multiplier);
    return this;
  }

  @Override
  public int targetFileSizeMultiplier() {
    return targetFileSizeMultiplier(nativeHandle_);
  }

  @Override
  public ColumnFamilyOptions setMaxBytesForLevelBase(
      final long maxBytesForLevelBase) {
    setMaxBytesForLevelBase(nativeHandle_, maxBytesForLevelBase);
    return this;
  }

  @Override
  public long maxBytesForLevelBase() {
    return maxBytesForLevelBase(nativeHandle_);
  }

  @Override
  public ColumnFamilyOptions setLevelCompactionDynamicLevelBytes(
      final boolean enableLevelCompactionDynamicLevelBytes) {
    setLevelCompactionDynamicLevelBytes(nativeHandle_,
        enableLevelCompactionDynamicLevelBytes);
    return this;
  }

  @Override
  public boolean levelCompactionDynamicLevelBytes() {
    return levelCompactionDynamicLevelBytes(nativeHandle_);
  }

  @Override
  public ColumnFamilyOptions setMaxBytesForLevelMultiplier(final double multiplier) {
    setMaxBytesForLevelMultiplier(nativeHandle_, multiplier);
    return this;
  }

  @Override
  public double maxBytesForLevelMultiplier() {
    return maxBytesForLevelMultiplier(nativeHandle_);
  }

  @Override
  public ColumnFamilyOptions setMaxCompactionBytes(final long maxCompactionBytes) {
    setMaxCompactionBytes(nativeHandle_, maxCompactionBytes);
    return this;
  }

  @Override
  public long maxCompactionBytes() {
    return maxCompactionBytes(nativeHandle_);
  }

  @Override
  public ColumnFamilyOptions setArenaBlockSize(
      final long arenaBlockSize) {
    setArenaBlockSize(nativeHandle_, arenaBlockSize);
    return this;
  }

  @Override
  public long arenaBlockSize() {
    return arenaBlockSize(nativeHandle_);
  }

  @Override
  public ColumnFamilyOptions setDisableAutoCompactions(
      final boolean disableAutoCompactions) {
    setDisableAutoCompactions(nativeHandle_, disableAutoCompactions);
    return this;
  }

  @Override
  public boolean disableAutoCompactions() {
    return disableAutoCompactions(nativeHandle_);
  }

  @Override
  public ColumnFamilyOptions setCompactionStyle(
      final CompactionStyle compactionStyle) {
    setCompactionStyle(nativeHandle_, compactionStyle.getValue());
    return this;
  }

  @Override
  public CompactionStyle compactionStyle() {
    return CompactionStyle.fromValue(compactionStyle(nativeHandle_));
  }

  @Override
  public ColumnFamilyOptions setMaxTableFilesSizeFIFO(
      final long maxTableFilesSize) {
    assert(maxTableFilesSize > 0); // unsigned native type
    assert(isOwningHandle());
    setMaxTableFilesSizeFIFO(nativeHandle_, maxTableFilesSize);
    return this;
  }

  @Override
  public long maxTableFilesSizeFIFO() {
    return maxTableFilesSizeFIFO(nativeHandle_);
  }

  @Override
  public ColumnFamilyOptions setMaxSequentialSkipInIterations(
      final long maxSequentialSkipInIterations) {
    setMaxSequentialSkipInIterations(nativeHandle_,
        maxSequentialSkipInIterations);
    return this;
  }

  @Override
  public long maxSequentialSkipInIterations() {
    return maxSequentialSkipInIterations(nativeHandle_);
  }

  @Override
  public MemTableConfig memTableConfig() {
    return this.memTableConfig_;
  }

  @Override
  public ColumnFamilyOptions setMemTableConfig(
      final MemTableConfig memTableConfig) {
    setMemTableFactory(
        nativeHandle_, memTableConfig.newMemTableFactoryHandle());
    this.memTableConfig_ = memTableConfig;
    return this;
  }

  @Override
  public String memTableFactoryName() {
    assert(isOwningHandle());
    return memTableFactoryName(nativeHandle_);
  }

  @Override
  public TableFormatConfig tableFormatConfig() {
    return this.tableFormatConfig_;
  }

  @Override
  public ColumnFamilyOptions setTableFormatConfig(
      final TableFormatConfig tableFormatConfig) {
    setTableFactory(nativeHandle_, tableFormatConfig.newTableFactoryHandle());
    this.tableFormatConfig_ = tableFormatConfig;
    return this;
  }

  @Override
  public String tableFactoryName() {
    assert(isOwningHandle());
    return tableFactoryName(nativeHandle_);
  }

  @Override
  public ColumnFamilyOptions setInplaceUpdateSupport(
      final boolean inplaceUpdateSupport) {
    setInplaceUpdateSupport(nativeHandle_, inplaceUpdateSupport);
    return this;
  }

  @Override
  public boolean inplaceUpdateSupport() {
    return inplaceUpdateSupport(nativeHandle_);
  }

  @Override
  public ColumnFamilyOptions setInplaceUpdateNumLocks(
      final long inplaceUpdateNumLocks) {
    setInplaceUpdateNumLocks(nativeHandle_, inplaceUpdateNumLocks);
    return this;
  }

  @Override
  public long inplaceUpdateNumLocks() {
    return inplaceUpdateNumLocks(nativeHandle_);
  }

  @Override
  public ColumnFamilyOptions setMemtablePrefixBloomSizeRatio(
      final double memtablePrefixBloomSizeRatio) {
    setMemtablePrefixBloomSizeRatio(nativeHandle_, memtablePrefixBloomSizeRatio);
    return this;
  }

  @Override
  public double memtablePrefixBloomSizeRatio() {
    return memtablePrefixBloomSizeRatio(nativeHandle_);
  }

  @Override
  public ColumnFamilyOptions setBloomLocality(int bloomLocality) {
    setBloomLocality(nativeHandle_, bloomLocality);
    return this;
  }

  @Override
  public int bloomLocality() {
    return bloomLocality(nativeHandle_);
  }

  @Override
  public ColumnFamilyOptions setMaxSuccessiveMerges(
      final long maxSuccessiveMerges) {
    setMaxSuccessiveMerges(nativeHandle_, maxSuccessiveMerges);
    return this;
  }

  @Override
  public long maxSuccessiveMerges() {
    return maxSuccessiveMerges(nativeHandle_);
  }

  @Override
  public ColumnFamilyOptions setOptimizeFiltersForHits(
      final boolean optimizeFiltersForHits) {
    setOptimizeFiltersForHits(nativeHandle_, optimizeFiltersForHits);
    return this;
  }

  @Override
  public boolean optimizeFiltersForHits() {
    return optimizeFiltersForHits(nativeHandle_);
  }

  @Override
  public ColumnFamilyOptions
  setMemtableHugePageSize(
      long memtableHugePageSize) {
    setMemtableHugePageSize(nativeHandle_,
        memtableHugePageSize);
    return this;
  }

  @Override
  public long memtableHugePageSize() {
    return memtableHugePageSize(nativeHandle_);
  }

  @Override
  public ColumnFamilyOptions setSoftPendingCompactionBytesLimit(long softPendingCompactionBytesLimit) {
    setSoftPendingCompactionBytesLimit(nativeHandle_,
        softPendingCompactionBytesLimit);
    return this;
  }

  @Override
  public long softPendingCompactionBytesLimit() {
    return softPendingCompactionBytesLimit(nativeHandle_);
  }

  @Override
  public ColumnFamilyOptions setHardPendingCompactionBytesLimit(long hardPendingCompactionBytesLimit) {
    setHardPendingCompactionBytesLimit(nativeHandle_, hardPendingCompactionBytesLimit);
    return this;
  }

  @Override
  public long hardPendingCompactionBytesLimit() {
    return hardPendingCompactionBytesLimit(nativeHandle_);
  }

  @Override
  public ColumnFamilyOptions setLevel0FileNumCompactionTrigger(int level0FileNumCompactionTrigger) {
    setLevel0FileNumCompactionTrigger(nativeHandle_, level0FileNumCompactionTrigger);
    return this;
  }

  @Override
  public int level0FileNumCompactionTrigger() {
    return level0FileNumCompactionTrigger(nativeHandle_);
  }

  @Override
  public ColumnFamilyOptions setLevel0SlowdownWritesTrigger(int level0SlowdownWritesTrigger) {
    setLevel0SlowdownWritesTrigger(nativeHandle_, level0SlowdownWritesTrigger);
    return this;
  }

  @Override
  public int level0SlowdownWritesTrigger() {
    return level0SlowdownWritesTrigger(nativeHandle_);
  }

  @Override
  public ColumnFamilyOptions setLevel0StopWritesTrigger(int level0StopWritesTrigger) {
    setLevel0StopWritesTrigger(nativeHandle_, level0StopWritesTrigger);
    return this;
  }

  @Override
  public int level0StopWritesTrigger() {
    return level0StopWritesTrigger(nativeHandle_);
  }

  @Override
  public ColumnFamilyOptions setMaxBytesForLevelMultiplierAdditional(int[] maxBytesForLevelMultiplierAdditional) {
    setMaxBytesForLevelMultiplierAdditional(nativeHandle_, maxBytesForLevelMultiplierAdditional);
    return this;
  }

  @Override
  public int[] maxBytesForLevelMultiplierAdditional() {
    return maxBytesForLevelMultiplierAdditional(nativeHandle_);
  }

  @Override
  public ColumnFamilyOptions setParanoidFileChecks(boolean paranoidFileChecks) {
    setParanoidFileChecks(nativeHandle_, paranoidFileChecks);
    return this;
  }

  @Override
  public boolean paranoidFileChecks() {
    return paranoidFileChecks(nativeHandle_);
  }

  @Override
  public ColumnFamilyOptions setMaxWriteBufferNumberToMaintain(
      final int maxWriteBufferNumberToMaintain) {
    setMaxWriteBufferNumberToMaintain(
        nativeHandle_, maxWriteBufferNumberToMaintain);
    return this;
  }

  @Override
  public int maxWriteBufferNumberToMaintain() {
    return maxWriteBufferNumberToMaintain(nativeHandle_);
  }

  @Override
  public ColumnFamilyOptions setCompactionPriority(
      final CompactionPriority compactionPriority) {
    setCompactionPriority(nativeHandle_, compactionPriority.getValue());
    return this;
  }

  @Override
  public CompactionPriority compactionPriority() {
    return CompactionPriority.getCompactionPriority(
        compactionPriority(nativeHandle_));
  }

  @Override
  public ColumnFamilyOptions setReportBgIoStats(final boolean reportBgIoStats) {
    setReportBgIoStats(nativeHandle_, reportBgIoStats);
    return this;
  }

  @Override
  public boolean reportBgIoStats() {
    return reportBgIoStats(nativeHandle_);
  }

  @Override
  public ColumnFamilyOptions setTtl(final long ttl) {
    setTtl(nativeHandle_, ttl);
    return this;
  }

  @Override
  public long ttl() {
    return ttl(nativeHandle_);
  }

  @Override
  public ColumnFamilyOptions setCompactionOptionsUniversal(
      final CompactionOptionsUniversal compactionOptionsUniversal) {
    setCompactionOptionsUniversal(nativeHandle_,
        compactionOptionsUniversal.nativeHandle_);
    this.compactionOptionsUniversal_ = compactionOptionsUniversal;
    return this;
  }

  @Override
  public CompactionOptionsUniversal compactionOptionsUniversal() {
    return this.compactionOptionsUniversal_;
  }

  @Override
  public ColumnFamilyOptions setCompactionOptionsFIFO(final CompactionOptionsFIFO compactionOptionsFIFO) {
    setCompactionOptionsFIFO(nativeHandle_,
        compactionOptionsFIFO.nativeHandle_);
    this.compactionOptionsFIFO_ = compactionOptionsFIFO;
    return this;
  }

  @Override
  public CompactionOptionsFIFO compactionOptionsFIFO() {
    return this.compactionOptionsFIFO_;
  }

  @Override
  public ColumnFamilyOptions setForceConsistencyChecks(final boolean forceConsistencyChecks) {
    setForceConsistencyChecks(nativeHandle_, forceConsistencyChecks);
    return this;
  }

  @Override
  public boolean forceConsistencyChecks() {
    return forceConsistencyChecks(nativeHandle_);
  }

  private static native long getColumnFamilyOptionsFromProps(
      String optString);

  private static native long newColumnFamilyOptions();
  private static native long copyColumnFamilyOptions(final long handle);
  private static native long newColumnFamilyOptionsFromOptions(
      final long optionsHandle);
  @Override protected final native void disposeInternal(final long handle);

  private native void optimizeForSmallDb(final long handle);
  private native void optimizeForPointLookup(long handle,
      long blockCacheSizeMb);
  private native void optimizeLevelStyleCompaction(long handle,
      long memtableMemoryBudget);
  private native void optimizeUniversalStyleCompaction(long handle,
      long memtableMemoryBudget);
  private native void setComparatorHandle(long handle, int builtinComparator);
  private native void setComparatorHandle(long optHandle,
      long comparatorHandle, byte comparatorType);
  private native void setMergeOperatorName(long handle, String name);
  private native void setMergeOperator(long handle, long mergeOperatorHandle);
  private native void setCompactionFilterHandle(long handle,
      long compactionFilterHandle);
  private native void setCompactionFilterFactoryHandle(long handle,
      long compactionFilterFactoryHandle);
  private native void setWriteBufferSize(long handle, long writeBufferSize)
      throws IllegalArgumentException;
  private native long writeBufferSize(long handle);
  private native void setMaxWriteBufferNumber(
      long handle, int maxWriteBufferNumber);
  private native int maxWriteBufferNumber(long handle);
  private native void setMinWriteBufferNumberToMerge(
      long handle, int minWriteBufferNumberToMerge);
  private native int minWriteBufferNumberToMerge(long handle);
  private native void setCompressionType(long handle, byte compressionType);
  private native byte compressionType(long handle);
  private native void setCompressionPerLevel(long handle,
      byte[] compressionLevels);
  private native byte[] compressionPerLevel(long handle);
  private native void setBottommostCompressionType(long handle,
      byte bottommostCompressionType);
  private native byte bottommostCompressionType(long handle);
  private native void setBottommostCompressionOptions(final long handle,
      final long bottommostCompressionOptionsHandle);
  private native void setCompressionOptions(long handle,
      long compressionOptionsHandle);
  private native void useFixedLengthPrefixExtractor(
      long handle, int prefixLength);
  private native void useCappedPrefixExtractor(
      long handle, int prefixLength);
  private native void setNumLevels(
      long handle, int numLevels);
  private native int numLevels(long handle);
  private native void setLevelZeroFileNumCompactionTrigger(
      long handle, int numFiles);
  private native int levelZeroFileNumCompactionTrigger(long handle);
  private native void setLevelZeroSlowdownWritesTrigger(
      long handle, int numFiles);
  private native int levelZeroSlowdownWritesTrigger(long handle);
  private native void setLevelZeroStopWritesTrigger(
      long handle, int numFiles);
  private native int levelZeroStopWritesTrigger(long handle);
  private native void setTargetFileSizeBase(
      long handle, long targetFileSizeBase);
  private native long targetFileSizeBase(long handle);
  private native void setTargetFileSizeMultiplier(
      long handle, int multiplier);
  private native int targetFileSizeMultiplier(long handle);
  private native void setMaxBytesForLevelBase(
      long handle, long maxBytesForLevelBase);
  private native long maxBytesForLevelBase(long handle);
  private native void setLevelCompactionDynamicLevelBytes(
      long handle, boolean enableLevelCompactionDynamicLevelBytes);
  private native boolean levelCompactionDynamicLevelBytes(
      long handle);
  private native void setMaxBytesForLevelMultiplier(long handle, double multiplier);
  private native double maxBytesForLevelMultiplier(long handle);
  private native void setMaxCompactionBytes(long handle, long maxCompactionBytes);
  private native long maxCompactionBytes(long handle);
  private native void setArenaBlockSize(
      long handle, long arenaBlockSize)
      throws IllegalArgumentException;
  private native long arenaBlockSize(long handle);
  private native void setDisableAutoCompactions(
      long handle, boolean disableAutoCompactions);
  private native boolean disableAutoCompactions(long handle);
  private native void setCompactionStyle(long handle, byte compactionStyle);
  private native byte compactionStyle(long handle);
   private native void setMaxTableFilesSizeFIFO(
      long handle, long max_table_files_size);
  private native long maxTableFilesSizeFIFO(long handle);
  private native void setMaxSequentialSkipInIterations(
      long handle, long maxSequentialSkipInIterations);
  private native long maxSequentialSkipInIterations(long handle);
  private native void setMemTableFactory(long handle, long factoryHandle);
  private native String memTableFactoryName(long handle);
  private native void setTableFactory(long handle, long factoryHandle);
  private native String tableFactoryName(long handle);
  private native void setInplaceUpdateSupport(
      long handle, boolean inplaceUpdateSupport);
  private native boolean inplaceUpdateSupport(long handle);
  private native void setInplaceUpdateNumLocks(
      long handle, long inplaceUpdateNumLocks)
      throws IllegalArgumentException;
  private native long inplaceUpdateNumLocks(long handle);
  private native void setMemtablePrefixBloomSizeRatio(
      long handle, double memtablePrefixBloomSizeRatio);
  private native double memtablePrefixBloomSizeRatio(long handle);
  private native void setBloomLocality(
      long handle, int bloomLocality);
  private native int bloomLocality(long handle);
  private native void setMaxSuccessiveMerges(
      long handle, long maxSuccessiveMerges)
      throws IllegalArgumentException;
  private native long maxSuccessiveMerges(long handle);
  private native void setOptimizeFiltersForHits(long handle,
      boolean optimizeFiltersForHits);
  private native boolean optimizeFiltersForHits(long handle);
  private native void setMemtableHugePageSize(long handle,
      long memtableHugePageSize);
  private native long memtableHugePageSize(long handle);
  private native void setSoftPendingCompactionBytesLimit(long handle,
      long softPendingCompactionBytesLimit);
  private native long softPendingCompactionBytesLimit(long handle);
  private native void setHardPendingCompactionBytesLimit(long handle,
      long hardPendingCompactionBytesLimit);
  private native long hardPendingCompactionBytesLimit(long handle);
  private native void setLevel0FileNumCompactionTrigger(long handle,
      int level0FileNumCompactionTrigger);
  private native int level0FileNumCompactionTrigger(long handle);
  private native void setLevel0SlowdownWritesTrigger(long handle,
      int level0SlowdownWritesTrigger);
  private native int level0SlowdownWritesTrigger(long handle);
  private native void setLevel0StopWritesTrigger(long handle,
      int level0StopWritesTrigger);
  private native int level0StopWritesTrigger(long handle);
  private native void setMaxBytesForLevelMultiplierAdditional(long handle,
      int[] maxBytesForLevelMultiplierAdditional);
  private native int[] maxBytesForLevelMultiplierAdditional(long handle);
  private native void setParanoidFileChecks(long handle,
      boolean paranoidFileChecks);
  private native boolean paranoidFileChecks(long handle);
  private native void setMaxWriteBufferNumberToMaintain(final long handle,
      final int maxWriteBufferNumberToMaintain);
  private native int maxWriteBufferNumberToMaintain(final long handle);
  private native void setCompactionPriority(final long handle,
      final byte compactionPriority);
  private native byte compactionPriority(final long handle);
  private native void setReportBgIoStats(final long handle,
    final boolean reportBgIoStats);
  private native boolean reportBgIoStats(final long handle);
  private native void setTtl(final long handle, final long ttl);
  private native long ttl(final long handle);
  private native void setCompactionOptionsUniversal(final long handle,
    final long compactionOptionsUniversalHandle);
  private native void setCompactionOptionsFIFO(final long handle,
    final long compactionOptionsFIFOHandle);
  private native void setForceConsistencyChecks(final long handle,
    final boolean forceConsistencyChecks);
  private native boolean forceConsistencyChecks(final long handle);

  // instance variables
  // NOTE: If you add new member variables, please update the copy constructor above!
  private MemTableConfig memTableConfig_;
  private TableFormatConfig tableFormatConfig_;
  private AbstractComparator<? extends AbstractSlice<?>> comparator_;
  private AbstractCompactionFilter<? extends AbstractSlice<?>> compactionFilter_;
  private AbstractCompactionFilterFactory<? extends AbstractCompactionFilter<?>>
      compactionFilterFactory_;
  private CompactionOptionsUniversal compactionOptionsUniversal_;
  private CompactionOptionsFIFO compactionOptionsFIFO_;
  private CompressionOptions bottommostCompressionOptions_;
  private CompressionOptions compressionOptions_;

}
