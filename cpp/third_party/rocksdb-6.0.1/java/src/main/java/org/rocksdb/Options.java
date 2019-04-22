// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Options to control the behavior of a database.  It will be used
 * during the creation of a {@link org.rocksdb.RocksDB} (i.e., RocksDB.open()).
 *
 * If {@link #dispose()} function is not called, then it will be GC'd
 * automaticallyand native resources will be released as part of the process.
 */
public class Options extends RocksObject
    implements DBOptionsInterface<Options>,
    MutableDBOptionsInterface<Options>,
    ColumnFamilyOptionsInterface<Options>,
    MutableColumnFamilyOptionsInterface<Options> {
  static {
    RocksDB.loadLibrary();
  }

  /**
   * Construct options for opening a RocksDB.
   *
   * This constructor will create (by allocating a block of memory)
   * an {@code rocksdb::Options} in the c++ side.
   */
  public Options() {
    super(newOptions());
    env_ = Env.getDefault();
  }

  /**
   * Construct options for opening a RocksDB. Reusing database options
   * and column family options.
   *
   * @param dbOptions {@link org.rocksdb.DBOptions} instance
   * @param columnFamilyOptions {@link org.rocksdb.ColumnFamilyOptions}
   *     instance
   */
  public Options(final DBOptions dbOptions,
      final ColumnFamilyOptions columnFamilyOptions) {
    super(newOptions(dbOptions.nativeHandle_,
        columnFamilyOptions.nativeHandle_));
    env_ = Env.getDefault();
  }

  /**
   * Copy constructor for ColumnFamilyOptions.
   *
   * NOTE: This does a shallow copy, which means comparator, merge_operator
   * and other pointers will be cloned!
   *
   * @param other The Options to copy.
   */
  public Options(Options other) {
    super(copyOptions(other.nativeHandle_));
    this.env_ = other.env_;
    this.memTableConfig_ = other.memTableConfig_;
    this.tableFormatConfig_ = other.tableFormatConfig_;
    this.rateLimiter_ = other.rateLimiter_;
    this.comparator_ = other.comparator_;
    this.compactionFilter_ = other.compactionFilter_;
    this.compactionFilterFactory_ = other.compactionFilterFactory_;
    this.compactionOptionsUniversal_ = other.compactionOptionsUniversal_;
    this.compactionOptionsFIFO_ = other.compactionOptionsFIFO_;
    this.compressionOptions_ = other.compressionOptions_;
    this.rowCache_ = other.rowCache_;
    this.writeBufferManager_ = other.writeBufferManager_;
  }

  @Override
  public Options setIncreaseParallelism(final int totalThreads) {
    assert(isOwningHandle());
    setIncreaseParallelism(nativeHandle_, totalThreads);
    return this;
  }

  @Override
  public Options setCreateIfMissing(final boolean flag) {
    assert(isOwningHandle());
    setCreateIfMissing(nativeHandle_, flag);
    return this;
  }

  @Override
  public Options setCreateMissingColumnFamilies(final boolean flag) {
    assert(isOwningHandle());
    setCreateMissingColumnFamilies(nativeHandle_, flag);
    return this;
  }

  @Override
  public Options setEnv(final Env env) {
    assert(isOwningHandle());
    setEnv(nativeHandle_, env.nativeHandle_);
    env_ = env;
    return this;
  }

  @Override
  public Env getEnv() {
    return env_;
  }

  /**
   * <p>Set appropriate parameters for bulk loading.
   * The reason that this is a function that returns "this" instead of a
   * constructor is to enable chaining of multiple similar calls in the future.
   * </p>
   *
   * <p>All data will be in level 0 without any automatic compaction.
   * It's recommended to manually call CompactRange(NULL, NULL) before reading
   * from the database, because otherwise the read can be very slow.</p>
   *
   * @return the instance of the current Options.
   */
  public Options prepareForBulkLoad() {
    prepareForBulkLoad(nativeHandle_);
    return this;
  }

  @Override
  public boolean createIfMissing() {
    assert(isOwningHandle());
    return createIfMissing(nativeHandle_);
  }

  @Override
  public boolean createMissingColumnFamilies() {
    assert(isOwningHandle());
    return createMissingColumnFamilies(nativeHandle_);
  }

  @Override
  public Options optimizeForSmallDb() {
    optimizeForSmallDb(nativeHandle_);
    return this;
  }

  @Override
  public Options optimizeForPointLookup(
      long blockCacheSizeMb) {
    optimizeForPointLookup(nativeHandle_,
        blockCacheSizeMb);
    return this;
  }

  @Override
  public Options optimizeLevelStyleCompaction() {
    optimizeLevelStyleCompaction(nativeHandle_,
        DEFAULT_COMPACTION_MEMTABLE_MEMORY_BUDGET);
    return this;
  }

  @Override
  public Options optimizeLevelStyleCompaction(
      long memtableMemoryBudget) {
    optimizeLevelStyleCompaction(nativeHandle_,
        memtableMemoryBudget);
    return this;
  }

  @Override
  public Options optimizeUniversalStyleCompaction() {
    optimizeUniversalStyleCompaction(nativeHandle_,
        DEFAULT_COMPACTION_MEMTABLE_MEMORY_BUDGET);
    return this;
  }

  @Override
  public Options optimizeUniversalStyleCompaction(
      final long memtableMemoryBudget) {
    optimizeUniversalStyleCompaction(nativeHandle_,
        memtableMemoryBudget);
    return this;
  }

  @Override
  public Options setComparator(final BuiltinComparator builtinComparator) {
    assert(isOwningHandle());
    setComparatorHandle(nativeHandle_, builtinComparator.ordinal());
    return this;
  }

  @Override
  public Options setComparator(
      final AbstractComparator<? extends AbstractSlice<?>> comparator) {
    assert(isOwningHandle());
    setComparatorHandle(nativeHandle_, comparator.nativeHandle_,
            comparator.getComparatorType().getValue());
    comparator_ = comparator;
    return this;
  }

  @Override
  public Options setMergeOperatorName(final String name) {
    assert(isOwningHandle());
    if (name == null) {
      throw new IllegalArgumentException(
          "Merge operator name must not be null.");
    }
    setMergeOperatorName(nativeHandle_, name);
    return this;
  }

  @Override
  public Options setMergeOperator(final MergeOperator mergeOperator) {
    setMergeOperator(nativeHandle_, mergeOperator.nativeHandle_);
    return this;
  }

  @Override
  public Options setCompactionFilter(
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
  public Options setCompactionFilterFactory(final AbstractCompactionFilterFactory<? extends AbstractCompactionFilter<?>> compactionFilterFactory) {
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
  public Options setWriteBufferSize(final long writeBufferSize) {
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
  public Options setMaxWriteBufferNumber(final int maxWriteBufferNumber) {
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
  public boolean errorIfExists() {
    assert(isOwningHandle());
    return errorIfExists(nativeHandle_);
  }

  @Override
  public Options setErrorIfExists(final boolean errorIfExists) {
    assert(isOwningHandle());
    setErrorIfExists(nativeHandle_, errorIfExists);
    return this;
  }

  @Override
  public boolean paranoidChecks() {
    assert(isOwningHandle());
    return paranoidChecks(nativeHandle_);
  }

  @Override
  public Options setParanoidChecks(final boolean paranoidChecks) {
    assert(isOwningHandle());
    setParanoidChecks(nativeHandle_, paranoidChecks);
    return this;
  }

  @Override
  public int maxOpenFiles() {
    assert(isOwningHandle());
    return maxOpenFiles(nativeHandle_);
  }

  @Override
  public Options setMaxFileOpeningThreads(final int maxFileOpeningThreads) {
    assert(isOwningHandle());
    setMaxFileOpeningThreads(nativeHandle_, maxFileOpeningThreads);
    return this;
  }

  @Override
  public int maxFileOpeningThreads() {
    assert(isOwningHandle());
    return maxFileOpeningThreads(nativeHandle_);
  }

  @Override
  public Options setMaxTotalWalSize(final long maxTotalWalSize) {
    assert(isOwningHandle());
    setMaxTotalWalSize(nativeHandle_, maxTotalWalSize);
    return this;
  }

  @Override
  public long maxTotalWalSize() {
    assert(isOwningHandle());
    return maxTotalWalSize(nativeHandle_);
  }

  @Override
  public Options setMaxOpenFiles(final int maxOpenFiles) {
    assert(isOwningHandle());
    setMaxOpenFiles(nativeHandle_, maxOpenFiles);
    return this;
  }

  @Override
  public boolean useFsync() {
    assert(isOwningHandle());
    return useFsync(nativeHandle_);
  }

  @Override
  public Options setUseFsync(final boolean useFsync) {
    assert(isOwningHandle());
    setUseFsync(nativeHandle_, useFsync);
    return this;
  }

  @Override
  public Options setDbPaths(final Collection<DbPath> dbPaths) {
    assert(isOwningHandle());

    final int len = dbPaths.size();
    final String paths[] = new String[len];
    final long targetSizes[] = new long[len];

    int i = 0;
    for(final DbPath dbPath : dbPaths) {
      paths[i] = dbPath.path.toString();
      targetSizes[i] = dbPath.targetSize;
      i++;
    }
    setDbPaths(nativeHandle_, paths, targetSizes);
    return this;
  }

  @Override
  public List<DbPath> dbPaths() {
    final int len = (int)dbPathsLen(nativeHandle_);
    if(len == 0) {
      return Collections.emptyList();
    } else {
      final String paths[] = new String[len];
      final long targetSizes[] = new long[len];

      dbPaths(nativeHandle_, paths, targetSizes);

      final List<DbPath> dbPaths = new ArrayList<>();
      for(int i = 0; i < len; i++) {
        dbPaths.add(new DbPath(Paths.get(paths[i]), targetSizes[i]));
      }
      return dbPaths;
    }
  }

  @Override
  public String dbLogDir() {
    assert(isOwningHandle());
    return dbLogDir(nativeHandle_);
  }

  @Override
  public Options setDbLogDir(final String dbLogDir) {
    assert(isOwningHandle());
    setDbLogDir(nativeHandle_, dbLogDir);
    return this;
  }

  @Override
  public String walDir() {
    assert(isOwningHandle());
    return walDir(nativeHandle_);
  }

  @Override
  public Options setWalDir(final String walDir) {
    assert(isOwningHandle());
    setWalDir(nativeHandle_, walDir);
    return this;
  }

  @Override
  public long deleteObsoleteFilesPeriodMicros() {
    assert(isOwningHandle());
    return deleteObsoleteFilesPeriodMicros(nativeHandle_);
  }

  @Override
  public Options setDeleteObsoleteFilesPeriodMicros(
      final long micros) {
    assert(isOwningHandle());
    setDeleteObsoleteFilesPeriodMicros(nativeHandle_, micros);
    return this;
  }

  @Override
  public int maxBackgroundCompactions() {
    assert(isOwningHandle());
    return maxBackgroundCompactions(nativeHandle_);
  }

  @Override
  public Options setStatistics(final Statistics statistics) {
    assert(isOwningHandle());
    setStatistics(nativeHandle_, statistics.nativeHandle_);
    return this;
  }

  @Override
  public Statistics statistics() {
    assert(isOwningHandle());
    final long statisticsNativeHandle = statistics(nativeHandle_);
    if(statisticsNativeHandle == 0) {
      return null;
    } else {
      return new Statistics(statisticsNativeHandle);
    }
  }

  @Override
  public void setBaseBackgroundCompactions(
      final int baseBackgroundCompactions) {
    assert(isOwningHandle());
    setBaseBackgroundCompactions(nativeHandle_, baseBackgroundCompactions);
  }

  @Override
  public int baseBackgroundCompactions() {
    assert(isOwningHandle());
    return baseBackgroundCompactions(nativeHandle_);
  }

  @Override
  public Options setMaxBackgroundCompactions(
      final int maxBackgroundCompactions) {
    assert(isOwningHandle());
    setMaxBackgroundCompactions(nativeHandle_, maxBackgroundCompactions);
    return this;
  }

  @Override
  public Options setMaxSubcompactions(final int maxSubcompactions) {
    assert(isOwningHandle());
    setMaxSubcompactions(nativeHandle_, maxSubcompactions);
    return this;
  }

  @Override
  public int maxSubcompactions() {
    assert(isOwningHandle());
    return maxSubcompactions(nativeHandle_);
  }

  @Override
  public int maxBackgroundFlushes() {
    assert(isOwningHandle());
    return maxBackgroundFlushes(nativeHandle_);
  }

  @Override
  public Options setMaxBackgroundFlushes(
      final int maxBackgroundFlushes) {
    assert(isOwningHandle());
    setMaxBackgroundFlushes(nativeHandle_, maxBackgroundFlushes);
    return this;
  }

  @Override
  public int maxBackgroundJobs() {
    assert(isOwningHandle());
    return maxBackgroundJobs(nativeHandle_);
  }

  @Override
  public Options setMaxBackgroundJobs(final int maxBackgroundJobs) {
    assert(isOwningHandle());
    setMaxBackgroundJobs(nativeHandle_, maxBackgroundJobs);
    return this;
  }

  @Override
  public long maxLogFileSize() {
    assert(isOwningHandle());
    return maxLogFileSize(nativeHandle_);
  }

  @Override
  public Options setMaxLogFileSize(final long maxLogFileSize) {
    assert(isOwningHandle());
    setMaxLogFileSize(nativeHandle_, maxLogFileSize);
    return this;
  }

  @Override
  public long logFileTimeToRoll() {
    assert(isOwningHandle());
    return logFileTimeToRoll(nativeHandle_);
  }

  @Override
  public Options setLogFileTimeToRoll(final long logFileTimeToRoll) {
    assert(isOwningHandle());
    setLogFileTimeToRoll(nativeHandle_, logFileTimeToRoll);
    return this;
  }

  @Override
  public long keepLogFileNum() {
    assert(isOwningHandle());
    return keepLogFileNum(nativeHandle_);
  }

  @Override
  public Options setKeepLogFileNum(final long keepLogFileNum) {
    assert(isOwningHandle());
    setKeepLogFileNum(nativeHandle_, keepLogFileNum);
    return this;
  }


  @Override
  public Options setRecycleLogFileNum(final long recycleLogFileNum) {
    assert(isOwningHandle());
    setRecycleLogFileNum(nativeHandle_, recycleLogFileNum);
    return this;
  }

  @Override
  public long recycleLogFileNum() {
    assert(isOwningHandle());
    return recycleLogFileNum(nativeHandle_);
  }

  @Override
  public long maxManifestFileSize() {
    assert(isOwningHandle());
    return maxManifestFileSize(nativeHandle_);
  }

  @Override
  public Options setMaxManifestFileSize(
      final long maxManifestFileSize) {
    assert(isOwningHandle());
    setMaxManifestFileSize(nativeHandle_, maxManifestFileSize);
    return this;
  }

  @Override
  public Options setMaxTableFilesSizeFIFO(
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
  public int tableCacheNumshardbits() {
    assert(isOwningHandle());
    return tableCacheNumshardbits(nativeHandle_);
  }

  @Override
  public Options setTableCacheNumshardbits(
      final int tableCacheNumshardbits) {
    assert(isOwningHandle());
    setTableCacheNumshardbits(nativeHandle_, tableCacheNumshardbits);
    return this;
  }

  @Override
  public long walTtlSeconds() {
    assert(isOwningHandle());
    return walTtlSeconds(nativeHandle_);
  }

  @Override
  public Options setWalTtlSeconds(final long walTtlSeconds) {
    assert(isOwningHandle());
    setWalTtlSeconds(nativeHandle_, walTtlSeconds);
    return this;
  }

  @Override
  public long walSizeLimitMB() {
    assert(isOwningHandle());
    return walSizeLimitMB(nativeHandle_);
  }

  @Override
  public Options setWalSizeLimitMB(final long sizeLimitMB) {
    assert(isOwningHandle());
    setWalSizeLimitMB(nativeHandle_, sizeLimitMB);
    return this;
  }

  @Override
  public long manifestPreallocationSize() {
    assert(isOwningHandle());
    return manifestPreallocationSize(nativeHandle_);
  }

  @Override
  public Options setManifestPreallocationSize(final long size) {
    assert(isOwningHandle());
    setManifestPreallocationSize(nativeHandle_, size);
    return this;
  }

  @Override
  public Options setUseDirectReads(final boolean useDirectReads) {
    assert(isOwningHandle());
    setUseDirectReads(nativeHandle_, useDirectReads);
    return this;
  }

  @Override
  public boolean useDirectReads() {
    assert(isOwningHandle());
    return useDirectReads(nativeHandle_);
  }

  @Override
  public Options setUseDirectIoForFlushAndCompaction(
      final boolean useDirectIoForFlushAndCompaction) {
    assert(isOwningHandle());
    setUseDirectIoForFlushAndCompaction(nativeHandle_, useDirectIoForFlushAndCompaction);
    return this;
  }

  @Override
  public boolean useDirectIoForFlushAndCompaction() {
    assert(isOwningHandle());
    return useDirectIoForFlushAndCompaction(nativeHandle_);
  }

  @Override
  public Options setAllowFAllocate(final boolean allowFAllocate) {
    assert(isOwningHandle());
    setAllowFAllocate(nativeHandle_, allowFAllocate);
    return this;
  }

  @Override
  public boolean allowFAllocate() {
    assert(isOwningHandle());
    return allowFAllocate(nativeHandle_);
  }

  @Override
  public boolean allowMmapReads() {
    assert(isOwningHandle());
    return allowMmapReads(nativeHandle_);
  }

  @Override
  public Options setAllowMmapReads(final boolean allowMmapReads) {
    assert(isOwningHandle());
    setAllowMmapReads(nativeHandle_, allowMmapReads);
    return this;
  }

  @Override
  public boolean allowMmapWrites() {
    assert(isOwningHandle());
    return allowMmapWrites(nativeHandle_);
  }

  @Override
  public Options setAllowMmapWrites(final boolean allowMmapWrites) {
    assert(isOwningHandle());
    setAllowMmapWrites(nativeHandle_, allowMmapWrites);
    return this;
  }

  @Override
  public boolean isFdCloseOnExec() {
    assert(isOwningHandle());
    return isFdCloseOnExec(nativeHandle_);
  }

  @Override
  public Options setIsFdCloseOnExec(final boolean isFdCloseOnExec) {
    assert(isOwningHandle());
    setIsFdCloseOnExec(nativeHandle_, isFdCloseOnExec);
    return this;
  }

  @Override
  public int statsDumpPeriodSec() {
    assert(isOwningHandle());
    return statsDumpPeriodSec(nativeHandle_);
  }

  @Override
  public Options setStatsDumpPeriodSec(final int statsDumpPeriodSec) {
    assert(isOwningHandle());
    setStatsDumpPeriodSec(nativeHandle_, statsDumpPeriodSec);
    return this;
  }

  @Override
  public boolean adviseRandomOnOpen() {
    return adviseRandomOnOpen(nativeHandle_);
  }

  @Override
  public Options setAdviseRandomOnOpen(final boolean adviseRandomOnOpen) {
    assert(isOwningHandle());
    setAdviseRandomOnOpen(nativeHandle_, adviseRandomOnOpen);
    return this;
  }

  @Override
  public Options setDbWriteBufferSize(final long dbWriteBufferSize) {
    assert(isOwningHandle());
    setDbWriteBufferSize(nativeHandle_, dbWriteBufferSize);
    return this;
  }

  @Override
  public Options setWriteBufferManager(final WriteBufferManager writeBufferManager) {
    assert(isOwningHandle());
    setWriteBufferManager(nativeHandle_, writeBufferManager.nativeHandle_);
    this.writeBufferManager_ = writeBufferManager;
    return this;
  }

  @Override
  public WriteBufferManager writeBufferManager() {
    assert(isOwningHandle());
    return this.writeBufferManager_;
  }

    @Override
  public long dbWriteBufferSize() {
    assert(isOwningHandle());
    return dbWriteBufferSize(nativeHandle_);
  }

  @Override
  public Options setAccessHintOnCompactionStart(final AccessHint accessHint) {
    assert(isOwningHandle());
    setAccessHintOnCompactionStart(nativeHandle_, accessHint.getValue());
    return this;
  }

  @Override
  public AccessHint accessHintOnCompactionStart() {
    assert(isOwningHandle());
    return AccessHint.getAccessHint(accessHintOnCompactionStart(nativeHandle_));
  }

  @Override
  public Options setNewTableReaderForCompactionInputs(
      final boolean newTableReaderForCompactionInputs) {
    assert(isOwningHandle());
    setNewTableReaderForCompactionInputs(nativeHandle_,
        newTableReaderForCompactionInputs);
    return this;
  }

  @Override
  public boolean newTableReaderForCompactionInputs() {
    assert(isOwningHandle());
    return newTableReaderForCompactionInputs(nativeHandle_);
  }

  @Override
  public Options setCompactionReadaheadSize(final long compactionReadaheadSize) {
    assert(isOwningHandle());
    setCompactionReadaheadSize(nativeHandle_, compactionReadaheadSize);
    return this;
  }

  @Override
  public long compactionReadaheadSize() {
    assert(isOwningHandle());
    return compactionReadaheadSize(nativeHandle_);
  }

  @Override
  public Options setRandomAccessMaxBufferSize(final long randomAccessMaxBufferSize) {
    assert(isOwningHandle());
    setRandomAccessMaxBufferSize(nativeHandle_, randomAccessMaxBufferSize);
    return this;
  }

  @Override
  public long randomAccessMaxBufferSize() {
    assert(isOwningHandle());
    return randomAccessMaxBufferSize(nativeHandle_);
  }

  @Override
  public Options setWritableFileMaxBufferSize(final long writableFileMaxBufferSize) {
    assert(isOwningHandle());
    setWritableFileMaxBufferSize(nativeHandle_, writableFileMaxBufferSize);
    return this;
  }

  @Override
  public long writableFileMaxBufferSize() {
    assert(isOwningHandle());
    return writableFileMaxBufferSize(nativeHandle_);
  }

  @Override
  public boolean useAdaptiveMutex() {
    assert(isOwningHandle());
    return useAdaptiveMutex(nativeHandle_);
  }

  @Override
  public Options setUseAdaptiveMutex(final boolean useAdaptiveMutex) {
    assert(isOwningHandle());
    setUseAdaptiveMutex(nativeHandle_, useAdaptiveMutex);
    return this;
  }

  @Override
  public long bytesPerSync() {
    return bytesPerSync(nativeHandle_);
  }

  @Override
  public Options setBytesPerSync(final long bytesPerSync) {
    assert(isOwningHandle());
    setBytesPerSync(nativeHandle_, bytesPerSync);
    return this;
  }

  @Override
  public Options setWalBytesPerSync(final long walBytesPerSync) {
    assert(isOwningHandle());
    setWalBytesPerSync(nativeHandle_, walBytesPerSync);
    return this;
  }

  @Override
  public long walBytesPerSync() {
    assert(isOwningHandle());
    return walBytesPerSync(nativeHandle_);
  }

  @Override
  public Options setEnableThreadTracking(final boolean enableThreadTracking) {
    assert(isOwningHandle());
    setEnableThreadTracking(nativeHandle_, enableThreadTracking);
    return this;
  }

  @Override
  public boolean enableThreadTracking() {
    assert(isOwningHandle());
    return enableThreadTracking(nativeHandle_);
  }

  @Override
  public Options setDelayedWriteRate(final long delayedWriteRate) {
    assert(isOwningHandle());
    setDelayedWriteRate(nativeHandle_, delayedWriteRate);
    return this;
  }

  @Override
  public long delayedWriteRate(){
    return delayedWriteRate(nativeHandle_);
  }

  @Override
  public Options setEnablePipelinedWrite(final boolean enablePipelinedWrite) {
    setEnablePipelinedWrite(nativeHandle_, enablePipelinedWrite);
    return this;
  }

  @Override
  public boolean enablePipelinedWrite() {
    return enablePipelinedWrite(nativeHandle_);
  }

  @Override
  public Options setAllowConcurrentMemtableWrite(
      final boolean allowConcurrentMemtableWrite) {
    setAllowConcurrentMemtableWrite(nativeHandle_,
        allowConcurrentMemtableWrite);
    return this;
  }

  @Override
  public boolean allowConcurrentMemtableWrite() {
    return allowConcurrentMemtableWrite(nativeHandle_);
  }

  @Override
  public Options setEnableWriteThreadAdaptiveYield(
      final boolean enableWriteThreadAdaptiveYield) {
    setEnableWriteThreadAdaptiveYield(nativeHandle_,
        enableWriteThreadAdaptiveYield);
    return this;
  }

  @Override
  public boolean enableWriteThreadAdaptiveYield() {
    return enableWriteThreadAdaptiveYield(nativeHandle_);
  }

  @Override
  public Options setWriteThreadMaxYieldUsec(final long writeThreadMaxYieldUsec) {
    setWriteThreadMaxYieldUsec(nativeHandle_, writeThreadMaxYieldUsec);
    return this;
  }

  @Override
  public long writeThreadMaxYieldUsec() {
    return writeThreadMaxYieldUsec(nativeHandle_);
  }

  @Override
  public Options setWriteThreadSlowYieldUsec(final long writeThreadSlowYieldUsec) {
    setWriteThreadSlowYieldUsec(nativeHandle_, writeThreadSlowYieldUsec);
    return this;
  }

  @Override
  public long writeThreadSlowYieldUsec() {
    return writeThreadSlowYieldUsec(nativeHandle_);
  }

  @Override
  public Options setSkipStatsUpdateOnDbOpen(final boolean skipStatsUpdateOnDbOpen) {
    assert(isOwningHandle());
    setSkipStatsUpdateOnDbOpen(nativeHandle_, skipStatsUpdateOnDbOpen);
    return this;
  }

  @Override
  public boolean skipStatsUpdateOnDbOpen() {
    assert(isOwningHandle());
    return skipStatsUpdateOnDbOpen(nativeHandle_);
  }

  @Override
  public Options setWalRecoveryMode(final WALRecoveryMode walRecoveryMode) {
    assert(isOwningHandle());
    setWalRecoveryMode(nativeHandle_, walRecoveryMode.getValue());
    return this;
  }

  @Override
  public WALRecoveryMode walRecoveryMode() {
    assert(isOwningHandle());
    return WALRecoveryMode.getWALRecoveryMode(walRecoveryMode(nativeHandle_));
  }

  @Override
  public Options setAllow2pc(final boolean allow2pc) {
    assert(isOwningHandle());
    setAllow2pc(nativeHandle_, allow2pc);
    return this;
  }

  @Override
  public boolean allow2pc() {
    assert(isOwningHandle());
    return allow2pc(nativeHandle_);
  }

  @Override
  public Options setRowCache(final Cache rowCache) {
    assert(isOwningHandle());
    setRowCache(nativeHandle_, rowCache.nativeHandle_);
    this.rowCache_ = rowCache;
    return this;
  }

  @Override
  public Cache rowCache() {
    assert(isOwningHandle());
    return this.rowCache_;
  }

  @Override
  public Options setWalFilter(final AbstractWalFilter walFilter) {
    assert(isOwningHandle());
    setWalFilter(nativeHandle_, walFilter.nativeHandle_);
    this.walFilter_ = walFilter;
    return this;
  }

  @Override
  public WalFilter walFilter() {
    assert(isOwningHandle());
    return this.walFilter_;
  }

  @Override
  public Options setFailIfOptionsFileError(final boolean failIfOptionsFileError) {
    assert(isOwningHandle());
    setFailIfOptionsFileError(nativeHandle_, failIfOptionsFileError);
    return this;
  }

  @Override
  public boolean failIfOptionsFileError() {
    assert(isOwningHandle());
    return failIfOptionsFileError(nativeHandle_);
  }

  @Override
  public Options setDumpMallocStats(final boolean dumpMallocStats) {
    assert(isOwningHandle());
    setDumpMallocStats(nativeHandle_, dumpMallocStats);
    return this;
  }

  @Override
  public boolean dumpMallocStats() {
    assert(isOwningHandle());
    return dumpMallocStats(nativeHandle_);
  }

  @Override
  public Options setAvoidFlushDuringRecovery(final boolean avoidFlushDuringRecovery) {
    assert(isOwningHandle());
    setAvoidFlushDuringRecovery(nativeHandle_, avoidFlushDuringRecovery);
    return this;
  }

  @Override
  public boolean avoidFlushDuringRecovery() {
    assert(isOwningHandle());
    return avoidFlushDuringRecovery(nativeHandle_);
  }

  @Override
  public Options setAvoidFlushDuringShutdown(final boolean avoidFlushDuringShutdown) {
    assert(isOwningHandle());
    setAvoidFlushDuringShutdown(nativeHandle_, avoidFlushDuringShutdown);
    return this;
  }

  @Override
  public boolean avoidFlushDuringShutdown() {
    assert(isOwningHandle());
    return avoidFlushDuringShutdown(nativeHandle_);
  }

  @Override
  public Options setAllowIngestBehind(final boolean allowIngestBehind) {
    assert(isOwningHandle());
    setAllowIngestBehind(nativeHandle_, allowIngestBehind);
    return this;
  }

  @Override
  public boolean allowIngestBehind() {
    assert(isOwningHandle());
    return allowIngestBehind(nativeHandle_);
  }

  @Override
  public Options setPreserveDeletes(final boolean preserveDeletes) {
    assert(isOwningHandle());
    setPreserveDeletes(nativeHandle_, preserveDeletes);
    return this;
  }

  @Override
  public boolean preserveDeletes() {
    assert(isOwningHandle());
    return preserveDeletes(nativeHandle_);
  }

  @Override
  public Options setTwoWriteQueues(final boolean twoWriteQueues) {
    assert(isOwningHandle());
    setTwoWriteQueues(nativeHandle_, twoWriteQueues);
    return this;
  }

  @Override
  public boolean twoWriteQueues() {
    assert(isOwningHandle());
    return twoWriteQueues(nativeHandle_);
  }

  @Override
  public Options setManualWalFlush(final boolean manualWalFlush) {
    assert(isOwningHandle());
    setManualWalFlush(nativeHandle_, manualWalFlush);
    return this;
  }

  @Override
  public boolean manualWalFlush() {
    assert(isOwningHandle());
    return manualWalFlush(nativeHandle_);
  }

  @Override
  public MemTableConfig memTableConfig() {
    return this.memTableConfig_;
  }

  @Override
  public Options setMemTableConfig(final MemTableConfig config) {
    memTableConfig_ = config;
    setMemTableFactory(nativeHandle_, config.newMemTableFactoryHandle());
    return this;
  }

  @Override
  public Options setRateLimiter(final RateLimiter rateLimiter) {
    assert(isOwningHandle());
    rateLimiter_ = rateLimiter;
    setRateLimiter(nativeHandle_, rateLimiter.nativeHandle_);
    return this;
  }

  @Override
  public Options setSstFileManager(final SstFileManager sstFileManager) {
    assert(isOwningHandle());
    setSstFileManager(nativeHandle_, sstFileManager.nativeHandle_);
    return this;
  }

  @Override
  public Options setLogger(final Logger logger) {
    assert(isOwningHandle());
    setLogger(nativeHandle_, logger.nativeHandle_);
    return this;
  }

  @Override
  public Options setInfoLogLevel(final InfoLogLevel infoLogLevel) {
    assert(isOwningHandle());
    setInfoLogLevel(nativeHandle_, infoLogLevel.getValue());
    return this;
  }

  @Override
  public InfoLogLevel infoLogLevel() {
    assert(isOwningHandle());
    return InfoLogLevel.getInfoLogLevel(
        infoLogLevel(nativeHandle_));
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
  public Options setTableFormatConfig(final TableFormatConfig config) {
    tableFormatConfig_ = config;
    setTableFactory(nativeHandle_, config.newTableFactoryHandle());
    return this;
  }

  @Override
  public String tableFactoryName() {
    assert(isOwningHandle());
    return tableFactoryName(nativeHandle_);
  }

  @Override
  public Options useFixedLengthPrefixExtractor(final int n) {
    assert(isOwningHandle());
    useFixedLengthPrefixExtractor(nativeHandle_, n);
    return this;
  }

  @Override
  public Options useCappedPrefixExtractor(final int n) {
    assert(isOwningHandle());
    useCappedPrefixExtractor(nativeHandle_, n);
    return this;
  }

  @Override
  public CompressionType compressionType() {
    return CompressionType.getCompressionType(compressionType(nativeHandle_));
  }

  @Override
  public Options setCompressionPerLevel(
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
  public Options setCompressionType(CompressionType compressionType) {
    setCompressionType(nativeHandle_, compressionType.getValue());
    return this;
  }


  @Override
  public Options setBottommostCompressionType(
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
  public Options setBottommostCompressionOptions(
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
  public Options setCompressionOptions(
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
  public CompactionStyle compactionStyle() {
    return CompactionStyle.fromValue(compactionStyle(nativeHandle_));
  }

  @Override
  public Options setCompactionStyle(
      final CompactionStyle compactionStyle) {
    setCompactionStyle(nativeHandle_, compactionStyle.getValue());
    return this;
  }

  @Override
  public int numLevels() {
    return numLevels(nativeHandle_);
  }

  @Override
  public Options setNumLevels(int numLevels) {
    setNumLevels(nativeHandle_, numLevels);
    return this;
  }

  @Override
  public int levelZeroFileNumCompactionTrigger() {
    return levelZeroFileNumCompactionTrigger(nativeHandle_);
  }

  @Override
  public Options setLevelZeroFileNumCompactionTrigger(
      final int numFiles) {
    setLevelZeroFileNumCompactionTrigger(
        nativeHandle_, numFiles);
    return this;
  }

  @Override
  public int levelZeroSlowdownWritesTrigger() {
    return levelZeroSlowdownWritesTrigger(nativeHandle_);
  }

  @Override
  public Options setLevelZeroSlowdownWritesTrigger(
      final int numFiles) {
    setLevelZeroSlowdownWritesTrigger(nativeHandle_, numFiles);
    return this;
  }

  @Override
  public int levelZeroStopWritesTrigger() {
    return levelZeroStopWritesTrigger(nativeHandle_);
  }

  @Override
  public Options setLevelZeroStopWritesTrigger(
      final int numFiles) {
    setLevelZeroStopWritesTrigger(nativeHandle_, numFiles);
    return this;
  }

  @Override
  public long targetFileSizeBase() {
    return targetFileSizeBase(nativeHandle_);
  }

  @Override
  public Options setTargetFileSizeBase(long targetFileSizeBase) {
    setTargetFileSizeBase(nativeHandle_, targetFileSizeBase);
    return this;
  }

  @Override
  public int targetFileSizeMultiplier() {
    return targetFileSizeMultiplier(nativeHandle_);
  }

  @Override
  public Options setTargetFileSizeMultiplier(int multiplier) {
    setTargetFileSizeMultiplier(nativeHandle_, multiplier);
    return this;
  }

  @Override
  public Options setMaxBytesForLevelBase(final long maxBytesForLevelBase) {
    setMaxBytesForLevelBase(nativeHandle_, maxBytesForLevelBase);
    return this;
  }

  @Override
  public long maxBytesForLevelBase() {
    return maxBytesForLevelBase(nativeHandle_);
  }

  @Override
  public Options setLevelCompactionDynamicLevelBytes(
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
  public double maxBytesForLevelMultiplier() {
    return maxBytesForLevelMultiplier(nativeHandle_);
  }

  @Override
  public Options setMaxBytesForLevelMultiplier(final double multiplier) {
    setMaxBytesForLevelMultiplier(nativeHandle_, multiplier);
    return this;
  }

  @Override
  public long maxCompactionBytes() {
    return maxCompactionBytes(nativeHandle_);
  }

  @Override
  public Options setMaxCompactionBytes(final long maxCompactionBytes) {
    setMaxCompactionBytes(nativeHandle_, maxCompactionBytes);
    return this;
  }

  @Override
  public long arenaBlockSize() {
    return arenaBlockSize(nativeHandle_);
  }

  @Override
  public Options setArenaBlockSize(final long arenaBlockSize) {
    setArenaBlockSize(nativeHandle_, arenaBlockSize);
    return this;
  }

  @Override
  public boolean disableAutoCompactions() {
    return disableAutoCompactions(nativeHandle_);
  }

  @Override
  public Options setDisableAutoCompactions(
      final boolean disableAutoCompactions) {
    setDisableAutoCompactions(nativeHandle_, disableAutoCompactions);
    return this;
  }

  @Override
  public long maxSequentialSkipInIterations() {
    return maxSequentialSkipInIterations(nativeHandle_);
  }

  @Override
  public Options setMaxSequentialSkipInIterations(
      final long maxSequentialSkipInIterations) {
    setMaxSequentialSkipInIterations(nativeHandle_,
        maxSequentialSkipInIterations);
    return this;
  }

  @Override
  public boolean inplaceUpdateSupport() {
    return inplaceUpdateSupport(nativeHandle_);
  }

  @Override
  public Options setInplaceUpdateSupport(
      final boolean inplaceUpdateSupport) {
    setInplaceUpdateSupport(nativeHandle_, inplaceUpdateSupport);
    return this;
  }

  @Override
  public long inplaceUpdateNumLocks() {
    return inplaceUpdateNumLocks(nativeHandle_);
  }

  @Override
  public Options setInplaceUpdateNumLocks(
      final long inplaceUpdateNumLocks) {
    setInplaceUpdateNumLocks(nativeHandle_, inplaceUpdateNumLocks);
    return this;
  }

  @Override
  public double memtablePrefixBloomSizeRatio() {
    return memtablePrefixBloomSizeRatio(nativeHandle_);
  }

  @Override
  public Options setMemtablePrefixBloomSizeRatio(final double memtablePrefixBloomSizeRatio) {
    setMemtablePrefixBloomSizeRatio(nativeHandle_, memtablePrefixBloomSizeRatio);
    return this;
  }

  @Override
  public int bloomLocality() {
    return bloomLocality(nativeHandle_);
  }

  @Override
  public Options setBloomLocality(final int bloomLocality) {
    setBloomLocality(nativeHandle_, bloomLocality);
    return this;
  }

  @Override
  public long maxSuccessiveMerges() {
    return maxSuccessiveMerges(nativeHandle_);
  }

  @Override
  public Options setMaxSuccessiveMerges(long maxSuccessiveMerges) {
    setMaxSuccessiveMerges(nativeHandle_, maxSuccessiveMerges);
    return this;
  }

  @Override
  public int minWriteBufferNumberToMerge() {
    return minWriteBufferNumberToMerge(nativeHandle_);
  }

  @Override
  public Options setMinWriteBufferNumberToMerge(
      final int minWriteBufferNumberToMerge) {
    setMinWriteBufferNumberToMerge(nativeHandle_, minWriteBufferNumberToMerge);
    return this;
  }

  @Override
  public Options setOptimizeFiltersForHits(
      final boolean optimizeFiltersForHits) {
    setOptimizeFiltersForHits(nativeHandle_, optimizeFiltersForHits);
    return this;
  }

  @Override
  public boolean optimizeFiltersForHits() {
    return optimizeFiltersForHits(nativeHandle_);
  }

  @Override
  public Options
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
  public Options setSoftPendingCompactionBytesLimit(long softPendingCompactionBytesLimit) {
    setSoftPendingCompactionBytesLimit(nativeHandle_,
        softPendingCompactionBytesLimit);
    return this;
  }

  @Override
  public long softPendingCompactionBytesLimit() {
    return softPendingCompactionBytesLimit(nativeHandle_);
  }

  @Override
  public Options setHardPendingCompactionBytesLimit(long hardPendingCompactionBytesLimit) {
    setHardPendingCompactionBytesLimit(nativeHandle_, hardPendingCompactionBytesLimit);
    return this;
  }

  @Override
  public long hardPendingCompactionBytesLimit() {
    return hardPendingCompactionBytesLimit(nativeHandle_);
  }

  @Override
  public Options setLevel0FileNumCompactionTrigger(int level0FileNumCompactionTrigger) {
    setLevel0FileNumCompactionTrigger(nativeHandle_, level0FileNumCompactionTrigger);
    return this;
  }

  @Override
  public int level0FileNumCompactionTrigger() {
    return level0FileNumCompactionTrigger(nativeHandle_);
  }

  @Override
  public Options setLevel0SlowdownWritesTrigger(int level0SlowdownWritesTrigger) {
    setLevel0SlowdownWritesTrigger(nativeHandle_, level0SlowdownWritesTrigger);
    return this;
  }

  @Override
  public int level0SlowdownWritesTrigger() {
    return level0SlowdownWritesTrigger(nativeHandle_);
  }

  @Override
  public Options setLevel0StopWritesTrigger(int level0StopWritesTrigger) {
    setLevel0StopWritesTrigger(nativeHandle_, level0StopWritesTrigger);
    return this;
  }

  @Override
  public int level0StopWritesTrigger() {
    return level0StopWritesTrigger(nativeHandle_);
  }

  @Override
  public Options setMaxBytesForLevelMultiplierAdditional(int[] maxBytesForLevelMultiplierAdditional) {
    setMaxBytesForLevelMultiplierAdditional(nativeHandle_, maxBytesForLevelMultiplierAdditional);
    return this;
  }

  @Override
  public int[] maxBytesForLevelMultiplierAdditional() {
    return maxBytesForLevelMultiplierAdditional(nativeHandle_);
  }

  @Override
  public Options setParanoidFileChecks(boolean paranoidFileChecks) {
    setParanoidFileChecks(nativeHandle_, paranoidFileChecks);
    return this;
  }

  @Override
  public boolean paranoidFileChecks() {
    return paranoidFileChecks(nativeHandle_);
  }

  @Override
  public Options setMaxWriteBufferNumberToMaintain(
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
  public Options setCompactionPriority(
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
  public Options setReportBgIoStats(final boolean reportBgIoStats) {
    setReportBgIoStats(nativeHandle_, reportBgIoStats);
    return this;
  }

  @Override
  public boolean reportBgIoStats() {
    return reportBgIoStats(nativeHandle_);
  }

  @Override
  public Options setTtl(final long ttl) {
    setTtl(nativeHandle_, ttl);
    return this;
  }

  @Override
  public long ttl() {
    return ttl(nativeHandle_);
  }

  @Override
  public Options setCompactionOptionsUniversal(
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
  public Options setCompactionOptionsFIFO(final CompactionOptionsFIFO compactionOptionsFIFO) {
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
  public Options setForceConsistencyChecks(final boolean forceConsistencyChecks) {
    setForceConsistencyChecks(nativeHandle_, forceConsistencyChecks);
    return this;
  }

  @Override
  public boolean forceConsistencyChecks() {
    return forceConsistencyChecks(nativeHandle_);
  }

  @Override
  public Options setAtomicFlush(final boolean atomicFlush) {
    setAtomicFlush(nativeHandle_, atomicFlush);
    return this;
  }

  @Override
  public boolean atomicFlush() {
    return atomicFlush(nativeHandle_);
  }

  private native static long newOptions();
  private native static long newOptions(long dbOptHandle,
      long cfOptHandle);
  private native static long copyOptions(long handle);
  @Override protected final native void disposeInternal(final long handle);
  private native void setEnv(long optHandle, long envHandle);
  private native void prepareForBulkLoad(long handle);

  // DB native handles
  private native void setIncreaseParallelism(long handle, int totalThreads);
  private native void setCreateIfMissing(long handle, boolean flag);
  private native boolean createIfMissing(long handle);
  private native void setCreateMissingColumnFamilies(
      long handle, boolean flag);
  private native boolean createMissingColumnFamilies(long handle);
  private native void setErrorIfExists(long handle, boolean errorIfExists);
  private native boolean errorIfExists(long handle);
  private native void setParanoidChecks(
      long handle, boolean paranoidChecks);
  private native boolean paranoidChecks(long handle);
  private native void setRateLimiter(long handle,
      long rateLimiterHandle);
  private native void setSstFileManager(final long handle,
      final long sstFileManagerHandle);
  private native void setLogger(long handle,
      long loggerHandle);
  private native void setInfoLogLevel(long handle, byte logLevel);
  private native byte infoLogLevel(long handle);
  private native void setMaxOpenFiles(long handle, int maxOpenFiles);
  private native int maxOpenFiles(long handle);
  private native void setMaxTotalWalSize(long handle,
      long maxTotalWalSize);
  private native void setMaxFileOpeningThreads(final long handle,
      final int maxFileOpeningThreads);
  private native int maxFileOpeningThreads(final long handle);
  private native long maxTotalWalSize(long handle);
  private native void setStatistics(final long handle, final long statisticsHandle);
  private native long statistics(final long handle);
  private native boolean useFsync(long handle);
  private native void setUseFsync(long handle, boolean useFsync);
  private native void setDbPaths(final long handle, final String[] paths,
      final long[] targetSizes);
  private native long dbPathsLen(final long handle);
  private native void dbPaths(final long handle, final String[] paths,
      final long[] targetSizes);
  private native void setDbLogDir(long handle, String dbLogDir);
  private native String dbLogDir(long handle);
  private native void setWalDir(long handle, String walDir);
  private native String walDir(long handle);
  private native void setDeleteObsoleteFilesPeriodMicros(
      long handle, long micros);
  private native long deleteObsoleteFilesPeriodMicros(long handle);
  private native void setBaseBackgroundCompactions(long handle,
      int baseBackgroundCompactions);
  private native int baseBackgroundCompactions(long handle);
  private native void setMaxBackgroundCompactions(
      long handle, int maxBackgroundCompactions);
  private native int maxBackgroundCompactions(long handle);
  private native void setMaxSubcompactions(long handle, int maxSubcompactions);
  private native int maxSubcompactions(long handle);
  private native void setMaxBackgroundFlushes(
      long handle, int maxBackgroundFlushes);
  private native int maxBackgroundFlushes(long handle);
  private native void setMaxBackgroundJobs(long handle, int maxMaxBackgroundJobs);
  private native int maxBackgroundJobs(long handle);
  private native void setMaxLogFileSize(long handle, long maxLogFileSize)
      throws IllegalArgumentException;
  private native long maxLogFileSize(long handle);
  private native void setLogFileTimeToRoll(
      long handle, long logFileTimeToRoll) throws IllegalArgumentException;
  private native long logFileTimeToRoll(long handle);
  private native void setKeepLogFileNum(long handle, long keepLogFileNum)
      throws IllegalArgumentException;
  private native long keepLogFileNum(long handle);
  private native void setRecycleLogFileNum(long handle, long recycleLogFileNum);
  private native long recycleLogFileNum(long handle);
  private native void setMaxManifestFileSize(
      long handle, long maxManifestFileSize);
  private native long maxManifestFileSize(long handle);
  private native void setMaxTableFilesSizeFIFO(
      long handle, long maxTableFilesSize);
  private native long maxTableFilesSizeFIFO(long handle);
  private native void setTableCacheNumshardbits(
      long handle, int tableCacheNumshardbits);
  private native int tableCacheNumshardbits(long handle);
  private native void setWalTtlSeconds(long handle, long walTtlSeconds);
  private native long walTtlSeconds(long handle);
  private native void setWalSizeLimitMB(long handle, long sizeLimitMB);
  private native long walSizeLimitMB(long handle);
  private native void setManifestPreallocationSize(
      long handle, long size) throws IllegalArgumentException;
  private native long manifestPreallocationSize(long handle);
  private native void setUseDirectReads(long handle, boolean useDirectReads);
  private native boolean useDirectReads(long handle);
  private native void setUseDirectIoForFlushAndCompaction(
      long handle, boolean useDirectIoForFlushAndCompaction);
  private native boolean useDirectIoForFlushAndCompaction(long handle);
  private native void setAllowFAllocate(final long handle,
      final boolean allowFAllocate);
  private native boolean allowFAllocate(final long handle);
  private native void setAllowMmapReads(
      long handle, boolean allowMmapReads);
  private native boolean allowMmapReads(long handle);
  private native void setAllowMmapWrites(
      long handle, boolean allowMmapWrites);
  private native boolean allowMmapWrites(long handle);
  private native void setIsFdCloseOnExec(
      long handle, boolean isFdCloseOnExec);
  private native boolean isFdCloseOnExec(long handle);
  private native void setStatsDumpPeriodSec(
      long handle, int statsDumpPeriodSec);
  private native int statsDumpPeriodSec(long handle);
  private native void setAdviseRandomOnOpen(
      long handle, boolean adviseRandomOnOpen);
  private native boolean adviseRandomOnOpen(long handle);
  private native void setDbWriteBufferSize(final long handle,
      final long dbWriteBufferSize);
  private native void setWriteBufferManager(final long handle,
      final long writeBufferManagerHandle);
  private native long dbWriteBufferSize(final long handle);
  private native void setAccessHintOnCompactionStart(final long handle,
      final byte accessHintOnCompactionStart);
  private native byte accessHintOnCompactionStart(final long handle);
  private native void setNewTableReaderForCompactionInputs(final long handle,
      final boolean newTableReaderForCompactionInputs);
  private native boolean newTableReaderForCompactionInputs(final long handle);
  private native void setCompactionReadaheadSize(final long handle,
      final long compactionReadaheadSize);
  private native long compactionReadaheadSize(final long handle);
  private native void setRandomAccessMaxBufferSize(final long handle,
      final long randomAccessMaxBufferSize);
  private native long randomAccessMaxBufferSize(final long handle);
  private native void setWritableFileMaxBufferSize(final long handle,
      final long writableFileMaxBufferSize);
  private native long writableFileMaxBufferSize(final long handle);
  private native void setUseAdaptiveMutex(
      long handle, boolean useAdaptiveMutex);
  private native boolean useAdaptiveMutex(long handle);
  private native void setBytesPerSync(
      long handle, long bytesPerSync);
  private native long bytesPerSync(long handle);
  private native void setWalBytesPerSync(long handle, long walBytesPerSync);
  private native long walBytesPerSync(long handle);
  private native void setEnableThreadTracking(long handle,
      boolean enableThreadTracking);
  private native boolean enableThreadTracking(long handle);
  private native void setDelayedWriteRate(long handle, long delayedWriteRate);
  private native long delayedWriteRate(long handle);
  private native void setEnablePipelinedWrite(final long handle,
      final boolean pipelinedWrite);
  private native boolean enablePipelinedWrite(final long handle);
  private native void setAllowConcurrentMemtableWrite(long handle,
      boolean allowConcurrentMemtableWrite);
  private native boolean allowConcurrentMemtableWrite(long handle);
  private native void setEnableWriteThreadAdaptiveYield(long handle,
      boolean enableWriteThreadAdaptiveYield);
  private native boolean enableWriteThreadAdaptiveYield(long handle);
  private native void setWriteThreadMaxYieldUsec(long handle,
      long writeThreadMaxYieldUsec);
  private native long writeThreadMaxYieldUsec(long handle);
  private native void setWriteThreadSlowYieldUsec(long handle,
      long writeThreadSlowYieldUsec);
  private native long writeThreadSlowYieldUsec(long handle);
  private native void setSkipStatsUpdateOnDbOpen(final long handle,
      final boolean skipStatsUpdateOnDbOpen);
  private native boolean skipStatsUpdateOnDbOpen(final long handle);
  private native void setWalRecoveryMode(final long handle,
      final byte walRecoveryMode);
  private native byte walRecoveryMode(final long handle);
  private native void setAllow2pc(final long handle,
      final boolean allow2pc);
  private native boolean allow2pc(final long handle);
  private native void setRowCache(final long handle,
      final long rowCacheHandle);
  private native void setWalFilter(final long handle,
      final long walFilterHandle);
  private native void setFailIfOptionsFileError(final long handle,
      final boolean failIfOptionsFileError);
  private native boolean failIfOptionsFileError(final long handle);
  private native void setDumpMallocStats(final long handle,
      final boolean dumpMallocStats);
  private native boolean dumpMallocStats(final long handle);
  private native void setAvoidFlushDuringRecovery(final long handle,
      final boolean avoidFlushDuringRecovery);
  private native boolean avoidFlushDuringRecovery(final long handle);
  private native void setAvoidFlushDuringShutdown(final long handle,
      final boolean avoidFlushDuringShutdown);
  private native boolean avoidFlushDuringShutdown(final long handle);
  private native void setAllowIngestBehind(final long handle,
      final boolean allowIngestBehind);
  private native boolean allowIngestBehind(final long handle);
  private native void setPreserveDeletes(final long handle,
      final boolean preserveDeletes);
  private native boolean preserveDeletes(final long handle);
  private native void setTwoWriteQueues(final long handle,
      final boolean twoWriteQueues);
  private native boolean twoWriteQueues(final long handle);
  private native void setManualWalFlush(final long handle,
      final boolean manualWalFlush);
  private native boolean manualWalFlush(final long handle);


  // CF native handles
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
  private native void setMergeOperatorName(
      long handle, String name);
  private native void setMergeOperator(
      long handle, long mergeOperatorHandle);
  private native void setCompactionFilterHandle(
          long handle, long compactionFilterHandle);
  private native void setCompactionFilterFactoryHandle(
          long handle, long compactionFilterFactoryHandle);
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
      long handle, long arenaBlockSize) throws IllegalArgumentException;
  private native long arenaBlockSize(long handle);
  private native void setDisableAutoCompactions(
      long handle, boolean disableAutoCompactions);
  private native boolean disableAutoCompactions(long handle);
  private native void setCompactionStyle(long handle, byte compactionStyle);
  private native byte compactionStyle(long handle);
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
  private native void setAtomicFlush(final long handle,
      final boolean atomicFlush);
  private native boolean atomicFlush(final long handle);

  // instance variables
  // NOTE: If you add new member variables, please update the copy constructor above!
  private Env env_;
  private MemTableConfig memTableConfig_;
  private TableFormatConfig tableFormatConfig_;
  private RateLimiter rateLimiter_;
  private AbstractComparator<? extends AbstractSlice<?>> comparator_;
  private AbstractCompactionFilter<? extends AbstractSlice<?>> compactionFilter_;
  private AbstractCompactionFilterFactory<? extends AbstractCompactionFilter<?>>
          compactionFilterFactory_;
  private CompactionOptionsUniversal compactionOptionsUniversal_;
  private CompactionOptionsFIFO compactionOptionsFIFO_;
  private CompressionOptions bottommostCompressionOptions_;
  private CompressionOptions compressionOptions_;
  private Cache rowCache_;
  private WalFilter walFilter_;
  private WriteBufferManager writeBufferManager_;
}
