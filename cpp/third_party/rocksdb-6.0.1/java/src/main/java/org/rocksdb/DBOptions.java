// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.nio.file.Paths;
import java.util.*;

/**
 * DBOptions to control the behavior of a database.  It will be used
 * during the creation of a {@link org.rocksdb.RocksDB} (i.e., RocksDB.open()).
 *
 * If {@link #dispose()} function is not called, then it will be GC'd
 * automatically and native resources will be released as part of the process.
 */
public class DBOptions extends RocksObject
    implements DBOptionsInterface<DBOptions>,
    MutableDBOptionsInterface<DBOptions> {
  static {
    RocksDB.loadLibrary();
  }

  /**
   * Construct DBOptions.
   *
   * This constructor will create (by allocating a block of memory)
   * an {@code rocksdb::DBOptions} in the c++ side.
   */
  public DBOptions() {
    super(newDBOptions());
    numShardBits_ = DEFAULT_NUM_SHARD_BITS;
  }

  /**
   * Copy constructor for DBOptions.
   *
   * NOTE: This does a shallow copy, which means env, rate_limiter, sst_file_manager,
   * info_log and other pointers will be cloned!
   *
   * @param other The DBOptions to copy.
   */
  public DBOptions(DBOptions other) {
    super(copyDBOptions(other.nativeHandle_));
    this.env_ = other.env_;
    this.numShardBits_ = other.numShardBits_;
    this.rateLimiter_ = other.rateLimiter_;
    this.rowCache_ = other.rowCache_;
    this.walFilter_ = other.walFilter_;
    this.writeBufferManager_ = other.writeBufferManager_;
  }

  /**
   * Constructor from Options
   *
   * @param options The options.
   */
  public DBOptions(final Options options) {
    super(newDBOptionsFromOptions(options.nativeHandle_));
  }

  /**
   * <p>Method to get a options instance by using pre-configured
   * property values. If one or many values are undefined in
   * the context of RocksDB the method will return a null
   * value.</p>
   *
   * <p><strong>Note</strong>: Property keys can be derived from
   * getter methods within the options class. Example: the method
   * {@code allowMmapReads()} has a property key:
   * {@code allow_mmap_reads}.</p>
   *
   * @param properties {@link java.util.Properties} instance.
   *
   * @return {@link org.rocksdb.DBOptions instance}
   *     or null.
   *
   * @throws java.lang.IllegalArgumentException if null or empty
   *     {@link java.util.Properties} instance is passed to the method call.
   */
  public static DBOptions getDBOptionsFromProps(
      final Properties properties) {
    if (properties == null || properties.size() == 0) {
      throw new IllegalArgumentException(
          "Properties value must contain at least one value.");
    }
    DBOptions dbOptions = null;
    StringBuilder stringBuilder = new StringBuilder();
    for (final String name : properties.stringPropertyNames()){
      stringBuilder.append(name);
      stringBuilder.append("=");
      stringBuilder.append(properties.getProperty(name));
      stringBuilder.append(";");
    }
    long handle = getDBOptionsFromProps(
        stringBuilder.toString());
    if (handle != 0){
      dbOptions = new DBOptions(handle);
    }
    return dbOptions;
  }

  @Override
  public DBOptions optimizeForSmallDb() {
    optimizeForSmallDb(nativeHandle_);
    return this;
  }

  @Override
  public DBOptions setIncreaseParallelism(
      final int totalThreads) {
    assert(isOwningHandle());
    setIncreaseParallelism(nativeHandle_, totalThreads);
    return this;
  }

  @Override
  public DBOptions setCreateIfMissing(final boolean flag) {
    assert(isOwningHandle());
    setCreateIfMissing(nativeHandle_, flag);
    return this;
  }

  @Override
  public boolean createIfMissing() {
    assert(isOwningHandle());
    return createIfMissing(nativeHandle_);
  }

  @Override
  public DBOptions setCreateMissingColumnFamilies(
      final boolean flag) {
    assert(isOwningHandle());
    setCreateMissingColumnFamilies(nativeHandle_, flag);
    return this;
  }

  @Override
  public boolean createMissingColumnFamilies() {
    assert(isOwningHandle());
    return createMissingColumnFamilies(nativeHandle_);
  }

  @Override
  public DBOptions setErrorIfExists(
      final boolean errorIfExists) {
    assert(isOwningHandle());
    setErrorIfExists(nativeHandle_, errorIfExists);
    return this;
  }

  @Override
  public boolean errorIfExists() {
    assert(isOwningHandle());
    return errorIfExists(nativeHandle_);
  }

  @Override
  public DBOptions setParanoidChecks(
      final boolean paranoidChecks) {
    assert(isOwningHandle());
    setParanoidChecks(nativeHandle_, paranoidChecks);
    return this;
  }

  @Override
  public boolean paranoidChecks() {
    assert(isOwningHandle());
    return paranoidChecks(nativeHandle_);
  }

  @Override
  public DBOptions setEnv(final Env env) {
    setEnv(nativeHandle_, env.nativeHandle_);
    this.env_ = env;
    return this;
  }

  @Override
  public Env getEnv() {
    return env_;
  }

  @Override
  public DBOptions setRateLimiter(final RateLimiter rateLimiter) {
    assert(isOwningHandle());
    rateLimiter_ = rateLimiter;
    setRateLimiter(nativeHandle_, rateLimiter.nativeHandle_);
    return this;
  }

  @Override
  public DBOptions setSstFileManager(final SstFileManager sstFileManager) {
    assert(isOwningHandle());
    setSstFileManager(nativeHandle_, sstFileManager.nativeHandle_);
    return this;
  }

  @Override
  public DBOptions setLogger(final Logger logger) {
    assert(isOwningHandle());
    setLogger(nativeHandle_, logger.nativeHandle_);
    return this;
  }

  @Override
  public DBOptions setInfoLogLevel(
      final InfoLogLevel infoLogLevel) {
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
  public DBOptions setMaxOpenFiles(
      final int maxOpenFiles) {
    assert(isOwningHandle());
    setMaxOpenFiles(nativeHandle_, maxOpenFiles);
    return this;
  }

  @Override
  public int maxOpenFiles() {
    assert(isOwningHandle());
    return maxOpenFiles(nativeHandle_);
  }

  @Override
  public DBOptions setMaxFileOpeningThreads(final int maxFileOpeningThreads) {
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
  public DBOptions setMaxTotalWalSize(
      final long maxTotalWalSize) {
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
  public DBOptions setStatistics(final Statistics statistics) {
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
  public DBOptions setUseFsync(
      final boolean useFsync) {
    assert(isOwningHandle());
    setUseFsync(nativeHandle_, useFsync);
    return this;
  }

  @Override
  public boolean useFsync() {
    assert(isOwningHandle());
    return useFsync(nativeHandle_);
  }

  @Override
  public DBOptions setDbPaths(final Collection<DbPath> dbPaths) {
    assert(isOwningHandle());

    final int len = dbPaths.size();
    final String[] paths = new String[len];
    final long[] targetSizes = new long[len];

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
      final String[] paths = new String[len];
      final long[] targetSizes = new long[len];

      dbPaths(nativeHandle_, paths, targetSizes);

      final List<DbPath> dbPaths = new ArrayList<>();
      for(int i = 0; i < len; i++) {
        dbPaths.add(new DbPath(Paths.get(paths[i]), targetSizes[i]));
      }
      return dbPaths;
    }
  }

  @Override
  public DBOptions setDbLogDir(
      final String dbLogDir) {
    assert(isOwningHandle());
    setDbLogDir(nativeHandle_, dbLogDir);
    return this;
  }

  @Override
  public String dbLogDir() {
    assert(isOwningHandle());
    return dbLogDir(nativeHandle_);
  }

  @Override
  public DBOptions setWalDir(
      final String walDir) {
    assert(isOwningHandle());
    setWalDir(nativeHandle_, walDir);
    return this;
  }

  @Override
  public String walDir() {
    assert(isOwningHandle());
    return walDir(nativeHandle_);
  }

  @Override
  public DBOptions setDeleteObsoleteFilesPeriodMicros(
      final long micros) {
    assert(isOwningHandle());
    setDeleteObsoleteFilesPeriodMicros(nativeHandle_, micros);
    return this;
  }

  @Override
  public long deleteObsoleteFilesPeriodMicros() {
    assert(isOwningHandle());
    return deleteObsoleteFilesPeriodMicros(nativeHandle_);
  }

  @Override
  public DBOptions setMaxBackgroundJobs(final int maxBackgroundJobs) {
    assert(isOwningHandle());
    setMaxBackgroundJobs(nativeHandle_, maxBackgroundJobs);
    return this;
  }

  @Override
  public int maxBackgroundJobs() {
    assert(isOwningHandle());
    return maxBackgroundJobs(nativeHandle_);
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
  public DBOptions setMaxBackgroundCompactions(
      final int maxBackgroundCompactions) {
    assert(isOwningHandle());
    setMaxBackgroundCompactions(nativeHandle_, maxBackgroundCompactions);
    return this;
  }

  @Override
  public int maxBackgroundCompactions() {
    assert(isOwningHandle());
    return maxBackgroundCompactions(nativeHandle_);
  }

  @Override
  public DBOptions setMaxSubcompactions(final int maxSubcompactions) {
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
  public DBOptions setMaxBackgroundFlushes(
      final int maxBackgroundFlushes) {
    assert(isOwningHandle());
    setMaxBackgroundFlushes(nativeHandle_, maxBackgroundFlushes);
    return this;
  }

  @Override
  public int maxBackgroundFlushes() {
    assert(isOwningHandle());
    return maxBackgroundFlushes(nativeHandle_);
  }

  @Override
  public DBOptions setMaxLogFileSize(final long maxLogFileSize) {
    assert(isOwningHandle());
    setMaxLogFileSize(nativeHandle_, maxLogFileSize);
    return this;
  }

  @Override
  public long maxLogFileSize() {
    assert(isOwningHandle());
    return maxLogFileSize(nativeHandle_);
  }

  @Override
  public DBOptions setLogFileTimeToRoll(
      final long logFileTimeToRoll) {
    assert(isOwningHandle());
    setLogFileTimeToRoll(nativeHandle_, logFileTimeToRoll);
    return this;
  }

  @Override
  public long logFileTimeToRoll() {
    assert(isOwningHandle());
    return logFileTimeToRoll(nativeHandle_);
  }

  @Override
  public DBOptions setKeepLogFileNum(
      final long keepLogFileNum) {
    assert(isOwningHandle());
    setKeepLogFileNum(nativeHandle_, keepLogFileNum);
    return this;
  }

  @Override
  public long keepLogFileNum() {
    assert(isOwningHandle());
    return keepLogFileNum(nativeHandle_);
  }

  @Override
  public DBOptions setRecycleLogFileNum(final long recycleLogFileNum) {
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
  public DBOptions setMaxManifestFileSize(
      final long maxManifestFileSize) {
    assert(isOwningHandle());
    setMaxManifestFileSize(nativeHandle_, maxManifestFileSize);
    return this;
  }

  @Override
  public long maxManifestFileSize() {
    assert(isOwningHandle());
    return maxManifestFileSize(nativeHandle_);
  }

  @Override
  public DBOptions setTableCacheNumshardbits(
      final int tableCacheNumshardbits) {
    assert(isOwningHandle());
    setTableCacheNumshardbits(nativeHandle_, tableCacheNumshardbits);
    return this;
  }

  @Override
  public int tableCacheNumshardbits() {
    assert(isOwningHandle());
    return tableCacheNumshardbits(nativeHandle_);
  }

  @Override
  public DBOptions setWalTtlSeconds(
      final long walTtlSeconds) {
    assert(isOwningHandle());
    setWalTtlSeconds(nativeHandle_, walTtlSeconds);
    return this;
  }

  @Override
  public long walTtlSeconds() {
    assert(isOwningHandle());
    return walTtlSeconds(nativeHandle_);
  }

  @Override
  public DBOptions setWalSizeLimitMB(
      final long sizeLimitMB) {
    assert(isOwningHandle());
    setWalSizeLimitMB(nativeHandle_, sizeLimitMB);
    return this;
  }

  @Override
  public long walSizeLimitMB() {
    assert(isOwningHandle());
    return walSizeLimitMB(nativeHandle_);
  }

  @Override
  public DBOptions setManifestPreallocationSize(
      final long size) {
    assert(isOwningHandle());
    setManifestPreallocationSize(nativeHandle_, size);
    return this;
  }

  @Override
  public long manifestPreallocationSize() {
    assert(isOwningHandle());
    return manifestPreallocationSize(nativeHandle_);
  }

  @Override
  public DBOptions setAllowMmapReads(
      final boolean allowMmapReads) {
    assert(isOwningHandle());
    setAllowMmapReads(nativeHandle_, allowMmapReads);
    return this;
  }

  @Override
  public boolean allowMmapReads() {
    assert(isOwningHandle());
    return allowMmapReads(nativeHandle_);
  }

  @Override
  public DBOptions setAllowMmapWrites(
      final boolean allowMmapWrites) {
    assert(isOwningHandle());
    setAllowMmapWrites(nativeHandle_, allowMmapWrites);
    return this;
  }

  @Override
  public boolean allowMmapWrites() {
    assert(isOwningHandle());
    return allowMmapWrites(nativeHandle_);
  }

  @Override
  public DBOptions setUseDirectReads(
      final boolean useDirectReads) {
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
  public DBOptions setUseDirectIoForFlushAndCompaction(
      final boolean useDirectIoForFlushAndCompaction) {
    assert(isOwningHandle());
    setUseDirectIoForFlushAndCompaction(nativeHandle_,
        useDirectIoForFlushAndCompaction);
    return this;
  }

  @Override
  public boolean useDirectIoForFlushAndCompaction() {
    assert(isOwningHandle());
    return useDirectIoForFlushAndCompaction(nativeHandle_);
  }

  @Override
  public DBOptions setAllowFAllocate(final boolean allowFAllocate) {
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
  public DBOptions setIsFdCloseOnExec(
      final boolean isFdCloseOnExec) {
    assert(isOwningHandle());
    setIsFdCloseOnExec(nativeHandle_, isFdCloseOnExec);
    return this;
  }

  @Override
  public boolean isFdCloseOnExec() {
    assert(isOwningHandle());
    return isFdCloseOnExec(nativeHandle_);
  }

  @Override
  public DBOptions setStatsDumpPeriodSec(
      final int statsDumpPeriodSec) {
    assert(isOwningHandle());
    setStatsDumpPeriodSec(nativeHandle_, statsDumpPeriodSec);
    return this;
  }

  @Override
  public int statsDumpPeriodSec() {
    assert(isOwningHandle());
    return statsDumpPeriodSec(nativeHandle_);
  }

  @Override
  public DBOptions setAdviseRandomOnOpen(
      final boolean adviseRandomOnOpen) {
    assert(isOwningHandle());
    setAdviseRandomOnOpen(nativeHandle_, adviseRandomOnOpen);
    return this;
  }

  @Override
  public boolean adviseRandomOnOpen() {
    return adviseRandomOnOpen(nativeHandle_);
  }

  @Override
  public DBOptions setDbWriteBufferSize(final long dbWriteBufferSize) {
    assert(isOwningHandle());
    setDbWriteBufferSize(nativeHandle_, dbWriteBufferSize);
    return this;
  }

  @Override
  public DBOptions setWriteBufferManager(final WriteBufferManager writeBufferManager) {
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
  public DBOptions setAccessHintOnCompactionStart(final AccessHint accessHint) {
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
  public DBOptions setNewTableReaderForCompactionInputs(
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
  public DBOptions setCompactionReadaheadSize(final long compactionReadaheadSize) {
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
  public DBOptions setRandomAccessMaxBufferSize(final long randomAccessMaxBufferSize) {
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
  public DBOptions setWritableFileMaxBufferSize(final long writableFileMaxBufferSize) {
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
  public DBOptions setUseAdaptiveMutex(
      final boolean useAdaptiveMutex) {
    assert(isOwningHandle());
    setUseAdaptiveMutex(nativeHandle_, useAdaptiveMutex);
    return this;
  }

  @Override
  public boolean useAdaptiveMutex() {
    assert(isOwningHandle());
    return useAdaptiveMutex(nativeHandle_);
  }

  @Override
  public DBOptions setBytesPerSync(
      final long bytesPerSync) {
    assert(isOwningHandle());
    setBytesPerSync(nativeHandle_, bytesPerSync);
    return this;
  }

  @Override
  public long bytesPerSync() {
    return bytesPerSync(nativeHandle_);
  }

  @Override
  public DBOptions setWalBytesPerSync(final long walBytesPerSync) {
    assert(isOwningHandle());
    setWalBytesPerSync(nativeHandle_, walBytesPerSync);
    return this;
  }

  @Override
  public long walBytesPerSync() {
    assert(isOwningHandle());
    return walBytesPerSync(nativeHandle_);
  }

  //TODO(AR) NOW
//  @Override
//  public DBOptions setListeners(final List<EventListener> listeners) {
//    assert(isOwningHandle());
//    final long[] eventListenerHandlers = new long[listeners.size()];
//    for (int i = 0; i < eventListenerHandlers.length; i++) {
//      eventListenerHandlers[i] = listeners.get(i).nativeHandle_;
//    }
//    setEventListeners(nativeHandle_, eventListenerHandlers);
//    return this;
//  }
//
//  @Override
//  public Collection<EventListener> listeners() {
//    assert(isOwningHandle());
//    final long[] eventListenerHandlers = listeners(nativeHandle_);
//    if (eventListenerHandlers == null || eventListenerHandlers.length == 0) {
//      return Collections.emptyList();
//    }
//
//    final List<EventListener> eventListeners = new ArrayList<>();
//    for (final long eventListenerHandle : eventListenerHandlers) {
//      eventListeners.add(new EventListener(eventListenerHandle)); //TODO(AR) check ownership is set to false!
//    }
//    return eventListeners;
//  }

  @Override
  public DBOptions setEnableThreadTracking(final boolean enableThreadTracking) {
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
  public DBOptions setDelayedWriteRate(final long delayedWriteRate) {
    assert(isOwningHandle());
    setDelayedWriteRate(nativeHandle_, delayedWriteRate);
    return this;
  }

  @Override
  public long delayedWriteRate(){
    return delayedWriteRate(nativeHandle_);
  }

  @Override
  public DBOptions setEnablePipelinedWrite(final boolean enablePipelinedWrite) {
    assert(isOwningHandle());
    setEnablePipelinedWrite(nativeHandle_, enablePipelinedWrite);
    return this;
  }

  @Override
  public boolean enablePipelinedWrite() {
    assert(isOwningHandle());
    return enablePipelinedWrite(nativeHandle_);
  }

  @Override
  public DBOptions setAllowConcurrentMemtableWrite(
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
  public DBOptions setEnableWriteThreadAdaptiveYield(
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
  public DBOptions setWriteThreadMaxYieldUsec(final long writeThreadMaxYieldUsec) {
    setWriteThreadMaxYieldUsec(nativeHandle_, writeThreadMaxYieldUsec);
    return this;
  }

  @Override
  public long writeThreadMaxYieldUsec() {
    return writeThreadMaxYieldUsec(nativeHandle_);
  }

  @Override
  public DBOptions setWriteThreadSlowYieldUsec(final long writeThreadSlowYieldUsec) {
    setWriteThreadSlowYieldUsec(nativeHandle_, writeThreadSlowYieldUsec);
    return this;
  }

  @Override
  public long writeThreadSlowYieldUsec() {
    return writeThreadSlowYieldUsec(nativeHandle_);
  }

  @Override
  public DBOptions setSkipStatsUpdateOnDbOpen(final boolean skipStatsUpdateOnDbOpen) {
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
  public DBOptions setWalRecoveryMode(final WALRecoveryMode walRecoveryMode) {
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
  public DBOptions setAllow2pc(final boolean allow2pc) {
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
  public DBOptions setRowCache(final Cache rowCache) {
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
  public DBOptions setWalFilter(final AbstractWalFilter walFilter) {
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
  public DBOptions setFailIfOptionsFileError(final boolean failIfOptionsFileError) {
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
  public DBOptions setDumpMallocStats(final boolean dumpMallocStats) {
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
  public DBOptions setAvoidFlushDuringRecovery(final boolean avoidFlushDuringRecovery) {
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
  public DBOptions setAvoidFlushDuringShutdown(final boolean avoidFlushDuringShutdown) {
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
  public DBOptions setAllowIngestBehind(final boolean allowIngestBehind) {
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
  public DBOptions setPreserveDeletes(final boolean preserveDeletes) {
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
  public DBOptions setTwoWriteQueues(final boolean twoWriteQueues) {
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
  public DBOptions setManualWalFlush(final boolean manualWalFlush) {
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
  public DBOptions setAtomicFlush(final boolean atomicFlush) {
    setAtomicFlush(nativeHandle_, atomicFlush);
    return this;
  }

  @Override
  public boolean atomicFlush() {
    return atomicFlush(nativeHandle_);
  }

  static final int DEFAULT_NUM_SHARD_BITS = -1;




  /**
   * <p>Private constructor to be used by
   * {@link #getDBOptionsFromProps(java.util.Properties)}</p>
   *
   * @param nativeHandle native handle to DBOptions instance.
   */
  private DBOptions(final long nativeHandle) {
    super(nativeHandle);
  }

  private static native long getDBOptionsFromProps(
      String optString);

  private static native long newDBOptions();
  private static native long copyDBOptions(final long handle);
  private static native long newDBOptionsFromOptions(final long optionsHandle);
  @Override protected final native void disposeInternal(final long handle);

  private native void optimizeForSmallDb(final long handle);
  private native void setIncreaseParallelism(long handle, int totalThreads);
  private native void setCreateIfMissing(long handle, boolean flag);
  private native boolean createIfMissing(long handle);
  private native void setCreateMissingColumnFamilies(
      long handle, boolean flag);
  private native boolean createMissingColumnFamilies(long handle);
  private native void setEnv(long handle, long envHandle);
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
  private native void setMaxFileOpeningThreads(final long handle,
      final int maxFileOpeningThreads);
  private native int maxFileOpeningThreads(final long handle);
  private native void setMaxTotalWalSize(long handle,
      long maxTotalWalSize);
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
  private native void setMaxBackgroundJobs(long handle, int maxBackgroundJobs);
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
  private native void setWriteBufferManager(final long dbOptionsHandle,
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
      final boolean enablePipelinedWrite);
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
  private native void setAtomicFlush(final long handle,
      final boolean atomicFlush);
  private native boolean atomicFlush(final long handle);

  // instance variables
  // NOTE: If you add new member variables, please update the copy constructor above!
  private Env env_;
  private int numShardBits_;
  private RateLimiter rateLimiter_;
  private Cache rowCache_;
  private WalFilter walFilter_;
  private WriteBufferManager writeBufferManager_;
}
