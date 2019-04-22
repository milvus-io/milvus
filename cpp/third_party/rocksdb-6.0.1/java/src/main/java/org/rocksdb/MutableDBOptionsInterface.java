package org.rocksdb;

public interface MutableDBOptionsInterface<T extends MutableDBOptionsInterface> {

  /**
   * Specifies the maximum number of concurrent background jobs (both flushes
   * and compactions combined).
   * Default: 2
   *
   * @param maxBackgroundJobs number of max concurrent background jobs
   * @return the instance of the current object.
   */
  T setMaxBackgroundJobs(int maxBackgroundJobs);

  /**
   * Returns the maximum number of concurrent background jobs (both flushes
   * and compactions combined).
   * Default: 2
   *
   * @return the maximum number of concurrent background jobs.
   */
  int maxBackgroundJobs();

  /**
   * Suggested number of concurrent background compaction jobs, submitted to
   * the default LOW priority thread pool.
   * Default: 1
   *
   * @param baseBackgroundCompactions Suggested number of background compaction
   *     jobs
   *
   * @deprecated Use {@link #setMaxBackgroundJobs(int)}
   */
  @Deprecated
  void setBaseBackgroundCompactions(int baseBackgroundCompactions);

  /**
   * Suggested number of concurrent background compaction jobs, submitted to
   * the default LOW priority thread pool.
   * Default: 1
   *
   * @return Suggested number of background compaction jobs
   */
  int baseBackgroundCompactions();

  /**
   * Specifies the maximum number of concurrent background compaction jobs,
   * submitted to the default LOW priority thread pool.
   * If you're increasing this, also consider increasing number of threads in
   * LOW priority thread pool. For more information, see
   * Default: 1
   *
   * @param maxBackgroundCompactions the maximum number of background
   *     compaction jobs.
   * @return the instance of the current object.
   *
   * @see RocksEnv#setBackgroundThreads(int)
   * @see RocksEnv#setBackgroundThreads(int, Priority)
   * @see DBOptionsInterface#maxBackgroundFlushes()
   */
  T setMaxBackgroundCompactions(int maxBackgroundCompactions);

  /**
   * Returns the maximum number of concurrent background compaction jobs,
   * submitted to the default LOW priority thread pool.
   * When increasing this number, we may also want to consider increasing
   * number of threads in LOW priority thread pool.
   * Default: 1
   *
   * @return the maximum number of concurrent background compaction jobs.
   * @see RocksEnv#setBackgroundThreads(int)
   * @see RocksEnv#setBackgroundThreads(int, Priority)
   *
   * @deprecated Use {@link #setMaxBackgroundJobs(int)}
   */
  @Deprecated
  int maxBackgroundCompactions();

  /**
   * By default RocksDB will flush all memtables on DB close if there are
   * unpersisted data (i.e. with WAL disabled) The flush can be skip to speedup
   * DB close. Unpersisted data WILL BE LOST.
   *
   * DEFAULT: false
   *
   * Dynamically changeable through
   *     {@link RocksDB#setOptions(ColumnFamilyHandle, MutableColumnFamilyOptions)}
   *     API.
   *
   * @param avoidFlushDuringShutdown true if we should avoid flush during
   *     shutdown
   *
   * @return the reference to the current options.
   */
  T setAvoidFlushDuringShutdown(boolean avoidFlushDuringShutdown);

  /**
   * By default RocksDB will flush all memtables on DB close if there are
   * unpersisted data (i.e. with WAL disabled) The flush can be skip to speedup
   * DB close. Unpersisted data WILL BE LOST.
   *
   * DEFAULT: false
   *
   * Dynamically changeable through
   *     {@link RocksDB#setOptions(ColumnFamilyHandle, MutableColumnFamilyOptions)}
   *     API.
   *
   * @return true if we should avoid flush during shutdown
   */
  boolean avoidFlushDuringShutdown();

  /**
   * This is the maximum buffer size that is used by WritableFileWriter.
   * On Windows, we need to maintain an aligned buffer for writes.
   * We allow the buffer to grow until it's size hits the limit.
   *
   * Default: 1024 * 1024 (1 MB)
   *
   * @param writableFileMaxBufferSize the maximum buffer size
   *
   * @return the reference to the current options.
   */
  T setWritableFileMaxBufferSize(long writableFileMaxBufferSize);

  /**
   * This is the maximum buffer size that is used by WritableFileWriter.
   * On Windows, we need to maintain an aligned buffer for writes.
   * We allow the buffer to grow until it's size hits the limit.
   *
   * Default: 1024 * 1024 (1 MB)
   *
   * @return the maximum buffer size
   */
  long writableFileMaxBufferSize();

  /**
   * The limited write rate to DB if
   * {@link ColumnFamilyOptions#softPendingCompactionBytesLimit()} or
   * {@link ColumnFamilyOptions#level0SlowdownWritesTrigger()} is triggered,
   * or we are writing to the last mem table allowed and we allow more than 3
   * mem tables. It is calculated using size of user write requests before
   * compression. RocksDB may decide to slow down more if the compaction still
   * gets behind further.
   *
   * Unit: bytes per second.
   *
   * Default: 16MB/s
   *
   * @param delayedWriteRate the rate in bytes per second
   *
   * @return the reference to the current options.
   */
  T setDelayedWriteRate(long delayedWriteRate);

  /**
   * The limited write rate to DB if
   * {@link ColumnFamilyOptions#softPendingCompactionBytesLimit()} or
   * {@link ColumnFamilyOptions#level0SlowdownWritesTrigger()} is triggered,
   * or we are writing to the last mem table allowed and we allow more than 3
   * mem tables. It is calculated using size of user write requests before
   * compression. RocksDB may decide to slow down more if the compaction still
   * gets behind further.
   *
   * Unit: bytes per second.
   *
   * Default: 16MB/s
   *
   * @return the rate in bytes per second
   */
  long delayedWriteRate();

  /**
   * <p>Once write-ahead logs exceed this size, we will start forcing the
   * flush of column families whose memtables are backed by the oldest live
   * WAL file (i.e. the ones that are causing all the space amplification).
   * </p>
   * <p>If set to 0 (default), we will dynamically choose the WAL size limit to
   * be [sum of all write_buffer_size * max_write_buffer_number] * 2</p>
   * <p>This option takes effect only when there are more than one column family as
   * otherwise the wal size is dictated by the write_buffer_size.</p>
   * <p>Default: 0</p>
   *
   * @param maxTotalWalSize max total wal size.
   * @return the instance of the current object.
   */
  T setMaxTotalWalSize(long maxTotalWalSize);

  /**
   * <p>Returns the max total wal size. Once write-ahead logs exceed this size,
   * we will start forcing the flush of column families whose memtables are
   * backed by the oldest live WAL file (i.e. the ones that are causing all
   * the space amplification).</p>
   *
   * <p>If set to 0 (default), we will dynamically choose the WAL size limit
   * to be [sum of all write_buffer_size * max_write_buffer_number] * 2
   * </p>
   *
   * @return max total wal size
   */
  long maxTotalWalSize();

  /**
   * The periodicity when obsolete files get deleted. The default
   * value is 6 hours. The files that get out of scope by compaction
   * process will still get automatically delete on every compaction,
   * regardless of this setting
   *
   * @param micros the time interval in micros
   * @return the instance of the current object.
   */
  T setDeleteObsoleteFilesPeriodMicros(long micros);

  /**
   * The periodicity when obsolete files get deleted. The default
   * value is 6 hours. The files that get out of scope by compaction
   * process will still get automatically delete on every compaction,
   * regardless of this setting
   *
   * @return the time interval in micros when obsolete files will be deleted.
   */
  long deleteObsoleteFilesPeriodMicros();

  /**
   * if not zero, dump rocksdb.stats to LOG every stats_dump_period_sec
   * Default: 600 (10 minutes)
   *
   * @param statsDumpPeriodSec time interval in seconds.
   * @return the instance of the current object.
   */
  T setStatsDumpPeriodSec(int statsDumpPeriodSec);

  /**
   * If not zero, dump rocksdb.stats to LOG every stats_dump_period_sec
   * Default: 600 (10 minutes)
   *
   * @return time interval in seconds.
   */
  int statsDumpPeriodSec();

  /**
   * Number of open files that can be used by the DB.  You may need to
   * increase this if your database has a large working set. Value -1 means
   * files opened are always kept open. You can estimate number of files based
   * on {@code target_file_size_base} and {@code target_file_size_multiplier}
   * for level-based compaction. For universal-style compaction, you can usually
   * set it to -1.
   * Default: 5000
   *
   * @param maxOpenFiles the maximum number of open files.
   * @return the instance of the current object.
   */
  T setMaxOpenFiles(int maxOpenFiles);

  /**
   * Number of open files that can be used by the DB.  You may need to
   * increase this if your database has a large working set. Value -1 means
   * files opened are always kept open. You can estimate number of files based
   * on {@code target_file_size_base} and {@code target_file_size_multiplier}
   * for level-based compaction. For universal-style compaction, you can usually
   * set it to -1.
   *
   * @return the maximum number of open files.
   */
  int maxOpenFiles();

  /**
   * Allows OS to incrementally sync files to disk while they are being
   * written, asynchronously, in the background.
   * Issue one request for every bytes_per_sync written. 0 turns it off.
   * Default: 0
   *
   * @param bytesPerSync size in bytes
   * @return the instance of the current object.
   */
  T setBytesPerSync(long bytesPerSync);

  /**
   * Allows OS to incrementally sync files to disk while they are being
   * written, asynchronously, in the background.
   * Issue one request for every bytes_per_sync written. 0 turns it off.
   * Default: 0
   *
   * @return size in bytes
   */
  long bytesPerSync();

  /**
   * Same as {@link #setBytesPerSync(long)} , but applies to WAL files
   *
   * Default: 0, turned off
   *
   * @param walBytesPerSync size in bytes
   * @return the instance of the current object.
   */
  T setWalBytesPerSync(long walBytesPerSync);

  /**
   * Same as {@link #bytesPerSync()} , but applies to WAL files
   *
   * Default: 0, turned off
   *
   * @return size in bytes
   */
  long walBytesPerSync();


  /**
   * If non-zero, we perform bigger reads when doing compaction. If you're
   * running RocksDB on spinning disks, you should set this to at least 2MB.
   *
   * That way RocksDB's compaction is doing sequential instead of random reads.
   * When non-zero, we also force
   * {@link DBOptionsInterface#newTableReaderForCompactionInputs()} to true.
   *
   * Default: 0
   *
   * @param compactionReadaheadSize The compaction read-ahead size
   *
   * @return the reference to the current options.
   */
  T setCompactionReadaheadSize(final long compactionReadaheadSize);

  /**
   * If non-zero, we perform bigger reads when doing compaction. If you're
   * running RocksDB on spinning disks, you should set this to at least 2MB.
   *
   * That way RocksDB's compaction is doing sequential instead of random reads.
   * When non-zero, we also force
   * {@link DBOptionsInterface#newTableReaderForCompactionInputs()} to true.
   *
   * Default: 0
   *
   * @return The compaction read-ahead size
   */
  long compactionReadaheadSize();
}
