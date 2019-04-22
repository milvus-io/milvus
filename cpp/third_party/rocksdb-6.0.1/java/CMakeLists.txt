cmake_minimum_required(VERSION 3.4)

set(JNI_NATIVE_SOURCES
        rocksjni/backupablejni.cc
        rocksjni/backupenginejni.cc
        rocksjni/cassandra_compactionfilterjni.cc
        rocksjni/cassandra_value_operator.cc
        rocksjni/checkpoint.cc
        rocksjni/clock_cache.cc
        rocksjni/columnfamilyhandle.cc
        rocksjni/compaction_filter.cc
        rocksjni/compaction_filter_factory.cc
        rocksjni/compaction_filter_factory_jnicallback.cc
	rocksjni/compaction_job_info.cc
	rocksjni/compaction_job_stats.cc
	rocksjni/compaction_options.cc
        rocksjni/compaction_options_fifo.cc
        rocksjni/compaction_options_universal.cc
        rocksjni/compact_range_options.cc
        rocksjni/comparator.cc
        rocksjni/comparatorjnicallback.cc
        rocksjni/compression_options.cc
        rocksjni/env.cc
        rocksjni/env_options.cc
        rocksjni/filter.cc
        rocksjni/ingest_external_file_options.cc
        rocksjni/iterator.cc
        rocksjni/jnicallback.cc
        rocksjni/loggerjnicallback.cc
        rocksjni/lru_cache.cc
        rocksjni/memory_util.cc
        rocksjni/memtablejni.cc
        rocksjni/merge_operator.cc
        rocksjni/native_comparator_wrapper_test.cc
        rocksjni/optimistic_transaction_db.cc
        rocksjni/optimistic_transaction_options.cc
        rocksjni/options.cc
        rocksjni/options_util.cc
        rocksjni/persistent_cache.cc
        rocksjni/ratelimiterjni.cc
        rocksjni/remove_emptyvalue_compactionfilterjni.cc
        rocksjni/restorejni.cc
        rocksjni/rocks_callback_object.cc
        rocksjni/rocksdb_exception_test.cc
        rocksjni/rocksjni.cc
        rocksjni/slice.cc
        rocksjni/snapshot.cc
        rocksjni/sst_file_manager.cc
        rocksjni/sst_file_writerjni.cc
        rocksjni/statistics.cc
        rocksjni/statisticsjni.cc
        rocksjni/table.cc
        rocksjni/table_filter.cc
        rocksjni/table_filter_jnicallback.cc
        rocksjni/thread_status.cc
        rocksjni/trace_writer.cc
        rocksjni/trace_writer_jnicallback.cc
        rocksjni/transaction.cc
        rocksjni/transaction_db.cc
        rocksjni/transaction_db_options.cc
        rocksjni/transaction_log.cc
        rocksjni/transaction_notifier.cc
        rocksjni/transaction_notifier_jnicallback.cc
        rocksjni/transaction_options.cc
        rocksjni/ttl.cc
        rocksjni/wal_filter.cc
        rocksjni/wal_filter_jnicallback.cc
        rocksjni/write_batch.cc
        rocksjni/writebatchhandlerjnicallback.cc
        rocksjni/write_batch_test.cc
        rocksjni/write_batch_with_index.cc
        rocksjni/write_buffer_manager.cc
)

set(NATIVE_JAVA_CLASSES
        org.rocksdb.AbstractCompactionFilter
        org.rocksdb.AbstractCompactionFilterFactory
        org.rocksdb.AbstractComparator
        org.rocksdb.AbstractImmutableNativeReference
        org.rocksdb.AbstractNativeReference
        org.rocksdb.AbstractRocksIterator
        org.rocksdb.AbstractSlice
        org.rocksdb.AbstractTableFilter
        org.rocksdb.AbstractTraceWriter
        org.rocksdb.AbstractTransactionNotifier
        org.rocksdb.AbstractWalFilter
        org.rocksdb.BackupableDBOptions
        org.rocksdb.BackupEngine
        org.rocksdb.BlockBasedTableConfig
        org.rocksdb.BloomFilter
        org.rocksdb.CassandraCompactionFilter
        org.rocksdb.CassandraValueMergeOperator
        org.rocksdb.Checkpoint
        org.rocksdb.ClockCache
        org.rocksdb.ColumnFamilyHandle
        org.rocksdb.ColumnFamilyOptions
        org.rocksdb.CompactionJobInfo
        org.rocksdb.CompactionJobStats
        org.rocksdb.CompactionOptions
        org.rocksdb.CompactionOptionsFIFO
        org.rocksdb.CompactionOptionsUniversal
        org.rocksdb.CompactRangeOptions
        org.rocksdb.Comparator
        org.rocksdb.ComparatorOptions
        org.rocksdb.CompressionOptions
        org.rocksdb.DBOptions
        org.rocksdb.DirectComparator
        org.rocksdb.DirectSlice
        org.rocksdb.Env
        org.rocksdb.EnvOptions
        org.rocksdb.Filter
        org.rocksdb.FlushOptions
        org.rocksdb.HashLinkedListMemTableConfig
        org.rocksdb.HashSkipListMemTableConfig
        org.rocksdb.HdfsEnv
        org.rocksdb.IngestExternalFileOptions
        org.rocksdb.Logger
        org.rocksdb.LRUCache
        org.rocksdb.MemoryUtil
        org.rocksdb.MemTableConfig
        org.rocksdb.NativeComparatorWrapper
        org.rocksdb.NativeLibraryLoader
        org.rocksdb.OptimisticTransactionDB
        org.rocksdb.OptimisticTransactionOptions
        org.rocksdb.Options
        org.rocksdb.OptionsUtil
        org.rocksdb.PersistentCache
        org.rocksdb.PlainTableConfig
        org.rocksdb.RateLimiter
        org.rocksdb.ReadOptions
        org.rocksdb.RemoveEmptyValueCompactionFilter
        org.rocksdb.RestoreOptions
        org.rocksdb.RocksCallbackObject
        org.rocksdb.RocksDB
        org.rocksdb.RocksEnv
        org.rocksdb.RocksIterator
        org.rocksdb.RocksIteratorInterface
        org.rocksdb.RocksMemEnv
        org.rocksdb.RocksMutableObject
        org.rocksdb.RocksObject
        org.rocksdb.SkipListMemTableConfig
        org.rocksdb.Slice
        org.rocksdb.Snapshot
        org.rocksdb.SstFileManager
        org.rocksdb.SstFileWriter
        org.rocksdb.Statistics
        org.rocksdb.StringAppendOperator
        org.rocksdb.TableFormatConfig
        org.rocksdb.ThreadStatus
        org.rocksdb.TimedEnv
        org.rocksdb.Transaction
        org.rocksdb.TransactionDB
        org.rocksdb.TransactionDBOptions
        org.rocksdb.TransactionLogIterator
        org.rocksdb.TransactionOptions
        org.rocksdb.TtlDB
        org.rocksdb.UInt64AddOperator
        org.rocksdb.VectorMemTableConfig
        org.rocksdb.WBWIRocksIterator
        org.rocksdb.WriteBatch
        org.rocksdb.WriteBatch.Handler
        org.rocksdb.WriteBatchInterface
        org.rocksdb.WriteBatchWithIndex
        org.rocksdb.WriteOptions
        org.rocksdb.NativeComparatorWrapperTest
        org.rocksdb.RocksDBExceptionTest
        org.rocksdb.SnapshotTest
        org.rocksdb.WriteBatchTest
        org.rocksdb.WriteBatchTestInternalHelper
        org.rocksdb.WriteBufferManager
)

include(FindJava)
include(UseJava)
include(FindJNI)

include_directories(${JNI_INCLUDE_DIRS})
include_directories(${PROJECT_SOURCE_DIR}/java)

set(JAVA_TEST_LIBDIR ${PROJECT_SOURCE_DIR}/java/test-libs)
set(JAVA_TMP_JAR ${JAVA_TEST_LIBDIR}/tmp.jar)
set(JAVA_JUNIT_JAR ${JAVA_TEST_LIBDIR}/junit-4.12.jar)
set(JAVA_HAMCR_JAR ${JAVA_TEST_LIBDIR}/hamcrest-core-1.3.jar)
set(JAVA_MOCKITO_JAR ${JAVA_TEST_LIBDIR}/mockito-all-1.10.19.jar)
set(JAVA_CGLIB_JAR ${JAVA_TEST_LIBDIR}/cglib-2.2.2.jar)
set(JAVA_ASSERTJ_JAR ${JAVA_TEST_LIBDIR}/assertj-core-1.7.1.jar)
set(JAVA_TESTCLASSPATH ${JAVA_JUNIT_JAR} ${JAVA_HAMCR_JAR} ${JAVA_MOCKITO_JAR} ${JAVA_CGLIB_JAR} ${JAVA_ASSERTJ_JAR})

add_jar(
  rocksdbjni_classes
  SOURCES
  src/main/java/org/rocksdb/AbstractCompactionFilterFactory.java
  src/main/java/org/rocksdb/AbstractCompactionFilter.java
  src/main/java/org/rocksdb/AbstractComparator.java
  src/main/java/org/rocksdb/AbstractImmutableNativeReference.java
  src/main/java/org/rocksdb/AbstractMutableOptions.java
  src/main/java/org/rocksdb/AbstractNativeReference.java
  src/main/java/org/rocksdb/AbstractRocksIterator.java
  src/main/java/org/rocksdb/AbstractSlice.java
  src/main/java/org/rocksdb/AbstractTableFilter.java
  src/main/java/org/rocksdb/AbstractTraceWriter.java
  src/main/java/org/rocksdb/AbstractTransactionNotifier.java
  src/main/java/org/rocksdb/AbstractWalFilter.java
  src/main/java/org/rocksdb/AbstractWriteBatch.java
  src/main/java/org/rocksdb/AccessHint.java
  src/main/java/org/rocksdb/AdvancedColumnFamilyOptionsInterface.java
  src/main/java/org/rocksdb/AdvancedMutableColumnFamilyOptionsInterface.java
  src/main/java/org/rocksdb/BackupableDBOptions.java
  src/main/java/org/rocksdb/BackupEngine.java
  src/main/java/org/rocksdb/BackupInfo.java
  src/main/java/org/rocksdb/BlockBasedTableConfig.java
  src/main/java/org/rocksdb/BloomFilter.java
  src/main/java/org/rocksdb/BuiltinComparator.java
  src/main/java/org/rocksdb/Cache.java
  src/main/java/org/rocksdb/CassandraCompactionFilter.java
  src/main/java/org/rocksdb/CassandraValueMergeOperator.java
  src/main/java/org/rocksdb/Checkpoint.java
  src/main/java/org/rocksdb/ChecksumType.java
  src/main/java/org/rocksdb/ClockCache.java
  src/main/java/org/rocksdb/ColumnFamilyDescriptor.java
  src/main/java/org/rocksdb/ColumnFamilyHandle.java
  src/main/java/org/rocksdb/ColumnFamilyMetaData.java
  src/main/java/org/rocksdb/ColumnFamilyOptionsInterface.java
  src/main/java/org/rocksdb/ColumnFamilyOptions.java
  src/main/java/org/rocksdb/CompactionJobInfo.java
  src/main/java/org/rocksdb/CompactionJobStats.java
  src/main/java/org/rocksdb/CompactionOptions.java
  src/main/java/org/rocksdb/CompactionOptionsFIFO.java
  src/main/java/org/rocksdb/CompactionOptionsUniversal.java
  src/main/java/org/rocksdb/CompactionPriority.java
  src/main/java/org/rocksdb/CompactionReason.java
  src/main/java/org/rocksdb/CompactRangeOptions.java
  src/main/java/org/rocksdb/CompactionStopStyle.java
  src/main/java/org/rocksdb/CompactionStyle.java
  src/main/java/org/rocksdb/Comparator.java
  src/main/java/org/rocksdb/ComparatorOptions.java
  src/main/java/org/rocksdb/ComparatorType.java
  src/main/java/org/rocksdb/CompressionOptions.java
  src/main/java/org/rocksdb/CompressionType.java
  src/main/java/org/rocksdb/DataBlockIndexType.java
  src/main/java/org/rocksdb/DBOptionsInterface.java
  src/main/java/org/rocksdb/DBOptions.java
  src/main/java/org/rocksdb/DbPath.java
  src/main/java/org/rocksdb/DirectComparator.java
  src/main/java/org/rocksdb/DirectSlice.java
  src/main/java/org/rocksdb/EncodingType.java
  src/main/java/org/rocksdb/Env.java
  src/main/java/org/rocksdb/EnvOptions.java
  src/main/java/org/rocksdb/Experimental.java
  src/main/java/org/rocksdb/Filter.java
  src/main/java/org/rocksdb/FlushOptions.java
  src/main/java/org/rocksdb/HashLinkedListMemTableConfig.java
  src/main/java/org/rocksdb/HashSkipListMemTableConfig.java
  src/main/java/org/rocksdb/HdfsEnv.java
  src/main/java/org/rocksdb/HistogramData.java
  src/main/java/org/rocksdb/HistogramType.java
  src/main/java/org/rocksdb/IndexType.java
  src/main/java/org/rocksdb/InfoLogLevel.java
  src/main/java/org/rocksdb/IngestExternalFileOptions.java
  src/main/java/org/rocksdb/LevelMetaData.java
  src/main/java/org/rocksdb/LiveFileMetaData.java
  src/main/java/org/rocksdb/LogFile.java
  src/main/java/org/rocksdb/Logger.java
  src/main/java/org/rocksdb/LRUCache.java
  src/main/java/org/rocksdb/MemoryUsageType.java
  src/main/java/org/rocksdb/MemoryUtil.java
  src/main/java/org/rocksdb/MemTableConfig.java
  src/main/java/org/rocksdb/MergeOperator.java
  src/main/java/org/rocksdb/MutableColumnFamilyOptions.java
  src/main/java/org/rocksdb/MutableColumnFamilyOptionsInterface.java
  src/main/java/org/rocksdb/MutableDBOptions.java
  src/main/java/org/rocksdb/MutableDBOptionsInterface.java
  src/main/java/org/rocksdb/MutableOptionKey.java
  src/main/java/org/rocksdb/MutableOptionValue.java
  src/main/java/org/rocksdb/NativeComparatorWrapper.java
  src/main/java/org/rocksdb/NativeLibraryLoader.java
  src/main/java/org/rocksdb/OperationStage.java
  src/main/java/org/rocksdb/OperationType.java
  src/main/java/org/rocksdb/OptimisticTransactionDB.java
  src/main/java/org/rocksdb/OptimisticTransactionOptions.java
  src/main/java/org/rocksdb/Options.java
  src/main/java/org/rocksdb/OptionsUtil.java
  src/main/java/org/rocksdb/PersistentCache.java
  src/main/java/org/rocksdb/PlainTableConfig.java
  src/main/java/org/rocksdb/Priority.java
  src/main/java/org/rocksdb/Range.java
  src/main/java/org/rocksdb/RateLimiter.java
  src/main/java/org/rocksdb/RateLimiterMode.java
  src/main/java/org/rocksdb/ReadOptions.java
  src/main/java/org/rocksdb/ReadTier.java
  src/main/java/org/rocksdb/RemoveEmptyValueCompactionFilter.java
  src/main/java/org/rocksdb/RestoreOptions.java
  src/main/java/org/rocksdb/RocksCallbackObject.java
  src/main/java/org/rocksdb/RocksDBException.java
  src/main/java/org/rocksdb/RocksDB.java
  src/main/java/org/rocksdb/RocksEnv.java
  src/main/java/org/rocksdb/RocksIteratorInterface.java
  src/main/java/org/rocksdb/RocksIterator.java
  src/main/java/org/rocksdb/RocksMemEnv.java
  src/main/java/org/rocksdb/RocksMutableObject.java
  src/main/java/org/rocksdb/RocksObject.java
  src/main/java/org/rocksdb/SizeApproximationFlag.java
  src/main/java/org/rocksdb/SkipListMemTableConfig.java
  src/main/java/org/rocksdb/Slice.java
  src/main/java/org/rocksdb/Snapshot.java
  src/main/java/org/rocksdb/SstFileManager.java
  src/main/java/org/rocksdb/SstFileMetaData.java
  src/main/java/org/rocksdb/SstFileWriter.java
  src/main/java/org/rocksdb/StateType.java
  src/main/java/org/rocksdb/StatisticsCollectorCallback.java
  src/main/java/org/rocksdb/StatisticsCollector.java
  src/main/java/org/rocksdb/Statistics.java
  src/main/java/org/rocksdb/StatsCollectorInput.java
  src/main/java/org/rocksdb/StatsLevel.java
  src/main/java/org/rocksdb/Status.java
  src/main/java/org/rocksdb/StringAppendOperator.java
  src/main/java/org/rocksdb/TableFilter.java
  src/main/java/org/rocksdb/TableProperties.java
  src/main/java/org/rocksdb/TableFormatConfig.java
  src/main/java/org/rocksdb/ThreadType.java
  src/main/java/org/rocksdb/ThreadStatus.java
  src/main/java/org/rocksdb/TickerType.java
  src/main/java/org/rocksdb/TimedEnv.java
  src/main/java/org/rocksdb/TraceOptions.java
  src/main/java/org/rocksdb/TraceWriter.java
  src/main/java/org/rocksdb/TransactionalDB.java
  src/main/java/org/rocksdb/TransactionalOptions.java
  src/main/java/org/rocksdb/TransactionDB.java
  src/main/java/org/rocksdb/TransactionDBOptions.java
  src/main/java/org/rocksdb/Transaction.java
  src/main/java/org/rocksdb/TransactionLogIterator.java
  src/main/java/org/rocksdb/TransactionOptions.java
  src/main/java/org/rocksdb/TtlDB.java
  src/main/java/org/rocksdb/TxnDBWritePolicy.java
  src/main/java/org/rocksdb/VectorMemTableConfig.java
  src/main/java/org/rocksdb/WalFileType.java
  src/main/java/org/rocksdb/WalFilter.java
  src/main/java/org/rocksdb/WalProcessingOption.java
  src/main/java/org/rocksdb/WALRecoveryMode.java
  src/main/java/org/rocksdb/WBWIRocksIterator.java
  src/main/java/org/rocksdb/WriteBatchInterface.java
  src/main/java/org/rocksdb/WriteBatch.java
  src/main/java/org/rocksdb/WriteBatchWithIndex.java
  src/main/java/org/rocksdb/WriteOptions.java
  src/main/java/org/rocksdb/WriteBufferManager.java
  src/main/java/org/rocksdb/util/BytewiseComparator.java
  src/main/java/org/rocksdb/util/DirectBytewiseComparator.java
  src/main/java/org/rocksdb/util/Environment.java
  src/main/java/org/rocksdb/util/ReverseBytewiseComparator.java
  src/main/java/org/rocksdb/util/SizeUnit.java
  src/test/java/org/rocksdb/BackupEngineTest.java
  src/test/java/org/rocksdb/IngestExternalFileOptionsTest.java
  src/test/java/org/rocksdb/NativeComparatorWrapperTest.java
  src/test/java/org/rocksdb/PlatformRandomHelper.java
  src/test/java/org/rocksdb/RocksDBExceptionTest.java
  src/test/java/org/rocksdb/RocksMemoryResource.java
  src/test/java/org/rocksdb/SnapshotTest.java
  src/main/java/org/rocksdb/UInt64AddOperator.java
  src/test/java/org/rocksdb/WriteBatchTest.java
  src/test/java/org/rocksdb/util/CapturingWriteBatchHandler.java
  src/test/java/org/rocksdb/util/WriteBatchGetter.java
  INCLUDE_JARS ${JAVA_TESTCLASSPATH}
)

if(NOT EXISTS ${PROJECT_SOURCE_DIR}/java/classes)
  file(MAKE_DIRECTORY ${PROJECT_SOURCE_DIR}/java/classes)
endif()

if(NOT EXISTS ${JAVA_TEST_LIBDIR})
  file(MAKE_DIRECTORY mkdir ${JAVA_TEST_LIBDIR})
endif()

if (DEFINED CUSTOM_REPO_URL)
  set(SEARCH_REPO_URL ${CUSTOM_REPO_URL}/)
  set(CENTRAL_REPO_URL ${CUSTOM_REPO_URL}/)
else ()
  set(SEARCH_REPO_URL "http://search.maven.org/remotecontent?filepath=")
  set(CENTRAL_REPO_URL "http://central.maven.org/maven2/")
endif()

if(NOT EXISTS ${JAVA_JUNIT_JAR})
  message("Downloading ${JAVA_JUNIT_JAR}")
  file(DOWNLOAD ${SEARCH_REPO_URL}junit/junit/4.12/junit-4.12.jar ${JAVA_TMP_JAR} STATUS downloadStatus)
  list(GET downloadStatus 0 error_code)
  if(NOT error_code EQUAL 0)
    message(FATAL_ERROR "Failed downloading ${JAVA_JUNIT_JAR}")
  endif()
  file(RENAME ${JAVA_TMP_JAR} ${JAVA_JUNIT_JAR})
endif()
if(NOT EXISTS ${JAVA_HAMCR_JAR})
  message("Downloading ${JAVA_HAMCR_JAR}")
  file(DOWNLOAD ${SEARCH_REPO_URL}org/hamcrest/hamcrest-core/1.3/hamcrest-core-1.3.jar ${JAVA_TMP_JAR} STATUS downloadStatus)
  list(GET downloadStatus 0 error_code)
  if(NOT error_code EQUAL 0)
    message(FATAL_ERROR "Failed downloading ${JAVA_HAMCR_JAR}")
  endif()
  file(RENAME ${JAVA_TMP_JAR} ${JAVA_HAMCR_JAR})
endif()
if(NOT EXISTS ${JAVA_MOCKITO_JAR})
  message("Downloading ${JAVA_MOCKITO_JAR}")
  file(DOWNLOAD ${SEARCH_REPO_URL}org/mockito/mockito-all/1.10.19/mockito-all-1.10.19.jar ${JAVA_TMP_JAR} STATUS downloadStatus)
  list(GET downloadStatus 0 error_code)
  if(NOT error_code EQUAL 0)
    message(FATAL_ERROR "Failed downloading ${JAVA_MOCKITO_JAR}")
  endif()
  file(RENAME ${JAVA_TMP_JAR} ${JAVA_MOCKITO_JAR})
endif()
if(NOT EXISTS ${JAVA_CGLIB_JAR})
  message("Downloading ${JAVA_CGLIB_JAR}")
  file(DOWNLOAD ${SEARCH_REPO_URL}cglib/cglib/2.2.2/cglib-2.2.2.jar ${JAVA_TMP_JAR} STATUS downloadStatus)
  list(GET downloadStatus 0 error_code)
  if(NOT error_code EQUAL 0)
    message(FATAL_ERROR "Failed downloading ${JAVA_CGLIB_JAR}")
  endif()
  file(RENAME ${JAVA_TMP_JAR} ${JAVA_CGLIB_JAR})
endif()
if(NOT EXISTS ${JAVA_ASSERTJ_JAR})
  message("Downloading ${JAVA_ASSERTJ_JAR}")
  file(DOWNLOAD ${CENTRAL_REPO_URL}org/assertj/assertj-core/1.7.1/assertj-core-1.7.1.jar ${JAVA_TMP_JAR} STATUS downloadStatus)
  list(GET downloadStatus 0 error_code)
  if(NOT error_code EQUAL 0)
    message(FATAL_ERROR "Failed downloading ${JAVA_ASSERTJ_JAR}")
  endif()
  file(RENAME ${JAVA_TMP_JAR} ${JAVA_ASSERTJ_JAR})
endif()

set(JNI_OUTPUT_DIR ${PROJECT_SOURCE_DIR}/java/include)

file(MAKE_DIRECTORY ${JNI_OUTPUT_DIR})
create_javah(
  TARGET rocksdbjni_headers
  CLASSES ${NATIVE_JAVA_CLASSES}
  CLASSPATH rocksdbjni_classes ${JAVA_TESTCLASSPATH}
  OUTPUT_DIR ${JNI_OUTPUT_DIR}
)

if(NOT MSVC)
  set_property(TARGET ${ROCKSDB_STATIC_LIB} PROPERTY POSITION_INDEPENDENT_CODE ON)
endif()

set(ROCKSDBJNI_STATIC_LIB rocksdbjni${ARTIFACT_SUFFIX})
add_library(${ROCKSDBJNI_STATIC_LIB} ${JNI_NATIVE_SOURCES})
add_dependencies(${ROCKSDBJNI_STATIC_LIB} rocksdbjni_headers)
target_link_libraries(${ROCKSDBJNI_STATIC_LIB} ${ROCKSDB_STATIC_LIB} ${LIBS})

if(NOT MINGW)
  set(ROCKSDBJNI_SHARED_LIB rocksdbjni-shared${ARTIFACT_SUFFIX})
  add_library(${ROCKSDBJNI_SHARED_LIB} SHARED ${JNI_NATIVE_SOURCES})
  add_dependencies(${ROCKSDBJNI_SHARED_LIB} rocksdbjni_headers)
  target_link_libraries(${ROCKSDBJNI_SHARED_LIB} ${ROCKSDB_STATIC_LIB} ${LIBS})

  set_target_properties(
    ${ROCKSDBJNI_SHARED_LIB}
    PROPERTIES
    COMPILE_PDB_OUTPUT_DIRECTORY ${CMAKE_CFG_INTDIR}
    COMPILE_PDB_NAME ${ROCKSDBJNI_STATIC_LIB}.pdb
  )
endif()
