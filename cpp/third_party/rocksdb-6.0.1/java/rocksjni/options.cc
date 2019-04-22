// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ for rocksdb::Options.

#include <jni.h>
#include <stdio.h>
#include <stdlib.h>
#include <memory>
#include <vector>

#include "include/org_rocksdb_ColumnFamilyOptions.h"
#include "include/org_rocksdb_ComparatorOptions.h"
#include "include/org_rocksdb_DBOptions.h"
#include "include/org_rocksdb_FlushOptions.h"
#include "include/org_rocksdb_Options.h"
#include "include/org_rocksdb_ReadOptions.h"
#include "include/org_rocksdb_WriteOptions.h"

#include "rocksjni/comparatorjnicallback.h"
#include "rocksjni/portal.h"
#include "rocksjni/statisticsjni.h"
#include "rocksjni/table_filter_jnicallback.h"

#include "rocksdb/comparator.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/options.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/statistics.h"
#include "rocksdb/table.h"
#include "utilities/merge_operators.h"

/*
 * Class:     org_rocksdb_Options
 * Method:    newOptions
 * Signature: ()J
 */
jlong Java_org_rocksdb_Options_newOptions__(
    JNIEnv*, jclass) {
  auto* op = new rocksdb::Options();
  return reinterpret_cast<jlong>(op);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    newOptions
 * Signature: (JJ)J
 */
jlong Java_org_rocksdb_Options_newOptions__JJ(
    JNIEnv*, jclass, jlong jdboptions, jlong jcfoptions) {
  auto* dbOpt = reinterpret_cast<const rocksdb::DBOptions*>(jdboptions);
  auto* cfOpt =
      reinterpret_cast<const rocksdb::ColumnFamilyOptions*>(jcfoptions);
  auto* op = new rocksdb::Options(*dbOpt, *cfOpt);
  return reinterpret_cast<jlong>(op);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    copyOptions
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_copyOptions(
    JNIEnv*, jclass, jlong jhandle) {
  auto new_opt =
      new rocksdb::Options(*(reinterpret_cast<rocksdb::Options*>(jhandle)));
  return reinterpret_cast<jlong>(new_opt);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_Options_disposeInternal(
    JNIEnv*, jobject, jlong handle) {
  auto* op = reinterpret_cast<rocksdb::Options*>(handle);
  assert(op != nullptr);
  delete op;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setIncreaseParallelism
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setIncreaseParallelism(
    JNIEnv*, jobject, jlong jhandle, jint totalThreads) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->IncreaseParallelism(
      static_cast<int>(totalThreads));
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setCreateIfMissing
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setCreateIfMissing(
    JNIEnv*, jobject, jlong jhandle, jboolean flag) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->create_if_missing = flag;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    createIfMissing
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_createIfMissing(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->create_if_missing;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setCreateMissingColumnFamilies
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setCreateMissingColumnFamilies(
    JNIEnv*, jobject, jlong jhandle, jboolean flag) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->create_missing_column_families =
      flag;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    createMissingColumnFamilies
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_createMissingColumnFamilies(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)
      ->create_missing_column_families;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setComparatorHandle
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setComparatorHandle__JI(
    JNIEnv*, jobject, jlong jhandle, jint builtinComparator) {
  switch (builtinComparator) {
    case 1:
      reinterpret_cast<rocksdb::Options*>(jhandle)->comparator =
          rocksdb::ReverseBytewiseComparator();
      break;
    default:
      reinterpret_cast<rocksdb::Options*>(jhandle)->comparator =
          rocksdb::BytewiseComparator();
      break;
  }
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setComparatorHandle
 * Signature: (JJB)V
 */
void Java_org_rocksdb_Options_setComparatorHandle__JJB(
    JNIEnv*, jobject, jlong jopt_handle, jlong jcomparator_handle,
    jbyte jcomparator_type) {
  rocksdb::Comparator* comparator = nullptr;
  switch (jcomparator_type) {
    // JAVA_COMPARATOR
    case 0x0:
      comparator =
          reinterpret_cast<rocksdb::ComparatorJniCallback*>(jcomparator_handle);
      break;

    // JAVA_DIRECT_COMPARATOR
    case 0x1:
      comparator = reinterpret_cast<rocksdb::DirectComparatorJniCallback*>(
          jcomparator_handle);
      break;

    // JAVA_NATIVE_COMPARATOR_WRAPPER
    case 0x2:
      comparator = reinterpret_cast<rocksdb::Comparator*>(jcomparator_handle);
      break;
  }
  auto* opt = reinterpret_cast<rocksdb::Options*>(jopt_handle);
  opt->comparator = comparator;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMergeOperatorName
 * Signature: (JJjava/lang/String)V
 */
void Java_org_rocksdb_Options_setMergeOperatorName(
    JNIEnv* env, jobject, jlong jhandle, jstring jop_name) {
  const char* op_name = env->GetStringUTFChars(jop_name, nullptr);
  if (op_name == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }

  auto* options = reinterpret_cast<rocksdb::Options*>(jhandle);
  options->merge_operator =
      rocksdb::MergeOperators::CreateFromStringId(op_name);

  env->ReleaseStringUTFChars(jop_name, op_name);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMergeOperator
 * Signature: (JJjava/lang/String)V
 */
void Java_org_rocksdb_Options_setMergeOperator(
    JNIEnv*, jobject, jlong jhandle, jlong mergeOperatorHandle) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->merge_operator =
      *(reinterpret_cast<std::shared_ptr<rocksdb::MergeOperator>*>(
          mergeOperatorHandle));
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setCompactionFilterHandle
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setCompactionFilterHandle(
    JNIEnv*, jobject, jlong jopt_handle,
    jlong jcompactionfilter_handle) {
  reinterpret_cast<rocksdb::Options*>(jopt_handle)->
      compaction_filter = reinterpret_cast<rocksdb::CompactionFilter*>
  (jcompactionfilter_handle);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setCompactionFilterFactoryHandle
 * Signature: (JJ)V
 */
void JNICALL Java_org_rocksdb_Options_setCompactionFilterFactoryHandle(
    JNIEnv*, jobject, jlong jopt_handle,
    jlong jcompactionfilterfactory_handle) {
  auto* cff_factory =
      reinterpret_cast<std::shared_ptr<rocksdb::CompactionFilterFactory> *>(
          jcompactionfilterfactory_handle);
  reinterpret_cast<rocksdb::Options*>(jopt_handle)->
      compaction_filter_factory = *cff_factory;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setWriteBufferSize
 * Signature: (JJ)I
 */
void Java_org_rocksdb_Options_setWriteBufferSize(
    JNIEnv* env, jobject, jlong jhandle, jlong jwrite_buffer_size) {
  auto s =
        rocksdb::JniUtil::check_if_jlong_fits_size_t(jwrite_buffer_size);
  if (s.ok()) {
    reinterpret_cast<rocksdb::Options*>(jhandle)->write_buffer_size =
        jwrite_buffer_size;
  } else {
    rocksdb::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setWriteBufferManager
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setWriteBufferManager(
    JNIEnv*, jobject, jlong joptions_handle,
    jlong jwrite_buffer_manager_handle) {
  auto* write_buffer_manager =
          reinterpret_cast<std::shared_ptr<rocksdb::WriteBufferManager> *>(jwrite_buffer_manager_handle);
  reinterpret_cast<rocksdb::Options*>(joptions_handle)->write_buffer_manager =
          *write_buffer_manager;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    writeBufferSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_writeBufferSize(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->write_buffer_size;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxWriteBufferNumber
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setMaxWriteBufferNumber(
    JNIEnv*, jobject, jlong jhandle,
    jint jmax_write_buffer_number) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->max_write_buffer_number =
      jmax_write_buffer_number;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setStatistics
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setStatistics(
    JNIEnv*, jobject, jlong jhandle, jlong jstatistics_handle) {
  auto* opt = reinterpret_cast<rocksdb::Options*>(jhandle);
  auto* pSptr = reinterpret_cast<std::shared_ptr<rocksdb::StatisticsJni>*>(
      jstatistics_handle);
  opt->statistics = *pSptr;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    statistics
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_statistics(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<rocksdb::Options*>(jhandle);
  std::shared_ptr<rocksdb::Statistics> sptr = opt->statistics;
  if (sptr == nullptr) {
    return 0;
  } else {
    std::shared_ptr<rocksdb::Statistics>* pSptr =
        new std::shared_ptr<rocksdb::Statistics>(sptr);
    return reinterpret_cast<jlong>(pSptr);
  }
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxWriteBufferNumber
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_maxWriteBufferNumber(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->max_write_buffer_number;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    errorIfExists
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_errorIfExists(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->error_if_exists;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setErrorIfExists
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setErrorIfExists(
    JNIEnv*, jobject, jlong jhandle, jboolean error_if_exists) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->error_if_exists =
      static_cast<bool>(error_if_exists);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    paranoidChecks
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_paranoidChecks(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->paranoid_checks;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setParanoidChecks
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setParanoidChecks(
    JNIEnv*, jobject, jlong jhandle, jboolean paranoid_checks) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->paranoid_checks =
      static_cast<bool>(paranoid_checks);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setEnv
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setEnv(
    JNIEnv*, jobject, jlong jhandle, jlong jenv) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->env =
      reinterpret_cast<rocksdb::Env*>(jenv);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxTotalWalSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setMaxTotalWalSize(
    JNIEnv*, jobject, jlong jhandle, jlong jmax_total_wal_size) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->max_total_wal_size =
      static_cast<jlong>(jmax_total_wal_size);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxTotalWalSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_maxTotalWalSize(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->max_total_wal_size;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxOpenFiles
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_maxOpenFiles(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->max_open_files;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxOpenFiles
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setMaxOpenFiles(
    JNIEnv*, jobject, jlong jhandle, jint max_open_files) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->max_open_files =
      static_cast<int>(max_open_files);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxFileOpeningThreads
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setMaxFileOpeningThreads(
    JNIEnv*, jobject, jlong jhandle, jint jmax_file_opening_threads) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->max_file_opening_threads =
      static_cast<int>(jmax_file_opening_threads);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxFileOpeningThreads
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_maxFileOpeningThreads(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<rocksdb::Options*>(jhandle);
  return static_cast<int>(opt->max_file_opening_threads);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    useFsync
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_useFsync(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->use_fsync;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setUseFsync
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setUseFsync(
    JNIEnv*, jobject, jlong jhandle, jboolean use_fsync) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->use_fsync =
      static_cast<bool>(use_fsync);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setDbPaths
 * Signature: (J[Ljava/lang/String;[J)V
 */
void Java_org_rocksdb_Options_setDbPaths(
    JNIEnv* env, jobject, jlong jhandle, jobjectArray jpaths,
    jlongArray jtarget_sizes) {
  std::vector<rocksdb::DbPath> db_paths;
  jlong* ptr_jtarget_size = env->GetLongArrayElements(jtarget_sizes, nullptr);
  if (ptr_jtarget_size == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }

  jboolean has_exception = JNI_FALSE;
  const jsize len = env->GetArrayLength(jpaths);
  for (jsize i = 0; i < len; i++) {
    jobject jpath =
        reinterpret_cast<jstring>(env->GetObjectArrayElement(jpaths, i));
    if (env->ExceptionCheck()) {
      // exception thrown: ArrayIndexOutOfBoundsException
      env->ReleaseLongArrayElements(jtarget_sizes, ptr_jtarget_size, JNI_ABORT);
      return;
    }
    std::string path = rocksdb::JniUtil::copyStdString(
        env, static_cast<jstring>(jpath), &has_exception);
    env->DeleteLocalRef(jpath);

    if (has_exception == JNI_TRUE) {
      env->ReleaseLongArrayElements(jtarget_sizes, ptr_jtarget_size, JNI_ABORT);
      return;
    }

    jlong jtarget_size = ptr_jtarget_size[i];

    db_paths.push_back(
        rocksdb::DbPath(path, static_cast<uint64_t>(jtarget_size)));
  }

  env->ReleaseLongArrayElements(jtarget_sizes, ptr_jtarget_size, JNI_ABORT);

  auto* opt = reinterpret_cast<rocksdb::Options*>(jhandle);
  opt->db_paths = db_paths;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    dbPathsLen
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_dbPathsLen(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<rocksdb::Options*>(jhandle);
  return static_cast<jlong>(opt->db_paths.size());
}

/*
 * Class:     org_rocksdb_Options
 * Method:    dbPaths
 * Signature: (J[Ljava/lang/String;[J)V
 */
void Java_org_rocksdb_Options_dbPaths(
    JNIEnv* env, jobject, jlong jhandle, jobjectArray jpaths,
    jlongArray jtarget_sizes) {
  jlong* ptr_jtarget_size = env->GetLongArrayElements(jtarget_sizes, nullptr);
  if (ptr_jtarget_size == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }

  auto* opt = reinterpret_cast<rocksdb::Options*>(jhandle);
  const jsize len = env->GetArrayLength(jpaths);
  for (jsize i = 0; i < len; i++) {
    rocksdb::DbPath db_path = opt->db_paths[i];

    jstring jpath = env->NewStringUTF(db_path.path.c_str());
    if (jpath == nullptr) {
      // exception thrown: OutOfMemoryError
      env->ReleaseLongArrayElements(jtarget_sizes, ptr_jtarget_size, JNI_ABORT);
      return;
    }
    env->SetObjectArrayElement(jpaths, i, jpath);
    if (env->ExceptionCheck()) {
      // exception thrown: ArrayIndexOutOfBoundsException
      env->DeleteLocalRef(jpath);
      env->ReleaseLongArrayElements(jtarget_sizes, ptr_jtarget_size, JNI_ABORT);
      return;
    }

    ptr_jtarget_size[i] = static_cast<jint>(db_path.target_size);
  }

  env->ReleaseLongArrayElements(jtarget_sizes, ptr_jtarget_size, JNI_COMMIT);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    dbLogDir
 * Signature: (J)Ljava/lang/String
 */
jstring Java_org_rocksdb_Options_dbLogDir(
    JNIEnv* env, jobject, jlong jhandle) {
  return env->NewStringUTF(
      reinterpret_cast<rocksdb::Options*>(jhandle)->db_log_dir.c_str());
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setDbLogDir
 * Signature: (JLjava/lang/String)V
 */
void Java_org_rocksdb_Options_setDbLogDir(
    JNIEnv* env, jobject, jlong jhandle, jstring jdb_log_dir) {
  const char* log_dir = env->GetStringUTFChars(jdb_log_dir, nullptr);
  if (log_dir == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }
  reinterpret_cast<rocksdb::Options*>(jhandle)->db_log_dir.assign(log_dir);
  env->ReleaseStringUTFChars(jdb_log_dir, log_dir);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    walDir
 * Signature: (J)Ljava/lang/String
 */
jstring Java_org_rocksdb_Options_walDir(
    JNIEnv* env, jobject, jlong jhandle) {
  return env->NewStringUTF(
      reinterpret_cast<rocksdb::Options*>(jhandle)->wal_dir.c_str());
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setWalDir
 * Signature: (JLjava/lang/String)V
 */
void Java_org_rocksdb_Options_setWalDir(
    JNIEnv* env, jobject, jlong jhandle, jstring jwal_dir) {
  const char* wal_dir = env->GetStringUTFChars(jwal_dir, nullptr);
  if (wal_dir == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }
  reinterpret_cast<rocksdb::Options*>(jhandle)->wal_dir.assign(wal_dir);
  env->ReleaseStringUTFChars(jwal_dir, wal_dir);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    deleteObsoleteFilesPeriodMicros
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_deleteObsoleteFilesPeriodMicros(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)
      ->delete_obsolete_files_period_micros;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setDeleteObsoleteFilesPeriodMicros
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setDeleteObsoleteFilesPeriodMicros(
    JNIEnv*, jobject, jlong jhandle, jlong micros) {
  reinterpret_cast<rocksdb::Options*>(jhandle)
      ->delete_obsolete_files_period_micros = static_cast<int64_t>(micros);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setBaseBackgroundCompactions
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setBaseBackgroundCompactions(
    JNIEnv*, jobject, jlong jhandle, jint max) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->base_background_compactions =
      static_cast<int>(max);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    baseBackgroundCompactions
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_baseBackgroundCompactions(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)
      ->base_background_compactions;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxBackgroundCompactions
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_maxBackgroundCompactions(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)
      ->max_background_compactions;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxBackgroundCompactions
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setMaxBackgroundCompactions(
    JNIEnv*, jobject, jlong jhandle, jint max) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->max_background_compactions =
      static_cast<int>(max);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxSubcompactions
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setMaxSubcompactions(
    JNIEnv*, jobject, jlong jhandle, jint max) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->max_subcompactions =
      static_cast<int32_t>(max);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxSubcompactions
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_maxSubcompactions(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->max_subcompactions;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxBackgroundFlushes
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_maxBackgroundFlushes(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->max_background_flushes;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxBackgroundFlushes
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setMaxBackgroundFlushes(
    JNIEnv*, jobject, jlong jhandle, jint max_background_flushes) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->max_background_flushes =
      static_cast<int>(max_background_flushes);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxBackgroundJobs
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_maxBackgroundJobs(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->max_background_jobs;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxBackgroundJobs
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setMaxBackgroundJobs(
    JNIEnv*, jobject, jlong jhandle, jint max_background_jobs) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->max_background_jobs =
      static_cast<int>(max_background_jobs);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxLogFileSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_maxLogFileSize(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->max_log_file_size;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxLogFileSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setMaxLogFileSize(
    JNIEnv* env, jobject, jlong jhandle, jlong max_log_file_size) {
  auto s = rocksdb::JniUtil::check_if_jlong_fits_size_t(max_log_file_size);
  if (s.ok()) {
    reinterpret_cast<rocksdb::Options*>(jhandle)->max_log_file_size =
        max_log_file_size;
  } else {
    rocksdb::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_Options
 * Method:    logFileTimeToRoll
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_logFileTimeToRoll(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->log_file_time_to_roll;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setLogFileTimeToRoll
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setLogFileTimeToRoll(
    JNIEnv* env, jobject, jlong jhandle, jlong log_file_time_to_roll) {
  auto s =
      rocksdb::JniUtil::check_if_jlong_fits_size_t(log_file_time_to_roll);
  if (s.ok()) {
    reinterpret_cast<rocksdb::Options*>(jhandle)->log_file_time_to_roll =
        log_file_time_to_roll;
  } else {
    rocksdb::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_Options
 * Method:    keepLogFileNum
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_keepLogFileNum(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->keep_log_file_num;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setKeepLogFileNum
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setKeepLogFileNum(
    JNIEnv* env, jobject, jlong jhandle, jlong keep_log_file_num) {
  auto s = rocksdb::JniUtil::check_if_jlong_fits_size_t(keep_log_file_num);
  if (s.ok()) {
    reinterpret_cast<rocksdb::Options*>(jhandle)->keep_log_file_num =
        keep_log_file_num;
  } else {
    rocksdb::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_Options
 * Method:    recycleLogFileNum
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_recycleLogFileNum(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->recycle_log_file_num;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setRecycleLogFileNum
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setRecycleLogFileNum(
    JNIEnv* env, jobject, jlong jhandle, jlong recycle_log_file_num) {
  auto s = rocksdb::JniUtil::check_if_jlong_fits_size_t(recycle_log_file_num);
  if (s.ok()) {
    reinterpret_cast<rocksdb::Options*>(jhandle)->recycle_log_file_num =
        recycle_log_file_num;
  } else {
    rocksdb::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxManifestFileSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_maxManifestFileSize(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->max_manifest_file_size;
}

/*
 * Method:    memTableFactoryName
 * Signature: (J)Ljava/lang/String
 */
jstring Java_org_rocksdb_Options_memTableFactoryName(
    JNIEnv* env, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<rocksdb::Options*>(jhandle);
  rocksdb::MemTableRepFactory* tf = opt->memtable_factory.get();

  // Should never be nullptr.
  // Default memtable factory is SkipListFactory
  assert(tf);

  // temporarly fix for the historical typo
  if (strcmp(tf->Name(), "HashLinkListRepFactory") == 0) {
    return env->NewStringUTF("HashLinkedListRepFactory");
  }

  return env->NewStringUTF(tf->Name());
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxManifestFileSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setMaxManifestFileSize(
    JNIEnv*, jobject, jlong jhandle, jlong max_manifest_file_size) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->max_manifest_file_size =
      static_cast<int64_t>(max_manifest_file_size);
}

/*
 * Method:    setMemTableFactory
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setMemTableFactory(
    JNIEnv*, jobject, jlong jhandle, jlong jfactory_handle) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->memtable_factory.reset(
      reinterpret_cast<rocksdb::MemTableRepFactory*>(jfactory_handle));
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setRateLimiter
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setRateLimiter(
    JNIEnv*, jobject, jlong jhandle, jlong jrate_limiter_handle) {
  std::shared_ptr<rocksdb::RateLimiter>* pRateLimiter =
      reinterpret_cast<std::shared_ptr<rocksdb::RateLimiter>*>(
          jrate_limiter_handle);
  reinterpret_cast<rocksdb::Options*>(jhandle)->rate_limiter = *pRateLimiter;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setSstFileManager
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setSstFileManager(
    JNIEnv*, jobject, jlong jhandle, jlong jsst_file_manager_handle) {
  auto* sptr_sst_file_manager =
      reinterpret_cast<std::shared_ptr<rocksdb::SstFileManager>*>(
          jsst_file_manager_handle);
  reinterpret_cast<rocksdb::Options*>(jhandle)->sst_file_manager =
      *sptr_sst_file_manager;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setLogger
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setLogger(
    JNIEnv*, jobject, jlong jhandle, jlong jlogger_handle) {
  std::shared_ptr<rocksdb::LoggerJniCallback>* pLogger =
      reinterpret_cast<std::shared_ptr<rocksdb::LoggerJniCallback>*>(
          jlogger_handle);
  reinterpret_cast<rocksdb::Options*>(jhandle)->info_log = *pLogger;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setInfoLogLevel
 * Signature: (JB)V
 */
void Java_org_rocksdb_Options_setInfoLogLevel(
    JNIEnv*, jobject, jlong jhandle, jbyte jlog_level) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->info_log_level =
      static_cast<rocksdb::InfoLogLevel>(jlog_level);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    infoLogLevel
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_Options_infoLogLevel(
    JNIEnv*, jobject, jlong jhandle) {
  return static_cast<jbyte>(
      reinterpret_cast<rocksdb::Options*>(jhandle)->info_log_level);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    tableCacheNumshardbits
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_tableCacheNumshardbits(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->table_cache_numshardbits;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setTableCacheNumshardbits
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setTableCacheNumshardbits(
    JNIEnv*, jobject, jlong jhandle, jint table_cache_numshardbits) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->table_cache_numshardbits =
      static_cast<int>(table_cache_numshardbits);
}

/*
 * Method:    useFixedLengthPrefixExtractor
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_useFixedLengthPrefixExtractor(
    JNIEnv*, jobject, jlong jhandle, jint jprefix_length) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->prefix_extractor.reset(
      rocksdb::NewFixedPrefixTransform(static_cast<int>(jprefix_length)));
}

/*
 * Method:    useCappedPrefixExtractor
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_useCappedPrefixExtractor(
    JNIEnv*, jobject, jlong jhandle, jint jprefix_length) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->prefix_extractor.reset(
      rocksdb::NewCappedPrefixTransform(static_cast<int>(jprefix_length)));
}

/*
 * Class:     org_rocksdb_Options
 * Method:    walTtlSeconds
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_walTtlSeconds(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->WAL_ttl_seconds;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setWalTtlSeconds
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setWalTtlSeconds(
    JNIEnv*, jobject, jlong jhandle, jlong WAL_ttl_seconds) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->WAL_ttl_seconds =
      static_cast<int64_t>(WAL_ttl_seconds);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    walTtlSeconds
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_walSizeLimitMB(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->WAL_size_limit_MB;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setWalSizeLimitMB
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setWalSizeLimitMB(
    JNIEnv*, jobject, jlong jhandle, jlong WAL_size_limit_MB) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->WAL_size_limit_MB =
      static_cast<int64_t>(WAL_size_limit_MB);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    manifestPreallocationSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_manifestPreallocationSize(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)
      ->manifest_preallocation_size;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setManifestPreallocationSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setManifestPreallocationSize(
    JNIEnv* env, jobject, jlong jhandle, jlong preallocation_size) {
  auto s = rocksdb::JniUtil::check_if_jlong_fits_size_t(preallocation_size);
  if (s.ok()) {
    reinterpret_cast<rocksdb::Options*>(jhandle)->manifest_preallocation_size =
        preallocation_size;
  } else {
    rocksdb::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Method:    setTableFactory
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setTableFactory(
    JNIEnv*, jobject, jlong jhandle, jlong jtable_factory_handle) {
  auto* options = reinterpret_cast<rocksdb::Options*>(jhandle);
  auto* table_factory =
      reinterpret_cast<rocksdb::TableFactory*>(jtable_factory_handle);
  options->table_factory.reset(table_factory);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    allowMmapReads
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_allowMmapReads(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->allow_mmap_reads;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setAllowMmapReads
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setAllowMmapReads(
    JNIEnv*, jobject, jlong jhandle, jboolean allow_mmap_reads) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->allow_mmap_reads =
      static_cast<bool>(allow_mmap_reads);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    allowMmapWrites
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_allowMmapWrites(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->allow_mmap_writes;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setAllowMmapWrites
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setAllowMmapWrites(
    JNIEnv*, jobject, jlong jhandle, jboolean allow_mmap_writes) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->allow_mmap_writes =
      static_cast<bool>(allow_mmap_writes);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    useDirectReads
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_useDirectReads(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->use_direct_reads;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setUseDirectReads
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setUseDirectReads(
    JNIEnv*, jobject, jlong jhandle, jboolean use_direct_reads) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->use_direct_reads =
      static_cast<bool>(use_direct_reads);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    useDirectIoForFlushAndCompaction
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_useDirectIoForFlushAndCompaction(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)
      ->use_direct_io_for_flush_and_compaction;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setUseDirectIoForFlushAndCompaction
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setUseDirectIoForFlushAndCompaction(
    JNIEnv*, jobject, jlong jhandle,
    jboolean use_direct_io_for_flush_and_compaction) {
  reinterpret_cast<rocksdb::Options*>(jhandle)
      ->use_direct_io_for_flush_and_compaction =
      static_cast<bool>(use_direct_io_for_flush_and_compaction);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setAllowFAllocate
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setAllowFAllocate(
    JNIEnv*, jobject, jlong jhandle, jboolean jallow_fallocate) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->allow_fallocate =
      static_cast<bool>(jallow_fallocate);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    allowFAllocate
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_allowFAllocate(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<rocksdb::Options*>(jhandle);
  return static_cast<jboolean>(opt->allow_fallocate);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    isFdCloseOnExec
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_isFdCloseOnExec(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->is_fd_close_on_exec;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setIsFdCloseOnExec
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setIsFdCloseOnExec(
    JNIEnv*, jobject, jlong jhandle, jboolean is_fd_close_on_exec) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->is_fd_close_on_exec =
      static_cast<bool>(is_fd_close_on_exec);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    statsDumpPeriodSec
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_statsDumpPeriodSec(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->stats_dump_period_sec;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setStatsDumpPeriodSec
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setStatsDumpPeriodSec(
    JNIEnv*, jobject, jlong jhandle,
    jint stats_dump_period_sec) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->stats_dump_period_sec =
      static_cast<int>(stats_dump_period_sec);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    adviseRandomOnOpen
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_adviseRandomOnOpen(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->advise_random_on_open;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setAdviseRandomOnOpen
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setAdviseRandomOnOpen(
    JNIEnv*, jobject, jlong jhandle,
    jboolean advise_random_on_open) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->advise_random_on_open =
      static_cast<bool>(advise_random_on_open);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setDbWriteBufferSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setDbWriteBufferSize(
    JNIEnv*, jobject, jlong jhandle,
    jlong jdb_write_buffer_size) {
  auto* opt = reinterpret_cast<rocksdb::Options*>(jhandle);
  opt->db_write_buffer_size = static_cast<size_t>(jdb_write_buffer_size);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    dbWriteBufferSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_dbWriteBufferSize(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<rocksdb::Options*>(jhandle);
  return static_cast<jlong>(opt->db_write_buffer_size);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setAccessHintOnCompactionStart
 * Signature: (JB)V
 */
void Java_org_rocksdb_Options_setAccessHintOnCompactionStart(
    JNIEnv*, jobject, jlong jhandle,
    jbyte jaccess_hint_value) {
  auto* opt = reinterpret_cast<rocksdb::Options*>(jhandle);
  opt->access_hint_on_compaction_start =
      rocksdb::AccessHintJni::toCppAccessHint(jaccess_hint_value);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    accessHintOnCompactionStart
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_Options_accessHintOnCompactionStart(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<rocksdb::Options*>(jhandle);
  return rocksdb::AccessHintJni::toJavaAccessHint(
      opt->access_hint_on_compaction_start);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setNewTableReaderForCompactionInputs
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setNewTableReaderForCompactionInputs(
    JNIEnv*, jobject, jlong jhandle,
    jboolean jnew_table_reader_for_compaction_inputs) {
  auto* opt = reinterpret_cast<rocksdb::Options*>(jhandle);
  opt->new_table_reader_for_compaction_inputs =
      static_cast<bool>(jnew_table_reader_for_compaction_inputs);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    newTableReaderForCompactionInputs
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_newTableReaderForCompactionInputs(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<rocksdb::Options*>(jhandle);
  return static_cast<bool>(opt->new_table_reader_for_compaction_inputs);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setCompactionReadaheadSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setCompactionReadaheadSize(
    JNIEnv*, jobject, jlong jhandle,
    jlong jcompaction_readahead_size) {
  auto* opt = reinterpret_cast<rocksdb::Options*>(jhandle);
  opt->compaction_readahead_size =
      static_cast<size_t>(jcompaction_readahead_size);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    compactionReadaheadSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_compactionReadaheadSize(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<rocksdb::Options*>(jhandle);
  return static_cast<jlong>(opt->compaction_readahead_size);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setRandomAccessMaxBufferSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setRandomAccessMaxBufferSize(
    JNIEnv*, jobject, jlong jhandle, jlong jrandom_access_max_buffer_size) {
  auto* opt = reinterpret_cast<rocksdb::Options*>(jhandle);
  opt->random_access_max_buffer_size =
      static_cast<size_t>(jrandom_access_max_buffer_size);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    randomAccessMaxBufferSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_randomAccessMaxBufferSize(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<rocksdb::Options*>(jhandle);
  return static_cast<jlong>(opt->random_access_max_buffer_size);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setWritableFileMaxBufferSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setWritableFileMaxBufferSize(
    JNIEnv*, jobject, jlong jhandle,
    jlong jwritable_file_max_buffer_size) {
  auto* opt = reinterpret_cast<rocksdb::Options*>(jhandle);
  opt->writable_file_max_buffer_size =
      static_cast<size_t>(jwritable_file_max_buffer_size);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    writableFileMaxBufferSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_writableFileMaxBufferSize(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<rocksdb::Options*>(jhandle);
  return static_cast<jlong>(opt->writable_file_max_buffer_size);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    useAdaptiveMutex
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_useAdaptiveMutex(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->use_adaptive_mutex;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setUseAdaptiveMutex
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setUseAdaptiveMutex(
    JNIEnv*, jobject, jlong jhandle, jboolean use_adaptive_mutex) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->use_adaptive_mutex =
      static_cast<bool>(use_adaptive_mutex);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    bytesPerSync
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_bytesPerSync(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->bytes_per_sync;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setBytesPerSync
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setBytesPerSync(
    JNIEnv*, jobject, jlong jhandle, jlong bytes_per_sync) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->bytes_per_sync =
      static_cast<int64_t>(bytes_per_sync);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setWalBytesPerSync
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setWalBytesPerSync(
    JNIEnv*, jobject, jlong jhandle, jlong jwal_bytes_per_sync) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->wal_bytes_per_sync =
      static_cast<int64_t>(jwal_bytes_per_sync);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    walBytesPerSync
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_walBytesPerSync(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<rocksdb::Options*>(jhandle);
  return static_cast<jlong>(opt->wal_bytes_per_sync);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setEnableThreadTracking
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setEnableThreadTracking(
    JNIEnv*, jobject, jlong jhandle, jboolean jenable_thread_tracking) {
  auto* opt = reinterpret_cast<rocksdb::Options*>(jhandle);
  opt->enable_thread_tracking = static_cast<bool>(jenable_thread_tracking);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    enableThreadTracking
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_enableThreadTracking(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<rocksdb::Options*>(jhandle);
  return static_cast<jboolean>(opt->enable_thread_tracking);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setDelayedWriteRate
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setDelayedWriteRate(
    JNIEnv*, jobject, jlong jhandle, jlong jdelayed_write_rate) {
  auto* opt = reinterpret_cast<rocksdb::Options*>(jhandle);
  opt->delayed_write_rate = static_cast<uint64_t>(jdelayed_write_rate);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    delayedWriteRate
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_delayedWriteRate(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<rocksdb::Options*>(jhandle);
  return static_cast<jlong>(opt->delayed_write_rate);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setEnablePipelinedWrite
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setEnablePipelinedWrite(
    JNIEnv*, jobject, jlong jhandle, jboolean jenable_pipelined_write) {
  auto* opt = reinterpret_cast<rocksdb::Options*>(jhandle);
  opt->enable_pipelined_write = jenable_pipelined_write == JNI_TRUE;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    enablePipelinedWrite
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_enablePipelinedWrite(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<rocksdb::Options*>(jhandle);
  return static_cast<jboolean>(opt->enable_pipelined_write);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setAllowConcurrentMemtableWrite
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setAllowConcurrentMemtableWrite(
    JNIEnv*, jobject, jlong jhandle, jboolean allow) {
  reinterpret_cast<rocksdb::Options*>(jhandle)
      ->allow_concurrent_memtable_write = static_cast<bool>(allow);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    allowConcurrentMemtableWrite
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_allowConcurrentMemtableWrite(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)
      ->allow_concurrent_memtable_write;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setEnableWriteThreadAdaptiveYield
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setEnableWriteThreadAdaptiveYield(
    JNIEnv*, jobject, jlong jhandle, jboolean yield) {
  reinterpret_cast<rocksdb::Options*>(jhandle)
      ->enable_write_thread_adaptive_yield = static_cast<bool>(yield);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    enableWriteThreadAdaptiveYield
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_enableWriteThreadAdaptiveYield(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)
      ->enable_write_thread_adaptive_yield;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setWriteThreadMaxYieldUsec
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setWriteThreadMaxYieldUsec(
    JNIEnv*, jobject, jlong jhandle, jlong max) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->write_thread_max_yield_usec =
      static_cast<int64_t>(max);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    writeThreadMaxYieldUsec
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_writeThreadMaxYieldUsec(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)
      ->write_thread_max_yield_usec;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setWriteThreadSlowYieldUsec
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setWriteThreadSlowYieldUsec(
    JNIEnv*, jobject, jlong jhandle, jlong slow) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->write_thread_slow_yield_usec =
      static_cast<int64_t>(slow);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    writeThreadSlowYieldUsec
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_writeThreadSlowYieldUsec(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)
      ->write_thread_slow_yield_usec;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setSkipStatsUpdateOnDbOpen
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setSkipStatsUpdateOnDbOpen(
    JNIEnv*, jobject, jlong jhandle,
    jboolean jskip_stats_update_on_db_open) {
  auto* opt = reinterpret_cast<rocksdb::Options*>(jhandle);
  opt->skip_stats_update_on_db_open =
      static_cast<bool>(jskip_stats_update_on_db_open);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    skipStatsUpdateOnDbOpen
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_skipStatsUpdateOnDbOpen(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<rocksdb::Options*>(jhandle);
  return static_cast<jboolean>(opt->skip_stats_update_on_db_open);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setWalRecoveryMode
 * Signature: (JB)V
 */
void Java_org_rocksdb_Options_setWalRecoveryMode(
    JNIEnv*, jobject, jlong jhandle,
    jbyte jwal_recovery_mode_value) {
  auto* opt = reinterpret_cast<rocksdb::Options*>(jhandle);
  opt->wal_recovery_mode = rocksdb::WALRecoveryModeJni::toCppWALRecoveryMode(
      jwal_recovery_mode_value);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    walRecoveryMode
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_Options_walRecoveryMode(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<rocksdb::Options*>(jhandle);
  return rocksdb::WALRecoveryModeJni::toJavaWALRecoveryMode(
      opt->wal_recovery_mode);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setAllow2pc
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setAllow2pc(
    JNIEnv*, jobject, jlong jhandle, jboolean jallow_2pc) {
  auto* opt = reinterpret_cast<rocksdb::Options*>(jhandle);
  opt->allow_2pc = static_cast<bool>(jallow_2pc);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    allow2pc
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_allow2pc(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<rocksdb::Options*>(jhandle);
  return static_cast<jboolean>(opt->allow_2pc);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setRowCache
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setRowCache(
    JNIEnv*, jobject, jlong jhandle, jlong jrow_cache_handle) {
  auto* opt = reinterpret_cast<rocksdb::Options*>(jhandle);
  auto* row_cache =
      reinterpret_cast<std::shared_ptr<rocksdb::Cache>*>(jrow_cache_handle);
  opt->row_cache = *row_cache;
}


/*
 * Class:     org_rocksdb_Options
 * Method:    setWalFilter
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setWalFilter(
    JNIEnv*, jobject, jlong jhandle, jlong jwal_filter_handle) {
  auto* opt = reinterpret_cast<rocksdb::Options*>(jhandle);
  auto* wal_filter =
      reinterpret_cast<rocksdb::WalFilterJniCallback*>(jwal_filter_handle);
  opt->wal_filter = wal_filter;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setFailIfOptionsFileError
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setFailIfOptionsFileError(
    JNIEnv*, jobject, jlong jhandle, jboolean jfail_if_options_file_error) {
  auto* opt = reinterpret_cast<rocksdb::Options*>(jhandle);
  opt->fail_if_options_file_error =
      static_cast<bool>(jfail_if_options_file_error);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    failIfOptionsFileError
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_failIfOptionsFileError(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<rocksdb::Options*>(jhandle);
  return static_cast<jboolean>(opt->fail_if_options_file_error);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setDumpMallocStats
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setDumpMallocStats(
    JNIEnv*, jobject, jlong jhandle, jboolean jdump_malloc_stats) {
  auto* opt = reinterpret_cast<rocksdb::Options*>(jhandle);
  opt->dump_malloc_stats = static_cast<bool>(jdump_malloc_stats);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    dumpMallocStats
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_dumpMallocStats(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<rocksdb::Options*>(jhandle);
  return static_cast<jboolean>(opt->dump_malloc_stats);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setAvoidFlushDuringRecovery
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setAvoidFlushDuringRecovery(
    JNIEnv*, jobject, jlong jhandle, jboolean javoid_flush_during_recovery) {
  auto* opt = reinterpret_cast<rocksdb::Options*>(jhandle);
  opt->avoid_flush_during_recovery =
      static_cast<bool>(javoid_flush_during_recovery);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    avoidFlushDuringRecovery
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_avoidFlushDuringRecovery(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<rocksdb::Options*>(jhandle);
  return static_cast<jboolean>(opt->avoid_flush_during_recovery);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setAvoidFlushDuringShutdown
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setAvoidFlushDuringShutdown(
    JNIEnv*, jobject, jlong jhandle, jboolean javoid_flush_during_shutdown) {
  auto* opt = reinterpret_cast<rocksdb::Options*>(jhandle);
  opt->avoid_flush_during_shutdown =
      static_cast<bool>(javoid_flush_during_shutdown);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    avoidFlushDuringShutdown
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_avoidFlushDuringShutdown(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<rocksdb::Options*>(jhandle);
  return static_cast<jboolean>(opt->avoid_flush_during_shutdown);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setAllowIngestBehind
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setAllowIngestBehind(
    JNIEnv*, jobject, jlong jhandle, jboolean jallow_ingest_behind) {
  auto* opt = reinterpret_cast<rocksdb::Options*>(jhandle);
  opt->allow_ingest_behind = jallow_ingest_behind == JNI_TRUE;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    allowIngestBehind
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_allowIngestBehind(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<rocksdb::Options*>(jhandle);
  return static_cast<jboolean>(opt->allow_ingest_behind);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setPreserveDeletes
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setPreserveDeletes(
    JNIEnv*, jobject, jlong jhandle, jboolean jpreserve_deletes) {
  auto* opt = reinterpret_cast<rocksdb::Options*>(jhandle);
  opt->preserve_deletes = jpreserve_deletes == JNI_TRUE;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    preserveDeletes
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_preserveDeletes(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<rocksdb::Options*>(jhandle);
  return static_cast<jboolean>(opt->preserve_deletes);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setTwoWriteQueues
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setTwoWriteQueues(
    JNIEnv*, jobject, jlong jhandle, jboolean jtwo_write_queues) {
  auto* opt = reinterpret_cast<rocksdb::Options*>(jhandle);
  opt->two_write_queues = jtwo_write_queues == JNI_TRUE;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    twoWriteQueues
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_twoWriteQueues(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<rocksdb::Options*>(jhandle);
  return static_cast<jboolean>(opt->two_write_queues);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setManualWalFlush
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setManualWalFlush(
    JNIEnv*, jobject, jlong jhandle, jboolean jmanual_wal_flush) {
  auto* opt = reinterpret_cast<rocksdb::Options*>(jhandle);
  opt->manual_wal_flush = jmanual_wal_flush == JNI_TRUE;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    manualWalFlush
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_manualWalFlush(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<rocksdb::Options*>(jhandle);
  return static_cast<jboolean>(opt->manual_wal_flush);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setAtomicFlush
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setAtomicFlush(
    JNIEnv*, jobject, jlong jhandle, jboolean jatomic_flush) {
  auto* opt = reinterpret_cast<rocksdb::Options*>(jhandle);
  opt->atomic_flush = jatomic_flush == JNI_TRUE;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    atomicFlush
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_atomicFlush(
    JNIEnv *, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<rocksdb::Options*>(jhandle);
  return static_cast<jboolean>(opt->atomic_flush);
}

/*
 * Method:    tableFactoryName
 * Signature: (J)Ljava/lang/String
 */
jstring Java_org_rocksdb_Options_tableFactoryName(
    JNIEnv* env, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<rocksdb::Options*>(jhandle);
  rocksdb::TableFactory* tf = opt->table_factory.get();

  // Should never be nullptr.
  // Default memtable factory is SkipListFactory
  assert(tf);

  return env->NewStringUTF(tf->Name());
}

/*
 * Class:     org_rocksdb_Options
 * Method:    minWriteBufferNumberToMerge
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_minWriteBufferNumberToMerge(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)
      ->min_write_buffer_number_to_merge;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMinWriteBufferNumberToMerge
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setMinWriteBufferNumberToMerge(
    JNIEnv*, jobject, jlong jhandle, jint jmin_write_buffer_number_to_merge) {
  reinterpret_cast<rocksdb::Options*>(jhandle)
      ->min_write_buffer_number_to_merge =
      static_cast<int>(jmin_write_buffer_number_to_merge);
}
/*
 * Class:     org_rocksdb_Options
 * Method:    maxWriteBufferNumberToMaintain
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_maxWriteBufferNumberToMaintain(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)
      ->max_write_buffer_number_to_maintain;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxWriteBufferNumberToMaintain
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setMaxWriteBufferNumberToMaintain(
    JNIEnv*, jobject, jlong jhandle,
    jint jmax_write_buffer_number_to_maintain) {
  reinterpret_cast<rocksdb::Options*>(jhandle)
      ->max_write_buffer_number_to_maintain =
      static_cast<int>(jmax_write_buffer_number_to_maintain);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setCompressionType
 * Signature: (JB)V
 */
void Java_org_rocksdb_Options_setCompressionType(
    JNIEnv*, jobject, jlong jhandle, jbyte jcompression_type_value) {
  auto* opts = reinterpret_cast<rocksdb::Options*>(jhandle);
  opts->compression = rocksdb::CompressionTypeJni::toCppCompressionType(
      jcompression_type_value);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    compressionType
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_Options_compressionType(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opts = reinterpret_cast<rocksdb::Options*>(jhandle);
  return rocksdb::CompressionTypeJni::toJavaCompressionType(opts->compression);
}

/**
 * Helper method to convert a Java byte array of compression levels
 * to a C++ vector of rocksdb::CompressionType
 *
 * @param env A pointer to the Java environment
 * @param jcompression_levels A reference to a java byte array
 *     where each byte indicates a compression level
 *
 * @return A std::unique_ptr to the vector, or std::unique_ptr(nullptr) if a JNI
 * exception occurs
 */
std::unique_ptr<std::vector<rocksdb::CompressionType>>rocksdb_compression_vector_helper(
    JNIEnv* env, jbyteArray jcompression_levels) {
  jsize len = env->GetArrayLength(jcompression_levels);
  jbyte* jcompression_level =
      env->GetByteArrayElements(jcompression_levels, nullptr);
  if (jcompression_level == nullptr) {
    // exception thrown: OutOfMemoryError
    return std::unique_ptr<std::vector<rocksdb::CompressionType>>();
  }

  auto* compression_levels = new std::vector<rocksdb::CompressionType>();
  std::unique_ptr<std::vector<rocksdb::CompressionType>>
      uptr_compression_levels(compression_levels);

  for (jsize i = 0; i < len; i++) {
    jbyte jcl = jcompression_level[i];
    compression_levels->push_back(static_cast<rocksdb::CompressionType>(jcl));
  }

  env->ReleaseByteArrayElements(jcompression_levels, jcompression_level,
                                JNI_ABORT);

  return uptr_compression_levels;
}

/**
 * Helper method to convert a C++ vector of rocksdb::CompressionType
 * to a Java byte array of compression levels
 *
 * @param env A pointer to the Java environment
 * @param jcompression_levels A reference to a java byte array
 *     where each byte indicates a compression level
 *
 * @return A jbytearray or nullptr if an exception occurs
 */
jbyteArray rocksdb_compression_list_helper(
    JNIEnv* env, std::vector<rocksdb::CompressionType> compression_levels) {
  const size_t len = compression_levels.size();
  jbyte* jbuf = new jbyte[len];

  for (size_t i = 0; i < len; i++) {
    jbuf[i] = compression_levels[i];
  }

  // insert in java array
  jbyteArray jcompression_levels = env->NewByteArray(static_cast<jsize>(len));
  if (jcompression_levels == nullptr) {
    // exception thrown: OutOfMemoryError
    delete[] jbuf;
    return nullptr;
  }
  env->SetByteArrayRegion(jcompression_levels, 0, static_cast<jsize>(len),
                          jbuf);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    env->DeleteLocalRef(jcompression_levels);
    delete[] jbuf;
    return nullptr;
  }

  delete[] jbuf;

  return jcompression_levels;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setCompressionPerLevel
 * Signature: (J[B)V
 */
void Java_org_rocksdb_Options_setCompressionPerLevel(
    JNIEnv* env, jobject, jlong jhandle, jbyteArray jcompressionLevels) {
  auto uptr_compression_levels =
      rocksdb_compression_vector_helper(env, jcompressionLevels);
  if (!uptr_compression_levels) {
    // exception occurred
    return;
  }
  auto* options = reinterpret_cast<rocksdb::Options*>(jhandle);
  options->compression_per_level = *(uptr_compression_levels.get());
}

/*
 * Class:     org_rocksdb_Options
 * Method:    compressionPerLevel
 * Signature: (J)[B
 */
jbyteArray Java_org_rocksdb_Options_compressionPerLevel(
    JNIEnv* env, jobject, jlong jhandle) {
  auto* options = reinterpret_cast<rocksdb::Options*>(jhandle);
  return rocksdb_compression_list_helper(env, options->compression_per_level);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setBottommostCompressionType
 * Signature: (JB)V
 */
void Java_org_rocksdb_Options_setBottommostCompressionType(
    JNIEnv*, jobject, jlong jhandle, jbyte jcompression_type_value) {
  auto* options = reinterpret_cast<rocksdb::Options*>(jhandle);
  options->bottommost_compression =
      rocksdb::CompressionTypeJni::toCppCompressionType(
          jcompression_type_value);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    bottommostCompressionType
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_Options_bottommostCompressionType(
    JNIEnv*, jobject, jlong jhandle) {
  auto* options = reinterpret_cast<rocksdb::Options*>(jhandle);
  return rocksdb::CompressionTypeJni::toJavaCompressionType(
      options->bottommost_compression);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setBottommostCompressionOptions
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setBottommostCompressionOptions(
    JNIEnv*, jobject, jlong jhandle,
    jlong jbottommost_compression_options_handle) {
  auto* options = reinterpret_cast<rocksdb::Options*>(jhandle);
  auto* bottommost_compression_options =
      reinterpret_cast<rocksdb::CompressionOptions*>(
          jbottommost_compression_options_handle);
  options->bottommost_compression_opts = *bottommost_compression_options;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setCompressionOptions
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setCompressionOptions(
    JNIEnv*, jobject, jlong jhandle, jlong jcompression_options_handle) {
  auto* options = reinterpret_cast<rocksdb::Options*>(jhandle);
  auto* compression_options = reinterpret_cast<rocksdb::CompressionOptions*>(
      jcompression_options_handle);
  options->compression_opts = *compression_options;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setCompactionStyle
 * Signature: (JB)V
 */
void Java_org_rocksdb_Options_setCompactionStyle(
    JNIEnv*, jobject, jlong jhandle, jbyte jcompaction_style) {
  auto* options = reinterpret_cast<rocksdb::Options*>(jhandle);
  options->compaction_style =
      rocksdb::CompactionStyleJni::toCppCompactionStyle(
          jcompaction_style);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    compactionStyle
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_Options_compactionStyle(
    JNIEnv*, jobject, jlong jhandle) {
  auto* options = reinterpret_cast<rocksdb::Options*>(jhandle);
  return rocksdb::CompactionStyleJni::toJavaCompactionStyle(
      options->compaction_style);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxTableFilesSizeFIFO
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setMaxTableFilesSizeFIFO(
    JNIEnv*, jobject, jlong jhandle, jlong jmax_table_files_size) {
  reinterpret_cast<rocksdb::Options*>(jhandle)
      ->compaction_options_fifo.max_table_files_size =
      static_cast<uint64_t>(jmax_table_files_size);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxTableFilesSizeFIFO
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_maxTableFilesSizeFIFO(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)
      ->compaction_options_fifo.max_table_files_size;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    numLevels
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_numLevels(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->num_levels;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setNumLevels
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setNumLevels(
    JNIEnv*, jobject, jlong jhandle, jint jnum_levels) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->num_levels =
      static_cast<int>(jnum_levels);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    levelZeroFileNumCompactionTrigger
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_levelZeroFileNumCompactionTrigger(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)
      ->level0_file_num_compaction_trigger;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setLevelZeroFileNumCompactionTrigger
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setLevelZeroFileNumCompactionTrigger(
    JNIEnv*, jobject, jlong jhandle,
    jint jlevel0_file_num_compaction_trigger) {
  reinterpret_cast<rocksdb::Options*>(jhandle)
      ->level0_file_num_compaction_trigger =
      static_cast<int>(jlevel0_file_num_compaction_trigger);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    levelZeroSlowdownWritesTrigger
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_levelZeroSlowdownWritesTrigger(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)
      ->level0_slowdown_writes_trigger;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setLevelSlowdownWritesTrigger
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setLevelZeroSlowdownWritesTrigger(
    JNIEnv*, jobject, jlong jhandle, jint jlevel0_slowdown_writes_trigger) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->level0_slowdown_writes_trigger =
      static_cast<int>(jlevel0_slowdown_writes_trigger);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    levelZeroStopWritesTrigger
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_levelZeroStopWritesTrigger(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)
      ->level0_stop_writes_trigger;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setLevelStopWritesTrigger
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setLevelZeroStopWritesTrigger(
    JNIEnv*, jobject, jlong jhandle, jint jlevel0_stop_writes_trigger) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->level0_stop_writes_trigger =
      static_cast<int>(jlevel0_stop_writes_trigger);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    targetFileSizeBase
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_targetFileSizeBase(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->target_file_size_base;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setTargetFileSizeBase
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setTargetFileSizeBase(
    JNIEnv*, jobject, jlong jhandle, jlong jtarget_file_size_base) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->target_file_size_base =
      static_cast<uint64_t>(jtarget_file_size_base);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    targetFileSizeMultiplier
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_targetFileSizeMultiplier(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)
      ->target_file_size_multiplier;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setTargetFileSizeMultiplier
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setTargetFileSizeMultiplier(
    JNIEnv*, jobject, jlong jhandle, jint jtarget_file_size_multiplier) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->target_file_size_multiplier =
      static_cast<int>(jtarget_file_size_multiplier);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxBytesForLevelBase
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_maxBytesForLevelBase(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->max_bytes_for_level_base;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxBytesForLevelBase
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setMaxBytesForLevelBase(
    JNIEnv*, jobject, jlong jhandle, jlong jmax_bytes_for_level_base) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->max_bytes_for_level_base =
      static_cast<int64_t>(jmax_bytes_for_level_base);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    levelCompactionDynamicLevelBytes
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_levelCompactionDynamicLevelBytes(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)
      ->level_compaction_dynamic_level_bytes;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setLevelCompactionDynamicLevelBytes
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setLevelCompactionDynamicLevelBytes(
    JNIEnv*, jobject, jlong jhandle, jboolean jenable_dynamic_level_bytes) {
  reinterpret_cast<rocksdb::Options*>(jhandle)
      ->level_compaction_dynamic_level_bytes = (jenable_dynamic_level_bytes);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxBytesForLevelMultiplier
 * Signature: (J)D
 */
jdouble Java_org_rocksdb_Options_maxBytesForLevelMultiplier(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)
      ->max_bytes_for_level_multiplier;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxBytesForLevelMultiplier
 * Signature: (JD)V
 */
void Java_org_rocksdb_Options_setMaxBytesForLevelMultiplier(
    JNIEnv*, jobject, jlong jhandle, jdouble jmax_bytes_for_level_multiplier) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->max_bytes_for_level_multiplier =
      static_cast<double>(jmax_bytes_for_level_multiplier);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxCompactionBytes
 * Signature: (J)I
 */
jlong Java_org_rocksdb_Options_maxCompactionBytes(
    JNIEnv*, jobject, jlong jhandle) {
  return static_cast<jlong>(
      reinterpret_cast<rocksdb::Options*>(jhandle)->max_compaction_bytes);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxCompactionBytes
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setMaxCompactionBytes(
    JNIEnv*, jobject, jlong jhandle, jlong jmax_compaction_bytes) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->max_compaction_bytes =
      static_cast<uint64_t>(jmax_compaction_bytes);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    arenaBlockSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_arenaBlockSize(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->arena_block_size;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setArenaBlockSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setArenaBlockSize(
    JNIEnv* env, jobject, jlong jhandle, jlong jarena_block_size) {
  auto s = rocksdb::JniUtil::check_if_jlong_fits_size_t(jarena_block_size);
  if (s.ok()) {
    reinterpret_cast<rocksdb::Options*>(jhandle)->arena_block_size =
        jarena_block_size;
  } else {
    rocksdb::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_Options
 * Method:    disableAutoCompactions
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_disableAutoCompactions(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->disable_auto_compactions;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setDisableAutoCompactions
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setDisableAutoCompactions(
    JNIEnv*, jobject, jlong jhandle, jboolean jdisable_auto_compactions) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->disable_auto_compactions =
      static_cast<bool>(jdisable_auto_compactions);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxSequentialSkipInIterations
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_maxSequentialSkipInIterations(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)
      ->max_sequential_skip_in_iterations;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxSequentialSkipInIterations
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setMaxSequentialSkipInIterations(
    JNIEnv*, jobject, jlong jhandle,
    jlong jmax_sequential_skip_in_iterations) {
  reinterpret_cast<rocksdb::Options*>(jhandle)
      ->max_sequential_skip_in_iterations =
      static_cast<int64_t>(jmax_sequential_skip_in_iterations);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    inplaceUpdateSupport
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_inplaceUpdateSupport(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->inplace_update_support;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setInplaceUpdateSupport
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setInplaceUpdateSupport(
    JNIEnv*, jobject, jlong jhandle, jboolean jinplace_update_support) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->inplace_update_support =
      static_cast<bool>(jinplace_update_support);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    inplaceUpdateNumLocks
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_inplaceUpdateNumLocks(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->inplace_update_num_locks;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setInplaceUpdateNumLocks
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setInplaceUpdateNumLocks(
    JNIEnv* env, jobject, jlong jhandle, jlong jinplace_update_num_locks) {
  auto s =
      rocksdb::JniUtil::check_if_jlong_fits_size_t(jinplace_update_num_locks);
  if (s.ok()) {
    reinterpret_cast<rocksdb::Options*>(jhandle)->inplace_update_num_locks =
        jinplace_update_num_locks;
  } else {
    rocksdb::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_Options
 * Method:    memtablePrefixBloomSizeRatio
 * Signature: (J)I
 */
jdouble Java_org_rocksdb_Options_memtablePrefixBloomSizeRatio(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)
      ->memtable_prefix_bloom_size_ratio;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMemtablePrefixBloomSizeRatio
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setMemtablePrefixBloomSizeRatio(
    JNIEnv*, jobject, jlong jhandle,
    jdouble jmemtable_prefix_bloom_size_ratio) {
  reinterpret_cast<rocksdb::Options*>(jhandle)
      ->memtable_prefix_bloom_size_ratio =
      static_cast<double>(jmemtable_prefix_bloom_size_ratio);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    bloomLocality
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_bloomLocality(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->bloom_locality;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setBloomLocality
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setBloomLocality(
    JNIEnv*, jobject, jlong jhandle, jint jbloom_locality) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->bloom_locality =
      static_cast<int32_t>(jbloom_locality);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxSuccessiveMerges
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_maxSuccessiveMerges(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->max_successive_merges;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxSuccessiveMerges
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setMaxSuccessiveMerges(
    JNIEnv* env, jobject, jlong jhandle, jlong jmax_successive_merges) {
  auto s =
      rocksdb::JniUtil::check_if_jlong_fits_size_t(jmax_successive_merges);
  if (s.ok()) {
    reinterpret_cast<rocksdb::Options*>(jhandle)->max_successive_merges =
        jmax_successive_merges;
  } else {
    rocksdb::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_Options
 * Method:    optimizeFiltersForHits
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_optimizeFiltersForHits(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)
      ->optimize_filters_for_hits;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setOptimizeFiltersForHits
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setOptimizeFiltersForHits(
    JNIEnv*, jobject, jlong jhandle, jboolean joptimize_filters_for_hits) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->optimize_filters_for_hits =
      static_cast<bool>(joptimize_filters_for_hits);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    optimizeForSmallDb
 * Signature: (J)V
 */
void Java_org_rocksdb_Options_optimizeForSmallDb(
    JNIEnv*, jobject, jlong jhandle) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->OptimizeForSmallDb();
}

/*
 * Class:     org_rocksdb_Options
 * Method:    optimizeForPointLookup
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_optimizeForPointLookup(
    JNIEnv*, jobject, jlong jhandle, jlong block_cache_size_mb) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->OptimizeForPointLookup(
      block_cache_size_mb);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    optimizeLevelStyleCompaction
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_optimizeLevelStyleCompaction(
    JNIEnv*, jobject, jlong jhandle, jlong memtable_memory_budget) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->OptimizeLevelStyleCompaction(
      memtable_memory_budget);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    optimizeUniversalStyleCompaction
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_optimizeUniversalStyleCompaction(
    JNIEnv*, jobject, jlong jhandle, jlong memtable_memory_budget) {
  reinterpret_cast<rocksdb::Options*>(jhandle)
      ->OptimizeUniversalStyleCompaction(memtable_memory_budget);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    prepareForBulkLoad
 * Signature: (J)V
 */
void Java_org_rocksdb_Options_prepareForBulkLoad(
    JNIEnv*, jobject, jlong jhandle) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->PrepareForBulkLoad();
}

/*
 * Class:     org_rocksdb_Options
 * Method:    memtableHugePageSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_memtableHugePageSize(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->memtable_huge_page_size;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMemtableHugePageSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setMemtableHugePageSize(
    JNIEnv* env, jobject, jlong jhandle, jlong jmemtable_huge_page_size) {
  auto s =
      rocksdb::JniUtil::check_if_jlong_fits_size_t(jmemtable_huge_page_size);
  if (s.ok()) {
    reinterpret_cast<rocksdb::Options*>(jhandle)->memtable_huge_page_size =
        jmemtable_huge_page_size;
  } else {
    rocksdb::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_Options
 * Method:    softPendingCompactionBytesLimit
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_softPendingCompactionBytesLimit(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)
      ->soft_pending_compaction_bytes_limit;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setSoftPendingCompactionBytesLimit
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setSoftPendingCompactionBytesLimit(
    JNIEnv*, jobject, jlong jhandle,
    jlong jsoft_pending_compaction_bytes_limit) {
  reinterpret_cast<rocksdb::Options*>(jhandle)
      ->soft_pending_compaction_bytes_limit =
      static_cast<int64_t>(jsoft_pending_compaction_bytes_limit);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    softHardCompactionBytesLimit
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_hardPendingCompactionBytesLimit(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)
      ->hard_pending_compaction_bytes_limit;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setHardPendingCompactionBytesLimit
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setHardPendingCompactionBytesLimit(
    JNIEnv*, jobject, jlong jhandle,
    jlong jhard_pending_compaction_bytes_limit) {
  reinterpret_cast<rocksdb::Options*>(jhandle)
      ->hard_pending_compaction_bytes_limit =
      static_cast<int64_t>(jhard_pending_compaction_bytes_limit);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    level0FileNumCompactionTrigger
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_level0FileNumCompactionTrigger(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)
      ->level0_file_num_compaction_trigger;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setLevel0FileNumCompactionTrigger
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setLevel0FileNumCompactionTrigger(
    JNIEnv*, jobject, jlong jhandle,
    jint jlevel0_file_num_compaction_trigger) {
  reinterpret_cast<rocksdb::Options*>(jhandle)
      ->level0_file_num_compaction_trigger =
      static_cast<int32_t>(jlevel0_file_num_compaction_trigger);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    level0SlowdownWritesTrigger
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_level0SlowdownWritesTrigger(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)
      ->level0_slowdown_writes_trigger;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setLevel0SlowdownWritesTrigger
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setLevel0SlowdownWritesTrigger(
    JNIEnv*, jobject, jlong jhandle, jint jlevel0_slowdown_writes_trigger) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->level0_slowdown_writes_trigger =
      static_cast<int32_t>(jlevel0_slowdown_writes_trigger);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    level0StopWritesTrigger
 * Signature: (J)I
 */
jint Java_org_rocksdb_Options_level0StopWritesTrigger(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)
      ->level0_stop_writes_trigger;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setLevel0StopWritesTrigger
 * Signature: (JI)V
 */
void Java_org_rocksdb_Options_setLevel0StopWritesTrigger(
    JNIEnv*, jobject, jlong jhandle, jint jlevel0_stop_writes_trigger) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->level0_stop_writes_trigger =
      static_cast<int32_t>(jlevel0_stop_writes_trigger);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    maxBytesForLevelMultiplierAdditional
 * Signature: (J)[I
 */
jintArray Java_org_rocksdb_Options_maxBytesForLevelMultiplierAdditional(
    JNIEnv* env, jobject, jlong jhandle) {
  auto mbflma = reinterpret_cast<rocksdb::Options*>(jhandle)
                    ->max_bytes_for_level_multiplier_additional;

  const size_t size = mbflma.size();

  jint* additionals = new jint[size];
  for (size_t i = 0; i < size; i++) {
    additionals[i] = static_cast<jint>(mbflma[i]);
  }

  jsize jlen = static_cast<jsize>(size);
  jintArray result = env->NewIntArray(jlen);
  if (result == nullptr) {
    // exception thrown: OutOfMemoryError
    delete[] additionals;
    return nullptr;
  }

  env->SetIntArrayRegion(result, 0, jlen, additionals);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    env->DeleteLocalRef(result);
    delete[] additionals;
    return nullptr;
  }

  delete[] additionals;

  return result;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setMaxBytesForLevelMultiplierAdditional
 * Signature: (J[I)V
 */
void Java_org_rocksdb_Options_setMaxBytesForLevelMultiplierAdditional(
    JNIEnv* env, jobject, jlong jhandle,
    jintArray jmax_bytes_for_level_multiplier_additional) {
  jsize len = env->GetArrayLength(jmax_bytes_for_level_multiplier_additional);
  jint* additionals = env->GetIntArrayElements(
      jmax_bytes_for_level_multiplier_additional, nullptr);
  if (additionals == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }

  auto* opt = reinterpret_cast<rocksdb::Options*>(jhandle);
  opt->max_bytes_for_level_multiplier_additional.clear();
  for (jsize i = 0; i < len; i++) {
    opt->max_bytes_for_level_multiplier_additional.push_back(
        static_cast<int32_t>(additionals[i]));
  }

  env->ReleaseIntArrayElements(jmax_bytes_for_level_multiplier_additional,
                               additionals, JNI_ABORT);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    paranoidFileChecks
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_paranoidFileChecks(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::Options*>(jhandle)->paranoid_file_checks;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setParanoidFileChecks
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setParanoidFileChecks(
    JNIEnv*, jobject, jlong jhandle, jboolean jparanoid_file_checks) {
  reinterpret_cast<rocksdb::Options*>(jhandle)->paranoid_file_checks =
      static_cast<bool>(jparanoid_file_checks);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setCompactionPriority
 * Signature: (JB)V
 */
void Java_org_rocksdb_Options_setCompactionPriority(
    JNIEnv*, jobject, jlong jhandle, jbyte jcompaction_priority_value) {
  auto* opts = reinterpret_cast<rocksdb::Options*>(jhandle);
  opts->compaction_pri =
      rocksdb::CompactionPriorityJni::toCppCompactionPriority(
          jcompaction_priority_value);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    compactionPriority
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_Options_compactionPriority(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opts = reinterpret_cast<rocksdb::Options*>(jhandle);
  return rocksdb::CompactionPriorityJni::toJavaCompactionPriority(
      opts->compaction_pri);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setReportBgIoStats
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setReportBgIoStats(
    JNIEnv*, jobject, jlong jhandle, jboolean jreport_bg_io_stats) {
  auto* opts = reinterpret_cast<rocksdb::Options*>(jhandle);
  opts->report_bg_io_stats = static_cast<bool>(jreport_bg_io_stats);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    reportBgIoStats
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_reportBgIoStats(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opts = reinterpret_cast<rocksdb::Options*>(jhandle);
  return static_cast<bool>(opts->report_bg_io_stats);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setTtl
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setTtl(
    JNIEnv*, jobject, jlong jhandle, jlong jttl) {
  auto* opts = reinterpret_cast<rocksdb::Options*>(jhandle);
  opts->ttl = static_cast<uint64_t>(jttl);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    ttl
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Options_ttl(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opts = reinterpret_cast<rocksdb::Options*>(jhandle);
  return static_cast<jlong>(opts->ttl);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setCompactionOptionsUniversal
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setCompactionOptionsUniversal(
    JNIEnv*, jobject, jlong jhandle,
    jlong jcompaction_options_universal_handle) {
  auto* opts = reinterpret_cast<rocksdb::Options*>(jhandle);
  auto* opts_uni = reinterpret_cast<rocksdb::CompactionOptionsUniversal*>(
      jcompaction_options_universal_handle);
  opts->compaction_options_universal = *opts_uni;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setCompactionOptionsFIFO
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Options_setCompactionOptionsFIFO(
    JNIEnv*, jobject, jlong jhandle, jlong jcompaction_options_fifo_handle) {
  auto* opts = reinterpret_cast<rocksdb::Options*>(jhandle);
  auto* opts_fifo = reinterpret_cast<rocksdb::CompactionOptionsFIFO*>(
      jcompaction_options_fifo_handle);
  opts->compaction_options_fifo = *opts_fifo;
}

/*
 * Class:     org_rocksdb_Options
 * Method:    setForceConsistencyChecks
 * Signature: (JZ)V
 */
void Java_org_rocksdb_Options_setForceConsistencyChecks(
    JNIEnv*, jobject, jlong jhandle, jboolean jforce_consistency_checks) {
  auto* opts = reinterpret_cast<rocksdb::Options*>(jhandle);
  opts->force_consistency_checks = static_cast<bool>(jforce_consistency_checks);
}

/*
 * Class:     org_rocksdb_Options
 * Method:    forceConsistencyChecks
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_Options_forceConsistencyChecks(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opts = reinterpret_cast<rocksdb::Options*>(jhandle);
  return static_cast<bool>(opts->force_consistency_checks);
}

//////////////////////////////////////////////////////////////////////////////
// rocksdb::ColumnFamilyOptions

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    newColumnFamilyOptions
 * Signature: ()J
 */
jlong Java_org_rocksdb_ColumnFamilyOptions_newColumnFamilyOptions(
    JNIEnv*, jclass) {
  auto* op = new rocksdb::ColumnFamilyOptions();
  return reinterpret_cast<jlong>(op);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    copyColumnFamilyOptions
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ColumnFamilyOptions_copyColumnFamilyOptions(
    JNIEnv*, jclass, jlong jhandle) {
  auto new_opt = new rocksdb::ColumnFamilyOptions(
      *(reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)));
  return reinterpret_cast<jlong>(new_opt);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    newColumnFamilyOptionsFromOptions
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ColumnFamilyOptions_newColumnFamilyOptionsFromOptions(
    JNIEnv*, jclass, jlong joptions_handle) {
  auto new_opt = new rocksdb::ColumnFamilyOptions(
      *reinterpret_cast<rocksdb::Options*>(joptions_handle));
  return reinterpret_cast<jlong>(new_opt);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    getColumnFamilyOptionsFromProps
 * Signature: (Ljava/util/String;)J
 */
jlong Java_org_rocksdb_ColumnFamilyOptions_getColumnFamilyOptionsFromProps(
    JNIEnv* env, jclass, jstring jopt_string) {
  const char* opt_string = env->GetStringUTFChars(jopt_string, nullptr);
  if (opt_string == nullptr) {
    // exception thrown: OutOfMemoryError
    return 0;
  }

  auto* cf_options = new rocksdb::ColumnFamilyOptions();
  rocksdb::Status status = rocksdb::GetColumnFamilyOptionsFromString(
      rocksdb::ColumnFamilyOptions(), opt_string, cf_options);

  env->ReleaseStringUTFChars(jopt_string, opt_string);

  // Check if ColumnFamilyOptions creation was possible.
  jlong ret_value = 0;
  if (status.ok()) {
    ret_value = reinterpret_cast<jlong>(cf_options);
  } else {
    // if operation failed the ColumnFamilyOptions need to be deleted
    // again to prevent a memory leak.
    delete cf_options;
  }
  return ret_value;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_disposeInternal(
    JNIEnv*, jobject, jlong handle) {
  auto* cfo = reinterpret_cast<rocksdb::ColumnFamilyOptions*>(handle);
  assert(cfo != nullptr);
  delete cfo;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    optimizeForSmallDb
 * Signature: (J)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_optimizeForSmallDb(
    JNIEnv*, jobject, jlong jhandle) {
  reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)
      ->OptimizeForSmallDb();
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    optimizeForPointLookup
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_optimizeForPointLookup(
    JNIEnv*, jobject, jlong jhandle, jlong block_cache_size_mb) {
  reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)
      ->OptimizeForPointLookup(block_cache_size_mb);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    optimizeLevelStyleCompaction
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_optimizeLevelStyleCompaction(
    JNIEnv*, jobject, jlong jhandle, jlong memtable_memory_budget) {
  reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)
      ->OptimizeLevelStyleCompaction(memtable_memory_budget);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    optimizeUniversalStyleCompaction
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_optimizeUniversalStyleCompaction(
    JNIEnv*, jobject, jlong jhandle, jlong memtable_memory_budget) {
  reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)
      ->OptimizeUniversalStyleCompaction(memtable_memory_budget);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setComparatorHandle
 * Signature: (JI)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setComparatorHandle__JI(
    JNIEnv*, jobject, jlong jhandle, jint builtinComparator) {
  switch (builtinComparator) {
    case 1:
      reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)->comparator =
          rocksdb::ReverseBytewiseComparator();
      break;
    default:
      reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)->comparator =
          rocksdb::BytewiseComparator();
      break;
  }
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setComparatorHandle
 * Signature: (JJB)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setComparatorHandle__JJB(
    JNIEnv*, jobject, jlong jopt_handle, jlong jcomparator_handle,
    jbyte jcomparator_type) {
  rocksdb::Comparator* comparator = nullptr;
  switch (jcomparator_type) {
    // JAVA_COMPARATOR
    case 0x0:
      comparator =
          reinterpret_cast<rocksdb::ComparatorJniCallback*>(jcomparator_handle);
      break;

    // JAVA_DIRECT_COMPARATOR
    case 0x1:
      comparator = reinterpret_cast<rocksdb::DirectComparatorJniCallback*>(
          jcomparator_handle);
      break;

    // JAVA_NATIVE_COMPARATOR_WRAPPER
    case 0x2:
      comparator = reinterpret_cast<rocksdb::Comparator*>(jcomparator_handle);
      break;
  }
  auto* opt = reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jopt_handle);
  opt->comparator = comparator;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setMergeOperatorName
 * Signature: (JJjava/lang/String)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setMergeOperatorName(
    JNIEnv* env, jobject, jlong jhandle, jstring jop_name) {
  auto* options = reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle);
  const char* op_name = env->GetStringUTFChars(jop_name, nullptr);
  if (op_name == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }

  options->merge_operator =
      rocksdb::MergeOperators::CreateFromStringId(op_name);
  env->ReleaseStringUTFChars(jop_name, op_name);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setMergeOperator
 * Signature: (JJjava/lang/String)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setMergeOperator(
    JNIEnv*, jobject, jlong jhandle, jlong mergeOperatorHandle) {
  reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)->merge_operator =
      *(reinterpret_cast<std::shared_ptr<rocksdb::MergeOperator>*>(
          mergeOperatorHandle));
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setCompactionFilterHandle
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setCompactionFilterHandle(
    JNIEnv*, jobject, jlong jopt_handle, jlong jcompactionfilter_handle) {
  reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jopt_handle)
      ->compaction_filter =
      reinterpret_cast<rocksdb::CompactionFilter*>(jcompactionfilter_handle);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setCompactionFilterFactoryHandle
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setCompactionFilterFactoryHandle(
    JNIEnv*, jobject, jlong jopt_handle,
    jlong jcompactionfilterfactory_handle) {
  auto* cff_factory =
      reinterpret_cast<std::shared_ptr<rocksdb::CompactionFilterFactoryJniCallback>*>(
          jcompactionfilterfactory_handle);
  reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jopt_handle)
      ->compaction_filter_factory = *cff_factory;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setWriteBufferSize
 * Signature: (JJ)I
 */
void Java_org_rocksdb_ColumnFamilyOptions_setWriteBufferSize(
    JNIEnv* env, jobject, jlong jhandle, jlong jwrite_buffer_size) {
  auto s = rocksdb::JniUtil::check_if_jlong_fits_size_t(jwrite_buffer_size);
  if (s.ok()) {
    reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)
        ->write_buffer_size = jwrite_buffer_size;
  } else {
    rocksdb::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    writeBufferSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ColumnFamilyOptions_writeBufferSize(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)
      ->write_buffer_size;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setMaxWriteBufferNumber
 * Signature: (JI)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setMaxWriteBufferNumber(
    JNIEnv*, jobject, jlong jhandle, jint jmax_write_buffer_number) {
  reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)
      ->max_write_buffer_number = jmax_write_buffer_number;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    maxWriteBufferNumber
 * Signature: (J)I
 */
jint Java_org_rocksdb_ColumnFamilyOptions_maxWriteBufferNumber(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)
      ->max_write_buffer_number;
}

/*
 * Method:    setMemTableFactory
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setMemTableFactory(
    JNIEnv*, jobject, jlong jhandle, jlong jfactory_handle) {
  reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)
      ->memtable_factory.reset(
          reinterpret_cast<rocksdb::MemTableRepFactory*>(jfactory_handle));
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    memTableFactoryName
 * Signature: (J)Ljava/lang/String
 */
jstring Java_org_rocksdb_ColumnFamilyOptions_memTableFactoryName(
    JNIEnv* env, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle);
  rocksdb::MemTableRepFactory* tf = opt->memtable_factory.get();

  // Should never be nullptr.
  // Default memtable factory is SkipListFactory
  assert(tf);

  // temporarly fix for the historical typo
  if (strcmp(tf->Name(), "HashLinkListRepFactory") == 0) {
    return env->NewStringUTF("HashLinkedListRepFactory");
  }

  return env->NewStringUTF(tf->Name());
}

/*
 * Method:    useFixedLengthPrefixExtractor
 * Signature: (JI)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_useFixedLengthPrefixExtractor(
    JNIEnv*, jobject, jlong jhandle, jint jprefix_length) {
  reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)
      ->prefix_extractor.reset(
          rocksdb::NewFixedPrefixTransform(static_cast<int>(jprefix_length)));
}

/*
 * Method:    useCappedPrefixExtractor
 * Signature: (JI)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_useCappedPrefixExtractor(
    JNIEnv*, jobject, jlong jhandle, jint jprefix_length) {
  reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)
      ->prefix_extractor.reset(
          rocksdb::NewCappedPrefixTransform(static_cast<int>(jprefix_length)));
}

/*
 * Method:    setTableFactory
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setTableFactory(
    JNIEnv*, jobject, jlong jhandle, jlong jfactory_handle) {
  reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)->table_factory.reset(
      reinterpret_cast<rocksdb::TableFactory*>(jfactory_handle));
}

/*
 * Method:    tableFactoryName
 * Signature: (J)Ljava/lang/String
 */
jstring Java_org_rocksdb_ColumnFamilyOptions_tableFactoryName(
    JNIEnv* env, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle);
  rocksdb::TableFactory* tf = opt->table_factory.get();

  // Should never be nullptr.
  // Default memtable factory is SkipListFactory
  assert(tf);

  return env->NewStringUTF(tf->Name());
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    minWriteBufferNumberToMerge
 * Signature: (J)I
 */
jint Java_org_rocksdb_ColumnFamilyOptions_minWriteBufferNumberToMerge(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)
      ->min_write_buffer_number_to_merge;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setMinWriteBufferNumberToMerge
 * Signature: (JI)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setMinWriteBufferNumberToMerge(
    JNIEnv*, jobject, jlong jhandle, jint jmin_write_buffer_number_to_merge) {
  reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)
      ->min_write_buffer_number_to_merge =
      static_cast<int>(jmin_write_buffer_number_to_merge);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    maxWriteBufferNumberToMaintain
 * Signature: (J)I
 */
jint Java_org_rocksdb_ColumnFamilyOptions_maxWriteBufferNumberToMaintain(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)
      ->max_write_buffer_number_to_maintain;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setMaxWriteBufferNumberToMaintain
 * Signature: (JI)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setMaxWriteBufferNumberToMaintain(
    JNIEnv*, jobject, jlong jhandle,
    jint jmax_write_buffer_number_to_maintain) {
  reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)
      ->max_write_buffer_number_to_maintain =
      static_cast<int>(jmax_write_buffer_number_to_maintain);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setCompressionType
 * Signature: (JB)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setCompressionType(
    JNIEnv*, jobject, jlong jhandle, jbyte jcompression_type_value) {
  auto* cf_opts = reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle);
  cf_opts->compression = rocksdb::CompressionTypeJni::toCppCompressionType(
      jcompression_type_value);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    compressionType
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_ColumnFamilyOptions_compressionType(
    JNIEnv*, jobject, jlong jhandle) {
  auto* cf_opts = reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle);
  return rocksdb::CompressionTypeJni::toJavaCompressionType(
      cf_opts->compression);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setCompressionPerLevel
 * Signature: (J[B)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setCompressionPerLevel(
    JNIEnv* env, jobject, jlong jhandle, jbyteArray jcompressionLevels) {
  auto* options = reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle);
  auto uptr_compression_levels =
      rocksdb_compression_vector_helper(env, jcompressionLevels);
  if (!uptr_compression_levels) {
    // exception occurred
    return;
  }
  options->compression_per_level = *(uptr_compression_levels.get());
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    compressionPerLevel
 * Signature: (J)[B
 */
jbyteArray Java_org_rocksdb_ColumnFamilyOptions_compressionPerLevel(
    JNIEnv* env, jobject, jlong jhandle) {
  auto* cf_options = reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle);
  return rocksdb_compression_list_helper(env,
                                         cf_options->compression_per_level);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setBottommostCompressionType
 * Signature: (JB)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setBottommostCompressionType(
    JNIEnv*, jobject, jlong jhandle, jbyte jcompression_type_value) {
  auto* cf_options = reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle);
  cf_options->bottommost_compression =
      rocksdb::CompressionTypeJni::toCppCompressionType(
          jcompression_type_value);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    bottommostCompressionType
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_ColumnFamilyOptions_bottommostCompressionType(
    JNIEnv*, jobject, jlong jhandle) {
  auto* cf_options = reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle);
  return rocksdb::CompressionTypeJni::toJavaCompressionType(
      cf_options->bottommost_compression);
}
/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setBottommostCompressionOptions
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setBottommostCompressionOptions(
    JNIEnv*, jobject, jlong jhandle,
    jlong jbottommost_compression_options_handle) {
  auto* cf_options = reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle);
  auto* bottommost_compression_options =
      reinterpret_cast<rocksdb::CompressionOptions*>(
          jbottommost_compression_options_handle);
  cf_options->bottommost_compression_opts = *bottommost_compression_options;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setCompressionOptions
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setCompressionOptions(
    JNIEnv*, jobject, jlong jhandle, jlong jcompression_options_handle) {
  auto* cf_options = reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle);
  auto* compression_options = reinterpret_cast<rocksdb::CompressionOptions*>(
      jcompression_options_handle);
  cf_options->compression_opts = *compression_options;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setCompactionStyle
 * Signature: (JB)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setCompactionStyle(
    JNIEnv*, jobject, jlong jhandle, jbyte jcompaction_style) {
  auto* cf_options = reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle);
  cf_options->compaction_style =
      rocksdb::CompactionStyleJni::toCppCompactionStyle(jcompaction_style);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    compactionStyle
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_ColumnFamilyOptions_compactionStyle(
    JNIEnv*, jobject, jlong jhandle) {
  auto* cf_options = reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle);
  return rocksdb::CompactionStyleJni::toJavaCompactionStyle(
      cf_options->compaction_style);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setMaxTableFilesSizeFIFO
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setMaxTableFilesSizeFIFO(
    JNIEnv*, jobject, jlong jhandle, jlong jmax_table_files_size) {
  reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)
      ->compaction_options_fifo.max_table_files_size =
      static_cast<uint64_t>(jmax_table_files_size);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    maxTableFilesSizeFIFO
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ColumnFamilyOptions_maxTableFilesSizeFIFO(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)
      ->compaction_options_fifo.max_table_files_size;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    numLevels
 * Signature: (J)I
 */
jint Java_org_rocksdb_ColumnFamilyOptions_numLevels(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)->num_levels;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setNumLevels
 * Signature: (JI)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setNumLevels(
    JNIEnv*, jobject, jlong jhandle, jint jnum_levels) {
  reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)->num_levels =
      static_cast<int>(jnum_levels);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    levelZeroFileNumCompactionTrigger
 * Signature: (J)I
 */
jint Java_org_rocksdb_ColumnFamilyOptions_levelZeroFileNumCompactionTrigger(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)
      ->level0_file_num_compaction_trigger;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setLevelZeroFileNumCompactionTrigger
 * Signature: (JI)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setLevelZeroFileNumCompactionTrigger(
    JNIEnv*, jobject, jlong jhandle,
    jint jlevel0_file_num_compaction_trigger) {
  reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)
      ->level0_file_num_compaction_trigger =
      static_cast<int>(jlevel0_file_num_compaction_trigger);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    levelZeroSlowdownWritesTrigger
 * Signature: (J)I
 */
jint Java_org_rocksdb_ColumnFamilyOptions_levelZeroSlowdownWritesTrigger(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)
      ->level0_slowdown_writes_trigger;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setLevelSlowdownWritesTrigger
 * Signature: (JI)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setLevelZeroSlowdownWritesTrigger(
    JNIEnv*, jobject, jlong jhandle, jint jlevel0_slowdown_writes_trigger) {
  reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)
      ->level0_slowdown_writes_trigger =
      static_cast<int>(jlevel0_slowdown_writes_trigger);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    levelZeroStopWritesTrigger
 * Signature: (J)I
 */
jint Java_org_rocksdb_ColumnFamilyOptions_levelZeroStopWritesTrigger(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)
      ->level0_stop_writes_trigger;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setLevelStopWritesTrigger
 * Signature: (JI)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setLevelZeroStopWritesTrigger(
    JNIEnv*, jobject, jlong jhandle, jint jlevel0_stop_writes_trigger) {
  reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)
      ->level0_stop_writes_trigger =
      static_cast<int>(jlevel0_stop_writes_trigger);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    targetFileSizeBase
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ColumnFamilyOptions_targetFileSizeBase(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)
      ->target_file_size_base;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setTargetFileSizeBase
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setTargetFileSizeBase(
    JNIEnv*, jobject, jlong jhandle, jlong jtarget_file_size_base) {
  reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)
      ->target_file_size_base = static_cast<uint64_t>(jtarget_file_size_base);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    targetFileSizeMultiplier
 * Signature: (J)I
 */
jint Java_org_rocksdb_ColumnFamilyOptions_targetFileSizeMultiplier(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)
      ->target_file_size_multiplier;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setTargetFileSizeMultiplier
 * Signature: (JI)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setTargetFileSizeMultiplier(
    JNIEnv*, jobject, jlong jhandle, jint jtarget_file_size_multiplier) {
  reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)
      ->target_file_size_multiplier =
      static_cast<int>(jtarget_file_size_multiplier);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    maxBytesForLevelBase
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ColumnFamilyOptions_maxBytesForLevelBase(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)
      ->max_bytes_for_level_base;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setMaxBytesForLevelBase
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setMaxBytesForLevelBase(
    JNIEnv*, jobject, jlong jhandle, jlong jmax_bytes_for_level_base) {
  reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)
      ->max_bytes_for_level_base =
      static_cast<int64_t>(jmax_bytes_for_level_base);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    levelCompactionDynamicLevelBytes
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_ColumnFamilyOptions_levelCompactionDynamicLevelBytes(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)
      ->level_compaction_dynamic_level_bytes;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setLevelCompactionDynamicLevelBytes
 * Signature: (JZ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setLevelCompactionDynamicLevelBytes(
    JNIEnv*, jobject, jlong jhandle, jboolean jenable_dynamic_level_bytes) {
  reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)
      ->level_compaction_dynamic_level_bytes = (jenable_dynamic_level_bytes);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    maxBytesForLevelMultiplier
 * Signature: (J)D
 */
jdouble Java_org_rocksdb_ColumnFamilyOptions_maxBytesForLevelMultiplier(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)
      ->max_bytes_for_level_multiplier;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setMaxBytesForLevelMultiplier
 * Signature: (JD)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setMaxBytesForLevelMultiplier(
    JNIEnv*, jobject, jlong jhandle, jdouble jmax_bytes_for_level_multiplier) {
  reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)
      ->max_bytes_for_level_multiplier =
      static_cast<double>(jmax_bytes_for_level_multiplier);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    maxCompactionBytes
 * Signature: (J)I
 */
jlong Java_org_rocksdb_ColumnFamilyOptions_maxCompactionBytes(
    JNIEnv*, jobject, jlong jhandle) {
  return static_cast<jlong>(
      reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)
          ->max_compaction_bytes);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setMaxCompactionBytes
 * Signature: (JI)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setMaxCompactionBytes(
    JNIEnv*, jobject, jlong jhandle, jlong jmax_compaction_bytes) {
  reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)
      ->max_compaction_bytes = static_cast<uint64_t>(jmax_compaction_bytes);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    arenaBlockSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ColumnFamilyOptions_arenaBlockSize(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)
      ->arena_block_size;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setArenaBlockSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setArenaBlockSize(
    JNIEnv* env, jobject, jlong jhandle, jlong jarena_block_size) {
  auto s = rocksdb::JniUtil::check_if_jlong_fits_size_t(jarena_block_size);
  if (s.ok()) {
    reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)->arena_block_size =
        jarena_block_size;
  } else {
    rocksdb::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    disableAutoCompactions
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_ColumnFamilyOptions_disableAutoCompactions(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)
      ->disable_auto_compactions;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setDisableAutoCompactions
 * Signature: (JZ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setDisableAutoCompactions(
    JNIEnv*, jobject, jlong jhandle, jboolean jdisable_auto_compactions) {
  reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)
      ->disable_auto_compactions = static_cast<bool>(jdisable_auto_compactions);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    maxSequentialSkipInIterations
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ColumnFamilyOptions_maxSequentialSkipInIterations(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)
      ->max_sequential_skip_in_iterations;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setMaxSequentialSkipInIterations
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setMaxSequentialSkipInIterations(
    JNIEnv*, jobject, jlong jhandle,
    jlong jmax_sequential_skip_in_iterations) {
  reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)
      ->max_sequential_skip_in_iterations =
      static_cast<int64_t>(jmax_sequential_skip_in_iterations);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    inplaceUpdateSupport
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_ColumnFamilyOptions_inplaceUpdateSupport(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)
      ->inplace_update_support;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setInplaceUpdateSupport
 * Signature: (JZ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setInplaceUpdateSupport(
    JNIEnv*, jobject, jlong jhandle, jboolean jinplace_update_support) {
  reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)
      ->inplace_update_support = static_cast<bool>(jinplace_update_support);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    inplaceUpdateNumLocks
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ColumnFamilyOptions_inplaceUpdateNumLocks(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)
      ->inplace_update_num_locks;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setInplaceUpdateNumLocks
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setInplaceUpdateNumLocks(
    JNIEnv* env, jobject, jlong jhandle, jlong jinplace_update_num_locks) {
  auto s =
      rocksdb::JniUtil::check_if_jlong_fits_size_t(jinplace_update_num_locks);
  if (s.ok()) {
    reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)
        ->inplace_update_num_locks = jinplace_update_num_locks;
  } else {
    rocksdb::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    memtablePrefixBloomSizeRatio
 * Signature: (J)I
 */
jdouble Java_org_rocksdb_ColumnFamilyOptions_memtablePrefixBloomSizeRatio(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)
      ->memtable_prefix_bloom_size_ratio;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setMemtablePrefixBloomSizeRatio
 * Signature: (JI)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setMemtablePrefixBloomSizeRatio(
    JNIEnv*, jobject, jlong jhandle,
    jdouble jmemtable_prefix_bloom_size_ratio) {
  reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)
      ->memtable_prefix_bloom_size_ratio =
      static_cast<double>(jmemtable_prefix_bloom_size_ratio);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    bloomLocality
 * Signature: (J)I
 */
jint Java_org_rocksdb_ColumnFamilyOptions_bloomLocality(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)
      ->bloom_locality;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setBloomLocality
 * Signature: (JI)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setBloomLocality(
    JNIEnv*, jobject, jlong jhandle, jint jbloom_locality) {
  reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)->bloom_locality =
      static_cast<int32_t>(jbloom_locality);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    maxSuccessiveMerges
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ColumnFamilyOptions_maxSuccessiveMerges(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)
      ->max_successive_merges;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setMaxSuccessiveMerges
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setMaxSuccessiveMerges(
    JNIEnv* env, jobject, jlong jhandle, jlong jmax_successive_merges) {
  auto s =
      rocksdb::JniUtil::check_if_jlong_fits_size_t(jmax_successive_merges);
  if (s.ok()) {
    reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)
        ->max_successive_merges = jmax_successive_merges;
  } else {
    rocksdb::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    optimizeFiltersForHits
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_ColumnFamilyOptions_optimizeFiltersForHits(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)
      ->optimize_filters_for_hits;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setOptimizeFiltersForHits
 * Signature: (JZ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setOptimizeFiltersForHits(
    JNIEnv*, jobject, jlong jhandle, jboolean joptimize_filters_for_hits) {
  reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)
      ->optimize_filters_for_hits =
      static_cast<bool>(joptimize_filters_for_hits);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    memtableHugePageSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ColumnFamilyOptions_memtableHugePageSize(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)
      ->memtable_huge_page_size;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setMemtableHugePageSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setMemtableHugePageSize(
    JNIEnv* env, jobject, jlong jhandle, jlong jmemtable_huge_page_size) {
  auto s =
      rocksdb::JniUtil::check_if_jlong_fits_size_t(jmemtable_huge_page_size);
  if (s.ok()) {
    reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)
        ->memtable_huge_page_size = jmemtable_huge_page_size;
  } else {
    rocksdb::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    softPendingCompactionBytesLimit
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ColumnFamilyOptions_softPendingCompactionBytesLimit(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)
      ->soft_pending_compaction_bytes_limit;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setSoftPendingCompactionBytesLimit
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setSoftPendingCompactionBytesLimit(
    JNIEnv*, jobject, jlong jhandle,
    jlong jsoft_pending_compaction_bytes_limit) {
  reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)
      ->soft_pending_compaction_bytes_limit =
      static_cast<int64_t>(jsoft_pending_compaction_bytes_limit);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    softHardCompactionBytesLimit
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ColumnFamilyOptions_hardPendingCompactionBytesLimit(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)
      ->hard_pending_compaction_bytes_limit;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setHardPendingCompactionBytesLimit
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setHardPendingCompactionBytesLimit(
    JNIEnv*, jobject, jlong jhandle,
    jlong jhard_pending_compaction_bytes_limit) {
  reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)
      ->hard_pending_compaction_bytes_limit =
      static_cast<int64_t>(jhard_pending_compaction_bytes_limit);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    level0FileNumCompactionTrigger
 * Signature: (J)I
 */
jint Java_org_rocksdb_ColumnFamilyOptions_level0FileNumCompactionTrigger(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)
      ->level0_file_num_compaction_trigger;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setLevel0FileNumCompactionTrigger
 * Signature: (JI)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setLevel0FileNumCompactionTrigger(
    JNIEnv*, jobject, jlong jhandle,
    jint jlevel0_file_num_compaction_trigger) {
  reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)
      ->level0_file_num_compaction_trigger =
      static_cast<int32_t>(jlevel0_file_num_compaction_trigger);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    level0SlowdownWritesTrigger
 * Signature: (J)I
 */
jint Java_org_rocksdb_ColumnFamilyOptions_level0SlowdownWritesTrigger(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)
      ->level0_slowdown_writes_trigger;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setLevel0SlowdownWritesTrigger
 * Signature: (JI)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setLevel0SlowdownWritesTrigger(
    JNIEnv*, jobject, jlong jhandle, jint jlevel0_slowdown_writes_trigger) {
  reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)
      ->level0_slowdown_writes_trigger =
      static_cast<int32_t>(jlevel0_slowdown_writes_trigger);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    level0StopWritesTrigger
 * Signature: (J)I
 */
jint Java_org_rocksdb_ColumnFamilyOptions_level0StopWritesTrigger(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)
      ->level0_stop_writes_trigger;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setLevel0StopWritesTrigger
 * Signature: (JI)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setLevel0StopWritesTrigger(
    JNIEnv*, jobject, jlong jhandle, jint jlevel0_stop_writes_trigger) {
  reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)
      ->level0_stop_writes_trigger =
      static_cast<int32_t>(jlevel0_stop_writes_trigger);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    maxBytesForLevelMultiplierAdditional
 * Signature: (J)[I
 */
jintArray Java_org_rocksdb_ColumnFamilyOptions_maxBytesForLevelMultiplierAdditional(
    JNIEnv* env, jobject, jlong jhandle) {
  auto mbflma = reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)
                    ->max_bytes_for_level_multiplier_additional;

  const size_t size = mbflma.size();

  jint* additionals = new jint[size];
  for (size_t i = 0; i < size; i++) {
    additionals[i] = static_cast<jint>(mbflma[i]);
  }

  jsize jlen = static_cast<jsize>(size);
  jintArray result = env->NewIntArray(jlen);
  if (result == nullptr) {
    // exception thrown: OutOfMemoryError
    delete[] additionals;
    return nullptr;
  }
  env->SetIntArrayRegion(result, 0, jlen, additionals);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    env->DeleteLocalRef(result);
    delete[] additionals;
    return nullptr;
  }

  delete[] additionals;

  return result;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setMaxBytesForLevelMultiplierAdditional
 * Signature: (J[I)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setMaxBytesForLevelMultiplierAdditional(
    JNIEnv* env, jobject, jlong jhandle,
    jintArray jmax_bytes_for_level_multiplier_additional) {
  jsize len = env->GetArrayLength(jmax_bytes_for_level_multiplier_additional);
  jint* additionals =
      env->GetIntArrayElements(jmax_bytes_for_level_multiplier_additional, 0);
  if (additionals == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }

  auto* cf_opt = reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle);
  cf_opt->max_bytes_for_level_multiplier_additional.clear();
  for (jsize i = 0; i < len; i++) {
    cf_opt->max_bytes_for_level_multiplier_additional.push_back(
        static_cast<int32_t>(additionals[i]));
  }

  env->ReleaseIntArrayElements(jmax_bytes_for_level_multiplier_additional,
                               additionals, JNI_ABORT);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    paranoidFileChecks
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_ColumnFamilyOptions_paranoidFileChecks(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)
      ->paranoid_file_checks;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setParanoidFileChecks
 * Signature: (JZ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setParanoidFileChecks(
    JNIEnv*, jobject, jlong jhandle, jboolean jparanoid_file_checks) {
  reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle)
      ->paranoid_file_checks = static_cast<bool>(jparanoid_file_checks);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setCompactionPriority
 * Signature: (JB)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setCompactionPriority(
    JNIEnv*, jobject, jlong jhandle, jbyte jcompaction_priority_value) {
  auto* cf_opts = reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle);
  cf_opts->compaction_pri =
      rocksdb::CompactionPriorityJni::toCppCompactionPriority(
          jcompaction_priority_value);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    compactionPriority
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_ColumnFamilyOptions_compactionPriority(
    JNIEnv*, jobject, jlong jhandle) {
  auto* cf_opts = reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle);
  return rocksdb::CompactionPriorityJni::toJavaCompactionPriority(
      cf_opts->compaction_pri);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setReportBgIoStats
 * Signature: (JZ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setReportBgIoStats(
    JNIEnv*, jobject, jlong jhandle, jboolean jreport_bg_io_stats) {
  auto* cf_opts = reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle);
  cf_opts->report_bg_io_stats = static_cast<bool>(jreport_bg_io_stats);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    reportBgIoStats
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_ColumnFamilyOptions_reportBgIoStats(
    JNIEnv*, jobject, jlong jhandle) {
  auto* cf_opts = reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle);
  return static_cast<bool>(cf_opts->report_bg_io_stats);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setTtl
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setTtl(
    JNIEnv*, jobject, jlong jhandle, jlong jttl) {
  auto* cf_opts = reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle);
  cf_opts->ttl = static_cast<uint64_t>(jttl);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    ttl
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_org_rocksdb_ColumnFamilyOptions_ttl(
    JNIEnv*, jobject, jlong jhandle) {
  auto* cf_opts = reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle);
  return static_cast<jlong>(cf_opts->ttl);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setCompactionOptionsUniversal
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setCompactionOptionsUniversal(
    JNIEnv*, jobject, jlong jhandle,
    jlong jcompaction_options_universal_handle) {
  auto* cf_opts = reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle);
  auto* opts_uni = reinterpret_cast<rocksdb::CompactionOptionsUniversal*>(
      jcompaction_options_universal_handle);
  cf_opts->compaction_options_universal = *opts_uni;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setCompactionOptionsFIFO
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setCompactionOptionsFIFO(
    JNIEnv*, jobject, jlong jhandle, jlong jcompaction_options_fifo_handle) {
  auto* cf_opts = reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle);
  auto* opts_fifo = reinterpret_cast<rocksdb::CompactionOptionsFIFO*>(
      jcompaction_options_fifo_handle);
  cf_opts->compaction_options_fifo = *opts_fifo;
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    setForceConsistencyChecks
 * Signature: (JZ)V
 */
void Java_org_rocksdb_ColumnFamilyOptions_setForceConsistencyChecks(
    JNIEnv*, jobject, jlong jhandle, jboolean jforce_consistency_checks) {
  auto* cf_opts = reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle);
  cf_opts->force_consistency_checks =
      static_cast<bool>(jforce_consistency_checks);
}

/*
 * Class:     org_rocksdb_ColumnFamilyOptions
 * Method:    forceConsistencyChecks
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_ColumnFamilyOptions_forceConsistencyChecks(
    JNIEnv*, jobject, jlong jhandle) {
  auto* cf_opts = reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jhandle);
  return static_cast<bool>(cf_opts->force_consistency_checks);
}

/////////////////////////////////////////////////////////////////////
// rocksdb::DBOptions

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    newDBOptions
 * Signature: ()J
 */
jlong Java_org_rocksdb_DBOptions_newDBOptions(
    JNIEnv*, jclass) {
  auto* dbop = new rocksdb::DBOptions();
  return reinterpret_cast<jlong>(dbop);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    copyDBOptions
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_copyDBOptions(
    JNIEnv*, jclass, jlong jhandle) {
  auto new_opt =
      new rocksdb::DBOptions(*(reinterpret_cast<rocksdb::DBOptions*>(jhandle)));
  return reinterpret_cast<jlong>(new_opt);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    newDBOptionsFromOptions
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_newDBOptionsFromOptions(
    JNIEnv*, jclass, jlong joptions_handle) {
  auto new_opt =
      new rocksdb::DBOptions(*reinterpret_cast<rocksdb::Options*>(joptions_handle));
  return reinterpret_cast<jlong>(new_opt);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    getDBOptionsFromProps
 * Signature: (Ljava/util/String;)J
 */
jlong Java_org_rocksdb_DBOptions_getDBOptionsFromProps(
    JNIEnv* env, jclass, jstring jopt_string) {
  const char* opt_string = env->GetStringUTFChars(jopt_string, nullptr);
  if (opt_string == nullptr) {
    // exception thrown: OutOfMemoryError
    return 0;
  }

  auto* db_options = new rocksdb::DBOptions();
  rocksdb::Status status = rocksdb::GetDBOptionsFromString(
      rocksdb::DBOptions(), opt_string, db_options);

  env->ReleaseStringUTFChars(jopt_string, opt_string);

  // Check if DBOptions creation was possible.
  jlong ret_value = 0;
  if (status.ok()) {
    ret_value = reinterpret_cast<jlong>(db_options);
  } else {
    // if operation failed the DBOptions need to be deleted
    // again to prevent a memory leak.
    delete db_options;
  }
  return ret_value;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_DBOptions_disposeInternal(
    JNIEnv*, jobject, jlong handle) {
  auto* dbo = reinterpret_cast<rocksdb::DBOptions*>(handle);
  assert(dbo != nullptr);
  delete dbo;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    optimizeForSmallDb
 * Signature: (J)V
 */
void Java_org_rocksdb_DBOptions_optimizeForSmallDb(
    JNIEnv*, jobject, jlong jhandle) {
  reinterpret_cast<rocksdb::DBOptions*>(jhandle)->OptimizeForSmallDb();
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setEnv
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setEnv(
    JNIEnv*, jobject, jlong jhandle, jlong jenv_handle) {
  reinterpret_cast<rocksdb::DBOptions*>(jhandle)->env =
      reinterpret_cast<rocksdb::Env*>(jenv_handle);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setIncreaseParallelism
 * Signature: (JI)V
 */
void Java_org_rocksdb_DBOptions_setIncreaseParallelism(
    JNIEnv*, jobject, jlong jhandle, jint totalThreads) {
  reinterpret_cast<rocksdb::DBOptions*>(jhandle)->IncreaseParallelism(
      static_cast<int>(totalThreads));
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setCreateIfMissing
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setCreateIfMissing(
    JNIEnv*, jobject, jlong jhandle, jboolean flag) {
  reinterpret_cast<rocksdb::DBOptions*>(jhandle)->create_if_missing = flag;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    createIfMissing
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_createIfMissing(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::DBOptions*>(jhandle)->create_if_missing;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setCreateMissingColumnFamilies
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setCreateMissingColumnFamilies(
    JNIEnv*, jobject, jlong jhandle, jboolean flag) {
  reinterpret_cast<rocksdb::DBOptions*>(jhandle)
      ->create_missing_column_families = flag;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    createMissingColumnFamilies
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_createMissingColumnFamilies(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::DBOptions*>(jhandle)
      ->create_missing_column_families;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setErrorIfExists
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setErrorIfExists(
    JNIEnv*, jobject, jlong jhandle, jboolean error_if_exists) {
  reinterpret_cast<rocksdb::DBOptions*>(jhandle)->error_if_exists =
      static_cast<bool>(error_if_exists);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    errorIfExists
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_errorIfExists(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::DBOptions*>(jhandle)->error_if_exists;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setParanoidChecks
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setParanoidChecks(
    JNIEnv*, jobject, jlong jhandle, jboolean paranoid_checks) {
  reinterpret_cast<rocksdb::DBOptions*>(jhandle)->paranoid_checks =
      static_cast<bool>(paranoid_checks);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    paranoidChecks
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_paranoidChecks(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::DBOptions*>(jhandle)->paranoid_checks;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setRateLimiter
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setRateLimiter(
    JNIEnv*, jobject, jlong jhandle, jlong jrate_limiter_handle) {
  std::shared_ptr<rocksdb::RateLimiter>* pRateLimiter =
      reinterpret_cast<std::shared_ptr<rocksdb::RateLimiter>*>(
          jrate_limiter_handle);
  reinterpret_cast<rocksdb::DBOptions*>(jhandle)->rate_limiter = *pRateLimiter;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setSstFileManager
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setSstFileManager(
    JNIEnv*, jobject, jlong jhandle, jlong jsst_file_manager_handle) {
  auto* sptr_sst_file_manager =
      reinterpret_cast<std::shared_ptr<rocksdb::SstFileManager>*>(
          jsst_file_manager_handle);
  reinterpret_cast<rocksdb::DBOptions*>(jhandle)->sst_file_manager =
      *sptr_sst_file_manager;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setLogger
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setLogger(
    JNIEnv*, jobject, jlong jhandle, jlong jlogger_handle) {
  std::shared_ptr<rocksdb::LoggerJniCallback>* pLogger =
      reinterpret_cast<std::shared_ptr<rocksdb::LoggerJniCallback>*>(
          jlogger_handle);
  reinterpret_cast<rocksdb::DBOptions*>(jhandle)->info_log = *pLogger;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setInfoLogLevel
 * Signature: (JB)V
 */
void Java_org_rocksdb_DBOptions_setInfoLogLevel(
    JNIEnv*, jobject, jlong jhandle, jbyte jlog_level) {
  reinterpret_cast<rocksdb::DBOptions*>(jhandle)->info_log_level =
      static_cast<rocksdb::InfoLogLevel>(jlog_level);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    infoLogLevel
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_DBOptions_infoLogLevel(
    JNIEnv*, jobject, jlong jhandle) {
  return static_cast<jbyte>(
      reinterpret_cast<rocksdb::DBOptions*>(jhandle)->info_log_level);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setMaxTotalWalSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setMaxTotalWalSize(
    JNIEnv*, jobject, jlong jhandle, jlong jmax_total_wal_size) {
  reinterpret_cast<rocksdb::DBOptions*>(jhandle)->max_total_wal_size =
      static_cast<jlong>(jmax_total_wal_size);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    maxTotalWalSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_maxTotalWalSize(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::DBOptions*>(jhandle)->max_total_wal_size;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setMaxOpenFiles
 * Signature: (JI)V
 */
void Java_org_rocksdb_DBOptions_setMaxOpenFiles(
    JNIEnv*, jobject, jlong jhandle, jint max_open_files) {
  reinterpret_cast<rocksdb::DBOptions*>(jhandle)->max_open_files =
      static_cast<int>(max_open_files);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    maxOpenFiles
 * Signature: (J)I
 */
jint Java_org_rocksdb_DBOptions_maxOpenFiles(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::DBOptions*>(jhandle)->max_open_files;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setMaxFileOpeningThreads
 * Signature: (JI)V
 */
void Java_org_rocksdb_DBOptions_setMaxFileOpeningThreads(
    JNIEnv*, jobject, jlong jhandle, jint jmax_file_opening_threads) {
  reinterpret_cast<rocksdb::DBOptions*>(jhandle)->max_file_opening_threads =
      static_cast<int>(jmax_file_opening_threads);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    maxFileOpeningThreads
 * Signature: (J)I
 */
jint Java_org_rocksdb_DBOptions_maxFileOpeningThreads(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<rocksdb::DBOptions*>(jhandle);
  return static_cast<int>(opt->max_file_opening_threads);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setStatistics
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setStatistics(
    JNIEnv*, jobject, jlong jhandle, jlong jstatistics_handle) {
  auto* opt = reinterpret_cast<rocksdb::DBOptions*>(jhandle);
  auto* pSptr = reinterpret_cast<std::shared_ptr<rocksdb::StatisticsJni>*>(
      jstatistics_handle);
  opt->statistics = *pSptr;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    statistics
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_statistics(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<rocksdb::DBOptions*>(jhandle);
  std::shared_ptr<rocksdb::Statistics> sptr = opt->statistics;
  if (sptr == nullptr) {
    return 0;
  } else {
    std::shared_ptr<rocksdb::Statistics>* pSptr =
        new std::shared_ptr<rocksdb::Statistics>(sptr);
    return reinterpret_cast<jlong>(pSptr);
  }
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setUseFsync
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setUseFsync(
    JNIEnv*, jobject, jlong jhandle, jboolean use_fsync) {
  reinterpret_cast<rocksdb::DBOptions*>(jhandle)->use_fsync =
      static_cast<bool>(use_fsync);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    useFsync
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_useFsync(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::DBOptions*>(jhandle)->use_fsync;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setDbPaths
 * Signature: (J[Ljava/lang/String;[J)V
 */
void Java_org_rocksdb_DBOptions_setDbPaths(
    JNIEnv* env, jobject, jlong jhandle, jobjectArray jpaths,
    jlongArray jtarget_sizes) {
  std::vector<rocksdb::DbPath> db_paths;
  jlong* ptr_jtarget_size = env->GetLongArrayElements(jtarget_sizes, nullptr);
  if (ptr_jtarget_size == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }

  jboolean has_exception = JNI_FALSE;
  const jsize len = env->GetArrayLength(jpaths);
  for (jsize i = 0; i < len; i++) {
    jobject jpath =
        reinterpret_cast<jstring>(env->GetObjectArrayElement(jpaths, i));
    if (env->ExceptionCheck()) {
      // exception thrown: ArrayIndexOutOfBoundsException
      env->ReleaseLongArrayElements(jtarget_sizes, ptr_jtarget_size, JNI_ABORT);
      return;
    }
    std::string path = rocksdb::JniUtil::copyStdString(
        env, static_cast<jstring>(jpath), &has_exception);
    env->DeleteLocalRef(jpath);

    if (has_exception == JNI_TRUE) {
      env->ReleaseLongArrayElements(jtarget_sizes, ptr_jtarget_size, JNI_ABORT);
      return;
    }

    jlong jtarget_size = ptr_jtarget_size[i];

    db_paths.push_back(
        rocksdb::DbPath(path, static_cast<uint64_t>(jtarget_size)));
  }

  env->ReleaseLongArrayElements(jtarget_sizes, ptr_jtarget_size, JNI_ABORT);

  auto* opt = reinterpret_cast<rocksdb::DBOptions*>(jhandle);
  opt->db_paths = db_paths;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    dbPathsLen
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_dbPathsLen(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<rocksdb::DBOptions*>(jhandle);
  return static_cast<jlong>(opt->db_paths.size());
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    dbPaths
 * Signature: (J[Ljava/lang/String;[J)V
 */
void Java_org_rocksdb_DBOptions_dbPaths(
    JNIEnv* env, jobject, jlong jhandle, jobjectArray jpaths,
    jlongArray jtarget_sizes) {
  jlong* ptr_jtarget_size = env->GetLongArrayElements(jtarget_sizes, nullptr);
  if (ptr_jtarget_size == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }

  auto* opt = reinterpret_cast<rocksdb::DBOptions*>(jhandle);
  const jsize len = env->GetArrayLength(jpaths);
  for (jsize i = 0; i < len; i++) {
    rocksdb::DbPath db_path = opt->db_paths[i];

    jstring jpath = env->NewStringUTF(db_path.path.c_str());
    if (jpath == nullptr) {
      // exception thrown: OutOfMemoryError
      env->ReleaseLongArrayElements(jtarget_sizes, ptr_jtarget_size, JNI_ABORT);
      return;
    }
    env->SetObjectArrayElement(jpaths, i, jpath);
    if (env->ExceptionCheck()) {
      // exception thrown: ArrayIndexOutOfBoundsException
      env->DeleteLocalRef(jpath);
      env->ReleaseLongArrayElements(jtarget_sizes, ptr_jtarget_size, JNI_ABORT);
      return;
    }

    ptr_jtarget_size[i] = static_cast<jint>(db_path.target_size);
  }

  env->ReleaseLongArrayElements(jtarget_sizes, ptr_jtarget_size, JNI_COMMIT);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setDbLogDir
 * Signature: (JLjava/lang/String)V
 */
void Java_org_rocksdb_DBOptions_setDbLogDir(
    JNIEnv* env, jobject, jlong jhandle, jstring jdb_log_dir) {
  const char* log_dir = env->GetStringUTFChars(jdb_log_dir, nullptr);
  if (log_dir == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }

  reinterpret_cast<rocksdb::DBOptions*>(jhandle)->db_log_dir.assign(log_dir);
  env->ReleaseStringUTFChars(jdb_log_dir, log_dir);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    dbLogDir
 * Signature: (J)Ljava/lang/String
 */
jstring Java_org_rocksdb_DBOptions_dbLogDir(
    JNIEnv* env, jobject, jlong jhandle) {
  return env->NewStringUTF(
      reinterpret_cast<rocksdb::DBOptions*>(jhandle)->db_log_dir.c_str());
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setWalDir
 * Signature: (JLjava/lang/String)V
 */
void Java_org_rocksdb_DBOptions_setWalDir(
    JNIEnv* env, jobject, jlong jhandle, jstring jwal_dir) {
  const char* wal_dir = env->GetStringUTFChars(jwal_dir, 0);
  reinterpret_cast<rocksdb::DBOptions*>(jhandle)->wal_dir.assign(wal_dir);
  env->ReleaseStringUTFChars(jwal_dir, wal_dir);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    walDir
 * Signature: (J)Ljava/lang/String
 */
jstring Java_org_rocksdb_DBOptions_walDir(
    JNIEnv* env, jobject, jlong jhandle) {
  return env->NewStringUTF(
      reinterpret_cast<rocksdb::DBOptions*>(jhandle)->wal_dir.c_str());
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setDeleteObsoleteFilesPeriodMicros
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setDeleteObsoleteFilesPeriodMicros(
    JNIEnv*, jobject, jlong jhandle, jlong micros) {
  reinterpret_cast<rocksdb::DBOptions*>(jhandle)
      ->delete_obsolete_files_period_micros = static_cast<int64_t>(micros);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    deleteObsoleteFilesPeriodMicros
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_deleteObsoleteFilesPeriodMicros(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::DBOptions*>(jhandle)
      ->delete_obsolete_files_period_micros;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setBaseBackgroundCompactions
 * Signature: (JI)V
 */
void Java_org_rocksdb_DBOptions_setBaseBackgroundCompactions(
    JNIEnv*, jobject, jlong jhandle, jint max) {
  reinterpret_cast<rocksdb::DBOptions*>(jhandle)->base_background_compactions =
      static_cast<int>(max);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    baseBackgroundCompactions
 * Signature: (J)I
 */
jint Java_org_rocksdb_DBOptions_baseBackgroundCompactions(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::DBOptions*>(jhandle)
      ->base_background_compactions;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setMaxBackgroundCompactions
 * Signature: (JI)V
 */
void Java_org_rocksdb_DBOptions_setMaxBackgroundCompactions(
    JNIEnv*, jobject, jlong jhandle, jint max) {
  reinterpret_cast<rocksdb::DBOptions*>(jhandle)->max_background_compactions =
      static_cast<int>(max);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    maxBackgroundCompactions
 * Signature: (J)I
 */
jint Java_org_rocksdb_DBOptions_maxBackgroundCompactions(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::DBOptions*>(jhandle)
      ->max_background_compactions;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setMaxSubcompactions
 * Signature: (JI)V
 */
void Java_org_rocksdb_DBOptions_setMaxSubcompactions(
    JNIEnv*, jobject, jlong jhandle, jint max) {
  reinterpret_cast<rocksdb::DBOptions*>(jhandle)->max_subcompactions =
      static_cast<int32_t>(max);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    maxSubcompactions
 * Signature: (J)I
 */
jint Java_org_rocksdb_DBOptions_maxSubcompactions(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::DBOptions*>(jhandle)->max_subcompactions;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setMaxBackgroundFlushes
 * Signature: (JI)V
 */
void Java_org_rocksdb_DBOptions_setMaxBackgroundFlushes(
    JNIEnv*, jobject, jlong jhandle, jint max_background_flushes) {
  reinterpret_cast<rocksdb::DBOptions*>(jhandle)->max_background_flushes =
      static_cast<int>(max_background_flushes);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    maxBackgroundFlushes
 * Signature: (J)I
 */
jint Java_org_rocksdb_DBOptions_maxBackgroundFlushes(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::DBOptions*>(jhandle)->max_background_flushes;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setMaxBackgroundJobs
 * Signature: (JI)V
 */
void Java_org_rocksdb_DBOptions_setMaxBackgroundJobs(
    JNIEnv*, jobject, jlong jhandle, jint max_background_jobs) {
  reinterpret_cast<rocksdb::DBOptions*>(jhandle)->max_background_jobs =
      static_cast<int>(max_background_jobs);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    maxBackgroundJobs
 * Signature: (J)I
 */
jint Java_org_rocksdb_DBOptions_maxBackgroundJobs(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::DBOptions*>(jhandle)->max_background_jobs;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setMaxLogFileSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setMaxLogFileSize(
    JNIEnv* env, jobject, jlong jhandle, jlong max_log_file_size) {
  auto s = rocksdb::JniUtil::check_if_jlong_fits_size_t(max_log_file_size);
  if (s.ok()) {
    reinterpret_cast<rocksdb::DBOptions*>(jhandle)->max_log_file_size =
        max_log_file_size;
  } else {
    rocksdb::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    maxLogFileSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_maxLogFileSize(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::DBOptions*>(jhandle)->max_log_file_size;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setLogFileTimeToRoll
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setLogFileTimeToRoll(
    JNIEnv* env, jobject, jlong jhandle, jlong log_file_time_to_roll) {
  auto s =
      rocksdb::JniUtil::check_if_jlong_fits_size_t(log_file_time_to_roll);
  if (s.ok()) {
    reinterpret_cast<rocksdb::DBOptions*>(jhandle)->log_file_time_to_roll =
        log_file_time_to_roll;
  } else {
    rocksdb::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    logFileTimeToRoll
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_logFileTimeToRoll(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::DBOptions*>(jhandle)->log_file_time_to_roll;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setKeepLogFileNum
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setKeepLogFileNum(
    JNIEnv* env, jobject, jlong jhandle, jlong keep_log_file_num) {
  auto s = rocksdb::JniUtil::check_if_jlong_fits_size_t(keep_log_file_num);
  if (s.ok()) {
    reinterpret_cast<rocksdb::DBOptions*>(jhandle)->keep_log_file_num =
        keep_log_file_num;
  } else {
    rocksdb::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    keepLogFileNum
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_keepLogFileNum(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::DBOptions*>(jhandle)->keep_log_file_num;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setRecycleLogFileNum
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setRecycleLogFileNum(
    JNIEnv* env, jobject, jlong jhandle, jlong recycle_log_file_num) {
  auto s = rocksdb::JniUtil::check_if_jlong_fits_size_t(recycle_log_file_num);
  if (s.ok()) {
    reinterpret_cast<rocksdb::DBOptions*>(jhandle)->recycle_log_file_num =
        recycle_log_file_num;
  } else {
    rocksdb::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    recycleLogFileNum
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_recycleLogFileNum(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::DBOptions*>(jhandle)->recycle_log_file_num;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setMaxManifestFileSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setMaxManifestFileSize(
    JNIEnv*, jobject, jlong jhandle, jlong max_manifest_file_size) {
  reinterpret_cast<rocksdb::DBOptions*>(jhandle)->max_manifest_file_size =
      static_cast<int64_t>(max_manifest_file_size);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    maxManifestFileSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_maxManifestFileSize(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::DBOptions*>(jhandle)->max_manifest_file_size;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setTableCacheNumshardbits
 * Signature: (JI)V
 */
void Java_org_rocksdb_DBOptions_setTableCacheNumshardbits(
    JNIEnv*, jobject, jlong jhandle, jint table_cache_numshardbits) {
  reinterpret_cast<rocksdb::DBOptions*>(jhandle)->table_cache_numshardbits =
      static_cast<int>(table_cache_numshardbits);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    tableCacheNumshardbits
 * Signature: (J)I
 */
jint Java_org_rocksdb_DBOptions_tableCacheNumshardbits(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::DBOptions*>(jhandle)
      ->table_cache_numshardbits;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setWalTtlSeconds
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setWalTtlSeconds(
    JNIEnv*, jobject, jlong jhandle, jlong WAL_ttl_seconds) {
  reinterpret_cast<rocksdb::DBOptions*>(jhandle)->WAL_ttl_seconds =
      static_cast<int64_t>(WAL_ttl_seconds);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    walTtlSeconds
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_walTtlSeconds(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::DBOptions*>(jhandle)->WAL_ttl_seconds;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setWalSizeLimitMB
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setWalSizeLimitMB(
    JNIEnv*, jobject, jlong jhandle, jlong WAL_size_limit_MB) {
  reinterpret_cast<rocksdb::DBOptions*>(jhandle)->WAL_size_limit_MB =
      static_cast<int64_t>(WAL_size_limit_MB);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    walTtlSeconds
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_walSizeLimitMB(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::DBOptions*>(jhandle)->WAL_size_limit_MB;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setManifestPreallocationSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setManifestPreallocationSize(
    JNIEnv* env, jobject, jlong jhandle, jlong preallocation_size) {
  auto s = rocksdb::JniUtil::check_if_jlong_fits_size_t(preallocation_size);
  if (s.ok()) {
    reinterpret_cast<rocksdb::DBOptions*>(jhandle)
        ->manifest_preallocation_size = preallocation_size;
  } else {
    rocksdb::IllegalArgumentExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    manifestPreallocationSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_manifestPreallocationSize(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::DBOptions*>(jhandle)
      ->manifest_preallocation_size;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    useDirectReads
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_useDirectReads(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::DBOptions*>(jhandle)->use_direct_reads;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setUseDirectReads
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setUseDirectReads(
    JNIEnv*, jobject, jlong jhandle, jboolean use_direct_reads) {
  reinterpret_cast<rocksdb::DBOptions*>(jhandle)->use_direct_reads =
      static_cast<bool>(use_direct_reads);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    useDirectIoForFlushAndCompaction
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_useDirectIoForFlushAndCompaction(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::DBOptions*>(jhandle)
      ->use_direct_io_for_flush_and_compaction;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setUseDirectReads
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setUseDirectIoForFlushAndCompaction(
    JNIEnv*, jobject, jlong jhandle,
    jboolean use_direct_io_for_flush_and_compaction) {
  reinterpret_cast<rocksdb::DBOptions*>(jhandle)
      ->use_direct_io_for_flush_and_compaction =
      static_cast<bool>(use_direct_io_for_flush_and_compaction);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setAllowFAllocate
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setAllowFAllocate(
    JNIEnv*, jobject, jlong jhandle, jboolean jallow_fallocate) {
  reinterpret_cast<rocksdb::DBOptions*>(jhandle)->allow_fallocate =
      static_cast<bool>(jallow_fallocate);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    allowFAllocate
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_allowFAllocate(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<rocksdb::DBOptions*>(jhandle);
  return static_cast<jboolean>(opt->allow_fallocate);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setAllowMmapReads
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setAllowMmapReads(
    JNIEnv*, jobject, jlong jhandle, jboolean allow_mmap_reads) {
  reinterpret_cast<rocksdb::DBOptions*>(jhandle)->allow_mmap_reads =
      static_cast<bool>(allow_mmap_reads);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    allowMmapReads
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_allowMmapReads(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::DBOptions*>(jhandle)->allow_mmap_reads;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setAllowMmapWrites
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setAllowMmapWrites(
    JNIEnv*, jobject, jlong jhandle, jboolean allow_mmap_writes) {
  reinterpret_cast<rocksdb::DBOptions*>(jhandle)->allow_mmap_writes =
      static_cast<bool>(allow_mmap_writes);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    allowMmapWrites
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_allowMmapWrites(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::DBOptions*>(jhandle)->allow_mmap_writes;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setIsFdCloseOnExec
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setIsFdCloseOnExec(
    JNIEnv*, jobject, jlong jhandle, jboolean is_fd_close_on_exec) {
  reinterpret_cast<rocksdb::DBOptions*>(jhandle)->is_fd_close_on_exec =
      static_cast<bool>(is_fd_close_on_exec);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    isFdCloseOnExec
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_isFdCloseOnExec(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::DBOptions*>(jhandle)->is_fd_close_on_exec;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setStatsDumpPeriodSec
 * Signature: (JI)V
 */
void Java_org_rocksdb_DBOptions_setStatsDumpPeriodSec(
    JNIEnv*, jobject, jlong jhandle, jint stats_dump_period_sec) {
  reinterpret_cast<rocksdb::DBOptions*>(jhandle)->stats_dump_period_sec =
      static_cast<int>(stats_dump_period_sec);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    statsDumpPeriodSec
 * Signature: (J)I
 */
jint Java_org_rocksdb_DBOptions_statsDumpPeriodSec(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::DBOptions*>(jhandle)->stats_dump_period_sec;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setAdviseRandomOnOpen
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setAdviseRandomOnOpen(
    JNIEnv*, jobject, jlong jhandle, jboolean advise_random_on_open) {
  reinterpret_cast<rocksdb::DBOptions*>(jhandle)->advise_random_on_open =
      static_cast<bool>(advise_random_on_open);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    adviseRandomOnOpen
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_adviseRandomOnOpen(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::DBOptions*>(jhandle)->advise_random_on_open;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setDbWriteBufferSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setDbWriteBufferSize(
    JNIEnv*, jobject, jlong jhandle, jlong jdb_write_buffer_size) {
  auto* opt = reinterpret_cast<rocksdb::DBOptions*>(jhandle);
  opt->db_write_buffer_size = static_cast<size_t>(jdb_write_buffer_size);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setWriteBufferManager
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setWriteBufferManager(
    JNIEnv*, jobject, jlong jdb_options_handle,
    jlong jwrite_buffer_manager_handle) {
  auto* write_buffer_manager =
      reinterpret_cast<std::shared_ptr<rocksdb::WriteBufferManager> *>(jwrite_buffer_manager_handle);
  reinterpret_cast<rocksdb::DBOptions*>(jdb_options_handle)->write_buffer_manager =
      *write_buffer_manager;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    dbWriteBufferSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_dbWriteBufferSize(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<rocksdb::DBOptions*>(jhandle);
  return static_cast<jlong>(opt->db_write_buffer_size);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setAccessHintOnCompactionStart
 * Signature: (JB)V
 */
void Java_org_rocksdb_DBOptions_setAccessHintOnCompactionStart(
    JNIEnv*, jobject, jlong jhandle, jbyte jaccess_hint_value) {
  auto* opt = reinterpret_cast<rocksdb::DBOptions*>(jhandle);
  opt->access_hint_on_compaction_start =
      rocksdb::AccessHintJni::toCppAccessHint(jaccess_hint_value);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    accessHintOnCompactionStart
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_DBOptions_accessHintOnCompactionStart(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<rocksdb::DBOptions*>(jhandle);
  return rocksdb::AccessHintJni::toJavaAccessHint(
      opt->access_hint_on_compaction_start);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setNewTableReaderForCompactionInputs
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setNewTableReaderForCompactionInputs(
    JNIEnv*, jobject, jlong jhandle,
    jboolean jnew_table_reader_for_compaction_inputs) {
  auto* opt = reinterpret_cast<rocksdb::DBOptions*>(jhandle);
  opt->new_table_reader_for_compaction_inputs =
      static_cast<bool>(jnew_table_reader_for_compaction_inputs);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    newTableReaderForCompactionInputs
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_newTableReaderForCompactionInputs(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<rocksdb::DBOptions*>(jhandle);
  return static_cast<bool>(opt->new_table_reader_for_compaction_inputs);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setCompactionReadaheadSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setCompactionReadaheadSize(
    JNIEnv*, jobject, jlong jhandle, jlong jcompaction_readahead_size) {
  auto* opt = reinterpret_cast<rocksdb::DBOptions*>(jhandle);
  opt->compaction_readahead_size =
      static_cast<size_t>(jcompaction_readahead_size);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    compactionReadaheadSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_compactionReadaheadSize(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<rocksdb::DBOptions*>(jhandle);
  return static_cast<jlong>(opt->compaction_readahead_size);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setRandomAccessMaxBufferSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setRandomAccessMaxBufferSize(
    JNIEnv*, jobject, jlong jhandle, jlong jrandom_access_max_buffer_size) {
  auto* opt = reinterpret_cast<rocksdb::DBOptions*>(jhandle);
  opt->random_access_max_buffer_size =
      static_cast<size_t>(jrandom_access_max_buffer_size);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    randomAccessMaxBufferSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_randomAccessMaxBufferSize(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<rocksdb::DBOptions*>(jhandle);
  return static_cast<jlong>(opt->random_access_max_buffer_size);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setWritableFileMaxBufferSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setWritableFileMaxBufferSize(
    JNIEnv*, jobject, jlong jhandle, jlong jwritable_file_max_buffer_size) {
  auto* opt = reinterpret_cast<rocksdb::DBOptions*>(jhandle);
  opt->writable_file_max_buffer_size =
      static_cast<size_t>(jwritable_file_max_buffer_size);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    writableFileMaxBufferSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_writableFileMaxBufferSize(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<rocksdb::DBOptions*>(jhandle);
  return static_cast<jlong>(opt->writable_file_max_buffer_size);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setUseAdaptiveMutex
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setUseAdaptiveMutex(
    JNIEnv*, jobject, jlong jhandle, jboolean use_adaptive_mutex) {
  reinterpret_cast<rocksdb::DBOptions*>(jhandle)->use_adaptive_mutex =
      static_cast<bool>(use_adaptive_mutex);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    useAdaptiveMutex
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_useAdaptiveMutex(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::DBOptions*>(jhandle)->use_adaptive_mutex;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setBytesPerSync
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setBytesPerSync(
    JNIEnv*, jobject, jlong jhandle, jlong bytes_per_sync) {
  reinterpret_cast<rocksdb::DBOptions*>(jhandle)->bytes_per_sync =
      static_cast<int64_t>(bytes_per_sync);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    bytesPerSync
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_bytesPerSync(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::DBOptions*>(jhandle)->bytes_per_sync;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setWalBytesPerSync
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setWalBytesPerSync(
    JNIEnv*, jobject, jlong jhandle, jlong jwal_bytes_per_sync) {
  reinterpret_cast<rocksdb::DBOptions*>(jhandle)->wal_bytes_per_sync =
      static_cast<int64_t>(jwal_bytes_per_sync);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    walBytesPerSync
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_walBytesPerSync(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<rocksdb::DBOptions*>(jhandle);
  return static_cast<jlong>(opt->wal_bytes_per_sync);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setDelayedWriteRate
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setDelayedWriteRate(
    JNIEnv*, jobject, jlong jhandle, jlong jdelayed_write_rate) {
  auto* opt = reinterpret_cast<rocksdb::DBOptions*>(jhandle);
  opt->delayed_write_rate = static_cast<uint64_t>(jdelayed_write_rate);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    delayedWriteRate
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_delayedWriteRate(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<rocksdb::DBOptions*>(jhandle);
  return static_cast<jlong>(opt->delayed_write_rate);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setEnablePipelinedWrite
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setEnablePipelinedWrite(
    JNIEnv*, jobject, jlong jhandle, jboolean jenable_pipelined_write) {
  auto* opt = reinterpret_cast<rocksdb::DBOptions*>(jhandle);
  opt->enable_pipelined_write = jenable_pipelined_write == JNI_TRUE;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    enablePipelinedWrite
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_enablePipelinedWrite(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<rocksdb::DBOptions*>(jhandle);
  return static_cast<jboolean>(opt->enable_pipelined_write);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setEnableThreadTracking
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setEnableThreadTracking(
    JNIEnv*, jobject, jlong jhandle, jboolean jenable_thread_tracking) {
  auto* opt = reinterpret_cast<rocksdb::DBOptions*>(jhandle);
  opt->enable_thread_tracking = jenable_thread_tracking == JNI_TRUE;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    enableThreadTracking
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_enableThreadTracking(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<rocksdb::DBOptions*>(jhandle);
  return static_cast<jboolean>(opt->enable_thread_tracking);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setAllowConcurrentMemtableWrite
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setAllowConcurrentMemtableWrite(
    JNIEnv*, jobject, jlong jhandle, jboolean allow) {
  reinterpret_cast<rocksdb::DBOptions*>(jhandle)
      ->allow_concurrent_memtable_write = static_cast<bool>(allow);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    allowConcurrentMemtableWrite
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_allowConcurrentMemtableWrite(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::DBOptions*>(jhandle)
      ->allow_concurrent_memtable_write;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setEnableWriteThreadAdaptiveYield
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setEnableWriteThreadAdaptiveYield(
    JNIEnv*, jobject, jlong jhandle, jboolean yield) {
  reinterpret_cast<rocksdb::DBOptions*>(jhandle)
      ->enable_write_thread_adaptive_yield = static_cast<bool>(yield);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    enableWriteThreadAdaptiveYield
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_enableWriteThreadAdaptiveYield(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::DBOptions*>(jhandle)
      ->enable_write_thread_adaptive_yield;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setWriteThreadMaxYieldUsec
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setWriteThreadMaxYieldUsec(
    JNIEnv*, jobject, jlong jhandle, jlong max) {
  reinterpret_cast<rocksdb::DBOptions*>(jhandle)->write_thread_max_yield_usec =
      static_cast<int64_t>(max);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    writeThreadMaxYieldUsec
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_writeThreadMaxYieldUsec(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::DBOptions*>(jhandle)
      ->write_thread_max_yield_usec;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setWriteThreadSlowYieldUsec
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setWriteThreadSlowYieldUsec(
    JNIEnv*, jobject, jlong jhandle, jlong slow) {
  reinterpret_cast<rocksdb::DBOptions*>(jhandle)->write_thread_slow_yield_usec =
      static_cast<int64_t>(slow);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    writeThreadSlowYieldUsec
 * Signature: (J)J
 */
jlong Java_org_rocksdb_DBOptions_writeThreadSlowYieldUsec(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::DBOptions*>(jhandle)
      ->write_thread_slow_yield_usec;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setSkipStatsUpdateOnDbOpen
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setSkipStatsUpdateOnDbOpen(
    JNIEnv*, jobject, jlong jhandle, jboolean jskip_stats_update_on_db_open) {
  auto* opt = reinterpret_cast<rocksdb::DBOptions*>(jhandle);
  opt->skip_stats_update_on_db_open =
      static_cast<bool>(jskip_stats_update_on_db_open);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    skipStatsUpdateOnDbOpen
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_skipStatsUpdateOnDbOpen(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<rocksdb::DBOptions*>(jhandle);
  return static_cast<jboolean>(opt->skip_stats_update_on_db_open);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setWalRecoveryMode
 * Signature: (JB)V
 */
void Java_org_rocksdb_DBOptions_setWalRecoveryMode(
    JNIEnv*, jobject, jlong jhandle, jbyte jwal_recovery_mode_value) {
  auto* opt = reinterpret_cast<rocksdb::DBOptions*>(jhandle);
  opt->wal_recovery_mode = rocksdb::WALRecoveryModeJni::toCppWALRecoveryMode(
      jwal_recovery_mode_value);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    walRecoveryMode
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_DBOptions_walRecoveryMode(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<rocksdb::DBOptions*>(jhandle);
  return rocksdb::WALRecoveryModeJni::toJavaWALRecoveryMode(
      opt->wal_recovery_mode);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setAllow2pc
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setAllow2pc(
    JNIEnv*, jobject, jlong jhandle, jboolean jallow_2pc) {
  auto* opt = reinterpret_cast<rocksdb::DBOptions*>(jhandle);
  opt->allow_2pc = static_cast<bool>(jallow_2pc);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    allow2pc
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_allow2pc(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<rocksdb::DBOptions*>(jhandle);
  return static_cast<jboolean>(opt->allow_2pc);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setRowCache
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setRowCache(
    JNIEnv*, jobject, jlong jhandle, jlong jrow_cache_handle) {
  auto* opt = reinterpret_cast<rocksdb::DBOptions*>(jhandle);
  auto* row_cache =
      reinterpret_cast<std::shared_ptr<rocksdb::Cache>*>(jrow_cache_handle);
  opt->row_cache = *row_cache;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setWalFilter
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DBOptions_setWalFilter(
    JNIEnv*, jobject, jlong jhandle, jlong jwal_filter_handle) {
  auto* opt = reinterpret_cast<rocksdb::DBOptions*>(jhandle);
  auto* wal_filter =
    reinterpret_cast<rocksdb::WalFilterJniCallback*>(jwal_filter_handle);
  opt->wal_filter = wal_filter;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setFailIfOptionsFileError
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setFailIfOptionsFileError(
    JNIEnv*, jobject, jlong jhandle, jboolean jfail_if_options_file_error) {
  auto* opt = reinterpret_cast<rocksdb::DBOptions*>(jhandle);
  opt->fail_if_options_file_error =
      static_cast<bool>(jfail_if_options_file_error);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    failIfOptionsFileError
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_failIfOptionsFileError(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<rocksdb::DBOptions*>(jhandle);
  return static_cast<jboolean>(opt->fail_if_options_file_error);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setDumpMallocStats
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setDumpMallocStats(
    JNIEnv*, jobject, jlong jhandle, jboolean jdump_malloc_stats) {
  auto* opt = reinterpret_cast<rocksdb::DBOptions*>(jhandle);
  opt->dump_malloc_stats = static_cast<bool>(jdump_malloc_stats);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    dumpMallocStats
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_dumpMallocStats(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<rocksdb::DBOptions*>(jhandle);
  return static_cast<jboolean>(opt->dump_malloc_stats);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setAvoidFlushDuringRecovery
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setAvoidFlushDuringRecovery(
    JNIEnv*, jobject, jlong jhandle, jboolean javoid_flush_during_recovery) {
  auto* opt = reinterpret_cast<rocksdb::DBOptions*>(jhandle);
  opt->avoid_flush_during_recovery =
      static_cast<bool>(javoid_flush_during_recovery);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    avoidFlushDuringRecovery
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_avoidFlushDuringRecovery(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<rocksdb::DBOptions*>(jhandle);
  return static_cast<jboolean>(opt->avoid_flush_during_recovery);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setAllowIngestBehind
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setAllowIngestBehind(
    JNIEnv*, jobject, jlong jhandle, jboolean jallow_ingest_behind) {
  auto* opt = reinterpret_cast<rocksdb::DBOptions*>(jhandle);
  opt->allow_ingest_behind = jallow_ingest_behind == JNI_TRUE;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    allowIngestBehind
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_allowIngestBehind(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<rocksdb::DBOptions*>(jhandle);
  return static_cast<jboolean>(opt->allow_ingest_behind);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setPreserveDeletes
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setPreserveDeletes(
    JNIEnv*, jobject, jlong jhandle, jboolean jpreserve_deletes) {
  auto* opt = reinterpret_cast<rocksdb::DBOptions*>(jhandle);
  opt->preserve_deletes = jpreserve_deletes == JNI_TRUE;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    preserveDeletes
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_preserveDeletes(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<rocksdb::DBOptions*>(jhandle);
  return static_cast<jboolean>(opt->preserve_deletes);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setTwoWriteQueues
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setTwoWriteQueues(
    JNIEnv*, jobject, jlong jhandle, jboolean jtwo_write_queues) {
  auto* opt = reinterpret_cast<rocksdb::DBOptions*>(jhandle);
  opt->two_write_queues = jtwo_write_queues == JNI_TRUE;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    twoWriteQueues
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_twoWriteQueues(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<rocksdb::DBOptions*>(jhandle);
  return static_cast<jboolean>(opt->two_write_queues);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setManualWalFlush
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setManualWalFlush(
    JNIEnv*, jobject, jlong jhandle, jboolean jmanual_wal_flush) {
  auto* opt = reinterpret_cast<rocksdb::DBOptions*>(jhandle);
  opt->manual_wal_flush = jmanual_wal_flush == JNI_TRUE;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    manualWalFlush
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_manualWalFlush(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<rocksdb::DBOptions*>(jhandle);
  return static_cast<jboolean>(opt->manual_wal_flush);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setAtomicFlush
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setAtomicFlush(
    JNIEnv*, jobject, jlong jhandle, jboolean jatomic_flush) {
  auto* opt = reinterpret_cast<rocksdb::DBOptions*>(jhandle);
  opt->atomic_flush = jatomic_flush == JNI_TRUE;
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    atomicFlush
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_atomicFlush(
    JNIEnv *, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<rocksdb::DBOptions*>(jhandle);
  return static_cast<jboolean>(opt->atomic_flush);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    setAvoidFlushDuringShutdown
 * Signature: (JZ)V
 */
void Java_org_rocksdb_DBOptions_setAvoidFlushDuringShutdown(
    JNIEnv*, jobject, jlong jhandle, jboolean javoid_flush_during_shutdown) {
  auto* opt = reinterpret_cast<rocksdb::DBOptions*>(jhandle);
  opt->avoid_flush_during_shutdown =
      static_cast<bool>(javoid_flush_during_shutdown);
}

/*
 * Class:     org_rocksdb_DBOptions
 * Method:    avoidFlushDuringShutdown
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_DBOptions_avoidFlushDuringShutdown(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<rocksdb::DBOptions*>(jhandle);
  return static_cast<jboolean>(opt->avoid_flush_during_shutdown);
}

//////////////////////////////////////////////////////////////////////////////
// rocksdb::WriteOptions

/*
 * Class:     org_rocksdb_WriteOptions
 * Method:    newWriteOptions
 * Signature: ()J
 */
jlong Java_org_rocksdb_WriteOptions_newWriteOptions(
    JNIEnv*, jclass) {
  auto* op = new rocksdb::WriteOptions();
  return reinterpret_cast<jlong>(op);
}

/*
 * Class:     org_rocksdb_WriteOptions
 * Method:    copyWriteOptions
 * Signature: (J)J
 */
jlong Java_org_rocksdb_WriteOptions_copyWriteOptions(
    JNIEnv*, jclass, jlong jhandle) {
  auto new_opt = new rocksdb::WriteOptions(
      *(reinterpret_cast<rocksdb::WriteOptions*>(jhandle)));
  return reinterpret_cast<jlong>(new_opt);
}

/*
 * Class:     org_rocksdb_WriteOptions
 * Method:    disposeInternal
 * Signature: ()V
 */
void Java_org_rocksdb_WriteOptions_disposeInternal(
    JNIEnv*, jobject, jlong jhandle) {
  auto* write_options = reinterpret_cast<rocksdb::WriteOptions*>(jhandle);
  assert(write_options != nullptr);
  delete write_options;
}

/*
 * Class:     org_rocksdb_WriteOptions
 * Method:    setSync
 * Signature: (JZ)V
 */
void Java_org_rocksdb_WriteOptions_setSync(
    JNIEnv*, jobject, jlong jhandle, jboolean jflag) {
  reinterpret_cast<rocksdb::WriteOptions*>(jhandle)->sync = jflag;
}

/*
 * Class:     org_rocksdb_WriteOptions
 * Method:    sync
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_WriteOptions_sync(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::WriteOptions*>(jhandle)->sync;
}

/*
 * Class:     org_rocksdb_WriteOptions
 * Method:    setDisableWAL
 * Signature: (JZ)V
 */
void Java_org_rocksdb_WriteOptions_setDisableWAL(
    JNIEnv*, jobject, jlong jhandle, jboolean jflag) {
  reinterpret_cast<rocksdb::WriteOptions*>(jhandle)->disableWAL = jflag;
}

/*
 * Class:     org_rocksdb_WriteOptions
 * Method:    disableWAL
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_WriteOptions_disableWAL(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::WriteOptions*>(jhandle)->disableWAL;
}

/*
 * Class:     org_rocksdb_WriteOptions
 * Method:    setIgnoreMissingColumnFamilies
 * Signature: (JZ)V
 */
void Java_org_rocksdb_WriteOptions_setIgnoreMissingColumnFamilies(
    JNIEnv*, jobject, jlong jhandle,
    jboolean jignore_missing_column_families) {
  reinterpret_cast<rocksdb::WriteOptions*>(jhandle)
      ->ignore_missing_column_families =
      static_cast<bool>(jignore_missing_column_families);
}

/*
 * Class:     org_rocksdb_WriteOptions
 * Method:    ignoreMissingColumnFamilies
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_WriteOptions_ignoreMissingColumnFamilies(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::WriteOptions*>(jhandle)
      ->ignore_missing_column_families;
}

/*
 * Class:     org_rocksdb_WriteOptions
 * Method:    setNoSlowdown
 * Signature: (JZ)V
 */
void Java_org_rocksdb_WriteOptions_setNoSlowdown(
    JNIEnv*, jobject, jlong jhandle, jboolean jno_slowdown) {
  reinterpret_cast<rocksdb::WriteOptions*>(jhandle)->no_slowdown =
      static_cast<bool>(jno_slowdown);
}

/*
 * Class:     org_rocksdb_WriteOptions
 * Method:    noSlowdown
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_WriteOptions_noSlowdown(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::WriteOptions*>(jhandle)->no_slowdown;
}

/*
 * Class:     org_rocksdb_WriteOptions
 * Method:    setLowPri
 * Signature: (JZ)V
 */
void Java_org_rocksdb_WriteOptions_setLowPri(
    JNIEnv*, jobject, jlong jhandle, jboolean jlow_pri) {
  reinterpret_cast<rocksdb::WriteOptions*>(jhandle)->low_pri =
      static_cast<bool>(jlow_pri);
}

/*
 * Class:     org_rocksdb_WriteOptions
 * Method:    lowPri
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_WriteOptions_lowPri(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::WriteOptions*>(jhandle)->low_pri;
}

/////////////////////////////////////////////////////////////////////
// rocksdb::ReadOptions

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    newReadOptions
 * Signature: ()J
 */
jlong Java_org_rocksdb_ReadOptions_newReadOptions__(
    JNIEnv*, jclass) {
  auto* read_options = new rocksdb::ReadOptions();
  return reinterpret_cast<jlong>(read_options);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    newReadOptions
 * Signature: (ZZ)J
 */
jlong Java_org_rocksdb_ReadOptions_newReadOptions__ZZ(
    JNIEnv*, jclass, jboolean jverify_checksums, jboolean jfill_cache) {
  auto* read_options =
      new rocksdb::ReadOptions(static_cast<bool>(jverify_checksums),
          static_cast<bool>(jfill_cache));
  return reinterpret_cast<jlong>(read_options);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    copyReadOptions
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ReadOptions_copyReadOptions(
    JNIEnv*, jclass, jlong jhandle) {
  auto new_opt = new rocksdb::ReadOptions(
      *(reinterpret_cast<rocksdb::ReadOptions*>(jhandle)));
  return reinterpret_cast<jlong>(new_opt);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_ReadOptions_disposeInternal(
    JNIEnv*, jobject, jlong jhandle) {
  auto* read_options = reinterpret_cast<rocksdb::ReadOptions*>(jhandle);
  assert(read_options != nullptr);
  delete read_options;
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    setVerifyChecksums
 * Signature: (JZ)V
 */
void Java_org_rocksdb_ReadOptions_setVerifyChecksums(
    JNIEnv*, jobject, jlong jhandle, jboolean jverify_checksums) {
  reinterpret_cast<rocksdb::ReadOptions*>(jhandle)->verify_checksums =
      static_cast<bool>(jverify_checksums);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    verifyChecksums
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_ReadOptions_verifyChecksums(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::ReadOptions*>(jhandle)->verify_checksums;
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    setFillCache
 * Signature: (JZ)V
 */
void Java_org_rocksdb_ReadOptions_setFillCache(
    JNIEnv*, jobject, jlong jhandle, jboolean jfill_cache) {
  reinterpret_cast<rocksdb::ReadOptions*>(jhandle)->fill_cache =
      static_cast<bool>(jfill_cache);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    fillCache
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_ReadOptions_fillCache(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::ReadOptions*>(jhandle)->fill_cache;
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    setTailing
 * Signature: (JZ)V
 */
void Java_org_rocksdb_ReadOptions_setTailing(
    JNIEnv*, jobject, jlong jhandle, jboolean jtailing) {
  reinterpret_cast<rocksdb::ReadOptions*>(jhandle)->tailing =
      static_cast<bool>(jtailing);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    tailing
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_ReadOptions_tailing(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::ReadOptions*>(jhandle)->tailing;
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    managed
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_ReadOptions_managed(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::ReadOptions*>(jhandle)->managed;
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    setManaged
 * Signature: (JZ)V
 */
void Java_org_rocksdb_ReadOptions_setManaged(
    JNIEnv*, jobject, jlong jhandle, jboolean jmanaged) {
  reinterpret_cast<rocksdb::ReadOptions*>(jhandle)->managed =
      static_cast<bool>(jmanaged);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    totalOrderSeek
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_ReadOptions_totalOrderSeek(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::ReadOptions*>(jhandle)->total_order_seek;
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    setTotalOrderSeek
 * Signature: (JZ)V
 */
void Java_org_rocksdb_ReadOptions_setTotalOrderSeek(
    JNIEnv*, jobject, jlong jhandle, jboolean jtotal_order_seek) {
  reinterpret_cast<rocksdb::ReadOptions*>(jhandle)->total_order_seek =
      static_cast<bool>(jtotal_order_seek);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    prefixSameAsStart
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_ReadOptions_prefixSameAsStart(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::ReadOptions*>(jhandle)->prefix_same_as_start;
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    setPrefixSameAsStart
 * Signature: (JZ)V
 */
void Java_org_rocksdb_ReadOptions_setPrefixSameAsStart(
    JNIEnv*, jobject, jlong jhandle, jboolean jprefix_same_as_start) {
  reinterpret_cast<rocksdb::ReadOptions*>(jhandle)->prefix_same_as_start =
      static_cast<bool>(jprefix_same_as_start);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    pinData
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_ReadOptions_pinData(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::ReadOptions*>(jhandle)->pin_data;
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    setPinData
 * Signature: (JZ)V
 */
void Java_org_rocksdb_ReadOptions_setPinData(
    JNIEnv*, jobject, jlong jhandle, jboolean jpin_data) {
  reinterpret_cast<rocksdb::ReadOptions*>(jhandle)->pin_data =
      static_cast<bool>(jpin_data);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    backgroundPurgeOnIteratorCleanup
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_ReadOptions_backgroundPurgeOnIteratorCleanup(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<rocksdb::ReadOptions*>(jhandle);
  return static_cast<jboolean>(opt->background_purge_on_iterator_cleanup);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    setBackgroundPurgeOnIteratorCleanup
 * Signature: (JZ)V
 */
void Java_org_rocksdb_ReadOptions_setBackgroundPurgeOnIteratorCleanup(
    JNIEnv*, jobject, jlong jhandle,
    jboolean jbackground_purge_on_iterator_cleanup) {
  auto* opt = reinterpret_cast<rocksdb::ReadOptions*>(jhandle);
  opt->background_purge_on_iterator_cleanup =
      static_cast<bool>(jbackground_purge_on_iterator_cleanup);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    readaheadSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ReadOptions_readaheadSize(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<rocksdb::ReadOptions*>(jhandle);
  return static_cast<jlong>(opt->readahead_size);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    setReadaheadSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ReadOptions_setReadaheadSize(
    JNIEnv*, jobject, jlong jhandle, jlong jreadahead_size) {
  auto* opt = reinterpret_cast<rocksdb::ReadOptions*>(jhandle);
  opt->readahead_size = static_cast<size_t>(jreadahead_size);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    maxSkippableInternalKeys
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ReadOptions_maxSkippableInternalKeys(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<rocksdb::ReadOptions*>(jhandle);
  return static_cast<jlong>(opt->max_skippable_internal_keys);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    setMaxSkippableInternalKeys
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ReadOptions_setMaxSkippableInternalKeys(
    JNIEnv*, jobject, jlong jhandle, jlong jmax_skippable_internal_keys) {
  auto* opt = reinterpret_cast<rocksdb::ReadOptions*>(jhandle);
  opt->max_skippable_internal_keys =
      static_cast<uint64_t>(jmax_skippable_internal_keys);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    ignoreRangeDeletions
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_ReadOptions_ignoreRangeDeletions(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<rocksdb::ReadOptions*>(jhandle);
  return static_cast<jboolean>(opt->ignore_range_deletions);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    setIgnoreRangeDeletions
 * Signature: (JZ)V
 */
void Java_org_rocksdb_ReadOptions_setIgnoreRangeDeletions(
    JNIEnv*, jobject, jlong jhandle, jboolean jignore_range_deletions) {
  auto* opt = reinterpret_cast<rocksdb::ReadOptions*>(jhandle);
  opt->ignore_range_deletions = static_cast<bool>(jignore_range_deletions);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    setSnapshot
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ReadOptions_setSnapshot(
    JNIEnv*, jobject, jlong jhandle, jlong jsnapshot) {
  reinterpret_cast<rocksdb::ReadOptions*>(jhandle)->snapshot =
      reinterpret_cast<rocksdb::Snapshot*>(jsnapshot);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    snapshot
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ReadOptions_snapshot(
    JNIEnv*, jobject, jlong jhandle) {
  auto& snapshot = reinterpret_cast<rocksdb::ReadOptions*>(jhandle)->snapshot;
  return reinterpret_cast<jlong>(snapshot);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    readTier
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_ReadOptions_readTier(
    JNIEnv*, jobject, jlong jhandle) {
  return static_cast<jbyte>(
      reinterpret_cast<rocksdb::ReadOptions*>(jhandle)->read_tier);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    setReadTier
 * Signature: (JB)V
 */
void Java_org_rocksdb_ReadOptions_setReadTier(
    JNIEnv*, jobject, jlong jhandle, jbyte jread_tier) {
  reinterpret_cast<rocksdb::ReadOptions*>(jhandle)->read_tier =
      static_cast<rocksdb::ReadTier>(jread_tier);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    setIterateUpperBound
 * Signature: (JJ)I
 */
void Java_org_rocksdb_ReadOptions_setIterateUpperBound(
    JNIEnv*, jobject, jlong jhandle, jlong jupper_bound_slice_handle) {
  reinterpret_cast<rocksdb::ReadOptions*>(jhandle)->iterate_upper_bound =
      reinterpret_cast<rocksdb::Slice*>(jupper_bound_slice_handle);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    iterateUpperBound
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ReadOptions_iterateUpperBound(
    JNIEnv*, jobject, jlong jhandle) {
  auto& upper_bound_slice_handle =
      reinterpret_cast<rocksdb::ReadOptions*>(jhandle)->iterate_upper_bound;
  return reinterpret_cast<jlong>(upper_bound_slice_handle);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    setIterateLowerBound
 * Signature: (JJ)I
 */
void Java_org_rocksdb_ReadOptions_setIterateLowerBound(
    JNIEnv*, jobject, jlong jhandle, jlong jlower_bound_slice_handle) {
  reinterpret_cast<rocksdb::ReadOptions*>(jhandle)->iterate_lower_bound =
      reinterpret_cast<rocksdb::Slice*>(jlower_bound_slice_handle);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    iterateLowerBound
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ReadOptions_iterateLowerBound(
    JNIEnv*, jobject, jlong jhandle) {
  auto& lower_bound_slice_handle =
      reinterpret_cast<rocksdb::ReadOptions*>(jhandle)->iterate_lower_bound;
  return reinterpret_cast<jlong>(lower_bound_slice_handle);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    setTableFilter
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ReadOptions_setTableFilter(
    JNIEnv*, jobject, jlong jhandle, jlong jjni_table_filter_handle) {
  auto* opt = reinterpret_cast<rocksdb::ReadOptions*>(jhandle);
  auto* jni_table_filter =
      reinterpret_cast<rocksdb::TableFilterJniCallback*>(jjni_table_filter_handle);
  opt->table_filter = jni_table_filter->GetTableFilterFunction();
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    setIterStartSeqnum
 * Signature: (JJ)V
 */
void Java_org_rocksdb_ReadOptions_setIterStartSeqnum(
    JNIEnv*, jobject, jlong jhandle, jlong jiter_start_seqnum) {
  auto* opt = reinterpret_cast<rocksdb::ReadOptions*>(jhandle);
  opt->iter_start_seqnum = static_cast<uint64_t>(jiter_start_seqnum);
}

/*
 * Class:     org_rocksdb_ReadOptions
 * Method:    iterStartSeqnum
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ReadOptions_iterStartSeqnum(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<rocksdb::ReadOptions*>(jhandle);
  return static_cast<jlong>(opt->iter_start_seqnum);
}

/////////////////////////////////////////////////////////////////////
// rocksdb::ComparatorOptions

/*
 * Class:     org_rocksdb_ComparatorOptions
 * Method:    newComparatorOptions
 * Signature: ()J
 */
jlong Java_org_rocksdb_ComparatorOptions_newComparatorOptions(
    JNIEnv*, jclass) {
  auto* comparator_opt = new rocksdb::ComparatorJniCallbackOptions();
  return reinterpret_cast<jlong>(comparator_opt);
}

/*
 * Class:     org_rocksdb_ComparatorOptions
 * Method:    useAdaptiveMutex
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_ComparatorOptions_useAdaptiveMutex(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::ComparatorJniCallbackOptions*>(jhandle)
      ->use_adaptive_mutex;
}

/*
 * Class:     org_rocksdb_ComparatorOptions
 * Method:    setUseAdaptiveMutex
 * Signature: (JZ)V
 */
void Java_org_rocksdb_ComparatorOptions_setUseAdaptiveMutex(
    JNIEnv*, jobject, jlong jhandle, jboolean juse_adaptive_mutex) {
  reinterpret_cast<rocksdb::ComparatorJniCallbackOptions*>(jhandle)
      ->use_adaptive_mutex = static_cast<bool>(juse_adaptive_mutex);
}

/*
 * Class:     org_rocksdb_ComparatorOptions
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_ComparatorOptions_disposeInternal(
    JNIEnv*, jobject, jlong jhandle) {
  auto* comparator_opt =
      reinterpret_cast<rocksdb::ComparatorJniCallbackOptions*>(jhandle);
  assert(comparator_opt != nullptr);
  delete comparator_opt;
}

/////////////////////////////////////////////////////////////////////
// rocksdb::FlushOptions

/*
 * Class:     org_rocksdb_FlushOptions
 * Method:    newFlushOptions
 * Signature: ()J
 */
jlong Java_org_rocksdb_FlushOptions_newFlushOptions(
    JNIEnv*, jclass) {
  auto* flush_opt = new rocksdb::FlushOptions();
  return reinterpret_cast<jlong>(flush_opt);
}

/*
 * Class:     org_rocksdb_FlushOptions
 * Method:    setWaitForFlush
 * Signature: (JZ)V
 */
void Java_org_rocksdb_FlushOptions_setWaitForFlush(
    JNIEnv*, jobject, jlong jhandle, jboolean jwait) {
  reinterpret_cast<rocksdb::FlushOptions*>(jhandle)->wait =
      static_cast<bool>(jwait);
}

/*
 * Class:     org_rocksdb_FlushOptions
 * Method:    waitForFlush
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_FlushOptions_waitForFlush(
    JNIEnv*, jobject, jlong jhandle) {
  return reinterpret_cast<rocksdb::FlushOptions*>(jhandle)->wait;
}

/*
 * Class:     org_rocksdb_FlushOptions
 * Method:    setAllowWriteStall
 * Signature: (JZ)V
 */
void Java_org_rocksdb_FlushOptions_setAllowWriteStall(
    JNIEnv*, jobject, jlong jhandle, jboolean jallow_write_stall) {
  auto* flush_options = reinterpret_cast<rocksdb::FlushOptions*>(jhandle);
  flush_options->allow_write_stall = jallow_write_stall == JNI_TRUE;
}

/*
 * Class:     org_rocksdb_FlushOptions
 * Method:    allowWriteStall
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_FlushOptions_allowWriteStall(
    JNIEnv*, jobject, jlong jhandle) {
  auto* flush_options = reinterpret_cast<rocksdb::FlushOptions*>(jhandle);
  return static_cast<jboolean>(flush_options->allow_write_stall);
}

/*
 * Class:     org_rocksdb_FlushOptions
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_FlushOptions_disposeInternal(
    JNIEnv*, jobject, jlong jhandle) {
  auto* flush_opt = reinterpret_cast<rocksdb::FlushOptions*>(jhandle);
  assert(flush_opt != nullptr);
  delete flush_opt;
}
