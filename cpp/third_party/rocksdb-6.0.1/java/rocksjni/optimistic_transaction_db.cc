// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++
// for rocksdb::TransactionDB.

#include <jni.h>

#include "include/org_rocksdb_OptimisticTransactionDB.h"

#include "rocksdb/options.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "rocksdb/utilities/transaction.h"

#include "rocksjni/portal.h"

/*
 * Class:     org_rocksdb_OptimisticTransactionDB
 * Method:    open
 * Signature: (JLjava/lang/String;)J
 */
jlong Java_org_rocksdb_OptimisticTransactionDB_open__JLjava_lang_String_2(
    JNIEnv* env, jclass, jlong joptions_handle, jstring jdb_path) {
  const char* db_path = env->GetStringUTFChars(jdb_path, nullptr);
  if (db_path == nullptr) {
    // exception thrown: OutOfMemoryError
    return 0;
  }

  auto* options = reinterpret_cast<rocksdb::Options*>(joptions_handle);
  rocksdb::OptimisticTransactionDB* otdb = nullptr;
  rocksdb::Status s =
      rocksdb::OptimisticTransactionDB::Open(*options, db_path, &otdb);
  env->ReleaseStringUTFChars(jdb_path, db_path);

  if (s.ok()) {
    return reinterpret_cast<jlong>(otdb);
  } else {
    rocksdb::RocksDBExceptionJni::ThrowNew(env, s);
    return 0;
  }
}

/*
 * Class:     org_rocksdb_OptimisticTransactionDB
 * Method:    open
 * Signature: (JLjava/lang/String;[[B[J)[J
 */
jlongArray
Java_org_rocksdb_OptimisticTransactionDB_open__JLjava_lang_String_2_3_3B_3J(
    JNIEnv* env, jclass, jlong jdb_options_handle, jstring jdb_path,
    jobjectArray jcolumn_names, jlongArray jcolumn_options_handles) {
  const char* db_path = env->GetStringUTFChars(jdb_path, nullptr);
  if (db_path == nullptr) {
    // exception thrown: OutOfMemoryError
    return nullptr;
  }

  std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
  const jsize len_cols = env->GetArrayLength(jcolumn_names);
  if (len_cols > 0) {
    if (env->EnsureLocalCapacity(len_cols) != 0) {
      // out of memory
      env->ReleaseStringUTFChars(jdb_path, db_path);
      return nullptr;
    }

    jlong* jco = env->GetLongArrayElements(jcolumn_options_handles, nullptr);
    if (jco == nullptr) {
      // exception thrown: OutOfMemoryError
      env->ReleaseStringUTFChars(jdb_path, db_path);
      return nullptr;
    }

    for (int i = 0; i < len_cols; i++) {
      const jobject jcn = env->GetObjectArrayElement(jcolumn_names, i);
      if (env->ExceptionCheck()) {
        // exception thrown: ArrayIndexOutOfBoundsException
        env->ReleaseLongArrayElements(jcolumn_options_handles, jco, JNI_ABORT);
        env->ReleaseStringUTFChars(jdb_path, db_path);
        return nullptr;
      }

      const jbyteArray jcn_ba = reinterpret_cast<jbyteArray>(jcn);
      const jsize jcf_name_len = env->GetArrayLength(jcn_ba);
      if (env->EnsureLocalCapacity(jcf_name_len) != 0) {
        // out of memory
        env->DeleteLocalRef(jcn);
        env->ReleaseLongArrayElements(jcolumn_options_handles, jco, JNI_ABORT);
        env->ReleaseStringUTFChars(jdb_path, db_path);
        return nullptr;
      }

      jbyte* jcf_name = env->GetByteArrayElements(jcn_ba, nullptr);
      if (jcf_name == nullptr) {
        // exception thrown: OutOfMemoryError
        env->DeleteLocalRef(jcn);
        env->ReleaseLongArrayElements(jcolumn_options_handles, jco, JNI_ABORT);
        env->ReleaseStringUTFChars(jdb_path, db_path);
        return nullptr;
      }

      const std::string cf_name(reinterpret_cast<char*>(jcf_name),
                                jcf_name_len);
      const rocksdb::ColumnFamilyOptions* cf_options =
          reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jco[i]);
      column_families.push_back(
          rocksdb::ColumnFamilyDescriptor(cf_name, *cf_options));

      env->ReleaseByteArrayElements(jcn_ba, jcf_name, JNI_ABORT);
      env->DeleteLocalRef(jcn);
    }
    env->ReleaseLongArrayElements(jcolumn_options_handles, jco, JNI_ABORT);
  }

  auto* db_options = reinterpret_cast<rocksdb::DBOptions*>(jdb_options_handle);
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  rocksdb::OptimisticTransactionDB* otdb = nullptr;
  const rocksdb::Status s = rocksdb::OptimisticTransactionDB::Open(
      *db_options, db_path, column_families, &handles, &otdb);

  env->ReleaseStringUTFChars(jdb_path, db_path);

  // check if open operation was successful
  if (s.ok()) {
    const jsize resultsLen = 1 + len_cols;  // db handle + column family handles
    std::unique_ptr<jlong[]> results =
        std::unique_ptr<jlong[]>(new jlong[resultsLen]);
    results[0] = reinterpret_cast<jlong>(otdb);
    for (int i = 1; i <= len_cols; i++) {
      results[i] = reinterpret_cast<jlong>(handles[i - 1]);
    }

    jlongArray jresults = env->NewLongArray(resultsLen);
    if (jresults == nullptr) {
      // exception thrown: OutOfMemoryError
      return nullptr;
    }
    env->SetLongArrayRegion(jresults, 0, resultsLen, results.get());
    if (env->ExceptionCheck()) {
      // exception thrown: ArrayIndexOutOfBoundsException
      return nullptr;
    }
    return jresults;
  }

  rocksdb::RocksDBExceptionJni::ThrowNew(env, s);
  return nullptr;
}

/*
 * Class:     org_rocksdb_OptimisticTransactionDB
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_OptimisticTransactionDB_disposeInternal(
    JNIEnv *, jobject, jlong jhandle) {
  auto* optimistic_txn_db =
      reinterpret_cast<rocksdb::OptimisticTransactionDB*>(jhandle);
  assert(optimistic_txn_db != nullptr);
  delete optimistic_txn_db;
}

/*
 * Class:     org_rocksdb_OptimisticTransactionDB
 * Method:    closeDatabase
 * Signature: (J)V
 */
void Java_org_rocksdb_OptimisticTransactionDB_closeDatabase(
    JNIEnv* env, jclass, jlong jhandle) {
  auto* optimistic_txn_db =
      reinterpret_cast<rocksdb::OptimisticTransactionDB*>(jhandle);
  assert(optimistic_txn_db != nullptr);
  rocksdb::Status s = optimistic_txn_db->Close();
  rocksdb::RocksDBExceptionJni::ThrowNew(env, s);
}

/*
 * Class:     org_rocksdb_OptimisticTransactionDB
 * Method:    beginTransaction
 * Signature: (JJ)J
 */
jlong Java_org_rocksdb_OptimisticTransactionDB_beginTransaction__JJ(
    JNIEnv*, jobject, jlong jhandle, jlong jwrite_options_handle) {
  auto* optimistic_txn_db =
      reinterpret_cast<rocksdb::OptimisticTransactionDB*>(jhandle);
  auto* write_options =
      reinterpret_cast<rocksdb::WriteOptions*>(jwrite_options_handle);
  rocksdb::Transaction* txn =
      optimistic_txn_db->BeginTransaction(*write_options);
  return reinterpret_cast<jlong>(txn);
}

/*
 * Class:     org_rocksdb_OptimisticTransactionDB
 * Method:    beginTransaction
 * Signature: (JJJ)J
 */
jlong Java_org_rocksdb_OptimisticTransactionDB_beginTransaction__JJJ(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle,
    jlong jwrite_options_handle, jlong joptimistic_txn_options_handle) {
  auto* optimistic_txn_db =
      reinterpret_cast<rocksdb::OptimisticTransactionDB*>(jhandle);
  auto* write_options =
      reinterpret_cast<rocksdb::WriteOptions*>(jwrite_options_handle);
  auto* optimistic_txn_options =
      reinterpret_cast<rocksdb::OptimisticTransactionOptions*>(
          joptimistic_txn_options_handle);
  rocksdb::Transaction* txn = optimistic_txn_db->BeginTransaction(
      *write_options, *optimistic_txn_options);
  return reinterpret_cast<jlong>(txn);
}

/*
 * Class:     org_rocksdb_OptimisticTransactionDB
 * Method:    beginTransaction_withOld
 * Signature: (JJJ)J
 */
jlong Java_org_rocksdb_OptimisticTransactionDB_beginTransaction_1withOld__JJJ(
    JNIEnv*, jobject, jlong jhandle, jlong jwrite_options_handle,
    jlong jold_txn_handle) {
  auto* optimistic_txn_db =
      reinterpret_cast<rocksdb::OptimisticTransactionDB*>(jhandle);
  auto* write_options =
      reinterpret_cast<rocksdb::WriteOptions*>(jwrite_options_handle);
  auto* old_txn = reinterpret_cast<rocksdb::Transaction*>(jold_txn_handle);
  rocksdb::OptimisticTransactionOptions optimistic_txn_options;
  rocksdb::Transaction* txn = optimistic_txn_db->BeginTransaction(
      *write_options, optimistic_txn_options, old_txn);

  // RocksJava relies on the assumption that
  // we do not allocate a new Transaction object
  // when providing an old_optimistic_txn
  assert(txn == old_txn);

  return reinterpret_cast<jlong>(txn);
}

/*
 * Class:     org_rocksdb_OptimisticTransactionDB
 * Method:    beginTransaction_withOld
 * Signature: (JJJJ)J
 */
jlong Java_org_rocksdb_OptimisticTransactionDB_beginTransaction_1withOld__JJJJ(
    JNIEnv*, jobject, jlong jhandle, jlong jwrite_options_handle,
    jlong joptimistic_txn_options_handle, jlong jold_txn_handle) {
  auto* optimistic_txn_db =
      reinterpret_cast<rocksdb::OptimisticTransactionDB*>(jhandle);
  auto* write_options =
      reinterpret_cast<rocksdb::WriteOptions*>(jwrite_options_handle);
  auto* optimistic_txn_options =
      reinterpret_cast<rocksdb::OptimisticTransactionOptions*>(
          joptimistic_txn_options_handle);
  auto* old_txn = reinterpret_cast<rocksdb::Transaction*>(jold_txn_handle);
  rocksdb::Transaction* txn = optimistic_txn_db->BeginTransaction(
      *write_options, *optimistic_txn_options, old_txn);

  // RocksJava relies on the assumption that
  // we do not allocate a new Transaction object
  // when providing an old_optimisic_txn
  assert(txn == old_txn);

  return reinterpret_cast<jlong>(txn);
}

/*
 * Class:     org_rocksdb_OptimisticTransactionDB
 * Method:    getBaseDB
 * Signature: (J)J
 */
jlong Java_org_rocksdb_OptimisticTransactionDB_getBaseDB(
    JNIEnv*, jobject, jlong jhandle) {
  auto* optimistic_txn_db =
      reinterpret_cast<rocksdb::OptimisticTransactionDB*>(jhandle);
  return reinterpret_cast<jlong>(optimistic_txn_db->GetBaseDB());
}
