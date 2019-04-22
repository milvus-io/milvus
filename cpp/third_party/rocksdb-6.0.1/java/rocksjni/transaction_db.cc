// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++
// for rocksdb::TransactionDB.

#include <jni.h>
#include <functional>
#include <memory>
#include <utility>

#include "include/org_rocksdb_TransactionDB.h"

#include "rocksdb/options.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"

#include "rocksjni/portal.h"

/*
 * Class:     org_rocksdb_TransactionDB
 * Method:    open
 * Signature: (JJLjava/lang/String;)J
 */
jlong Java_org_rocksdb_TransactionDB_open__JJLjava_lang_String_2(
    JNIEnv* env, jclass, jlong joptions_handle,
    jlong jtxn_db_options_handle, jstring jdb_path) {
  auto* options = reinterpret_cast<rocksdb::Options*>(joptions_handle);
  auto* txn_db_options =
      reinterpret_cast<rocksdb::TransactionDBOptions*>(jtxn_db_options_handle);
  rocksdb::TransactionDB* tdb = nullptr;
  const char* db_path = env->GetStringUTFChars(jdb_path, nullptr);
  if (db_path == nullptr) {
    // exception thrown: OutOfMemoryError
    return 0;
  }
  rocksdb::Status s =
      rocksdb::TransactionDB::Open(*options, *txn_db_options, db_path, &tdb);
  env->ReleaseStringUTFChars(jdb_path, db_path);

  if (s.ok()) {
    return reinterpret_cast<jlong>(tdb);
  } else {
    rocksdb::RocksDBExceptionJni::ThrowNew(env, s);
    return 0;
  }
}

/*
 * Class:     org_rocksdb_TransactionDB
 * Method:    open
 * Signature: (JJLjava/lang/String;[[B[J)[J
 */
jlongArray Java_org_rocksdb_TransactionDB_open__JJLjava_lang_String_2_3_3B_3J(
    JNIEnv* env, jclass, jlong jdb_options_handle,
    jlong jtxn_db_options_handle, jstring jdb_path, jobjectArray jcolumn_names,
    jlongArray jcolumn_options_handles) {
  const char* db_path = env->GetStringUTFChars(jdb_path, nullptr);
  if (db_path == nullptr) {
    // exception thrown: OutOfMemoryError
    return nullptr;
  }

  const jsize len_cols = env->GetArrayLength(jcolumn_names);
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
  std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
  for (int i = 0; i < len_cols; i++) {
    const jobject jcn = env->GetObjectArrayElement(jcolumn_names, i);
    if (env->ExceptionCheck()) {
      // exception thrown: ArrayIndexOutOfBoundsException
      env->ReleaseLongArrayElements(jcolumn_options_handles, jco, JNI_ABORT);
      env->ReleaseStringUTFChars(jdb_path, db_path);
      return nullptr;
    }
    const jbyteArray jcn_ba = reinterpret_cast<jbyteArray>(jcn);
    jbyte* jcf_name = env->GetByteArrayElements(jcn_ba, nullptr);
    if (jcf_name == nullptr) {
      // exception thrown: OutOfMemoryError
      env->DeleteLocalRef(jcn);
      env->ReleaseLongArrayElements(jcolumn_options_handles, jco, JNI_ABORT);
      env->ReleaseStringUTFChars(jdb_path, db_path);
      return nullptr;
    }

    const int jcf_name_len = env->GetArrayLength(jcn_ba);
    if (env->EnsureLocalCapacity(jcf_name_len) != 0) {
      // out of memory
      env->ReleaseByteArrayElements(jcn_ba, jcf_name, JNI_ABORT);
      env->DeleteLocalRef(jcn);
      env->ReleaseLongArrayElements(jcolumn_options_handles, jco, JNI_ABORT);
      env->ReleaseStringUTFChars(jdb_path, db_path);
      return nullptr;
    }
    const std::string cf_name(reinterpret_cast<char*>(jcf_name), jcf_name_len);
    const rocksdb::ColumnFamilyOptions* cf_options =
        reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jco[i]);
    column_families.push_back(
        rocksdb::ColumnFamilyDescriptor(cf_name, *cf_options));

    env->ReleaseByteArrayElements(jcn_ba, jcf_name, JNI_ABORT);
    env->DeleteLocalRef(jcn);
  }
  env->ReleaseLongArrayElements(jcolumn_options_handles, jco, JNI_ABORT);

  auto* db_options = reinterpret_cast<rocksdb::DBOptions*>(jdb_options_handle);
  auto* txn_db_options =
      reinterpret_cast<rocksdb::TransactionDBOptions*>(jtxn_db_options_handle);
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  rocksdb::TransactionDB* tdb = nullptr;
  const rocksdb::Status s = rocksdb::TransactionDB::Open(
      *db_options, *txn_db_options, db_path, column_families, &handles, &tdb);

  // check if open operation was successful
  if (s.ok()) {
    const jsize resultsLen = 1 + len_cols;  // db handle + column family handles
    std::unique_ptr<jlong[]> results =
        std::unique_ptr<jlong[]>(new jlong[resultsLen]);
    results[0] = reinterpret_cast<jlong>(tdb);
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
      env->DeleteLocalRef(jresults);
      return nullptr;
    }
    return jresults;
  } else {
    rocksdb::RocksDBExceptionJni::ThrowNew(env, s);
    return nullptr;
  }
}

/*
 * Class:     org_rocksdb_TransactionDB
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_TransactionDB_disposeInternal(
    JNIEnv*, jobject, jlong jhandle) {
  auto* txn_db = reinterpret_cast<rocksdb::TransactionDB*>(jhandle);
  assert(txn_db != nullptr);
  delete txn_db;
}

/*
 * Class:     org_rocksdb_TransactionDB
 * Method:    closeDatabase
 * Signature: (J)V
 */
void Java_org_rocksdb_TransactionDB_closeDatabase(
    JNIEnv* env, jclass, jlong jhandle) {
  auto* txn_db = reinterpret_cast<rocksdb::TransactionDB*>(jhandle);
  assert(txn_db != nullptr);
  rocksdb::Status s = txn_db->Close();
  rocksdb::RocksDBExceptionJni::ThrowNew(env, s);
}

/*
 * Class:     org_rocksdb_TransactionDB
 * Method:    beginTransaction
 * Signature: (JJ)J
 */
jlong Java_org_rocksdb_TransactionDB_beginTransaction__JJ(
    JNIEnv*, jobject, jlong jhandle, jlong jwrite_options_handle) {
  auto* txn_db = reinterpret_cast<rocksdb::TransactionDB*>(jhandle);
  auto* write_options =
      reinterpret_cast<rocksdb::WriteOptions*>(jwrite_options_handle);
  rocksdb::Transaction* txn = txn_db->BeginTransaction(*write_options);
  return reinterpret_cast<jlong>(txn);
}

/*
 * Class:     org_rocksdb_TransactionDB
 * Method:    beginTransaction
 * Signature: (JJJ)J
 */
jlong Java_org_rocksdb_TransactionDB_beginTransaction__JJJ(
    JNIEnv*, jobject, jlong jhandle, jlong jwrite_options_handle,
    jlong jtxn_options_handle) {
  auto* txn_db = reinterpret_cast<rocksdb::TransactionDB*>(jhandle);
  auto* write_options =
      reinterpret_cast<rocksdb::WriteOptions*>(jwrite_options_handle);
  auto* txn_options =
      reinterpret_cast<rocksdb::TransactionOptions*>(jtxn_options_handle);
  rocksdb::Transaction* txn =
      txn_db->BeginTransaction(*write_options, *txn_options);
  return reinterpret_cast<jlong>(txn);
}

/*
 * Class:     org_rocksdb_TransactionDB
 * Method:    beginTransaction_withOld
 * Signature: (JJJ)J
 */
jlong Java_org_rocksdb_TransactionDB_beginTransaction_1withOld__JJJ(
    JNIEnv*, jobject, jlong jhandle, jlong jwrite_options_handle,
    jlong jold_txn_handle) {
  auto* txn_db = reinterpret_cast<rocksdb::TransactionDB*>(jhandle);
  auto* write_options =
      reinterpret_cast<rocksdb::WriteOptions*>(jwrite_options_handle);
  auto* old_txn = reinterpret_cast<rocksdb::Transaction*>(jold_txn_handle);
  rocksdb::TransactionOptions txn_options;
  rocksdb::Transaction* txn =
      txn_db->BeginTransaction(*write_options, txn_options, old_txn);

  // RocksJava relies on the assumption that
  // we do not allocate a new Transaction object
  // when providing an old_txn
  assert(txn == old_txn);

  return reinterpret_cast<jlong>(txn);
}

/*
 * Class:     org_rocksdb_TransactionDB
 * Method:    beginTransaction_withOld
 * Signature: (JJJJ)J
 */
jlong Java_org_rocksdb_TransactionDB_beginTransaction_1withOld__JJJJ(
    JNIEnv*, jobject, jlong jhandle, jlong jwrite_options_handle,
    jlong jtxn_options_handle, jlong jold_txn_handle) {
  auto* txn_db = reinterpret_cast<rocksdb::TransactionDB*>(jhandle);
  auto* write_options =
      reinterpret_cast<rocksdb::WriteOptions*>(jwrite_options_handle);
  auto* txn_options =
      reinterpret_cast<rocksdb::TransactionOptions*>(jtxn_options_handle);
  auto* old_txn = reinterpret_cast<rocksdb::Transaction*>(jold_txn_handle);
  rocksdb::Transaction* txn =
      txn_db->BeginTransaction(*write_options, *txn_options, old_txn);

  // RocksJava relies on the assumption that
  // we do not allocate a new Transaction object
  // when providing an old_txn
  assert(txn == old_txn);

  return reinterpret_cast<jlong>(txn);
}

/*
 * Class:     org_rocksdb_TransactionDB
 * Method:    getTransactionByName
 * Signature: (JLjava/lang/String;)J
 */
jlong Java_org_rocksdb_TransactionDB_getTransactionByName(
    JNIEnv* env, jobject, jlong jhandle, jstring jname) {
  auto* txn_db = reinterpret_cast<rocksdb::TransactionDB*>(jhandle);
  const char* name = env->GetStringUTFChars(jname, nullptr);
  if (name == nullptr) {
    // exception thrown: OutOfMemoryError
    return 0;
  }
  rocksdb::Transaction* txn = txn_db->GetTransactionByName(name);
  env->ReleaseStringUTFChars(jname, name);
  return reinterpret_cast<jlong>(txn);
}

/*
 * Class:     org_rocksdb_TransactionDB
 * Method:    getAllPreparedTransactions
 * Signature: (J)[J
 */
jlongArray Java_org_rocksdb_TransactionDB_getAllPreparedTransactions(
    JNIEnv* env, jobject, jlong jhandle) {
  auto* txn_db = reinterpret_cast<rocksdb::TransactionDB*>(jhandle);
  std::vector<rocksdb::Transaction*> txns;
  txn_db->GetAllPreparedTransactions(&txns);

  const size_t size = txns.size();
  assert(size < UINT32_MAX);  // does it fit in a jint?

  const jsize len = static_cast<jsize>(size);
  std::vector<jlong> tmp(len);
  for (jsize i = 0; i < len; ++i) {
    tmp[i] = reinterpret_cast<jlong>(txns[i]);
  }

  jlongArray jtxns = env->NewLongArray(len);
  if (jtxns == nullptr) {
    // exception thrown: OutOfMemoryError
    return nullptr;
  }
  env->SetLongArrayRegion(jtxns, 0, len, tmp.data());
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    env->DeleteLocalRef(jtxns);
    return nullptr;
  }

  return jtxns;
}

/*
 * Class:     org_rocksdb_TransactionDB
 * Method:    getLockStatusData
 * Signature: (J)Ljava/util/Map;
 */
jobject Java_org_rocksdb_TransactionDB_getLockStatusData(
    JNIEnv* env, jobject, jlong jhandle) {
  auto* txn_db = reinterpret_cast<rocksdb::TransactionDB*>(jhandle);
  const std::unordered_multimap<uint32_t, rocksdb::KeyLockInfo>
      lock_status_data = txn_db->GetLockStatusData();
  const jobject jlock_status_data = rocksdb::HashMapJni::construct(
      env, static_cast<uint32_t>(lock_status_data.size()));
  if (jlock_status_data == nullptr) {
    // exception occurred
    return nullptr;
  }

  const rocksdb::HashMapJni::FnMapKV<const int32_t, const rocksdb::KeyLockInfo, jobject, jobject>
      fn_map_kv =
          [env](
              const std::pair<const int32_t, const rocksdb::KeyLockInfo>&
                  pair) {
            const jobject jlong_column_family_id =
                rocksdb::LongJni::valueOf(env, pair.first);
            if (jlong_column_family_id == nullptr) {
              // an error occurred
              return std::unique_ptr<std::pair<jobject, jobject>>(nullptr);
            }
            const jobject jkey_lock_info =
                rocksdb::KeyLockInfoJni::construct(env, pair.second);
            if (jkey_lock_info == nullptr) {
              // an error occurred
              return std::unique_ptr<std::pair<jobject, jobject>>(nullptr);
            }
            return std::unique_ptr<std::pair<jobject, jobject>>(
                new std::pair<jobject, jobject>(jlong_column_family_id,
                                                jkey_lock_info));
          };

  if (!rocksdb::HashMapJni::putAll(env, jlock_status_data,
                                   lock_status_data.begin(),
                                   lock_status_data.end(), fn_map_kv)) {
    // exception occcurred
    return nullptr;
  }

  return jlock_status_data;
}

/*
 * Class:     org_rocksdb_TransactionDB
 * Method:    getDeadlockInfoBuffer
 * Signature: (J)[Lorg/rocksdb/TransactionDB/DeadlockPath;
 */
jobjectArray Java_org_rocksdb_TransactionDB_getDeadlockInfoBuffer(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  auto* txn_db = reinterpret_cast<rocksdb::TransactionDB*>(jhandle);
  const std::vector<rocksdb::DeadlockPath> deadlock_info_buffer =
      txn_db->GetDeadlockInfoBuffer();

  const jsize deadlock_info_buffer_len =
      static_cast<jsize>(deadlock_info_buffer.size());
  jobjectArray jdeadlock_info_buffer =
      env->NewObjectArray(deadlock_info_buffer_len,
                          rocksdb::DeadlockPathJni::getJClass(env), nullptr);
  if (jdeadlock_info_buffer == nullptr) {
    // exception thrown: OutOfMemoryError
    return nullptr;
  }
  jsize jdeadlock_info_buffer_offset = 0;

  auto buf_end = deadlock_info_buffer.end();
  for (auto buf_it = deadlock_info_buffer.begin(); buf_it != buf_end;
       ++buf_it) {
    const rocksdb::DeadlockPath deadlock_path = *buf_it;
    const std::vector<rocksdb::DeadlockInfo> deadlock_infos =
        deadlock_path.path;
    const jsize deadlock_infos_len =
        static_cast<jsize>(deadlock_info_buffer.size());
    jobjectArray jdeadlock_infos = env->NewObjectArray(
        deadlock_infos_len, rocksdb::DeadlockInfoJni::getJClass(env), nullptr);
    if (jdeadlock_infos == nullptr) {
      // exception thrown: OutOfMemoryError
      env->DeleteLocalRef(jdeadlock_info_buffer);
      return nullptr;
    }
    jsize jdeadlock_infos_offset = 0;

    auto infos_end = deadlock_infos.end();
    for (auto infos_it = deadlock_infos.begin(); infos_it != infos_end;
         ++infos_it) {
      const rocksdb::DeadlockInfo deadlock_info = *infos_it;
      const jobject jdeadlock_info = rocksdb::TransactionDBJni::newDeadlockInfo(
          env, jobj, deadlock_info.m_txn_id, deadlock_info.m_cf_id,
          deadlock_info.m_waiting_key, deadlock_info.m_exclusive);
      if (jdeadlock_info == nullptr) {
        // exception occcurred
        env->DeleteLocalRef(jdeadlock_info_buffer);
        return nullptr;
      }
      env->SetObjectArrayElement(jdeadlock_infos, jdeadlock_infos_offset++,
                                 jdeadlock_info);
      if (env->ExceptionCheck()) {
        // exception thrown: ArrayIndexOutOfBoundsException or
        // ArrayStoreException
        env->DeleteLocalRef(jdeadlock_info);
        env->DeleteLocalRef(jdeadlock_info_buffer);
        return nullptr;
      }
    }

    const jobject jdeadlock_path = rocksdb::DeadlockPathJni::construct(
        env, jdeadlock_infos, deadlock_path.limit_exceeded);
    if (jdeadlock_path == nullptr) {
      // exception occcurred
      env->DeleteLocalRef(jdeadlock_info_buffer);
      return nullptr;
    }
    env->SetObjectArrayElement(jdeadlock_info_buffer,
                               jdeadlock_info_buffer_offset++, jdeadlock_path);
    if (env->ExceptionCheck()) {
      // exception thrown: ArrayIndexOutOfBoundsException or ArrayStoreException
      env->DeleteLocalRef(jdeadlock_path);
      env->DeleteLocalRef(jdeadlock_info_buffer);
      return nullptr;
    }
  }

  return jdeadlock_info_buffer;
}

/*
 * Class:     org_rocksdb_TransactionDB
 * Method:    setDeadlockInfoBufferSize
 * Signature: (JI)V
 */
void Java_org_rocksdb_TransactionDB_setDeadlockInfoBufferSize(
    JNIEnv*, jobject, jlong jhandle, jint jdeadlock_info_buffer_size) {
  auto* txn_db = reinterpret_cast<rocksdb::TransactionDB*>(jhandle);
  txn_db->SetDeadlockInfoBufferSize(jdeadlock_info_buffer_size);
}
