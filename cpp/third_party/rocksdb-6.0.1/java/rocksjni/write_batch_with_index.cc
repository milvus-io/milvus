// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ and enables
// calling c++ rocksdb::WriteBatchWithIndex methods from Java side.

#include "rocksdb/utilities/write_batch_with_index.h"
#include "include/org_rocksdb_WBWIRocksIterator.h"
#include "include/org_rocksdb_WriteBatchWithIndex.h"
#include "rocksdb/comparator.h"
#include "rocksjni/portal.h"

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    newWriteBatchWithIndex
 * Signature: ()J
 */
jlong Java_org_rocksdb_WriteBatchWithIndex_newWriteBatchWithIndex__(
    JNIEnv* /*env*/, jclass /*jcls*/) {
  auto* wbwi = new rocksdb::WriteBatchWithIndex();
  return reinterpret_cast<jlong>(wbwi);
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    newWriteBatchWithIndex
 * Signature: (Z)J
 */
jlong Java_org_rocksdb_WriteBatchWithIndex_newWriteBatchWithIndex__Z(
    JNIEnv* /*env*/, jclass /*jcls*/, jboolean joverwrite_key) {
  auto* wbwi = new rocksdb::WriteBatchWithIndex(
      rocksdb::BytewiseComparator(), 0, static_cast<bool>(joverwrite_key));
  return reinterpret_cast<jlong>(wbwi);
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    newWriteBatchWithIndex
 * Signature: (JBIZ)J
 */
jlong Java_org_rocksdb_WriteBatchWithIndex_newWriteBatchWithIndex__JBIZ(
    JNIEnv* /*env*/, jclass /*jcls*/, jlong jfallback_index_comparator_handle,
    jbyte jcomparator_type, jint jreserved_bytes, jboolean joverwrite_key) {
  rocksdb::Comparator* fallback_comparator = nullptr;
  switch (jcomparator_type) {
    // JAVA_COMPARATOR
    case 0x0:
      fallback_comparator = reinterpret_cast<rocksdb::ComparatorJniCallback*>(
          jfallback_index_comparator_handle);
      break;

    // JAVA_DIRECT_COMPARATOR
    case 0x1:
      fallback_comparator =
          reinterpret_cast<rocksdb::DirectComparatorJniCallback*>(
              jfallback_index_comparator_handle);
      break;

    // JAVA_NATIVE_COMPARATOR_WRAPPER
    case 0x2:
      fallback_comparator = reinterpret_cast<rocksdb::Comparator*>(
          jfallback_index_comparator_handle);
      break;
  }
  auto* wbwi = new rocksdb::WriteBatchWithIndex(
      fallback_comparator, static_cast<size_t>(jreserved_bytes),
      static_cast<bool>(joverwrite_key));
  return reinterpret_cast<jlong>(wbwi);
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    count0
 * Signature: (J)I
 */
jint Java_org_rocksdb_WriteBatchWithIndex_count0(JNIEnv* /*env*/,
                                                 jobject /*jobj*/,
                                                 jlong jwbwi_handle) {
  auto* wbwi = reinterpret_cast<rocksdb::WriteBatchWithIndex*>(jwbwi_handle);
  assert(wbwi != nullptr);

  return static_cast<jint>(wbwi->GetWriteBatch()->Count());
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    put
 * Signature: (J[BI[BI)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_put__J_3BI_3BI(
    JNIEnv* env, jobject jobj, jlong jwbwi_handle, jbyteArray jkey,
    jint jkey_len, jbyteArray jentry_value, jint jentry_value_len) {
  auto* wbwi = reinterpret_cast<rocksdb::WriteBatchWithIndex*>(jwbwi_handle);
  assert(wbwi != nullptr);
  auto put = [&wbwi](rocksdb::Slice key, rocksdb::Slice value) {
    return wbwi->Put(key, value);
  };
  std::unique_ptr<rocksdb::Status> status = rocksdb::JniUtil::kv_op(
      put, env, jobj, jkey, jkey_len, jentry_value, jentry_value_len);
  if (status != nullptr && !status->ok()) {
    rocksdb::RocksDBExceptionJni::ThrowNew(env, status);
  }
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    put
 * Signature: (J[BI[BIJ)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_put__J_3BI_3BIJ(
    JNIEnv* env, jobject jobj, jlong jwbwi_handle, jbyteArray jkey,
    jint jkey_len, jbyteArray jentry_value, jint jentry_value_len,
    jlong jcf_handle) {
  auto* wbwi = reinterpret_cast<rocksdb::WriteBatchWithIndex*>(jwbwi_handle);
  assert(wbwi != nullptr);
  auto* cf_handle = reinterpret_cast<rocksdb::ColumnFamilyHandle*>(jcf_handle);
  assert(cf_handle != nullptr);
  auto put = [&wbwi, &cf_handle](rocksdb::Slice key, rocksdb::Slice value) {
    return wbwi->Put(cf_handle, key, value);
  };
  std::unique_ptr<rocksdb::Status> status = rocksdb::JniUtil::kv_op(
      put, env, jobj, jkey, jkey_len, jentry_value, jentry_value_len);
  if (status != nullptr && !status->ok()) {
    rocksdb::RocksDBExceptionJni::ThrowNew(env, status);
  }
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    merge
 * Signature: (J[BI[BI)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_merge__J_3BI_3BI(
    JNIEnv* env, jobject jobj, jlong jwbwi_handle, jbyteArray jkey,
    jint jkey_len, jbyteArray jentry_value, jint jentry_value_len) {
  auto* wbwi = reinterpret_cast<rocksdb::WriteBatchWithIndex*>(jwbwi_handle);
  assert(wbwi != nullptr);
  auto merge = [&wbwi](rocksdb::Slice key, rocksdb::Slice value) {
    return wbwi->Merge(key, value);
  };
  std::unique_ptr<rocksdb::Status> status = rocksdb::JniUtil::kv_op(
      merge, env, jobj, jkey, jkey_len, jentry_value, jentry_value_len);
  if (status != nullptr && !status->ok()) {
    rocksdb::RocksDBExceptionJni::ThrowNew(env, status);
  }
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    merge
 * Signature: (J[BI[BIJ)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_merge__J_3BI_3BIJ(
    JNIEnv* env, jobject jobj, jlong jwbwi_handle, jbyteArray jkey,
    jint jkey_len, jbyteArray jentry_value, jint jentry_value_len,
    jlong jcf_handle) {
  auto* wbwi = reinterpret_cast<rocksdb::WriteBatchWithIndex*>(jwbwi_handle);
  assert(wbwi != nullptr);
  auto* cf_handle = reinterpret_cast<rocksdb::ColumnFamilyHandle*>(jcf_handle);
  assert(cf_handle != nullptr);
  auto merge = [&wbwi, &cf_handle](rocksdb::Slice key, rocksdb::Slice value) {
    return wbwi->Merge(cf_handle, key, value);
  };
  std::unique_ptr<rocksdb::Status> status = rocksdb::JniUtil::kv_op(
      merge, env, jobj, jkey, jkey_len, jentry_value, jentry_value_len);
  if (status != nullptr && !status->ok()) {
    rocksdb::RocksDBExceptionJni::ThrowNew(env, status);
  }
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    delete
 * Signature: (J[BI)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_delete__J_3BI(JNIEnv* env,
                                                        jobject jobj,
                                                        jlong jwbwi_handle,
                                                        jbyteArray jkey,
                                                        jint jkey_len) {
  auto* wbwi = reinterpret_cast<rocksdb::WriteBatchWithIndex*>(jwbwi_handle);
  assert(wbwi != nullptr);
  auto remove = [&wbwi](rocksdb::Slice key) { return wbwi->Delete(key); };
  std::unique_ptr<rocksdb::Status> status =
      rocksdb::JniUtil::k_op(remove, env, jobj, jkey, jkey_len);
  if (status != nullptr && !status->ok()) {
    rocksdb::RocksDBExceptionJni::ThrowNew(env, status);
  }
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    delete
 * Signature: (J[BIJ)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_delete__J_3BIJ(
    JNIEnv* env, jobject jobj, jlong jwbwi_handle, jbyteArray jkey,
    jint jkey_len, jlong jcf_handle) {
  auto* wbwi = reinterpret_cast<rocksdb::WriteBatchWithIndex*>(jwbwi_handle);
  assert(wbwi != nullptr);
  auto* cf_handle = reinterpret_cast<rocksdb::ColumnFamilyHandle*>(jcf_handle);
  assert(cf_handle != nullptr);
  auto remove = [&wbwi, &cf_handle](rocksdb::Slice key) {
    return wbwi->Delete(cf_handle, key);
  };
  std::unique_ptr<rocksdb::Status> status =
      rocksdb::JniUtil::k_op(remove, env, jobj, jkey, jkey_len);
  if (status != nullptr && !status->ok()) {
    rocksdb::RocksDBExceptionJni::ThrowNew(env, status);
  }
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    singleDelete
 * Signature: (J[BI)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_singleDelete__J_3BI(
    JNIEnv* env, jobject jobj, jlong jwbwi_handle, jbyteArray jkey,
    jint jkey_len) {
  auto* wbwi = reinterpret_cast<rocksdb::WriteBatchWithIndex*>(jwbwi_handle);
  assert(wbwi != nullptr);
  auto single_delete = [&wbwi](rocksdb::Slice key) {
    return wbwi->SingleDelete(key);
  };
  std::unique_ptr<rocksdb::Status> status =
      rocksdb::JniUtil::k_op(single_delete, env, jobj, jkey, jkey_len);
  if (status != nullptr && !status->ok()) {
    rocksdb::RocksDBExceptionJni::ThrowNew(env, status);
  }
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    singleDelete
 * Signature: (J[BIJ)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_singleDelete__J_3BIJ(
    JNIEnv* env, jobject jobj, jlong jwbwi_handle, jbyteArray jkey,
    jint jkey_len, jlong jcf_handle) {
  auto* wbwi = reinterpret_cast<rocksdb::WriteBatchWithIndex*>(jwbwi_handle);
  assert(wbwi != nullptr);
  auto* cf_handle = reinterpret_cast<rocksdb::ColumnFamilyHandle*>(jcf_handle);
  assert(cf_handle != nullptr);
  auto single_delete = [&wbwi, &cf_handle](rocksdb::Slice key) {
    return wbwi->SingleDelete(cf_handle, key);
  };
  std::unique_ptr<rocksdb::Status> status =
      rocksdb::JniUtil::k_op(single_delete, env, jobj, jkey, jkey_len);
  if (status != nullptr && !status->ok()) {
    rocksdb::RocksDBExceptionJni::ThrowNew(env, status);
  }
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    deleteRange
 * Signature: (J[BI[BI)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_deleteRange__J_3BI_3BI(
    JNIEnv* env, jobject jobj, jlong jwbwi_handle, jbyteArray jbegin_key,
    jint jbegin_key_len, jbyteArray jend_key, jint jend_key_len) {
  auto* wbwi = reinterpret_cast<rocksdb::WriteBatchWithIndex*>(jwbwi_handle);
  assert(wbwi != nullptr);
  auto deleteRange = [&wbwi](rocksdb::Slice beginKey, rocksdb::Slice endKey) {
    return wbwi->DeleteRange(beginKey, endKey);
  };
  std::unique_ptr<rocksdb::Status> status =
      rocksdb::JniUtil::kv_op(deleteRange, env, jobj, jbegin_key,
                              jbegin_key_len, jend_key, jend_key_len);
  if (status != nullptr && !status->ok()) {
    rocksdb::RocksDBExceptionJni::ThrowNew(env, status);
  }
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    deleteRange
 * Signature: (J[BI[BIJ)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_deleteRange__J_3BI_3BIJ(
    JNIEnv* env, jobject jobj, jlong jwbwi_handle, jbyteArray jbegin_key,
    jint jbegin_key_len, jbyteArray jend_key, jint jend_key_len,
    jlong jcf_handle) {
  auto* wbwi = reinterpret_cast<rocksdb::WriteBatchWithIndex*>(jwbwi_handle);
  assert(wbwi != nullptr);
  auto* cf_handle = reinterpret_cast<rocksdb::ColumnFamilyHandle*>(jcf_handle);
  assert(cf_handle != nullptr);
  auto deleteRange = [&wbwi, &cf_handle](rocksdb::Slice beginKey,
                                         rocksdb::Slice endKey) {
    return wbwi->DeleteRange(cf_handle, beginKey, endKey);
  };
  std::unique_ptr<rocksdb::Status> status =
      rocksdb::JniUtil::kv_op(deleteRange, env, jobj, jbegin_key,
                              jbegin_key_len, jend_key, jend_key_len);
  if (status != nullptr && !status->ok()) {
    rocksdb::RocksDBExceptionJni::ThrowNew(env, status);
  }
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    putLogData
 * Signature: (J[BI)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_putLogData(JNIEnv* env, jobject jobj,
                                                     jlong jwbwi_handle,
                                                     jbyteArray jblob,
                                                     jint jblob_len) {
  auto* wbwi = reinterpret_cast<rocksdb::WriteBatchWithIndex*>(jwbwi_handle);
  assert(wbwi != nullptr);
  auto putLogData = [&wbwi](rocksdb::Slice blob) {
    return wbwi->PutLogData(blob);
  };
  std::unique_ptr<rocksdb::Status> status =
      rocksdb::JniUtil::k_op(putLogData, env, jobj, jblob, jblob_len);
  if (status != nullptr && !status->ok()) {
    rocksdb::RocksDBExceptionJni::ThrowNew(env, status);
  }
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    clear
 * Signature: (J)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_clear0(JNIEnv* /*env*/,
                                                 jobject /*jobj*/,
                                                 jlong jwbwi_handle) {
  auto* wbwi = reinterpret_cast<rocksdb::WriteBatchWithIndex*>(jwbwi_handle);
  assert(wbwi != nullptr);

  wbwi->Clear();
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    setSavePoint0
 * Signature: (J)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_setSavePoint0(JNIEnv* /*env*/,
                                                        jobject /*jobj*/,
                                                        jlong jwbwi_handle) {
  auto* wbwi = reinterpret_cast<rocksdb::WriteBatchWithIndex*>(jwbwi_handle);
  assert(wbwi != nullptr);

  wbwi->SetSavePoint();
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    rollbackToSavePoint0
 * Signature: (J)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_rollbackToSavePoint0(
    JNIEnv* env, jobject /*jobj*/, jlong jwbwi_handle) {
  auto* wbwi = reinterpret_cast<rocksdb::WriteBatchWithIndex*>(jwbwi_handle);
  assert(wbwi != nullptr);

  auto s = wbwi->RollbackToSavePoint();

  if (s.ok()) {
    return;
  }

  rocksdb::RocksDBExceptionJni::ThrowNew(env, s);
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    popSavePoint
 * Signature: (J)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_popSavePoint(JNIEnv* env,
                                                       jobject /*jobj*/,
                                                       jlong jwbwi_handle) {
  auto* wbwi = reinterpret_cast<rocksdb::WriteBatchWithIndex*>(jwbwi_handle);
  assert(wbwi != nullptr);

  auto s = wbwi->PopSavePoint();

  if (s.ok()) {
    return;
  }

  rocksdb::RocksDBExceptionJni::ThrowNew(env, s);
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    setMaxBytes
 * Signature: (JJ)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_setMaxBytes(JNIEnv* /*env*/,
                                                      jobject /*jobj*/,
                                                      jlong jwbwi_handle,
                                                      jlong jmax_bytes) {
  auto* wbwi = reinterpret_cast<rocksdb::WriteBatchWithIndex*>(jwbwi_handle);
  assert(wbwi != nullptr);

  wbwi->SetMaxBytes(static_cast<size_t>(jmax_bytes));
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    getWriteBatch
 * Signature: (J)Lorg/rocksdb/WriteBatch;
 */
jobject Java_org_rocksdb_WriteBatchWithIndex_getWriteBatch(JNIEnv* env,
                                                           jobject /*jobj*/,
                                                           jlong jwbwi_handle) {
  auto* wbwi = reinterpret_cast<rocksdb::WriteBatchWithIndex*>(jwbwi_handle);
  assert(wbwi != nullptr);

  auto* wb = wbwi->GetWriteBatch();

  // TODO(AR) is the `wb` object owned by us?
  return rocksdb::WriteBatchJni::construct(env, wb);
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    iterator0
 * Signature: (J)J
 */
jlong Java_org_rocksdb_WriteBatchWithIndex_iterator0(JNIEnv* /*env*/,
                                                     jobject /*jobj*/,
                                                     jlong jwbwi_handle) {
  auto* wbwi = reinterpret_cast<rocksdb::WriteBatchWithIndex*>(jwbwi_handle);
  auto* wbwi_iterator = wbwi->NewIterator();
  return reinterpret_cast<jlong>(wbwi_iterator);
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    iterator1
 * Signature: (JJ)J
 */
jlong Java_org_rocksdb_WriteBatchWithIndex_iterator1(JNIEnv* /*env*/,
                                                     jobject /*jobj*/,
                                                     jlong jwbwi_handle,
                                                     jlong jcf_handle) {
  auto* wbwi = reinterpret_cast<rocksdb::WriteBatchWithIndex*>(jwbwi_handle);
  auto* cf_handle = reinterpret_cast<rocksdb::ColumnFamilyHandle*>(jcf_handle);
  auto* wbwi_iterator = wbwi->NewIterator(cf_handle);
  return reinterpret_cast<jlong>(wbwi_iterator);
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    iteratorWithBase
 * Signature: (JJJ)J
 */
jlong Java_org_rocksdb_WriteBatchWithIndex_iteratorWithBase(JNIEnv* /*env*/,
                                                            jobject /*jobj*/,
                                                            jlong jwbwi_handle,
                                                            jlong jcf_handle,
                                                            jlong jbi_handle) {
  auto* wbwi = reinterpret_cast<rocksdb::WriteBatchWithIndex*>(jwbwi_handle);
  auto* cf_handle = reinterpret_cast<rocksdb::ColumnFamilyHandle*>(jcf_handle);
  auto* base_iterator = reinterpret_cast<rocksdb::Iterator*>(jbi_handle);
  auto* iterator = wbwi->NewIteratorWithBase(cf_handle, base_iterator);
  return reinterpret_cast<jlong>(iterator);
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    getFromBatch
 * Signature: (JJ[BI)[B
 */
jbyteArray JNICALL Java_org_rocksdb_WriteBatchWithIndex_getFromBatch__JJ_3BI(
    JNIEnv* env, jobject /*jobj*/, jlong jwbwi_handle, jlong jdbopt_handle,
    jbyteArray jkey, jint jkey_len) {
  auto* wbwi = reinterpret_cast<rocksdb::WriteBatchWithIndex*>(jwbwi_handle);
  auto* dbopt = reinterpret_cast<rocksdb::DBOptions*>(jdbopt_handle);

  auto getter = [&wbwi, &dbopt](const rocksdb::Slice& key, std::string* value) {
    return wbwi->GetFromBatch(*dbopt, key, value);
  };

  return rocksdb::JniUtil::v_op(getter, env, jkey, jkey_len);
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    getFromBatch
 * Signature: (JJ[BIJ)[B
 */
jbyteArray Java_org_rocksdb_WriteBatchWithIndex_getFromBatch__JJ_3BIJ(
    JNIEnv* env, jobject /*jobj*/, jlong jwbwi_handle, jlong jdbopt_handle,
    jbyteArray jkey, jint jkey_len, jlong jcf_handle) {
  auto* wbwi = reinterpret_cast<rocksdb::WriteBatchWithIndex*>(jwbwi_handle);
  auto* dbopt = reinterpret_cast<rocksdb::DBOptions*>(jdbopt_handle);
  auto* cf_handle = reinterpret_cast<rocksdb::ColumnFamilyHandle*>(jcf_handle);

  auto getter = [&wbwi, &cf_handle, &dbopt](const rocksdb::Slice& key,
                                            std::string* value) {
    return wbwi->GetFromBatch(cf_handle, *dbopt, key, value);
  };

  return rocksdb::JniUtil::v_op(getter, env, jkey, jkey_len);
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    getFromBatchAndDB
 * Signature: (JJJ[BI)[B
 */
jbyteArray Java_org_rocksdb_WriteBatchWithIndex_getFromBatchAndDB__JJJ_3BI(
    JNIEnv* env, jobject /*jobj*/, jlong jwbwi_handle, jlong jdb_handle,
    jlong jreadopt_handle, jbyteArray jkey, jint jkey_len) {
  auto* wbwi = reinterpret_cast<rocksdb::WriteBatchWithIndex*>(jwbwi_handle);
  auto* db = reinterpret_cast<rocksdb::DB*>(jdb_handle);
  auto* readopt = reinterpret_cast<rocksdb::ReadOptions*>(jreadopt_handle);

  auto getter = [&wbwi, &db, &readopt](const rocksdb::Slice& key,
                                       std::string* value) {
    return wbwi->GetFromBatchAndDB(db, *readopt, key, value);
  };

  return rocksdb::JniUtil::v_op(getter, env, jkey, jkey_len);
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    getFromBatchAndDB
 * Signature: (JJJ[BIJ)[B
 */
jbyteArray Java_org_rocksdb_WriteBatchWithIndex_getFromBatchAndDB__JJJ_3BIJ(
    JNIEnv* env, jobject /*jobj*/, jlong jwbwi_handle, jlong jdb_handle,
    jlong jreadopt_handle, jbyteArray jkey, jint jkey_len, jlong jcf_handle) {
  auto* wbwi = reinterpret_cast<rocksdb::WriteBatchWithIndex*>(jwbwi_handle);
  auto* db = reinterpret_cast<rocksdb::DB*>(jdb_handle);
  auto* readopt = reinterpret_cast<rocksdb::ReadOptions*>(jreadopt_handle);
  auto* cf_handle = reinterpret_cast<rocksdb::ColumnFamilyHandle*>(jcf_handle);

  auto getter = [&wbwi, &db, &cf_handle, &readopt](const rocksdb::Slice& key,
                                                   std::string* value) {
    return wbwi->GetFromBatchAndDB(db, *readopt, cf_handle, key, value);
  };

  return rocksdb::JniUtil::v_op(getter, env, jkey, jkey_len);
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_disposeInternal(JNIEnv* /*env*/,
                                                          jobject /*jobj*/,
                                                          jlong handle) {
  auto* wbwi = reinterpret_cast<rocksdb::WriteBatchWithIndex*>(handle);
  assert(wbwi != nullptr);
  delete wbwi;
}

/* WBWIRocksIterator below */

/*
 * Class:     org_rocksdb_WBWIRocksIterator
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_WBWIRocksIterator_disposeInternal(JNIEnv* /*env*/,
                                                        jobject /*jobj*/,
                                                        jlong handle) {
  auto* it = reinterpret_cast<rocksdb::WBWIIterator*>(handle);
  assert(it != nullptr);
  delete it;
}

/*
 * Class:     org_rocksdb_WBWIRocksIterator
 * Method:    isValid0
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_WBWIRocksIterator_isValid0(JNIEnv* /*env*/,
                                                     jobject /*jobj*/,
                                                     jlong handle) {
  return reinterpret_cast<rocksdb::WBWIIterator*>(handle)->Valid();
}

/*
 * Class:     org_rocksdb_WBWIRocksIterator
 * Method:    seekToFirst0
 * Signature: (J)V
 */
void Java_org_rocksdb_WBWIRocksIterator_seekToFirst0(JNIEnv* /*env*/,
                                                     jobject /*jobj*/,
                                                     jlong handle) {
  reinterpret_cast<rocksdb::WBWIIterator*>(handle)->SeekToFirst();
}

/*
 * Class:     org_rocksdb_WBWIRocksIterator
 * Method:    seekToLast0
 * Signature: (J)V
 */
void Java_org_rocksdb_WBWIRocksIterator_seekToLast0(JNIEnv* /*env*/,
                                                    jobject /*jobj*/,
                                                    jlong handle) {
  reinterpret_cast<rocksdb::WBWIIterator*>(handle)->SeekToLast();
}

/*
 * Class:     org_rocksdb_WBWIRocksIterator
 * Method:    next0
 * Signature: (J)V
 */
void Java_org_rocksdb_WBWIRocksIterator_next0(JNIEnv* /*env*/, jobject /*jobj*/,
                                              jlong handle) {
  reinterpret_cast<rocksdb::WBWIIterator*>(handle)->Next();
}

/*
 * Class:     org_rocksdb_WBWIRocksIterator
 * Method:    prev0
 * Signature: (J)V
 */
void Java_org_rocksdb_WBWIRocksIterator_prev0(JNIEnv* /*env*/, jobject /*jobj*/,
                                              jlong handle) {
  reinterpret_cast<rocksdb::WBWIIterator*>(handle)->Prev();
}

/*
 * Class:     org_rocksdb_WBWIRocksIterator
 * Method:    seek0
 * Signature: (J[BI)V
 */
void Java_org_rocksdb_WBWIRocksIterator_seek0(JNIEnv* env, jobject /*jobj*/,
                                              jlong handle, jbyteArray jtarget,
                                              jint jtarget_len) {
  auto* it = reinterpret_cast<rocksdb::WBWIIterator*>(handle);
  jbyte* target = env->GetByteArrayElements(jtarget, nullptr);
  if (target == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }

  rocksdb::Slice target_slice(reinterpret_cast<char*>(target), jtarget_len);

  it->Seek(target_slice);

  env->ReleaseByteArrayElements(jtarget, target, JNI_ABORT);
}

/*
 * Class:     org_rocksdb_WBWIRocksIterator
 * Method:    seekForPrev0
 * Signature: (J[BI)V
 */
void Java_org_rocksdb_WBWIRocksIterator_seekForPrev0(JNIEnv* env,
                                                     jobject /*jobj*/,
                                                     jlong handle,
                                                     jbyteArray jtarget,
                                                     jint jtarget_len) {
  auto* it = reinterpret_cast<rocksdb::WBWIIterator*>(handle);
  jbyte* target = env->GetByteArrayElements(jtarget, nullptr);
  if (target == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }

  rocksdb::Slice target_slice(reinterpret_cast<char*>(target), jtarget_len);

  it->SeekForPrev(target_slice);

  env->ReleaseByteArrayElements(jtarget, target, JNI_ABORT);
}

/*
 * Class:     org_rocksdb_WBWIRocksIterator
 * Method:    status0
 * Signature: (J)V
 */
void Java_org_rocksdb_WBWIRocksIterator_status0(JNIEnv* env, jobject /*jobj*/,
                                                jlong handle) {
  auto* it = reinterpret_cast<rocksdb::WBWIIterator*>(handle);
  rocksdb::Status s = it->status();

  if (s.ok()) {
    return;
  }

  rocksdb::RocksDBExceptionJni::ThrowNew(env, s);
}

/*
 * Class:     org_rocksdb_WBWIRocksIterator
 * Method:    entry1
 * Signature: (J)[J
 */
jlongArray Java_org_rocksdb_WBWIRocksIterator_entry1(JNIEnv* env,
                                                     jobject /*jobj*/,
                                                     jlong handle) {
  auto* it = reinterpret_cast<rocksdb::WBWIIterator*>(handle);
  const rocksdb::WriteEntry& we = it->Entry();

  jlong results[3];

  // set the type of the write entry
  results[0] = rocksdb::WriteTypeJni::toJavaWriteType(we.type);

  // NOTE: key_slice and value_slice will be freed by
  // org.rocksdb.DirectSlice#close

  auto* key_slice = new rocksdb::Slice(we.key.data(), we.key.size());
  results[1] = reinterpret_cast<jlong>(key_slice);
  if (we.type == rocksdb::kDeleteRecord ||
      we.type == rocksdb::kSingleDeleteRecord ||
      we.type == rocksdb::kLogDataRecord) {
    // set native handle of value slice to null if no value available
    results[2] = 0;
  } else {
    auto* value_slice = new rocksdb::Slice(we.value.data(), we.value.size());
    results[2] = reinterpret_cast<jlong>(value_slice);
  }

  jlongArray jresults = env->NewLongArray(3);
  if (jresults == nullptr) {
    // exception thrown: OutOfMemoryError
    if (results[2] != 0) {
      auto* value_slice = reinterpret_cast<rocksdb::Slice*>(results[2]);
      delete value_slice;
    }
    delete key_slice;
    return nullptr;
  }

  env->SetLongArrayRegion(jresults, 0, 3, results);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    env->DeleteLocalRef(jresults);
    if (results[2] != 0) {
      auto* value_slice = reinterpret_cast<rocksdb::Slice*>(results[2]);
      delete value_slice;
    }
    delete key_slice;
    return nullptr;
  }

  return jresults;
}
