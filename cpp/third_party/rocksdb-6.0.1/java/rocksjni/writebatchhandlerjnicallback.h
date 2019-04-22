// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the callback "bridge" between Java and C++ for
// rocksdb::WriteBatch::Handler.

#ifndef JAVA_ROCKSJNI_WRITEBATCHHANDLERJNICALLBACK_H_
#define JAVA_ROCKSJNI_WRITEBATCHHANDLERJNICALLBACK_H_

#include <functional>
#include <jni.h>
#include <memory>
#include "rocksjni/jnicallback.h"
#include "rocksdb/write_batch.h"

namespace rocksdb {
/**
 * This class acts as a bridge between C++
 * and Java. The methods in this class will be
 * called back from the RocksDB storage engine (C++)
 * which calls the appropriate Java method.
 * This enables Write Batch Handlers to be implemented in Java.
 */
class WriteBatchHandlerJniCallback : public JniCallback, public WriteBatch::Handler {
 public:
    WriteBatchHandlerJniCallback(
      JNIEnv* env, jobject jWriteBackHandler);
    Status PutCF(uint32_t column_family_id, const Slice& key,
        const Slice& value);
    void Put(const Slice& key, const Slice& value);
    Status MergeCF(uint32_t column_family_id, const Slice& key,
        const Slice& value);
    void Merge(const Slice& key, const Slice& value);
    Status DeleteCF(uint32_t column_family_id, const Slice& key);
    void Delete(const Slice& key);
    Status SingleDeleteCF(uint32_t column_family_id, const Slice& key);
    void SingleDelete(const Slice& key);
    Status DeleteRangeCF(uint32_t column_family_id, const Slice& beginKey,
        const Slice& endKey);
    void DeleteRange(const Slice& beginKey, const Slice& endKey);
    void LogData(const Slice& blob);
    Status PutBlobIndexCF(uint32_t column_family_id, const Slice& key,
                          const Slice& value);
    Status MarkBeginPrepare(bool);
    Status MarkEndPrepare(const Slice& xid);
    Status MarkNoop(bool empty_batch);
    Status MarkRollback(const Slice& xid);
    Status MarkCommit(const Slice& xid);
    bool Continue();

 private:
    JNIEnv* m_env;
    jmethodID m_jPutCfMethodId;
    jmethodID m_jPutMethodId;
    jmethodID m_jMergeCfMethodId;
    jmethodID m_jMergeMethodId;
    jmethodID m_jDeleteCfMethodId;
    jmethodID m_jDeleteMethodId;
    jmethodID m_jSingleDeleteCfMethodId;
    jmethodID m_jSingleDeleteMethodId;
    jmethodID m_jDeleteRangeCfMethodId;
    jmethodID m_jDeleteRangeMethodId;
    jmethodID m_jLogDataMethodId;
    jmethodID m_jPutBlobIndexCfMethodId;
    jmethodID m_jMarkBeginPrepareMethodId;
    jmethodID m_jMarkEndPrepareMethodId;
    jmethodID m_jMarkNoopMethodId;
    jmethodID m_jMarkRollbackMethodId;
    jmethodID m_jMarkCommitMethodId;
    jmethodID m_jContinueMethodId;
    /**
     * @return A pointer to a rocksdb::Status or nullptr if an unexpected exception occurred
     */
    std::unique_ptr<rocksdb::Status> kv_op(const Slice& key, const Slice& value, std::function<void(jbyteArray, jbyteArray)> kvFn);
    /**
     * @return A pointer to a rocksdb::Status or nullptr if an unexpected exception occurred
     */
    std::unique_ptr<rocksdb::Status> k_op(const Slice& key, std::function<void(jbyteArray)> kFn);
};
}  // namespace rocksdb

#endif  // JAVA_ROCKSJNI_WRITEBATCHHANDLERJNICALLBACK_H_
