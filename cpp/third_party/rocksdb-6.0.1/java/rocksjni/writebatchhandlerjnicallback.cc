// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the callback "bridge" between Java and C++ for
// rocksdb::Comparator.

#include "rocksjni/writebatchhandlerjnicallback.h"
#include "rocksjni/portal.h"

namespace rocksdb {
WriteBatchHandlerJniCallback::WriteBatchHandlerJniCallback(
    JNIEnv* env, jobject jWriteBatchHandler)
    : JniCallback(env, jWriteBatchHandler), m_env(env) {

  m_jPutCfMethodId = WriteBatchHandlerJni::getPutCfMethodId(env);
  if(m_jPutCfMethodId == nullptr) {
    // exception thrown
    return;
  }

  m_jPutMethodId = WriteBatchHandlerJni::getPutMethodId(env);
  if(m_jPutMethodId == nullptr) {
    // exception thrown
    return;
  }

  m_jMergeCfMethodId = WriteBatchHandlerJni::getMergeCfMethodId(env);
  if(m_jMergeCfMethodId == nullptr) {
    // exception thrown
    return;
  }

  m_jMergeMethodId = WriteBatchHandlerJni::getMergeMethodId(env);
  if(m_jMergeMethodId == nullptr) {
    // exception thrown
    return;
  }

  m_jDeleteCfMethodId = WriteBatchHandlerJni::getDeleteCfMethodId(env);
  if(m_jDeleteCfMethodId == nullptr) {
    // exception thrown
    return;
  }

  m_jDeleteMethodId = WriteBatchHandlerJni::getDeleteMethodId(env);
  if(m_jDeleteMethodId == nullptr) {
    // exception thrown
    return;
  }

  m_jSingleDeleteCfMethodId =
      WriteBatchHandlerJni::getSingleDeleteCfMethodId(env);
  if(m_jSingleDeleteCfMethodId == nullptr) {
    // exception thrown
    return;
  }

  m_jSingleDeleteMethodId = WriteBatchHandlerJni::getSingleDeleteMethodId(env);
  if(m_jSingleDeleteMethodId == nullptr) {
    // exception thrown
    return;
  }

  m_jDeleteRangeCfMethodId =
      WriteBatchHandlerJni::getDeleteRangeCfMethodId(env);
  if (m_jDeleteRangeCfMethodId == nullptr) {
    // exception thrown
    return;
  }

  m_jDeleteRangeMethodId = WriteBatchHandlerJni::getDeleteRangeMethodId(env);
  if (m_jDeleteRangeMethodId == nullptr) {
    // exception thrown
    return;
  }

  m_jLogDataMethodId = WriteBatchHandlerJni::getLogDataMethodId(env);
  if(m_jLogDataMethodId == nullptr) {
    // exception thrown
    return;
  }

  m_jPutBlobIndexCfMethodId =
      WriteBatchHandlerJni::getPutBlobIndexCfMethodId(env);
  if(m_jPutBlobIndexCfMethodId == nullptr) {
    // exception thrown
    return;
  }

  m_jMarkBeginPrepareMethodId =
      WriteBatchHandlerJni::getMarkBeginPrepareMethodId(env);
  if(m_jMarkBeginPrepareMethodId == nullptr) {
    // exception thrown
    return;
  }

  m_jMarkEndPrepareMethodId =
      WriteBatchHandlerJni::getMarkEndPrepareMethodId(env);
  if(m_jMarkEndPrepareMethodId == nullptr) {
    // exception thrown
    return;
  }

  m_jMarkNoopMethodId = WriteBatchHandlerJni::getMarkNoopMethodId(env);
  if(m_jMarkNoopMethodId == nullptr) {
    // exception thrown
    return;
  }
    
  m_jMarkRollbackMethodId = WriteBatchHandlerJni::getMarkRollbackMethodId(env);
  if(m_jMarkRollbackMethodId == nullptr) {
    // exception thrown
    return;
  }

  m_jMarkCommitMethodId = WriteBatchHandlerJni::getMarkCommitMethodId(env);
  if(m_jMarkCommitMethodId == nullptr) {
    // exception thrown
    return;
  }

  m_jContinueMethodId = WriteBatchHandlerJni::getContinueMethodId(env);
  if(m_jContinueMethodId == nullptr) {
    // exception thrown
    return;
  }
}

rocksdb::Status WriteBatchHandlerJniCallback::PutCF(uint32_t column_family_id,
    const Slice& key, const Slice& value) {
  auto put = [this, column_family_id] (
      jbyteArray j_key, jbyteArray j_value) {
    m_env->CallVoidMethod(
      m_jcallback_obj,
      m_jPutCfMethodId,
      static_cast<jint>(column_family_id),
      j_key,
      j_value);
  };
  auto status = WriteBatchHandlerJniCallback::kv_op(key, value, put);
  if(status == nullptr) {
    return rocksdb::Status::OK();   // TODO(AR) what to do if there is an Exception but we don't know the rocksdb::Status?
  } else {
    return rocksdb::Status(*status);
  }
}

void WriteBatchHandlerJniCallback::Put(const Slice& key, const Slice& value) {
  auto put = [this] (
        jbyteArray j_key, jbyteArray j_value) {
    m_env->CallVoidMethod(
      m_jcallback_obj,
      m_jPutMethodId,
      j_key,
      j_value);
  };
  WriteBatchHandlerJniCallback::kv_op(key, value, put);
}

rocksdb::Status WriteBatchHandlerJniCallback::MergeCF(uint32_t column_family_id,
    const Slice& key, const Slice& value) {
  auto merge = [this, column_family_id] (
        jbyteArray j_key, jbyteArray j_value) {
    m_env->CallVoidMethod(
      m_jcallback_obj,
      m_jMergeCfMethodId,
      static_cast<jint>(column_family_id),
      j_key,
      j_value);
  };
  auto status = WriteBatchHandlerJniCallback::kv_op(key, value, merge);
  if(status == nullptr) {
    return rocksdb::Status::OK();   // TODO(AR) what to do if there is an Exception but we don't know the rocksdb::Status?
  } else {
    return rocksdb::Status(*status);
  }
}

void WriteBatchHandlerJniCallback::Merge(const Slice& key, const Slice& value) {
  auto merge = [this] (
        jbyteArray j_key, jbyteArray j_value) {
    m_env->CallVoidMethod(
      m_jcallback_obj,
      m_jMergeMethodId,
      j_key,
      j_value);
  };
  WriteBatchHandlerJniCallback::kv_op(key, value, merge);
}

rocksdb::Status WriteBatchHandlerJniCallback::DeleteCF(uint32_t column_family_id,
    const Slice& key) {
  auto remove = [this, column_family_id] (jbyteArray j_key) {
    m_env->CallVoidMethod(
      m_jcallback_obj,
      m_jDeleteCfMethodId,
      static_cast<jint>(column_family_id),
      j_key);
  };
  auto status = WriteBatchHandlerJniCallback::k_op(key, remove);
  if(status == nullptr) {
    return rocksdb::Status::OK();   // TODO(AR) what to do if there is an Exception but we don't know the rocksdb::Status?
  } else {
    return rocksdb::Status(*status);
  }
}

void WriteBatchHandlerJniCallback::Delete(const Slice& key) {
  auto remove = [this] (jbyteArray j_key) {
    m_env->CallVoidMethod(
      m_jcallback_obj,
      m_jDeleteMethodId,
      j_key);
  };
  WriteBatchHandlerJniCallback::k_op(key, remove);
}

rocksdb::Status WriteBatchHandlerJniCallback::SingleDeleteCF(uint32_t column_family_id,
    const Slice& key) {
  auto singleDelete = [this, column_family_id] (jbyteArray j_key) {
    m_env->CallVoidMethod(
      m_jcallback_obj,
      m_jSingleDeleteCfMethodId,
      static_cast<jint>(column_family_id),
      j_key);
  };
  auto status = WriteBatchHandlerJniCallback::k_op(key, singleDelete);
  if(status == nullptr) {
    return rocksdb::Status::OK();   // TODO(AR) what to do if there is an Exception but we don't know the rocksdb::Status?
  } else {
    return rocksdb::Status(*status);
  }
}

void WriteBatchHandlerJniCallback::SingleDelete(const Slice& key) {
  auto singleDelete = [this] (jbyteArray j_key) {
    m_env->CallVoidMethod(
      m_jcallback_obj,
      m_jSingleDeleteMethodId,
      j_key);
  };
  WriteBatchHandlerJniCallback::k_op(key, singleDelete);
}

rocksdb::Status WriteBatchHandlerJniCallback::DeleteRangeCF(uint32_t column_family_id,
    const Slice& beginKey, const Slice& endKey) {
  auto deleteRange = [this, column_family_id] (
        jbyteArray j_beginKey, jbyteArray j_endKey) {
    m_env->CallVoidMethod(
      m_jcallback_obj,
      m_jDeleteRangeCfMethodId,
      static_cast<jint>(column_family_id),
      j_beginKey,
      j_endKey);
  };
  auto status = WriteBatchHandlerJniCallback::kv_op(beginKey, endKey, deleteRange);
  if(status == nullptr) {
    return rocksdb::Status::OK();   // TODO(AR) what to do if there is an Exception but we don't know the rocksdb::Status?
  } else {
    return rocksdb::Status(*status);
  }
}

void WriteBatchHandlerJniCallback::DeleteRange(const Slice& beginKey,
    const Slice& endKey) {
  auto deleteRange = [this] (
        jbyteArray j_beginKey, jbyteArray j_endKey) {
    m_env->CallVoidMethod(
      m_jcallback_obj,
      m_jDeleteRangeMethodId,
      j_beginKey,
      j_endKey);
  };
  WriteBatchHandlerJniCallback::kv_op(beginKey, endKey, deleteRange);
}

void WriteBatchHandlerJniCallback::LogData(const Slice& blob) {
  auto logData = [this] (jbyteArray j_blob) {
    m_env->CallVoidMethod(
      m_jcallback_obj,
      m_jLogDataMethodId,
      j_blob);
  };
  WriteBatchHandlerJniCallback::k_op(blob, logData);
}

rocksdb::Status WriteBatchHandlerJniCallback::PutBlobIndexCF(uint32_t column_family_id,
    const Slice& key, const Slice& value) {
  auto putBlobIndex = [this, column_family_id] (
      jbyteArray j_key, jbyteArray j_value) {
    m_env->CallVoidMethod(
      m_jcallback_obj,
      m_jPutBlobIndexCfMethodId,
      static_cast<jint>(column_family_id),
      j_key,
      j_value);
  };
  auto status = WriteBatchHandlerJniCallback::kv_op(key, value, putBlobIndex);
  if(status == nullptr) {
    return rocksdb::Status::OK();   // TODO(AR) what to do if there is an Exception but we don't know the rocksdb::Status?
  } else {
    return rocksdb::Status(*status);
  }
}

rocksdb::Status WriteBatchHandlerJniCallback::MarkBeginPrepare(bool unprepare) {
#ifndef DEBUG
  (void) unprepare;
#else
  assert(!unprepare);
#endif
  m_env->CallVoidMethod(m_jcallback_obj, m_jMarkBeginPrepareMethodId);

  // check for Exception, in-particular RocksDBException
  if (m_env->ExceptionCheck()) {
    // exception thrown
    jthrowable exception = m_env->ExceptionOccurred();
    std::unique_ptr<rocksdb::Status> status = rocksdb::RocksDBExceptionJni::toCppStatus(m_env, exception);
    if (status == nullptr) {
      // unkown status or exception occurred extracting status
      m_env->ExceptionDescribe();
      return rocksdb::Status::OK();  // TODO(AR) probably need a better error code here

    } else {
      m_env->ExceptionClear();  // clear the exception, as we have extracted the status
      return rocksdb::Status(*status);
    }
  }

  return rocksdb::Status::OK();
}

rocksdb::Status WriteBatchHandlerJniCallback::MarkEndPrepare(const Slice& xid) {
  auto markEndPrepare = [this] (
      jbyteArray j_xid) {
    m_env->CallVoidMethod(
      m_jcallback_obj,
      m_jMarkEndPrepareMethodId,
      j_xid);
  };
  auto status = WriteBatchHandlerJniCallback::k_op(xid, markEndPrepare);
  if(status == nullptr) {
    return rocksdb::Status::OK();   // TODO(AR) what to do if there is an Exception but we don't know the rocksdb::Status?
  } else {
    return rocksdb::Status(*status);
  }
}

rocksdb::Status WriteBatchHandlerJniCallback::MarkNoop(bool empty_batch) {
  m_env->CallVoidMethod(m_jcallback_obj, m_jMarkNoopMethodId, static_cast<jboolean>(empty_batch));

  // check for Exception, in-particular RocksDBException
  if (m_env->ExceptionCheck()) {
    // exception thrown
    jthrowable exception = m_env->ExceptionOccurred();
    std::unique_ptr<rocksdb::Status> status = rocksdb::RocksDBExceptionJni::toCppStatus(m_env, exception);
    if (status == nullptr) {
      // unkown status or exception occurred extracting status
      m_env->ExceptionDescribe();
      return rocksdb::Status::OK();  // TODO(AR) probably need a better error code here

    } else {
      m_env->ExceptionClear();  // clear the exception, as we have extracted the status
      return rocksdb::Status(*status);
    }
  }

  return rocksdb::Status::OK();
}

rocksdb::Status WriteBatchHandlerJniCallback::MarkRollback(const Slice& xid) {
  auto markRollback = [this] (
      jbyteArray j_xid) {
    m_env->CallVoidMethod(
      m_jcallback_obj,
      m_jMarkRollbackMethodId,
      j_xid);
  };
  auto status = WriteBatchHandlerJniCallback::k_op(xid, markRollback);
  if(status == nullptr) {
    return rocksdb::Status::OK();   // TODO(AR) what to do if there is an Exception but we don't know the rocksdb::Status?
  } else {
    return rocksdb::Status(*status);
  }
}

rocksdb::Status WriteBatchHandlerJniCallback::MarkCommit(const Slice& xid) {
  auto markCommit = [this] (
      jbyteArray j_xid) {
    m_env->CallVoidMethod(
      m_jcallback_obj,
      m_jMarkCommitMethodId,
      j_xid);
  };
  auto status = WriteBatchHandlerJniCallback::k_op(xid, markCommit);
  if(status == nullptr) {
    return rocksdb::Status::OK();   // TODO(AR) what to do if there is an Exception but we don't know the rocksdb::Status?
  } else {
    return rocksdb::Status(*status);
  }
}

bool WriteBatchHandlerJniCallback::Continue() {
  jboolean jContinue = m_env->CallBooleanMethod(
      m_jcallback_obj,
      m_jContinueMethodId);
  if(m_env->ExceptionCheck()) {
    // exception thrown
    m_env->ExceptionDescribe();
  }

  return static_cast<bool>(jContinue == JNI_TRUE);
}

std::unique_ptr<rocksdb::Status> WriteBatchHandlerJniCallback::kv_op(const Slice& key, const Slice& value, std::function<void(jbyteArray, jbyteArray)> kvFn) {
    const jbyteArray j_key = JniUtil::copyBytes(m_env, key);
  if (j_key == nullptr) {
    // exception thrown
    if (m_env->ExceptionCheck()) {
      m_env->ExceptionDescribe();
    }
    return nullptr;
  }

  const jbyteArray j_value = JniUtil::copyBytes(m_env, value);
  if (j_value == nullptr) {
    // exception thrown
    if (m_env->ExceptionCheck()) {
      m_env->ExceptionDescribe();
    }
    if (j_key != nullptr) {
      m_env->DeleteLocalRef(j_key);
    }
    return nullptr;
  }

  kvFn(j_key, j_value);

  // check for Exception, in-particular RocksDBException
  if (m_env->ExceptionCheck()) {
    if (j_value != nullptr) {
      m_env->DeleteLocalRef(j_value);
    }
    if (j_key != nullptr) {
      m_env->DeleteLocalRef(j_key);
    }

    // exception thrown
    jthrowable exception = m_env->ExceptionOccurred();
    std::unique_ptr<rocksdb::Status> status = rocksdb::RocksDBExceptionJni::toCppStatus(m_env, exception);
    if (status == nullptr) {
      // unkown status or exception occurred extracting status
      m_env->ExceptionDescribe();
      return nullptr;

    } else {
      m_env->ExceptionClear();  // clear the exception, as we have extracted the status
      return status;
    }
  }

  if (j_value != nullptr) {
    m_env->DeleteLocalRef(j_value);
  }
  if (j_key != nullptr) {
    m_env->DeleteLocalRef(j_key);
  }

  // all OK
  return std::unique_ptr<rocksdb::Status>(new rocksdb::Status(rocksdb::Status::OK()));
}

std::unique_ptr<rocksdb::Status> WriteBatchHandlerJniCallback::k_op(const Slice& key, std::function<void(jbyteArray)> kFn) {
    const jbyteArray j_key = JniUtil::copyBytes(m_env, key);
  if (j_key == nullptr) {
    // exception thrown
    if (m_env->ExceptionCheck()) {
      m_env->ExceptionDescribe();
    }
    return nullptr;
  }

  kFn(j_key);

  // check for Exception, in-particular RocksDBException
  if (m_env->ExceptionCheck()) {
    if (j_key != nullptr) {
      m_env->DeleteLocalRef(j_key);
    }

    // exception thrown
    jthrowable exception = m_env->ExceptionOccurred();
    std::unique_ptr<rocksdb::Status> status = rocksdb::RocksDBExceptionJni::toCppStatus(m_env, exception);
    if (status == nullptr) {
      // unkown status or exception occurred extracting status
      m_env->ExceptionDescribe();
      return nullptr;

    } else {
      m_env->ExceptionClear();  // clear the exception, as we have extracted the status
      return status;
    }
  }

  if (j_key != nullptr) {
    m_env->DeleteLocalRef(j_key);
  }

  // all OK
  return std::unique_ptr<rocksdb::Status>(new rocksdb::Status(rocksdb::Status::OK()));
}
}  // namespace rocksdb
