// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ and enables
// calling c++ rocksdb::Env methods from Java side.

#include <jni.h>
#include <vector>

#include "portal.h"
#include "rocksdb/env.h"
#include "include/org_rocksdb_Env.h"
#include "include/org_rocksdb_HdfsEnv.h"
#include "include/org_rocksdb_RocksEnv.h"
#include "include/org_rocksdb_RocksMemEnv.h"
#include "include/org_rocksdb_TimedEnv.h"

/*
 * Class:     org_rocksdb_Env
 * Method:    getDefaultEnvInternal
 * Signature: ()J
 */
jlong Java_org_rocksdb_Env_getDefaultEnvInternal(
    JNIEnv*, jclass) {
  return reinterpret_cast<jlong>(rocksdb::Env::Default());
}

/*
 * Class:     org_rocksdb_RocksEnv
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_RocksEnv_disposeInternal(
    JNIEnv*, jobject, jlong jhandle) {
  auto* e = reinterpret_cast<rocksdb::Env*>(jhandle);
  assert(e != nullptr);
  delete e;
}

/*
 * Class:     org_rocksdb_Env
 * Method:    setBackgroundThreads
 * Signature: (JIB)V
 */
void Java_org_rocksdb_Env_setBackgroundThreads(
    JNIEnv*, jobject, jlong jhandle, jint jnum, jbyte jpriority_value) {
  auto* rocks_env = reinterpret_cast<rocksdb::Env*>(jhandle);
  rocks_env->SetBackgroundThreads(static_cast<int>(jnum),
      rocksdb::PriorityJni::toCppPriority(jpriority_value));
}

/*
 * Class:     org_rocksdb_Env
 * Method:    getBackgroundThreads
 * Signature: (JB)I
 */
jint Java_org_rocksdb_Env_getBackgroundThreads(
    JNIEnv*, jobject, jlong jhandle, jbyte jpriority_value) {
  auto* rocks_env = reinterpret_cast<rocksdb::Env*>(jhandle);
  const int num = rocks_env->GetBackgroundThreads(
      rocksdb::PriorityJni::toCppPriority(jpriority_value));
  return static_cast<jint>(num);
}

/*
 * Class:     org_rocksdb_Env
 * Method:    getThreadPoolQueueLen
 * Signature: (JB)I
 */
jint Java_org_rocksdb_Env_getThreadPoolQueueLen(
    JNIEnv*, jobject, jlong jhandle, jbyte jpriority_value) {
  auto* rocks_env = reinterpret_cast<rocksdb::Env*>(jhandle);
  const int queue_len = rocks_env->GetThreadPoolQueueLen(
      rocksdb::PriorityJni::toCppPriority(jpriority_value));
  return static_cast<jint>(queue_len);
}

/*
 * Class:     org_rocksdb_Env
 * Method:    incBackgroundThreadsIfNeeded
 * Signature: (JIB)V
 */
void Java_org_rocksdb_Env_incBackgroundThreadsIfNeeded(
    JNIEnv*, jobject, jlong jhandle, jint jnum, jbyte jpriority_value) {
  auto* rocks_env = reinterpret_cast<rocksdb::Env*>(jhandle);
  rocks_env->IncBackgroundThreadsIfNeeded(static_cast<int>(jnum),
      rocksdb::PriorityJni::toCppPriority(jpriority_value));
}

/*
 * Class:     org_rocksdb_Env
 * Method:    lowerThreadPoolIOPriority
 * Signature: (JB)V
 */
void Java_org_rocksdb_Env_lowerThreadPoolIOPriority(
    JNIEnv*, jobject, jlong jhandle, jbyte jpriority_value) {
  auto* rocks_env = reinterpret_cast<rocksdb::Env*>(jhandle);
  rocks_env->LowerThreadPoolIOPriority(
      rocksdb::PriorityJni::toCppPriority(jpriority_value));
}

/*
 * Class:     org_rocksdb_Env
 * Method:    lowerThreadPoolCPUPriority
 * Signature: (JB)V
 */
void Java_org_rocksdb_Env_lowerThreadPoolCPUPriority(
    JNIEnv*, jobject, jlong jhandle, jbyte jpriority_value) {
  auto* rocks_env = reinterpret_cast<rocksdb::Env*>(jhandle);
  rocks_env->LowerThreadPoolCPUPriority(
      rocksdb::PriorityJni::toCppPriority(jpriority_value));
}

/*
 * Class:     org_rocksdb_Env
 * Method:    getThreadList
 * Signature: (J)[Lorg/rocksdb/ThreadStatus;
 */
jobjectArray Java_org_rocksdb_Env_getThreadList(
    JNIEnv* env, jobject, jlong jhandle) {
  auto* rocks_env = reinterpret_cast<rocksdb::Env*>(jhandle);
  std::vector<rocksdb::ThreadStatus> thread_status;
  rocksdb::Status s = rocks_env->GetThreadList(&thread_status);
  if (!s.ok()) {
    // error, throw exception
    rocksdb::RocksDBExceptionJni::ThrowNew(env, s);
    return nullptr;
  }

  // object[]
  const jsize len = static_cast<jsize>(thread_status.size());
  jobjectArray jthread_status =
      env->NewObjectArray(len, rocksdb::ThreadStatusJni::getJClass(env), nullptr);
  if (jthread_status == nullptr) {
    // an exception occurred
    return nullptr;
  }
  for (jsize i = 0; i < len; ++i) {
    jobject jts =
        rocksdb::ThreadStatusJni::construct(env, &(thread_status[i]));
    env->SetObjectArrayElement(jthread_status, i, jts);
    if (env->ExceptionCheck()) {
      // exception occurred
      env->DeleteLocalRef(jthread_status);
      return nullptr;
    }
  }

  return jthread_status;
}

/*
 * Class:     org_rocksdb_RocksMemEnv
 * Method:    createMemEnv
 * Signature: (J)J
 */
jlong Java_org_rocksdb_RocksMemEnv_createMemEnv(
    JNIEnv*, jclass, jlong jbase_env_handle) {
  auto* base_env = reinterpret_cast<rocksdb::Env*>(jbase_env_handle);
  return reinterpret_cast<jlong>(rocksdb::NewMemEnv(base_env));
}

/*
 * Class:     org_rocksdb_RocksMemEnv
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_RocksMemEnv_disposeInternal(
    JNIEnv*, jobject, jlong jhandle) {
  auto* e = reinterpret_cast<rocksdb::Env*>(jhandle);
  assert(e != nullptr);
  delete e;
}

/*
 * Class:     org_rocksdb_HdfsEnv
 * Method:    createHdfsEnv
 * Signature: (Ljava/lang/String;)J
 */
jlong Java_org_rocksdb_HdfsEnv_createHdfsEnv(
    JNIEnv* env, jclass, jstring jfsname) {
  jboolean has_exception = JNI_FALSE;
  auto fsname = rocksdb::JniUtil::copyStdString(env, jfsname, &has_exception);
  if (has_exception == JNI_TRUE) {
    // exception occurred
    return 0;
  }
  rocksdb::Env* hdfs_env;
  rocksdb::Status s = rocksdb::NewHdfsEnv(&hdfs_env, fsname);
  if (!s.ok()) {
    // error occurred
    rocksdb::RocksDBExceptionJni::ThrowNew(env, s);
    return 0;
  }
  return reinterpret_cast<jlong>(hdfs_env);
}

/*
 * Class:     org_rocksdb_HdfsEnv
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_HdfsEnv_disposeInternal(
    JNIEnv*, jobject, jlong jhandle) {
  auto* e = reinterpret_cast<rocksdb::Env*>(jhandle);
  assert(e != nullptr);
  delete e;
}

/*
 * Class:     org_rocksdb_TimedEnv
 * Method:    createTimedEnv
 * Signature: (J)J
 */
jlong Java_org_rocksdb_TimedEnv_createTimedEnv(
    JNIEnv*, jclass, jlong jbase_env_handle) {
  auto* base_env = reinterpret_cast<rocksdb::Env*>(jbase_env_handle);
  return reinterpret_cast<jlong>(rocksdb::NewTimedEnv(base_env));
}

/*
 * Class:     org_rocksdb_TimedEnv
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_TimedEnv_disposeInternal(
    JNIEnv*, jobject, jlong jhandle) {
  auto* e = reinterpret_cast<rocksdb::Env*>(jhandle);
  assert(e != nullptr);
  delete e;
}

