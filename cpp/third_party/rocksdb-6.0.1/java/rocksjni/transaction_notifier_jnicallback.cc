// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the callback "bridge" between Java and C++ for
// rocksdb::TransactionNotifier.

#include "rocksjni/transaction_notifier_jnicallback.h"
#include "rocksjni/portal.h"

namespace rocksdb {

TransactionNotifierJniCallback::TransactionNotifierJniCallback(JNIEnv* env,
    jobject jtransaction_notifier) : JniCallback(env, jtransaction_notifier) {
  // we cache the method id for the JNI callback
  m_jsnapshot_created_methodID =
      AbstractTransactionNotifierJni::getSnapshotCreatedMethodId(env);
}

void TransactionNotifierJniCallback::SnapshotCreated(
    const Snapshot* newSnapshot) {
  jboolean attached_thread = JNI_FALSE;
  JNIEnv* env = getJniEnv(&attached_thread);
  assert(env != nullptr);

  env->CallVoidMethod(m_jcallback_obj,
      m_jsnapshot_created_methodID, reinterpret_cast<jlong>(newSnapshot));

  if(env->ExceptionCheck()) {
    // exception thrown from CallVoidMethod
    env->ExceptionDescribe();  // print out exception to stderr
    releaseJniEnv(attached_thread);
    return;
  }

  releaseJniEnv(attached_thread);
}
}  // namespace rocksdb
