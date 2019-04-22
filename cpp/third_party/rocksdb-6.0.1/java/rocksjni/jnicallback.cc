//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the callback "bridge" between Java and C++ for
// JNI Callbacks from C++ to sub-classes or org.rocksdb.RocksCallbackObject

#include <assert.h>
#include "rocksjni/jnicallback.h"
#include "rocksjni/portal.h"

namespace rocksdb {
JniCallback::JniCallback(JNIEnv* env, jobject jcallback_obj) {
  // Note: jcallback_obj may be accessed by multiple threads,
  // so we ref the jvm not the env
  const jint rs = env->GetJavaVM(&m_jvm);
  if(rs != JNI_OK) {
    // exception thrown
    return;
  }

  // Note: we may want to access the Java callback object instance
  // across multiple method calls, so we create a global ref
  assert(jcallback_obj != nullptr);
  m_jcallback_obj = env->NewGlobalRef(jcallback_obj);
  if(jcallback_obj == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }
}

JNIEnv* JniCallback::getJniEnv(jboolean* attached) const {
  return JniUtil::getJniEnv(m_jvm, attached);
}

void JniCallback::releaseJniEnv(jboolean& attached) const {
  JniUtil::releaseJniEnv(m_jvm, attached);
}

JniCallback::~JniCallback() {
  jboolean attached_thread = JNI_FALSE;
  JNIEnv* env = getJniEnv(&attached_thread);
  assert(env != nullptr);

  if(m_jcallback_obj != nullptr) {    
    env->DeleteGlobalRef(m_jcallback_obj);
  }

  releaseJniEnv(attached_thread);
}
// @lint-ignore TXT4 T25377293 Grandfathered in
}  // namespace rocksdb