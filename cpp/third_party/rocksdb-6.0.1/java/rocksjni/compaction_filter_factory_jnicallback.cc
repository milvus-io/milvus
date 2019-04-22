//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the callback "bridge" between Java and C++ for
// rocksdb::CompactionFilterFactory.

#include "rocksjni/compaction_filter_factory_jnicallback.h"
#include "rocksjni/portal.h"

namespace rocksdb {
CompactionFilterFactoryJniCallback::CompactionFilterFactoryJniCallback(
    JNIEnv* env, jobject jcompaction_filter_factory)
    : JniCallback(env, jcompaction_filter_factory) {
  
  // Note: The name of a CompactionFilterFactory will not change during
  // it's lifetime, so we cache it in a global var
  jmethodID jname_method_id =
      AbstractCompactionFilterFactoryJni::getNameMethodId(env);
  if(jname_method_id == nullptr) {
    // exception thrown: NoSuchMethodException or OutOfMemoryError
    return;
  }

  jstring jname =
      (jstring)env->CallObjectMethod(m_jcallback_obj, jname_method_id);
  if(env->ExceptionCheck()) {
    // exception thrown
    return;
  }
  jboolean has_exception = JNI_FALSE;
  m_name = JniUtil::copyString(env, jname, &has_exception);  // also releases jname
  if (has_exception == JNI_TRUE) {
    // exception thrown
    return;
  }

  m_jcreate_compaction_filter_methodid =
      AbstractCompactionFilterFactoryJni::getCreateCompactionFilterMethodId(env);
  if(m_jcreate_compaction_filter_methodid == nullptr) {
    // exception thrown: NoSuchMethodException or OutOfMemoryError
    return;
  }
}

const char* CompactionFilterFactoryJniCallback::Name() const {
  return m_name.get();
}

std::unique_ptr<CompactionFilter> CompactionFilterFactoryJniCallback::CreateCompactionFilter(
    const CompactionFilter::Context& context) {
  jboolean attached_thread = JNI_FALSE;
  JNIEnv* env = getJniEnv(&attached_thread);
  assert(env != nullptr);

  jlong addr_compaction_filter = env->CallLongMethod(m_jcallback_obj,
      m_jcreate_compaction_filter_methodid,
      static_cast<jboolean>(context.is_full_compaction),
      static_cast<jboolean>(context.is_manual_compaction));

  if(env->ExceptionCheck()) {
    // exception thrown from CallLongMethod
    env->ExceptionDescribe();  // print out exception to stderr
    releaseJniEnv(attached_thread);
    return nullptr;
  }

  auto* cff = reinterpret_cast<CompactionFilter*>(addr_compaction_filter);

  releaseJniEnv(attached_thread);

  return std::unique_ptr<CompactionFilter>(cff);
}

}  // namespace rocksdb
