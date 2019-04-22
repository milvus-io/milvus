//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the callback "bridge" between Java and C++ for
// rocksdb::TableFilter.

#include "rocksjni/table_filter_jnicallback.h"
#include "rocksjni/portal.h"

namespace rocksdb {
TableFilterJniCallback::TableFilterJniCallback(
    JNIEnv* env, jobject jtable_filter)
    : JniCallback(env, jtable_filter) {
  m_jfilter_methodid =
      AbstractTableFilterJni::getFilterMethod(env);
  if(m_jfilter_methodid == nullptr) {
    // exception thrown: NoSuchMethodException or OutOfMemoryError
    return;
  }

  // create the function reference
  /*
  Note the JNI ENV must be obtained/release
  on each call to the function itself as
  it may be called from multiple threads
  */
  m_table_filter_function = [this](const rocksdb::TableProperties& table_properties) {
    jboolean attached_thread = JNI_FALSE;
    JNIEnv* thread_env = getJniEnv(&attached_thread);
    assert(thread_env != nullptr);

    // create a Java TableProperties object
    jobject jtable_properties = TablePropertiesJni::fromCppTableProperties(thread_env, table_properties);
    if (jtable_properties == nullptr) {
      // exception thrown from fromCppTableProperties
      thread_env->ExceptionDescribe();  // print out exception to stderr
      releaseJniEnv(attached_thread);
      return false;
    }

    jboolean result = thread_env->CallBooleanMethod(m_jcallback_obj, m_jfilter_methodid, jtable_properties);
    if (thread_env->ExceptionCheck()) {
      // exception thrown from CallBooleanMethod
      thread_env->DeleteLocalRef(jtable_properties);
      thread_env->ExceptionDescribe();  // print out exception to stderr
      releaseJniEnv(attached_thread);
      return false;
    }

    // ok... cleanup and then return
    releaseJniEnv(attached_thread);
    return static_cast<bool>(result);
  };
}

std::function<bool(const rocksdb::TableProperties&)> TableFilterJniCallback::GetTableFilterFunction() {
  return m_table_filter_function;
}

}  // namespace rocksdb
