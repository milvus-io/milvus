//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the callback "bridge" between Java and C++ for
// rocksdb::TraceWriter.

#include "rocksjni/trace_writer_jnicallback.h"
#include "rocksjni/portal.h"

namespace rocksdb {
TraceWriterJniCallback::TraceWriterJniCallback(
    JNIEnv* env, jobject jtrace_writer)
    : JniCallback(env, jtrace_writer) {
  m_jwrite_proxy_methodid =
      AbstractTraceWriterJni::getWriteProxyMethodId(env);
  if(m_jwrite_proxy_methodid == nullptr) {
    // exception thrown: NoSuchMethodException or OutOfMemoryError
    return;
  }

  m_jclose_writer_proxy_methodid =
      AbstractTraceWriterJni::getCloseWriterProxyMethodId(env);
  if(m_jclose_writer_proxy_methodid == nullptr) {
    // exception thrown: NoSuchMethodException or OutOfMemoryError
    return;
  }

  m_jget_file_size_methodid =
      AbstractTraceWriterJni::getGetFileSizeMethodId(env);
  if(m_jget_file_size_methodid == nullptr) {
    // exception thrown: NoSuchMethodException or OutOfMemoryError
    return;
  }
}

Status TraceWriterJniCallback::Write(const Slice& data) {
  jboolean attached_thread = JNI_FALSE;
  JNIEnv* env = getJniEnv(&attached_thread);
  if (env == nullptr) {
    return Status::IOError("Unable to attach JNI Environment");
  }

  jshort jstatus = env->CallShortMethod(m_jcallback_obj,
      m_jwrite_proxy_methodid,
      &data);

  if(env->ExceptionCheck()) {
    // exception thrown from CallShortMethod
    env->ExceptionDescribe();  // print out exception to stderr
    releaseJniEnv(attached_thread);
    return Status::IOError("Unable to call AbstractTraceWriter#writeProxy(long)");
  }

  // unpack status code and status sub-code from jstatus
  jbyte jcode_value = (jstatus >> 8) & 0xFF;
  jbyte jsub_code_value = jstatus & 0xFF;
  std::unique_ptr<Status> s = StatusJni::toCppStatus(jcode_value, jsub_code_value);

  releaseJniEnv(attached_thread);

  return Status(*s);
}

Status TraceWriterJniCallback::Close() {
  jboolean attached_thread = JNI_FALSE;
  JNIEnv* env = getJniEnv(&attached_thread);
  if (env == nullptr) {
    return Status::IOError("Unable to attach JNI Environment");
  }

  jshort jstatus = env->CallShortMethod(m_jcallback_obj,
      m_jclose_writer_proxy_methodid);

  if(env->ExceptionCheck()) {
    // exception thrown from CallShortMethod
    env->ExceptionDescribe();  // print out exception to stderr
    releaseJniEnv(attached_thread);
    return Status::IOError("Unable to call AbstractTraceWriter#closeWriterProxy()");
  }

  // unpack status code and status sub-code from jstatus
  jbyte code_value = (jstatus >> 8) & 0xFF;
  jbyte sub_code_value = jstatus & 0xFF;
  std::unique_ptr<Status> s = StatusJni::toCppStatus(code_value, sub_code_value);

  releaseJniEnv(attached_thread);

  return Status(*s);
}

uint64_t TraceWriterJniCallback::GetFileSize() {
  jboolean attached_thread = JNI_FALSE;
  JNIEnv* env = getJniEnv(&attached_thread);
  if (env == nullptr) {
    return 0;
  }

  jlong jfile_size = env->CallLongMethod(m_jcallback_obj,
      m_jget_file_size_methodid);

  if(env->ExceptionCheck()) {
    // exception thrown from CallLongMethod
    env->ExceptionDescribe();  // print out exception to stderr
    releaseJniEnv(attached_thread);
    return 0;
  }

  releaseJniEnv(attached_thread);

  return static_cast<uint64_t>(jfile_size);
}

}  // namespace rocksdb