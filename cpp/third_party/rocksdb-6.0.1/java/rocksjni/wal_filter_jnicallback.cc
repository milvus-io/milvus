//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the callback "bridge" between Java and C++ for
// rocksdb::WalFilter.

#include "rocksjni/wal_filter_jnicallback.h"
#include "rocksjni/portal.h"

namespace rocksdb {
WalFilterJniCallback::WalFilterJniCallback(
    JNIEnv* env, jobject jwal_filter)
    : JniCallback(env, jwal_filter) {
  // Note: The name of a WalFilter will not change during it's lifetime,
  // so we cache it in a global var
  jmethodID jname_mid = AbstractWalFilterJni::getNameMethodId(env);
  if(jname_mid == nullptr) {
    // exception thrown: NoSuchMethodException or OutOfMemoryError
    return;
  }
  jstring jname = (jstring)env->CallObjectMethod(m_jcallback_obj, jname_mid);
  if(env->ExceptionCheck()) {
    // exception thrown
    return;
  }
  jboolean has_exception = JNI_FALSE;
  m_name = JniUtil::copyString(env, jname,
      &has_exception);  // also releases jname
  if (has_exception == JNI_TRUE) {
    // exception thrown
    return;
  }

  m_column_family_log_number_map_mid =
      AbstractWalFilterJni::getColumnFamilyLogNumberMapMethodId(env);
  if(m_column_family_log_number_map_mid == nullptr) {
    // exception thrown: NoSuchMethodException or OutOfMemoryError
    return;
  }

  m_log_record_found_proxy_mid =
      AbstractWalFilterJni::getLogRecordFoundProxyMethodId(env);
  if(m_log_record_found_proxy_mid == nullptr) {
    // exception thrown: NoSuchMethodException or OutOfMemoryError
    return;
  }
}

void WalFilterJniCallback::ColumnFamilyLogNumberMap(
    const std::map<uint32_t, uint64_t>& cf_lognumber_map,
    const std::map<std::string, uint32_t>& cf_name_id_map) {
  jboolean attached_thread = JNI_FALSE;
  JNIEnv* env = getJniEnv(&attached_thread);
  if (env == nullptr) {
    return;
  }

  jobject jcf_lognumber_map =
      rocksdb::HashMapJni::fromCppMap(env, &cf_lognumber_map);
  if (jcf_lognumber_map == nullptr) {
    // exception occurred
    env->ExceptionDescribe(); // print out exception to stderr
    releaseJniEnv(attached_thread);
    return;
  }

  jobject jcf_name_id_map =
      rocksdb::HashMapJni::fromCppMap(env, &cf_name_id_map);
  if (jcf_name_id_map == nullptr) {
    // exception occurred
    env->ExceptionDescribe(); // print out exception to stderr
    env->DeleteLocalRef(jcf_lognumber_map);
    releaseJniEnv(attached_thread);
    return;
  }

  env->CallVoidMethod(m_jcallback_obj,
      m_column_family_log_number_map_mid,
      jcf_lognumber_map,
      jcf_name_id_map);

  env->DeleteLocalRef(jcf_lognumber_map);
  env->DeleteLocalRef(jcf_name_id_map);

  if(env->ExceptionCheck()) {
    // exception thrown from CallVoidMethod
    env->ExceptionDescribe();  // print out exception to stderr
  }

  releaseJniEnv(attached_thread);
}

 WalFilter::WalProcessingOption WalFilterJniCallback::LogRecordFound(
    unsigned long long log_number, const std::string& log_file_name,
    const WriteBatch& batch, WriteBatch* new_batch, bool* batch_changed) {
  jboolean attached_thread = JNI_FALSE;
  JNIEnv* env = getJniEnv(&attached_thread);
  if (env == nullptr) {
    return  WalFilter::WalProcessingOption::kCorruptedRecord;
  }
  
  jstring jlog_file_name = JniUtil::toJavaString(env, &log_file_name);
  if (jlog_file_name == nullptr) {
    // exception occcurred
      env->ExceptionDescribe(); // print out exception to stderr
    releaseJniEnv(attached_thread);
    return  WalFilter::WalProcessingOption::kCorruptedRecord;
  }

  jshort jlog_record_found_result = env->CallShortMethod(m_jcallback_obj,
      m_log_record_found_proxy_mid,
      static_cast<jlong>(log_number),
      jlog_file_name,
      reinterpret_cast<jlong>(&batch),
      reinterpret_cast<jlong>(new_batch));
  
  env->DeleteLocalRef(jlog_file_name);

  if (env->ExceptionCheck()) {
    // exception thrown from CallShortMethod
    env->ExceptionDescribe();  // print out exception to stderr
    releaseJniEnv(attached_thread);
    return  WalFilter::WalProcessingOption::kCorruptedRecord;
  }

  // unpack WalProcessingOption and batch_changed from jlog_record_found_result
  jbyte jwal_processing_option_value = (jlog_record_found_result >> 8) & 0xFF;
  jbyte jbatch_changed_value = jlog_record_found_result & 0xFF;

  releaseJniEnv(attached_thread);

  *batch_changed = jbatch_changed_value == JNI_TRUE;

  return WalProcessingOptionJni::toCppWalProcessingOption(
      jwal_processing_option_value);
}

const char* WalFilterJniCallback::Name() const {
  return m_name.get();
}

}  // namespace rocksdb