// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the callback "bridge" between Java and C++ for
// rocksdb::Logger.

#include "include/org_rocksdb_Logger.h"

#include <cstdarg>
#include <cstdio>
#include "rocksjni/loggerjnicallback.h"
#include "rocksjni/portal.h"

namespace rocksdb {

LoggerJniCallback::LoggerJniCallback(JNIEnv* env, jobject jlogger)
    : JniCallback(env, jlogger) {
  m_jLogMethodId = LoggerJni::getLogMethodId(env);
  if (m_jLogMethodId == nullptr) {
    // exception thrown: NoSuchMethodException or OutOfMemoryError
    return;
  }

  jobject jdebug_level = InfoLogLevelJni::DEBUG_LEVEL(env);
  if (jdebug_level == nullptr) {
    // exception thrown: NoSuchFieldError, ExceptionInInitializerError
    // or OutOfMemoryError
    return;
  }
  m_jdebug_level = env->NewGlobalRef(jdebug_level);
  if (m_jdebug_level == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }

  jobject jinfo_level = InfoLogLevelJni::INFO_LEVEL(env);
  if (jinfo_level == nullptr) {
    // exception thrown: NoSuchFieldError, ExceptionInInitializerError
    // or OutOfMemoryError
    return;
  }
  m_jinfo_level = env->NewGlobalRef(jinfo_level);
  if (m_jinfo_level == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }

  jobject jwarn_level = InfoLogLevelJni::WARN_LEVEL(env);
  if (jwarn_level == nullptr) {
    // exception thrown: NoSuchFieldError, ExceptionInInitializerError
    // or OutOfMemoryError
    return;
  }
  m_jwarn_level = env->NewGlobalRef(jwarn_level);
  if (m_jwarn_level == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }

  jobject jerror_level = InfoLogLevelJni::ERROR_LEVEL(env);
  if (jerror_level == nullptr) {
    // exception thrown: NoSuchFieldError, ExceptionInInitializerError
    // or OutOfMemoryError
    return;
  }
  m_jerror_level = env->NewGlobalRef(jerror_level);
  if (m_jerror_level == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }

  jobject jfatal_level = InfoLogLevelJni::FATAL_LEVEL(env);
  if (jfatal_level == nullptr) {
    // exception thrown: NoSuchFieldError, ExceptionInInitializerError
    // or OutOfMemoryError
    return;
  }
  m_jfatal_level = env->NewGlobalRef(jfatal_level);
  if (m_jfatal_level == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }

  jobject jheader_level = InfoLogLevelJni::HEADER_LEVEL(env);
  if (jheader_level == nullptr) {
    // exception thrown: NoSuchFieldError, ExceptionInInitializerError
    // or OutOfMemoryError
    return;
  }
  m_jheader_level = env->NewGlobalRef(jheader_level);
  if (m_jheader_level == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }
}

void LoggerJniCallback::Logv(const char* /*format*/, va_list /*ap*/) {
  // We implement this method because it is virtual but we don't
  // use it because we need to know about the log level.
}

void LoggerJniCallback::Logv(const InfoLogLevel log_level, const char* format,
                             va_list ap) {
  if (GetInfoLogLevel() <= log_level) {
    // determine InfoLogLevel java enum instance
    jobject jlog_level;
    switch (log_level) {
      case rocksdb::InfoLogLevel::DEBUG_LEVEL:
        jlog_level = m_jdebug_level;
        break;
      case rocksdb::InfoLogLevel::INFO_LEVEL:
        jlog_level = m_jinfo_level;
        break;
      case rocksdb::InfoLogLevel::WARN_LEVEL:
        jlog_level = m_jwarn_level;
        break;
      case rocksdb::InfoLogLevel::ERROR_LEVEL:
        jlog_level = m_jerror_level;
        break;
      case rocksdb::InfoLogLevel::FATAL_LEVEL:
        jlog_level = m_jfatal_level;
        break;
      case rocksdb::InfoLogLevel::HEADER_LEVEL:
        jlog_level = m_jheader_level;
        break;
      default:
        jlog_level = m_jfatal_level;
        break;
    }

    assert(format != nullptr);
    assert(ap != nullptr);
    const std::unique_ptr<char[]> msg = format_str(format, ap);

    // pass msg to java callback handler
    jboolean attached_thread = JNI_FALSE;
    JNIEnv* env = getJniEnv(&attached_thread);
    assert(env != nullptr);

    jstring jmsg = env->NewStringUTF(msg.get());
    if (jmsg == nullptr) {
      // unable to construct string
      if (env->ExceptionCheck()) {
        env->ExceptionDescribe();  // print out exception to stderr
      }
      releaseJniEnv(attached_thread);
      return;
    }
    if (env->ExceptionCheck()) {
      // exception thrown: OutOfMemoryError
      env->ExceptionDescribe();  // print out exception to stderr
      env->DeleteLocalRef(jmsg);
      releaseJniEnv(attached_thread);
      return;
    }

    env->CallVoidMethod(m_jcallback_obj, m_jLogMethodId, jlog_level, jmsg);
    if (env->ExceptionCheck()) {
      // exception thrown
      env->ExceptionDescribe();  // print out exception to stderr
      env->DeleteLocalRef(jmsg);
      releaseJniEnv(attached_thread);
      return;
    }

    env->DeleteLocalRef(jmsg);
    releaseJniEnv(attached_thread);
  }
}

std::unique_ptr<char[]> LoggerJniCallback::format_str(const char* format,
                                                      va_list ap) const {
  va_list ap_copy;

  va_copy(ap_copy, ap);
  const size_t required =
      vsnprintf(nullptr, 0, format, ap_copy) + 1;  // Extra space for '\0'
  va_end(ap_copy);

  std::unique_ptr<char[]> buf(new char[required]);

  va_copy(ap_copy, ap);
  vsnprintf(buf.get(), required, format, ap_copy);
  va_end(ap_copy);

  return buf;
}
LoggerJniCallback::~LoggerJniCallback() {
  jboolean attached_thread = JNI_FALSE;
  JNIEnv* env = getJniEnv(&attached_thread);
  assert(env != nullptr);

  if (m_jdebug_level != nullptr) {
    env->DeleteGlobalRef(m_jdebug_level);
  }

  if (m_jinfo_level != nullptr) {
    env->DeleteGlobalRef(m_jinfo_level);
  }

  if (m_jwarn_level != nullptr) {
    env->DeleteGlobalRef(m_jwarn_level);
  }

  if (m_jerror_level != nullptr) {
    env->DeleteGlobalRef(m_jerror_level);
  }

  if (m_jfatal_level != nullptr) {
    env->DeleteGlobalRef(m_jfatal_level);
  }

  if (m_jheader_level != nullptr) {
    env->DeleteGlobalRef(m_jheader_level);
  }

  releaseJniEnv(attached_thread);
}

}  // namespace rocksdb

/*
 * Class:     org_rocksdb_Logger
 * Method:    createNewLoggerOptions
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Logger_createNewLoggerOptions(JNIEnv* env, jobject jobj,
                                                     jlong joptions) {
  auto* sptr_logger = new std::shared_ptr<rocksdb::LoggerJniCallback>(
      new rocksdb::LoggerJniCallback(env, jobj));

  // set log level
  auto* options = reinterpret_cast<rocksdb::Options*>(joptions);
  sptr_logger->get()->SetInfoLogLevel(options->info_log_level);

  return reinterpret_cast<jlong>(sptr_logger);
}

/*
 * Class:     org_rocksdb_Logger
 * Method:    createNewLoggerDbOptions
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Logger_createNewLoggerDbOptions(JNIEnv* env,
                                                       jobject jobj,
                                                       jlong jdb_options) {
  auto* sptr_logger = new std::shared_ptr<rocksdb::LoggerJniCallback>(
      new rocksdb::LoggerJniCallback(env, jobj));

  // set log level
  auto* db_options = reinterpret_cast<rocksdb::DBOptions*>(jdb_options);
  sptr_logger->get()->SetInfoLogLevel(db_options->info_log_level);

  return reinterpret_cast<jlong>(sptr_logger);
}

/*
 * Class:     org_rocksdb_Logger
 * Method:    setInfoLogLevel
 * Signature: (JB)V
 */
void Java_org_rocksdb_Logger_setInfoLogLevel(JNIEnv* /*env*/, jobject /*jobj*/,
                                             jlong jhandle, jbyte jlog_level) {
  auto* handle =
      reinterpret_cast<std::shared_ptr<rocksdb::LoggerJniCallback>*>(jhandle);
  handle->get()->SetInfoLogLevel(
      static_cast<rocksdb::InfoLogLevel>(jlog_level));
}

/*
 * Class:     org_rocksdb_Logger
 * Method:    infoLogLevel
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_Logger_infoLogLevel(JNIEnv* /*env*/, jobject /*jobj*/,
                                           jlong jhandle) {
  auto* handle =
      reinterpret_cast<std::shared_ptr<rocksdb::LoggerJniCallback>*>(jhandle);
  return static_cast<jbyte>(handle->get()->GetInfoLogLevel());
}

/*
 * Class:     org_rocksdb_Logger
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_Logger_disposeInternal(JNIEnv* /*env*/, jobject /*jobj*/,
                                             jlong jhandle) {
  auto* handle =
      reinterpret_cast<std::shared_ptr<rocksdb::LoggerJniCallback>*>(jhandle);
  delete handle;  // delete std::shared_ptr
}
