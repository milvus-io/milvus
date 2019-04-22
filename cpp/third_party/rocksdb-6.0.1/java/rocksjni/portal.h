// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// This file is designed for caching those frequently used IDs and provide
// efficient portal (i.e, a set of static functions) to access java code
// from c++.

#ifndef JAVA_ROCKSJNI_PORTAL_H_
#define JAVA_ROCKSJNI_PORTAL_H_

#include <algorithm>
#include <cstring>
#include <functional>
#include <iostream>
#include <iterator>
#include <jni.h>
#include <limits>
#include <memory>
#include <string>
#include <type_traits>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "rocksdb/utilities/backupable_db.h"
#include "rocksdb/utilities/memory_util.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/utilities/write_batch_with_index.h"
#include "rocksjni/compaction_filter_factory_jnicallback.h"
#include "rocksjni/comparatorjnicallback.h"
#include "rocksjni/loggerjnicallback.h"
#include "rocksjni/table_filter_jnicallback.h"
#include "rocksjni/trace_writer_jnicallback.h"
#include "rocksjni/transaction_notifier_jnicallback.h"
#include "rocksjni/wal_filter_jnicallback.h"
#include "rocksjni/writebatchhandlerjnicallback.h"

// Remove macro on windows
#ifdef DELETE
#undef DELETE
#endif

namespace rocksdb {

class JavaClass {
 public:
  /**
   * Gets and initializes a Java Class
   *
   * @param env A pointer to the Java environment
   * @param jclazz_name The fully qualified JNI name of the Java Class
   *     e.g. "java/lang/String"
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env, const char* jclazz_name) {
    jclass jclazz = env->FindClass(jclazz_name);
    assert(jclazz != nullptr);
    return jclazz;
  }
};

// Native class template
template<class PTR, class DERIVED> class RocksDBNativeClass : public JavaClass {
};

// Native class template for sub-classes of RocksMutableObject
template<class PTR, class DERIVED> class NativeRocksMutableObject
    : public RocksDBNativeClass<PTR, DERIVED> {
 public:

  /**
   * Gets the Java Method ID for the
   * RocksMutableObject#setNativeHandle(long, boolean) method
   *
   * @param env A pointer to the Java environment
   * @return The Java Method ID or nullptr the RocksMutableObject class cannot
   *     be accessed, or if one of the NoSuchMethodError,
   *     ExceptionInInitializerError or OutOfMemoryError exceptions is thrown
   */
  static jmethodID getSetNativeHandleMethod(JNIEnv* env) {
    static jclass jclazz = DERIVED::getJClass(env);
    if(jclazz == nullptr) {
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(
        jclazz, "setNativeHandle", "(JZ)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Sets the C++ object pointer handle in the Java object
   *
   * @param env A pointer to the Java environment
   * @param jobj The Java object on which to set the pointer handle
   * @param ptr The C++ object pointer
   * @param java_owns_handle JNI_TRUE if ownership of the C++ object is
   *     managed by the Java object
   *
   * @return true if a Java exception is pending, false otherwise
   */
  static bool setHandle(JNIEnv* env, jobject jobj, PTR ptr,
      jboolean java_owns_handle) {
    assert(jobj != nullptr);
    static jmethodID mid = getSetNativeHandleMethod(env);
    if(mid == nullptr) {
      return true;  // signal exception
    }

    env->CallVoidMethod(jobj, mid, reinterpret_cast<jlong>(ptr), java_owns_handle);
    if(env->ExceptionCheck()) {
      return true;  // signal exception
    }

    return false;
  }
};

// Java Exception template
template<class DERIVED> class JavaException : public JavaClass {
 public:
  /**
   * Create and throw a java exception with the provided message
   *
   * @param env A pointer to the Java environment
   * @param msg The message for the exception
   *
   * @return true if an exception was thrown, false otherwise
   */
  static bool ThrowNew(JNIEnv* env, const std::string& msg) {
    jclass jclazz = DERIVED::getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      std::cerr << "JavaException::ThrowNew - Error: unexpected exception!" << std::endl;
      return env->ExceptionCheck();
    }

    const jint rs = env->ThrowNew(jclazz, msg.c_str());
    if(rs != JNI_OK) {
      // exception could not be thrown
      std::cerr << "JavaException::ThrowNew - Fatal: could not throw exception!" << std::endl;
      return env->ExceptionCheck();
    }

    return true;
  }
};

// The portal class for java.lang.IllegalArgumentException
class IllegalArgumentExceptionJni :
    public JavaException<IllegalArgumentExceptionJni> {
 public:
  /**
   * Get the Java Class java.lang.IllegalArgumentException
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaException::getJClass(env, "java/lang/IllegalArgumentException");
  }

  /**
   * Create and throw a Java IllegalArgumentException with the provided status
   *
   * If s.ok() == true, then this function will not throw any exception.
   *
   * @param env A pointer to the Java environment
   * @param s The status for the exception
   *
   * @return true if an exception was thrown, false otherwise
   */
  static bool ThrowNew(JNIEnv* env, const Status& s) {
    assert(!s.ok());
    if (s.ok()) {
      return false;
    }

    // get the IllegalArgumentException class
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      std::cerr << "IllegalArgumentExceptionJni::ThrowNew/class - Error: unexpected exception!" << std::endl;
      return env->ExceptionCheck();
    }

    return JavaException::ThrowNew(env, s.ToString());
  }
};

// The portal class for org.rocksdb.Status.Code
class CodeJni : public JavaClass {
 public:
  /**
   * Get the Java Class org.rocksdb.Status.Code
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/Status$Code");
  }

  /**
   * Get the Java Method: Status.Code#getValue
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getValueMethod(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid =
        env->GetMethodID(jclazz, "getValue", "()b");
    assert(mid != nullptr);
    return mid;
  }
};

// The portal class for org.rocksdb.Status.SubCode
class SubCodeJni : public JavaClass {
 public:
  /**
   * Get the Java Class org.rocksdb.Status.SubCode
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/Status$SubCode");
  }

  /**
   * Get the Java Method: Status.SubCode#getValue
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getValueMethod(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid =
        env->GetMethodID(jclazz, "getValue", "()b");
    assert(mid != nullptr);
    return mid;
  }

  static rocksdb::Status::SubCode toCppSubCode(const jbyte jsub_code) {
    switch (jsub_code) {
      case 0x0:
        return rocksdb::Status::SubCode::kNone;
      case 0x1:
        return rocksdb::Status::SubCode::kMutexTimeout;
      case 0x2:
        return rocksdb::Status::SubCode::kLockTimeout;
      case 0x3:
        return rocksdb::Status::SubCode::kLockLimit;
      case 0x4:
        return rocksdb::Status::SubCode::kNoSpace;
      case 0x5:
        return rocksdb::Status::SubCode::kDeadlock;
      case 0x6:
        return rocksdb::Status::SubCode::kStaleFile;
      case 0x7:
        return rocksdb::Status::SubCode::kMemoryLimit;

      case 0x7F:
      default:
        return rocksdb::Status::SubCode::kNone;
    }
  }
};

// The portal class for org.rocksdb.Status
class StatusJni : public RocksDBNativeClass<rocksdb::Status*, StatusJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.Status
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/Status");
  }

  /**
   * Get the Java Method: Status#getCode
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getCodeMethod(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid =
        env->GetMethodID(jclazz, "getCode", "()Lorg/rocksdb/Status$Code;");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: Status#getSubCode
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getSubCodeMethod(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid =
        env->GetMethodID(jclazz, "getSubCode", "()Lorg/rocksdb/Status$SubCode;");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: Status#getState
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getStateMethod(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid =
        env->GetMethodID(jclazz, "getState", "()Ljava/lang/String;");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Create a new Java org.rocksdb.Status object with the same properties as
   * the provided C++ rocksdb::Status object
   *
   * @param env A pointer to the Java environment
   * @param status The rocksdb::Status object
   *
   * @return A reference to a Java org.rocksdb.Status object, or nullptr
   *     if an an exception occurs
   */
  static jobject construct(JNIEnv* env, const Status& status) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID mid =
        env->GetMethodID(jclazz, "<init>", "(BBLjava/lang/String;)V");
    if(mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    // convert the Status state for Java
    jstring jstate = nullptr;
    if (status.getState() != nullptr) {
      const char* const state = status.getState();
      jstate = env->NewStringUTF(state);
      if(env->ExceptionCheck()) {
        if(jstate != nullptr) {
          env->DeleteLocalRef(jstate);
        }
        return nullptr;
      }
    }

    jobject jstatus =
        env->NewObject(jclazz, mid, toJavaStatusCode(status.code()),
            toJavaStatusSubCode(status.subcode()), jstate);
    if(env->ExceptionCheck()) {
      // exception occurred
      if(jstate != nullptr) {
        env->DeleteLocalRef(jstate);
      }
      return nullptr;
    }

    if(jstate != nullptr) {
      env->DeleteLocalRef(jstate);
    }

    return jstatus;
  }

  // Returns the equivalent org.rocksdb.Status.Code for the provided
  // C++ rocksdb::Status::Code enum
  static jbyte toJavaStatusCode(const rocksdb::Status::Code& code) {
    switch (code) {
      case rocksdb::Status::Code::kOk:
        return 0x0;
      case rocksdb::Status::Code::kNotFound:
        return 0x1;
      case rocksdb::Status::Code::kCorruption:
        return 0x2;
      case rocksdb::Status::Code::kNotSupported:
        return 0x3;
      case rocksdb::Status::Code::kInvalidArgument:
        return 0x4;
      case rocksdb::Status::Code::kIOError:
        return 0x5;
      case rocksdb::Status::Code::kMergeInProgress:
        return 0x6;
      case rocksdb::Status::Code::kIncomplete:
        return 0x7;
      case rocksdb::Status::Code::kShutdownInProgress:
        return 0x8;
      case rocksdb::Status::Code::kTimedOut:
        return 0x9;
      case rocksdb::Status::Code::kAborted:
        return 0xA;
      case rocksdb::Status::Code::kBusy:
        return 0xB;
      case rocksdb::Status::Code::kExpired:
        return 0xC;
      case rocksdb::Status::Code::kTryAgain:
        return 0xD;
      default:
        return 0x7F;  // undefined
    }
  }

  // Returns the equivalent org.rocksdb.Status.SubCode for the provided
  // C++ rocksdb::Status::SubCode enum
  static jbyte toJavaStatusSubCode(const rocksdb::Status::SubCode& subCode) {
    switch (subCode) {
      case rocksdb::Status::SubCode::kNone:
        return 0x0;
      case rocksdb::Status::SubCode::kMutexTimeout:
        return 0x1;
      case rocksdb::Status::SubCode::kLockTimeout:
        return 0x2;
      case rocksdb::Status::SubCode::kLockLimit:
        return 0x3;
      case rocksdb::Status::SubCode::kNoSpace:
        return 0x4;
      case rocksdb::Status::SubCode::kDeadlock:
        return 0x5;
      case rocksdb::Status::SubCode::kStaleFile:
        return 0x6;
      case rocksdb::Status::SubCode::kMemoryLimit:
        return 0x7;
      default:
        return 0x7F;  // undefined
    }
  }

  static std::unique_ptr<rocksdb::Status> toCppStatus(
      const jbyte jcode_value, const jbyte jsub_code_value) {
    std::unique_ptr<rocksdb::Status> status;
    switch (jcode_value) {
      case 0x0:
        //Ok
        status = std::unique_ptr<rocksdb::Status>(
            new rocksdb::Status(rocksdb::Status::OK()));
        break;
      case 0x1:
        //NotFound
        status = std::unique_ptr<rocksdb::Status>(
            new rocksdb::Status(rocksdb::Status::NotFound(
              rocksdb::SubCodeJni::toCppSubCode(jsub_code_value))));
        break;
      case 0x2:
        //Corruption
        status = std::unique_ptr<rocksdb::Status>(
            new rocksdb::Status(rocksdb::Status::Corruption(
              rocksdb::SubCodeJni::toCppSubCode(jsub_code_value))));
        break;
      case 0x3:
        //NotSupported
        status = std::unique_ptr<rocksdb::Status>(
            new rocksdb::Status(rocksdb::Status::NotSupported(
                rocksdb::SubCodeJni::toCppSubCode(jsub_code_value))));
        break;
      case 0x4:
        //InvalidArgument
        status = std::unique_ptr<rocksdb::Status>(
            new rocksdb::Status(rocksdb::Status::InvalidArgument(
              rocksdb::SubCodeJni::toCppSubCode(jsub_code_value))));
        break;
      case 0x5:
        //IOError
        status = std::unique_ptr<rocksdb::Status>(
            new rocksdb::Status(rocksdb::Status::IOError(
              rocksdb::SubCodeJni::toCppSubCode(jsub_code_value))));
        break;
      case 0x6:
        //MergeInProgress
        status = std::unique_ptr<rocksdb::Status>(
            new rocksdb::Status(rocksdb::Status::MergeInProgress(
              rocksdb::SubCodeJni::toCppSubCode(jsub_code_value))));
        break;
      case 0x7:
        //Incomplete
        status = std::unique_ptr<rocksdb::Status>(
            new rocksdb::Status(rocksdb::Status::Incomplete(
              rocksdb::SubCodeJni::toCppSubCode(jsub_code_value))));
        break;
      case 0x8:
        //ShutdownInProgress
        status = std::unique_ptr<rocksdb::Status>(
            new rocksdb::Status(rocksdb::Status::ShutdownInProgress(
              rocksdb::SubCodeJni::toCppSubCode(jsub_code_value))));
        break;
      case 0x9:
        //TimedOut
        status = std::unique_ptr<rocksdb::Status>(
            new rocksdb::Status(rocksdb::Status::TimedOut(
              rocksdb::SubCodeJni::toCppSubCode(jsub_code_value))));
        break;
      case 0xA:
        //Aborted
        status = std::unique_ptr<rocksdb::Status>(
            new rocksdb::Status(rocksdb::Status::Aborted(
              rocksdb::SubCodeJni::toCppSubCode(jsub_code_value))));
        break;
      case 0xB:
        //Busy
        status = std::unique_ptr<rocksdb::Status>(
            new rocksdb::Status(rocksdb::Status::Busy(
              rocksdb::SubCodeJni::toCppSubCode(jsub_code_value))));
        break;
      case 0xC:
        //Expired
        status = std::unique_ptr<rocksdb::Status>(
            new rocksdb::Status(rocksdb::Status::Expired(
              rocksdb::SubCodeJni::toCppSubCode(jsub_code_value))));
        break;
      case 0xD:
        //TryAgain
        status = std::unique_ptr<rocksdb::Status>(
            new rocksdb::Status(rocksdb::Status::TryAgain(
              rocksdb::SubCodeJni::toCppSubCode(jsub_code_value))));
        break;
      case 0x7F:
      default:
        return nullptr;
    }
    return status;
  }

  // Returns the equivalent rocksdb::Status for the Java org.rocksdb.Status
  static std::unique_ptr<rocksdb::Status> toCppStatus(JNIEnv* env, const jobject jstatus) {
    jmethodID mid_code = getCodeMethod(env);
    if (mid_code == nullptr) {
      // exception occurred
      return nullptr;
    }
    jobject jcode = env->CallObjectMethod(jstatus, mid_code);
    if (env->ExceptionCheck()) {
      // exception occurred
      return nullptr;
    }

    jmethodID mid_code_value = rocksdb::CodeJni::getValueMethod(env);
    if (mid_code_value == nullptr) {
      // exception occurred
      return nullptr;
    }
    jbyte jcode_value = env->CallByteMethod(jcode, mid_code_value);
    if (env->ExceptionCheck()) {
      // exception occurred
      if (jcode != nullptr) {
        env->DeleteLocalRef(jcode);
      }
      return nullptr;
    }

    jmethodID mid_subCode = getSubCodeMethod(env);
    if (mid_subCode == nullptr) {
      // exception occurred
      return nullptr;
    }
    jobject jsubCode = env->CallObjectMethod(jstatus, mid_subCode);
    if (env->ExceptionCheck()) {
      // exception occurred
      if (jcode != nullptr) {
        env->DeleteLocalRef(jcode);
      }
      return nullptr;
    }

    jbyte jsub_code_value = 0x0;  // None
    if (jsubCode != nullptr) {
      jmethodID mid_subCode_value = rocksdb::SubCodeJni::getValueMethod(env);
      if (mid_subCode_value == nullptr) {
        // exception occurred
        return nullptr;
      }
      jsub_code_value = env->CallByteMethod(jsubCode, mid_subCode_value);
      if (env->ExceptionCheck()) {
        // exception occurred
        if (jcode != nullptr) {
          env->DeleteLocalRef(jcode);
        }
        return nullptr;
      }
    }

    jmethodID mid_state = getStateMethod(env);
    if (mid_state == nullptr) {
      // exception occurred
      return nullptr;
    }
    jobject jstate = env->CallObjectMethod(jstatus, mid_state);
    if (env->ExceptionCheck()) {
      // exception occurred
      if (jsubCode != nullptr) {
        env->DeleteLocalRef(jsubCode);
      }
      if (jcode != nullptr) {
        env->DeleteLocalRef(jcode);
      }
      return nullptr;
    }

    std::unique_ptr<rocksdb::Status> status =
        toCppStatus(jcode_value, jsub_code_value);

    // delete all local refs
    if (jstate != nullptr) {
      env->DeleteLocalRef(jstate);
    }
    if (jsubCode != nullptr) {
      env->DeleteLocalRef(jsubCode);
    }
    if (jcode != nullptr) {
      env->DeleteLocalRef(jcode);
    }

    return status;
  }
};

// The portal class for org.rocksdb.RocksDBException
class RocksDBExceptionJni :
    public JavaException<RocksDBExceptionJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.RocksDBException
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaException::getJClass(env, "org/rocksdb/RocksDBException");
  }

  /**
   * Create and throw a Java RocksDBException with the provided message
   *
   * @param env A pointer to the Java environment
   * @param msg The message for the exception
   *
   * @return true if an exception was thrown, false otherwise
   */
  static bool ThrowNew(JNIEnv* env, const std::string& msg) {
    return JavaException::ThrowNew(env, msg);
  }

  /**
   * Create and throw a Java RocksDBException with the provided status
   *
   * If s->ok() == true, then this function will not throw any exception.
   *
   * @param env A pointer to the Java environment
   * @param s The status for the exception
   *
   * @return true if an exception was thrown, false otherwise
   */
  static bool ThrowNew(JNIEnv* env, std::unique_ptr<Status>& s) {
    return rocksdb::RocksDBExceptionJni::ThrowNew(env, *(s.get()));
  }

  /**
   * Create and throw a Java RocksDBException with the provided status
   *
   * If s.ok() == true, then this function will not throw any exception.
   *
   * @param env A pointer to the Java environment
   * @param s The status for the exception
   *
   * @return true if an exception was thrown, false otherwise
   */
  static bool ThrowNew(JNIEnv* env, const Status& s) {
    if (s.ok()) {
      return false;
    }

    // get the RocksDBException class
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      std::cerr << "RocksDBExceptionJni::ThrowNew/class - Error: unexpected exception!" << std::endl;
      return env->ExceptionCheck();
    }

    // get the constructor of org.rocksdb.RocksDBException
    jmethodID mid =
        env->GetMethodID(jclazz, "<init>", "(Lorg/rocksdb/Status;)V");
    if(mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      std::cerr << "RocksDBExceptionJni::ThrowNew/cstr - Error: unexpected exception!" << std::endl;
      return env->ExceptionCheck();
    }

    // get the Java status object
    jobject jstatus = StatusJni::construct(env, s);
    if(jstatus == nullptr) {
      // exception occcurred
      std::cerr << "RocksDBExceptionJni::ThrowNew/StatusJni - Error: unexpected exception!" << std::endl;
      return env->ExceptionCheck();
    }

    // construct the RocksDBException
    jthrowable rocksdb_exception = reinterpret_cast<jthrowable>(env->NewObject(jclazz, mid, jstatus));
    if(env->ExceptionCheck()) {
      if(jstatus != nullptr) {
        env->DeleteLocalRef(jstatus);
      }
      if(rocksdb_exception != nullptr) {
        env->DeleteLocalRef(rocksdb_exception);
      }
      std::cerr << "RocksDBExceptionJni::ThrowNew/NewObject - Error: unexpected exception!" << std::endl;
      return true;
    }

    // throw the RocksDBException
    const jint rs = env->Throw(rocksdb_exception);
    if(rs != JNI_OK) {
      // exception could not be thrown
      std::cerr << "RocksDBExceptionJni::ThrowNew - Fatal: could not throw exception!" << std::endl;
      if(jstatus != nullptr) {
        env->DeleteLocalRef(jstatus);
      }
      if(rocksdb_exception != nullptr) {
        env->DeleteLocalRef(rocksdb_exception);
      }
      return env->ExceptionCheck();
    }

    if(jstatus != nullptr) {
      env->DeleteLocalRef(jstatus);
    }
    if(rocksdb_exception != nullptr) {
      env->DeleteLocalRef(rocksdb_exception);
    }

    return true;
  }

  /**
   * Create and throw a Java RocksDBException with the provided message
   * and status
   *
   * If s.ok() == true, then this function will not throw any exception.
   *
   * @param env A pointer to the Java environment
   * @param msg The message for the exception
   * @param s The status for the exception
   *
   * @return true if an exception was thrown, false otherwise
   */
  static bool ThrowNew(JNIEnv* env, const std::string& msg, const Status& s) {
    assert(!s.ok());
    if (s.ok()) {
      return false;
    }

    // get the RocksDBException class
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      std::cerr << "RocksDBExceptionJni::ThrowNew/class - Error: unexpected exception!" << std::endl;
      return env->ExceptionCheck();
    }

    // get the constructor of org.rocksdb.RocksDBException
    jmethodID mid =
        env->GetMethodID(jclazz, "<init>", "(Ljava/lang/String;Lorg/rocksdb/Status;)V");
    if(mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      std::cerr << "RocksDBExceptionJni::ThrowNew/cstr - Error: unexpected exception!" << std::endl;
      return env->ExceptionCheck();
    }

    jstring jmsg = env->NewStringUTF(msg.c_str());
    if(jmsg == nullptr) {
      // exception thrown: OutOfMemoryError
      std::cerr << "RocksDBExceptionJni::ThrowNew/msg - Error: unexpected exception!" << std::endl;
      return env->ExceptionCheck();
    }

    // get the Java status object
    jobject jstatus = StatusJni::construct(env, s);
    if(jstatus == nullptr) {
      // exception occcurred
      std::cerr << "RocksDBExceptionJni::ThrowNew/StatusJni - Error: unexpected exception!" << std::endl;
      if(jmsg != nullptr) {
        env->DeleteLocalRef(jmsg);
      }
      return env->ExceptionCheck();
    }

    // construct the RocksDBException
    jthrowable rocksdb_exception = reinterpret_cast<jthrowable>(env->NewObject(jclazz, mid, jmsg, jstatus));
    if(env->ExceptionCheck()) {
      if(jstatus != nullptr) {
        env->DeleteLocalRef(jstatus);
      }
      if(jmsg != nullptr) {
        env->DeleteLocalRef(jmsg);
      }
      if(rocksdb_exception != nullptr) {
        env->DeleteLocalRef(rocksdb_exception);
      }
      std::cerr << "RocksDBExceptionJni::ThrowNew/NewObject - Error: unexpected exception!" << std::endl;
      return true;
    }

    // throw the RocksDBException
    const jint rs = env->Throw(rocksdb_exception);
    if(rs != JNI_OK) {
      // exception could not be thrown
      std::cerr << "RocksDBExceptionJni::ThrowNew - Fatal: could not throw exception!" << std::endl;
      if(jstatus != nullptr) {
        env->DeleteLocalRef(jstatus);
      }
      if(jmsg != nullptr) {
        env->DeleteLocalRef(jmsg);
      }
      if(rocksdb_exception != nullptr) {
        env->DeleteLocalRef(rocksdb_exception);
      }
      return env->ExceptionCheck();
    }

    if(jstatus != nullptr) {
      env->DeleteLocalRef(jstatus);
    }
    if(jmsg != nullptr) {
      env->DeleteLocalRef(jmsg);
    }
    if(rocksdb_exception != nullptr) {
      env->DeleteLocalRef(rocksdb_exception);
    }

    return true;
  }

  /**
   * Get the Java Method: RocksDBException#getStatus
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getStatusMethod(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid =
        env->GetMethodID(jclazz, "getStatus", "()Lorg/rocksdb/Status;");
    assert(mid != nullptr);
    return mid;
  }

  static std::unique_ptr<rocksdb::Status> toCppStatus(
      JNIEnv* env, jthrowable jrocksdb_exception) {
    if(!env->IsInstanceOf(jrocksdb_exception, getJClass(env))) {
      // not an instance of RocksDBException
      return nullptr;
    }

    // get the java status object
    jmethodID mid = getStatusMethod(env);
    if(mid == nullptr) {
      // exception occurred accessing class or method
      return nullptr;
    }

    jobject jstatus = env->CallObjectMethod(jrocksdb_exception, mid);
    if(env->ExceptionCheck()) {
      // exception occurred
      return nullptr;
    }

    if(jstatus == nullptr) {
      return nullptr;   // no status available
    }

    return rocksdb::StatusJni::toCppStatus(env, jstatus);
  }
};

// The portal class for java.util.List
class ListJni : public JavaClass {
 public:
  /**
   * Get the Java Class java.util.List
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getListClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "java/util/List");
  }

  /**
   * Get the Java Class java.util.ArrayList
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getArrayListClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "java/util/ArrayList");
  }

  /**
   * Get the Java Class java.util.Iterator
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getIteratorClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "java/util/Iterator");
  }

  /**
   * Get the Java Method: List#iterator
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getIteratorMethod(JNIEnv* env) {
    jclass jlist_clazz = getListClass(env);
    if(jlist_clazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid =
        env->GetMethodID(jlist_clazz, "iterator", "()Ljava/util/Iterator;");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: Iterator#hasNext
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getHasNextMethod(JNIEnv* env) {
    jclass jiterator_clazz = getIteratorClass(env);
    if(jiterator_clazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jiterator_clazz, "hasNext", "()Z");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: Iterator#next
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getNextMethod(JNIEnv* env) {
    jclass jiterator_clazz = getIteratorClass(env);
    if(jiterator_clazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid =
        env->GetMethodID(jiterator_clazz, "next", "()Ljava/lang/Object;");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: ArrayList constructor
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getArrayListConstructorMethodId(JNIEnv* env) {
    jclass jarray_list_clazz = getArrayListClass(env);
    if(jarray_list_clazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }
    static jmethodID mid =
        env->GetMethodID(jarray_list_clazz, "<init>", "(I)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: List#add
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getListAddMethodId(JNIEnv* env) {
    jclass jlist_clazz = getListClass(env);
    if(jlist_clazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid =
        env->GetMethodID(jlist_clazz, "add", "(Ljava/lang/Object;)Z");
    assert(mid != nullptr);
    return mid;
  }
};

// The portal class for java.lang.Byte
class ByteJni : public JavaClass {
 public:
  /**
   * Get the Java Class java.lang.Byte
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "java/lang/Byte");
  }

  /**
   * Get the Java Class byte[]
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getArrayJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "[B");
  }

  /**
   * Creates a new 2-dimensional Java Byte Array byte[][]
   *
   * @param env A pointer to the Java environment
   * @param len The size of the first dimension
   *
   * @return A reference to the Java byte[][] or nullptr if an exception occurs
   */
  static jobjectArray new2dByteArray(JNIEnv* env, const jsize len) {
    jclass clazz = getArrayJClass(env);
    if(clazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    return env->NewObjectArray(len, clazz, nullptr);
  }

  /**
   * Get the Java Method: Byte#byteValue
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getByteValueMethod(JNIEnv* env) {
    jclass clazz = getJClass(env);
    if(clazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(clazz, "byteValue", "()B");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Calls the Java Method: Byte#valueOf, returning a constructed Byte jobject
   *
   * @param env A pointer to the Java environment
   *
   * @return A constructing Byte object or nullptr if the class or method id could not
   *     be retrieved, or an exception occurred
   */
  static jobject valueOf(JNIEnv* env, jbyte jprimitive_byte) {
    jclass clazz = getJClass(env);
    if (clazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid =
        env->GetStaticMethodID(clazz, "valueOf", "(B)Ljava/lang/Byte;");
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    const jobject jbyte_obj =
        env->CallStaticObjectMethod(clazz, mid, jprimitive_byte);
    if (env->ExceptionCheck()) {
      // exception occurred
      return nullptr;
    }

    return jbyte_obj;
  }

};

// The portal class for java.lang.Integer
class IntegerJni : public JavaClass {
 public:
  /**
   * Get the Java Class java.lang.Integer
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "java/lang/Integer");
  }

  static jobject valueOf(JNIEnv* env, jint jprimitive_int) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID mid =
        env->GetStaticMethodID(jclazz, "valueOf", "(I)Ljava/lang/Integer;");
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    const jobject jinteger_obj =
        env->CallStaticObjectMethod(jclazz, mid, jprimitive_int);
    if (env->ExceptionCheck()) {
      // exception occurred
      return nullptr;
    }

    return jinteger_obj;
  }
};

// The portal class for java.lang.Long
class LongJni : public JavaClass {
 public:
  /**
   * Get the Java Class java.lang.Long
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "java/lang/Long");
  }

  static jobject valueOf(JNIEnv* env, jlong jprimitive_long) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID mid =
        env->GetStaticMethodID(jclazz, "valueOf", "(J)Ljava/lang/Long;");
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    const jobject jlong_obj =
        env->CallStaticObjectMethod(jclazz, mid, jprimitive_long);
    if (env->ExceptionCheck()) {
      // exception occurred
      return nullptr;
    }

    return jlong_obj;
  }
};

// The portal class for java.lang.StringBuilder
class StringBuilderJni : public JavaClass {
  public:
  /**
   * Get the Java Class java.lang.StringBuilder
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "java/lang/StringBuilder");
  }

  /**
   * Get the Java Method: StringBuilder#append
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getListAddMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid =
        env->GetMethodID(jclazz, "append",
            "(Ljava/lang/String;)Ljava/lang/StringBuilder;");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Appends a C-style string to a StringBuilder
   *
   * @param env A pointer to the Java environment
   * @param jstring_builder Reference to a java.lang.StringBuilder
   * @param c_str A C-style string to append to the StringBuilder
   *
   * @return A reference to the updated StringBuilder, or a nullptr if
   *     an exception occurs
   */
  static jobject append(JNIEnv* env, jobject jstring_builder,
      const char* c_str) {
    jmethodID mid = getListAddMethodId(env);
    if(mid == nullptr) {
      // exception occurred accessing class or method
      return nullptr;
    }

    jstring new_value_str = env->NewStringUTF(c_str);
    if(new_value_str == nullptr) {
      // exception thrown: OutOfMemoryError
      return nullptr;
    }

    jobject jresult_string_builder =
        env->CallObjectMethod(jstring_builder, mid, new_value_str);
    if(env->ExceptionCheck()) {
      // exception occurred
      env->DeleteLocalRef(new_value_str);
      return nullptr;
    }

    return jresult_string_builder;
  }
};

// various utility functions for working with RocksDB and JNI
class JniUtil {
 public:
    /**
     * Detect if jlong overflows size_t
     * 
     * @param jvalue the jlong value
     * 
     * @return
     */
    inline static Status check_if_jlong_fits_size_t(const jlong& jvalue) {
      Status s = Status::OK();
      if (static_cast<uint64_t>(jvalue) > std::numeric_limits<size_t>::max()) {
        s = Status::InvalidArgument(Slice("jlong overflows 32 bit value."));
      }
      return s;
    }

    /**
     * Obtains a reference to the JNIEnv from
     * the JVM
     *
     * If the current thread is not attached to the JavaVM
     * then it will be attached so as to retrieve the JNIEnv
     *
     * If a thread is attached, it must later be manually
     * released by calling JavaVM::DetachCurrentThread.
     * This can be handled by always matching calls to this
     * function with calls to {@link JniUtil::releaseJniEnv(JavaVM*, jboolean)}
     *
     * @param jvm (IN) A pointer to the JavaVM instance
     * @param attached (OUT) A pointer to a boolean which
     *     will be set to JNI_TRUE if we had to attach the thread
     *
     * @return A pointer to the JNIEnv or nullptr if a fatal error
     *     occurs and the JNIEnv cannot be retrieved
     */
    static JNIEnv* getJniEnv(JavaVM* jvm, jboolean* attached) {
      assert(jvm != nullptr);

      JNIEnv *env;
      const jint env_rs = jvm->GetEnv(reinterpret_cast<void**>(&env),
          JNI_VERSION_1_2);

      if(env_rs == JNI_OK) {
        // current thread is already attached, return the JNIEnv
        *attached = JNI_FALSE;
        return env;
      } else if(env_rs == JNI_EDETACHED) {
        // current thread is not attached, attempt to attach
        const jint rs_attach = jvm->AttachCurrentThread(reinterpret_cast<void**>(&env), NULL);
        if(rs_attach == JNI_OK) {
          *attached = JNI_TRUE;
          return env;
        } else {
          // error, could not attach the thread
          std::cerr << "JniUtil::getJniEnv - Fatal: could not attach current thread to JVM!" << std::endl;
          return nullptr;
        }
      } else if(env_rs == JNI_EVERSION) {
        // error, JDK does not support JNI_VERSION_1_2+
        std::cerr << "JniUtil::getJniEnv - Fatal: JDK does not support JNI_VERSION_1_2" << std::endl;
        return nullptr;
      } else {
        std::cerr << "JniUtil::getJniEnv - Fatal: Unknown error: env_rs=" << env_rs << std::endl;
        return nullptr;
      }
    }

    /**
     * Counterpart to {@link JniUtil::getJniEnv(JavaVM*, jboolean*)}
     *
     * Detachess the current thread from the JVM if it was previously
     * attached
     *
     * @param jvm (IN) A pointer to the JavaVM instance
     * @param attached (IN) JNI_TRUE if we previously had to attach the thread
     *     to the JavaVM to get the JNIEnv
     */
    static void releaseJniEnv(JavaVM* jvm, jboolean& attached) {
      assert(jvm != nullptr);
      if(attached == JNI_TRUE) {
        const jint rs_detach = jvm->DetachCurrentThread();
        assert(rs_detach == JNI_OK);
        if(rs_detach != JNI_OK) {
          std::cerr << "JniUtil::getJniEnv - Warn: Unable to detach current thread from JVM!" << std::endl;
        }
      }
    }

    /**
     * Copies a Java String[] to a C++ std::vector<std::string>
     *
     * @param env (IN) A pointer to the java environment
     * @param jss (IN) The Java String array to copy
     * @param has_exception (OUT) will be set to JNI_TRUE
     *     if an OutOfMemoryError or ArrayIndexOutOfBoundsException
     *     exception occurs
     *
     * @return A std::vector<std:string> containing copies of the Java strings
     */
    static std::vector<std::string> copyStrings(JNIEnv* env,
        jobjectArray jss, jboolean* has_exception) {
          return rocksdb::JniUtil::copyStrings(env, jss,
              env->GetArrayLength(jss), has_exception);
    }

    /**
     * Copies a Java String[] to a C++ std::vector<std::string>
     *
     * @param env (IN) A pointer to the java environment
     * @param jss (IN) The Java String array to copy
     * @param jss_len (IN) The length of the Java String array to copy
     * @param has_exception (OUT) will be set to JNI_TRUE
     *     if an OutOfMemoryError or ArrayIndexOutOfBoundsException
     *     exception occurs
     *
     * @return A std::vector<std:string> containing copies of the Java strings
     */
    static std::vector<std::string> copyStrings(JNIEnv* env,
        jobjectArray jss, const jsize jss_len, jboolean* has_exception) {
      std::vector<std::string> strs;
      strs.reserve(jss_len);
      for (jsize i = 0; i < jss_len; i++) {
        jobject js = env->GetObjectArrayElement(jss, i);
        if(env->ExceptionCheck()) {
          // exception thrown: ArrayIndexOutOfBoundsException
          *has_exception = JNI_TRUE;
          return strs;
        }

        jstring jstr = static_cast<jstring>(js);
        const char* str = env->GetStringUTFChars(jstr, nullptr);
        if(str == nullptr) {
          // exception thrown: OutOfMemoryError
          env->DeleteLocalRef(js);
          *has_exception = JNI_TRUE;
          return strs;
        }

        strs.push_back(std::string(str));

        env->ReleaseStringUTFChars(jstr, str);
        env->DeleteLocalRef(js);
      }

      *has_exception = JNI_FALSE;
      return strs;
    }

    /**
     * Copies a jstring to a C-style null-terminated byte string
     * and releases the original jstring
     *
     * The jstring is copied as UTF-8
     *
     * If an exception occurs, then JNIEnv::ExceptionCheck()
     * will have been called
     *
     * @param env (IN) A pointer to the java environment
     * @param js (IN) The java string to copy
     * @param has_exception (OUT) will be set to JNI_TRUE
     *     if an OutOfMemoryError exception occurs
     *
     * @return A pointer to the copied string, or a
     *     nullptr if has_exception == JNI_TRUE
     */
    static std::unique_ptr<char[]> copyString(JNIEnv* env, jstring js,
        jboolean* has_exception) {
      const char *utf = env->GetStringUTFChars(js, nullptr);
      if(utf == nullptr) {
        // exception thrown: OutOfMemoryError
        env->ExceptionCheck();
        *has_exception = JNI_TRUE;
        return nullptr;
      } else if(env->ExceptionCheck()) {
        // exception thrown
        env->ReleaseStringUTFChars(js, utf);
        *has_exception = JNI_TRUE;
        return nullptr;
      }

      const jsize utf_len = env->GetStringUTFLength(js);
      std::unique_ptr<char[]> str(new char[utf_len + 1]);  // Note: + 1 is needed for the c_str null terminator
      std::strcpy(str.get(), utf);
      env->ReleaseStringUTFChars(js, utf);
      *has_exception = JNI_FALSE;
      return str;
    }

    /**
     * Copies a jstring to a std::string
     * and releases the original jstring
     *
     * If an exception occurs, then JNIEnv::ExceptionCheck()
     * will have been called
     *
     * @param env (IN) A pointer to the java environment
     * @param js (IN) The java string to copy
     * @param has_exception (OUT) will be set to JNI_TRUE
     *     if an OutOfMemoryError exception occurs
     *
     * @return A std:string copy of the jstring, or an
     *     empty std::string if has_exception == JNI_TRUE
     */
    static std::string copyStdString(JNIEnv* env, jstring js,
      jboolean* has_exception) {
      const char *utf = env->GetStringUTFChars(js, nullptr);
      if(utf == nullptr) {
        // exception thrown: OutOfMemoryError
        env->ExceptionCheck();
        *has_exception = JNI_TRUE;
        return std::string();
      } else if(env->ExceptionCheck()) {
        // exception thrown
        env->ReleaseStringUTFChars(js, utf);
        *has_exception = JNI_TRUE;
        return std::string();
      }

      std::string name(utf);
      env->ReleaseStringUTFChars(js, utf);
      *has_exception = JNI_FALSE;
      return name;
    }

    /**
     * Copies bytes from a std::string to a jByteArray
     *
     * @param env A pointer to the java environment
     * @param bytes The bytes to copy
     *
     * @return the Java byte[], or nullptr if an exception occurs
     * 
     * @throws RocksDBException thrown 
     *   if memory size to copy exceeds general java specific array size limitation.
     */
    static jbyteArray copyBytes(JNIEnv* env, std::string bytes) {
      return createJavaByteArrayWithSizeCheck(env, bytes.c_str(), bytes.size());
    }

    /**
     * Given a Java byte[][] which is an array of java.lang.Strings
     * where each String is a byte[], the passed function `string_fn`
     * will be called on each String, the result is the collected by
     * calling the passed function `collector_fn`
     *
     * @param env (IN) A pointer to the java environment
     * @param jbyte_strings (IN) A Java array of Strings expressed as bytes
     * @param string_fn (IN) A transform function to call for each String
     * @param collector_fn (IN) A collector which is called for the result
     *     of each `string_fn`
     * @param has_exception (OUT) will be set to JNI_TRUE
     *     if an ArrayIndexOutOfBoundsException or OutOfMemoryError
     *     exception occurs
     */
    template <typename T> static void byteStrings(JNIEnv* env,
        jobjectArray jbyte_strings,
        std::function<T(const char*, const size_t)> string_fn,
        std::function<void(size_t, T)> collector_fn,
        jboolean *has_exception) {
      const jsize jlen = env->GetArrayLength(jbyte_strings);

      for(jsize i = 0; i < jlen; i++) {
        jobject jbyte_string_obj = env->GetObjectArrayElement(jbyte_strings, i);
        if(env->ExceptionCheck()) {
          // exception thrown: ArrayIndexOutOfBoundsException
          *has_exception = JNI_TRUE;  // signal error
          return;
        }

        jbyteArray jbyte_string_ary =
            reinterpret_cast<jbyteArray>(jbyte_string_obj);
        T result = byteString(env, jbyte_string_ary, string_fn, has_exception);

        env->DeleteLocalRef(jbyte_string_obj);

        if(*has_exception == JNI_TRUE) {
          // exception thrown: OutOfMemoryError
          return;
        }

        collector_fn(i, result);
      }

      *has_exception = JNI_FALSE;
    }

    /**
     * Given a Java String which is expressed as a Java Byte Array byte[],
     * the passed function `string_fn` will be called on the String
     * and the result returned
     *
     * @param env (IN) A pointer to the java environment
     * @param jbyte_string_ary (IN) A Java String expressed in bytes
     * @param string_fn (IN) A transform function to call on the String
     * @param has_exception (OUT) will be set to JNI_TRUE
     *     if an OutOfMemoryError exception occurs
     */
    template <typename T> static T byteString(JNIEnv* env,
        jbyteArray jbyte_string_ary,
        std::function<T(const char*, const size_t)> string_fn,
        jboolean* has_exception) {
      const jsize jbyte_string_len = env->GetArrayLength(jbyte_string_ary);
      return byteString<T>(env, jbyte_string_ary, jbyte_string_len, string_fn,
          has_exception);
    }

    /**
     * Given a Java String which is expressed as a Java Byte Array byte[],
     * the passed function `string_fn` will be called on the String
     * and the result returned
     *
     * @param env (IN) A pointer to the java environment
     * @param jbyte_string_ary (IN) A Java String expressed in bytes
     * @param jbyte_string_len (IN) The length of the Java String
     *     expressed in bytes
     * @param string_fn (IN) A transform function to call on the String
     * @param has_exception (OUT) will be set to JNI_TRUE
     *     if an OutOfMemoryError exception occurs
     */
    template <typename T> static T byteString(JNIEnv* env,
        jbyteArray jbyte_string_ary, const jsize jbyte_string_len,
        std::function<T(const char*, const size_t)> string_fn,
        jboolean* has_exception) {
      jbyte* jbyte_string =
          env->GetByteArrayElements(jbyte_string_ary, nullptr);
      if(jbyte_string == nullptr) {
        // exception thrown: OutOfMemoryError
        *has_exception = JNI_TRUE;
        return nullptr;  // signal error
      }

      T result =
          string_fn(reinterpret_cast<char *>(jbyte_string), jbyte_string_len);

      env->ReleaseByteArrayElements(jbyte_string_ary, jbyte_string, JNI_ABORT);

      *has_exception = JNI_FALSE;
      return result;
    }

    /**
     * Converts a std::vector<string> to a Java byte[][] where each Java String
     * is expressed as a Java Byte Array byte[].
     *
     * @param env A pointer to the java environment
     * @param strings A vector of Strings
     *
     * @return A Java array of Strings expressed as bytes,
     *     or nullptr if an exception is thrown
     */
    static jobjectArray stringsBytes(JNIEnv* env, std::vector<std::string> strings) {
      jclass jcls_ba = ByteJni::getArrayJClass(env);
      if(jcls_ba == nullptr) {
        // exception occurred
        return nullptr;
      }

      const jsize len = static_cast<jsize>(strings.size());

      jobjectArray jbyte_strings = env->NewObjectArray(len, jcls_ba, nullptr);
      if(jbyte_strings == nullptr) {
        // exception thrown: OutOfMemoryError
        return nullptr;
      }

      for (jsize i = 0; i < len; i++) {
        std::string *str = &strings[i];
        const jsize str_len = static_cast<jsize>(str->size());

        jbyteArray jbyte_string_ary = env->NewByteArray(str_len);
        if(jbyte_string_ary == nullptr) {
          // exception thrown: OutOfMemoryError
          env->DeleteLocalRef(jbyte_strings);
          return nullptr;
        }

        env->SetByteArrayRegion(
          jbyte_string_ary, 0, str_len,
          const_cast<jbyte*>(reinterpret_cast<const jbyte*>(str->c_str())));
        if(env->ExceptionCheck()) {
          // exception thrown: ArrayIndexOutOfBoundsException
          env->DeleteLocalRef(jbyte_string_ary);
          env->DeleteLocalRef(jbyte_strings);
          return nullptr;
        }

        env->SetObjectArrayElement(jbyte_strings, i, jbyte_string_ary);
        if(env->ExceptionCheck()) {
          // exception thrown: ArrayIndexOutOfBoundsException
          // or ArrayStoreException
          env->DeleteLocalRef(jbyte_string_ary);
          env->DeleteLocalRef(jbyte_strings);
          return nullptr;
        }

        env->DeleteLocalRef(jbyte_string_ary);
      }

      return jbyte_strings;
    }

     /**
     * Converts a std::vector<std::string> to a Java String[].
     *
     * @param env A pointer to the java environment
     * @param strings A vector of Strings
     *
     * @return A Java array of Strings,
     *     or nullptr if an exception is thrown
     */
    static jobjectArray toJavaStrings(JNIEnv* env,
        const std::vector<std::string>* strings) {
      jclass jcls_str = env->FindClass("java/lang/String");
      if(jcls_str == nullptr) {
        // exception occurred
        return nullptr;
      }

      const jsize len = static_cast<jsize>(strings->size());

      jobjectArray jstrings = env->NewObjectArray(len, jcls_str, nullptr);
      if(jstrings == nullptr) {
        // exception thrown: OutOfMemoryError
        return nullptr;
      }

      for (jsize i = 0; i < len; i++) {
        const std::string *str = &((*strings)[i]);
        jstring js = rocksdb::JniUtil::toJavaString(env, str);
        if (js == nullptr) {
          env->DeleteLocalRef(jstrings);
          return nullptr;
        }

        env->SetObjectArrayElement(jstrings, i, js);
        if(env->ExceptionCheck()) {
          // exception thrown: ArrayIndexOutOfBoundsException
          // or ArrayStoreException
          env->DeleteLocalRef(js);
          env->DeleteLocalRef(jstrings);
          return nullptr;
        }
      }

      return jstrings;
    }

    /**
     * Creates a Java UTF String from a C++ std::string
     *
     * @param env A pointer to the java environment
     * @param string the C++ std::string
     * @param treat_empty_as_null true if empty strings should be treated as null
     *
     * @return the Java UTF string, or nullptr if the provided string
     *     is null (or empty and treat_empty_as_null is set), or if an
     *     exception occurs allocating the Java String.
     */
    static jstring toJavaString(JNIEnv* env, const std::string* string,
        const bool treat_empty_as_null = false) {
      if (string == nullptr) {
        return nullptr;
      }

      if (treat_empty_as_null && string->empty()) {
        return nullptr;
      }

      return env->NewStringUTF(string->c_str());
    }
    
    /**
      * Copies bytes to a new jByteArray with the check of java array size limitation.
      *
      * @param bytes pointer to memory to copy to a new jByteArray
      * @param size number of bytes to copy
      *
      * @return the Java byte[], or nullptr if an exception occurs
      * 
      * @throws RocksDBException thrown 
      *   if memory size to copy exceeds general java array size limitation to avoid overflow.
      */
    static jbyteArray createJavaByteArrayWithSizeCheck(JNIEnv* env, const char* bytes, const size_t size) {
      // Limitation for java array size is vm specific
      // In general it cannot exceed Integer.MAX_VALUE (2^31 - 1)
      // Current HotSpot VM limitation for array size is Integer.MAX_VALUE - 5 (2^31 - 1 - 5)
      // It means that the next call to env->NewByteArray can still end with 
      // OutOfMemoryError("Requested array size exceeds VM limit") coming from VM
      static const size_t MAX_JARRAY_SIZE = (static_cast<size_t>(1)) << 31;
      if(size > MAX_JARRAY_SIZE) {
        rocksdb::RocksDBExceptionJni::ThrowNew(env, "Requested array size exceeds VM limit");
        return nullptr;
      }
      
      const jsize jlen = static_cast<jsize>(size);
      jbyteArray jbytes = env->NewByteArray(jlen);
      if(jbytes == nullptr) {
        // exception thrown: OutOfMemoryError	
        return nullptr;
      }
      
      env->SetByteArrayRegion(jbytes, 0, jlen,
        const_cast<jbyte*>(reinterpret_cast<const jbyte*>(bytes)));
      if(env->ExceptionCheck()) {
        // exception thrown: ArrayIndexOutOfBoundsException
        env->DeleteLocalRef(jbytes);
        return nullptr;
      }

      return jbytes;
    }

    /**
     * Copies bytes from a rocksdb::Slice to a jByteArray
     *
     * @param env A pointer to the java environment
     * @param bytes The bytes to copy
     *
     * @return the Java byte[] or nullptr if an exception occurs
     * 
     * @throws RocksDBException thrown 
     *   if memory size to copy exceeds general java specific array size limitation.
     */
    static jbyteArray copyBytes(JNIEnv* env, const Slice& bytes) {
      return createJavaByteArrayWithSizeCheck(env, bytes.data(), bytes.size());
    }

    /*
     * Helper for operations on a key and value
     * for example WriteBatch->Put
     *
     * TODO(AR) could be used for RocksDB->Put etc.
     */
    static std::unique_ptr<rocksdb::Status> kv_op(
        std::function<rocksdb::Status(rocksdb::Slice, rocksdb::Slice)> op,
        JNIEnv* env, jobject /*jobj*/,
        jbyteArray jkey, jint jkey_len,
        jbyteArray jvalue, jint jvalue_len) {
      jbyte* key = env->GetByteArrayElements(jkey, nullptr);
      if(env->ExceptionCheck()) {
        // exception thrown: OutOfMemoryError
        return nullptr;
      }

      jbyte* value = env->GetByteArrayElements(jvalue, nullptr);
      if(env->ExceptionCheck()) {
        // exception thrown: OutOfMemoryError
        if(key != nullptr) {
          env->ReleaseByteArrayElements(jkey, key, JNI_ABORT);
        }
        return nullptr;
      }

      rocksdb::Slice key_slice(reinterpret_cast<char*>(key), jkey_len);
      rocksdb::Slice value_slice(reinterpret_cast<char*>(value),
          jvalue_len);

      auto status = op(key_slice, value_slice);

      if(value != nullptr) {
        env->ReleaseByteArrayElements(jvalue, value, JNI_ABORT);
      }
      if(key != nullptr) {
        env->ReleaseByteArrayElements(jkey, key, JNI_ABORT);
      }

      return std::unique_ptr<rocksdb::Status>(new rocksdb::Status(status));
    }

    /*
     * Helper for operations on a key
     * for example WriteBatch->Delete
     *
     * TODO(AR) could be used for RocksDB->Delete etc.
     */
    static std::unique_ptr<rocksdb::Status> k_op(
        std::function<rocksdb::Status(rocksdb::Slice)> op,
        JNIEnv* env, jobject /*jobj*/,
        jbyteArray jkey, jint jkey_len) {
      jbyte* key = env->GetByteArrayElements(jkey, nullptr);
      if(env->ExceptionCheck()) {
        // exception thrown: OutOfMemoryError
        return nullptr;
      }

      rocksdb::Slice key_slice(reinterpret_cast<char*>(key), jkey_len);

      auto status = op(key_slice);

      if(key != nullptr) {
        env->ReleaseByteArrayElements(jkey, key, JNI_ABORT);
      }

      return std::unique_ptr<rocksdb::Status>(new rocksdb::Status(status));
    }

    /*
     * Helper for operations on a value
     * for example WriteBatchWithIndex->GetFromBatch
     */
    static jbyteArray v_op(
        std::function<rocksdb::Status(rocksdb::Slice, std::string*)> op,
        JNIEnv* env, jbyteArray jkey, jint jkey_len) {
      jbyte* key = env->GetByteArrayElements(jkey, nullptr);
      if(env->ExceptionCheck()) {
        // exception thrown: OutOfMemoryError
        return nullptr;
      }

      rocksdb::Slice key_slice(reinterpret_cast<char*>(key), jkey_len);

      std::string value;
      rocksdb::Status s = op(key_slice, &value);

      if(key != nullptr) {
        env->ReleaseByteArrayElements(jkey, key, JNI_ABORT);
      }

      if (s.IsNotFound()) {
        return nullptr;
      }

      if (s.ok()) {
        jbyteArray jret_value =
            env->NewByteArray(static_cast<jsize>(value.size()));
        if(jret_value == nullptr) {
          // exception thrown: OutOfMemoryError
          return nullptr;
        }

        env->SetByteArrayRegion(jret_value, 0, static_cast<jsize>(value.size()),
                                const_cast<jbyte*>(reinterpret_cast<const jbyte*>(value.c_str())));
        if(env->ExceptionCheck()) {
          // exception thrown: ArrayIndexOutOfBoundsException
          if(jret_value != nullptr) {
            env->DeleteLocalRef(jret_value);
          }
          return nullptr;
        }

        return jret_value;
      }

      rocksdb::RocksDBExceptionJni::ThrowNew(env, s);
      return nullptr;
    }

    /**
     * Creates a vector<T*> of C++ pointers from
     *     a Java array of C++ pointer addresses.
     * 
     * @param env (IN) A pointer to the java environment
     * @param pointers (IN) A Java array of C++ pointer addresses
     * @param has_exception (OUT) will be set to JNI_TRUE
     *     if an ArrayIndexOutOfBoundsException or OutOfMemoryError
     *     exception occurs.
     * 
     * @return A vector of C++ pointers.
     */
    template<typename T> static std::vector<T*> fromJPointers(
        JNIEnv* env, jlongArray jptrs, jboolean *has_exception) {
      const jsize jptrs_len = env->GetArrayLength(jptrs);
      std::vector<T*> ptrs;
      jlong* jptr = env->GetLongArrayElements(jptrs, nullptr);
      if (jptr == nullptr) {
        // exception thrown: OutOfMemoryError
        *has_exception = JNI_TRUE;
        return ptrs;
      }
      ptrs.reserve(jptrs_len);
      for (jsize i = 0; i < jptrs_len; i++) {
        ptrs.push_back(reinterpret_cast<T*>(jptr[i]));
      }
      env->ReleaseLongArrayElements(jptrs, jptr, JNI_ABORT);
      return ptrs;
    }

    /**
     * Creates a Java array of C++ pointer addresses
     *     from a vector of C++ pointers.
     * 
     * @param env (IN) A pointer to the java environment
     * @param pointers (IN) A vector of C++ pointers
     * @param has_exception (OUT) will be set to JNI_TRUE
     *     if an ArrayIndexOutOfBoundsException or OutOfMemoryError
     *     exception occurs
     * 
     * @return Java array of C++ pointer addresses.
     */
    template<typename T> static jlongArray toJPointers(JNIEnv* env,
        const std::vector<T*> &pointers,
        jboolean *has_exception) {
      const jsize len = static_cast<jsize>(pointers.size());
      std::unique_ptr<jlong[]> results(new jlong[len]);
      std::transform(pointers.begin(), pointers.end(), results.get(), [](T* pointer) -> jlong {
        return reinterpret_cast<jlong>(pointer);
      });

      jlongArray jpointers = env->NewLongArray(len);
      if (jpointers == nullptr) {
        // exception thrown: OutOfMemoryError
        *has_exception = JNI_TRUE;
        return nullptr;
      }

      env->SetLongArrayRegion(jpointers, 0, len, results.get());
      if (env->ExceptionCheck()) {
        // exception thrown: ArrayIndexOutOfBoundsException
        *has_exception = JNI_TRUE;
        env->DeleteLocalRef(jpointers);
        return nullptr;
      }

      *has_exception = JNI_FALSE;

      return jpointers;
    }
};

class MapJni : public JavaClass {
 public:
  /**
   * Get the Java Class java.util.Map
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "java/util/Map");
  }

  /**
   * Get the Java Method: Map#put
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getMapPutMethodId(JNIEnv* env) {
    jclass jlist_clazz = getJClass(env);
    if(jlist_clazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid =
        env->GetMethodID(jlist_clazz, "put", "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;");
    assert(mid != nullptr);
    return mid;
  }
};

class HashMapJni : public JavaClass {
 public:
  /**
   * Get the Java Class java.util.HashMap
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "java/util/HashMap");
  }

  /**
   * Create a new Java java.util.HashMap object.
   *
   * @param env A pointer to the Java environment
   *
   * @return A reference to a Java java.util.HashMap object, or
   * nullptr if an an exception occurs
   */
  static jobject construct(JNIEnv* env, const uint32_t initial_capacity = 16) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID mid = env->GetMethodID(jclazz, "<init>", "(I)V");
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    jobject jhash_map = env->NewObject(jclazz, mid, static_cast<jint>(initial_capacity));
    if (env->ExceptionCheck()) {
      return nullptr;
    }

    return jhash_map;
  }

  /**
   * A function which maps a std::pair<K,V> to a std::pair<JK, JV>
   *
   * @return Either a pointer to a std::pair<jobject, jobject>, or nullptr
   *     if an error occurs during the mapping
   */
  template <typename K, typename V, typename JK, typename JV>
  using FnMapKV = std::function<std::unique_ptr<std::pair<JK, JV>> (const std::pair<K, V>&)>;

  // template <class I, typename K, typename V, typename K1, typename V1, typename std::enable_if<std::is_same<typename std::iterator_traits<I>::value_type, std::pair<const K,V>>::value, int32_t>::type = 0>
  // static void putAll(JNIEnv* env, const jobject jhash_map, I iterator, const FnMapKV<const K,V,K1,V1> &fn_map_kv) {
  /**
   * Returns true if it succeeds, false if an error occurs
   */
  template<class iterator_type, typename K, typename V>
  static bool putAll(JNIEnv* env, const jobject jhash_map, iterator_type iterator, iterator_type end, const FnMapKV<K, V, jobject, jobject> &fn_map_kv) {
    const jmethodID jmid_put = rocksdb::MapJni::getMapPutMethodId(env);
    if (jmid_put == nullptr) {
      return false;
    }

    for (auto it = iterator; it != end; ++it) {
      const std::unique_ptr<std::pair<jobject, jobject>> result = fn_map_kv(*it);
      if (result == nullptr) {
          // an error occurred during fn_map_kv
          return false;
      }
      env->CallObjectMethod(jhash_map, jmid_put, result->first, result->second);
      if (env->ExceptionCheck()) {
        // exception occurred
        env->DeleteLocalRef(result->second);
        env->DeleteLocalRef(result->first);
        return false;
      }

      // release local references
      env->DeleteLocalRef(result->second);
      env->DeleteLocalRef(result->first);
    }

    return true;
  }

  /**
   * Creates a java.util.Map<String, String> from a std::map<std::string, std::string>
   *
   * @param env A pointer to the Java environment
   * @param map the Cpp map
   *
   * @return a reference to the Java java.util.Map object, or nullptr if an exception occcurred
   */
  static jobject fromCppMap(JNIEnv* env, const std::map<std::string, std::string>* map) {
    if (map == nullptr) {
      return nullptr;
    }

    jobject jhash_map = construct(env, static_cast<uint32_t>(map->size()));
    if (jhash_map == nullptr) {
      // exception occurred
      return nullptr;
    }

    const rocksdb::HashMapJni::FnMapKV<const std::string, const std::string, jobject, jobject> fn_map_kv =
        [env](const std::pair<const std::string, const std::string>& kv) {
      jstring jkey = rocksdb::JniUtil::toJavaString(env, &(kv.first), false);
      if (env->ExceptionCheck()) {
        // an error occurred
        return std::unique_ptr<std::pair<jobject, jobject>>(nullptr);
      }

      jstring jvalue = rocksdb::JniUtil::toJavaString(env, &(kv.second), true);
      if (env->ExceptionCheck()) {
        // an error occurred
        env->DeleteLocalRef(jkey);
        return std::unique_ptr<std::pair<jobject, jobject>>(nullptr);
      }

      return std::unique_ptr<std::pair<jobject, jobject>>(new std::pair<jobject, jobject>(static_cast<jobject>(jkey), static_cast<jobject>(jvalue)));
    };

    if (!putAll(env, jhash_map, map->begin(), map->end(), fn_map_kv)) {
      // exception occurred
      return nullptr;
    }

    return jhash_map;
  }

  /**
   * Creates a java.util.Map<String, Long> from a std::map<std::string, uint32_t>
   *
   * @param env A pointer to the Java environment
   * @param map the Cpp map
   *
   * @return a reference to the Java java.util.Map object, or nullptr if an exception occcurred
   */
  static jobject fromCppMap(JNIEnv* env, const std::map<std::string, uint32_t>* map) {
    if (map == nullptr) {
      return nullptr;
    }

    if (map == nullptr) {
      return nullptr;
    }

    jobject jhash_map = construct(env, static_cast<uint32_t>(map->size()));
    if (jhash_map == nullptr) {
      // exception occurred
      return nullptr;
    }

    const rocksdb::HashMapJni::FnMapKV<const std::string, const uint32_t, jobject, jobject> fn_map_kv =
        [env](const std::pair<const std::string, const uint32_t>& kv) {
      jstring jkey = rocksdb::JniUtil::toJavaString(env, &(kv.first), false);
      if (env->ExceptionCheck()) {
        // an error occurred
        return std::unique_ptr<std::pair<jobject, jobject>>(nullptr);
      }

      jobject jvalue = rocksdb::IntegerJni::valueOf(env, static_cast<jint>(kv.second));
      if (env->ExceptionCheck()) {
        // an error occurred
        env->DeleteLocalRef(jkey);
        return std::unique_ptr<std::pair<jobject, jobject>>(nullptr);
      }

      return std::unique_ptr<std::pair<jobject, jobject>>(new std::pair<jobject, jobject>(static_cast<jobject>(jkey), jvalue));
    };

    if (!putAll(env, jhash_map, map->begin(), map->end(), fn_map_kv)) {
      // exception occurred
      return nullptr;
    }

    return jhash_map;
  }

  /**
   * Creates a java.util.Map<String, Long> from a std::map<std::string, uint64_t>
   *
   * @param env A pointer to the Java environment
   * @param map the Cpp map
   *
   * @return a reference to the Java java.util.Map object, or nullptr if an exception occcurred
   */
  static jobject fromCppMap(JNIEnv* env, const std::map<std::string, uint64_t>* map) {
    if (map == nullptr) {
      return nullptr;
    }

    jobject jhash_map = construct(env, static_cast<uint32_t>(map->size()));
    if (jhash_map == nullptr) {
      // exception occurred
      return nullptr;
    }

    const rocksdb::HashMapJni::FnMapKV<const std::string, const uint64_t, jobject, jobject> fn_map_kv =
        [env](const std::pair<const std::string, const uint64_t>& kv) {
      jstring jkey = rocksdb::JniUtil::toJavaString(env, &(kv.first), false);
      if (env->ExceptionCheck()) {
        // an error occurred
        return std::unique_ptr<std::pair<jobject, jobject>>(nullptr);
      }

      jobject jvalue = rocksdb::LongJni::valueOf(env, static_cast<jlong>(kv.second));
      if (env->ExceptionCheck()) {
        // an error occurred
        env->DeleteLocalRef(jkey);
        return std::unique_ptr<std::pair<jobject, jobject>>(nullptr);
      }

      return std::unique_ptr<std::pair<jobject, jobject>>(new std::pair<jobject, jobject>(static_cast<jobject>(jkey), jvalue));
    };

    if (!putAll(env, jhash_map, map->begin(), map->end(), fn_map_kv)) {
      // exception occurred
      return nullptr;
    }

    return jhash_map;
  }

    /**
   * Creates a java.util.Map<String, Long> from a std::map<uint32_t, uint64_t>
   *
   * @param env A pointer to the Java environment
   * @param map the Cpp map
   *
   * @return a reference to the Java java.util.Map object, or nullptr if an exception occcurred
   */
  static jobject fromCppMap(JNIEnv* env, const std::map<uint32_t, uint64_t>* map) {
    if (map == nullptr) {
      return nullptr;
    }

    jobject jhash_map = construct(env, static_cast<uint32_t>(map->size()));
    if (jhash_map == nullptr) {
      // exception occurred
      return nullptr;
    }

    const rocksdb::HashMapJni::FnMapKV<const uint32_t, const uint64_t, jobject, jobject> fn_map_kv =
        [env](const std::pair<const uint32_t, const uint64_t>& kv) {
      jobject jkey = rocksdb::IntegerJni::valueOf(env, static_cast<jint>(kv.first));
      if (env->ExceptionCheck()) {
        // an error occurred
        return std::unique_ptr<std::pair<jobject, jobject>>(nullptr);
      }

      jobject jvalue = rocksdb::LongJni::valueOf(env, static_cast<jlong>(kv.second));
      if (env->ExceptionCheck()) {
        // an error occurred
        env->DeleteLocalRef(jkey);
        return std::unique_ptr<std::pair<jobject, jobject>>(nullptr);
      }

      return std::unique_ptr<std::pair<jobject, jobject>>(new std::pair<jobject, jobject>(static_cast<jobject>(jkey), jvalue));
    };

    if (!putAll(env, jhash_map, map->begin(), map->end(), fn_map_kv)) {
      // exception occurred
      return nullptr;
    }

    return jhash_map;
  }
};

// The portal class for org.rocksdb.RocksDB
class RocksDBJni : public RocksDBNativeClass<rocksdb::DB*, RocksDBJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.RocksDB
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/RocksDB");
  }
};

// The portal class for org.rocksdb.Options
class OptionsJni : public RocksDBNativeClass<
    rocksdb::Options*, OptionsJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.Options
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/Options");
  }
};

// The portal class for org.rocksdb.DBOptions
class DBOptionsJni : public RocksDBNativeClass<
    rocksdb::DBOptions*, DBOptionsJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.DBOptions
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/DBOptions");
  }
};

// The portal class for org.rocksdb.ColumnFamilyOptions
class ColumnFamilyOptionsJni
    : public RocksDBNativeClass<rocksdb::ColumnFamilyOptions*,
                                ColumnFamilyOptionsJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.ColumnFamilyOptions
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env,
                                         "org/rocksdb/ColumnFamilyOptions");
  }

  /**
   * Create a new Java org.rocksdb.ColumnFamilyOptions object with the same
   * properties as the provided C++ rocksdb::ColumnFamilyOptions object
   *
   * @param env A pointer to the Java environment
   * @param cfoptions A pointer to rocksdb::ColumnFamilyOptions object
   *
   * @return A reference to a Java org.rocksdb.ColumnFamilyOptions object, or
   * nullptr if an an exception occurs
   */
  static jobject construct(JNIEnv* env, const ColumnFamilyOptions* cfoptions) {
    auto* cfo = new rocksdb::ColumnFamilyOptions(*cfoptions);
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID mid = env->GetMethodID(jclazz, "<init>", "(J)V");
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    jobject jcfd = env->NewObject(jclazz, mid, reinterpret_cast<jlong>(cfo));
    if (env->ExceptionCheck()) {
      return nullptr;
    }

    return jcfd;
  }
};

// The portal class for org.rocksdb.WriteOptions
class WriteOptionsJni : public RocksDBNativeClass<
    rocksdb::WriteOptions*, WriteOptionsJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.WriteOptions
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/WriteOptions");
  }
};

// The portal class for org.rocksdb.ReadOptions
class ReadOptionsJni : public RocksDBNativeClass<
    rocksdb::ReadOptions*, ReadOptionsJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.ReadOptions
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/ReadOptions");
  }
};

// The portal class for org.rocksdb.WriteBatch
class WriteBatchJni : public RocksDBNativeClass<
    rocksdb::WriteBatch*, WriteBatchJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.WriteBatch
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/WriteBatch");
  }

  /**
   * Create a new Java org.rocksdb.WriteBatch object
   *
   * @param env A pointer to the Java environment
   * @param wb A pointer to rocksdb::WriteBatch object
   *
   * @return A reference to a Java org.rocksdb.WriteBatch object, or
   * nullptr if an an exception occurs
   */
  static jobject construct(JNIEnv* env, const WriteBatch* wb) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID mid = env->GetMethodID(jclazz, "<init>", "(J)V");
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    jobject jwb = env->NewObject(jclazz, mid, reinterpret_cast<jlong>(wb));
    if (env->ExceptionCheck()) {
      return nullptr;
    }

    return jwb;
  }
};

// The portal class for org.rocksdb.WriteBatch.Handler
class WriteBatchHandlerJni : public RocksDBNativeClass<
    const rocksdb::WriteBatchHandlerJniCallback*,
    WriteBatchHandlerJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.WriteBatch.Handler
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env,
        "org/rocksdb/WriteBatch$Handler");
  }

  /**
   * Get the Java Method: WriteBatch.Handler#put
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getPutCfMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "put", "(I[B[B)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: WriteBatch.Handler#put
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getPutMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "put", "([B[B)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: WriteBatch.Handler#merge
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getMergeCfMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "merge", "(I[B[B)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: WriteBatch.Handler#merge
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getMergeMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "merge", "([B[B)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: WriteBatch.Handler#delete
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getDeleteCfMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "delete", "(I[B)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: WriteBatch.Handler#delete
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getDeleteMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "delete", "([B)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: WriteBatch.Handler#singleDelete
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getSingleDeleteCfMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "singleDelete", "(I[B)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: WriteBatch.Handler#singleDelete
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getSingleDeleteMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "singleDelete", "([B)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: WriteBatch.Handler#deleteRange
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getDeleteRangeCfMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "deleteRange", "(I[B[B)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: WriteBatch.Handler#deleteRange
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getDeleteRangeMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "deleteRange", "([B[B)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: WriteBatch.Handler#logData
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getLogDataMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "logData", "([B)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: WriteBatch.Handler#putBlobIndex
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getPutBlobIndexCfMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "putBlobIndex", "(I[B[B)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: WriteBatch.Handler#markBeginPrepare
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getMarkBeginPrepareMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "markBeginPrepare", "()V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: WriteBatch.Handler#markEndPrepare
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getMarkEndPrepareMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "markEndPrepare", "([B)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: WriteBatch.Handler#markNoop
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getMarkNoopMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "markNoop", "(Z)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: WriteBatch.Handler#markRollback
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getMarkRollbackMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "markRollback", "([B)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: WriteBatch.Handler#markCommit
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getMarkCommitMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "markCommit", "([B)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: WriteBatch.Handler#shouldContinue
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getContinueMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "shouldContinue", "()Z");
    assert(mid != nullptr);
    return mid;
  }
};

class WriteBatchSavePointJni : public JavaClass {
 public:
  /**
   * Get the Java Class org.rocksdb.WriteBatch.SavePoint
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/WriteBatch$SavePoint");
  }

  /**
   * Get the Java Method: HistogramData constructor
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getConstructorMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "<init>", "(JJJ)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Create a new Java org.rocksdb.WriteBatch.SavePoint object
   *
   * @param env A pointer to the Java environment
   * @param savePoint A pointer to rocksdb::WriteBatch::SavePoint object
   *
   * @return A reference to a Java org.rocksdb.WriteBatch.SavePoint object, or
   * nullptr if an an exception occurs
   */
  static jobject construct(JNIEnv* env, const SavePoint &save_point) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID mid = getConstructorMethodId(env);
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    jobject jsave_point = env->NewObject(jclazz, mid,
        static_cast<jlong>(save_point.size),
        static_cast<jlong>(save_point.count),
        static_cast<jlong>(save_point.content_flags));
    if (env->ExceptionCheck()) {
      return nullptr;
    }

    return jsave_point;
  }
};

// The portal class for org.rocksdb.WriteBatchWithIndex
class WriteBatchWithIndexJni : public RocksDBNativeClass<
    rocksdb::WriteBatchWithIndex*, WriteBatchWithIndexJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.WriteBatchWithIndex
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env,
        "org/rocksdb/WriteBatchWithIndex");
  }
};

// The portal class for org.rocksdb.HistogramData
class HistogramDataJni : public JavaClass {
 public:
  /**
   * Get the Java Class org.rocksdb.HistogramData
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/HistogramData");
  }

  /**
   * Get the Java Method: HistogramData constructor
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getConstructorMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "<init>", "(DDDDDDJJD)V");
    assert(mid != nullptr);
    return mid;
  }
};

// The portal class for org.rocksdb.BackupableDBOptions
class BackupableDBOptionsJni : public RocksDBNativeClass<
    rocksdb::BackupableDBOptions*, BackupableDBOptionsJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.BackupableDBOptions
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env,
        "org/rocksdb/BackupableDBOptions");
  }
};

// The portal class for org.rocksdb.BackupEngine
class BackupEngineJni : public RocksDBNativeClass<
    rocksdb::BackupEngine*, BackupEngineJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.BackupableEngine
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/BackupEngine");
  }
};

// The portal class for org.rocksdb.RocksIterator
class IteratorJni : public RocksDBNativeClass<
    rocksdb::Iterator*, IteratorJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.RocksIterator
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/RocksIterator");
  }
};

// The portal class for org.rocksdb.Filter
class FilterJni : public RocksDBNativeClass<
    std::shared_ptr<rocksdb::FilterPolicy>*, FilterJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.Filter
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/Filter");
  }
};

// The portal class for org.rocksdb.ColumnFamilyHandle
class ColumnFamilyHandleJni : public RocksDBNativeClass<
    rocksdb::ColumnFamilyHandle*, ColumnFamilyHandleJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.ColumnFamilyHandle
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env,
        "org/rocksdb/ColumnFamilyHandle");
  }
};

// The portal class for org.rocksdb.FlushOptions
class FlushOptionsJni : public RocksDBNativeClass<
    rocksdb::FlushOptions*, FlushOptionsJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.FlushOptions
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/FlushOptions");
  }
};

// The portal class for org.rocksdb.ComparatorOptions
class ComparatorOptionsJni : public RocksDBNativeClass<
    rocksdb::ComparatorJniCallbackOptions*, ComparatorOptionsJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.ComparatorOptions
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/ComparatorOptions");
  }
};

// The portal class for org.rocksdb.AbstractCompactionFilterFactory
class AbstractCompactionFilterFactoryJni : public RocksDBNativeClass<
    const rocksdb::CompactionFilterFactoryJniCallback*,
    AbstractCompactionFilterFactoryJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.AbstractCompactionFilterFactory
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env,
        "org/rocksdb/AbstractCompactionFilterFactory");
  }

  /**
   * Get the Java Method: AbstractCompactionFilterFactory#name
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getNameMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(
        jclazz, "name", "()Ljava/lang/String;");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: AbstractCompactionFilterFactory#createCompactionFilter
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getCreateCompactionFilterMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz,
      "createCompactionFilter",
      "(ZZ)J");
    assert(mid != nullptr);
    return mid;
  }
};

// The portal class for org.rocksdb.AbstractTransactionNotifier
class AbstractTransactionNotifierJni : public RocksDBNativeClass<
    const rocksdb::TransactionNotifierJniCallback*,
    AbstractTransactionNotifierJni> {
 public:
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env,
        "org/rocksdb/AbstractTransactionNotifier");
  }

  // Get the java method `snapshotCreated`
  // of org.rocksdb.AbstractTransactionNotifier.
  static jmethodID getSnapshotCreatedMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "snapshotCreated", "(J)V");
    assert(mid != nullptr);
    return mid;
  }
};

// The portal class for org.rocksdb.AbstractComparator
class AbstractComparatorJni : public RocksDBNativeClass<
    const rocksdb::BaseComparatorJniCallback*,
    AbstractComparatorJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.AbstractComparator
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env,
        "org/rocksdb/AbstractComparator");
  }

  /**
   * Get the Java Method: Comparator#name
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getNameMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid =
        env->GetMethodID(jclazz, "name", "()Ljava/lang/String;");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: Comparator#compare
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getCompareMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid =
        env->GetMethodID(jclazz, "compare",
            "(Lorg/rocksdb/AbstractSlice;Lorg/rocksdb/AbstractSlice;)I");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: Comparator#findShortestSeparator
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getFindShortestSeparatorMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid =
        env->GetMethodID(jclazz, "findShortestSeparator",
            "(Ljava/lang/String;Lorg/rocksdb/AbstractSlice;)Ljava/lang/String;");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: Comparator#findShortSuccessor
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getFindShortSuccessorMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid =
        env->GetMethodID(jclazz, "findShortSuccessor",
            "(Ljava/lang/String;)Ljava/lang/String;");
    assert(mid != nullptr);
    return mid;
  }
};

// The portal class for org.rocksdb.AbstractSlice
class AbstractSliceJni : public NativeRocksMutableObject<
    const rocksdb::Slice*, AbstractSliceJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.AbstractSlice
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/AbstractSlice");
  }
};

// The portal class for org.rocksdb.Slice
class SliceJni : public NativeRocksMutableObject<
    const rocksdb::Slice*, AbstractSliceJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.Slice
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/Slice");
  }

  /**
   * Constructs a Slice object
   *
   * @param env A pointer to the Java environment
   *
   * @return A reference to a Java Slice object, or a nullptr if an
   *     exception occurs
   */
  static jobject construct0(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "<init>", "()V");
    if(mid == nullptr) {
      // exception occurred accessing method
      return nullptr;
    }

    jobject jslice = env->NewObject(jclazz, mid);
    if(env->ExceptionCheck()) {
      return nullptr;
    }

    return jslice;
  }
};

// The portal class for org.rocksdb.DirectSlice
class DirectSliceJni : public NativeRocksMutableObject<
    const rocksdb::Slice*, AbstractSliceJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.DirectSlice
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/DirectSlice");
  }

  /**
   * Constructs a DirectSlice object
   *
   * @param env A pointer to the Java environment
   *
   * @return A reference to a Java DirectSlice object, or a nullptr if an
   *     exception occurs
   */
  static jobject construct0(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "<init>", "()V");
    if(mid == nullptr) {
      // exception occurred accessing method
      return nullptr;
    }

    jobject jdirect_slice = env->NewObject(jclazz, mid);
    if(env->ExceptionCheck()) {
      return nullptr;
    }

    return jdirect_slice;
  }
};

// The portal class for org.rocksdb.BackupInfo
class BackupInfoJni : public JavaClass {
 public:
  /**
   * Get the Java Class org.rocksdb.BackupInfo
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/BackupInfo");
  }

  /**
   * Constructs a BackupInfo object
   *
   * @param env A pointer to the Java environment
   * @param backup_id id of the backup
   * @param timestamp timestamp of the backup
   * @param size size of the backup
   * @param number_files number of files related to the backup
   * @param app_metadata application specific metadata
   *
   * @return A reference to a Java BackupInfo object, or a nullptr if an
   *     exception occurs
   */
  static jobject construct0(JNIEnv* env, uint32_t backup_id, int64_t timestamp,
                            uint64_t size, uint32_t number_files,
                            const std::string& app_metadata) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid =
        env->GetMethodID(jclazz, "<init>", "(IJJILjava/lang/String;)V");
    if(mid == nullptr) {
      // exception occurred accessing method
      return nullptr;
    }

    jstring japp_metadata = nullptr;
    if (app_metadata != nullptr) {
      japp_metadata = env->NewStringUTF(app_metadata.c_str());
      if (japp_metadata == nullptr) {
        // exception occurred creating java string
        return nullptr;
      }
    }

    jobject jbackup_info = env->NewObject(jclazz, mid, backup_id, timestamp,
                                          size, number_files, japp_metadata);
    if(env->ExceptionCheck()) {
      env->DeleteLocalRef(japp_metadata);
      return nullptr;
    }

    return jbackup_info;
  }
};

class BackupInfoListJni {
 public:
  /**
   * Converts a C++ std::vector<BackupInfo> object to
   * a Java ArrayList<org.rocksdb.BackupInfo> object
   *
   * @param env A pointer to the Java environment
   * @param backup_infos A vector of BackupInfo
   *
   * @return Either a reference to a Java ArrayList object, or a nullptr
   *     if an exception occurs
   */
  static jobject getBackupInfo(JNIEnv* env,
      std::vector<BackupInfo> backup_infos) {
    jclass jarray_list_clazz = rocksdb::ListJni::getArrayListClass(env);
    if(jarray_list_clazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID cstr_mid = rocksdb::ListJni::getArrayListConstructorMethodId(env);
    if(cstr_mid == nullptr) {
      // exception occurred accessing method
      return nullptr;
    }

    jmethodID add_mid = rocksdb::ListJni::getListAddMethodId(env);
    if(add_mid == nullptr) {
      // exception occurred accessing method
      return nullptr;
    }

    // create java list
    jobject jbackup_info_handle_list =
        env->NewObject(jarray_list_clazz, cstr_mid, backup_infos.size());
    if(env->ExceptionCheck()) {
      // exception occurred constructing object
      return nullptr;
    }

    // insert in java list
    auto end = backup_infos.end();
    for (auto it = backup_infos.begin(); it != end; ++it) {
      auto backup_info = *it;

      jobject obj = rocksdb::BackupInfoJni::construct0(
          env, backup_info.backup_id, backup_info.timestamp, backup_info.size,
          backup_info.number_files, backup_info.app_metadata);
      if(env->ExceptionCheck()) {
        // exception occurred constructing object
        if(obj != nullptr) {
          env->DeleteLocalRef(obj);
        }
        if(jbackup_info_handle_list != nullptr) {
          env->DeleteLocalRef(jbackup_info_handle_list);
        }
        return nullptr;
      }

      jboolean rs =
          env->CallBooleanMethod(jbackup_info_handle_list, add_mid, obj);
      if(env->ExceptionCheck() || rs == JNI_FALSE) {
        // exception occurred calling method, or could not add
        if(obj != nullptr) {
          env->DeleteLocalRef(obj);
        }
        if(jbackup_info_handle_list != nullptr) {
          env->DeleteLocalRef(jbackup_info_handle_list);
        }
        return nullptr;
      }
    }

    return jbackup_info_handle_list;
  }
};

// The portal class for org.rocksdb.WBWIRocksIterator
class WBWIRocksIteratorJni : public JavaClass {
 public:
  /**
   * Get the Java Class org.rocksdb.WBWIRocksIterator
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/WBWIRocksIterator");
  }

  /**
   * Get the Java Field: WBWIRocksIterator#entry
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Field ID or nullptr if the class or field id could not
   *     be retieved
   */
  static jfieldID getWriteEntryField(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jfieldID fid =
        env->GetFieldID(jclazz, "entry",
            "Lorg/rocksdb/WBWIRocksIterator$WriteEntry;");
    assert(fid != nullptr);
    return fid;
  }

  /**
   * Gets the value of the WBWIRocksIterator#entry
   *
   * @param env A pointer to the Java environment
   * @param jwbwi_rocks_iterator A reference to a WBWIIterator
   *
   * @return A reference to a Java WBWIRocksIterator.WriteEntry object, or
   *     a nullptr if an exception occurs
   */
  static jobject getWriteEntry(JNIEnv* env, jobject jwbwi_rocks_iterator) {
    assert(jwbwi_rocks_iterator != nullptr);

    jfieldID jwrite_entry_field = getWriteEntryField(env);
    if(jwrite_entry_field == nullptr) {
      // exception occurred accessing the field
      return nullptr;
    }

    jobject jwe = env->GetObjectField(jwbwi_rocks_iterator, jwrite_entry_field);
    assert(jwe != nullptr);
    return jwe;
  }
};

// The portal class for org.rocksdb.WBWIRocksIterator.WriteType
class WriteTypeJni : public JavaClass {
 public:
  /**
   * Get the PUT enum field value of WBWIRocksIterator.WriteType
   *
   * @param env A pointer to the Java environment
   *
   * @return A reference to the enum field value or a nullptr if
   *     the enum field value could not be retrieved
   */
  static jobject PUT(JNIEnv* env) {
    return getEnum(env, "PUT");
  }

  /**
   * Get the MERGE enum field value of WBWIRocksIterator.WriteType
   *
   * @param env A pointer to the Java environment
   *
   * @return A reference to the enum field value or a nullptr if
   *     the enum field value could not be retrieved
   */
  static jobject MERGE(JNIEnv* env) {
    return getEnum(env, "MERGE");
  }

  /**
   * Get the DELETE enum field value of WBWIRocksIterator.WriteType
   *
   * @param env A pointer to the Java environment
   *
   * @return A reference to the enum field value or a nullptr if
   *     the enum field value could not be retrieved
   */
  static jobject DELETE(JNIEnv* env) {
    return getEnum(env, "DELETE");
  }

  /**
   * Get the LOG enum field value of WBWIRocksIterator.WriteType
   *
   * @param env A pointer to the Java environment
   *
   * @return A reference to the enum field value or a nullptr if
   *     the enum field value could not be retrieved
   */
  static jobject LOG(JNIEnv* env) {
    return getEnum(env, "LOG");
  }

  // Returns the equivalent org.rocksdb.WBWIRocksIterator.WriteType for the
  // provided C++ rocksdb::WriteType enum
  static jbyte toJavaWriteType(const rocksdb::WriteType& writeType) {
    switch (writeType) {
      case rocksdb::WriteType::kPutRecord:
        return 0x0;
      case rocksdb::WriteType::kMergeRecord:
        return 0x1;
      case rocksdb::WriteType::kDeleteRecord:
        return 0x2;
      case rocksdb::WriteType::kSingleDeleteRecord:
        return 0x3;
      case rocksdb::WriteType::kDeleteRangeRecord:
        return 0x4;
      case rocksdb::WriteType::kLogDataRecord:
        return 0x5;
      case rocksdb::WriteType::kXIDRecord:
        return 0x6;
      default:
        return 0x7F;  // undefined
    }
  }

 private:
  /**
   * Get the Java Class org.rocksdb.WBWIRocksIterator.WriteType
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/WBWIRocksIterator$WriteType");
  }

  /**
   * Get an enum field of org.rocksdb.WBWIRocksIterator.WriteType
   *
   * @param env A pointer to the Java environment
   * @param name The name of the enum field
   *
   * @return A reference to the enum field value or a nullptr if
   *     the enum field value could not be retrieved
   */
  static jobject getEnum(JNIEnv* env, const char name[]) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jfieldID jfid =
        env->GetStaticFieldID(jclazz, name,
            "Lorg/rocksdb/WBWIRocksIterator$WriteType;");
    if(env->ExceptionCheck()) {
      // exception occurred while getting field
      return nullptr;
    } else if(jfid == nullptr) {
      return nullptr;
    }

    jobject jwrite_type = env->GetStaticObjectField(jclazz, jfid);
    assert(jwrite_type != nullptr);
    return jwrite_type;
  }
};

// The portal class for org.rocksdb.WBWIRocksIterator.WriteEntry
class WriteEntryJni : public JavaClass {
 public:
  /**
   * Get the Java Class org.rocksdb.WBWIRocksIterator.WriteEntry
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
    static jclass getJClass(JNIEnv* env) {
      return JavaClass::getJClass(env, "org/rocksdb/WBWIRocksIterator$WriteEntry");
    }
};

// The portal class for org.rocksdb.InfoLogLevel
class InfoLogLevelJni : public JavaClass {
 public:
    /**
     * Get the DEBUG_LEVEL enum field value of InfoLogLevel
     *
     * @param env A pointer to the Java environment
     *
     * @return A reference to the enum field value or a nullptr if
     *     the enum field value could not be retrieved
     */
    static jobject DEBUG_LEVEL(JNIEnv* env) {
      return getEnum(env, "DEBUG_LEVEL");
    }

    /**
     * Get the INFO_LEVEL enum field value of InfoLogLevel
     *
     * @param env A pointer to the Java environment
     *
     * @return A reference to the enum field value or a nullptr if
     *     the enum field value could not be retrieved
     */
    static jobject INFO_LEVEL(JNIEnv* env) {
      return getEnum(env, "INFO_LEVEL");
    }

    /**
     * Get the WARN_LEVEL enum field value of InfoLogLevel
     *
     * @param env A pointer to the Java environment
     *
     * @return A reference to the enum field value or a nullptr if
     *     the enum field value could not be retrieved
     */
    static jobject WARN_LEVEL(JNIEnv* env) {
      return getEnum(env, "WARN_LEVEL");
    }

    /**
     * Get the ERROR_LEVEL enum field value of InfoLogLevel
     *
     * @param env A pointer to the Java environment
     *
     * @return A reference to the enum field value or a nullptr if
     *     the enum field value could not be retrieved
     */
    static jobject ERROR_LEVEL(JNIEnv* env) {
      return getEnum(env, "ERROR_LEVEL");
    }

    /**
     * Get the FATAL_LEVEL enum field value of InfoLogLevel
     *
     * @param env A pointer to the Java environment
     *
     * @return A reference to the enum field value or a nullptr if
     *     the enum field value could not be retrieved
     */
    static jobject FATAL_LEVEL(JNIEnv* env) {
      return getEnum(env, "FATAL_LEVEL");
    }

    /**
     * Get the HEADER_LEVEL enum field value of InfoLogLevel
     *
     * @param env A pointer to the Java environment
     *
     * @return A reference to the enum field value or a nullptr if
     *     the enum field value could not be retrieved
     */
    static jobject HEADER_LEVEL(JNIEnv* env) {
      return getEnum(env, "HEADER_LEVEL");
    }

 private:
  /**
   * Get the Java Class org.rocksdb.InfoLogLevel
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/InfoLogLevel");
  }

  /**
   * Get an enum field of org.rocksdb.InfoLogLevel
   *
   * @param env A pointer to the Java environment
   * @param name The name of the enum field
   *
   * @return A reference to the enum field value or a nullptr if
   *     the enum field value could not be retrieved
   */
  static jobject getEnum(JNIEnv* env, const char name[]) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jfieldID jfid =
        env->GetStaticFieldID(jclazz, name, "Lorg/rocksdb/InfoLogLevel;");
    if(env->ExceptionCheck()) {
      // exception occurred while getting field
      return nullptr;
    } else if(jfid == nullptr) {
      return nullptr;
    }

    jobject jinfo_log_level = env->GetStaticObjectField(jclazz, jfid);
    assert(jinfo_log_level != nullptr);
    return jinfo_log_level;
  }
};

// The portal class for org.rocksdb.Logger
class LoggerJni : public RocksDBNativeClass<
    std::shared_ptr<rocksdb::LoggerJniCallback>*, LoggerJni> {
 public:
  /**
   * Get the Java Class org/rocksdb/Logger
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/Logger");
  }

  /**
   * Get the Java Method: Logger#log
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getLogMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid =
        env->GetMethodID(jclazz, "log",
            "(Lorg/rocksdb/InfoLogLevel;Ljava/lang/String;)V");
    assert(mid != nullptr);
    return mid;
  }
};

// The portal class for org.rocksdb.TransactionLogIterator.BatchResult
class BatchResultJni : public JavaClass {
  public:
  /**
   * Get the Java Class org.rocksdb.TransactionLogIterator.BatchResult
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env,
        "org/rocksdb/TransactionLogIterator$BatchResult");
  }

  /**
   * Create a new Java org.rocksdb.TransactionLogIterator.BatchResult object
   * with the same properties as the provided C++ rocksdb::BatchResult object
   *
   * @param env A pointer to the Java environment
   * @param batch_result The rocksdb::BatchResult object
   *
   * @return A reference to a Java
   *     org.rocksdb.TransactionLogIterator.BatchResult object,
   *     or nullptr if an an exception occurs
   */
  static jobject construct(JNIEnv* env,
      rocksdb::BatchResult& batch_result) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID mid = env->GetMethodID(
      jclazz, "<init>", "(JJ)V");
    if(mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    jobject jbatch_result = env->NewObject(jclazz, mid,
      batch_result.sequence, batch_result.writeBatchPtr.get());
    if(jbatch_result == nullptr) {
      // exception thrown: InstantiationException or OutOfMemoryError
      return nullptr;
    }

    batch_result.writeBatchPtr.release();
    return jbatch_result;
  }
};

// The portal class for org.rocksdb.BottommostLevelCompaction
class BottommostLevelCompactionJni {
 public:
  // Returns the equivalent org.rocksdb.BottommostLevelCompaction for the provided
  // C++ rocksdb::BottommostLevelCompaction enum
  static jint toJavaBottommostLevelCompaction(
      const rocksdb::BottommostLevelCompaction& bottommost_level_compaction) {
    switch(bottommost_level_compaction) {
      case rocksdb::BottommostLevelCompaction::kSkip:
        return 0x0;
      case rocksdb::BottommostLevelCompaction::kIfHaveCompactionFilter:
        return 0x1;
      case rocksdb::BottommostLevelCompaction::kForce:
        return 0x2;
      default:
        return 0x7F;  // undefined
    }
  }

  // Returns the equivalent C++ rocksdb::BottommostLevelCompaction enum for the
  // provided Java org.rocksdb.BottommostLevelCompaction
  static rocksdb::BottommostLevelCompaction toCppBottommostLevelCompaction(
      jint bottommost_level_compaction) {
    switch(bottommost_level_compaction) {
      case 0x0:
        return rocksdb::BottommostLevelCompaction::kSkip;
      case 0x1:
        return rocksdb::BottommostLevelCompaction::kIfHaveCompactionFilter;
      case 0x2:
        return rocksdb::BottommostLevelCompaction::kForce;
      default:
        // undefined/default
        return rocksdb::BottommostLevelCompaction::kIfHaveCompactionFilter;
    }
  }
};

// The portal class for org.rocksdb.CompactionStopStyle
class CompactionStopStyleJni {
 public:
  // Returns the equivalent org.rocksdb.CompactionStopStyle for the provided
  // C++ rocksdb::CompactionStopStyle enum
  static jbyte toJavaCompactionStopStyle(
      const rocksdb::CompactionStopStyle& compaction_stop_style) {
    switch(compaction_stop_style) {
      case rocksdb::CompactionStopStyle::kCompactionStopStyleSimilarSize:
        return 0x0;
      case rocksdb::CompactionStopStyle::kCompactionStopStyleTotalSize:
        return 0x1;
      default:
        return 0x7F;  // undefined
    }
  }

  // Returns the equivalent C++ rocksdb::CompactionStopStyle enum for the
  // provided Java org.rocksdb.CompactionStopStyle
  static rocksdb::CompactionStopStyle toCppCompactionStopStyle(
      jbyte jcompaction_stop_style) {
    switch(jcompaction_stop_style) {
      case 0x0:
        return rocksdb::CompactionStopStyle::kCompactionStopStyleSimilarSize;
      case 0x1:
        return rocksdb::CompactionStopStyle::kCompactionStopStyleTotalSize;
      default:
        // undefined/default
        return rocksdb::CompactionStopStyle::kCompactionStopStyleSimilarSize;
    }
  }
};

// The portal class for org.rocksdb.CompressionType
class CompressionTypeJni {
 public:
  // Returns the equivalent org.rocksdb.CompressionType for the provided
  // C++ rocksdb::CompressionType enum
  static jbyte toJavaCompressionType(
      const rocksdb::CompressionType& compression_type) {
    switch(compression_type) {
      case rocksdb::CompressionType::kNoCompression:
        return 0x0;
      case rocksdb::CompressionType::kSnappyCompression:
        return 0x1;
      case rocksdb::CompressionType::kZlibCompression:
        return 0x2;
      case rocksdb::CompressionType::kBZip2Compression:
        return 0x3;
      case rocksdb::CompressionType::kLZ4Compression:
        return 0x4;
      case rocksdb::CompressionType::kLZ4HCCompression:
        return 0x5;
      case rocksdb::CompressionType::kXpressCompression:
        return 0x6;
      case rocksdb::CompressionType::kZSTD:
        return 0x7;
      case rocksdb::CompressionType::kDisableCompressionOption:
      default:
        return 0x7F;
    }
  }

  // Returns the equivalent C++ rocksdb::CompressionType enum for the
  // provided Java org.rocksdb.CompressionType
  static rocksdb::CompressionType toCppCompressionType(
      jbyte jcompression_type) {
    switch(jcompression_type) {
      case 0x0:
        return rocksdb::CompressionType::kNoCompression;
      case 0x1:
        return rocksdb::CompressionType::kSnappyCompression;
      case 0x2:
        return rocksdb::CompressionType::kZlibCompression;
      case 0x3:
        return rocksdb::CompressionType::kBZip2Compression;
      case 0x4:
        return rocksdb::CompressionType::kLZ4Compression;
      case 0x5:
        return rocksdb::CompressionType::kLZ4HCCompression;
      case 0x6:
        return rocksdb::CompressionType::kXpressCompression;
      case 0x7:
        return rocksdb::CompressionType::kZSTD;
      case 0x7F:
      default:
        return rocksdb::CompressionType::kDisableCompressionOption;
    }
  }
};

// The portal class for org.rocksdb.CompactionPriority
class CompactionPriorityJni {
 public:
  // Returns the equivalent org.rocksdb.CompactionPriority for the provided
  // C++ rocksdb::CompactionPri enum
  static jbyte toJavaCompactionPriority(
      const rocksdb::CompactionPri& compaction_priority) {
    switch(compaction_priority) {
      case rocksdb::CompactionPri::kByCompensatedSize:
        return 0x0;
      case rocksdb::CompactionPri::kOldestLargestSeqFirst:
        return 0x1;
      case rocksdb::CompactionPri::kOldestSmallestSeqFirst:
        return 0x2;
      case rocksdb::CompactionPri::kMinOverlappingRatio:
        return 0x3;
      default:
        return 0x0;  // undefined
    }
  }

  // Returns the equivalent C++ rocksdb::CompactionPri enum for the
  // provided Java org.rocksdb.CompactionPriority
  static rocksdb::CompactionPri toCppCompactionPriority(
      jbyte jcompaction_priority) {
    switch(jcompaction_priority) {
      case 0x0:
        return rocksdb::CompactionPri::kByCompensatedSize;
      case 0x1:
        return rocksdb::CompactionPri::kOldestLargestSeqFirst;
      case 0x2:
        return rocksdb::CompactionPri::kOldestSmallestSeqFirst;
      case 0x3:
        return rocksdb::CompactionPri::kMinOverlappingRatio;
      default:
        // undefined/default
        return rocksdb::CompactionPri::kByCompensatedSize;
    }
  }
};

// The portal class for org.rocksdb.AccessHint
class AccessHintJni {
 public:
  // Returns the equivalent org.rocksdb.AccessHint for the provided
  // C++ rocksdb::DBOptions::AccessHint enum
  static jbyte toJavaAccessHint(
      const rocksdb::DBOptions::AccessHint& access_hint) {
    switch(access_hint) {
      case rocksdb::DBOptions::AccessHint::NONE:
        return 0x0;
      case rocksdb::DBOptions::AccessHint::NORMAL:
        return 0x1;
      case rocksdb::DBOptions::AccessHint::SEQUENTIAL:
        return 0x2;
      case rocksdb::DBOptions::AccessHint::WILLNEED:
        return 0x3;
      default:
        // undefined/default
        return 0x1;
    }
  }

  // Returns the equivalent C++ rocksdb::DBOptions::AccessHint enum for the
  // provided Java org.rocksdb.AccessHint
  static rocksdb::DBOptions::AccessHint toCppAccessHint(jbyte jaccess_hint) {
    switch(jaccess_hint) {
      case 0x0:
        return rocksdb::DBOptions::AccessHint::NONE;
      case 0x1:
        return rocksdb::DBOptions::AccessHint::NORMAL;
      case 0x2:
        return rocksdb::DBOptions::AccessHint::SEQUENTIAL;
      case 0x3:
        return rocksdb::DBOptions::AccessHint::WILLNEED;
      default:
        // undefined/default
        return rocksdb::DBOptions::AccessHint::NORMAL;
    }
  }
};

// The portal class for org.rocksdb.WALRecoveryMode
class WALRecoveryModeJni {
 public:
  // Returns the equivalent org.rocksdb.WALRecoveryMode for the provided
  // C++ rocksdb::WALRecoveryMode enum
  static jbyte toJavaWALRecoveryMode(
      const rocksdb::WALRecoveryMode& wal_recovery_mode) {
    switch(wal_recovery_mode) {
      case rocksdb::WALRecoveryMode::kTolerateCorruptedTailRecords:
        return 0x0;
      case rocksdb::WALRecoveryMode::kAbsoluteConsistency:
        return 0x1;
      case rocksdb::WALRecoveryMode::kPointInTimeRecovery:
        return 0x2;
      case rocksdb::WALRecoveryMode::kSkipAnyCorruptedRecords:
        return 0x3;
      default:
        // undefined/default
        return 0x2;
    }
  }

  // Returns the equivalent C++ rocksdb::WALRecoveryMode enum for the
  // provided Java org.rocksdb.WALRecoveryMode
  static rocksdb::WALRecoveryMode toCppWALRecoveryMode(jbyte jwal_recovery_mode) {
    switch(jwal_recovery_mode) {
      case 0x0:
        return rocksdb::WALRecoveryMode::kTolerateCorruptedTailRecords;
      case 0x1:
        return rocksdb::WALRecoveryMode::kAbsoluteConsistency;
      case 0x2:
        return rocksdb::WALRecoveryMode::kPointInTimeRecovery;
      case 0x3:
        return rocksdb::WALRecoveryMode::kSkipAnyCorruptedRecords;
      default:
        // undefined/default
        return rocksdb::WALRecoveryMode::kPointInTimeRecovery;
    }
  }
};

// The portal class for org.rocksdb.TickerType
class TickerTypeJni {
 public:
  // Returns the equivalent org.rocksdb.TickerType for the provided
  // C++ rocksdb::Tickers enum
  static jbyte toJavaTickerType(
      const rocksdb::Tickers& tickers) {
    switch(tickers) {
      case rocksdb::Tickers::BLOCK_CACHE_MISS:
        return 0x0;
      case rocksdb::Tickers::BLOCK_CACHE_HIT:
        return 0x1;
      case rocksdb::Tickers::BLOCK_CACHE_ADD:
        return 0x2;
      case rocksdb::Tickers::BLOCK_CACHE_ADD_FAILURES:
        return 0x3;
      case rocksdb::Tickers::BLOCK_CACHE_INDEX_MISS:
        return 0x4;
      case rocksdb::Tickers::BLOCK_CACHE_INDEX_HIT:
        return 0x5;
      case rocksdb::Tickers::BLOCK_CACHE_INDEX_ADD:
        return 0x6;
      case rocksdb::Tickers::BLOCK_CACHE_INDEX_BYTES_INSERT:
        return 0x7;
      case rocksdb::Tickers::BLOCK_CACHE_INDEX_BYTES_EVICT:
        return 0x8;
      case rocksdb::Tickers::BLOCK_CACHE_FILTER_MISS:
        return 0x9;
      case rocksdb::Tickers::BLOCK_CACHE_FILTER_HIT:
        return 0xA;
      case rocksdb::Tickers::BLOCK_CACHE_FILTER_ADD:
        return 0xB;
      case rocksdb::Tickers::BLOCK_CACHE_FILTER_BYTES_INSERT:
        return 0xC;
      case rocksdb::Tickers::BLOCK_CACHE_FILTER_BYTES_EVICT:
        return 0xD;
      case rocksdb::Tickers::BLOCK_CACHE_DATA_MISS:
        return 0xE;
      case rocksdb::Tickers::BLOCK_CACHE_DATA_HIT:
        return 0xF;
      case rocksdb::Tickers::BLOCK_CACHE_DATA_ADD:
        return 0x10;
      case rocksdb::Tickers::BLOCK_CACHE_DATA_BYTES_INSERT:
        return 0x11;
      case rocksdb::Tickers::BLOCK_CACHE_BYTES_READ:
        return 0x12;
      case rocksdb::Tickers::BLOCK_CACHE_BYTES_WRITE:
        return 0x13;
      case rocksdb::Tickers::BLOOM_FILTER_USEFUL:
        return 0x14;
      case rocksdb::Tickers::PERSISTENT_CACHE_HIT:
        return 0x15;
      case rocksdb::Tickers::PERSISTENT_CACHE_MISS:
        return 0x16;
      case rocksdb::Tickers::SIM_BLOCK_CACHE_HIT:
        return 0x17;
      case rocksdb::Tickers::SIM_BLOCK_CACHE_MISS:
        return 0x18;
      case rocksdb::Tickers::MEMTABLE_HIT:
        return 0x19;
      case rocksdb::Tickers::MEMTABLE_MISS:
        return 0x1A;
      case rocksdb::Tickers::GET_HIT_L0:
        return 0x1B;
      case rocksdb::Tickers::GET_HIT_L1:
        return 0x1C;
      case rocksdb::Tickers::GET_HIT_L2_AND_UP:
        return 0x1D;
      case rocksdb::Tickers::COMPACTION_KEY_DROP_NEWER_ENTRY:
        return 0x1E;
      case rocksdb::Tickers::COMPACTION_KEY_DROP_OBSOLETE:
        return 0x1F;
      case rocksdb::Tickers::COMPACTION_KEY_DROP_RANGE_DEL:
        return 0x20;
      case rocksdb::Tickers::COMPACTION_KEY_DROP_USER:
        return 0x21;
      case rocksdb::Tickers::COMPACTION_RANGE_DEL_DROP_OBSOLETE:
        return 0x22;
      case rocksdb::Tickers::NUMBER_KEYS_WRITTEN:
        return 0x23;
      case rocksdb::Tickers::NUMBER_KEYS_READ:
        return 0x24;
      case rocksdb::Tickers::NUMBER_KEYS_UPDATED:
        return 0x25;
      case rocksdb::Tickers::BYTES_WRITTEN:
        return 0x26;
      case rocksdb::Tickers::BYTES_READ:
        return 0x27;
      case rocksdb::Tickers::NUMBER_DB_SEEK:
        return 0x28;
      case rocksdb::Tickers::NUMBER_DB_NEXT:
        return 0x29;
      case rocksdb::Tickers::NUMBER_DB_PREV:
        return 0x2A;
      case rocksdb::Tickers::NUMBER_DB_SEEK_FOUND:
        return 0x2B;
      case rocksdb::Tickers::NUMBER_DB_NEXT_FOUND:
        return 0x2C;
      case rocksdb::Tickers::NUMBER_DB_PREV_FOUND:
        return 0x2D;
      case rocksdb::Tickers::ITER_BYTES_READ:
        return 0x2E;
      case rocksdb::Tickers::NO_FILE_CLOSES:
        return 0x2F;
      case rocksdb::Tickers::NO_FILE_OPENS:
        return 0x30;
      case rocksdb::Tickers::NO_FILE_ERRORS:
        return 0x31;
      case rocksdb::Tickers::STALL_L0_SLOWDOWN_MICROS:
        return 0x32;
      case rocksdb::Tickers::STALL_MEMTABLE_COMPACTION_MICROS:
        return 0x33;
      case rocksdb::Tickers::STALL_L0_NUM_FILES_MICROS:
        return 0x34;
      case rocksdb::Tickers::STALL_MICROS:
        return 0x35;
      case rocksdb::Tickers::DB_MUTEX_WAIT_MICROS:
        return 0x36;
      case rocksdb::Tickers::RATE_LIMIT_DELAY_MILLIS:
        return 0x37;
      case rocksdb::Tickers::NO_ITERATORS:
        return 0x38;
      case rocksdb::Tickers::NUMBER_MULTIGET_CALLS:
        return 0x39;
      case rocksdb::Tickers::NUMBER_MULTIGET_KEYS_READ:
        return 0x3A;
      case rocksdb::Tickers::NUMBER_MULTIGET_BYTES_READ:
        return 0x3B;
      case rocksdb::Tickers::NUMBER_FILTERED_DELETES:
        return 0x3C;
      case rocksdb::Tickers::NUMBER_MERGE_FAILURES:
        return 0x3D;
      case rocksdb::Tickers::BLOOM_FILTER_PREFIX_CHECKED:
        return 0x3E;
      case rocksdb::Tickers::BLOOM_FILTER_PREFIX_USEFUL:
        return 0x3F;
      case rocksdb::Tickers::NUMBER_OF_RESEEKS_IN_ITERATION:
        return 0x40;
      case rocksdb::Tickers::GET_UPDATES_SINCE_CALLS:
        return 0x41;
      case rocksdb::Tickers::BLOCK_CACHE_COMPRESSED_MISS:
        return 0x42;
      case rocksdb::Tickers::BLOCK_CACHE_COMPRESSED_HIT:
        return 0x43;
      case rocksdb::Tickers::BLOCK_CACHE_COMPRESSED_ADD:
        return 0x44;
      case rocksdb::Tickers::BLOCK_CACHE_COMPRESSED_ADD_FAILURES:
        return 0x45;
      case rocksdb::Tickers::WAL_FILE_SYNCED:
        return 0x46;
      case rocksdb::Tickers::WAL_FILE_BYTES:
        return 0x47;
      case rocksdb::Tickers::WRITE_DONE_BY_SELF:
        return 0x48;
      case rocksdb::Tickers::WRITE_DONE_BY_OTHER:
        return 0x49;
      case rocksdb::Tickers::WRITE_TIMEDOUT:
        return 0x4A;
      case rocksdb::Tickers::WRITE_WITH_WAL:
        return 0x4B;
      case rocksdb::Tickers::COMPACT_READ_BYTES:
        return 0x4C;
      case rocksdb::Tickers::COMPACT_WRITE_BYTES:
        return 0x4D;
      case rocksdb::Tickers::FLUSH_WRITE_BYTES:
        return 0x4E;
      case rocksdb::Tickers::NUMBER_DIRECT_LOAD_TABLE_PROPERTIES:
        return 0x4F;
      case rocksdb::Tickers::NUMBER_SUPERVERSION_ACQUIRES:
        return 0x50;
      case rocksdb::Tickers::NUMBER_SUPERVERSION_RELEASES:
        return 0x51;
      case rocksdb::Tickers::NUMBER_SUPERVERSION_CLEANUPS:
        return 0x52;
      case rocksdb::Tickers::NUMBER_BLOCK_COMPRESSED:
        return 0x53;
      case rocksdb::Tickers::NUMBER_BLOCK_DECOMPRESSED:
        return 0x54;
      case rocksdb::Tickers::NUMBER_BLOCK_NOT_COMPRESSED:
        return 0x55;
      case rocksdb::Tickers::MERGE_OPERATION_TOTAL_TIME:
        return 0x56;
      case rocksdb::Tickers::FILTER_OPERATION_TOTAL_TIME:
        return 0x57;
      case rocksdb::Tickers::ROW_CACHE_HIT:
        return 0x58;
      case rocksdb::Tickers::ROW_CACHE_MISS:
        return 0x59;
      case rocksdb::Tickers::READ_AMP_ESTIMATE_USEFUL_BYTES:
        return 0x5A;
      case rocksdb::Tickers::READ_AMP_TOTAL_READ_BYTES:
        return 0x5B;
      case rocksdb::Tickers::NUMBER_RATE_LIMITER_DRAINS:
        return 0x5C;
      case rocksdb::Tickers::NUMBER_ITER_SKIP:
        return 0x5D;
      case rocksdb::Tickers::NUMBER_MULTIGET_KEYS_FOUND:
        return 0x5E;
      case rocksdb::Tickers::NO_ITERATOR_CREATED:
        // -0x01 to fixate the new value that incorrectly changed TICKER_ENUM_MAX.
        return -0x01;
      case rocksdb::Tickers::NO_ITERATOR_DELETED:
        return 0x60;
      case rocksdb::Tickers::COMPACTION_OPTIMIZED_DEL_DROP_OBSOLETE:
        return 0x61;
      case rocksdb::Tickers::COMPACTION_CANCELLED:
        return 0x62;
      case rocksdb::Tickers::BLOOM_FILTER_FULL_POSITIVE:
        return 0x63;
      case rocksdb::Tickers::BLOOM_FILTER_FULL_TRUE_POSITIVE:
        return 0x64;
      case rocksdb::Tickers::BLOB_DB_NUM_PUT:
        return 0x65;
      case rocksdb::Tickers::BLOB_DB_NUM_WRITE:
        return 0x66;
      case rocksdb::Tickers::BLOB_DB_NUM_GET:
        return 0x67;
      case rocksdb::Tickers::BLOB_DB_NUM_MULTIGET:
        return 0x68;
      case rocksdb::Tickers::BLOB_DB_NUM_SEEK:
        return 0x69;
      case rocksdb::Tickers::BLOB_DB_NUM_NEXT:
        return 0x6A;
      case rocksdb::Tickers::BLOB_DB_NUM_PREV:
        return 0x6B;
      case rocksdb::Tickers::BLOB_DB_NUM_KEYS_WRITTEN:
        return 0x6C;
      case rocksdb::Tickers::BLOB_DB_NUM_KEYS_READ:
        return 0x6D;
      case rocksdb::Tickers::BLOB_DB_BYTES_WRITTEN:
        return 0x6E;
      case rocksdb::Tickers::BLOB_DB_BYTES_READ:
        return 0x6F;
      case rocksdb::Tickers::BLOB_DB_WRITE_INLINED:
        return 0x70;
      case rocksdb::Tickers::BLOB_DB_WRITE_INLINED_TTL:
        return 0x71;
      case rocksdb::Tickers::BLOB_DB_WRITE_BLOB:
        return 0x72;
      case rocksdb::Tickers::BLOB_DB_WRITE_BLOB_TTL:
        return 0x73;
      case rocksdb::Tickers::BLOB_DB_BLOB_FILE_BYTES_WRITTEN:
        return 0x74;
      case rocksdb::Tickers::BLOB_DB_BLOB_FILE_BYTES_READ:
        return 0x75;
      case rocksdb::Tickers::BLOB_DB_BLOB_FILE_SYNCED:
        return 0x76;
      case rocksdb::Tickers::BLOB_DB_BLOB_INDEX_EXPIRED_COUNT:
        return 0x77;
      case rocksdb::Tickers::BLOB_DB_BLOB_INDEX_EXPIRED_SIZE:
        return 0x78;
      case rocksdb::Tickers::BLOB_DB_BLOB_INDEX_EVICTED_COUNT:
        return 0x79;
      case rocksdb::Tickers::BLOB_DB_BLOB_INDEX_EVICTED_SIZE:
        return 0x7A;
      case rocksdb::Tickers::BLOB_DB_GC_NUM_FILES:
        return 0x7B;
      case rocksdb::Tickers::BLOB_DB_GC_NUM_NEW_FILES:
        return 0x7C;
      case rocksdb::Tickers::BLOB_DB_GC_FAILURES:
        return 0x7D;
      case rocksdb::Tickers::BLOB_DB_GC_NUM_KEYS_OVERWRITTEN:
        return 0x7E;
      case rocksdb::Tickers::BLOB_DB_GC_NUM_KEYS_EXPIRED:
        return 0x7F;
      case rocksdb::Tickers::BLOB_DB_GC_NUM_KEYS_RELOCATED:
        return -0x02;
      case rocksdb::Tickers::BLOB_DB_GC_BYTES_OVERWRITTEN:
        return -0x03;
      case rocksdb::Tickers::BLOB_DB_GC_BYTES_EXPIRED:
        return -0x04;
      case rocksdb::Tickers::BLOB_DB_GC_BYTES_RELOCATED:
        return -0x05;
      case rocksdb::Tickers::BLOB_DB_FIFO_NUM_FILES_EVICTED:
        return -0x06;
      case rocksdb::Tickers::BLOB_DB_FIFO_NUM_KEYS_EVICTED:
        return -0x07;
      case rocksdb::Tickers::BLOB_DB_FIFO_BYTES_EVICTED:
        return -0x08;
      case rocksdb::Tickers::TXN_PREPARE_MUTEX_OVERHEAD:
        return -0x09;
      case rocksdb::Tickers::TXN_OLD_COMMIT_MAP_MUTEX_OVERHEAD:
        return -0x0A;
      case rocksdb::Tickers::TXN_DUPLICATE_KEY_OVERHEAD:
        return -0x0B;
      case rocksdb::Tickers::TXN_SNAPSHOT_MUTEX_OVERHEAD:
        return -0x0C;
      case rocksdb::Tickers::TICKER_ENUM_MAX:
        // 0x5F for backwards compatibility on current minor version.
        return 0x5F;
      default:
        // undefined/default
        return 0x0;
    }
  }

  // Returns the equivalent C++ rocksdb::Tickers enum for the
  // provided Java org.rocksdb.TickerType
  static rocksdb::Tickers toCppTickers(jbyte jticker_type) {
    switch(jticker_type) {
      case 0x0:
        return rocksdb::Tickers::BLOCK_CACHE_MISS;
      case 0x1:
        return rocksdb::Tickers::BLOCK_CACHE_HIT;
      case 0x2:
        return rocksdb::Tickers::BLOCK_CACHE_ADD;
      case 0x3:
        return rocksdb::Tickers::BLOCK_CACHE_ADD_FAILURES;
      case 0x4:
        return rocksdb::Tickers::BLOCK_CACHE_INDEX_MISS;
      case 0x5:
        return rocksdb::Tickers::BLOCK_CACHE_INDEX_HIT;
      case 0x6:
        return rocksdb::Tickers::BLOCK_CACHE_INDEX_ADD;
      case 0x7:
        return rocksdb::Tickers::BLOCK_CACHE_INDEX_BYTES_INSERT;
      case 0x8:
        return rocksdb::Tickers::BLOCK_CACHE_INDEX_BYTES_EVICT;
      case 0x9:
        return rocksdb::Tickers::BLOCK_CACHE_FILTER_MISS;
      case 0xA:
        return rocksdb::Tickers::BLOCK_CACHE_FILTER_HIT;
      case 0xB:
        return rocksdb::Tickers::BLOCK_CACHE_FILTER_ADD;
      case 0xC:
        return rocksdb::Tickers::BLOCK_CACHE_FILTER_BYTES_INSERT;
      case 0xD:
        return rocksdb::Tickers::BLOCK_CACHE_FILTER_BYTES_EVICT;
      case 0xE:
        return rocksdb::Tickers::BLOCK_CACHE_DATA_MISS;
      case 0xF:
        return rocksdb::Tickers::BLOCK_CACHE_DATA_HIT;
      case 0x10:
        return rocksdb::Tickers::BLOCK_CACHE_DATA_ADD;
      case 0x11:
        return rocksdb::Tickers::BLOCK_CACHE_DATA_BYTES_INSERT;
      case 0x12:
        return rocksdb::Tickers::BLOCK_CACHE_BYTES_READ;
      case 0x13:
        return rocksdb::Tickers::BLOCK_CACHE_BYTES_WRITE;
      case 0x14:
        return rocksdb::Tickers::BLOOM_FILTER_USEFUL;
      case 0x15:
        return rocksdb::Tickers::PERSISTENT_CACHE_HIT;
      case 0x16:
        return rocksdb::Tickers::PERSISTENT_CACHE_MISS;
      case 0x17:
        return rocksdb::Tickers::SIM_BLOCK_CACHE_HIT;
      case 0x18:
        return rocksdb::Tickers::SIM_BLOCK_CACHE_MISS;
      case 0x19:
        return rocksdb::Tickers::MEMTABLE_HIT;
      case 0x1A:
        return rocksdb::Tickers::MEMTABLE_MISS;
      case 0x1B:
        return rocksdb::Tickers::GET_HIT_L0;
      case 0x1C:
        return rocksdb::Tickers::GET_HIT_L1;
      case 0x1D:
        return rocksdb::Tickers::GET_HIT_L2_AND_UP;
      case 0x1E:
        return rocksdb::Tickers::COMPACTION_KEY_DROP_NEWER_ENTRY;
      case 0x1F:
        return rocksdb::Tickers::COMPACTION_KEY_DROP_OBSOLETE;
      case 0x20:
        return rocksdb::Tickers::COMPACTION_KEY_DROP_RANGE_DEL;
      case 0x21:
        return rocksdb::Tickers::COMPACTION_KEY_DROP_USER;
      case 0x22:
        return rocksdb::Tickers::COMPACTION_RANGE_DEL_DROP_OBSOLETE;
      case 0x23:
        return rocksdb::Tickers::NUMBER_KEYS_WRITTEN;
      case 0x24:
        return rocksdb::Tickers::NUMBER_KEYS_READ;
      case 0x25:
        return rocksdb::Tickers::NUMBER_KEYS_UPDATED;
      case 0x26:
        return rocksdb::Tickers::BYTES_WRITTEN;
      case 0x27:
        return rocksdb::Tickers::BYTES_READ;
      case 0x28:
        return rocksdb::Tickers::NUMBER_DB_SEEK;
      case 0x29:
        return rocksdb::Tickers::NUMBER_DB_NEXT;
      case 0x2A:
        return rocksdb::Tickers::NUMBER_DB_PREV;
      case 0x2B:
        return rocksdb::Tickers::NUMBER_DB_SEEK_FOUND;
      case 0x2C:
        return rocksdb::Tickers::NUMBER_DB_NEXT_FOUND;
      case 0x2D:
        return rocksdb::Tickers::NUMBER_DB_PREV_FOUND;
      case 0x2E:
        return rocksdb::Tickers::ITER_BYTES_READ;
      case 0x2F:
        return rocksdb::Tickers::NO_FILE_CLOSES;
      case 0x30:
        return rocksdb::Tickers::NO_FILE_OPENS;
      case 0x31:
        return rocksdb::Tickers::NO_FILE_ERRORS;
      case 0x32:
        return rocksdb::Tickers::STALL_L0_SLOWDOWN_MICROS;
      case 0x33:
        return rocksdb::Tickers::STALL_MEMTABLE_COMPACTION_MICROS;
      case 0x34:
        return rocksdb::Tickers::STALL_L0_NUM_FILES_MICROS;
      case 0x35:
        return rocksdb::Tickers::STALL_MICROS;
      case 0x36:
        return rocksdb::Tickers::DB_MUTEX_WAIT_MICROS;
      case 0x37:
        return rocksdb::Tickers::RATE_LIMIT_DELAY_MILLIS;
      case 0x38:
        return rocksdb::Tickers::NO_ITERATORS;
      case 0x39:
        return rocksdb::Tickers::NUMBER_MULTIGET_CALLS;
      case 0x3A:
        return rocksdb::Tickers::NUMBER_MULTIGET_KEYS_READ;
      case 0x3B:
        return rocksdb::Tickers::NUMBER_MULTIGET_BYTES_READ;
      case 0x3C:
        return rocksdb::Tickers::NUMBER_FILTERED_DELETES;
      case 0x3D:
        return rocksdb::Tickers::NUMBER_MERGE_FAILURES;
      case 0x3E:
        return rocksdb::Tickers::BLOOM_FILTER_PREFIX_CHECKED;
      case 0x3F:
        return rocksdb::Tickers::BLOOM_FILTER_PREFIX_USEFUL;
      case 0x40:
        return rocksdb::Tickers::NUMBER_OF_RESEEKS_IN_ITERATION;
      case 0x41:
        return rocksdb::Tickers::GET_UPDATES_SINCE_CALLS;
      case 0x42:
        return rocksdb::Tickers::BLOCK_CACHE_COMPRESSED_MISS;
      case 0x43:
        return rocksdb::Tickers::BLOCK_CACHE_COMPRESSED_HIT;
      case 0x44:
        return rocksdb::Tickers::BLOCK_CACHE_COMPRESSED_ADD;
      case 0x45:
        return rocksdb::Tickers::BLOCK_CACHE_COMPRESSED_ADD_FAILURES;
      case 0x46:
        return rocksdb::Tickers::WAL_FILE_SYNCED;
      case 0x47:
        return rocksdb::Tickers::WAL_FILE_BYTES;
      case 0x48:
        return rocksdb::Tickers::WRITE_DONE_BY_SELF;
      case 0x49:
        return rocksdb::Tickers::WRITE_DONE_BY_OTHER;
      case 0x4A:
        return rocksdb::Tickers::WRITE_TIMEDOUT;
      case 0x4B:
        return rocksdb::Tickers::WRITE_WITH_WAL;
      case 0x4C:
        return rocksdb::Tickers::COMPACT_READ_BYTES;
      case 0x4D:
        return rocksdb::Tickers::COMPACT_WRITE_BYTES;
      case 0x4E:
        return rocksdb::Tickers::FLUSH_WRITE_BYTES;
      case 0x4F:
        return rocksdb::Tickers::NUMBER_DIRECT_LOAD_TABLE_PROPERTIES;
      case 0x50:
        return rocksdb::Tickers::NUMBER_SUPERVERSION_ACQUIRES;
      case 0x51:
        return rocksdb::Tickers::NUMBER_SUPERVERSION_RELEASES;
      case 0x52:
        return rocksdb::Tickers::NUMBER_SUPERVERSION_CLEANUPS;
      case 0x53:
        return rocksdb::Tickers::NUMBER_BLOCK_COMPRESSED;
      case 0x54:
        return rocksdb::Tickers::NUMBER_BLOCK_DECOMPRESSED;
      case 0x55:
        return rocksdb::Tickers::NUMBER_BLOCK_NOT_COMPRESSED;
      case 0x56:
        return rocksdb::Tickers::MERGE_OPERATION_TOTAL_TIME;
      case 0x57:
        return rocksdb::Tickers::FILTER_OPERATION_TOTAL_TIME;
      case 0x58:
        return rocksdb::Tickers::ROW_CACHE_HIT;
      case 0x59:
        return rocksdb::Tickers::ROW_CACHE_MISS;
      case 0x5A:
        return rocksdb::Tickers::READ_AMP_ESTIMATE_USEFUL_BYTES;
      case 0x5B:
        return rocksdb::Tickers::READ_AMP_TOTAL_READ_BYTES;
      case 0x5C:
        return rocksdb::Tickers::NUMBER_RATE_LIMITER_DRAINS;
      case 0x5D:
        return rocksdb::Tickers::NUMBER_ITER_SKIP;
      case 0x5E:
        return rocksdb::Tickers::NUMBER_MULTIGET_KEYS_FOUND;
      case -0x01:
        // -0x01 to fixate the new value that incorrectly changed TICKER_ENUM_MAX.
        return rocksdb::Tickers::NO_ITERATOR_CREATED;
      case 0x60:
        return rocksdb::Tickers::NO_ITERATOR_DELETED;
      case 0x61:
        return rocksdb::Tickers::COMPACTION_OPTIMIZED_DEL_DROP_OBSOLETE;
      case 0x62:
        return rocksdb::Tickers::COMPACTION_CANCELLED;
      case 0x63:
        return rocksdb::Tickers::BLOOM_FILTER_FULL_POSITIVE;
      case 0x64:
        return rocksdb::Tickers::BLOOM_FILTER_FULL_TRUE_POSITIVE;
      case 0x65:
        return rocksdb::Tickers::BLOB_DB_NUM_PUT;
      case 0x66:
        return rocksdb::Tickers::BLOB_DB_NUM_WRITE;
      case 0x67:
        return rocksdb::Tickers::BLOB_DB_NUM_GET;
      case 0x68:
        return rocksdb::Tickers::BLOB_DB_NUM_MULTIGET;
      case 0x69:
        return rocksdb::Tickers::BLOB_DB_NUM_SEEK;
      case 0x6A:
        return rocksdb::Tickers::BLOB_DB_NUM_NEXT;
      case 0x6B:
        return rocksdb::Tickers::BLOB_DB_NUM_PREV;
      case 0x6C:
        return rocksdb::Tickers::BLOB_DB_NUM_KEYS_WRITTEN;
      case 0x6D:
        return rocksdb::Tickers::BLOB_DB_NUM_KEYS_READ;
      case 0x6E:
        return rocksdb::Tickers::BLOB_DB_BYTES_WRITTEN;
      case 0x6F:
        return rocksdb::Tickers::BLOB_DB_BYTES_READ;
      case 0x70:
        return rocksdb::Tickers::BLOB_DB_WRITE_INLINED;
      case 0x71:
        return rocksdb::Tickers::BLOB_DB_WRITE_INLINED_TTL;
      case 0x72:
        return rocksdb::Tickers::BLOB_DB_WRITE_BLOB;
      case 0x73:
        return rocksdb::Tickers::BLOB_DB_WRITE_BLOB_TTL;
      case 0x74:
        return rocksdb::Tickers::BLOB_DB_BLOB_FILE_BYTES_WRITTEN;
      case 0x75:
        return rocksdb::Tickers::BLOB_DB_BLOB_FILE_BYTES_READ;
      case 0x76:
        return rocksdb::Tickers::BLOB_DB_BLOB_FILE_SYNCED;
      case 0x77:
        return rocksdb::Tickers::BLOB_DB_BLOB_INDEX_EXPIRED_COUNT;
      case 0x78:
        return rocksdb::Tickers::BLOB_DB_BLOB_INDEX_EXPIRED_SIZE;
      case 0x79:
        return rocksdb::Tickers::BLOB_DB_BLOB_INDEX_EVICTED_COUNT;
      case 0x7A:
        return rocksdb::Tickers::BLOB_DB_BLOB_INDEX_EVICTED_SIZE;
      case 0x7B:
        return rocksdb::Tickers::BLOB_DB_GC_NUM_FILES;
      case 0x7C:
        return rocksdb::Tickers::BLOB_DB_GC_NUM_NEW_FILES;
      case 0x7D:
        return rocksdb::Tickers::BLOB_DB_GC_FAILURES;
      case 0x7E:
        return rocksdb::Tickers::BLOB_DB_GC_NUM_KEYS_OVERWRITTEN;
      case 0x7F:
        return rocksdb::Tickers::BLOB_DB_GC_NUM_KEYS_EXPIRED;
      case -0x02:
        return rocksdb::Tickers::BLOB_DB_GC_NUM_KEYS_RELOCATED;
      case -0x03:
        return rocksdb::Tickers::BLOB_DB_GC_BYTES_OVERWRITTEN;
      case -0x04:
        return rocksdb::Tickers::BLOB_DB_GC_BYTES_EXPIRED;
      case -0x05:
        return rocksdb::Tickers::BLOB_DB_GC_BYTES_RELOCATED;
      case -0x06:
        return rocksdb::Tickers::BLOB_DB_FIFO_NUM_FILES_EVICTED;
      case -0x07:
        return rocksdb::Tickers::BLOB_DB_FIFO_NUM_KEYS_EVICTED;
      case -0x08:
        return rocksdb::Tickers::BLOB_DB_FIFO_BYTES_EVICTED;
      case -0x09:
        return rocksdb::Tickers::TXN_PREPARE_MUTEX_OVERHEAD;
      case -0x0A:
        return rocksdb::Tickers::TXN_OLD_COMMIT_MAP_MUTEX_OVERHEAD;
      case -0x0B:
        return rocksdb::Tickers::TXN_DUPLICATE_KEY_OVERHEAD;
      case -0x0C:
        return rocksdb::Tickers::TXN_SNAPSHOT_MUTEX_OVERHEAD;
      case 0x5F:
        // 0x5F for backwards compatibility on current minor version.
        return rocksdb::Tickers::TICKER_ENUM_MAX;

      default:
        // undefined/default
        return rocksdb::Tickers::BLOCK_CACHE_MISS;
    }
  }
};

// The portal class for org.rocksdb.HistogramType
class HistogramTypeJni {
 public:
  // Returns the equivalent org.rocksdb.HistogramType for the provided
  // C++ rocksdb::Histograms enum
  static jbyte toJavaHistogramsType(
      const rocksdb::Histograms& histograms) {
    switch(histograms) {
      case rocksdb::Histograms::DB_GET:
        return 0x0;
      case rocksdb::Histograms::DB_WRITE:
        return 0x1;
      case rocksdb::Histograms::COMPACTION_TIME:
        return 0x2;
      case rocksdb::Histograms::SUBCOMPACTION_SETUP_TIME:
        return 0x3;
      case rocksdb::Histograms::TABLE_SYNC_MICROS:
        return 0x4;
      case rocksdb::Histograms::COMPACTION_OUTFILE_SYNC_MICROS:
        return 0x5;
      case rocksdb::Histograms::WAL_FILE_SYNC_MICROS:
        return 0x6;
      case rocksdb::Histograms::MANIFEST_FILE_SYNC_MICROS:
        return 0x7;
      case rocksdb::Histograms::TABLE_OPEN_IO_MICROS:
        return 0x8;
      case rocksdb::Histograms::DB_MULTIGET:
        return 0x9;
      case rocksdb::Histograms::READ_BLOCK_COMPACTION_MICROS:
        return 0xA;
      case rocksdb::Histograms::READ_BLOCK_GET_MICROS:
        return 0xB;
      case rocksdb::Histograms::WRITE_RAW_BLOCK_MICROS:
        return 0xC;
      case rocksdb::Histograms::STALL_L0_SLOWDOWN_COUNT:
        return 0xD;
      case rocksdb::Histograms::STALL_MEMTABLE_COMPACTION_COUNT:
        return 0xE;
      case rocksdb::Histograms::STALL_L0_NUM_FILES_COUNT:
        return 0xF;
      case rocksdb::Histograms::HARD_RATE_LIMIT_DELAY_COUNT:
        return 0x10;
      case rocksdb::Histograms::SOFT_RATE_LIMIT_DELAY_COUNT:
        return 0x11;
      case rocksdb::Histograms::NUM_FILES_IN_SINGLE_COMPACTION:
        return 0x12;
      case rocksdb::Histograms::DB_SEEK:
        return 0x13;
      case rocksdb::Histograms::WRITE_STALL:
        return 0x14;
      case rocksdb::Histograms::SST_READ_MICROS:
        return 0x15;
      case rocksdb::Histograms::NUM_SUBCOMPACTIONS_SCHEDULED:
        return 0x16;
      case rocksdb::Histograms::BYTES_PER_READ:
        return 0x17;
      case rocksdb::Histograms::BYTES_PER_WRITE:
        return 0x18;
      case rocksdb::Histograms::BYTES_PER_MULTIGET:
        return 0x19;
      case rocksdb::Histograms::BYTES_COMPRESSED:
        return 0x1A;
      case rocksdb::Histograms::BYTES_DECOMPRESSED:
        return 0x1B;
      case rocksdb::Histograms::COMPRESSION_TIMES_NANOS:
        return 0x1C;
      case rocksdb::Histograms::DECOMPRESSION_TIMES_NANOS:
        return 0x1D;
      case rocksdb::Histograms::READ_NUM_MERGE_OPERANDS:
        return 0x1E;
      // 0x20 to skip 0x1F so TICKER_ENUM_MAX remains unchanged for minor version compatibility.
      case rocksdb::Histograms::FLUSH_TIME:
        return 0x20;
      case rocksdb::Histograms::BLOB_DB_KEY_SIZE:
        return 0x21;
      case rocksdb::Histograms::BLOB_DB_VALUE_SIZE:
        return 0x22;
      case rocksdb::Histograms::BLOB_DB_WRITE_MICROS:
        return 0x23;
      case rocksdb::Histograms::BLOB_DB_GET_MICROS:
        return 0x24;
      case rocksdb::Histograms::BLOB_DB_MULTIGET_MICROS:
        return 0x25;
      case rocksdb::Histograms::BLOB_DB_SEEK_MICROS:
        return 0x26;
      case rocksdb::Histograms::BLOB_DB_NEXT_MICROS:
        return 0x27;
      case rocksdb::Histograms::BLOB_DB_PREV_MICROS:
        return 0x28;
      case rocksdb::Histograms::BLOB_DB_BLOB_FILE_WRITE_MICROS:
        return 0x29;
      case rocksdb::Histograms::BLOB_DB_BLOB_FILE_READ_MICROS:
        return 0x2A;
      case rocksdb::Histograms::BLOB_DB_BLOB_FILE_SYNC_MICROS:
        return 0x2B;
      case rocksdb::Histograms::BLOB_DB_GC_MICROS:
        return 0x2C;
      case rocksdb::Histograms::BLOB_DB_COMPRESSION_MICROS:
        return 0x2D;
      case rocksdb::Histograms::BLOB_DB_DECOMPRESSION_MICROS:
        return 0x2E;
      case rocksdb::Histograms::HISTOGRAM_ENUM_MAX:
        // 0x1F for backwards compatibility on current minor version.
        return 0x1F;

      default:
        // undefined/default
        return 0x0;
    }
  }

  // Returns the equivalent C++ rocksdb::Histograms enum for the
  // provided Java org.rocksdb.HistogramsType
  static rocksdb::Histograms toCppHistograms(jbyte jhistograms_type) {
    switch(jhistograms_type) {
      case 0x0:
        return rocksdb::Histograms::DB_GET;
      case 0x1:
        return rocksdb::Histograms::DB_WRITE;
      case 0x2:
        return rocksdb::Histograms::COMPACTION_TIME;
      case 0x3:
        return rocksdb::Histograms::SUBCOMPACTION_SETUP_TIME;
      case 0x4:
        return rocksdb::Histograms::TABLE_SYNC_MICROS;
      case 0x5:
        return rocksdb::Histograms::COMPACTION_OUTFILE_SYNC_MICROS;
      case 0x6:
        return rocksdb::Histograms::WAL_FILE_SYNC_MICROS;
      case 0x7:
        return rocksdb::Histograms::MANIFEST_FILE_SYNC_MICROS;
      case 0x8:
        return rocksdb::Histograms::TABLE_OPEN_IO_MICROS;
      case 0x9:
        return rocksdb::Histograms::DB_MULTIGET;
      case 0xA:
        return rocksdb::Histograms::READ_BLOCK_COMPACTION_MICROS;
      case 0xB:
        return rocksdb::Histograms::READ_BLOCK_GET_MICROS;
      case 0xC:
        return rocksdb::Histograms::WRITE_RAW_BLOCK_MICROS;
      case 0xD:
        return rocksdb::Histograms::STALL_L0_SLOWDOWN_COUNT;
      case 0xE:
        return rocksdb::Histograms::STALL_MEMTABLE_COMPACTION_COUNT;
      case 0xF:
        return rocksdb::Histograms::STALL_L0_NUM_FILES_COUNT;
      case 0x10:
        return rocksdb::Histograms::HARD_RATE_LIMIT_DELAY_COUNT;
      case 0x11:
        return rocksdb::Histograms::SOFT_RATE_LIMIT_DELAY_COUNT;
      case 0x12:
        return rocksdb::Histograms::NUM_FILES_IN_SINGLE_COMPACTION;
      case 0x13:
        return rocksdb::Histograms::DB_SEEK;
      case 0x14:
        return rocksdb::Histograms::WRITE_STALL;
      case 0x15:
        return rocksdb::Histograms::SST_READ_MICROS;
      case 0x16:
        return rocksdb::Histograms::NUM_SUBCOMPACTIONS_SCHEDULED;
      case 0x17:
        return rocksdb::Histograms::BYTES_PER_READ;
      case 0x18:
        return rocksdb::Histograms::BYTES_PER_WRITE;
      case 0x19:
        return rocksdb::Histograms::BYTES_PER_MULTIGET;
      case 0x1A:
        return rocksdb::Histograms::BYTES_COMPRESSED;
      case 0x1B:
        return rocksdb::Histograms::BYTES_DECOMPRESSED;
      case 0x1C:
        return rocksdb::Histograms::COMPRESSION_TIMES_NANOS;
      case 0x1D:
        return rocksdb::Histograms::DECOMPRESSION_TIMES_NANOS;
      case 0x1E:
        return rocksdb::Histograms::READ_NUM_MERGE_OPERANDS;
      // 0x20 to skip 0x1F so TICKER_ENUM_MAX remains unchanged for minor version compatibility.
      case 0x20:
        return rocksdb::Histograms::FLUSH_TIME;
      case 0x21:
        return rocksdb::Histograms::BLOB_DB_KEY_SIZE;
      case 0x22:
        return rocksdb::Histograms::BLOB_DB_VALUE_SIZE;
      case 0x23:
        return rocksdb::Histograms::BLOB_DB_WRITE_MICROS;
      case 0x24:
        return rocksdb::Histograms::BLOB_DB_GET_MICROS;
      case 0x25:
        return rocksdb::Histograms::BLOB_DB_MULTIGET_MICROS;
      case 0x26:
        return rocksdb::Histograms::BLOB_DB_SEEK_MICROS;
      case 0x27:
        return rocksdb::Histograms::BLOB_DB_NEXT_MICROS;
      case 0x28:
        return rocksdb::Histograms::BLOB_DB_PREV_MICROS;
      case 0x29:
        return rocksdb::Histograms::BLOB_DB_BLOB_FILE_WRITE_MICROS;
      case 0x2A:
        return rocksdb::Histograms::BLOB_DB_BLOB_FILE_READ_MICROS;
      case 0x2B:
        return rocksdb::Histograms::BLOB_DB_BLOB_FILE_SYNC_MICROS;
      case 0x2C:
        return rocksdb::Histograms::BLOB_DB_GC_MICROS;
      case 0x2D:
        return rocksdb::Histograms::BLOB_DB_COMPRESSION_MICROS;
      case 0x2E:
        return rocksdb::Histograms::BLOB_DB_DECOMPRESSION_MICROS;
      case 0x1F:
        // 0x1F for backwards compatibility on current minor version.
        return rocksdb::Histograms::HISTOGRAM_ENUM_MAX;

      default:
        // undefined/default
        return rocksdb::Histograms::DB_GET;
    }
  }
};

// The portal class for org.rocksdb.StatsLevel
class StatsLevelJni {
 public:
  // Returns the equivalent org.rocksdb.StatsLevel for the provided
  // C++ rocksdb::StatsLevel enum
  static jbyte toJavaStatsLevel(
      const rocksdb::StatsLevel& stats_level) {
    switch(stats_level) {
      case rocksdb::StatsLevel::kExceptDetailedTimers:
        return 0x0;
      case rocksdb::StatsLevel::kExceptTimeForMutex:
        return 0x1;
      case rocksdb::StatsLevel::kAll:
        return 0x2;

      default:
        // undefined/default
        return 0x0;
    }
  }

  // Returns the equivalent C++ rocksdb::StatsLevel enum for the
  // provided Java org.rocksdb.StatsLevel
  static rocksdb::StatsLevel toCppStatsLevel(jbyte jstats_level) {
    switch(jstats_level) {
      case 0x0:
        return rocksdb::StatsLevel::kExceptDetailedTimers;
      case 0x1:
        return rocksdb::StatsLevel::kExceptTimeForMutex;
      case 0x2:
        return rocksdb::StatsLevel::kAll;

      default:
        // undefined/default
        return rocksdb::StatsLevel::kExceptDetailedTimers;
    }
  }
};

// The portal class for org.rocksdb.RateLimiterMode
class RateLimiterModeJni {
 public:
  // Returns the equivalent org.rocksdb.RateLimiterMode for the provided
  // C++ rocksdb::RateLimiter::Mode enum
  static jbyte toJavaRateLimiterMode(
      const rocksdb::RateLimiter::Mode& rate_limiter_mode) {
    switch(rate_limiter_mode) {
      case rocksdb::RateLimiter::Mode::kReadsOnly:
        return 0x0;
      case rocksdb::RateLimiter::Mode::kWritesOnly:
        return 0x1;
      case rocksdb::RateLimiter::Mode::kAllIo:
        return 0x2;

      default:
        // undefined/default
        return 0x1;
    }
  }

  // Returns the equivalent C++ rocksdb::RateLimiter::Mode enum for the
  // provided Java org.rocksdb.RateLimiterMode
  static rocksdb::RateLimiter::Mode toCppRateLimiterMode(jbyte jrate_limiter_mode) {
    switch(jrate_limiter_mode) {
      case 0x0:
        return rocksdb::RateLimiter::Mode::kReadsOnly;
      case 0x1:
        return rocksdb::RateLimiter::Mode::kWritesOnly;
      case 0x2:
        return rocksdb::RateLimiter::Mode::kAllIo;

      default:
        // undefined/default
        return rocksdb::RateLimiter::Mode::kWritesOnly;
    }
  }
};

// The portal class for org.rocksdb.MemoryUsageType
class MemoryUsageTypeJni {
public:
  // Returns the equivalent org.rocksdb.MemoryUsageType for the provided
  // C++ rocksdb::MemoryUtil::UsageType enum
  static jbyte toJavaMemoryUsageType(
      const rocksdb::MemoryUtil::UsageType& usage_type) {
    switch(usage_type) {
      case rocksdb::MemoryUtil::UsageType::kMemTableTotal:
        return 0x0;
      case rocksdb::MemoryUtil::UsageType::kMemTableUnFlushed:
        return 0x1;
      case rocksdb::MemoryUtil::UsageType::kTableReadersTotal:
        return 0x2;
      case rocksdb::MemoryUtil::UsageType::kCacheTotal:
        return 0x3;
      default:
        // undefined: use kNumUsageTypes
        return 0x4;
    }
  }

  // Returns the equivalent C++ rocksdb::MemoryUtil::UsageType enum for the
  // provided Java org.rocksdb.MemoryUsageType
  static rocksdb::MemoryUtil::UsageType toCppMemoryUsageType(
      jbyte usage_type) {
    switch(usage_type) {
      case 0x0:
        return rocksdb::MemoryUtil::UsageType::kMemTableTotal;
      case 0x1:
        return rocksdb::MemoryUtil::UsageType::kMemTableUnFlushed;
      case 0x2:
        return rocksdb::MemoryUtil::UsageType::kTableReadersTotal;
      case 0x3:
        return rocksdb::MemoryUtil::UsageType::kCacheTotal;
      default:
        // undefined/default: use kNumUsageTypes
        return rocksdb::MemoryUtil::UsageType::kNumUsageTypes;
    }
  }
};

// The portal class for org.rocksdb.Transaction
class TransactionJni : public JavaClass {
 public:
  /**
   * Get the Java Class org.rocksdb.Transaction
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env,
        "org/rocksdb/Transaction");
  }

  /**
   * Create a new Java org.rocksdb.Transaction.WaitingTransactions object
   *
   * @param env A pointer to the Java environment
   * @param jtransaction A Java org.rocksdb.Transaction object
   * @param column_family_id The id of the column family
   * @param key The key
   * @param transaction_ids The transaction ids
   *
   * @return A reference to a Java
   *     org.rocksdb.Transaction.WaitingTransactions object,
   *     or nullptr if an an exception occurs
   */
  static jobject newWaitingTransactions(JNIEnv* env, jobject jtransaction,
      const uint32_t column_family_id, const std::string &key,
      const std::vector<TransactionID> &transaction_ids) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID mid = env->GetMethodID(
      jclazz, "newWaitingTransactions", "(JLjava/lang/String;[J)Lorg/rocksdb/Transaction$WaitingTransactions;");
    if(mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    jstring jkey = env->NewStringUTF(key.c_str());
    if(jkey == nullptr) {
      // exception thrown: OutOfMemoryError
      return nullptr;
    }

    const size_t len = transaction_ids.size();
    jlongArray jtransaction_ids = env->NewLongArray(static_cast<jsize>(len));
    if(jtransaction_ids == nullptr) {
      // exception thrown: OutOfMemoryError
      env->DeleteLocalRef(jkey);
      return nullptr;
    }

    jlong *body = env->GetLongArrayElements(jtransaction_ids, nullptr);
    if(body == nullptr) {
        // exception thrown: OutOfMemoryError
        env->DeleteLocalRef(jkey);
        env->DeleteLocalRef(jtransaction_ids);
        return nullptr;
    }
    for(size_t i = 0; i < len; ++i) {
      body[i] = static_cast<jlong>(transaction_ids[i]);
    }
    env->ReleaseLongArrayElements(jtransaction_ids, body, 0);

    jobject jwaiting_transactions = env->CallObjectMethod(jtransaction,
      mid, static_cast<jlong>(column_family_id), jkey, jtransaction_ids);
    if(env->ExceptionCheck()) {
      // exception thrown: InstantiationException or OutOfMemoryError
      env->DeleteLocalRef(jkey);
      env->DeleteLocalRef(jtransaction_ids);
      return nullptr;
    }

    return jwaiting_transactions;
  }
};

// The portal class for org.rocksdb.TransactionDB
class TransactionDBJni : public JavaClass {
 public:
 /**
  * Get the Java Class org.rocksdb.TransactionDB
  *
  * @param env A pointer to the Java environment
  *
  * @return The Java Class or nullptr if one of the
  *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
  *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
  */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env,
       "org/rocksdb/TransactionDB");
  }

  /**
   * Create a new Java org.rocksdb.TransactionDB.DeadlockInfo object
   *
   * @param env A pointer to the Java environment
   * @param jtransaction A Java org.rocksdb.Transaction object
   * @param column_family_id The id of the column family
   * @param key The key
   * @param transaction_ids The transaction ids
   *
   * @return A reference to a Java
   *     org.rocksdb.Transaction.WaitingTransactions object,
   *     or nullptr if an an exception occurs
   */
  static jobject newDeadlockInfo(JNIEnv* env, jobject jtransaction_db,
      const rocksdb::TransactionID transaction_id,
      const uint32_t column_family_id, const std::string &waiting_key,
      const bool exclusive) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID mid = env->GetMethodID(
        jclazz, "newDeadlockInfo", "(JJLjava/lang/String;Z)Lorg/rocksdb/TransactionDB$DeadlockInfo;");
    if(mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    jstring jwaiting_key = env->NewStringUTF(waiting_key.c_str());
    if(jwaiting_key == nullptr) {
      // exception thrown: OutOfMemoryError
      return nullptr;
    }

    // resolve the column family id to a ColumnFamilyHandle
    jobject jdeadlock_info = env->CallObjectMethod(jtransaction_db,
        mid, transaction_id, static_cast<jlong>(column_family_id),
        jwaiting_key, exclusive);
    if(env->ExceptionCheck()) {
      // exception thrown: InstantiationException or OutOfMemoryError
      env->DeleteLocalRef(jwaiting_key);
      return nullptr;
    }

    return jdeadlock_info;
  }
};

// The portal class for org.rocksdb.TxnDBWritePolicy
class TxnDBWritePolicyJni {
 public:
 // Returns the equivalent org.rocksdb.TxnDBWritePolicy for the provided
 // C++ rocksdb::TxnDBWritePolicy enum
 static jbyte toJavaTxnDBWritePolicy(
     const rocksdb::TxnDBWritePolicy& txndb_write_policy) {
   switch(txndb_write_policy) {
     case rocksdb::TxnDBWritePolicy::WRITE_COMMITTED:
       return 0x0;
     case rocksdb::TxnDBWritePolicy::WRITE_PREPARED:
       return 0x1;
    case rocksdb::TxnDBWritePolicy::WRITE_UNPREPARED:
       return 0x2;
     default:
       return 0x7F;  // undefined
   }
 }

 // Returns the equivalent C++ rocksdb::TxnDBWritePolicy enum for the
 // provided Java org.rocksdb.TxnDBWritePolicy
 static rocksdb::TxnDBWritePolicy toCppTxnDBWritePolicy(
     jbyte jtxndb_write_policy) {
   switch(jtxndb_write_policy) {
     case 0x0:
       return rocksdb::TxnDBWritePolicy::WRITE_COMMITTED;
     case 0x1:
       return rocksdb::TxnDBWritePolicy::WRITE_PREPARED;
     case 0x2:
       return rocksdb::TxnDBWritePolicy::WRITE_UNPREPARED;
     default:
       // undefined/default
       return rocksdb::TxnDBWritePolicy::WRITE_COMMITTED;
   }
 }
};

// The portal class for org.rocksdb.TransactionDB.KeyLockInfo
class KeyLockInfoJni : public JavaClass {
 public:
  /**
   * Get the Java Class org.rocksdb.TransactionDB.KeyLockInfo
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env,
        "org/rocksdb/TransactionDB$KeyLockInfo");
  }

  /**
   * Create a new Java org.rocksdb.TransactionDB.KeyLockInfo object
   * with the same properties as the provided C++ rocksdb::KeyLockInfo object
   *
   * @param env A pointer to the Java environment
   * @param key_lock_info The rocksdb::KeyLockInfo object
   *
   * @return A reference to a Java
   *     org.rocksdb.TransactionDB.KeyLockInfo object,
   *     or nullptr if an an exception occurs
   */
  static jobject construct(JNIEnv* env,
      const rocksdb::KeyLockInfo& key_lock_info) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID mid = env->GetMethodID(
      jclazz, "<init>", "(Ljava/lang/String;[JZ)V");
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    jstring jkey = env->NewStringUTF(key_lock_info.key.c_str());
    if (jkey == nullptr) {
      // exception thrown: OutOfMemoryError
      return nullptr;
    }

    const jsize jtransaction_ids_len = static_cast<jsize>(key_lock_info.ids.size());
    jlongArray jtransactions_ids = env->NewLongArray(jtransaction_ids_len);
    if (jtransactions_ids == nullptr) {
      // exception thrown: OutOfMemoryError
      env->DeleteLocalRef(jkey);
      return nullptr;
    }

    const jobject jkey_lock_info = env->NewObject(jclazz, mid,
      jkey, jtransactions_ids, key_lock_info.exclusive);
    if(jkey_lock_info == nullptr) {
      // exception thrown: InstantiationException or OutOfMemoryError
      env->DeleteLocalRef(jtransactions_ids);
      env->DeleteLocalRef(jkey);
      return nullptr;
    }

    return jkey_lock_info;
  }
};

// The portal class for org.rocksdb.TransactionDB.DeadlockInfo
class DeadlockInfoJni : public JavaClass {
 public:
  /**
   * Get the Java Class org.rocksdb.TransactionDB.DeadlockInfo
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
   static jclass getJClass(JNIEnv* env) {
     return JavaClass::getJClass(env,"org/rocksdb/TransactionDB$DeadlockInfo");
  }
};

// The portal class for org.rocksdb.TransactionDB.DeadlockPath
class DeadlockPathJni : public JavaClass {
 public:
  /**
   * Get the Java Class org.rocksdb.TransactionDB.DeadlockPath
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env,
        "org/rocksdb/TransactionDB$DeadlockPath");
  }

  /**
   * Create a new Java org.rocksdb.TransactionDB.DeadlockPath object
   *
   * @param env A pointer to the Java environment
   *
   * @return A reference to a Java
   *     org.rocksdb.TransactionDB.DeadlockPath object,
   *     or nullptr if an an exception occurs
   */
  static jobject construct(JNIEnv* env,
    const jobjectArray jdeadlock_infos, const bool limit_exceeded) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID mid = env->GetMethodID(
      jclazz, "<init>", "([LDeadlockInfo;Z)V");
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    const jobject jdeadlock_path = env->NewObject(jclazz, mid,
      jdeadlock_infos, limit_exceeded);
    if(jdeadlock_path == nullptr) {
      // exception thrown: InstantiationException or OutOfMemoryError
      return nullptr;
    }

    return jdeadlock_path;
  }
};

class AbstractTableFilterJni : public RocksDBNativeClass<const rocksdb::TableFilterJniCallback*, AbstractTableFilterJni> {
 public:
  /**
   * Get the Java Method: TableFilter#filter(TableProperties)
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getFilterMethod(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid =
        env->GetMethodID(jclazz, "filter", "(Lorg/rocksdb/TableProperties;)Z");
    assert(mid != nullptr);
    return mid;
  }

 private:
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/TableFilter");
  }
};

class TablePropertiesJni : public JavaClass {
 public:
  /**
   * Create a new Java org.rocksdb.TableProperties object.
   *
   * @param env A pointer to the Java environment
   * @param table_properties A Cpp table properties object
   *
   * @return A reference to a Java org.rocksdb.TableProperties object, or
   * nullptr if an an exception occurs
   */
  static jobject fromCppTableProperties(JNIEnv* env, const rocksdb::TableProperties& table_properties) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID mid = env->GetMethodID(jclazz, "<init>", "(JJJJJJJJJJJJJJJJJJJ[BLjava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/util/Map;Ljava/util/Map;Ljava/util/Map;)V");
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    jbyteArray jcolumn_family_name = rocksdb::JniUtil::copyBytes(env, table_properties.column_family_name);
    if (jcolumn_family_name == nullptr) {
      // exception occurred creating java string
      return nullptr;
    }

    jstring jfilter_policy_name = rocksdb::JniUtil::toJavaString(env, &table_properties.filter_policy_name, true);
    if (env->ExceptionCheck()) {
      // exception occurred creating java string
      env->DeleteLocalRef(jcolumn_family_name);
      return nullptr;
    }

    jstring jcomparator_name = rocksdb::JniUtil::toJavaString(env, &table_properties.comparator_name, true);
    if (env->ExceptionCheck()) {
      // exception occurred creating java string
      env->DeleteLocalRef(jcolumn_family_name);
      env->DeleteLocalRef(jfilter_policy_name);
      return nullptr;
    }

    jstring jmerge_operator_name = rocksdb::JniUtil::toJavaString(env, &table_properties.merge_operator_name, true);
    if (env->ExceptionCheck()) {
      // exception occurred creating java string
      env->DeleteLocalRef(jcolumn_family_name);
      env->DeleteLocalRef(jfilter_policy_name);
      env->DeleteLocalRef(jcomparator_name);
      return nullptr;
    }

    jstring jprefix_extractor_name = rocksdb::JniUtil::toJavaString(env, &table_properties.prefix_extractor_name, true);
    if (env->ExceptionCheck()) {
      // exception occurred creating java string
      env->DeleteLocalRef(jcolumn_family_name);
      env->DeleteLocalRef(jfilter_policy_name);
      env->DeleteLocalRef(jcomparator_name);
      env->DeleteLocalRef(jmerge_operator_name);
      return nullptr;
    }
  
    jstring jproperty_collectors_names = rocksdb::JniUtil::toJavaString(env, &table_properties.property_collectors_names, true);
    if (env->ExceptionCheck()) {
      // exception occurred creating java string
      env->DeleteLocalRef(jcolumn_family_name);
      env->DeleteLocalRef(jfilter_policy_name);
      env->DeleteLocalRef(jcomparator_name);
      env->DeleteLocalRef(jmerge_operator_name);
      env->DeleteLocalRef(jprefix_extractor_name);
      return nullptr;
    }

    jstring jcompression_name = rocksdb::JniUtil::toJavaString(env, &table_properties.compression_name, true);
    if (env->ExceptionCheck()) {
      // exception occurred creating java string
      env->DeleteLocalRef(jcolumn_family_name);
      env->DeleteLocalRef(jfilter_policy_name);
      env->DeleteLocalRef(jcomparator_name);
      env->DeleteLocalRef(jmerge_operator_name);
      env->DeleteLocalRef(jprefix_extractor_name);
      env->DeleteLocalRef(jproperty_collectors_names);
      return nullptr;
    }

    // Map<String, String>
    jobject juser_collected_properties = rocksdb::HashMapJni::fromCppMap(env, &table_properties.user_collected_properties);
    if (env->ExceptionCheck()) {
      // exception occurred creating java map
      env->DeleteLocalRef(jcolumn_family_name);
      env->DeleteLocalRef(jfilter_policy_name);
      env->DeleteLocalRef(jcomparator_name);
      env->DeleteLocalRef(jmerge_operator_name);
      env->DeleteLocalRef(jprefix_extractor_name);
      env->DeleteLocalRef(jproperty_collectors_names);
      env->DeleteLocalRef(jcompression_name);
      return nullptr;
    }

    // Map<String, String>
    jobject jreadable_properties = rocksdb::HashMapJni::fromCppMap(env, &table_properties.readable_properties);
    if (env->ExceptionCheck()) {
      // exception occurred creating java map
      env->DeleteLocalRef(jcolumn_family_name);
      env->DeleteLocalRef(jfilter_policy_name);
      env->DeleteLocalRef(jcomparator_name);
      env->DeleteLocalRef(jmerge_operator_name);
      env->DeleteLocalRef(jprefix_extractor_name);
      env->DeleteLocalRef(jproperty_collectors_names);
      env->DeleteLocalRef(jcompression_name);
      env->DeleteLocalRef(juser_collected_properties);
      return nullptr;
    }

    // Map<String, Long>
    jobject jproperties_offsets = rocksdb::HashMapJni::fromCppMap(env, &table_properties.properties_offsets);
    if (env->ExceptionCheck()) {
      // exception occurred creating java map
      env->DeleteLocalRef(jcolumn_family_name);
      env->DeleteLocalRef(jfilter_policy_name);
      env->DeleteLocalRef(jcomparator_name);
      env->DeleteLocalRef(jmerge_operator_name);
      env->DeleteLocalRef(jprefix_extractor_name);
      env->DeleteLocalRef(jproperty_collectors_names);
      env->DeleteLocalRef(jcompression_name);
      env->DeleteLocalRef(juser_collected_properties);
      env->DeleteLocalRef(jreadable_properties);
      return nullptr;
    }

    jobject jtable_properties = env->NewObject(jclazz, mid,
        static_cast<jlong>(table_properties.data_size),
        static_cast<jlong>(table_properties.index_size),
        static_cast<jlong>(table_properties.index_partitions),
        static_cast<jlong>(table_properties.top_level_index_size),
        static_cast<jlong>(table_properties.index_key_is_user_key),
        static_cast<jlong>(table_properties.index_value_is_delta_encoded),
        static_cast<jlong>(table_properties.filter_size),
        static_cast<jlong>(table_properties.raw_key_size),
        static_cast<jlong>(table_properties.raw_value_size),
        static_cast<jlong>(table_properties.num_data_blocks),
        static_cast<jlong>(table_properties.num_entries),
        static_cast<jlong>(table_properties.num_deletions),
        static_cast<jlong>(table_properties.num_merge_operands),
        static_cast<jlong>(table_properties.num_range_deletions),
        static_cast<jlong>(table_properties.format_version),
        static_cast<jlong>(table_properties.fixed_key_len),
        static_cast<jlong>(table_properties.column_family_id),
        static_cast<jlong>(table_properties.creation_time),
        static_cast<jlong>(table_properties.oldest_key_time),
        jcolumn_family_name,
        jfilter_policy_name,
        jcomparator_name,
        jmerge_operator_name,
        jprefix_extractor_name,
        jproperty_collectors_names,
        jcompression_name,
        juser_collected_properties,
        jreadable_properties,
        jproperties_offsets
    );

    if (env->ExceptionCheck()) {
      return nullptr;
    }

    return jtable_properties;
  }

 private:
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/TableProperties");
  }
};

class ColumnFamilyDescriptorJni : public JavaClass {
 public:
  /**
   * Get the Java Class org.rocksdb.ColumnFamilyDescriptor
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/ColumnFamilyDescriptor");
  }

  /**
   * Create a new Java org.rocksdb.ColumnFamilyDescriptor object with the same
   * properties as the provided C++ rocksdb::ColumnFamilyDescriptor object
   *
   * @param env A pointer to the Java environment
   * @param cfd A pointer to rocksdb::ColumnFamilyDescriptor object
   *
   * @return A reference to a Java org.rocksdb.ColumnFamilyDescriptor object, or
   * nullptr if an an exception occurs
   */
  static jobject construct(JNIEnv* env, ColumnFamilyDescriptor* cfd) {
    jbyteArray jcf_name = JniUtil::copyBytes(env, cfd->name);
    jobject cfopts = ColumnFamilyOptionsJni::construct(env, &(cfd->options));

    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID mid = env->GetMethodID(jclazz, "<init>",
                                     "([BLorg/rocksdb/ColumnFamilyOptions;)V");
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      env->DeleteLocalRef(jcf_name);
      return nullptr;
    }

    jobject jcfd = env->NewObject(jclazz, mid, jcf_name, cfopts);
    if (env->ExceptionCheck()) {
      env->DeleteLocalRef(jcf_name);
      return nullptr;
    }

    return jcfd;
  }

  /**
   * Get the Java Method: ColumnFamilyDescriptor#columnFamilyName
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getColumnFamilyNameMethod(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "columnFamilyName", "()[B");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: ColumnFamilyDescriptor#columnFamilyOptions
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getColumnFamilyOptionsMethod(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(
        jclazz, "columnFamilyOptions", "()Lorg/rocksdb/ColumnFamilyOptions;");
    assert(mid != nullptr);
    return mid;
  }
};

// The portal class for org.rocksdb.IndexType
class IndexTypeJni {
 public:
 // Returns the equivalent org.rocksdb.IndexType for the provided
 // C++ rocksdb::IndexType enum
 static jbyte toJavaIndexType(
     const rocksdb::BlockBasedTableOptions::IndexType& index_type) {
   switch(index_type) {
     case rocksdb::BlockBasedTableOptions::IndexType::kBinarySearch:
       return 0x0;
     case rocksdb::BlockBasedTableOptions::IndexType::kHashSearch:
       return 0x1;
    case rocksdb::BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch:
       return 0x2;
     default:
       return 0x7F;  // undefined
   }
 }

 // Returns the equivalent C++ rocksdb::IndexType enum for the
 // provided Java org.rocksdb.IndexType
 static rocksdb::BlockBasedTableOptions::IndexType toCppIndexType(
     jbyte jindex_type) {
   switch(jindex_type) {
     case 0x0:
       return rocksdb::BlockBasedTableOptions::IndexType::kBinarySearch;
     case 0x1:
       return rocksdb::BlockBasedTableOptions::IndexType::kHashSearch;
     case 0x2:
       return rocksdb::BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
     default:
       // undefined/default
       return rocksdb::BlockBasedTableOptions::IndexType::kBinarySearch;
   }
 }
};

// The portal class for org.rocksdb.DataBlockIndexType
class DataBlockIndexTypeJni {
 public:
 // Returns the equivalent org.rocksdb.DataBlockIndexType for the provided
 // C++ rocksdb::DataBlockIndexType enum
 static jbyte toJavaDataBlockIndexType(
     const rocksdb::BlockBasedTableOptions::DataBlockIndexType& index_type) {
   switch(index_type) {
     case rocksdb::BlockBasedTableOptions::DataBlockIndexType::kDataBlockBinarySearch:
       return 0x0;
     case rocksdb::BlockBasedTableOptions::DataBlockIndexType::kDataBlockBinaryAndHash:
       return 0x1;
     default:
       return 0x7F;  // undefined
   }
 }

 // Returns the equivalent C++ rocksdb::DataBlockIndexType enum for the
 // provided Java org.rocksdb.DataBlockIndexType
 static rocksdb::BlockBasedTableOptions::DataBlockIndexType toCppDataBlockIndexType(
     jbyte jindex_type) {
   switch(jindex_type) {
     case 0x0:
       return rocksdb::BlockBasedTableOptions::DataBlockIndexType::kDataBlockBinarySearch;
     case 0x1:
       return rocksdb::BlockBasedTableOptions::DataBlockIndexType::kDataBlockBinaryAndHash;
     default:
       // undefined/default
       return rocksdb::BlockBasedTableOptions::DataBlockIndexType::kDataBlockBinarySearch;
   }
 }
};

// The portal class for org.rocksdb.ChecksumType
class ChecksumTypeJni {
 public:
 // Returns the equivalent org.rocksdb.ChecksumType for the provided
 // C++ rocksdb::ChecksumType enum
 static jbyte toJavaChecksumType(
     const rocksdb::ChecksumType& checksum_type) {
   switch(checksum_type) {
     case rocksdb::ChecksumType::kNoChecksum:
       return 0x0;
     case rocksdb::ChecksumType::kCRC32c:
       return 0x1;
     case rocksdb::ChecksumType::kxxHash:
       return 0x2;
     case rocksdb::ChecksumType::kxxHash64:
       return 0x3;
     default:
       return 0x7F;  // undefined
   }
 }

 // Returns the equivalent C++ rocksdb::ChecksumType enum for the
 // provided Java org.rocksdb.ChecksumType
 static rocksdb::ChecksumType toCppChecksumType(
     jbyte jchecksum_type) {
   switch(jchecksum_type) {
     case 0x0:
       return rocksdb::ChecksumType::kNoChecksum;
     case 0x1:
       return rocksdb::ChecksumType::kCRC32c;
     case 0x2:
       return rocksdb::ChecksumType::kxxHash;
     case 0x3:
       return rocksdb::ChecksumType::kxxHash64;
     default:
       // undefined/default
       return rocksdb::ChecksumType::kCRC32c;
   }
 }
};

// The portal class for org.rocksdb.Priority
class PriorityJni {
 public:
 // Returns the equivalent org.rocksdb.Priority for the provided
 // C++ rocksdb::Env::Priority enum
 static jbyte toJavaPriority(
     const rocksdb::Env::Priority& priority) {
   switch(priority) {
     case rocksdb::Env::Priority::BOTTOM:
       return 0x0;
     case rocksdb::Env::Priority::LOW:
       return 0x1;
     case rocksdb::Env::Priority::HIGH:
       return 0x2;
     case rocksdb::Env::Priority::TOTAL:
       return 0x3;
     default:
       return 0x7F;  // undefined
   }
 }

 // Returns the equivalent C++ rocksdb::env::Priority enum for the
 // provided Java org.rocksdb.Priority
 static rocksdb::Env::Priority toCppPriority(
     jbyte jpriority) {
   switch(jpriority) {
     case 0x0:
       return rocksdb::Env::Priority::BOTTOM;
     case 0x1:
       return rocksdb::Env::Priority::LOW;
     case 0x2:
       return rocksdb::Env::Priority::HIGH;
     case 0x3:
       return rocksdb::Env::Priority::TOTAL;
     default:
       // undefined/default
       return rocksdb::Env::Priority::LOW;
   }
 }
};

// The portal class for org.rocksdb.ThreadType
class ThreadTypeJni {
 public:
 // Returns the equivalent org.rocksdb.ThreadType for the provided
 // C++ rocksdb::ThreadStatus::ThreadType enum
 static jbyte toJavaThreadType(
     const rocksdb::ThreadStatus::ThreadType& thread_type) {
   switch(thread_type) {
     case rocksdb::ThreadStatus::ThreadType::HIGH_PRIORITY:
       return 0x0;
     case rocksdb::ThreadStatus::ThreadType::LOW_PRIORITY:
       return 0x1;
     case rocksdb::ThreadStatus::ThreadType::USER:
       return 0x2;
     case rocksdb::ThreadStatus::ThreadType::BOTTOM_PRIORITY:
       return 0x3;
     default:
       return 0x7F;  // undefined
   }
 }

 // Returns the equivalent C++ rocksdb::ThreadStatus::ThreadType enum for the
 // provided Java org.rocksdb.ThreadType
 static rocksdb::ThreadStatus::ThreadType toCppThreadType(
     jbyte jthread_type) {
   switch(jthread_type) {
     case 0x0:
       return rocksdb::ThreadStatus::ThreadType::HIGH_PRIORITY;
     case 0x1:
       return rocksdb::ThreadStatus::ThreadType::LOW_PRIORITY;
     case 0x2:
       return ThreadStatus::ThreadType::USER;
     case 0x3:
       return rocksdb::ThreadStatus::ThreadType::BOTTOM_PRIORITY;
     default:
       // undefined/default
       return rocksdb::ThreadStatus::ThreadType::LOW_PRIORITY;
   }
 }
};

// The portal class for org.rocksdb.OperationType
class OperationTypeJni {
 public:
 // Returns the equivalent org.rocksdb.OperationType for the provided
 // C++ rocksdb::ThreadStatus::OperationType enum
 static jbyte toJavaOperationType(
     const rocksdb::ThreadStatus::OperationType& operation_type) {
   switch(operation_type) {
     case rocksdb::ThreadStatus::OperationType::OP_UNKNOWN:
       return 0x0;
     case rocksdb::ThreadStatus::OperationType::OP_COMPACTION:
       return 0x1;
     case rocksdb::ThreadStatus::OperationType::OP_FLUSH:
       return 0x2;
     default:
       return 0x7F;  // undefined
   }
 }

 // Returns the equivalent C++ rocksdb::ThreadStatus::OperationType enum for the
 // provided Java org.rocksdb.OperationType
 static rocksdb::ThreadStatus::OperationType toCppOperationType(
     jbyte joperation_type) {
   switch(joperation_type) {
     case 0x0:
       return rocksdb::ThreadStatus::OperationType::OP_UNKNOWN;
     case 0x1:
       return rocksdb::ThreadStatus::OperationType::OP_COMPACTION;
     case 0x2:
       return rocksdb::ThreadStatus::OperationType::OP_FLUSH;
     default:
       // undefined/default
       return rocksdb::ThreadStatus::OperationType::OP_UNKNOWN;
   }
 }
};

// The portal class for org.rocksdb.OperationStage
class OperationStageJni {
 public:
 // Returns the equivalent org.rocksdb.OperationStage for the provided
 // C++ rocksdb::ThreadStatus::OperationStage enum
 static jbyte toJavaOperationStage(
     const rocksdb::ThreadStatus::OperationStage& operation_stage) {
   switch(operation_stage) {
     case rocksdb::ThreadStatus::OperationStage::STAGE_UNKNOWN:
       return 0x0;
     case rocksdb::ThreadStatus::OperationStage::STAGE_FLUSH_RUN:
       return 0x1;
     case rocksdb::ThreadStatus::OperationStage::STAGE_FLUSH_WRITE_L0:
       return 0x2;
     case rocksdb::ThreadStatus::OperationStage::STAGE_COMPACTION_PREPARE:
       return 0x3;
     case rocksdb::ThreadStatus::OperationStage::STAGE_COMPACTION_RUN:
       return 0x4;
     case rocksdb::ThreadStatus::OperationStage::STAGE_COMPACTION_PROCESS_KV:
       return 0x5;
     case rocksdb::ThreadStatus::OperationStage::STAGE_COMPACTION_INSTALL:
       return 0x6;
     case rocksdb::ThreadStatus::OperationStage::STAGE_COMPACTION_SYNC_FILE:
       return 0x7;
     case rocksdb::ThreadStatus::OperationStage::STAGE_PICK_MEMTABLES_TO_FLUSH:
       return 0x8;
     case rocksdb::ThreadStatus::OperationStage::STAGE_MEMTABLE_ROLLBACK:
       return 0x9;
     case rocksdb::ThreadStatus::OperationStage::STAGE_MEMTABLE_INSTALL_FLUSH_RESULTS:
       return 0xA;
     default:
       return 0x7F;  // undefined
   }
 }

 // Returns the equivalent C++ rocksdb::ThreadStatus::OperationStage enum for the
 // provided Java org.rocksdb.OperationStage
 static rocksdb::ThreadStatus::OperationStage toCppOperationStage(
     jbyte joperation_stage) {
   switch(joperation_stage) {
     case 0x0:
       return rocksdb::ThreadStatus::OperationStage::STAGE_UNKNOWN;
     case 0x1:
       return rocksdb::ThreadStatus::OperationStage::STAGE_FLUSH_RUN;
     case 0x2:
       return rocksdb::ThreadStatus::OperationStage::STAGE_FLUSH_WRITE_L0;
     case 0x3:
       return rocksdb::ThreadStatus::OperationStage::STAGE_COMPACTION_PREPARE;
     case 0x4:
       return rocksdb::ThreadStatus::OperationStage::STAGE_COMPACTION_RUN;
     case 0x5:
       return rocksdb::ThreadStatus::OperationStage::STAGE_COMPACTION_PROCESS_KV;
     case 0x6:
       return rocksdb::ThreadStatus::OperationStage::STAGE_COMPACTION_INSTALL;
     case 0x7:
       return rocksdb::ThreadStatus::OperationStage::STAGE_COMPACTION_SYNC_FILE;
     case 0x8:
       return rocksdb::ThreadStatus::OperationStage::STAGE_PICK_MEMTABLES_TO_FLUSH;
     case 0x9:
       return rocksdb::ThreadStatus::OperationStage::STAGE_MEMTABLE_ROLLBACK;
     case 0xA:
       return rocksdb::ThreadStatus::OperationStage::STAGE_MEMTABLE_INSTALL_FLUSH_RESULTS;
     default:
       // undefined/default
       return rocksdb::ThreadStatus::OperationStage::STAGE_UNKNOWN;
   }
 }
};

// The portal class for org.rocksdb.StateType
class StateTypeJni {
 public:
 // Returns the equivalent org.rocksdb.StateType for the provided
 // C++ rocksdb::ThreadStatus::StateType enum
 static jbyte toJavaStateType(
     const rocksdb::ThreadStatus::StateType& state_type) {
   switch(state_type) {
     case rocksdb::ThreadStatus::StateType::STATE_UNKNOWN:
       return 0x0;
     case rocksdb::ThreadStatus::StateType::STATE_MUTEX_WAIT:
       return 0x1;
     default:
       return 0x7F;  // undefined
   }
 }

 // Returns the equivalent C++ rocksdb::ThreadStatus::StateType enum for the
 // provided Java org.rocksdb.StateType
 static rocksdb::ThreadStatus::StateType toCppStateType(
     jbyte jstate_type) {
   switch(jstate_type) {
     case 0x0:
       return rocksdb::ThreadStatus::StateType::STATE_UNKNOWN;
     case 0x1:
       return rocksdb::ThreadStatus::StateType::STATE_MUTEX_WAIT;
     default:
       // undefined/default
       return rocksdb::ThreadStatus::StateType::STATE_UNKNOWN;
   }
 }
};

// The portal class for org.rocksdb.ThreadStatus
class ThreadStatusJni : public JavaClass {
 public:
  /**
   * Get the Java Class org.rocksdb.ThreadStatus
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env,
        "org/rocksdb/ThreadStatus");
  }

  /**
   * Create a new Java org.rocksdb.ThreadStatus object with the same
   * properties as the provided C++ rocksdb::ThreadStatus object
   *
   * @param env A pointer to the Java environment
   * @param thread_status A pointer to rocksdb::ThreadStatus object
   *
   * @return A reference to a Java org.rocksdb.ColumnFamilyOptions object, or
   * nullptr if an an exception occurs
   */
  static jobject construct(JNIEnv* env,
      const rocksdb::ThreadStatus* thread_status) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID mid = env->GetMethodID(jclazz, "<init>", "(JBLjava/lang/String;Ljava/lang/String;BJB[JB)V");
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    jstring jdb_name =
        JniUtil::toJavaString(env, &(thread_status->db_name), true);
    if (env->ExceptionCheck()) {
        // an error occurred
        return nullptr;
    }

    jstring jcf_name =
        JniUtil::toJavaString(env, &(thread_status->cf_name), true);
    if (env->ExceptionCheck()) {
        // an error occurred
        env->DeleteLocalRef(jdb_name);
        return nullptr;
    }

    // long[]
    const jsize len = static_cast<jsize>(rocksdb::ThreadStatus::kNumOperationProperties);
    jlongArray joperation_properties =
        env->NewLongArray(len);
    if (joperation_properties == nullptr) {
      // an exception occurred
      env->DeleteLocalRef(jdb_name);
      env->DeleteLocalRef(jcf_name);
      return nullptr;
    }
    jlong *body = env->GetLongArrayElements(joperation_properties, nullptr);
    if (body == nullptr) {
        // exception thrown: OutOfMemoryError
        env->DeleteLocalRef(jdb_name);
        env->DeleteLocalRef(jcf_name);
        env->DeleteLocalRef(joperation_properties);
        return nullptr;
    }
    for (size_t i = 0; i < len; ++i) {
      body[i] = static_cast<jlong>(thread_status->op_properties[i]);
    }
    env->ReleaseLongArrayElements(joperation_properties, body, 0);

    jobject jcfd = env->NewObject(jclazz, mid,
        static_cast<jlong>(thread_status->thread_id),
        ThreadTypeJni::toJavaThreadType(thread_status->thread_type),
        jdb_name,
        jcf_name,
        OperationTypeJni::toJavaOperationType(thread_status->operation_type),
        static_cast<jlong>(thread_status->op_elapsed_micros),
        OperationStageJni::toJavaOperationStage(thread_status->operation_stage),
        joperation_properties,
        StateTypeJni::toJavaStateType(thread_status->state_type));
    if (env->ExceptionCheck()) {
      // exception occurred
        env->DeleteLocalRef(jdb_name);
        env->DeleteLocalRef(jcf_name);
        env->DeleteLocalRef(joperation_properties);
      return nullptr;
    }

    // cleanup
    env->DeleteLocalRef(jdb_name);
    env->DeleteLocalRef(jcf_name);
    env->DeleteLocalRef(joperation_properties);

    return jcfd;
  }
};

// The portal class for org.rocksdb.CompactionStyle
class CompactionStyleJni {
 public:
 // Returns the equivalent org.rocksdb.CompactionStyle for the provided
 // C++ rocksdb::CompactionStyle enum
 static jbyte toJavaCompactionStyle(
     const rocksdb::CompactionStyle& compaction_style) {
   switch(compaction_style) {
     case rocksdb::CompactionStyle::kCompactionStyleLevel:
       return 0x0;
     case rocksdb::CompactionStyle::kCompactionStyleUniversal:
       return 0x1;
     case rocksdb::CompactionStyle::kCompactionStyleFIFO:
       return 0x2;
     case rocksdb::CompactionStyle::kCompactionStyleNone:
       return 0x3;
     default:
       return 0x7F;  // undefined
   }
 }

 // Returns the equivalent C++ rocksdb::CompactionStyle enum for the
 // provided Java org.rocksdb.CompactionStyle
 static rocksdb::CompactionStyle toCppCompactionStyle(
     jbyte jcompaction_style) {
   switch(jcompaction_style) {
     case 0x0:
       return rocksdb::CompactionStyle::kCompactionStyleLevel;
     case 0x1:
       return rocksdb::CompactionStyle::kCompactionStyleUniversal;
     case 0x2:
       return rocksdb::CompactionStyle::kCompactionStyleFIFO;
     case 0x3:
       return rocksdb::CompactionStyle::kCompactionStyleNone;
     default:
       // undefined/default
       return rocksdb::CompactionStyle::kCompactionStyleLevel;
   }
 }
};

// The portal class for org.rocksdb.CompactionReason
class CompactionReasonJni {
 public:
 // Returns the equivalent org.rocksdb.CompactionReason for the provided
 // C++ rocksdb::CompactionReason enum
 static jbyte toJavaCompactionReason(
     const rocksdb::CompactionReason& compaction_reason) {
   switch(compaction_reason) {
     case rocksdb::CompactionReason::kUnknown:
       return 0x0;
     case rocksdb::CompactionReason::kLevelL0FilesNum:
       return 0x1;
     case rocksdb::CompactionReason::kLevelMaxLevelSize:
       return 0x2;
     case rocksdb::CompactionReason::kUniversalSizeAmplification:
       return 0x3;
     case rocksdb::CompactionReason::kUniversalSizeRatio:
       return 0x4;
     case rocksdb::CompactionReason::kUniversalSortedRunNum:
       return 0x5;
     case rocksdb::CompactionReason::kFIFOMaxSize:
       return 0x6;
     case rocksdb::CompactionReason::kFIFOReduceNumFiles:
       return 0x7;
     case rocksdb::CompactionReason::kFIFOTtl:
       return 0x8;
     case rocksdb::CompactionReason::kManualCompaction:
       return 0x9;
     case rocksdb::CompactionReason::kFilesMarkedForCompaction:
       return 0x10;
     case rocksdb::CompactionReason::kBottommostFiles:
       return 0x0A;
     case rocksdb::CompactionReason::kTtl:
       return 0x0B;
     case rocksdb::CompactionReason::kFlush:
       return 0x0C;
     case rocksdb::CompactionReason::kExternalSstIngestion:
       return 0x0D;
     default:
       return 0x7F;  // undefined
   }
 }

 // Returns the equivalent C++ rocksdb::CompactionReason enum for the
 // provided Java org.rocksdb.CompactionReason
 static rocksdb::CompactionReason toCppCompactionReason(
     jbyte jcompaction_reason) {
   switch(jcompaction_reason) {
     case 0x0:
       return rocksdb::CompactionReason::kUnknown;
     case 0x1:
       return rocksdb::CompactionReason::kLevelL0FilesNum;
     case 0x2:
       return rocksdb::CompactionReason::kLevelMaxLevelSize;
     case 0x3:
       return rocksdb::CompactionReason::kUniversalSizeAmplification;
     case 0x4:
       return rocksdb::CompactionReason::kUniversalSizeRatio;
     case 0x5:
       return rocksdb::CompactionReason::kUniversalSortedRunNum;
     case 0x6:
       return rocksdb::CompactionReason::kFIFOMaxSize;
     case 0x7:
       return rocksdb::CompactionReason::kFIFOReduceNumFiles;
     case 0x8:
       return rocksdb::CompactionReason::kFIFOTtl;
     case 0x9:
       return rocksdb::CompactionReason::kManualCompaction;
     case 0x10:
       return rocksdb::CompactionReason::kFilesMarkedForCompaction;
     case 0x0A:
       return rocksdb::CompactionReason::kBottommostFiles;
     case 0x0B:
       return rocksdb::CompactionReason::kTtl;
     case 0x0C:
       return rocksdb::CompactionReason::kFlush;
     case 0x0D:
       return rocksdb::CompactionReason::kExternalSstIngestion;
     default:
       // undefined/default
       return rocksdb::CompactionReason::kUnknown;
   }
 }
};

// The portal class for org.rocksdb.WalFileType
class WalFileTypeJni {
 public:
 // Returns the equivalent org.rocksdb.WalFileType for the provided
 // C++ rocksdb::WalFileType enum
 static jbyte toJavaWalFileType(
     const rocksdb::WalFileType& wal_file_type) {
   switch(wal_file_type) {
     case rocksdb::WalFileType::kArchivedLogFile:
       return 0x0;
     case rocksdb::WalFileType::kAliveLogFile:
       return 0x1;
     default:
       return 0x7F;  // undefined
   }
 }

 // Returns the equivalent C++ rocksdb::WalFileType enum for the
 // provided Java org.rocksdb.WalFileType
 static rocksdb::WalFileType toCppWalFileType(
     jbyte jwal_file_type) {
   switch(jwal_file_type) {
     case 0x0:
       return rocksdb::WalFileType::kArchivedLogFile;
     case 0x1:
       return rocksdb::WalFileType::kAliveLogFile;
     default:
       // undefined/default
       return rocksdb::WalFileType::kAliveLogFile;
   }
 }
};

class LogFileJni : public JavaClass {
 public:
  /**
   * Create a new Java org.rocksdb.LogFile object.
   *
   * @param env A pointer to the Java environment
   * @param log_file A Cpp log file object
   *
   * @return A reference to a Java org.rocksdb.LogFile object, or
   * nullptr if an an exception occurs
   */
  static jobject fromCppLogFile(JNIEnv* env, rocksdb::LogFile* log_file) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID mid = env->GetMethodID(jclazz, "<init>", "(Ljava/lang/String;JBJJ)V");
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    std::string path_name = log_file->PathName();
    jstring jpath_name = rocksdb::JniUtil::toJavaString(env, &path_name, true);
    if (env->ExceptionCheck()) {
      // exception occurred creating java string
      return nullptr;
    }

    jobject jlog_file = env->NewObject(jclazz, mid,
        jpath_name,
        static_cast<jlong>(log_file->LogNumber()),
        rocksdb::WalFileTypeJni::toJavaWalFileType(log_file->Type()),
        static_cast<jlong>(log_file->StartSequence()),
        static_cast<jlong>(log_file->SizeFileBytes())
    );

    if (env->ExceptionCheck()) {
      env->DeleteLocalRef(jpath_name);
      return nullptr;
    }

    // cleanup
    env->DeleteLocalRef(jpath_name);

    return jlog_file;
  }

  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/LogFile");
  }
};

class LiveFileMetaDataJni : public JavaClass {
 public:
  /**
   * Create a new Java org.rocksdb.LiveFileMetaData object.
   *
   * @param env A pointer to the Java environment
   * @param live_file_meta_data A Cpp live file meta data object
   *
   * @return A reference to a Java org.rocksdb.LiveFileMetaData object, or
   * nullptr if an an exception occurs
   */
  static jobject fromCppLiveFileMetaData(JNIEnv* env,
      rocksdb::LiveFileMetaData* live_file_meta_data) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID mid = env->GetMethodID(jclazz, "<init>", "([BILjava/lang/String;Ljava/lang/String;JJJ[B[BJZJJ)V");
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    jbyteArray jcolumn_family_name = rocksdb::JniUtil::copyBytes(
        env, live_file_meta_data->column_family_name);
    if (jcolumn_family_name == nullptr) {
      // exception occurred creating java byte array
      return nullptr;
    }

    jstring jfile_name = rocksdb::JniUtil::toJavaString(
        env, &live_file_meta_data->name, true);
    if (env->ExceptionCheck()) {
      // exception occurred creating java string
      env->DeleteLocalRef(jcolumn_family_name);
      return nullptr;
    }

    jstring jpath = rocksdb::JniUtil::toJavaString(
        env, &live_file_meta_data->db_path, true);
    if (env->ExceptionCheck()) {
      // exception occurred creating java string
      env->DeleteLocalRef(jcolumn_family_name);
      env->DeleteLocalRef(jfile_name);
      return nullptr;
    }

    jbyteArray jsmallest_key = rocksdb::JniUtil::copyBytes(
        env, live_file_meta_data->smallestkey);
    if (jsmallest_key == nullptr) {
      // exception occurred creating java byte array
      env->DeleteLocalRef(jcolumn_family_name);
      env->DeleteLocalRef(jfile_name);
      env->DeleteLocalRef(jpath);
      return nullptr;
    }

    jbyteArray jlargest_key = rocksdb::JniUtil::copyBytes(
        env, live_file_meta_data->largestkey);
    if (jlargest_key == nullptr) {
      // exception occurred creating java byte array
      env->DeleteLocalRef(jcolumn_family_name);
      env->DeleteLocalRef(jfile_name);
      env->DeleteLocalRef(jpath);
      env->DeleteLocalRef(jsmallest_key);
      return nullptr;
    }

    jobject jlive_file_meta_data = env->NewObject(jclazz, mid,
        jcolumn_family_name,
        static_cast<jint>(live_file_meta_data->level),
        jfile_name,
        jpath,
        static_cast<jlong>(live_file_meta_data->size),
        static_cast<jlong>(live_file_meta_data->smallest_seqno),
        static_cast<jlong>(live_file_meta_data->largest_seqno),
        jsmallest_key,
        jlargest_key,
        static_cast<jlong>(live_file_meta_data->num_reads_sampled),
        static_cast<jboolean>(live_file_meta_data->being_compacted),
        static_cast<jlong>(live_file_meta_data->num_entries),
        static_cast<jlong>(live_file_meta_data->num_deletions)
    );

    if (env->ExceptionCheck()) {
      env->DeleteLocalRef(jcolumn_family_name);
      env->DeleteLocalRef(jfile_name);
      env->DeleteLocalRef(jpath);
      env->DeleteLocalRef(jsmallest_key);
      env->DeleteLocalRef(jlargest_key);
      return nullptr;
    }

    // cleanup
    env->DeleteLocalRef(jcolumn_family_name);
    env->DeleteLocalRef(jfile_name);
    env->DeleteLocalRef(jpath);
    env->DeleteLocalRef(jsmallest_key);
    env->DeleteLocalRef(jlargest_key);

    return jlive_file_meta_data;
  }

  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/LiveFileMetaData");
  }
};

class SstFileMetaDataJni : public JavaClass {
 public:
  /**
   * Create a new Java org.rocksdb.SstFileMetaData object.
   *
   * @param env A pointer to the Java environment
   * @param sst_file_meta_data A Cpp sst file meta data object
   *
   * @return A reference to a Java org.rocksdb.SstFileMetaData object, or
   * nullptr if an an exception occurs
   */
  static jobject fromCppSstFileMetaData(JNIEnv* env,
      const rocksdb::SstFileMetaData* sst_file_meta_data) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID mid = env->GetMethodID(jclazz, "<init>", "(Ljava/lang/String;Ljava/lang/String;JJJ[B[BJZJJ)V");
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    jstring jfile_name = rocksdb::JniUtil::toJavaString(
        env, &sst_file_meta_data->name, true);
    if (jfile_name == nullptr) {
      // exception occurred creating java byte array
      return nullptr;
    }

    jstring jpath = rocksdb::JniUtil::toJavaString(
        env, &sst_file_meta_data->db_path, true);
    if (jpath == nullptr) {
      // exception occurred creating java byte array
      env->DeleteLocalRef(jfile_name);
      return nullptr;
    }

    jbyteArray jsmallest_key = rocksdb::JniUtil::copyBytes(
        env, sst_file_meta_data->smallestkey);
    if (jsmallest_key == nullptr) {
      // exception occurred creating java byte array
      env->DeleteLocalRef(jfile_name);
      env->DeleteLocalRef(jpath);
      return nullptr;
    }

    jbyteArray jlargest_key = rocksdb::JniUtil::copyBytes(
        env, sst_file_meta_data->largestkey);
    if (jlargest_key == nullptr) {
      // exception occurred creating java byte array
      env->DeleteLocalRef(jfile_name);
      env->DeleteLocalRef(jpath);
      env->DeleteLocalRef(jsmallest_key);
      return nullptr;
    }

    jobject jsst_file_meta_data = env->NewObject(jclazz, mid,
        jfile_name,
        jpath,
        static_cast<jlong>(sst_file_meta_data->size),
        static_cast<jint>(sst_file_meta_data->smallest_seqno),
        static_cast<jlong>(sst_file_meta_data->largest_seqno),
        jsmallest_key,
        jlargest_key,
        static_cast<jlong>(sst_file_meta_data->num_reads_sampled),
        static_cast<jboolean>(sst_file_meta_data->being_compacted),
        static_cast<jlong>(sst_file_meta_data->num_entries),
        static_cast<jlong>(sst_file_meta_data->num_deletions)
    );

    if (env->ExceptionCheck()) {
      env->DeleteLocalRef(jfile_name);
      env->DeleteLocalRef(jpath);
      env->DeleteLocalRef(jsmallest_key);
      env->DeleteLocalRef(jlargest_key);
      return nullptr;
    }

    // cleanup
      env->DeleteLocalRef(jfile_name);
      env->DeleteLocalRef(jpath);
      env->DeleteLocalRef(jsmallest_key);
      env->DeleteLocalRef(jlargest_key);

    return jsst_file_meta_data;
  }

  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/SstFileMetaData");
  }
};

class LevelMetaDataJni : public JavaClass {
 public:
  /**
   * Create a new Java org.rocksdb.LevelMetaData object.
   *
   * @param env A pointer to the Java environment
   * @param level_meta_data A Cpp level meta data object
   *
   * @return A reference to a Java org.rocksdb.LevelMetaData object, or
   * nullptr if an an exception occurs
   */
  static jobject fromCppLevelMetaData(JNIEnv* env,
      const rocksdb::LevelMetaData* level_meta_data) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID mid = env->GetMethodID(jclazz, "<init>", "(IJ[Lorg/rocksdb/SstFileMetaData;)V");
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    const jsize jlen =
        static_cast<jsize>(level_meta_data->files.size());
    jobjectArray jfiles = env->NewObjectArray(jlen, SstFileMetaDataJni::getJClass(env), nullptr);
    if (jfiles == nullptr) {
      // exception thrown: OutOfMemoryError
      return nullptr;
    }

    jsize i = 0;
    for (auto it = level_meta_data->files.begin();
        it != level_meta_data->files.end(); ++it) {
      jobject jfile = SstFileMetaDataJni::fromCppSstFileMetaData(env, &(*it));
      if (jfile == nullptr) {
        // exception occurred
        env->DeleteLocalRef(jfiles);
        return nullptr;
      }
      env->SetObjectArrayElement(jfiles, i++, jfile);
    }

    jobject jlevel_meta_data = env->NewObject(jclazz, mid,
        static_cast<jint>(level_meta_data->level),
        static_cast<jlong>(level_meta_data->size),
        jfiles
    );

    if (env->ExceptionCheck()) {
      env->DeleteLocalRef(jfiles);
      return nullptr;
    }

    // cleanup
    env->DeleteLocalRef(jfiles);

    return jlevel_meta_data;
  }

  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/LevelMetaData");
  }
};

class ColumnFamilyMetaDataJni : public JavaClass {
 public:
  /**
   * Create a new Java org.rocksdb.ColumnFamilyMetaData object.
   *
   * @param env A pointer to the Java environment
   * @param column_famly_meta_data A Cpp live file meta data object
   *
   * @return A reference to a Java org.rocksdb.ColumnFamilyMetaData object, or
   * nullptr if an an exception occurs
   */
  static jobject fromCppColumnFamilyMetaData(JNIEnv* env,
      const rocksdb::ColumnFamilyMetaData* column_famly_meta_data) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID mid = env->GetMethodID(jclazz, "<init>", "(JJ[B[Lorg/rocksdb/LevelMetaData;)V");
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    jbyteArray jname = rocksdb::JniUtil::copyBytes(
        env, column_famly_meta_data->name);
    if (jname == nullptr) {
      // exception occurred creating java byte array
      return nullptr;
    }

    const jsize jlen =
        static_cast<jsize>(column_famly_meta_data->levels.size());
    jobjectArray jlevels = env->NewObjectArray(jlen, LevelMetaDataJni::getJClass(env), nullptr);
    if(jlevels == nullptr) {
      // exception thrown: OutOfMemoryError
      env->DeleteLocalRef(jname);
      return nullptr;
    }

    jsize i = 0;
    for (auto it = column_famly_meta_data->levels.begin();
        it != column_famly_meta_data->levels.end(); ++it) {
      jobject jlevel = LevelMetaDataJni::fromCppLevelMetaData(env, &(*it));
      if (jlevel == nullptr) {
        // exception occurred
        env->DeleteLocalRef(jname);
        env->DeleteLocalRef(jlevels);
        return nullptr;
      }
      env->SetObjectArrayElement(jlevels, i++, jlevel);
    }

    jobject jcolumn_family_meta_data = env->NewObject(jclazz, mid,
        static_cast<jlong>(column_famly_meta_data->size),
        static_cast<jlong>(column_famly_meta_data->file_count),
        jname,
        jlevels
    );

    if (env->ExceptionCheck()) {
      env->DeleteLocalRef(jname);
      env->DeleteLocalRef(jlevels);
      return nullptr;
    }

    // cleanup
    env->DeleteLocalRef(jname);
    env->DeleteLocalRef(jlevels);

    return jcolumn_family_meta_data;
  }

  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/ColumnFamilyMetaData");
  }
};

// The portal class for org.rocksdb.AbstractTraceWriter
class AbstractTraceWriterJni : public RocksDBNativeClass<
    const rocksdb::TraceWriterJniCallback*,
    AbstractTraceWriterJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.AbstractTraceWriter
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env,
        "org/rocksdb/AbstractTraceWriter");
  }

  /**
   * Get the Java Method: AbstractTraceWriter#write
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getWriteProxyMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(
        jclazz, "writeProxy", "(J)S");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: AbstractTraceWriter#closeWriter
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getCloseWriterProxyMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(
        jclazz, "closeWriterProxy", "()S");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: AbstractTraceWriter#getFileSize
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getGetFileSizeMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(
        jclazz, "getFileSize", "()J");
    assert(mid != nullptr);
    return mid;
  }
};

// The portal class for org.rocksdb.AbstractWalFilter
class AbstractWalFilterJni : public RocksDBNativeClass<
    const rocksdb::WalFilterJniCallback*,
    AbstractWalFilterJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.AbstractWalFilter
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env,
        "org/rocksdb/AbstractWalFilter");
  }

  /**
   * Get the Java Method: AbstractWalFilter#columnFamilyLogNumberMap
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getColumnFamilyLogNumberMapMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(
        jclazz, "columnFamilyLogNumberMap",
        "(Ljava/util/Map;Ljava/util/Map;)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: AbstractTraceWriter#logRecordFoundProxy
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getLogRecordFoundProxyMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(
        jclazz, "logRecordFoundProxy", "(JLjava/lang/String;JJ)S");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: AbstractTraceWriter#name
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getNameMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if(jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(
        jclazz, "name", "()Ljava/lang/String;");
    assert(mid != nullptr);
    return mid;
  }
};

// The portal class for org.rocksdb.WalProcessingOption
class WalProcessingOptionJni {
 public:
 // Returns the equivalent org.rocksdb.WalProcessingOption for the provided
 // C++ rocksdb::WalFilter::WalProcessingOption enum
 static jbyte toJavaWalProcessingOption(
     const rocksdb::WalFilter::WalProcessingOption& wal_processing_option) {
   switch(wal_processing_option) {
     case rocksdb::WalFilter::WalProcessingOption::kContinueProcessing:
       return 0x0;
     case rocksdb::WalFilter::WalProcessingOption::kIgnoreCurrentRecord:
       return 0x1;
     case rocksdb::WalFilter::WalProcessingOption::kStopReplay:
       return 0x2;
     case rocksdb::WalFilter::WalProcessingOption::kCorruptedRecord:
       return 0x3;
     default:
       return 0x7F;  // undefined
   }
 }

 // Returns the equivalent C++ rocksdb::WalFilter::WalProcessingOption enum for
 // the provided Java org.rocksdb.WalProcessingOption
 static rocksdb::WalFilter::WalProcessingOption toCppWalProcessingOption(
     jbyte jwal_processing_option) {
   switch(jwal_processing_option) {
     case 0x0:
       return rocksdb::WalFilter::WalProcessingOption::kContinueProcessing;
     case 0x1:
       return rocksdb::WalFilter::WalProcessingOption::kIgnoreCurrentRecord;
     case 0x2:
       return rocksdb::WalFilter::WalProcessingOption::kStopReplay;
     case 0x3:
       return rocksdb::WalFilter::WalProcessingOption::kCorruptedRecord;
     default:
       // undefined/default
       return rocksdb::WalFilter::WalProcessingOption::kCorruptedRecord;
   }
 }
};
}  // namespace rocksdb
#endif  // JAVA_ROCKSJNI_PORTAL_H_
