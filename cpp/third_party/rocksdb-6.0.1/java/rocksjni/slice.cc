// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ for
// rocksdb::Slice.

#include <jni.h>
#include <stdio.h>
#include <stdlib.h>
#include <string>

#include "include/org_rocksdb_AbstractSlice.h"
#include "include/org_rocksdb_DirectSlice.h"
#include "include/org_rocksdb_Slice.h"
#include "rocksdb/slice.h"
#include "rocksjni/portal.h"

// <editor-fold desc="org.rocksdb.AbstractSlice>

/*
 * Class:     org_rocksdb_AbstractSlice
 * Method:    createNewSliceFromString
 * Signature: (Ljava/lang/String;)J
 */
jlong Java_org_rocksdb_AbstractSlice_createNewSliceFromString(JNIEnv* env,
                                                              jclass /*jcls*/,
                                                              jstring jstr) {
  const auto* str = env->GetStringUTFChars(jstr, nullptr);
  if (str == nullptr) {
    // exception thrown: OutOfMemoryError
    return 0;
  }

  const size_t len = strlen(str);

  // NOTE: buf will be deleted in the
  // Java_org_rocksdb_Slice_disposeInternalBuf or
  // or Java_org_rocksdb_DirectSlice_disposeInternalBuf methods
  char* buf = new char[len + 1];
  memcpy(buf, str, len);
  buf[len] = 0;
  env->ReleaseStringUTFChars(jstr, str);

  const auto* slice = new rocksdb::Slice(buf);
  return reinterpret_cast<jlong>(slice);
}

/*
 * Class:     org_rocksdb_AbstractSlice
 * Method:    size0
 * Signature: (J)I
 */
jint Java_org_rocksdb_AbstractSlice_size0(JNIEnv* /*env*/, jobject /*jobj*/,
                                          jlong handle) {
  const auto* slice = reinterpret_cast<rocksdb::Slice*>(handle);
  return static_cast<jint>(slice->size());
}

/*
 * Class:     org_rocksdb_AbstractSlice
 * Method:    empty0
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_AbstractSlice_empty0(JNIEnv* /*env*/,
                                               jobject /*jobj*/, jlong handle) {
  const auto* slice = reinterpret_cast<rocksdb::Slice*>(handle);
  return slice->empty();
}

/*
 * Class:     org_rocksdb_AbstractSlice
 * Method:    toString0
 * Signature: (JZ)Ljava/lang/String;
 */
jstring Java_org_rocksdb_AbstractSlice_toString0(JNIEnv* env, jobject /*jobj*/,
                                                 jlong handle, jboolean hex) {
  const auto* slice = reinterpret_cast<rocksdb::Slice*>(handle);
  const std::string s = slice->ToString(hex);
  return env->NewStringUTF(s.c_str());
}

/*
 * Class:     org_rocksdb_AbstractSlice
 * Method:    compare0
 * Signature: (JJ)I;
 */
jint Java_org_rocksdb_AbstractSlice_compare0(JNIEnv* /*env*/, jobject /*jobj*/,
                                             jlong handle, jlong otherHandle) {
  const auto* slice = reinterpret_cast<rocksdb::Slice*>(handle);
  const auto* otherSlice = reinterpret_cast<rocksdb::Slice*>(otherHandle);
  return slice->compare(*otherSlice);
}

/*
 * Class:     org_rocksdb_AbstractSlice
 * Method:    startsWith0
 * Signature: (JJ)Z;
 */
jboolean Java_org_rocksdb_AbstractSlice_startsWith0(JNIEnv* /*env*/,
                                                    jobject /*jobj*/,
                                                    jlong handle,
                                                    jlong otherHandle) {
  const auto* slice = reinterpret_cast<rocksdb::Slice*>(handle);
  const auto* otherSlice = reinterpret_cast<rocksdb::Slice*>(otherHandle);
  return slice->starts_with(*otherSlice);
}

/*
 * Class:     org_rocksdb_AbstractSlice
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_AbstractSlice_disposeInternal(JNIEnv* /*env*/,
                                                    jobject /*jobj*/,
                                                    jlong handle) {
  delete reinterpret_cast<rocksdb::Slice*>(handle);
}

// </editor-fold>

// <editor-fold desc="org.rocksdb.Slice>

/*
 * Class:     org_rocksdb_Slice
 * Method:    createNewSlice0
 * Signature: ([BI)J
 */
jlong Java_org_rocksdb_Slice_createNewSlice0(JNIEnv* env, jclass /*jcls*/,
                                             jbyteArray data, jint offset) {
  const jsize dataSize = env->GetArrayLength(data);
  const int len = dataSize - offset;

  // NOTE: buf will be deleted in the Java_org_rocksdb_Slice_disposeInternalBuf
  // method
  jbyte* buf = new jbyte[len];
  env->GetByteArrayRegion(data, offset, len, buf);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    return 0;
  }

  const auto* slice = new rocksdb::Slice((const char*)buf, len);
  return reinterpret_cast<jlong>(slice);
}

/*
 * Class:     org_rocksdb_Slice
 * Method:    createNewSlice1
 * Signature: ([B)J
 */
jlong Java_org_rocksdb_Slice_createNewSlice1(JNIEnv* env, jclass /*jcls*/,
                                             jbyteArray data) {
  jbyte* ptrData = env->GetByteArrayElements(data, nullptr);
  if (ptrData == nullptr) {
    // exception thrown: OutOfMemoryError
    return 0;
  }
  const int len = env->GetArrayLength(data) + 1;

  // NOTE: buf will be deleted in the Java_org_rocksdb_Slice_disposeInternalBuf
  // method
  char* buf = new char[len];
  memcpy(buf, ptrData, len - 1);
  buf[len - 1] = '\0';

  const auto* slice = new rocksdb::Slice(buf, len - 1);

  env->ReleaseByteArrayElements(data, ptrData, JNI_ABORT);

  return reinterpret_cast<jlong>(slice);
}

/*
 * Class:     org_rocksdb_Slice
 * Method:    data0
 * Signature: (J)[B
 */
jbyteArray Java_org_rocksdb_Slice_data0(JNIEnv* env, jobject /*jobj*/,
                                        jlong handle) {
  const auto* slice = reinterpret_cast<rocksdb::Slice*>(handle);
  const jsize len = static_cast<jsize>(slice->size());
  const jbyteArray data = env->NewByteArray(len);
  if (data == nullptr) {
    // exception thrown: OutOfMemoryError
    return nullptr;
  }

  env->SetByteArrayRegion(
      data, 0, len,
      const_cast<jbyte*>(reinterpret_cast<const jbyte*>(slice->data())));
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    env->DeleteLocalRef(data);
    return nullptr;
  }

  return data;
}

/*
 * Class:     org_rocksdb_Slice
 * Method:    clear0
 * Signature: (JZJ)V
 */
void Java_org_rocksdb_Slice_clear0(JNIEnv* /*env*/, jobject /*jobj*/,
                                   jlong handle, jboolean shouldRelease,
                                   jlong internalBufferOffset) {
  auto* slice = reinterpret_cast<rocksdb::Slice*>(handle);
  if (shouldRelease == JNI_TRUE) {
    const char* buf = slice->data_ - internalBufferOffset;
    delete[] buf;
  }
  slice->clear();
}

/*
 * Class:     org_rocksdb_Slice
 * Method:    removePrefix0
 * Signature: (JI)V
 */
void Java_org_rocksdb_Slice_removePrefix0(JNIEnv* /*env*/, jobject /*jobj*/,
                                          jlong handle, jint length) {
  auto* slice = reinterpret_cast<rocksdb::Slice*>(handle);
  slice->remove_prefix(length);
}

/*
 * Class:     org_rocksdb_Slice
 * Method:    disposeInternalBuf
 * Signature: (JJ)V
 */
void Java_org_rocksdb_Slice_disposeInternalBuf(JNIEnv* /*env*/,
                                               jobject /*jobj*/, jlong handle,
                                               jlong internalBufferOffset) {
  const auto* slice = reinterpret_cast<rocksdb::Slice*>(handle);
  const char* buf = slice->data_ - internalBufferOffset;
  delete[] buf;
}

// </editor-fold>

// <editor-fold desc="org.rocksdb.DirectSlice>

/*
 * Class:     org_rocksdb_DirectSlice
 * Method:    createNewDirectSlice0
 * Signature: (Ljava/nio/ByteBuffer;I)J
 */
jlong Java_org_rocksdb_DirectSlice_createNewDirectSlice0(JNIEnv* env,
                                                         jclass /*jcls*/,
                                                         jobject data,
                                                         jint length) {
  assert(data != nullptr);
  void* data_addr = env->GetDirectBufferAddress(data);
  if (data_addr == nullptr) {
    // error: memory region is undefined, given object is not a direct
    // java.nio.Buffer, or JNI access to direct buffers is not supported by JVM
    rocksdb::IllegalArgumentExceptionJni::ThrowNew(
        env, rocksdb::Status::InvalidArgument("Could not access DirectBuffer"));
    return 0;
  }

  const auto* ptrData = reinterpret_cast<char*>(data_addr);
  const auto* slice = new rocksdb::Slice(ptrData, length);
  return reinterpret_cast<jlong>(slice);
}

/*
 * Class:     org_rocksdb_DirectSlice
 * Method:    createNewDirectSlice1
 * Signature: (Ljava/nio/ByteBuffer;)J
 */
jlong Java_org_rocksdb_DirectSlice_createNewDirectSlice1(JNIEnv* env,
                                                         jclass /*jcls*/,
                                                         jobject data) {
  void* data_addr = env->GetDirectBufferAddress(data);
  if (data_addr == nullptr) {
    // error: memory region is undefined, given object is not a direct
    // java.nio.Buffer, or JNI access to direct buffers is not supported by JVM
    rocksdb::IllegalArgumentExceptionJni::ThrowNew(
        env, rocksdb::Status::InvalidArgument("Could not access DirectBuffer"));
    return 0;
  }

  const auto* ptrData = reinterpret_cast<char*>(data_addr);
  const auto* slice = new rocksdb::Slice(ptrData);
  return reinterpret_cast<jlong>(slice);
}

/*
 * Class:     org_rocksdb_DirectSlice
 * Method:    data0
 * Signature: (J)Ljava/lang/Object;
 */
jobject Java_org_rocksdb_DirectSlice_data0(JNIEnv* env, jobject /*jobj*/,
                                           jlong handle) {
  const auto* slice = reinterpret_cast<rocksdb::Slice*>(handle);
  return env->NewDirectByteBuffer(const_cast<char*>(slice->data()),
                                  slice->size());
}

/*
 * Class:     org_rocksdb_DirectSlice
 * Method:    get0
 * Signature: (JI)B
 */
jbyte Java_org_rocksdb_DirectSlice_get0(JNIEnv* /*env*/, jobject /*jobj*/,
                                        jlong handle, jint offset) {
  const auto* slice = reinterpret_cast<rocksdb::Slice*>(handle);
  return (*slice)[offset];
}

/*
 * Class:     org_rocksdb_DirectSlice
 * Method:    clear0
 * Signature: (JZJ)V
 */
void Java_org_rocksdb_DirectSlice_clear0(JNIEnv* /*env*/, jobject /*jobj*/,
                                         jlong handle, jboolean shouldRelease,
                                         jlong internalBufferOffset) {
  auto* slice = reinterpret_cast<rocksdb::Slice*>(handle);
  if (shouldRelease == JNI_TRUE) {
    const char* buf = slice->data_ - internalBufferOffset;
    delete[] buf;
  }
  slice->clear();
}

/*
 * Class:     org_rocksdb_DirectSlice
 * Method:    removePrefix0
 * Signature: (JI)V
 */
void Java_org_rocksdb_DirectSlice_removePrefix0(JNIEnv* /*env*/,
                                                jobject /*jobj*/, jlong handle,
                                                jint length) {
  auto* slice = reinterpret_cast<rocksdb::Slice*>(handle);
  slice->remove_prefix(length);
}

/*
 * Class:     org_rocksdb_DirectSlice
 * Method:    disposeInternalBuf
 * Signature: (JJ)V
 */
void Java_org_rocksdb_DirectSlice_disposeInternalBuf(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong handle,
    jlong internalBufferOffset) {
  const auto* slice = reinterpret_cast<rocksdb::Slice*>(handle);
  const char* buf = slice->data_ - internalBufferOffset;
  delete[] buf;
}

// </editor-fold>
