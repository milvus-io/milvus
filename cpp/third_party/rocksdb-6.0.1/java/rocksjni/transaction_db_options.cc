// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++
// for rocksdb::TransactionDBOptions.

#include <jni.h>

#include "include/org_rocksdb_TransactionDBOptions.h"

#include "rocksdb/utilities/transaction_db.h"

#include "rocksjni/portal.h"

/*
 * Class:     org_rocksdb_TransactionDBOptions
 * Method:    newTransactionDBOptions
 * Signature: ()J
 */
jlong Java_org_rocksdb_TransactionDBOptions_newTransactionDBOptions(
    JNIEnv* /*env*/, jclass /*jcls*/) {
  rocksdb::TransactionDBOptions* opts = new rocksdb::TransactionDBOptions();
  return reinterpret_cast<jlong>(opts);
}

/*
 * Class:     org_rocksdb_TransactionDBOptions
 * Method:    getMaxNumLocks
 * Signature: (J)J
 */
jlong Java_org_rocksdb_TransactionDBOptions_getMaxNumLocks(JNIEnv* /*env*/,
                                                           jobject /*jobj*/,
                                                           jlong jhandle) {
  auto* opts = reinterpret_cast<rocksdb::TransactionDBOptions*>(jhandle);
  return opts->max_num_locks;
}

/*
 * Class:     org_rocksdb_TransactionDBOptions
 * Method:    setMaxNumLocks
 * Signature: (JJ)V
 */
void Java_org_rocksdb_TransactionDBOptions_setMaxNumLocks(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle, jlong jmax_num_locks) {
  auto* opts = reinterpret_cast<rocksdb::TransactionDBOptions*>(jhandle);
  opts->max_num_locks = jmax_num_locks;
}

/*
 * Class:     org_rocksdb_TransactionDBOptions
 * Method:    getNumStripes
 * Signature: (J)J
 */
jlong Java_org_rocksdb_TransactionDBOptions_getNumStripes(JNIEnv* /*env*/,
                                                          jobject /*jobj*/,
                                                          jlong jhandle) {
  auto* opts = reinterpret_cast<rocksdb::TransactionDBOptions*>(jhandle);
  return opts->num_stripes;
}

/*
 * Class:     org_rocksdb_TransactionDBOptions
 * Method:    setNumStripes
 * Signature: (JJ)V
 */
void Java_org_rocksdb_TransactionDBOptions_setNumStripes(JNIEnv* /*env*/,
                                                         jobject /*jobj*/,
                                                         jlong jhandle,
                                                         jlong jnum_stripes) {
  auto* opts = reinterpret_cast<rocksdb::TransactionDBOptions*>(jhandle);
  opts->num_stripes = jnum_stripes;
}

/*
 * Class:     org_rocksdb_TransactionDBOptions
 * Method:    getTransactionLockTimeout
 * Signature: (J)J
 */
jlong Java_org_rocksdb_TransactionDBOptions_getTransactionLockTimeout(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle) {
  auto* opts = reinterpret_cast<rocksdb::TransactionDBOptions*>(jhandle);
  return opts->transaction_lock_timeout;
}

/*
 * Class:     org_rocksdb_TransactionDBOptions
 * Method:    setTransactionLockTimeout
 * Signature: (JJ)V
 */
void Java_org_rocksdb_TransactionDBOptions_setTransactionLockTimeout(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle,
    jlong jtransaction_lock_timeout) {
  auto* opts = reinterpret_cast<rocksdb::TransactionDBOptions*>(jhandle);
  opts->transaction_lock_timeout = jtransaction_lock_timeout;
}

/*
 * Class:     org_rocksdb_TransactionDBOptions
 * Method:    getDefaultLockTimeout
 * Signature: (J)J
 */
jlong Java_org_rocksdb_TransactionDBOptions_getDefaultLockTimeout(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle) {
  auto* opts = reinterpret_cast<rocksdb::TransactionDBOptions*>(jhandle);
  return opts->default_lock_timeout;
}

/*
 * Class:     org_rocksdb_TransactionDBOptions
 * Method:    setDefaultLockTimeout
 * Signature: (JJ)V
 */
void Java_org_rocksdb_TransactionDBOptions_setDefaultLockTimeout(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle,
    jlong jdefault_lock_timeout) {
  auto* opts = reinterpret_cast<rocksdb::TransactionDBOptions*>(jhandle);
  opts->default_lock_timeout = jdefault_lock_timeout;
}

/*
 * Class:     org_rocksdb_TransactionDBOptions
 * Method:    getWritePolicy
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_TransactionDBOptions_getWritePolicy(JNIEnv* /*env*/,
                                                           jobject /*jobj*/,
                                                           jlong jhandle) {
  auto* opts = reinterpret_cast<rocksdb::TransactionDBOptions*>(jhandle);
  return rocksdb::TxnDBWritePolicyJni::toJavaTxnDBWritePolicy(
      opts->write_policy);
}

/*
 * Class:     org_rocksdb_TransactionDBOptions
 * Method:    setWritePolicy
 * Signature: (JB)V
 */
void Java_org_rocksdb_TransactionDBOptions_setWritePolicy(JNIEnv* /*env*/,
                                                          jobject /*jobj*/,
                                                          jlong jhandle,
                                                          jbyte jwrite_policy) {
  auto* opts = reinterpret_cast<rocksdb::TransactionDBOptions*>(jhandle);
  opts->write_policy =
      rocksdb::TxnDBWritePolicyJni::toCppTxnDBWritePolicy(jwrite_policy);
}

/*
 * Class:     org_rocksdb_TransactionDBOptions
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_TransactionDBOptions_disposeInternal(JNIEnv* /*env*/,
                                                           jobject /*jobj*/,
                                                           jlong jhandle) {
  delete reinterpret_cast<rocksdb::TransactionDBOptions*>(jhandle);
}
