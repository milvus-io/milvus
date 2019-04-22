// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the callback "bridge" between Java and C++ for
// rocksdb::TransactionNotifier.

#ifndef JAVA_ROCKSJNI_TRANSACTION_NOTIFIER_JNICALLBACK_H_
#define JAVA_ROCKSJNI_TRANSACTION_NOTIFIER_JNICALLBACK_H_

#include <jni.h>

#include "rocksdb/utilities/transaction.h"
#include "rocksjni/jnicallback.h"

namespace rocksdb {

/**
 * This class acts as a bridge between C++
 * and Java. The methods in this class will be
 * called back from the RocksDB TransactionDB or OptimisticTransactionDB (C++),
 * we then callback to the appropriate Java method
 * this enables TransactionNotifier to be implemented in Java.
 *
 * Unlike RocksJava's Comparator JNI Callback, we do not attempt
 * to reduce Java object allocations by caching the Snapshot object
 * presented to the callback. This could be revisited in future
 * if performance is lacking.
 */
class TransactionNotifierJniCallback: public JniCallback,
    public TransactionNotifier {
 public:
  TransactionNotifierJniCallback(JNIEnv* env, jobject jtransaction_notifier);
  virtual void SnapshotCreated(const Snapshot* newSnapshot);

 private:
  jmethodID m_jsnapshot_created_methodID;
};
}  // namespace rocksdb

#endif  // JAVA_ROCKSJNI_TRANSACTION_NOTIFIER_JNICALLBACK_H_
