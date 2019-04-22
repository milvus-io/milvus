//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the callback "bridge" between Java and C++ for
// rocksdb::TableFilter.

#ifndef JAVA_ROCKSJNI_TABLE_FILTER_JNICALLBACK_H_
#define JAVA_ROCKSJNI_TABLE_FILTER_JNICALLBACK_H_

#include <jni.h>
#include <functional>
#include <memory>

#include "rocksdb/table_properties.h"
#include "rocksjni/jnicallback.h"

namespace rocksdb {

class TableFilterJniCallback : public JniCallback {
 public:
    TableFilterJniCallback(
        JNIEnv* env, jobject jtable_filter);
    std::function<bool(const rocksdb::TableProperties&)> GetTableFilterFunction();

 private:
    jmethodID m_jfilter_methodid;
    std::function<bool(const rocksdb::TableProperties&)> m_table_filter_function;
};

}  //namespace rocksdb

#endif  // JAVA_ROCKSJNI_TABLE_FILTER_JNICALLBACK_H_
