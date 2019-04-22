//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the callback "bridge" between Java and C++ for
// rocksdb::CompactionFilterFactory.

#ifndef JAVA_ROCKSJNI_COMPACTION_FILTER_FACTORY_JNICALLBACK_H_
#define JAVA_ROCKSJNI_COMPACTION_FILTER_FACTORY_JNICALLBACK_H_

#include <jni.h>
#include <memory>

#include "rocksdb/compaction_filter.h"
#include "rocksjni/jnicallback.h"

namespace rocksdb {

class CompactionFilterFactoryJniCallback : public JniCallback, public CompactionFilterFactory {
 public:
    CompactionFilterFactoryJniCallback(
        JNIEnv* env, jobject jcompaction_filter_factory);
    virtual std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& context);
    virtual const char* Name() const;

 private:
    std::unique_ptr<const char[]> m_name;
    jmethodID m_jcreate_compaction_filter_methodid;
};

}  //namespace rocksdb

#endif  // JAVA_ROCKSJNI_COMPACTION_FILTER_FACTORY_JNICALLBACK_H_
