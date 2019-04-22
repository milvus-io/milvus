//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the callback "bridge" between Java and C++ for
// rocksdb::TraceWriter.

#ifndef JAVA_ROCKSJNI_TRACE_WRITER_JNICALLBACK_H_
#define JAVA_ROCKSJNI_TRACE_WRITER_JNICALLBACK_H_

#include <jni.h>
#include <memory>

#include "rocksdb/trace_reader_writer.h"
#include "rocksjni/jnicallback.h"

namespace rocksdb {

class TraceWriterJniCallback : public JniCallback, public TraceWriter {
 public:
    TraceWriterJniCallback(
        JNIEnv* env, jobject jtrace_writer);
    virtual Status Write(const Slice& data);
    virtual Status Close();
    virtual uint64_t GetFileSize();

 private:
    jmethodID m_jwrite_proxy_methodid;
    jmethodID m_jclose_writer_proxy_methodid;
    jmethodID m_jget_file_size_methodid;
};

}  //namespace rocksdb

#endif  // JAVA_ROCKSJNI_TRACE_WRITER_JNICALLBACK_H_
