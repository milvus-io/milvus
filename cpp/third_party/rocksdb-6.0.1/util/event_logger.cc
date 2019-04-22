//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include "util/event_logger.h"

#include <inttypes.h>
#include <cassert>
#include <sstream>
#include <string>

#include "util/logging.h"
#include "util/string_util.h"

namespace rocksdb {


EventLoggerStream::EventLoggerStream(Logger* logger)
    : logger_(logger), log_buffer_(nullptr), json_writer_(nullptr) {}

EventLoggerStream::EventLoggerStream(LogBuffer* log_buffer)
    : logger_(nullptr), log_buffer_(log_buffer), json_writer_(nullptr) {}

EventLoggerStream::~EventLoggerStream() {
  if (json_writer_) {
    json_writer_->EndObject();
#ifdef ROCKSDB_PRINT_EVENTS_TO_STDOUT
    printf("%s\n", json_writer_->Get().c_str());
#else
    if (logger_) {
      EventLogger::Log(logger_, *json_writer_);
    } else if (log_buffer_) {
      EventLogger::LogToBuffer(log_buffer_, *json_writer_);
    }
#endif
    delete json_writer_;
  }
}

void EventLogger::Log(const JSONWriter& jwriter) {
  Log(logger_, jwriter);
}

void EventLogger::Log(Logger* logger, const JSONWriter& jwriter) {
#ifdef ROCKSDB_PRINT_EVENTS_TO_STDOUT
  printf("%s\n", jwriter.Get().c_str());
#else
  rocksdb::Log(logger, "%s %s", Prefix(), jwriter.Get().c_str());
#endif
}

void EventLogger::LogToBuffer(
    LogBuffer* log_buffer, const JSONWriter& jwriter) {
#ifdef ROCKSDB_PRINT_EVENTS_TO_STDOUT
  printf("%s\n", jwriter.Get().c_str());
#else
  assert(log_buffer);
  rocksdb::LogToBuffer(log_buffer, "%s %s", Prefix(), jwriter.Get().c_str());
#endif
}

}  // namespace rocksdb
