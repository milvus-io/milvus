// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "logging_c.h"
#include <glog/logging.h>

#ifdef WITHOUT_GO_LOGGING

// Empty implementation when there's no go logging implementation.
void
goZapLogExt(int severity,
            const char* file,
            int file_len,
            int line,
            const char* msg,
            int msg_len) {
}

#elif defined(__APPLE__)

// Go export function.
// will be implemented in github.com/milvus-io/milvus/internal/util/cgo/logging
// macOS linker requires weak_import to allow unresolved symbols.
extern "C" void
goZapLogExt(int severity,
            const char* file,
            int file_len,
            int line,
            const char* msg,
            int msg_len) {
}
__attribute__((weak_import));

#else

// Go export function.
// will be implemented in github.com/milvus-io/milvus/internal/util/cgo/logging
extern "C" void
goZapLogExt(int severity,
            const char* file,
            int file_len,
            int line,
            const char* msg,
            int msg_len);

#endif

class GoZapSink : public google::LogSink {
    void
    send(google::LogSeverity severity,
         const char* full_filename,
         const char* base_filename,
         int line,
         const struct tm*,
         const char* message,
         size_t message_len) override;
};

void
GoZapSink::send(google::LogSeverity severity,
                const char* full_filename,
                const char* base_filename,
                int line,
                const struct tm*,
                const char* message,
                size_t message_len) {
    goZapLogExt(static_cast<int>(severity),
                full_filename,
                strlen(full_filename),
                line,
                message,
                message_len);
};

static GoZapSink g_sink;

extern "C" {
void
InitGoogleLoggingWithZapSink() {
    if (google::IsGoogleLoggingInitialized()) {
        return;
    }
    google::InitGoogleLogging("milvus");
    google::AddLogSink(&g_sink);

    // log is catched by zap, so we don't need to log to stderr/stdout/files anymore.
    FLAGS_logtostdout = false;
    FLAGS_logtostderr = false;
    FLAGS_alsologtostderr = false;
    FLAGS_log_dir = "";
    return;
}

void
InitGoogleLoggingWithoutZapSink() {
    if (google::IsGoogleLoggingInitialized()) {
        return;
    }
    google::InitGoogleLogging("milvus");
    // log is catched by zap, so we don't need to log to stderr/stdout/files anymore.
    FLAGS_logtostdout = true;
    FLAGS_logtostderr = false;
    FLAGS_alsologtostderr = false;
    FLAGS_log_dir = "";
    return;
}

void
GoogleLoggingAtLevel(int severity, const char* msg) {
    google::LogAtLevel(severity, msg);
}
}