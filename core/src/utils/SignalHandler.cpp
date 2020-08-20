// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#include "utils/SignalHandler.h"
#include "utils/Log.h"

#include <execinfo.h>
#include <string>

namespace milvus {

signal_func_ptr signal_routine_func = nullptr;

void
HandleSignal(int signum) {
    int32_t exit_code = 1; /* 0: normal exit; 1: exception */
    switch (signum) {
        case SIGINT:
        case SIGUSR2:
            exit_code = 0;
            /* no break */
        default: {
            if (exit_code == 0) {
                LOG_SERVER_INFO_ << "Server received signal: " << signum;
            } else {
                LOG_SERVER_INFO_ << "Server received critical signal: " << signum;
                PrintStacktrace();
            }
            if (signal_routine_func != nullptr) {
                (*signal_routine_func)(exit_code);
            }
        }
    }
}

void
PrintStacktrace() {
    const int bt_depth = 128;
    void* array[bt_depth];
    int stack_num = backtrace(array, bt_depth);
    char** stacktrace = backtrace_symbols(array, stack_num);

    LOG_SERVER_INFO_ << "Call stack:";
    for (int i = 0; i < stack_num; ++i) {
        std::string info = stacktrace[i];
        std::cout << "No." << i << ": " << info << std::endl;
        LOG_SERVER_INFO_ << info;
    }
    free(stacktrace);
}

}  // namespace milvus
