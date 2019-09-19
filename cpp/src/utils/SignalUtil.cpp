// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "SignalUtil.h"
#include "src/server/Server.h"
#include "utils/Log.h"

#include <signal.h>
#include <execinfo.h>


namespace zilliz {
namespace milvus {
namespace server {

void SignalUtil::HandleSignal(int signum) {

    switch (signum) {
        case SIGINT:
        case SIGUSR2: {
            SERVER_LOG_INFO << "Server received signal: " << signum;

            server::Server &server_ptr = server::Server::Instance();
            server_ptr.Stop();

            exit(0);
        }
        default: {
            SERVER_LOG_INFO << "Server received critical signal: " << signum;
            SignalUtil::PrintStacktrace();

            server::Server &server_ptr = server::Server::Instance();
            server_ptr.Stop();

            exit(1);
        }
    }
}

void SignalUtil::PrintStacktrace() {
    SERVER_LOG_INFO << "Call stack:";

    const int size = 32;
    void *array[size];
    int stack_num = backtrace(array, size);
    char **stacktrace = backtrace_symbols(array, stack_num);
    for (int i = 0; i < stack_num; ++i) {
        std::string info = stacktrace[i];
        SERVER_LOG_INFO << info;
    }
    free(stacktrace);
}

}
}
}
