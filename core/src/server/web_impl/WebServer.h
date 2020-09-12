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

#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <thread>

#include "server/web_impl/component/AppComponent.hpp"

#include "utils/Status.h"

namespace milvus {
namespace server {
namespace web {

class WebServer {
 public:
    static WebServer&
    GetInstance() {
        static WebServer web_server;
        return web_server;
    }

    void
    Start();

    void
    Stop();

 private:
    WebServer() {
        try_stop_.store(false);
    }

    ~WebServer() = default;

    Status
    StartService();
    Status
    StopService();

 private:
    std::atomic_bool try_stop_;

    std::shared_ptr<std::thread> thread_ptr_;
};

}  // namespace web
}  // namespace server
}  // namespace milvus
