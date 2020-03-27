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

#include "include/MilvusApi.h"
#include "examples/utils/ThreadPool.h"

#include <string>
#include <list>
#include <memory>
#include <vector>
#include <future>

class ClientTest {
 public:
    ClientTest(const std::string&, const std::string&);
    ~ClientTest();

    void
    Test();

 private:
    std::shared_ptr<milvus::Connection>
    Connect();

    bool
    BuildCollection();

    void
    CreateIndex();

    void
    DropCollection();

    using EntityList = std::vector<milvus::Entity>;
    void
    BuildSearchEntities(std::vector<EntityList>& entity_array);

    void
    Search();

    milvus::TopKQueryResult
    SearchWorker(EntityList& entities);

 private:
    std::string server_ip_;
    std::string server_port_;

    milvus_sdk::ThreadPool query_thread_pool_;
    std::list<std::future<milvus::TopKQueryResult>> query_thread_results_;
};
