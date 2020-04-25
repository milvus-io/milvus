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

#include <string>
#include <memory>
#include <utility>
#include <vector>

#include <MilvusApi.h>

class ClientTest {
 public:
    ClientTest(const std::string&, const std::string&);
    ~ClientTest();

    void
    Test();

 private:
    void
    ShowServerVersion();

    void
    ShowSdkVersion();

    void
    ShowCollections(std::vector<std::string>&);

    void
    CreateCollection(const std::string&, int64_t, milvus::MetricType);

    void
    DescribeCollection(const std::string&);

    void
    InsertEntities(const std::string&, int64_t);

    void
    BuildSearchEntities(int64_t, int64_t);

    void
    Flush(const std::string&);

    void
    ShowCollectionInfo(const std::string&);

    void
    GetEntitiesByID(const std::string&, const std::vector<int64_t>&);

    void
    SearchEntities(const std::string&, int64_t, int64_t);

    void
    SearchEntitiesByID(const std::string&, int64_t, int64_t);

    void
    CreateIndex(const std::string&, milvus::IndexType, int64_t);

    void
    PreloadCollection(const std::string&);

    void
    CompactCollection(const std::string&);

    void
    DeleteByIds(const std::string&, const std::vector<int64_t>& id_array);

    void
    DropIndex(const std::string&);

    void
    DropCollection(const std::string&);

 private:
    std::shared_ptr<milvus::Connection> conn_;
    std::vector<std::pair<int64_t, milvus::Entity>> search_entity_array_;
    std::vector<int64_t> search_id_array_;
};
