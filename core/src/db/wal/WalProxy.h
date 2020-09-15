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

#include "db/DBProxy.h"

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace milvus {
namespace engine {

class WalProxy : public DBProxy {
 public:
    WalProxy(const DBPtr& db, const DBOptions& options);

    Status
    Start() override;

    Status
    Stop() override;

    Status
    DropCollection(const std::string& collection_name) override;

    Status
    Insert(const std::string& collection_name, const std::string& partition_name, DataChunkPtr& data_chunk,
           idx_t op_id) override;

    Status
    DeleteEntityByID(const std::string& collection_name, const IDNumbers& entity_ids, idx_t op_id) override;

 private:
};

}  // namespace engine
}  // namespace milvus
