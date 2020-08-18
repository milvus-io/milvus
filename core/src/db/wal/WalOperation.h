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

#include "db/Types.h"

#include <memory>
#include <string>

namespace milvus {
namespace engine {

///////////////////////////////////////////////////////////////////////////////////////////////////
enum WalOperationType {
    INVALID = 0,
    INSERT_ENTITY = 1,
    DELETE_ENTITY = 2,
};

///////////////////////////////////////////////////////////////////////////////////////////////////
class WalOperation {
 public:
    explicit WalOperation(WalOperationType type);

    void
    SetID(id_t id) {
        id_ = id;
    }
    id_t
    ID() const {
        return id_;
    }

    WalOperationType
    Type() const {
        return type_;
    }

 protected:
    id_t id_ = 0;
    WalOperationType type_ = WalOperationType::INVALID;
};

using WalOperationPtr = std::shared_ptr<WalOperation>;

///////////////////////////////////////////////////////////////////////////////////////////////////
class InsertEntityOperation : public WalOperation {
 public:
    InsertEntityOperation();

 public:
    std::string collection_name_;
    std::string partition_name;
    DataChunkPtr data_chunk_;
};

using InsertEntityOperationPtr = std::shared_ptr<InsertEntityOperation>;

///////////////////////////////////////////////////////////////////////////////////////////////////
class DeleteEntityOperation : public WalOperation {
 public:
    DeleteEntityOperation();

 public:
    std::string collection_name_;
    engine::IDNumbers entity_ids_;
};

using DeleteEntityOperationPtr = std::shared_ptr<DeleteEntityOperation>;

}  // namespace engine
}  // namespace milvus
