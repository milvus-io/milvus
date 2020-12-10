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

#include <memory>
#include <string>

#include "db/Types.h"
#include "db/metax/MetaDef.h"
#include "db/metax/MetaQuery.h"
#include "db/metax/MetaResField.h"
#include "db/metax/backend/MetaMysqlWare.h"
#include "db/snapshot/ResourceTypes.h"
#include "utils/Status.h"

namespace milvus::engine::metax {

class MetaProxy {
 public:
    explicit MetaProxy(EngineType type) : type_(type) {
        switch (type) {
            case EngineType::mysql_: {
                ware_ = std::make_shared<MetaMysqlWare>();
                break;
            }
            case EngineType::sqlite_: {
                break;
            }
            case EngineType::mock_: {
                break;
            }
            default: { break; }
        }
    }

    Status
    Insert(const MetaResFieldTuple& fields, snapshot::ID_TYPE& result_id) {
        return ware_->Insert(fields, result_id);
    }



 private:
    EngineType type_;
    MetaEngineWarePtr ware_;
};

using MetaProxyPtr = std::shared_ptr<MetaProxy>;

}  // namespace milvus::engine::metax
