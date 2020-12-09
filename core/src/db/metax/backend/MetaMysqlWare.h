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

#include "db/metax/MetaTraits.h"
#include "db/metax/backend/MetaEngineWare.h"
#include "db/metax/backend/MetaSqlContext.h"
#include "db/metax/backend/convertor/MetaSqlConvertor.h"
#include "db/metax/backend/engine/SqlBaseEngine.h"
#include "utils/Json.h"
#include "utils/Status.h"

namespace milvus::engine::metax {

class MetaMysqlWare : public MetaEngineWare, public MetaSqlConvertor, std::enable_shared_from_this<MetaMysqlWare> {
 public:
    MetaMysqlWare() = default;

    ~MetaMysqlWare() override = default;

    Status
    Insert(const MetaResFieldTuple& fields, snapshot::ID_TYPE& result_id) override;

 protected:
    Status
    Ser2InsertContext(const MetaResFieldTuple& fields, MetaSqlCUDContext& context);

 private:
    SqlEnginePtr engine_;
};

}  // namespace milvus::engine::metax
