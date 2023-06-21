// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#pragma once

#include <pb/schema.pb.h>
#include <cmath>
#include <memory>
#include <string>

#include "indexbuilder/IndexCreatorBase.h"
#include "indexbuilder/ScalarIndexCreator.h"
#include "indexbuilder/VecIndexCreator.h"
#include "indexbuilder/type_c.h"
#include "storage/Types.h"
#include "storage/FileManager.h"

namespace milvus::indexbuilder {

// consider template factory if too many factories are needed.
class IndexFactory {
 public:
    IndexFactory() = default;
    IndexFactory(const IndexFactory&) = delete;
    IndexFactory
    operator=(const IndexFactory&) = delete;

 public:
    static IndexFactory&
    GetInstance() {
        // thread-safe enough after c++ 11
        static IndexFactory instance;
        return instance;
    }

    IndexCreatorBasePtr
    CreateIndex(DataType type,
                Config& config,
                storage::FileManagerImplPtr file_manager) {
        auto invalid_dtype_msg =
            std::string("invalid data type: ") + std::to_string(int(type));

        switch (type) {
            case DataType::BOOL:
            case DataType::INT8:
            case DataType::INT16:
            case DataType::INT32:
            case DataType::INT64:
            case DataType::FLOAT:
            case DataType::DOUBLE:
            case DataType::VARCHAR:
            case DataType::STRING:
                return CreateScalarIndex(type, config, file_manager);

            case DataType::VECTOR_FLOAT:
            case DataType::VECTOR_BINARY:
                return std::make_unique<VecIndexCreator>(
                    type, config, file_manager);
            default:
                throw std::invalid_argument(invalid_dtype_msg);
        }
    }
};

}  // namespace milvus::indexbuilder
