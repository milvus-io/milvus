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
#include "indexbuilder/IndexCreatorBase.h"
#include "indexbuilder/ScalarIndexCreator.h"
#include "indexbuilder/VecIndexCreator.h"
#include "indexbuilder/type_c.h"
#include <memory>
#include <string>

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
    CreateIndex(CDataType dtype, const char* type_params, const char* index_params) {
        auto real_dtype = proto::schema::DataType(dtype);
        auto invalid_dtype_msg = std::string("invalid data type: ") + std::to_string(real_dtype);

        switch (real_dtype) {
            case milvus::proto::schema::Bool:
                return std::make_unique<ScalarIndexCreator<bool>>(type_params, index_params);
            case milvus::proto::schema::Int8:
                return std::make_unique<ScalarIndexCreator<int8_t>>(type_params, index_params);
            case milvus::proto::schema::Int16:
                return std::make_unique<ScalarIndexCreator<int16_t>>(type_params, index_params);
            case milvus::proto::schema::Int32:
                return std::make_unique<ScalarIndexCreator<int32_t>>(type_params, index_params);
            case milvus::proto::schema::Int64:
                return std::make_unique<ScalarIndexCreator<int64_t>>(type_params, index_params);
            case milvus::proto::schema::Float:
                return std::make_unique<ScalarIndexCreator<float_t>>(type_params, index_params);
            case milvus::proto::schema::Double:
                return std::make_unique<ScalarIndexCreator<double_t>>(type_params, index_params);

            case proto::schema::VarChar:
            case milvus::proto::schema::String:
                return std::make_unique<ScalarIndexCreator<std::string>>(type_params, index_params);

            case milvus::proto::schema::BinaryVector:
            case milvus::proto::schema::FloatVector:
                return std::make_unique<VecIndexCreator>(type_params, index_params);

            case milvus::proto::schema::None:
            case milvus::proto::schema::DataType_INT_MIN_SENTINEL_DO_NOT_USE_:
            case milvus::proto::schema::DataType_INT_MAX_SENTINEL_DO_NOT_USE_:
            default:
                throw std::invalid_argument(invalid_dtype_msg);
        }
    }
};

}  // namespace milvus::indexbuilder
