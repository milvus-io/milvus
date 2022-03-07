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

#ifndef __APPLE__
#include <malloc.h>
#endif

#include <knowhere/index/vector_index/adapter/VectorAdapter.h>
#include "exceptions/EasyAssert.h"
#include "indexbuilder/scalar_index_c.h"
#include "indexbuilder/ScalarIndexFactory.h"

CStatus
CreateScalarIndex(enum DataType dtype,
                  const char* serialized_type_params,
                  const char* serialized_index_params,
                  CScalarIndexBuilder* res_index) {
    auto status = CStatus();
    try {
        AssertInfo(res_index, "failed to create scalar index, passed index was null");

        *res_index = milvus::indexbuilder::ScalarIndexFactory::GetInstance()
                         .CreateScalarIndex(dtype, serialized_type_params, serialized_index_params)
                         .release();

        status.error_code = Success;
        status.error_msg = "";
    } catch (std::exception& e) {
        status.error_code = UnexpectedError;
        status.error_msg = strdup(e.what());
    }
    return status;
}

// field_data:
//  1, serialized proto::schema::BoolArray, if type is bool;
//  2, serialized proto::schema::StringArray, if type is string;
//  3, raw pointer, if type is of fundamental;
// TODO: optimize here if necessary.
CStatus
BuildScalarIndex(CScalarIndex c_index, const void* field_data, uint64_t size) {
    auto status = CStatus();
    try {
        AssertInfo(c_index, "failed to build scalar index, passed index was null");

        auto real_index = (milvus::indexbuilder::ScalarIndexCreatorBase*)c_index;
        const int64_t dim = 8;  // not important here
        auto dataset = milvus::knowhere::GenDataset(size, dim, field_data);
        real_index->Build(dataset);

        status.error_code = Success;
        status.error_msg = "";
    } catch (std::exception& e) {
        status.error_code = UnexpectedError;
        status.error_msg = strdup(e.what());
    }
    return status;
}

CStatus
SerializeScalarIndexToBinarySet(CScalarIndex c_index, CBinarySet* c_binary_set) {
    auto status = CStatus();
    try {
        AssertInfo(c_index, "failed to serialize scalar index, passed index was null");
        AssertInfo(c_binary_set, "failed to serialize scalar index, passed c_binary_set was null");

        auto real_index = (milvus::indexbuilder::ScalarIndexCreatorBase*)c_index;
        *c_binary_set = real_index->Serialize().release();

        status.error_code = Success;
        status.error_msg = "";
    } catch (std::exception& e) {
        status.error_code = UnexpectedError;
        status.error_msg = strdup(e.what());
    }
    return status;
}

CStatus
DeleteScalarIndex(CScalarIndex c_index) {
    auto status = CStatus();
    try {
        AssertInfo(c_index, "failed to serialize scalar index, passed index was null");

        auto real_index = (milvus::indexbuilder::ScalarIndexCreatorBase*)c_index;
        delete real_index;

#ifndef __APPLE__
        malloc_trim(0);
#endif

        status.error_code = Success;
        status.error_msg = "";
    } catch (std::exception& e) {
        status.error_code = UnexpectedError;
        status.error_msg = strdup(e.what());
    }
    return status;
}
