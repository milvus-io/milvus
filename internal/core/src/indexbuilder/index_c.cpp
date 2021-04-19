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

#include <string>
#include "index/knowhere/knowhere/index/vector_index/adapter/VectorAdapter.h"
#include "indexbuilder/IndexWrapper.h"
#include "indexbuilder/index_c.h"

CIndex
CreateIndex(const char* serialized_type_params, const char* serialized_index_params) {
    auto index = std::make_unique<milvus::indexbuilder::IndexWrapper>(serialized_type_params, serialized_index_params);

    return (void*)(index.release());
}

void
DeleteIndex(CIndex index) {
    auto cIndex = (milvus::indexbuilder::IndexWrapper*)index;
    delete cIndex;
}

void
BuildFloatVecIndex(CIndex index, int64_t float_value_num, const float* vectors) {
    auto cIndex = (milvus::indexbuilder::IndexWrapper*)index;
    auto dim = cIndex->dim();
    auto row_nums = float_value_num / dim;
    auto ds = milvus::knowhere::GenDataset(row_nums, dim, vectors);
    cIndex->BuildWithoutIds(ds);
}

char*
SerializeToSlicedBuffer(CIndex index, int32_t* buffer_size) {
    auto cIndex = (milvus::indexbuilder::IndexWrapper*)index;
    auto binary = cIndex->Serialize();
    *buffer_size = binary.size;
    return binary.data;
}

void
LoadFromSlicedBuffer(CIndex index, const char* serialized_sliced_blob_buffer) {
    auto cIndex = (milvus::indexbuilder::IndexWrapper*)index;
    cIndex->Load(serialized_sliced_blob_buffer);
}
