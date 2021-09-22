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

class CGODebugUtils {
 public:
    static int64_t
    Strlen(const char* str, int64_t size) {
        if (size == 0) {
            return size;
        } else {
            return strlen(str);
        }
    }
};

CStatus
CreateIndex(const char* serialized_type_params, const char* serialized_index_params, CIndex* res_index) {
    auto status = CStatus();
    try {
        auto index =
            std::make_unique<milvus::indexbuilder::IndexWrapper>(serialized_type_params, serialized_index_params);
        *res_index = index.release();
        status.error_code = Success;
        status.error_msg = "";
    } catch (std::exception& e) {
        status.error_code = UnexpectedError;
        status.error_msg = strdup(e.what());
    }
    return status;
}

void
DeleteIndex(CIndex index) {
    auto cIndex = (milvus::indexbuilder::IndexWrapper*)index;
    delete cIndex;
}

CStatus
BuildFloatVecIndexWithoutIds(CIndex index, int64_t float_value_num, const float* vectors) {
    auto status = CStatus();
    try {
        auto cIndex = (milvus::indexbuilder::IndexWrapper*)index;
        auto dim = cIndex->dim();
        auto row_nums = float_value_num / dim;
        auto ds = milvus::knowhere::GenDataset(row_nums, dim, vectors);
        cIndex->BuildWithoutIds(ds);
        status.error_code = Success;
        status.error_msg = "";
    } catch (std::exception& e) {
        status.error_code = UnexpectedError;
        status.error_msg = strdup(e.what());
    }
    return status;
}

CStatus
BuildBinaryVecIndexWithoutIds(CIndex index, int64_t data_size, const uint8_t* vectors) {
    auto status = CStatus();
    try {
        auto cIndex = (milvus::indexbuilder::IndexWrapper*)index;
        auto dim = cIndex->dim();
        auto row_nums = (data_size * 8) / dim;
        auto ds = milvus::knowhere::GenDataset(row_nums, dim, vectors);
        cIndex->BuildWithoutIds(ds);
        status.error_code = Success;
        status.error_msg = "";
    } catch (std::exception& e) {
        status.error_code = UnexpectedError;
        status.error_msg = strdup(e.what());
    }
    return status;
}

CStatus
SerializeToSlicedBuffer(CIndex index, CBinary* c_binary) {
    auto status = CStatus();
    try {
        auto cIndex = (milvus::indexbuilder::IndexWrapper*)index;
        auto binary = cIndex->Serialize();
        *c_binary = binary.release();
        status.error_code = Success;
        status.error_msg = "";
    } catch (std::exception& e) {
        status.error_code = UnexpectedError;
        status.error_msg = strdup(e.what());
    }
    return status;
}

int64_t
GetCBinarySize(CBinary c_binary) {
    auto cBinary = (milvus::indexbuilder::IndexWrapper::Binary*)c_binary;
    return cBinary->data.size();
}

// Note: the memory of data is allocated outside
void
GetCBinaryData(CBinary c_binary, void* data) {
    auto cBinary = (milvus::indexbuilder::IndexWrapper::Binary*)c_binary;
    memcpy(data, cBinary->data.data(), cBinary->data.size());
}

void
DeleteCBinary(CBinary c_binary) {
    auto cBinary = (milvus::indexbuilder::IndexWrapper::Binary*)c_binary;
    delete cBinary;
}

CStatus
LoadFromSlicedBuffer(CIndex index, const char* serialized_sliced_blob_buffer, int32_t size) {
    auto status = CStatus();
    try {
        auto cIndex = (milvus::indexbuilder::IndexWrapper*)index;
        cIndex->Load(serialized_sliced_blob_buffer, size);
        status.error_code = Success;
        status.error_msg = "";
    } catch (std::exception& e) {
        status.error_code = UnexpectedError;
        status.error_msg = strdup(e.what());
    }
    return status;
}

CStatus
QueryOnFloatVecIndex(CIndex index, int64_t float_value_num, const float* vectors, CIndexQueryResult* res) {
    auto status = CStatus();
    try {
        auto cIndex = (milvus::indexbuilder::IndexWrapper*)index;
        auto dim = cIndex->dim();
        auto row_nums = float_value_num / dim;
        auto query_ds = milvus::knowhere::GenDataset(row_nums, dim, vectors);
        auto query_res = cIndex->Query(query_ds);
        *res = query_res.release();

        status.error_code = Success;
        status.error_msg = "";
    } catch (std::exception& e) {
        status.error_code = UnexpectedError;
        status.error_msg = strdup(e.what());
    }
    return status;
}

CStatus
QueryOnFloatVecIndexWithParam(CIndex index,
                              int64_t float_value_num,
                              const float* vectors,
                              const char* serialized_search_params,
                              CIndexQueryResult* res) {
    auto status = CStatus();
    try {
        auto cIndex = (milvus::indexbuilder::IndexWrapper*)index;
        auto dim = cIndex->dim();
        auto row_nums = float_value_num / dim;
        auto query_ds = milvus::knowhere::GenDataset(row_nums, dim, vectors);
        auto query_res = cIndex->QueryWithParam(query_ds, serialized_search_params);
        *res = query_res.release();

        status.error_code = Success;
        status.error_msg = "";
    } catch (std::exception& e) {
        status.error_code = UnexpectedError;
        status.error_msg = strdup(e.what());
    }
    return status;
}

CStatus
QueryOnBinaryVecIndex(CIndex index, int64_t data_size, const uint8_t* vectors, CIndexQueryResult* res) {
    auto status = CStatus();
    try {
        auto cIndex = (milvus::indexbuilder::IndexWrapper*)index;
        auto dim = cIndex->dim();
        auto row_nums = (data_size * 8) / dim;
        auto query_ds = milvus::knowhere::GenDataset(row_nums, dim, vectors);
        auto query_res = cIndex->Query(query_ds);
        *res = query_res.release();

        status.error_code = Success;
        status.error_msg = "";
    } catch (std::exception& e) {
        status.error_code = UnexpectedError;
        status.error_msg = strdup(e.what());
    }
    return status;
}

CStatus
QueryOnBinaryVecIndexWithParam(CIndex index,
                               int64_t data_size,
                               const uint8_t* vectors,
                               const char* serialized_search_params,
                               CIndexQueryResult* res) {
    auto status = CStatus();
    try {
        auto cIndex = (milvus::indexbuilder::IndexWrapper*)index;
        auto dim = cIndex->dim();
        auto row_nums = (data_size * 8) / dim;
        auto query_ds = milvus::knowhere::GenDataset(row_nums, dim, vectors);
        auto query_res = cIndex->QueryWithParam(query_ds, serialized_search_params);
        *res = query_res.release();

        status.error_code = Success;
        status.error_msg = "";
    } catch (std::exception& e) {
        status.error_code = UnexpectedError;
        status.error_msg = strdup(e.what());
    }
    return status;
}

CStatus
CreateQueryResult(CIndexQueryResult* res) {
    auto status = CStatus();
    try {
        auto query_result = std::make_unique<milvus::indexbuilder::IndexWrapper::QueryResult>();
        *res = query_result.release();

        status.error_code = Success;
        status.error_msg = "";
    } catch (std::exception& e) {
        status.error_code = UnexpectedError;
        status.error_msg = strdup(e.what());
    }
    return status;
}

int64_t
NqOfQueryResult(CIndexQueryResult res) {
    auto c_res = (milvus::indexbuilder::IndexWrapper::QueryResult*)res;
    return c_res->nq;
}

int64_t
TopkOfQueryResult(CIndexQueryResult res) {
    auto c_res = (milvus::indexbuilder::IndexWrapper::QueryResult*)res;
    return c_res->topk;
}

void
GetIdsOfQueryResult(CIndexQueryResult res, int64_t* ids) {
    auto c_res = (milvus::indexbuilder::IndexWrapper::QueryResult*)res;
    auto nq = c_res->nq;
    auto k = c_res->topk;
    // TODO: how could we avoid memory copy every time when this called
    memcpy(ids, c_res->ids.data(), sizeof(int64_t) * nq * k);
}

void
GetDistancesOfQueryResult(CIndexQueryResult res, float* distances) {
    auto c_res = (milvus::indexbuilder::IndexWrapper::QueryResult*)res;
    auto nq = c_res->nq;
    auto k = c_res->topk;
    // TODO: how could we avoid memory copy every time when this called
    memcpy(distances, c_res->distances.data(), sizeof(float) * nq * k);
}

CStatus
DeleteIndexQueryResult(CIndexQueryResult res) {
    auto status = CStatus();
    try {
        auto c_res = (milvus::indexbuilder::IndexWrapper::QueryResult*)res;
        delete c_res;

        status.error_code = Success;
        status.error_msg = "";
    } catch (std::exception& e) {
        status.error_code = UnexpectedError;
        status.error_msg = strdup(e.what());
    }
    return status;
}

void
DeleteByteArray(const char* array) {
    delete[] array;
}
