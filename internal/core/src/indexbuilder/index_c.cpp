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

#ifndef __APPLE__

#include <malloc.h>

#endif

#include "exceptions/EasyAssert.h"
#include "knowhere/index/vector_index/adapter/VectorAdapter.h"
#include "indexbuilder/VecIndexCreator.h"
#include "indexbuilder/index_c.h"
#include "indexbuilder/IndexFactory.h"
#include "common/type_c.h"
#include "common/CGoHelper.h"

CStatus
CreateIndex(enum CDataType dtype,
            const char* serialized_type_params,
            const char* serialized_index_params,
            CIndex* res_index) {
    try {
        AssertInfo(res_index, "failed to create index, passed index was null");

        auto index = milvus::indexbuilder::IndexFactory::GetInstance().CreateIndex(dtype, serialized_type_params,
                                                                                   serialized_index_params);
        *res_index = index.release();
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

CStatus
DeleteIndex(CIndex index) {
    try {
        AssertInfo(index, "failed to delete index, passed index was null");
        auto cIndex = reinterpret_cast<milvus::indexbuilder::IndexCreatorBase*>(index);
        delete cIndex;
#ifdef __linux__
        malloc_trim(0);
#endif
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

CStatus
BuildFloatVecIndex(CIndex index, int64_t float_value_num, const float* vectors) {
    try {
        AssertInfo(index, "failed to build float vector index, passed index was null");
        auto real_index = reinterpret_cast<milvus::indexbuilder::IndexCreatorBase*>(index);
        auto cIndex = dynamic_cast<milvus::indexbuilder::VecIndexCreator*>(real_index);
        auto dim = cIndex->dim();
        auto row_nums = float_value_num / dim;
        auto ds = knowhere::GenDataset(row_nums, dim, vectors);
        cIndex->Build(ds);
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

CStatus
BuildBinaryVecIndex(CIndex index, int64_t data_size, const uint8_t* vectors) {
    try {
        AssertInfo(index, "failed to build binary vector index, passed index was null");
        auto real_index = reinterpret_cast<milvus::indexbuilder::IndexCreatorBase*>(index);
        auto cIndex = dynamic_cast<milvus::indexbuilder::VecIndexCreator*>(real_index);
        auto dim = cIndex->dim();
        auto row_nums = (data_size * 8) / dim;
        auto ds = knowhere::GenDataset(row_nums, dim, vectors);
        cIndex->Build(ds);
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

// field_data:
//  1, serialized proto::schema::BoolArray, if type is bool;
//  2, serialized proto::schema::StringArray, if type is string;
//  3, raw pointer, if type is of fundamental except bool type;
// TODO: optimize here if necessary.
CStatus
BuildScalarIndex(CIndex c_index, int64_t size, const void* field_data) {
    try {
        AssertInfo(c_index, "failed to build scalar index, passed index was null");

        auto real_index = reinterpret_cast<milvus::indexbuilder::IndexCreatorBase*>(c_index);
        const int64_t dim = 8;  // not important here
        auto dataset = knowhere::GenDataset(size, dim, field_data);
        real_index->Build(dataset);
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

CStatus
SerializeIndexToBinarySet(CIndex index, CBinarySet* c_binary_set) {
    try {
        AssertInfo(index, "failed to serialize index to binary set, passed index was null");
        auto real_index = reinterpret_cast<milvus::indexbuilder::IndexCreatorBase*>(index);
        auto binary = std::make_unique<knowhere::BinarySet>(real_index->Serialize());
        *c_binary_set = binary.release();
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

CStatus
LoadIndexFromBinarySet(CIndex index, CBinarySet c_binary_set) {
    try {
        AssertInfo(index, "failed to load index from binary set, passed index was null");
        auto real_index = reinterpret_cast<milvus::indexbuilder::IndexCreatorBase*>(index);
        auto binary_set = reinterpret_cast<knowhere::BinarySet*>(c_binary_set);
        real_index->Load(*binary_set);
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

CStatus
QueryOnFloatVecIndex(CIndex index, int64_t float_value_num, const float* vectors, CIndexQueryResult* res) {
    try {
        auto cIndex = reinterpret_cast<milvus::indexbuilder::VecIndexCreator*>(index);
        auto dim = cIndex->dim();
        auto row_nums = float_value_num / dim;
        auto query_ds = knowhere::GenDataset(row_nums, dim, vectors);
        auto query_res = cIndex->Query(query_ds);
        *res = query_res.release();
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

CStatus
QueryOnFloatVecIndexWithParam(CIndex index,
                              int64_t float_value_num,
                              const float* vectors,
                              const char* serialized_search_params,
                              CIndexQueryResult* res) {
    try {
        auto cIndex = reinterpret_cast<milvus::indexbuilder::VecIndexCreator*>(index);
        auto dim = cIndex->dim();
        auto row_nums = float_value_num / dim;
        auto query_ds = knowhere::GenDataset(row_nums, dim, vectors);
        auto query_res = cIndex->QueryWithParam(query_ds, serialized_search_params);
        *res = query_res.release();
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

CStatus
QueryOnBinaryVecIndex(CIndex index, int64_t data_size, const uint8_t* vectors, CIndexQueryResult* res) {
    try {
        auto cIndex = reinterpret_cast<milvus::indexbuilder::VecIndexCreator*>(index);
        auto dim = cIndex->dim();
        auto row_nums = (data_size * 8) / dim;
        auto query_ds = knowhere::GenDataset(row_nums, dim, vectors);
        auto query_res = cIndex->Query(query_ds);
        *res = query_res.release();
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

CStatus
QueryOnBinaryVecIndexWithParam(CIndex index,
                               int64_t data_size,
                               const uint8_t* vectors,
                               const char* serialized_search_params,
                               CIndexQueryResult* res) {
    try {
        auto cIndex = reinterpret_cast<milvus::indexbuilder::VecIndexCreator*>(index);
        auto dim = cIndex->dim();
        auto row_nums = (data_size * 8) / dim;
        auto query_ds = knowhere::GenDataset(row_nums, dim, vectors);
        auto query_res = cIndex->QueryWithParam(query_ds, serialized_search_params);
        *res = query_res.release();
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

CStatus
CreateQueryResult(CIndexQueryResult* res) {
    try {
        auto query_result = std::make_unique<milvus::indexbuilder::VecIndexCreator::QueryResult>();
        *res = query_result.release();
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

CStatus
NqOfQueryResult(CIndexQueryResult res, int64_t* nq) {
    try {
        auto c_res = reinterpret_cast<milvus::indexbuilder::VecIndexCreator::QueryResult*>(res);
        *nq = c_res->nq;
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

CStatus
TopkOfQueryResult(CIndexQueryResult res, int64_t* topK) {
    try {
        auto c_res = reinterpret_cast<milvus::indexbuilder::VecIndexCreator::QueryResult*>(res);
        *topK = c_res->topk;
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

CStatus
GetIdsOfQueryResult(CIndexQueryResult res, int64_t* ids) {
    try {
        auto c_res = reinterpret_cast<milvus::indexbuilder::VecIndexCreator::QueryResult*>(res);
        auto nq = c_res->nq;
        auto k = c_res->topk;
        // TODO: how could we avoid memory copy whenever this called
        memcpy(ids, c_res->ids.data(), sizeof(int64_t) * nq * k);
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

CStatus
GetDistancesOfQueryResult(CIndexQueryResult res, float* distances) {
    try {
        auto c_res = reinterpret_cast<milvus::indexbuilder::VecIndexCreator::QueryResult*>(res);
        auto nq = c_res->nq;
        auto k = c_res->topk;
        // TODO: how could we avoid memory copy whenever this called
        memcpy(distances, c_res->distances.data(), sizeof(float) * nq * k);
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

CStatus
DeleteIndexQueryResult(CIndexQueryResult res) {
    try {
        auto c_res = reinterpret_cast<milvus::indexbuilder::VecIndexCreator::QueryResult*>(res);
        delete c_res;
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}
