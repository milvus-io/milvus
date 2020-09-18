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

#include "examples/utils/Utils.h"

#include <time.h>
#include <unistd.h>

#include <iostream>
#include <memory>
#include <random>
#include <utility>
#include <vector>

#include "examples/utils/TimeRecorder.h"

namespace milvus_sdk {

constexpr int64_t SECONDS_EACH_HOUR = 3600;
constexpr int64_t BATCH_ENTITY_COUNT = 100000;
constexpr int64_t SEARCH_TARGET = BATCH_ENTITY_COUNT / 2;  // change this value, result is different

#define BLOCK_SPLITER std::cout << "===========================================" << std::endl;

std::string
Utils::CurrentTime() {
    time_t tt;
    time(&tt);
    tt = tt + 8 * SECONDS_EACH_HOUR;
    tm t;
    gmtime_r(&tt, &t);

    std::string str = std::to_string(t.tm_year + 1900) + "_" + std::to_string(t.tm_mon + 1) + "_" +
                      std::to_string(t.tm_mday) + "_" + std::to_string(t.tm_hour) + "_" + std::to_string(t.tm_min) +
                      "_" + std::to_string(t.tm_sec);

    return str;
}

std::string
Utils::CurrentTmDate(int64_t offset_day) {
    time_t tt;
    time(&tt);
    tt = tt + 8 * SECONDS_EACH_HOUR;
    tt = tt + 24 * SECONDS_EACH_HOUR * offset_day;
    tm t;
    gmtime_r(&tt, &t);

    std::string str =
        std::to_string(t.tm_year + 1900) + "-" + std::to_string(t.tm_mon + 1) + "-" + std::to_string(t.tm_mday);

    return str;
}

void
Utils::Sleep(int seconds) {
    std::cout << "Waiting " << seconds << " seconds ..." << std::endl;
    sleep(seconds);
}

const std::string&
Utils::GenCollectionName() {
    static std::string s_id("C_" + CurrentTime());
    return s_id;
}

std::string
Utils::MetricTypeName(const milvus::MetricType& metric_type) {
    switch (metric_type) {
        case milvus::MetricType::L2:
            return "L2 distance";
        case milvus::MetricType::IP:
            return "Inner product";
        case milvus::MetricType::HAMMING:
            return "Hamming distance";
        case milvus::MetricType::JACCARD:
            return "Jaccard distance";
        case milvus::MetricType::TANIMOTO:
            return "Tanimoto distance";
        case milvus::MetricType::SUBSTRUCTURE:
            return "Substructure distance";
        case milvus::MetricType::SUPERSTRUCTURE:
            return "Superstructure distance";
        default:
            return "Unknown metric type";
    }
}

std::string
Utils::IndexTypeName(const milvus::IndexType& index_type) {
    switch (index_type) {
        case milvus::IndexType::FLAT:
            return "FLAT";
        case milvus::IndexType::IVFFLAT:
            return "IVFFLAT";
        case milvus::IndexType::IVFSQ8:
            return "IVFSQ8";
        case milvus::IndexType::RNSG:
            return "NSG";
        case milvus::IndexType::IVFSQ8H:
            return "IVFSQ8H";
        case milvus::IndexType::IVFPQ:
            return "IVFPQ";
        case milvus::IndexType::SPTAGKDT:
            return "SPTAGKDT";
        case milvus::IndexType::SPTAGBKT:
            return "SPTAGBKT";
        case milvus::IndexType::HNSW:
            return "HNSW";
        case milvus::IndexType::RHNSWFLAT:
            return "RHNSWFLAT";
        case milvus::IndexType::RHNSWSQ:
            return "RHNSWSQ";
        case milvus::IndexType::RHNSWPQ:
            return "RHNSWPQ";
        case milvus::IndexType::ANNOY:
            return "ANNOY";
        case milvus::IndexType::NGTPANNG:
            return "NGTPANNG";
        case milvus::IndexType::NGTONNG:
            return "NGTONNG";
        default:
            return "Unknown index type";
    }
}

void
Utils::PrintCollectionParam(const milvus::Mapping& mapping) {
    BLOCK_SPLITER
    std::cout << "Collection name: " << mapping.collection_name << std::endl;
    for (const auto& field : mapping.fields) {
        std::cout << "field_name: " << field->field_name;
        std::cout << "\tfield_type: " << std::to_string((int)field->field_type);
        std::cout << "\tindex_param: " << field->index_params;
        std::cout << "\textra_param:" << field->extra_params << std::endl;
    }
    BLOCK_SPLITER
}

void
Utils::PrintPartitionParam(const milvus::PartitionParam& partition_param) {
    BLOCK_SPLITER
    std::cout << "Collection name: " << partition_param.collection_name << std::endl;
    std::cout << "Partition tag: " << partition_param.partition_tag << std::endl;
    BLOCK_SPLITER
}

void
Utils::PrintIndexParam(const milvus::IndexParam& index_param) {
    BLOCK_SPLITER
    std::cout << "Index collection name: " << index_param.collection_name << std::endl;
    std::cout << "Index field name: " << index_param.field_name << std::endl;
    std::cout << "Index extra_params: " << index_param.index_params << std::endl;
    BLOCK_SPLITER
}

void
Utils::PrintMapping(const milvus::Mapping& mapping) {
    BLOCK_SPLITER
    std::cout << "Collection name: " << mapping.collection_name << std::endl;
    for (const auto& field : mapping.fields) {
        std::cout << "field name: " << field->field_name << "\t field type: " << (int32_t)field->field_type
                  << "\t field index params:" << field->index_params << "\t field extra params: " << field->extra_params
                  << std::endl;
    }
    std::cout << "Collection extra params: " << mapping.extra_params << std::endl;
    BLOCK_SPLITER
}

void
Utils::BuildEntities(int64_t from, int64_t to, milvus::FieldValue& field_value, std::vector<int64_t>& entity_ids,
                     int64_t dimension) {
    if (to <= from) {
        return;
    }

    int64_t row_num = to - from;
    std::vector<int8_t> int8_data(row_num);
    std::vector<int64_t> int64_data(row_num);
    std::vector<float> float_data(row_num);
    std::vector<milvus::VectorData> entity_array;
    entity_array.clear();
    entity_ids.clear();
    for (int64_t k = from; k < to; k++) {
        milvus::VectorData vector_data;
        vector_data.float_data.resize(dimension);
        for (int64_t i = 0; i < dimension; i++) {
            vector_data.float_data[i] = (float)((k + 100) % (i + 1));
        }

        int8_data[k - from] = 1;
        int64_data[k - from] = k;
        float_data[k - from] = (float)k + row_num;

        entity_array.emplace_back(vector_data);
        entity_ids.push_back(k);
    }
    field_value.int8_value.insert(std::make_pair("field_3", int8_data));
    field_value.int64_value.insert(std::make_pair("field_1", int64_data));
    field_value.float_value.insert(std::make_pair("field_2", float_data));
    field_value.vector_value.insert(std::make_pair("field_vec", entity_array));
}

void
Utils::PrintSearchResult(const std::vector<std::pair<int64_t, milvus::VectorData>>& entity_array,
                         const milvus::TopKQueryResult& topk_query_result) {
    BLOCK_SPLITER
    std::cout << "Returned result count: " << topk_query_result.size() << std::endl;

    if (topk_query_result.size() != entity_array.size()) {
        std::cout << "ERROR: Returned result count not equal nq" << std::endl;
        return;
    }

    for (size_t i = 0; i < topk_query_result.size(); i++) {
        const milvus::QueryResult& one_result = topk_query_result[i];
        size_t topk = one_result.ids.size();
        auto search_id = entity_array[i].first;
        std::cout << "No." << i << " entity " << search_id << " top " << topk << " search result:" << std::endl;
        for (size_t j = 0; j < topk; j++) {
            std::cout << "\t" << one_result.ids[j] << "\t" << one_result.distances[j] << std::endl;
        }
    }
    BLOCK_SPLITER
}

void
Utils::CheckSearchResult(const std::vector<std::pair<int64_t, milvus::VectorData>>& entity_array,
                         const milvus::TopKQueryResult& topk_query_result) {
    BLOCK_SPLITER
    size_t nq = topk_query_result.size();
    for (size_t i = 0; i < nq; i++) {
        const milvus::QueryResult& one_result = topk_query_result[i];
        auto search_id = entity_array[i].first;

        uint64_t match_index = one_result.ids.size();
        for (uint64_t index = 0; index < one_result.ids.size(); index++) {
            if (search_id == one_result.ids[index]) {
                match_index = index;
                break;
            }
        }

        if (match_index >= one_result.ids.size()) {
            std::cout << "The topk result is wrong: not return search target in result set" << std::endl;
        } else {
            std::cout << "No." << i << " Check result successfully for target: " << search_id << " at top "
                      << match_index << std::endl;
        }
    }
    BLOCK_SPLITER
}

void
Utils::ConstructVectors(int64_t from, int64_t to, std::vector<milvus::VectorData>& query_vector,
                        std::vector<int64_t>& search_ids, int64_t dimension) {
    if (to <= from) {
        return;
    }

    query_vector.clear();
    search_ids.clear();
    for (int64_t k = from; k < to; k++) {
        milvus::VectorData entity;
        entity.float_data.resize(dimension);
        for (int64_t i = 0; i < dimension; i++) {
            entity.float_data[i] = (float)((k + 100) % (i + 1));
        }

        query_vector.emplace_back(entity);
        search_ids.push_back(k);
    }
}

std::vector<milvus::LeafQueryPtr>
Utils::GenLeafQuery() {
    // Construct TermQuery
    uint64_t row_num = 10000;
    std::vector<int64_t> field_value;
    field_value.resize(row_num);
    for (uint64_t i = 0; i < row_num; ++i) {
        field_value[i] = i;
    }
    milvus::TermQueryPtr tq = std::make_shared<milvus::TermQuery>();
    tq->field_name = "field_1";
    tq->int_value = field_value;

    // Construct RangeQuery
    milvus::CompareExpr ce1 = {milvus::CompareOperator::LTE, "100000"}, ce2 = {milvus::CompareOperator::GTE, "1"};
    std::vector<milvus::CompareExpr> ces{ce1, ce2};
    milvus::RangeQueryPtr rq = std::make_shared<milvus::RangeQuery>();
    rq->field_name = "field_2";
    rq->compare_expr = ces;

    // Construct VectorQuery
    uint64_t NQ = 10;
    uint64_t DIMENSION = 128;
    uint64_t NPROBE = 32;
    milvus::VectorQueryPtr vq = std::make_shared<milvus::VectorQuery>();

    std::vector<milvus::VectorData> search_entity_array;
    for (int64_t i = 0; i < NQ; i++) {
        std::vector<milvus::VectorData> entity_array;
        std::vector<int64_t> record_ids;
        int64_t index = i * BATCH_ENTITY_COUNT + SEARCH_TARGET;
        milvus_sdk::Utils::ConstructVectors(index, index + 1, entity_array, record_ids, DIMENSION);
        search_entity_array.push_back(entity_array[0]);
    }

    vq->query_vector = search_entity_array;
    vq->field_name = "field_vec";
    vq->topk = 10;
    JSON json_params = {{"nprobe", NPROBE}};
    vq->extra_params = json_params.dump();

    std::vector<milvus::LeafQueryPtr> lq;
    milvus::LeafQueryPtr lq1 = std::make_shared<milvus::LeafQuery>();
    milvus::LeafQueryPtr lq2 = std::make_shared<milvus::LeafQuery>();
    milvus::LeafQueryPtr lq3 = std::make_shared<milvus::LeafQuery>();
    lq.emplace_back(lq1);
    lq.emplace_back(lq2);
    lq.emplace_back(lq3);
    lq1->term_query_ptr = tq;
    lq2->range_query_ptr = rq;
    lq3->vector_query_ptr = vq;

    lq1->query_boost = 1.0;
    lq2->query_boost = 2.0;
    lq3->query_boost = 3.0;
    return lq;
}

void
Utils::GenDSLJson(nlohmann::json& dsl_json, nlohmann::json& vector_param_json, const std::string metric_type) {
    uint64_t row_num = 10000;
    std::vector<int64_t> term_value;
    term_value.resize(row_num);
    for (uint64_t i = 0; i < row_num; ++i) {
        term_value[i] = i;
    }

    nlohmann::json bool_json, term_json, range_json, vector_json;
    nlohmann::json term_value_json;
    term_value_json["values"] = term_value;
    term_json["term"]["field_1"] = term_value_json;
    bool_json["must"].push_back(term_json);

    nlohmann::json comp_json;
    comp_json["GT"] = 0;
    comp_json["LT"] = 100000;
    range_json["range"]["field_1"] = comp_json;
    bool_json["must"].push_back(range_json);

    std::string placeholder = "placeholder_1";
    vector_json["vector"] = placeholder;
    bool_json["must"].push_back(vector_json);

    dsl_json["bool"] = bool_json;

    nlohmann::json query_vector_json, vector_extra_params;
    int64_t topk = 10;
    query_vector_json["topk"] = topk;
    query_vector_json["metric_type"] = metric_type;
    vector_extra_params["nprobe"] = 64;
    query_vector_json["params"] = vector_extra_params;
    vector_param_json[placeholder]["field_vec"] = query_vector_json;
}

void
Utils::GenPureVecDSLJson(nlohmann::json& dsl_json, nlohmann::json& vector_param_json, const std::string metric_type) {
    nlohmann::json bool_json, vector_json;
    std::string placeholder = "placeholder_1";
    vector_json["vector"] = placeholder;
    bool_json["must"].push_back(vector_json);

    dsl_json["bool"] = bool_json;

    nlohmann::json query_vector_json, vector_extra_params;
    int64_t topk = 10;
    query_vector_json["topk"] = topk;
    query_vector_json["metric_type"] = metric_type;
    vector_extra_params["nprobe"] = 32;
    query_vector_json["params"] = vector_extra_params;
    vector_param_json[placeholder]["field_vec"] = query_vector_json;
}

void
Utils::PrintTopKQueryResult(milvus::TopKQueryResult& topk_query_result) {
    for (size_t i = 0; i < topk_query_result.size(); i++) {
        auto field_value = topk_query_result[i].field_value;
        for (auto& int32_it : field_value.int32_value) {
            std::cout << int32_it.first << ":";
            for (auto& data : int32_it.second) {
                std::cout << " " << data;
            }
            std::cout << std::endl;
        }
        for (auto& int64_it : field_value.int64_value) {
            std::cout << int64_it.first << ":";
            for (auto& data : int64_it.second) {
                std::cout << " " << data;
            }
            std::cout << std::endl;
        }
        for (auto& float_it : field_value.float_value) {
            std::cout << float_it.first << ":";
            for (auto& data : float_it.second) {
                std::cout << " " << data;
            }
            std::cout << std::endl;
        }
        for (auto& double_it : field_value.double_value) {
            std::cout << double_it.first << ":";
            for (auto& data : double_it.second) {
                std::cout << " " << data;
            }
            std::cout << std::endl;
        }
        for (size_t j = 0; j < topk_query_result[i].ids.size(); j++) {
            std::cout << topk_query_result[i].ids[j] << "  ---------  " << topk_query_result[i].distances[j]
                      << std::endl;
        }
        std::cout << std::endl;
    }
}

}  // namespace milvus_sdk
