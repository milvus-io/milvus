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
#include <utility>
#include <vector>

#include "examples/utils/TimeRecorder.h"

namespace milvus_sdk {

constexpr int64_t SECONDS_EACH_HOUR = 3600;

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
        case milvus::MetricType::L2:return "L2 distance";
        case milvus::MetricType::IP:return "Inner product";
        case milvus::MetricType::HAMMING:return "Hamming distance";
        case milvus::MetricType::JACCARD:return "Jaccard distance";
        case milvus::MetricType::TANIMOTO:return "Tanimoto distance";
        case milvus::MetricType::SUBSTRUCTURE:return "Substructure distance";
        case milvus::MetricType::SUPERSTRUCTURE:return "Superstructure distance";
        default:return "Unknown metric type";
    }
}

std::string
Utils::IndexTypeName(const milvus::IndexType& index_type) {
    switch (index_type) {
        case milvus::IndexType::FLAT:return "FLAT";
        case milvus::IndexType::IVFFLAT:return "IVFFLAT";
        case milvus::IndexType::IVFSQ8:return "IVFSQ8";
        case milvus::IndexType::RNSG:return "NSG";
        case milvus::IndexType::IVFSQ8H:return "IVFSQ8H";
        case milvus::IndexType::IVFPQ:return "IVFPQ";
        case milvus::IndexType::SPTAGKDT:return "SPTAGKDT";
        case milvus::IndexType::SPTAGBKT:return "SPTAGBKT";
        case milvus::IndexType::HNSW:return "HNSW";
        case milvus::IndexType::ANNOY:return "ANNOY";
        default:return "Unknown index type";
    }
}

void
Utils::PrintCollectionParam(const milvus::CollectionParam& collection_param) {
    BLOCK_SPLITER
    std::cout << "Collection name: " << collection_param.collection_name << std::endl;
    std::cout << "Collection dimension: " << collection_param.dimension << std::endl;
    std::cout << "Collection index file size: " << collection_param.index_file_size << std::endl;
    std::cout << "Collection metric type: " << MetricTypeName(collection_param.metric_type) << std::endl;
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
    std::cout << "Index type: " << IndexTypeName(index_param.index_type) << std::endl;
    std::cout << "Index extra_params: " << index_param.extra_params << std::endl;
    BLOCK_SPLITER
}

void
Utils::BuildEntities(int64_t from, int64_t to, std::vector<milvus::Entity>& entity_array,
                     std::vector<int64_t>& entity_ids, int64_t dimension) {
    if (to <= from) {
        return;
    }

    entity_array.clear();
    entity_ids.clear();
    for (int64_t k = from; k < to; k++) {
        milvus::Entity entity;
        entity.float_data.resize(dimension);
        for (int64_t i = 0; i < dimension; i++) {
            entity.float_data[i] = (float)((k + 100) % (i + 1));
        }

        entity_array.emplace_back(entity);
        entity_ids.push_back(k);
    }
}

void
Utils::PrintSearchResult(const std::vector<std::pair<int64_t, milvus::Entity>>& entity_array,
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
Utils::CheckSearchResult(const std::vector<std::pair<int64_t, milvus::Entity>>& entity_array,
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
Utils::DoSearch(std::shared_ptr<milvus::Connection> conn, const std::string& collection_name,
                const std::vector<std::string>& partition_tags, int64_t top_k, int64_t nprobe,
                const std::vector<std::pair<int64_t, milvus::Entity>>& entity_array,
                milvus::TopKQueryResult& topk_query_result) {
    topk_query_result.clear();

    std::vector<milvus::Entity> temp_entity_array;
    for (auto& pair : entity_array) {
        temp_entity_array.push_back(pair.second);
    }

    {
        BLOCK_SPLITER
        JSON json_params = {{"nprobe", nprobe}};
        milvus_sdk::TimeRecorder rc("search");
        milvus::Status stat =
            conn->Search(collection_name,
                         partition_tags,
                         temp_entity_array,
                         top_k,
                         json_params.dump(),
                         topk_query_result);
        std::cout << "Search function call status: " << stat.message() << std::endl;
        BLOCK_SPLITER
    }

    PrintSearchResult(entity_array, topk_query_result);
    CheckSearchResult(entity_array, topk_query_result);
}

void
PrintPartitionStat(const milvus::PartitionStat& partition_stat) {
    std::cout << "\tPartition " << partition_stat.tag << " entity count: " << partition_stat.row_count << std::endl;
    for (auto& seg_stat : partition_stat.segments_stat) {
        std::cout << "\t\tsegment " << seg_stat.segment_name << " entity count: " << seg_stat.row_count
                  << " index: " << seg_stat.index_name << " data size: " << seg_stat.data_size << std::endl;
    }
}

void
Utils::PrintCollectionInfo(const milvus::CollectionInfo& info) {
    BLOCK_SPLITER
    std::cout << "Collection " << " total entity count: " << info.total_row_count << std::endl;
    for (const milvus::PartitionStat& partition_stat : info.partitions_stat) {
        PrintPartitionStat(partition_stat);
    }

    BLOCK_SPLITER
}

void ConstructVector(uint64_t nq, uint64_t dimension, std::vector<milvus::Entity>& query_vector) {
    query_vector.resize(nq);
    for (uint64_t i = 0; i < nq; ++i) {
        query_vector[i].float_data.resize(dimension);
        for (uint64_t j = 0; j < dimension; ++j) {
            query_vector[i].float_data[j] = (float)((i + 100) / (j + 1));
        }
    }
}

std::vector<milvus::LeafQueryPtr>
Utils::GenLeafQuery() {
    //Construct TermQuery
    uint64_t row_num = 1000;
    std::vector<int64_t> field_value;
    field_value.resize(row_num);
    for (uint64_t i = 0; i < row_num; ++i) {
        field_value[i] = i;
    }
    std::vector<int8_t> term_value(row_num * sizeof(int64_t));
    memcpy(term_value.data(), field_value.data(), row_num * sizeof(int64_t));
    milvus::TermQueryPtr tq = std::make_shared<milvus::TermQuery>();
    tq->field_name = "field_1";
    tq->field_value = term_value;

    //Construct RangeQuery
    milvus::CompareExpr ce1 = {milvus::CompareOperator::LTE, "10000"}, ce2 = {milvus::CompareOperator::GTE, "1"};
    std::vector<milvus::CompareExpr> ces{ce1, ce2};
    milvus::RangeQueryPtr rq = std::make_shared<milvus::RangeQuery>();
    rq->field_name = "field_2";
    rq->compare_expr = ces;

    //Construct VectorQuery
    uint64_t NQ = 10;
    uint64_t DIMENSION = 128;
    uint64_t NPROBE = 32;
    milvus::VectorQueryPtr vq = std::make_shared<milvus::VectorQuery>();
    ConstructVector(NQ, DIMENSION, vq->query_vector);
    vq->field_name = "field_3";
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

}  // namespace milvus_sdk
