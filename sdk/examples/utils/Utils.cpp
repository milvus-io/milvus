// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "examples/utils/Utils.h"
#include "examples/utils/TimeRecorder.h"

#include <time.h>
#include <unistd.h>
#include <iostream>
#include <memory>
#include <utility>
#include <vector>

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
Utils::GenTableName() {
    static std::string s_id("tbl_" + CurrentTime());
    return s_id;
}

std::string
Utils::MetricTypeName(const milvus::MetricType& metric_type) {
    switch (metric_type) {
        case milvus::MetricType::L2:
            return "L2 distance";
        case milvus::MetricType::IP:
            return "Inner product";
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
        default:
            return "Unknown index type";
    }
}

void
Utils::PrintTableSchema(const milvus::TableSchema& tb_schema) {
    BLOCK_SPLITER
    std::cout << "Table name: " << tb_schema.table_name << std::endl;
    std::cout << "Table dimension: " << tb_schema.dimension << std::endl;
    std::cout << "Table index file size: " << tb_schema.index_file_size << std::endl;
    std::cout << "Table metric type: " << MetricTypeName(tb_schema.metric_type) << std::endl;
    BLOCK_SPLITER
}

void
Utils::PrintPartitionParam(const milvus::PartitionParam& partition_param) {
    BLOCK_SPLITER
    std::cout << "Table name: " << partition_param.table_name << std::endl;
    std::cout << "Partition name: " << partition_param.partition_name << std::endl;
    std::cout << "Partition tag: " << partition_param.partition_tag << std::endl;
    BLOCK_SPLITER
}

void
Utils::PrintIndexParam(const milvus::IndexParam& index_param) {
    BLOCK_SPLITER
    std::cout << "Index table name: " << index_param.table_name << std::endl;
    std::cout << "Index type: " << IndexTypeName(index_param.index_type) << std::endl;
    std::cout << "Index nlist: " << index_param.nlist << std::endl;
    BLOCK_SPLITER
}

void
Utils::BuildVectors(int64_t from, int64_t to, std::vector<milvus::RowRecord>& vector_record_array,
                    std::vector<int64_t>& record_ids, int64_t dimension) {
    if (to <= from) {
        return;
    }

    vector_record_array.clear();
    record_ids.clear();
    for (int64_t k = from; k < to; k++) {
        milvus::RowRecord record;
        record.data.resize(dimension);
        for (int64_t i = 0; i < dimension; i++) {
            record.data[i] = (float)(k % (i + 1));
        }

        vector_record_array.emplace_back(record);
        record_ids.push_back(k);
    }
}

void
Utils::PrintSearchResult(const std::vector<std::pair<int64_t, milvus::RowRecord>>& search_record_array,
                         const milvus::TopKQueryResult& topk_query_result) {
    BLOCK_SPLITER
    std::cout << "Returned result count: " << topk_query_result.size() << std::endl;

    if (topk_query_result.size() != search_record_array.size()) {
        std::cout << "ERROR: Returned result count dones equal nq" << std::endl;
        return;
    }

    for (size_t i = 0; i < topk_query_result.size(); i++) {
        const milvus::QueryResult& one_result = topk_query_result[i];
        size_t topk = one_result.ids.size();
        auto search_id = search_record_array[i].first;
        std::cout << "No." << i << " vector " << search_id << " top " << topk << " search result:" << std::endl;
        for (size_t j = 0; j < topk; j++) {
            std::cout << "\t" << one_result.ids[j] << "\t" << one_result.distances[j] << std::endl;
        }
    }
    BLOCK_SPLITER
}

void
Utils::CheckSearchResult(const std::vector<std::pair<int64_t, milvus::RowRecord>>& search_record_array,
                         const milvus::TopKQueryResult& topk_query_result) {
    BLOCK_SPLITER
    size_t nq = topk_query_result.size();
    for (size_t i = 0; i < nq; i++) {
        const milvus::QueryResult& one_result = topk_query_result[i];
        auto search_id = search_record_array[i].first;
        int64_t result_id = one_result.ids[0];
        if (result_id != search_id) {
            std::cout << "The top 1 result is wrong: " << result_id << " vs. " << search_id << std::endl;
        } else {
            std::cout << "No." << i << " Check result successfully" << std::endl;
        }
    }
    BLOCK_SPLITER
}

void
Utils::DoSearch(std::shared_ptr<milvus::Connection> conn, const std::string& table_name,
                const std::vector<std::string>& partition_tags, int64_t top_k, int64_t nprobe,
                const std::vector<std::pair<int64_t, milvus::RowRecord>>& search_record_array,
                milvus::TopKQueryResult& topk_query_result) {
    topk_query_result.clear();

    std::vector<milvus::Range> query_range_array;
    milvus::Range rg;
    rg.start_value = CurrentTmDate();
    rg.end_value = CurrentTmDate(1);
    query_range_array.emplace_back(rg);

    std::vector<milvus::RowRecord> record_array;
    for (auto& pair : search_record_array) {
        record_array.push_back(pair.second);
    }

    {
        BLOCK_SPLITER
        milvus_sdk::TimeRecorder rc("search");
        milvus::Status stat =
            conn->Search(table_name, partition_tags, record_array, query_range_array, top_k, nprobe, topk_query_result);
        std::cout << "SearchVector function call status: " << stat.message() << std::endl;
        BLOCK_SPLITER
    }

    PrintSearchResult(search_record_array, topk_query_result);
    CheckSearchResult(search_record_array, topk_query_result);
}

}  // namespace milvus_sdk
