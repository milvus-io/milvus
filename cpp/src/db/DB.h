/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "Options.h"
#include "meta/Meta.h"
#include "Status.h"
#include "Types.h"

#include <string>

namespace zilliz {
namespace milvus {
namespace engine {

class Env;

class DB {
public:
    static void Open(const Options& options, DB** dbptr);

    virtual Status CreateTable(meta::TableSchema& table_schema_) = 0;
    virtual Status DeleteTable(const std::string& table_id, const meta::DatesT& dates) = 0;
    virtual Status DescribeTable(meta::TableSchema& table_schema_) = 0;
    virtual Status HasTable(const std::string& table_id, bool& has_or_not_) = 0;
    virtual Status AllTables(std::vector<meta::TableSchema>& table_schema_array) = 0;
    virtual Status GetTableRowCount(const std::string& table_id, uint64_t& row_count) = 0;
    virtual Status PreloadTable(const std::string& table_id) = 0;

    virtual Status InsertVectors(const std::string& table_id_,
            uint64_t n, const float* vectors, IDNumbers& vector_ids_) = 0;

    virtual Status Query(const std::string& table_id, uint64_t k, uint64_t nq, uint64_t nprobe,
            const float* vectors, QueryResults& results) = 0;

    virtual Status Query(const std::string& table_id, uint64_t k, uint64_t nq, uint64_t nprobe,
            const float* vectors, const meta::DatesT& dates, QueryResults& results) = 0;

    virtual Status Query(const std::string& table_id, const std::vector<std::string>& file_ids,
            uint64_t k, uint64_t nq, uint64_t nprobe, const float* vectors,
            const meta::DatesT& dates, QueryResults& results) = 0;

    virtual Status Size(uint64_t& result) = 0;

    virtual Status BuildIndex(const std::string& table_id) = 0;

    virtual Status DropAll() = 0;

    DB() = default;
    DB(const DB&) = delete;
    DB& operator=(const DB&) = delete;

    virtual ~DB() = 0;
}; // DB

} // namespace engine
} // namespace milvus
} // namespace zilliz
