/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "Options.h"
#include "Meta.h"
#include "Status.h"
#include "Types.h"

#include <string>

namespace zilliz {
namespace vecwise {
namespace engine {

class Env;

class DB {
public:
    static void Open(const Options& options, DB** dbptr);

    virtual Status CreateTable(meta::TableSchema& table_schema_) = 0;
    virtual Status DescribeTable(meta::TableSchema& table_schema_) = 0;
    virtual Status HasTable(const std::string& table_id_, bool& has_or_not_) = 0;

    virtual Status InsertVectors(const std::string& table_id_,
            size_t n, const float* vectors, IDNumbers& vector_ids_) = 0;

    virtual Status Query(const std::string& table_id, size_t k, size_t nq,
            const float* vectors, QueryResults& results) = 0;

    virtual Status Query(const std::string& table_id, size_t k, size_t nq,
            const float* vectors, const meta::DatesT& dates, QueryResults& results) = 0;

    virtual Status Size(long& result) = 0;

    virtual Status DropAll() = 0;

    DB() = default;
    DB(const DB&) = delete;
    DB& operator=(const DB&) = delete;

    virtual ~DB();
}; // DB

} // namespace engine
} // namespace vecwise
} // namespace zilliz
