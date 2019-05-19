/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include <string>
#include "Options.h"
#include "Meta.h"
#include "Status.h"
#include "Types.h"

namespace zilliz {
namespace vecwise {
namespace engine {

class Env;

class DB {
public:
    static void Open(const Options& options, DB** dbptr);

    virtual Status add_group(meta::GroupSchema& group_info_) = 0;
    virtual Status get_group(meta::GroupSchema& group_info_) = 0;
    virtual Status delete_vectors(const std::string& group_id,
            const meta::DatesT& dates) = 0;
    virtual Status has_group(const std::string& group_id_, bool& has_or_not_) = 0;
    virtual Status get_group_files(const std::string& group_id_,
                                   const int date_delta_,
                                   meta::GroupFilesSchema& group_files_info_) = 0;

    virtual Status add_vectors(const std::string& group_id_,
            size_t n, const float* vectors, IDNumbers& vector_ids_) = 0;

    virtual Status search(const std::string& group_id, size_t k, size_t nq,
            const float* vectors, QueryResults& results) = 0;

    virtual Status search(const std::string& group_id, size_t k, size_t nq,
            const float* vectors, const meta::DatesT& dates, QueryResults& results) = 0;

    virtual Status drop_all() = 0;

    virtual Status count(const std::string& group_id, long& result) = 0;

    DB() = default;
    DB(const DB&) = delete;
    DB& operator=(const DB&) = delete;

    virtual ~DB();
}; // DB

} // namespace engine
} // namespace vecwise
} // namespace zilliz
