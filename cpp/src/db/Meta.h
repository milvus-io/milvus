/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once
#include <cstddef>
#include <ctime>
#include <memory>

#include "MetaTypes.h"
#include "Options.h"
#include "Status.h"

namespace zilliz {
namespace vecwise {
namespace engine {
namespace meta {


class Meta {
public:
    using Ptr = std::shared_ptr<Meta>;

    virtual Status CreateTable(TableSchema& table_schema) = 0;
    virtual Status DescribeTable(TableSchema& table_schema) = 0;
    virtual Status has_group(const std::string& table_id_, bool& has_or_not_) = 0;

    virtual Status add_group_file(TableFileSchema& group_file_info) = 0;
    virtual Status delete_group_partitions(const std::string& table_id,
            const meta::DatesT& dates) = 0;

    virtual Status has_group_file(const std::string& table_id_,
                                  const std::string& file_id_,
                                  bool& has_or_not_) = 0;
    virtual Status get_group_file(const std::string& table_id_,
                                  const std::string& file_id_,
                                  TableFileSchema& group_file_info_) = 0;
    virtual Status update_group_file(TableFileSchema& group_file_) = 0;

    virtual Status get_group_files(const std::string& table_id_,
                                   const int date_delta_,
                                   TableFilesSchema& group_files_info_) = 0;

    virtual Status update_files(TableFilesSchema& files) = 0;

    virtual Status files_to_search(const std::string& table_id,
                                   const DatesT& partition,
                                   DatePartionedTableFilesSchema& files) = 0;

    virtual Status files_to_merge(const std::string& table_id,
            DatePartionedTableFilesSchema& files) = 0;

    virtual Status size(long& result) = 0;

    virtual Status archive_files() = 0;

    virtual Status files_to_index(TableFilesSchema&) = 0;

    virtual Status cleanup() = 0;
    virtual Status cleanup_ttl_files(uint16_t) = 0;

    virtual Status drop_all() = 0;

    virtual Status count(const std::string& table_id, long& result) = 0;

    static DateT GetDate(const std::time_t& t, int day_delta = 0);
    static DateT GetDate();
    static DateT GetDateWithDelta(int day_delta);

}; // MetaData

} // namespace meta
} // namespace engine
} // namespace vecwise
} // namespace zilliz
