////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////
#pragma once

#include "Meta.h"
#include "Options.h"

namespace zilliz {
namespace vecwise {
namespace engine {
namespace meta {

class LocalMetaImpl : public Meta {
public:
    const size_t INDEX_TRIGGER_SIZE = 1024*1024*500;
    LocalMetaImpl(const DBMetaOptions& options_);

    virtual Status add_group(TableSchema& group_info_) override;
    virtual Status get_group(TableSchema& group_info_) override;
    virtual Status has_group(const std::string& table_id_, bool& has_or_not_) override;

    virtual Status add_group_file(TableFileSchema& group_file_info) override;
    /* virtual Status delete_group_partitions(const std::string& table_id, */
    /*         const meta::DatesT& dates) override; */

    virtual Status has_group_file(const std::string& table_id_,
                                  const std::string& file_id_,
                                  bool& has_or_not_) override;
    virtual Status get_group_file(const std::string& table_id_,
                                  const std::string& file_id_,
                                  TableFileSchema& group_file_info_) override;
    virtual Status update_group_file(TableFileSchema& group_file_) override;

    virtual Status get_group_files(const std::string& table_id_,
                                   const int date_delta_,
                                   TableFilesSchema& group_files_info_) override;

    virtual Status update_files(TableFilesSchema& files) override;

    virtual Status cleanup() override;

    virtual Status files_to_merge(const std::string& table_id,
            DatePartionedTableFilesSchema& files) override;

    virtual Status files_to_index(TableFilesSchema&) override;

    virtual Status archive_files() override;

    virtual Status cleanup_ttl_files(uint16_t seconds) override;

    virtual Status count(const std::string& table_id, long& result) override;

    virtual Status drop_all() override;

    virtual Status size(long& result) override;

private:

    Status GetGroupMetaInfoByPath(const std::string& path, TableSchema& group_info);
    std::string GetGroupMetaPathByGroupPath(const std::string& group_path);
    Status GetGroupMetaInfo(const std::string& table_id, TableSchema& group_info);
    std::string GetNextGroupFileLocationByPartition(const std::string& table_id, DateT& date,
        TableFileSchema::FILE_TYPE file_type);
    std::string GetGroupDatePartitionPath(const std::string& table_id, DateT& date);
    std::string GetGroupPath(const std::string& table_id);
    std::string GetGroupMetaPath(const std::string& table_id);

    Status CreateGroupMeta(const TableSchema& group_schema);
    long GetFileSize(const std::string& filename);

    Status initialize();

    const DBMetaOptions _options;

}; // LocalMetaImpl

} // namespace meta
} // namespace engine
} // namespace vecwise
} // namespace zilliz
