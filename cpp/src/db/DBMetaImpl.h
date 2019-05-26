/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "Meta.h"
#include "Options.h"

namespace zilliz {
namespace vecwise {
namespace engine {
namespace meta {

auto StoragePrototype(const std::string& path);

class DBMetaImpl : public Meta {
public:
    DBMetaImpl(const DBMetaOptions& options_);

    virtual Status add_group(GroupSchema& group_info) override;
    virtual Status get_group(GroupSchema& group_info_) override;
    virtual Status has_group(const std::string& group_id_, bool& has_or_not_) override;

    virtual Status add_group_file(GroupFileSchema& group_file_info) override;
    virtual Status delete_group_partitions(const std::string& group_id,
            const meta::DatesT& dates) override;

    virtual Status has_group_file(const std::string& group_id_,
                                  const std::string& file_id_,
                                  bool& has_or_not_) override;
    virtual Status get_group_file(const std::string& group_id_,
                                  const std::string& file_id_,
                                  GroupFileSchema& group_file_info_) override;
    virtual Status update_group_file(GroupFileSchema& group_file_) override;

    virtual Status get_group_files(const std::string& group_id_,
                                   const int date_delta_,
                                   GroupFilesSchema& group_files_info_) override;

    virtual Status update_files(GroupFilesSchema& files) override;

    virtual Status files_to_merge(const std::string& group_id,
            DatePartionedGroupFilesSchema& files) override;

    virtual Status files_to_search(const std::string& group_id,
                                  const DatesT& partition,
                                  DatePartionedGroupFilesSchema& files) override;

    virtual Status files_to_index(GroupFilesSchema&) override;

    virtual Status archive_files() override;

    virtual Status size(long& result) override;

    virtual Status cleanup() override;

    virtual Status cleanup_ttl_files(uint16_t seconds) override;

    virtual Status drop_all() override;

    virtual Status count(const std::string& group_id, long& result) override;

    virtual ~DBMetaImpl();

private:
    Status NextFileId(std::string& file_id);
    Status NextGroupId(std::string& group_id);
    Status discard_files_of_size(long to_discard_size);
    Status get_group_no_lock(GroupSchema& group_info);
    std::string GetGroupPath(const std::string& group_id);
    std::string GetGroupDatePartitionPath(const std::string& group_id, DateT& date);
    void GetGroupFilePath(GroupFileSchema& group_file);
    Status initialize();

    const DBMetaOptions _options;
}; // DBMetaImpl

} // namespace meta
} // namespace engine
} // namespace vecwise
} // namespace zilliz
