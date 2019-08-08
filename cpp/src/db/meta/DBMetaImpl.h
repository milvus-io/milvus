/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "Meta.h"
#include "Options.h"

#include <mutex>

namespace zilliz {
namespace milvus {
namespace engine {
namespace meta {

auto StoragePrototype(const std::string &path);

class DBMetaImpl : public Meta {
 public:
    explicit DBMetaImpl(const DBMetaOptions &options_);

    Status
    CreateTable(TableSchema &table_schema) override;

    Status
    DescribeTable(TableSchema &group_info_) override;

    Status
    HasTable(const std::string &table_id, bool &has_or_not) override;

    Status
    AllTables(std::vector<TableSchema> &table_schema_array) override;

    Status
    DeleteTable(const std::string &table_id) override;

    Status
    DeleteTableFiles(const std::string &table_id) override;

    Status
    CreateTableFile(TableFileSchema &file_schema) override;

    Status
    DropPartitionsByDates(const std::string &table_id, const DatesT &dates) override;

    Status
    GetTableFiles(const std::string &table_id, const std::vector<size_t> &ids, TableFilesSchema &table_files) override;

    Status
    HasNonIndexFiles(const std::string &table_id, bool &has) override;

    Status
    UpdateTableFilesToIndex(const std::string &table_id) override;

    Status
    UpdateTableFile(TableFileSchema &file_schema) override;

    Status
    UpdateTableFiles(TableFilesSchema &files) override;

    Status
    FilesToSearch(const std::string &table_id, const DatesT &partition, DatePartionedTableFilesSchema &files) override;

    Status FilesToSearch(const std::string &table_id,
                         const std::vector<size_t> &ids,
                         const DatesT &partition,
                         DatePartionedTableFilesSchema &files) override;

    Status
    FilesToMerge(const std::string &table_id, DatePartionedTableFilesSchema &files) override;

    Status
    FilesToIndex(TableFilesSchema &) override;

    Status
    Archive() override;

    Status
    Size(uint64_t &result) override;

    Status
    CleanUp() override;

    Status
    CleanUpFilesWithTTL(uint16_t seconds) override;

    Status
    DropAll() override;

    Status Count(const std::string &table_id, uint64_t &result) override;

    ~DBMetaImpl() override;

 private:
    Status NextFileId(std::string &file_id);
    Status NextTableId(std::string &table_id);
    Status DiscardFiles(long to_discard_size);
    Status Initialize();

    const DBMetaOptions options_;

    std::mutex meta_mutex_;
}; // DBMetaImpl

} // namespace meta
} // namespace engine
} // namespace milvus
} // namespace zilliz
