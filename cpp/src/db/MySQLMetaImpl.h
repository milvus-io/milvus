/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "Meta.h"
#include "Options.h"
#include "MySQLConnectionPool.h"

#include "mysql++/mysql++.h"
#include <mutex>

namespace zilliz {
namespace milvus {
namespace engine {
namespace meta {

//    auto StoragePrototype(const std::string& path);
    using namespace mysqlpp;

    class MySQLMetaImpl : public Meta {
    public:
        MySQLMetaImpl(const DBMetaOptions& options_);

        virtual Status CreateTable(TableSchema& table_schema) override;
        virtual Status DescribeTable(TableSchema& group_info_) override;
        virtual Status HasTable(const std::string& table_id, bool& has_or_not) override;
        virtual Status AllTables(std::vector<TableSchema>& table_schema_array) override;

        virtual Status DeleteTable(const std::string& table_id) override;
        virtual Status DeleteTableFiles(const std::string& table_id) override;

        virtual Status CreateTableFile(TableFileSchema& file_schema) override;
        virtual Status DropPartitionsByDates(const std::string& table_id,
                                             const DatesT& dates) override;

        virtual Status GetTableFiles(const std::string& table_id,
                                     const std::vector<size_t>& ids,
                                     TableFilesSchema& table_files) override;

        virtual Status UpdateTableFile(TableFileSchema& file_schema) override;

        virtual Status UpdateTableFiles(TableFilesSchema& files) override;

        virtual Status FilesToSearch(const std::string& table_id,
                                     const DatesT& partition,
                                     DatePartionedTableFilesSchema& files) override;

        virtual Status FilesToMerge(const std::string& table_id,
                                    DatePartionedTableFilesSchema& files) override;

        virtual Status FilesToIndex(TableFilesSchema&) override;

        virtual Status Archive() override;

        virtual Status Size(uint64_t& result) override;

        virtual Status CleanUp() override;

        virtual Status CleanUpFilesWithTTL(uint16_t seconds) override;

        virtual Status DropAll() override;

        virtual Status Count(const std::string& table_id, uint64_t& result) override;

        virtual ~MySQLMetaImpl();

    private:
        Status NextFileId(std::string& file_id);
        Status NextTableId(std::string& table_id);
        Status DiscardFiles(long long to_discard_size);
        std::string GetTablePath(const std::string& table_id);
        std::string GetTableDatePartitionPath(const std::string& table_id, DateT& date);
        void GetTableFilePath(TableFileSchema& group_file);
        Status Initialize();

        const DBMetaOptions options_;

        std::shared_ptr<MySQLConnectionPool> mySQLConnectionPool_;
        bool safe_grab = false;

//        std::mutex connectionMutex_;
    }; // DBMetaImpl

} // namespace meta
} // namespace engine
} // namespace milvus
} // namespace zilliz
