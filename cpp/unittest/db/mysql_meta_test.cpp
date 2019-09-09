////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////
#include <gtest/gtest.h>
#include <thread>
#include <easylogging++.h>
#include <stdlib.h>
#include <time.h>

#include "utils.h"
#include "db/meta/MySQLMetaImpl.h"
#include "db/Factories.h"
#include "db/Utils.h"
#include "db/meta/MetaConsts.h"

#include "mysql++/mysql++.h"

#include <iostream>

using namespace zilliz::milvus;
using namespace zilliz::milvus::engine;

TEST_F(MySqlMetaTest, TABLE_TEST) {
    auto table_id = "meta_test_table";

    meta::TableSchema table;
    table.table_id_ = table_id;
    auto status = impl_->CreateTable(table);
    ASSERT_TRUE(status.ok());

    auto gid = table.id_;
    table.id_ = -1;
    status = impl_->DescribeTable(table);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(table.id_, gid);
    ASSERT_EQ(table.table_id_, table_id);

    table.table_id_ = "not_found";
    status = impl_->DescribeTable(table);
    ASSERT_TRUE(!status.ok());

    table.table_id_ = table_id;
    status = impl_->CreateTable(table);
    ASSERT_EQ(status.code(), DB_ALREADY_EXIST);

    table.table_id_ = "";
    status = impl_->CreateTable(table);
//    ASSERT_TRUE(status.ok());

    status = impl_->DropAll();
    ASSERT_TRUE(status.ok());
}

TEST_F(MySqlMetaTest, TABLE_FILE_TEST) {
    auto table_id = "meta_test_table";

    meta::TableSchema table;
    table.table_id_ = table_id;
    table.dimension_ = 256;
    auto status = impl_->CreateTable(table);


    meta::TableFileSchema table_file;
    table_file.table_id_ = table.table_id_;
    status = impl_->CreateTableFile(table_file);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(table_file.file_type_, meta::TableFileSchema::NEW);

    meta::DatesT dates;
    dates.push_back(utils::GetDate());
    status = impl_->DropPartitionsByDates(table_file.table_id_, dates);
    ASSERT_TRUE(status.ok());

    uint64_t cnt = 0;
    status = impl_->Count(table_id, cnt);
//    ASSERT_TRUE(status.ok());
//    ASSERT_EQ(cnt, 0UL);

    auto file_id = table_file.file_id_;

    auto new_file_type = meta::TableFileSchema::INDEX;
    table_file.file_type_ = new_file_type;

    status = impl_->UpdateTableFile(table_file);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(table_file.file_type_, new_file_type);

    dates.clear();
    for (auto i=2; i < 10; ++i) {
        dates.push_back(utils::GetDateWithDelta(-1*i));
    }
    status = impl_->DropPartitionsByDates(table_file.table_id_, dates);
    ASSERT_TRUE(status.ok());

    table_file.date_ = utils::GetDateWithDelta(-2);
    status = impl_->UpdateTableFile(table_file);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(table_file.date_, utils::GetDateWithDelta(-2));
    ASSERT_FALSE(table_file.file_type_ == meta::TableFileSchema::TO_DELETE);

    dates.clear();
    dates.push_back(table_file.date_);
    status = impl_->DropPartitionsByDates(table_file.table_id_, dates);
    ASSERT_TRUE(status.ok());

    std::vector<size_t> ids = {table_file.id_};
    meta::TableFilesSchema files;
    status = impl_->GetTableFiles(table_file.table_id_, ids, files);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(files.size(), 1UL);
    ASSERT_TRUE(files[0].file_type_ == meta::TableFileSchema::TO_DELETE);
}

TEST_F(MySqlMetaTest, ARCHIVE_TEST_DAYS) {
    srand(time(0));
    DBMetaOptions options = GetOptions().meta;

    int days_num = rand() % 100;
    std::stringstream ss;
    ss << "days:" << days_num;
    options.archive_conf = ArchiveConf("delete", ss.str());
    int mode = Options::MODE::SINGLE;
    meta::MySQLMetaImpl impl(options, mode);

    auto table_id = "meta_test_table";

    meta::TableSchema table;
    table.table_id_ = table_id;
    auto status = impl.CreateTable(table);

    meta::TableFilesSchema files;
    meta::TableFileSchema table_file;
    table_file.table_id_ = table.table_id_;

    auto cnt = 100;
    long ts = utils::GetMicroSecTimeStamp();
    std::vector<int> days;
    std::vector<size_t> ids;
    for (auto i=0; i<cnt; ++i) {
        status = impl.CreateTableFile(table_file);
        table_file.file_type_ = meta::TableFileSchema::NEW;
        int day = rand() % (days_num*2);
        table_file.created_on_ = ts - day*meta::D_SEC*meta::US_PS - 10000;
        status = impl.UpdateTableFile(table_file);
        files.push_back(table_file);
        days.push_back(day);
        ids.push_back(table_file.id_);
    }

    impl.Archive();
    int i = 0;

    meta::TableFilesSchema files_get;
    status = impl.GetTableFiles(table_file.table_id_, ids, files_get);
    ASSERT_TRUE(status.ok());

    for(auto& file : files_get) {
        if (days[i] < days_num) {
            ASSERT_EQ(file.file_type_, meta::TableFileSchema::NEW);
        } else {
            ASSERT_EQ(file.file_type_, meta::TableFileSchema::TO_DELETE);
        }
        i++;
    }

    std::vector<int> file_types = {
        (int) meta::TableFileSchema::NEW,
    };
    std::vector<std::string> file_ids;
    status = impl.FilesByType(table_id, file_types, file_ids);
    ASSERT_FALSE(file_ids.empty());

    status = impl.UpdateTableFilesToIndex(table_id);
    ASSERT_TRUE(status.ok());

    status = impl.DropAll();
    ASSERT_TRUE(status.ok());
}

TEST_F(MySqlMetaTest, ARCHIVE_TEST_DISK) {
    DBMetaOptions options = GetOptions().meta;

    options.archive_conf = ArchiveConf("delete", "disk:11");
    int mode = Options::MODE::SINGLE;
    auto impl = meta::MySQLMetaImpl(options, mode);
    auto table_id = "meta_test_group";

    meta::TableSchema table;
    table.table_id_ = table_id;
    auto status = impl.CreateTable(table);

    meta::TableSchema table_schema;
    table_schema.table_id_ = "";
    status = impl.CreateTable(table_schema);

    meta::TableFilesSchema files;
    meta::TableFileSchema table_file;
    table_file.table_id_ = table.table_id_;

    auto cnt = 10;
    auto each_size = 2UL;
    std::vector<size_t> ids;
    for (auto i=0; i<cnt; ++i) {
        status = impl.CreateTableFile(table_file);
        table_file.file_type_ = meta::TableFileSchema::NEW;
        table_file.file_size_ = each_size * meta::G;
        status = impl.UpdateTableFile(table_file);
        files.push_back(table_file);
        ids.push_back(table_file.id_);
    }

    impl.Archive();
    int i = 0;

    meta::TableFilesSchema files_get;
    status = impl.GetTableFiles(table_file.table_id_, ids, files_get);
    ASSERT_TRUE(status.ok());

    for(auto& file : files_get) {
        if (i < 5) {
            ASSERT_TRUE(file.file_type_ == meta::TableFileSchema::TO_DELETE);
        } else {
            ASSERT_EQ(file.file_type_, meta::TableFileSchema::NEW);
        }
        ++i;
    }

    status = impl.DropAll();
    ASSERT_TRUE(status.ok());
}

TEST_F(MySqlMetaTest, TABLE_FILES_TEST) {
    auto table_id = "meta_test_group";

    meta::TableSchema table;
    table.table_id_ = table_id;
    auto status = impl_->CreateTable(table);

    uint64_t new_merge_files_cnt = 1;
    uint64_t new_index_files_cnt = 2;
    uint64_t backup_files_cnt = 3;
    uint64_t new_files_cnt = 4;
    uint64_t raw_files_cnt = 5;
    uint64_t to_index_files_cnt = 6;
    uint64_t index_files_cnt = 7;

    meta::TableFileSchema table_file;
    table_file.table_id_ = table.table_id_;

    for (auto i=0; i<new_merge_files_cnt; ++i) {
        status = impl_->CreateTableFile(table_file);
        table_file.file_type_ = meta::TableFileSchema::NEW_MERGE;
        status = impl_->UpdateTableFile(table_file);
    }

    for (auto i=0; i<new_index_files_cnt; ++i) {
        status = impl_->CreateTableFile(table_file);
        table_file.file_type_ = meta::TableFileSchema::NEW_INDEX;
        status = impl_->UpdateTableFile(table_file);
    }

    for (auto i=0; i<backup_files_cnt; ++i) {
        status = impl_->CreateTableFile(table_file);
        table_file.file_type_ = meta::TableFileSchema::BACKUP;
        table_file.row_count_ = 1;
        status = impl_->UpdateTableFile(table_file);
    }

    for (auto i=0; i<new_files_cnt; ++i) {
        status = impl_->CreateTableFile(table_file);
        table_file.file_type_ = meta::TableFileSchema::NEW;
        status = impl_->UpdateTableFile(table_file);
    }

    for (auto i=0; i<raw_files_cnt; ++i) {
        status = impl_->CreateTableFile(table_file);
        table_file.file_type_ = meta::TableFileSchema::RAW;
        table_file.row_count_ = 1;
        status = impl_->UpdateTableFile(table_file);
    }

    for (auto i=0; i<to_index_files_cnt; ++i) {
        status = impl_->CreateTableFile(table_file);
        table_file.file_type_ = meta::TableFileSchema::TO_INDEX;
        table_file.row_count_ = 1;
        status = impl_->UpdateTableFile(table_file);
    }

    for (auto i=0; i<index_files_cnt; ++i) {
        status = impl_->CreateTableFile(table_file);
        table_file.file_type_ = meta::TableFileSchema::INDEX;
        table_file.row_count_ = 1;
        status = impl_->UpdateTableFile(table_file);
    }

    uint64_t total_row_count = 0;
    status = impl_->Count(table_id, total_row_count);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(total_row_count, raw_files_cnt+to_index_files_cnt+index_files_cnt);

    meta::TableFilesSchema files;
    status = impl_->FilesToIndex(files);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(files.size(), to_index_files_cnt);

    meta::DatePartionedTableFilesSchema dated_files;
    status = impl_->FilesToMerge(table.table_id_, dated_files);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(dated_files[table_file.date_].size(), raw_files_cnt);

    status = impl_->FilesToIndex(files);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(files.size(), to_index_files_cnt);

    meta::DatesT dates = {table_file.date_};
    std::vector<size_t> ids;
    status = impl_->FilesToSearch(table_id, ids, dates, dated_files);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(dated_files[table_file.date_].size(),
              to_index_files_cnt+raw_files_cnt+index_files_cnt);

    status = impl_->FilesToSearch(table_id, ids, meta::DatesT(), dated_files);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(dated_files[table_file.date_].size(),
              to_index_files_cnt+raw_files_cnt+index_files_cnt);

    status = impl_->FilesToSearch(table_id, ids, meta::DatesT(), dated_files);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(dated_files[table_file.date_].size(),
              to_index_files_cnt+raw_files_cnt+index_files_cnt);

    ids.push_back(size_t(9999999999));
    status = impl_->FilesToSearch(table_id, ids, dates, dated_files);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(dated_files[table_file.date_].size(),0);

    std::vector<int> file_types;
    std::vector<std::string> file_ids;
    status = impl_->FilesByType(table.table_id_, file_types, file_ids);
    ASSERT_TRUE(file_ids.empty());
    ASSERT_FALSE(status.ok());

    file_types = {
            meta::TableFileSchema::NEW,
            meta::TableFileSchema::NEW_MERGE,
            meta::TableFileSchema::NEW_INDEX,
            meta::TableFileSchema::TO_INDEX,
            meta::TableFileSchema::INDEX,
            meta::TableFileSchema::RAW,
            meta::TableFileSchema::BACKUP,
    };
    status = impl_->FilesByType(table.table_id_, file_types, file_ids);
    ASSERT_TRUE(status.ok());
    uint64_t total_cnt = new_index_files_cnt + new_merge_files_cnt +
            backup_files_cnt + new_files_cnt + raw_files_cnt +
            to_index_files_cnt + index_files_cnt;
    ASSERT_EQ(file_ids.size(), total_cnt);

    status = impl_->DeleteTableFiles(table_id);
    ASSERT_TRUE(status.ok());

    status = impl_->DeleteTable(table_id);
    ASSERT_TRUE(status.ok());

    status = impl_->CleanUpFilesWithTTL(1UL);
    ASSERT_TRUE(status.ok());
}

TEST_F(MySqlMetaTest, INDEX_TEST) {
    auto table_id = "index_test";

    meta::TableSchema table;
    table.table_id_ = table_id;
    auto status = impl_->CreateTable(table);

    TableIndex index;
    index.metric_type_ = 2;
    index.nlist_ = 1234;
    index.engine_type_ = 3;
    status = impl_->UpdateTableIndex(table_id, index);
    ASSERT_TRUE(status.ok());

    int64_t flag = 65536;
    status = impl_->UpdateTableFlag(table_id, flag);
    ASSERT_TRUE(status.ok());

    engine::meta::TableSchema table_info;
    table_info.table_id_ = table_id;
    status = impl_->DescribeTable(table_info);
    ASSERT_EQ(table_info.flag_, flag);

    TableIndex index_out;
    status = impl_->DescribeTableIndex(table_id, index_out);
    ASSERT_EQ(index_out.metric_type_, index.metric_type_);
    ASSERT_EQ(index_out.nlist_, index.nlist_);
    ASSERT_EQ(index_out.engine_type_, index.engine_type_);

    status = impl_->DropTableIndex(table_id);
    ASSERT_TRUE(status.ok());
    status = impl_->DescribeTableIndex(table_id, index_out);
    ASSERT_NE(index_out.metric_type_, index.metric_type_);
    ASSERT_NE(index_out.nlist_, index.nlist_);
    ASSERT_NE(index_out.engine_type_, index.engine_type_);

    status = impl_->UpdateTableFilesToIndex(table_id);
    ASSERT_TRUE(status.ok());
}
