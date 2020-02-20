// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#include "db/Utils.h"
#include "db/meta/MetaConsts.h"
#include "db/meta/MySQLMetaImpl.h"
#include "db/utils.h"

#include <gtest/gtest.h>
#include <mysql++/mysql++.h>
#include <stdlib.h>
#include <time.h>
#include <boost/filesystem/operations.hpp>
#include <iostream>
#include <thread>
#include <fiu-local.h>
#include <fiu-control.h>
#include <src/db/OngoingFileChecker.h>

const char* FAILED_CONNECT_SQL_SERVER = "Failed to connect to meta server(mysql)";
const char* TABLE_ALREADY_EXISTS = "Table already exists and it is in delete state, please wait a second";

TEST_F(MySqlMetaTest, TABLE_TEST) {
    auto table_id = "meta_test_table";
    fiu_init(0);

    milvus::engine::meta::TableSchema table;
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
    ASSERT_EQ(status.code(), milvus::DB_ALREADY_EXIST);

    table.table_id_ = "";
    status = impl_->CreateTable(table);
    //    ASSERT_TRUE(status.ok());

    table.table_id_ = table_id;
    FIU_ENABLE_FIU("MySQLMetaImpl.CreateTable.null_connection");
    auto stat = impl_->CreateTable(table);
    ASSERT_FALSE(stat.ok());
    ASSERT_EQ(stat.message(), FAILED_CONNECT_SQL_SERVER);
    fiu_disable("MySQLMetaImpl.CreateTable.null_connection");

    FIU_ENABLE_FIU("MySQLMetaImpl.CreateTable.throw_exception");
    stat = impl_->CreateTable(table);
    ASSERT_FALSE(stat.ok());
    fiu_disable("MySQLMetaImpl.CreateTable.throw_exception");

    //ensure table exists
    stat = impl_->CreateTable(table);
    FIU_ENABLE_FIU("MySQLMetaImpl.CreateTableTable.schema_TO_DELETE");
    stat = impl_->CreateTable(table);
    ASSERT_FALSE(stat.ok());
    ASSERT_EQ(stat.message(), TABLE_ALREADY_EXISTS);
    fiu_disable("MySQLMetaImpl.CreateTableTable.schema_TO_DELETE");

    FIU_ENABLE_FIU("MySQLMetaImpl.DescribeTable.null_connection");
    stat = impl_->DescribeTable(table);
    ASSERT_FALSE(stat.ok());
    fiu_disable("MySQLMetaImpl.DescribeTable.null_connection");

    FIU_ENABLE_FIU("MySQLMetaImpl.DescribeTable.throw_exception");
    stat = impl_->DescribeTable(table);
    ASSERT_FALSE(stat.ok());
    fiu_disable("MySQLMetaImpl.DescribeTable.throw_exception");

    bool has_table = false;
    stat = impl_->HasTable(table_id, has_table);
    ASSERT_TRUE(stat.ok());
    ASSERT_TRUE(has_table);

    has_table = false;
    FIU_ENABLE_FIU("MySQLMetaImpl.HasTable.null_connection");
    stat = impl_->HasTable(table_id, has_table);
    ASSERT_FALSE(stat.ok());
    ASSERT_FALSE(has_table);
    fiu_disable("MySQLMetaImpl.HasTable.null_connection");

    FIU_ENABLE_FIU("MySQLMetaImpl.HasTable.throw_exception");
    stat = impl_->HasTable(table_id, has_table);
    ASSERT_FALSE(stat.ok());
    ASSERT_FALSE(has_table);
    fiu_disable("MySQLMetaImpl.HasTable.throw_exception");

    FIU_ENABLE_FIU("MySQLMetaImpl.DropTable.CLUSTER_WRITABLE_MODE");
    stat = impl_->DropTable(table_id);
    fiu_disable("MySQLMetaImpl.DropTable.CLUSTER_WRITABLE_MODE");

    FIU_ENABLE_FIU("MySQLMetaImpl.DropAll.null_connection");
    status = impl_->DropAll();
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.DropAll.null_connection");

    FIU_ENABLE_FIU("MySQLMetaImpl.DropAll.throw_exception");
    status = impl_->DropAll();
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.DropAll.throw_exception");

    status = impl_->DropAll();
    ASSERT_TRUE(status.ok());
}

TEST_F(MySqlMetaTest, TABLE_FILE_TEST) {
    auto table_id = "meta_test_table";
    fiu_init(0);

    uint64_t size = 0;
    auto status = impl_->Size(size);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(size, 0);

    milvus::engine::meta::TableSchema table;
    table.table_id_ = table_id;
    table.dimension_ = 256;
    status = impl_->CreateTable(table);

    //CreateTableFile
    milvus::engine::meta::TableFileSchema table_file;
    table_file.table_id_ = table.table_id_;
    status = impl_->CreateTableFile(table_file);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(table_file.file_type_, milvus::engine::meta::TableFileSchema::NEW);

    FIU_ENABLE_FIU("MySQLMetaImpl.CreateTableFiles.null_connection");
    status = impl_->CreateTableFile(table_file);
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.CreateTableFiles.null_connection");

    FIU_ENABLE_FIU("MySQLMetaImpl.CreateTableFiles.throw_exception");
    status = impl_->CreateTableFile(table_file);
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.CreateTableFiles.throw_exception");

    FIU_ENABLE_FIU("MySQLMetaImpl.DescribeTable.throw_exception");
    status = impl_->CreateTableFile(table_file);
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.DescribeTable.throw_exception");

    //DropDataByDate
    milvus::engine::meta::DatesT dates;
    dates.clear();
    status = impl_->DropDataByDate(table_file.table_id_, dates);
    ASSERT_TRUE(status.ok());

    dates.push_back(milvus::engine::utils::GetDate());
    status = impl_->DropDataByDate("notexist", dates);
    ASSERT_EQ(status.code(), milvus::DB_NOT_FOUND);

    FIU_ENABLE_FIU("MySQLMetaImpl.DropDataByDate.null_connection");
    status = impl_->DropDataByDate(table_file.table_id_, dates);
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.DropDataByDate.null_connection");

    FIU_ENABLE_FIU("MySQLMetaImpl.DropDataByDate.throw_exception");
    status = impl_->DropDataByDate(table_file.table_id_, dates);
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.DropDataByDate.throw_exception");

    status = impl_->DropDataByDate(table_file.table_id_, dates);
    ASSERT_TRUE(status.ok());

    //Count
    uint64_t cnt = 0;
    status = impl_->Count(table_id, cnt);
    //    ASSERT_TRUE(status.ok());
    //    ASSERT_EQ(cnt, 0UL);

    FIU_ENABLE_FIU("MySQLMetaImpl.DescribeTable.throw_exception");
    status = impl_->Count(table_id, cnt);
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.DescribeTable.throw_exception");

    FIU_ENABLE_FIU("MySQLMetaImpl.Count.null_connection");
    status = impl_->Count(table_id, cnt);
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.Count.null_connection");

    FIU_ENABLE_FIU("MySQLMetaImpl.Count.throw_exception");
    status = impl_->Count(table_id, cnt);
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.Count.throw_exception");
    auto file_id = table_file.file_id_;

    auto new_file_type = milvus::engine::meta::TableFileSchema::INDEX;
    table_file.file_type_ = new_file_type;

    //UpdateTableFile
    FIU_ENABLE_FIU("MySQLMetaImpl.UpdateTableFile.null_connection");
    status = impl_->UpdateTableFile(table_file);
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.UpdateTableFile.null_connection");

    FIU_ENABLE_FIU("MySQLMetaImpl.UpdateTableFile.throw_exception");
    status = impl_->UpdateTableFile(table_file);
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.UpdateTableFile.throw_exception");

    status = impl_->UpdateTableFile(table_file);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(table_file.file_type_, new_file_type);

    auto no_table_file = table_file;
    no_table_file.table_id_ = "notexist";
    status = impl_->UpdateTableFile(no_table_file);
    ASSERT_TRUE(status.ok());

    FIU_ENABLE_FIU("MySQLMetaImpl.CleanUpShadowFiles.null_connection");
    status = impl_->CleanUpShadowFiles();
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.CleanUpShadowFiles.null_connection");

    FIU_ENABLE_FIU("MySQLMetaImpl.CleanUpShadowFiles.throw_exception");
    status = impl_->CleanUpShadowFiles();
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.CleanUpShadowFiles.throw_exception");

    status = impl_->CleanUpShadowFiles();
    ASSERT_TRUE(status.ok());

    milvus::engine::meta::TableFilesSchema files_schema;
    FIU_ENABLE_FIU("MySQLMetaImpl.UpdateTableFiles.null_connection");
    status = impl_->UpdateTableFiles(files_schema);
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.UpdateTableFiles.null_connection");

    FIU_ENABLE_FIU("MySQLMetaImpl.UpdateTableFiles.throw_exception");
    status = impl_->UpdateTableFiles(files_schema);
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.UpdateTableFiles.throw_exception");

    status = impl_->UpdateTableFiles(files_schema);
    ASSERT_TRUE(status.ok());

    dates.clear();
    for (auto i = 2; i < 10; ++i) {
        dates.push_back(milvus::engine::utils::GetDateWithDelta(-1 * i));
    }
    status = impl_->DropDataByDate(table_file.table_id_, dates);
    ASSERT_TRUE(status.ok());

    table_file.date_ = milvus::engine::utils::GetDateWithDelta(-2);
    status = impl_->UpdateTableFile(table_file);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(table_file.date_, milvus::engine::utils::GetDateWithDelta(-2));
    ASSERT_FALSE(table_file.file_type_ == milvus::engine::meta::TableFileSchema::TO_DELETE);

    dates.clear();
    dates.push_back(table_file.date_);
    status = impl_->DropDataByDate(table_file.table_id_, dates);
    ASSERT_TRUE(status.ok());

    std::vector<size_t> ids = {table_file.id_};
    milvus::engine::meta::TableFilesSchema files;
    status = impl_->GetTableFiles(table_file.table_id_, ids, files);
    ASSERT_EQ(files.size(), 0UL);

    FIU_ENABLE_FIU("MySQLMetaImpl.GetTableFiles.null_connection");
    status = impl_->GetTableFiles(table_file.table_id_, ids, files);
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.GetTableFiles.null_connection");

    FIU_ENABLE_FIU("MySQLMetaImpl.GetTableFiles.throw_exception");
    status = impl_->GetTableFiles(table_file.table_id_, ids, files);
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.GetTableFiles.throw_exception");

    ids.clear();
    status = impl_->GetTableFiles(table_file.table_id_, ids, files);
    ASSERT_TRUE(status.ok());

    sleep(1);
    std::vector<int> files_to_delete;
    files_to_delete.push_back(milvus::engine::meta::TableFileSchema::TO_DELETE);
    status = impl_->FilesByType(table_id, files_to_delete, files_schema);
    ASSERT_TRUE(status.ok());

    table_file.table_id_ = table_id;
    table_file.file_type_ = milvus::engine::meta::TableFileSchema::TO_DELETE;
    milvus::engine::OngoingFileChecker filter;
    table_file.file_id_ = files_schema.front().file_id_;
    filter.MarkOngoingFile(table_file);
    status = impl_->CleanUpFilesWithTTL(1UL, &filter);
    ASSERT_TRUE(status.ok());

    status = impl_->DropTable(table_file.table_id_);
    ASSERT_TRUE(status.ok());
    status = impl_->UpdateTableFile(table_file);
    ASSERT_TRUE(status.ok());
}

TEST_F(MySqlMetaTest, ARCHIVE_TEST_DAYS) {
    fiu_init(0);

    srand(time(0));
    milvus::engine::DBMetaOptions options = GetOptions().meta_;

    unsigned int seed = 1;
    int days_num = rand_r(&seed) % 100;
    std::stringstream ss;
    ss << "days:" << days_num;
    options.archive_conf_ = milvus::engine::ArchiveConf("delete", ss.str());
    int mode = milvus::engine::DBOptions::MODE::SINGLE;
    milvus::engine::meta::MySQLMetaImpl impl(options, mode);

    auto table_id = "meta_test_table";

    milvus::engine::meta::TableSchema table;
    table.table_id_ = table_id;
    auto status = impl.CreateTable(table);

    milvus::engine::meta::TableFilesSchema files;
    milvus::engine::meta::TableFileSchema table_file;
    table_file.table_id_ = table.table_id_;

    auto cnt = 100;
    int64_t ts = milvus::engine::utils::GetMicroSecTimeStamp();
    std::vector<int> days;
    std::vector<size_t> ids;
    for (auto i = 0; i < cnt; ++i) {
        status = impl.CreateTableFile(table_file);
        table_file.file_type_ = milvus::engine::meta::TableFileSchema::NEW;
        int day = rand_r(&seed) % (days_num * 2);
        table_file.created_on_ = ts - day * milvus::engine::meta::DAY * milvus::engine::meta::US_PS - 10000;
        status = impl.UpdateTableFile(table_file);
        files.push_back(table_file);
        days.push_back(day);
        ids.push_back(table_file.id_);
    }

    FIU_ENABLE_FIU("MySQLMetaImpl.Archive.null_connection");
    status = impl.Archive();
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.Archive.null_connection");

    FIU_ENABLE_FIU("MySQLMetaImpl.Archive.throw_exception");
    status = impl.Archive();
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.Archive.throw_exception");

    impl.Archive();
    int i = 0;

    milvus::engine::meta::TableFilesSchema files_get;
    status = impl.GetTableFiles(table_file.table_id_, ids, files_get);
    ASSERT_TRUE(status.ok());

    for (auto& file : files_get) {
        if (days[i] < days_num) {
            ASSERT_EQ(file.file_type_, milvus::engine::meta::TableFileSchema::NEW);
        }
        i++;
    }

    std::vector<int> file_types = {
        (int)milvus::engine::meta::TableFileSchema::NEW,
    };
    milvus::engine::meta::TableFilesSchema table_files;
    status = impl.FilesByType(table_id, file_types, table_files);
    ASSERT_FALSE(table_files.empty());

    FIU_ENABLE_FIU("MySQLMetaImpl.FilesByType.null_connection");
    table_files.clear();
    status = impl.FilesByType(table_id, file_types, table_files);
    ASSERT_FALSE(status.ok());
    ASSERT_TRUE(table_files.empty());
    fiu_disable("MySQLMetaImpl.FilesByType.null_connection");

    FIU_ENABLE_FIU("MySQLMetaImpl.FilesByType.throw_exception");
    status = impl.FilesByType(table_id, file_types, table_files);
    ASSERT_FALSE(status.ok());
    ASSERT_TRUE(table_files.empty());
    fiu_disable("MySQLMetaImpl.FilesByType.throw_exception");

    status = impl.UpdateTableFilesToIndex(table_id);
    ASSERT_TRUE(status.ok());

    FIU_ENABLE_FIU("MySQLMetaImpl.UpdateTableFilesToIndex.null_connection");
    status = impl.UpdateTableFilesToIndex(table_id);
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.UpdateTableFilesToIndex.null_connection");

    FIU_ENABLE_FIU("MySQLMetaImpl.UpdateTableFilesToIndex.throw_exception");
    status = impl.UpdateTableFilesToIndex(table_id);
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.UpdateTableFilesToIndex.throw_exception");

    status = impl.DropAll();
    ASSERT_TRUE(status.ok());
}

TEST_F(MySqlMetaTest, ARCHIVE_TEST_DISK) {
    fiu_init(0);
    milvus::engine::DBMetaOptions options = GetOptions().meta_;

    options.archive_conf_ = milvus::engine::ArchiveConf("delete", "disk:11");
    int mode = milvus::engine::DBOptions::MODE::SINGLE;
    milvus::engine::meta::MySQLMetaImpl impl(options, mode);
    auto table_id = "meta_test_group";

    milvus::engine::meta::TableSchema table;
    table.table_id_ = table_id;
    auto status = impl.CreateTable(table);

    milvus::engine::meta::TableSchema table_schema;
    table_schema.table_id_ = "";
    status = impl.CreateTable(table_schema);

    milvus::engine::meta::TableFilesSchema files;
    milvus::engine::meta::TableFileSchema table_file;
    table_file.table_id_ = table.table_id_;

    auto cnt = 10;
    auto each_size = 2UL;
    std::vector<size_t> ids;
    for (auto i = 0; i < cnt; ++i) {
        status = impl.CreateTableFile(table_file);
        table_file.file_type_ = milvus::engine::meta::TableFileSchema::NEW;
        table_file.file_size_ = each_size * milvus::engine::G;
        status = impl.UpdateTableFile(table_file);
        files.push_back(table_file);
        ids.push_back(table_file.id_);
    }

    FIU_ENABLE_FIU("MySQLMetaImpl.DiscardFiles.null_connection");
    impl.Archive();
    fiu_disable("MySQLMetaImpl.DiscardFiles.null_connection");

    FIU_ENABLE_FIU("MySQLMetaImpl.DiscardFiles.throw_exception");
    impl.Archive();
    fiu_disable("MySQLMetaImpl.DiscardFiles.throw_exception");

    impl.Archive();
    int i = 0;

    milvus::engine::meta::TableFilesSchema files_get;
    status = impl.GetTableFiles(table_file.table_id_, ids, files_get);
    ASSERT_TRUE(status.ok());

    for (auto& file : files_get) {
        if (i >= 5) {
            ASSERT_EQ(file.file_type_, milvus::engine::meta::TableFileSchema::NEW);
        }
        ++i;
    }

    status = impl.DropAll();
    ASSERT_TRUE(status.ok());
}

TEST_F(MySqlMetaTest, INVALID_INITILIZE_TEST) {
    fiu_init(0);
    auto table_id = "meta_test_group";
    milvus::engine::meta::TableSchema table;
    table.table_id_ = table_id;
    milvus::engine::DBMetaOptions meta = GetOptions().meta_;
    {
        FIU_ENABLE_FIU("MySQLMetaImpl.Initialize.fail_create_directory");
        //delete directory created by SetUp
        boost::filesystem::remove_all(meta.path_);
        ASSERT_ANY_THROW(milvus::engine::meta::MySQLMetaImpl impl(meta, GetOptions().mode_));
        fiu_disable("MySQLMetaImpl.Initialize.fail_create_directory");
    }
    {
        meta.backend_uri_ = "null";
        ASSERT_ANY_THROW(milvus::engine::meta::MySQLMetaImpl impl(meta, GetOptions().mode_));
    }
    {
        meta.backend_uri_ = "notmysql://root:123456@127.0.0.1:3306/test";
        ASSERT_ANY_THROW(milvus::engine::meta::MySQLMetaImpl impl(meta, GetOptions().mode_));
    }
    {
        FIU_ENABLE_FIU("MySQLMetaImpl.Initialize.is_thread_aware");
        ASSERT_ANY_THROW(milvus::engine::meta::MySQLMetaImpl impl(GetOptions().meta_, GetOptions().mode_));
        fiu_disable("MySQLMetaImpl.Initialize.is_thread_aware");
    }
    {
        FIU_ENABLE_FIU("MySQLMetaImpl.Initialize.fail_create_table_scheme");
        ASSERT_ANY_THROW(milvus::engine::meta::MySQLMetaImpl impl(GetOptions().meta_, GetOptions().mode_));
        fiu_disable("MySQLMetaImpl.Initialize.fail_create_table_scheme");
    }
    {
        FIU_ENABLE_FIU("MySQLMetaImpl.Initialize.fail_create_table_files");
        ASSERT_ANY_THROW(milvus::engine::meta::MySQLMetaImpl impl(GetOptions().meta_, GetOptions().mode_));
        fiu_disable("MySQLMetaImpl.Initialize.fail_create_table_files");
    }
    {
        FIU_ENABLE_FIU("MySQLConnectionPool.create.throw_exception");
        ASSERT_ANY_THROW(milvus::engine::meta::MySQLMetaImpl impl(GetOptions().meta_, GetOptions().mode_));
        fiu_disable("MySQLConnectionPool.create.throw_exception");
    }
    {
        FIU_ENABLE_FIU("MySQLMetaImpl.ValidateMetaSchema.fail_validate");
        ASSERT_ANY_THROW(milvus::engine::meta::MySQLMetaImpl impl(GetOptions().meta_, GetOptions().mode_));
        fiu_disable("MySQLMetaImpl.ValidateMetaSchema.fail_validate");
    }
}

TEST_F(MySqlMetaTest, TABLE_FILES_TEST) {
    auto table_id = "meta_test_group";
    fiu_init(0);

    milvus::engine::meta::TableSchema table;
    table.table_id_ = table_id;
    auto status = impl_->CreateTable(table);

    uint64_t new_merge_files_cnt = 1;
    uint64_t new_index_files_cnt = 2;
    uint64_t backup_files_cnt = 3;
    uint64_t new_files_cnt = 4;
    uint64_t raw_files_cnt = 5;
    uint64_t to_index_files_cnt = 6;
    uint64_t index_files_cnt = 7;

    milvus::engine::meta::TableFileSchema table_file;
    table_file.table_id_ = table.table_id_;

    for (auto i = 0; i < new_merge_files_cnt; ++i) {
        status = impl_->CreateTableFile(table_file);
        table_file.file_type_ = milvus::engine::meta::TableFileSchema::NEW_MERGE;
        status = impl_->UpdateTableFile(table_file);
    }

    for (auto i = 0; i < new_index_files_cnt; ++i) {
        status = impl_->CreateTableFile(table_file);
        table_file.file_type_ = milvus::engine::meta::TableFileSchema::NEW_INDEX;
        status = impl_->UpdateTableFile(table_file);
    }

    for (auto i = 0; i < backup_files_cnt; ++i) {
        status = impl_->CreateTableFile(table_file);
        table_file.file_type_ = milvus::engine::meta::TableFileSchema::BACKUP;
        table_file.row_count_ = 1;
        status = impl_->UpdateTableFile(table_file);
    }

    for (auto i = 0; i < new_files_cnt; ++i) {
        status = impl_->CreateTableFile(table_file);
        table_file.file_type_ = milvus::engine::meta::TableFileSchema::NEW;
        status = impl_->UpdateTableFile(table_file);
    }

    for (auto i = 0; i < raw_files_cnt; ++i) {
        status = impl_->CreateTableFile(table_file);
        table_file.file_type_ = milvus::engine::meta::TableFileSchema::RAW;
        table_file.row_count_ = 1;
        status = impl_->UpdateTableFile(table_file);
    }

    for (auto i = 0; i < to_index_files_cnt; ++i) {
        status = impl_->CreateTableFile(table_file);
        table_file.file_type_ = milvus::engine::meta::TableFileSchema::TO_INDEX;
        table_file.row_count_ = 1;
        status = impl_->UpdateTableFile(table_file);
    }

    for (auto i = 0; i < index_files_cnt; ++i) {
        status = impl_->CreateTableFile(table_file);
        table_file.file_type_ = milvus::engine::meta::TableFileSchema::INDEX;
        table_file.row_count_ = 1;
        status = impl_->UpdateTableFile(table_file);
    }

    uint64_t total_row_count = 0;
    status = impl_->Count(table_id, total_row_count);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(total_row_count, raw_files_cnt + to_index_files_cnt + index_files_cnt);

    milvus::engine::meta::TableFilesSchema files;
    status = impl_->FilesToIndex(files);
    ASSERT_EQ(files.size(), to_index_files_cnt);

    milvus::engine::meta::DatePartionedTableFilesSchema dated_files;
    status = impl_->FilesToMerge(table.table_id_, dated_files);
    ASSERT_EQ(dated_files[table_file.date_].size(), raw_files_cnt);

    FIU_ENABLE_FIU("MySQLMetaImpl.FilesToMerge.null_connection");
    status = impl_->FilesToMerge(table.table_id_, dated_files);
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.FilesToMerge.null_connection");

    FIU_ENABLE_FIU("MySQLMetaImpl.FilesToMerge.throw_exception");
    status = impl_->FilesToMerge(table.table_id_, dated_files);
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.FilesToMerge.throw_exception");

    status = impl_->FilesToMerge("notexist", dated_files);
    ASSERT_EQ(status.code(), milvus::DB_NOT_FOUND);

    table_file.file_type_ = milvus::engine::meta::TableFileSchema::RAW;
    table_file.file_size_ = milvus::engine::ONE_GB + 1;
    status = impl_->UpdateTableFile(table_file);
    ASSERT_TRUE(status.ok());

    {
        //skip large files
        milvus::engine::meta::DatePartionedTableFilesSchema dated_files;
        status = impl_->FilesToMerge(table.table_id_, dated_files);
        ASSERT_EQ(dated_files[table_file.date_].size(), raw_files_cnt);
    }

    status = impl_->FilesToIndex(files);
    ASSERT_EQ(files.size(), to_index_files_cnt);

    FIU_ENABLE_FIU("MySQLMetaImpl.DescribeTable.throw_exception");
    status = impl_->FilesToIndex(files);
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.DescribeTable.throw_exception");

    FIU_ENABLE_FIU("MySQLMetaImpl.FilesToIndex.null_connection");
    status = impl_->FilesToIndex(files);
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.FilesToIndex.null_connection");

    FIU_ENABLE_FIU("MySQLMetaImpl.FilesToIndex.throw_exception");
    status = impl_->FilesToIndex(files);
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.FilesToIndex.throw_exception");

    milvus::engine::meta::DatesT dates = {table_file.date_};
    std::vector<size_t> ids;
    status = impl_->FilesToSearch(table_id, ids, dates, dated_files);
    ASSERT_EQ(dated_files[table_file.date_].size(), to_index_files_cnt + raw_files_cnt + index_files_cnt);

    status = impl_->FilesToSearch(table_id, ids, milvus::engine::meta::DatesT(), dated_files);
    ASSERT_EQ(dated_files[table_file.date_].size(), to_index_files_cnt + raw_files_cnt + index_files_cnt);

    status = impl_->FilesToSearch(table_id, ids, milvus::engine::meta::DatesT(), dated_files);
    ASSERT_EQ(dated_files[table_file.date_].size(), to_index_files_cnt + raw_files_cnt + index_files_cnt);

    ids.push_back(size_t(9999999999));
    status = impl_->FilesToSearch(table_id, ids, dates, dated_files);
    ASSERT_EQ(dated_files[table_file.date_].size(), 0);

    FIU_ENABLE_FIU("MySQLMetaImpl.FilesToSearch.null_connection");
    status = impl_->FilesToSearch(table_id, ids, dates, dated_files);
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.FilesToSearch.null_connection");

    FIU_ENABLE_FIU("MySQLMetaImpl.FilesToSearch.throw_exception");
    status = impl_->FilesToSearch(table_id, ids, dates, dated_files);
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.FilesToSearch.throw_exception");

    status = impl_->FilesToSearch("notexist", ids, dates, dated_files);
    ASSERT_EQ(status.code(), milvus::DB_NOT_FOUND);

    std::vector<int> file_types;
    milvus::engine::meta::TableFilesSchema table_files;
    status = impl_->FilesByType(table.table_id_, file_types, table_files);
    ASSERT_TRUE(table_files.empty());
    ASSERT_FALSE(status.ok());

    file_types = {
        milvus::engine::meta::TableFileSchema::NEW, milvus::engine::meta::TableFileSchema::NEW_MERGE,
        milvus::engine::meta::TableFileSchema::NEW_INDEX, milvus::engine::meta::TableFileSchema::TO_INDEX,
        milvus::engine::meta::TableFileSchema::INDEX, milvus::engine::meta::TableFileSchema::RAW,
        milvus::engine::meta::TableFileSchema::BACKUP,
    };
    status = impl_->FilesByType(table.table_id_, file_types, table_files);
    ASSERT_TRUE(status.ok());
    uint64_t total_cnt = new_index_files_cnt + new_merge_files_cnt + backup_files_cnt + new_files_cnt + raw_files_cnt +
                         to_index_files_cnt + index_files_cnt;
    ASSERT_EQ(table_files.size(), total_cnt);

    FIU_ENABLE_FIU("MySQLMetaImpl.DeleteTableFiles.null_connection");
    status = impl_->DeleteTableFiles(table_id);
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.DeleteTableFiles.null_connection");

    FIU_ENABLE_FIU("MySQLMetaImpl.DeleteTableFiles.throw_exception");
    status = impl_->DeleteTableFiles(table_id);
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.DeleteTableFiles.throw_exception");

    status = impl_->DeleteTableFiles(table_id);
    ASSERT_TRUE(status.ok());

    status = impl_->DropTable(table_id);
    ASSERT_TRUE(status.ok());

    status = impl_->CleanUpFilesWithTTL(0UL);
    ASSERT_TRUE(status.ok());

    FIU_ENABLE_FIU("MySQLMetaImpl.CleanUpFilesWithTTL.RomoveToDeleteFiles_NullConnection");
    status = impl_->CleanUpFilesWithTTL(0UL);
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.CleanUpFilesWithTTL.RomoveToDeleteFiles_NullConnection");

    FIU_ENABLE_FIU("MySQLMetaImpl.CleanUpFilesWithTTL.RomoveToDeleteFiles_ThrowException");
    status = impl_->CleanUpFilesWithTTL(0UL);
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.CleanUpFilesWithTTL.RomoveToDeleteFiles_ThrowException");

    FIU_ENABLE_FIU("MySQLMetaImpl.CleanUpFilesWithTTL.RemoveToDeleteTables_NUllConnection");
    status = impl_->CleanUpFilesWithTTL(0UL);
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.CleanUpFilesWithTTL.RemoveToDeleteTables_NUllConnection");

    FIU_ENABLE_FIU("MySQLMetaImpl.CleanUpFilesWithTTL.RemoveToDeleteTables_ThrowException");
    status = impl_->CleanUpFilesWithTTL(0UL);
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.CleanUpFilesWithTTL.RemoveToDeleteTables_ThrowException");

    FIU_ENABLE_FIU("MySQLMetaImpl.CleanUpFilesWithTTL.RemoveDeletedTableFolder_NUllConnection");
    status = impl_->CleanUpFilesWithTTL(0UL);
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.CleanUpFilesWithTTL.RemoveDeletedTableFolder_NUllConnection");

    FIU_ENABLE_FIU("MySQLMetaImpl.CleanUpFilesWithTTL.RemoveDeletedTableFolder_ThrowException");
    status = impl_->CleanUpFilesWithTTL(0UL);
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.CleanUpFilesWithTTL.RemoveDeletedTableFolder_ThrowException");
}

TEST_F(MySqlMetaTest, INDEX_TEST) {
    auto table_id = "index_test";
    fiu_init(0);

    milvus::engine::meta::TableSchema table;
    table.table_id_ = table_id;
    auto status = impl_->CreateTable(table);

    milvus::engine::TableIndex index;
    index.metric_type_ = 2;
    index.nlist_ = 1234;
    index.engine_type_ = 3;
    status = impl_->UpdateTableIndex(table_id, index);
    ASSERT_TRUE(status.ok());

    FIU_ENABLE_FIU("MySQLMetaImpl.UpdateTableIndex.null_connection");
    status = impl_->UpdateTableIndex(table_id, index);
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.UpdateTableIndex.null_connection");

    FIU_ENABLE_FIU("MySQLMetaImpl.UpdateTableIndex.throw_exception");
    status = impl_->UpdateTableIndex(table_id, index);
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.UpdateTableIndex.throw_exception");

    status = impl_->UpdateTableIndex("notexist", index);
    ASSERT_EQ(status.code(), milvus::DB_NOT_FOUND);

    int64_t flag = 65536;
    status = impl_->UpdateTableFlag(table_id, flag);
    ASSERT_TRUE(status.ok());

    FIU_ENABLE_FIU("MySQLMetaImpl.UpdateTableFlag.null_connection");
    status = impl_->UpdateTableFlag(table_id, flag);
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.UpdateTableFlag.null_connection");

    FIU_ENABLE_FIU("MySQLMetaImpl.UpdateTableFlag.throw_exception");
    status = impl_->UpdateTableFlag(table_id, flag);
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.UpdateTableFlag.throw_exception");

    milvus::engine::meta::TableSchema table_info;
    table_info.table_id_ = table_id;
    status = impl_->DescribeTable(table_info);
    ASSERT_EQ(table_info.flag_, flag);

    milvus::engine::TableIndex index_out;
    status = impl_->DescribeTableIndex(table_id, index_out);
    ASSERT_EQ(index_out.metric_type_, index.metric_type_);
    ASSERT_EQ(index_out.nlist_, index.nlist_);
    ASSERT_EQ(index_out.engine_type_, index.engine_type_);

    status = impl_->DropTableIndex(table_id);
    ASSERT_TRUE(status.ok());
    status = impl_->DescribeTableIndex(table_id, index_out);
    ASSERT_EQ(index_out.metric_type_, index.metric_type_);
    ASSERT_NE(index_out.nlist_, index.nlist_);
    ASSERT_NE(index_out.engine_type_, index.engine_type_);

    FIU_ENABLE_FIU("MySQLMetaImpl.DescribeTableIndex.null_connection");
    status = impl_->DescribeTableIndex(table_id, index_out);
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.DescribeTableIndex.null_connection");

    FIU_ENABLE_FIU("MySQLMetaImpl.DescribeTableIndex.throw_exception");
    status = impl_->DescribeTableIndex(table_id, index_out);
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.DescribeTableIndex.throw_exception");

    status = impl_->DescribeTableIndex("notexist", index_out);
    ASSERT_EQ(status.code(), milvus::DB_NOT_FOUND);

    status = impl_->UpdateTableFilesToIndex(table_id);
    ASSERT_TRUE(status.ok());
}

