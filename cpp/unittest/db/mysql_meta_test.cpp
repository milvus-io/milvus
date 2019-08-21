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

using namespace zilliz::milvus::engine;

TEST_F(DISABLED_MySQLTest, TABLE_TEST) {
    DBMetaOptions options;
    try {
        options = getDBMetaOptions();
    } catch(std::exception& ex) {
        ASSERT_TRUE(false);
        return;
    }

    int mode = Options::MODE::SINGLE;
    meta::MySQLMetaImpl impl(options, mode);

    auto table_id = "meta_test_table";

    meta::TableSchema table;
    table.table_id_ = table_id;
    auto status = impl.CreateTable(table);
    ASSERT_TRUE(status.ok());

    auto gid = table.id_;
    table.id_ = -1;
    status = impl.DescribeTable(table);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(table.id_, gid);
    ASSERT_EQ(table.table_id_, table_id);

    table.table_id_ = "not_found";
    status = impl.DescribeTable(table);
    ASSERT_TRUE(!status.ok());

    table.table_id_ = table_id;
    status = impl.CreateTable(table);
    ASSERT_TRUE(status.ok());

    table.table_id_ = "";
    status = impl.CreateTable(table);
//    ASSERT_TRUE(status.ok());

    status = impl.DropAll();
    ASSERT_TRUE(status.ok());
}

TEST_F(DISABLED_MySQLTest, TABLE_FILE_TEST) {
    DBMetaOptions options;
    try {
        options = getDBMetaOptions();
    } catch(std::exception& ex) {
        ASSERT_TRUE(false);
        return;
    }

    int mode = Options::MODE::SINGLE;
    meta::MySQLMetaImpl impl(options, mode);

    auto table_id = "meta_test_table";

    meta::TableSchema table;
    table.table_id_ = table_id;
    table.dimension_ = 256;
    auto status = impl.CreateTable(table);


    meta::TableFileSchema table_file;
    table_file.table_id_ = table.table_id_;
    status = impl.CreateTableFile(table_file);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(table_file.file_type_, meta::TableFileSchema::NEW);

    meta::DatesT dates;
    dates.push_back(meta::Meta::GetDate());
    status = impl.DropPartitionsByDates(table_file.table_id_, dates);
    ASSERT_FALSE(status.ok());

    uint64_t cnt = 0;
    status = impl.Count(table_id, cnt);
//    ASSERT_TRUE(status.ok());
//    ASSERT_EQ(cnt, 0UL);

    auto file_id = table_file.file_id_;

    auto new_file_type = meta::TableFileSchema::INDEX;
    table_file.file_type_ = new_file_type;

    status = impl.UpdateTableFile(table_file);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(table_file.file_type_, new_file_type);

    dates.clear();
    for (auto i=2; i < 10; ++i) {
        dates.push_back(meta::Meta::GetDateWithDelta(-1*i));
    }
    status = impl.DropPartitionsByDates(table_file.table_id_, dates);
    ASSERT_TRUE(status.ok());

    table_file.date_ = meta::Meta::GetDateWithDelta(-2);
    status = impl.UpdateTableFile(table_file);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(table_file.date_, meta::Meta::GetDateWithDelta(-2));
    ASSERT_FALSE(table_file.file_type_ == meta::TableFileSchema::TO_DELETE);

    dates.clear();
    dates.push_back(table_file.date_);
    status = impl.DropPartitionsByDates(table_file.table_id_, dates);
    ASSERT_TRUE(status.ok());

    std::vector<size_t> ids = {table_file.id_};
    meta::TableFilesSchema files;
    status = impl.GetTableFiles(table_file.table_id_, ids, files);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(files.size(), 1UL);
    ASSERT_TRUE(files[0].file_type_ == meta::TableFileSchema::TO_DELETE);

//    status = impl.NextTableId(table_id);

    status = impl.DropAll();
    ASSERT_TRUE(status.ok());
}

TEST_F(DISABLED_MySQLTest, ARCHIVE_TEST_DAYS) {
    srand(time(0));
    DBMetaOptions options;
    try {
        options = getDBMetaOptions();
    } catch(std::exception& ex) {
        ASSERT_TRUE(false);
        return;
    }

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

    bool has;
    status = impl.HasNonIndexFiles(table_id, has);
    ASSERT_TRUE(status.ok());

    status = impl.UpdateTableFilesToIndex(table_id);
    ASSERT_TRUE(status.ok());

    status = impl.DropAll();
    ASSERT_TRUE(status.ok());
}

TEST_F(DISABLED_MySQLTest, ARCHIVE_TEST_DISK) {
    DBMetaOptions options;
    try {
        options = getDBMetaOptions();
    } catch(std::exception& ex) {
        ASSERT_TRUE(false);
        return;
    }

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

TEST_F(DISABLED_MySQLTest, TABLE_FILES_TEST) {
    DBMetaOptions options;
    try {
        options = getDBMetaOptions();
    } catch(std::exception& ex) {
        ASSERT_TRUE(false);
        return;
    }

    int mode = Options::MODE::SINGLE;
    auto impl = meta::MySQLMetaImpl(options, mode);

    auto table_id = "meta_test_group";

    meta::TableSchema table;
    table.table_id_ = table_id;
    auto status = impl.CreateTable(table);

    int new_files_cnt = 4;
    int raw_files_cnt = 5;
    int to_index_files_cnt = 6;
    int index_files_cnt = 7;

    meta::TableFileSchema table_file;
    table_file.table_id_ = table.table_id_;

    for (auto i=0; i<new_files_cnt; ++i) {
        status = impl.CreateTableFile(table_file);
        table_file.file_type_ = meta::TableFileSchema::NEW;
        status = impl.UpdateTableFile(table_file);
    }

    for (auto i=0; i<raw_files_cnt; ++i) {
        status = impl.CreateTableFile(table_file);
        table_file.file_type_ = meta::TableFileSchema::RAW;
        status = impl.UpdateTableFile(table_file);
    }

    for (auto i=0; i<to_index_files_cnt; ++i) {
        status = impl.CreateTableFile(table_file);
        table_file.file_type_ = meta::TableFileSchema::TO_INDEX;
        status = impl.UpdateTableFile(table_file);
    }

    for (auto i=0; i<index_files_cnt; ++i) {
        status = impl.CreateTableFile(table_file);
        table_file.file_type_ = meta::TableFileSchema::INDEX;
        status = impl.UpdateTableFile(table_file);
    }

    meta::TableFilesSchema files;

    status = impl.FilesToIndex(files);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(files.size(), to_index_files_cnt);

    meta::DatePartionedTableFilesSchema dated_files;
    status = impl.FilesToMerge(table.table_id_, dated_files);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(dated_files[table_file.date_].size(), raw_files_cnt);

    status = impl.FilesToIndex(files);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(files.size(), to_index_files_cnt);

    meta::DatesT dates = {table_file.date_};
    status = impl.FilesToSearch(table_id, dates, dated_files);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(dated_files[table_file.date_].size(),
              to_index_files_cnt+raw_files_cnt+index_files_cnt);

    status = impl.FilesToSearch(table_id, meta::DatesT(), dated_files);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(dated_files[table_file.date_].size(),
              to_index_files_cnt+raw_files_cnt+index_files_cnt);

    std::vector<size_t> ids;
    status = impl.FilesToSearch(table_id, ids, meta::DatesT(), dated_files);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(dated_files[table_file.date_].size(),
              to_index_files_cnt+raw_files_cnt+index_files_cnt);

    ids.push_back(size_t(9999999999));
    status = impl.FilesToSearch(table_id, ids, dates, dated_files);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(dated_files[table_file.date_].size(),0);

    status = impl.DropAll();
    ASSERT_TRUE(status.ok());
}
