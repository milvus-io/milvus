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
#include "db/DBMetaImpl.h"
#include "db/Factories.h"
#include "db/Utils.h"
#include "db/MetaConsts.h"

using namespace zilliz::milvus::engine;

TEST_F(MetaTest, GROUP_TEST) {
    auto table_id = "meta_test_group";

    meta::TableSchema group;
    group.table_id_ = table_id;
    auto status = impl_->CreateTable(group);
    ASSERT_TRUE(status.ok());

    auto gid = group.id_;
    group.id_ = -1;
    status = impl_->DescribeTable(group);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(group.id_, gid);
    ASSERT_EQ(group.table_id_, table_id);

    group.table_id_ = "not_found";
    status = impl_->DescribeTable(group);
    ASSERT_TRUE(!status.ok());

    group.table_id_ = table_id;
    status = impl_->CreateTable(group);
    ASSERT_TRUE(!status.ok());
}

TEST_F(MetaTest, table_file_TEST) {
    auto table_id = "meta_test_group";

    meta::TableSchema group;
    group.table_id_ = table_id;
    auto status = impl_->CreateTable(group);

    meta::TableFileSchema table_file;
    table_file.table_id_ = group.table_id_;
    status = impl_->CreateTableFile(table_file);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(table_file.file_type_, meta::TableFileSchema::NEW);

    auto file_id = table_file.file_id_;

    auto new_file_type = meta::TableFileSchema::INDEX;
    table_file.file_type_ = new_file_type;

    status = impl_->UpdateTableFile(table_file);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(table_file.file_type_, new_file_type);

    meta::DatesT dates;
    dates.push_back(meta::Meta::GetDate());
    status = impl_->DropPartitionsByDates(table_file.table_id_, dates);
    ASSERT_FALSE(status.ok());

    dates.clear();
    for (auto i=2; i < 10; ++i) {
        dates.push_back(meta::Meta::GetDateWithDelta(-1*i));
    }
    status = impl_->DropPartitionsByDates(table_file.table_id_, dates);
    ASSERT_TRUE(status.ok());

    table_file.date_ = meta::Meta::GetDateWithDelta(-2);
    status = impl_->UpdateTableFile(table_file);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(table_file.date_, meta::Meta::GetDateWithDelta(-2));
    ASSERT_FALSE(table_file.file_type_ == meta::TableFileSchema::TO_DELETE);

    dates.clear();
    dates.push_back(table_file.date_);
    status = impl_->DropPartitionsByDates(table_file.table_id_, dates);
    ASSERT_TRUE(status.ok());
    status = impl_->GetTableFile(table_file);
    ASSERT_TRUE(status.ok());
    ASSERT_TRUE(table_file.file_type_ == meta::TableFileSchema::TO_DELETE);
}

TEST_F(MetaTest, ARCHIVE_TEST_DAYS) {
    srand(time(0));
    DBMetaOptions options;
    options.path = "/tmp/milvus_test";
    int days_num = rand() % 100;
    std::stringstream ss;
    ss << "days:" << days_num;
    options.archive_conf = ArchiveConf("delete", ss.str());

    auto impl = meta::DBMetaImpl(options);
    auto table_id = "meta_test_group";

    meta::TableSchema group;
    group.table_id_ = table_id;
    auto status = impl.CreateTable(group);

    meta::TableFilesSchema files;
    meta::TableFileSchema table_file;
    table_file.table_id_ = group.table_id_;

    auto cnt = 100;
    long ts = utils::GetMicroSecTimeStamp();
    std::vector<int> days;
    for (auto i=0; i<cnt; ++i) {
        status = impl.CreateTableFile(table_file);
        table_file.file_type_ = meta::TableFileSchema::NEW;
        int day = rand() % (days_num*2);
        table_file.created_on_ = ts - day*meta::D_SEC*meta::US_PS - 10000;
        status = impl.UpdateTableFile(table_file);
        files.push_back(table_file);
        days.push_back(day);
    }

    impl.Archive();
    int i = 0;

    for (auto file : files) {
        status = impl.GetTableFile(file);
        ASSERT_TRUE(status.ok());
        if (days[i] < days_num) {
            ASSERT_EQ(file.file_type_, meta::TableFileSchema::NEW);
        } else {
            ASSERT_EQ(file.file_type_, meta::TableFileSchema::TO_DELETE);
        }
        i++;
    }

    impl.DropAll();
}

TEST_F(MetaTest, ARCHIVE_TEST_DISK) {
    DBMetaOptions options;
    options.path = "/tmp/milvus_test";
    options.archive_conf = ArchiveConf("delete", "disk:11");

    auto impl = meta::DBMetaImpl(options);
    auto table_id = "meta_test_group";

    meta::TableSchema group;
    group.table_id_ = table_id;
    auto status = impl.CreateTable(group);

    meta::TableFilesSchema files;
    meta::TableFileSchema table_file;
    table_file.table_id_ = group.table_id_;

    auto cnt = 10;
    auto each_size = 2UL;
    for (auto i=0; i<cnt; ++i) {
        status = impl.CreateTableFile(table_file);
        table_file.file_type_ = meta::TableFileSchema::NEW;
        table_file.size_ = each_size * meta::G;
        status = impl.UpdateTableFile(table_file);
        files.push_back(table_file);
    }

    impl.Archive();
    int i = 0;

    for (auto file : files) {
        status = impl.GetTableFile(file);
        ASSERT_TRUE(status.ok());
        if (i < 5) {
            ASSERT_TRUE(file.file_type_ == meta::TableFileSchema::TO_DELETE);
        } else {
            ASSERT_EQ(file.file_type_, meta::TableFileSchema::NEW);
        }
        ++i;
    }

    impl.DropAll();
}

TEST_F(MetaTest, TABLE_FILES_TEST) {
    auto table_id = "meta_test_group";

    meta::TableSchema group;
    group.table_id_ = table_id;
    auto status = impl_->CreateTable(group);

    int new_files_cnt = 4;
    int raw_files_cnt = 5;
    int to_index_files_cnt = 6;
    int index_files_cnt = 7;

    meta::TableFileSchema table_file;
    table_file.table_id_ = group.table_id_;

    for (auto i=0; i<new_files_cnt; ++i) {
        status = impl_->CreateTableFile(table_file);
        table_file.file_type_ = meta::TableFileSchema::NEW;
        status = impl_->UpdateTableFile(table_file);
    }

    for (auto i=0; i<raw_files_cnt; ++i) {
        status = impl_->CreateTableFile(table_file);
        table_file.file_type_ = meta::TableFileSchema::RAW;
        status = impl_->UpdateTableFile(table_file);
    }

    for (auto i=0; i<to_index_files_cnt; ++i) {
        status = impl_->CreateTableFile(table_file);
        table_file.file_type_ = meta::TableFileSchema::TO_INDEX;
        status = impl_->UpdateTableFile(table_file);
    }

    for (auto i=0; i<index_files_cnt; ++i) {
        status = impl_->CreateTableFile(table_file);
        table_file.file_type_ = meta::TableFileSchema::INDEX;
        status = impl_->UpdateTableFile(table_file);
    }

    meta::TableFilesSchema files;

    status = impl_->FilesToIndex(files);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(files.size(), to_index_files_cnt);

    meta::DatePartionedTableFilesSchema dated_files;
    status = impl_->FilesToMerge(group.table_id_, dated_files);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(dated_files[table_file.date_].size(), raw_files_cnt);

    status = impl_->FilesToIndex(files);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(files.size(), to_index_files_cnt);

    meta::DatesT dates = {table_file.date_};
    status = impl_->FilesToSearch(table_id, dates, dated_files);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(dated_files[table_file.date_].size(),
            to_index_files_cnt+raw_files_cnt+index_files_cnt);
}
