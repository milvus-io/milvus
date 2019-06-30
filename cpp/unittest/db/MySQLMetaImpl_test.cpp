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
#include "db/MySQLMetaImpl.h"
#include "db/Factories.h"
#include "db/Utils.h"
#include "db/MetaConsts.h"

#include "mysql++/mysql++.h"

#include <iostream>

using namespace zilliz::milvus::engine;

//TEST_F(MySQLTest, InitializeTest) {
//    DBMetaOptions options;
//    //dialect+driver://username:password@host:port/database
//    options.backend_uri = "mysql://root:1234@:/test";
//    meta::MySQLMetaImpl impl(options);
//    auto status = impl.Initialize();
//    std::cout << status.ToString() << std::endl;
//    ASSERT_TRUE(status.ok());
//}

TEST_F(MySQLTest, core) {
    DBMetaOptions options;
//    //dialect+driver://username:password@host:port/database
//    options.backend_uri = "mysql://root:1234@:/test";
//    options.path = "/tmp/vecwise_test";
    try {
        options = getDBMetaOptions();
    } catch(std::exception& ex) {
        ASSERT_TRUE(false);
        return;
    }

    int mode = Options::MODE::SINGLE;
    meta::MySQLMetaImpl impl(options, mode);
//    auto status = impl.Initialize();
//    ASSERT_TRUE(status.ok());

    meta::TableSchema schema1;
    schema1.table_id_ = "test1";
    schema1.dimension_ = 123;

    auto status = impl.CreateTable(schema1);
//    std::cout << status.ToString() << std::endl;
    ASSERT_TRUE(status.ok());

    meta::TableSchema schema2;
    schema2.table_id_ = "test2";
    schema2.dimension_ = 321;
    status = impl.CreateTable(schema2);
//    std::cout << status.ToString() << std::endl;
    ASSERT_TRUE(status.ok());

    status = impl.CreateTable(schema2);
//    std::cout << status.ToString() << std::endl;
//    ASSERT_THROW(impl.CreateTable(schema), mysqlpp::BadQuery);
    ASSERT_TRUE(status.ok());

    status = impl.DeleteTable(schema2.table_id_);
//    std::cout << status.ToString() << std::endl;
    ASSERT_TRUE(status.ok());

    size_t id1 = schema1.id_;
    long created_on1 = schema1.created_on_;
    status = impl.DescribeTable(schema1);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(schema1.id_, id1);
    ASSERT_EQ(schema1.table_id_, "test1");
    ASSERT_EQ(schema1.created_on_, created_on1);
    ASSERT_EQ(schema1.files_cnt_, 0);
    ASSERT_EQ(schema1.engine_type_, 1);
    ASSERT_EQ(schema1.store_raw_data_, false);

    bool check;
    status = impl.HasTable("test1", check);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(check, true);

    std::vector<meta::TableSchema> table_schema_array;
    status = impl.AllTables(table_schema_array);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(table_schema_array.size(), 1);
    meta::TableSchema resultSchema = table_schema_array[0];
    ASSERT_EQ(resultSchema.id_, id1);
    ASSERT_EQ(resultSchema.table_id_, "test1");
    ASSERT_EQ(resultSchema.dimension_, 123);
    ASSERT_EQ(resultSchema.files_cnt_, 0);
    ASSERT_EQ(resultSchema.engine_type_, 1);
    ASSERT_EQ(resultSchema.store_raw_data_, false);

    meta::TableFileSchema tableFileSchema;
    tableFileSchema.table_id_ = "test1";

    status = impl.CreateTableFile(tableFileSchema);
//    std::cout << status.ToString() << std::endl;
    ASSERT_TRUE(status.ok());

    tableFileSchema.file_type_ = meta::TableFileSchema::TO_INDEX;
    status = impl.UpdateTableFile(tableFileSchema);
//    std::cout << status.ToString() << std::endl;
    ASSERT_TRUE(status.ok());

    meta::TableFilesSchema filesToIndex;
    status = impl.FilesToIndex(filesToIndex);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(filesToIndex.size(), 1);
    meta::TableFileSchema fileToIndex = filesToIndex[0];
    ASSERT_EQ(fileToIndex.table_id_, "test1");
    ASSERT_EQ(fileToIndex.dimension_, 123);

//    meta::TableFilesSchema filesToIndex;
//    status = impl.FilesToIndex(filesToIndex);
//    ASSERT_TRUE(status.ok());
//    ASSERT_EQ(filesToIndex.size(), 0);

    meta::DatesT partition;
    partition.push_back(tableFileSchema.date_);
    meta::DatePartionedTableFilesSchema filesToSearch;
    status = impl.FilesToSearch(tableFileSchema.table_id_, partition, filesToSearch);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(filesToSearch.size(), 1);
    ASSERT_EQ(filesToSearch[tableFileSchema.date_].size(), 1);
    meta::TableFileSchema fileToSearch = filesToSearch[tableFileSchema.date_][0];
    ASSERT_EQ(fileToSearch.table_id_, "test1");
    ASSERT_EQ(fileToSearch.dimension_, 123);

    tableFileSchema.file_type_ = meta::TableFileSchema::RAW;
    status = impl.UpdateTableFile(tableFileSchema);
    ASSERT_TRUE(status.ok());

    meta::DatePartionedTableFilesSchema filesToMerge;
    status = impl.FilesToMerge(tableFileSchema.table_id_, filesToMerge);
//    std::cout << status.ToString() << std::endl;
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(filesToMerge.size(), 1);
    ASSERT_EQ(filesToMerge[tableFileSchema.date_].size(), 1);
    meta::TableFileSchema fileToMerge = filesToMerge[tableFileSchema.date_][0];
    ASSERT_EQ(fileToMerge.table_id_, "test1");
    ASSERT_EQ(fileToMerge.dimension_, 123);

    meta::TableFilesSchema resultTableFilesSchema;
    std::vector<size_t> ids;
    ids.push_back(tableFileSchema.id_);
    status = impl.GetTableFiles(tableFileSchema.table_id_, ids, resultTableFilesSchema);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(resultTableFilesSchema.size(), 1);
    meta::TableFileSchema resultTableFileSchema = resultTableFilesSchema[0];
//    ASSERT_EQ(resultTableFileSchema.id_, tableFileSchema.id_);
    ASSERT_EQ(resultTableFileSchema.table_id_, tableFileSchema.table_id_);
    ASSERT_EQ(resultTableFileSchema.file_id_, tableFileSchema.file_id_);
    ASSERT_EQ(resultTableFileSchema.file_type_, tableFileSchema.file_type_);
    ASSERT_EQ(resultTableFileSchema.size_, tableFileSchema.size_);
    ASSERT_EQ(resultTableFileSchema.date_, tableFileSchema.date_);
    ASSERT_EQ(resultTableFileSchema.engine_type_, tableFileSchema.engine_type_);
    ASSERT_EQ(resultTableFileSchema.dimension_, tableFileSchema.dimension_);

    tableFileSchema.size_ = 234;
    meta::TableSchema schema3;
    schema3.table_id_ = "test3";
    schema3.dimension_ = 321;
    status = impl.CreateTable(schema3);
    ASSERT_TRUE(status.ok());
    meta::TableFileSchema tableFileSchema2;
    tableFileSchema2.table_id_ = "test3";
    tableFileSchema2.size_ = 345;
    status = impl.CreateTableFile(tableFileSchema2);
    ASSERT_TRUE(status.ok());
    meta::TableFilesSchema filesToUpdate;
    filesToUpdate.emplace_back(tableFileSchema);
    filesToUpdate.emplace_back(tableFileSchema2);
    status = impl.UpdateTableFile(tableFileSchema);
    ASSERT_TRUE(status.ok());

    uint64_t resultSize;
    status = impl.Size(resultSize);
//    std::cout << status.ToString() << std::endl;
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(resultSize, tableFileSchema.size_ + tableFileSchema2.size_);

    uint64_t countResult;
    status = impl.Count(tableFileSchema.table_id_, countResult);
    ASSERT_TRUE(status.ok());

    status = impl.DropAll();
    ASSERT_TRUE(status.ok());

}

TEST_F(MySQLTest, GROUP_TEST) {
    DBMetaOptions options;
    try {
        options = getDBMetaOptions();
    } catch(std::exception& ex) {
        ASSERT_TRUE(false);
        return;
    }

    int mode = Options::MODE::SINGLE;
    meta::MySQLMetaImpl impl(options, mode);

    auto table_id = "meta_test_group";

    meta::TableSchema group;
    group.table_id_ = table_id;
    auto status = impl.CreateTable(group);
    ASSERT_TRUE(status.ok());

    auto gid = group.id_;
    group.id_ = -1;
    status = impl.DescribeTable(group);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(group.id_, gid);
    ASSERT_EQ(group.table_id_, table_id);

    group.table_id_ = "not_found";
    status = impl.DescribeTable(group);
    ASSERT_TRUE(!status.ok());

    group.table_id_ = table_id;
    status = impl.CreateTable(group);
    ASSERT_TRUE(status.ok());

    group.table_id_ = "";
    status = impl.CreateTable(group);
    ASSERT_TRUE(status.ok());


    status = impl.DropAll();
    ASSERT_TRUE(status.ok());
}

TEST_F(MySQLTest, table_file_TEST) {
    DBMetaOptions options;
    try {
        options = getDBMetaOptions();
    } catch(std::exception& ex) {
        ASSERT_TRUE(false);
        return;
    }

    int mode = Options::MODE::SINGLE;
    meta::MySQLMetaImpl impl(options, mode);

    auto table_id = "meta_test_group";

    meta::TableSchema group;
    group.table_id_ = table_id;
    group.dimension_ = 256;
    auto status = impl.CreateTable(group);

    meta::TableFileSchema table_file;
    table_file.table_id_ = group.table_id_;
    status = impl.CreateTableFile(table_file);
//    std::cout << status.ToString() << std::endl;
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(table_file.file_type_, meta::TableFileSchema::NEW);

    uint64_t cnt = 0;
    status = impl.Count(table_id, cnt);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(cnt, 0UL);

    auto file_id = table_file.file_id_;

    auto new_file_type = meta::TableFileSchema::INDEX;
    table_file.file_type_ = new_file_type;

    status = impl.UpdateTableFile(table_file);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(table_file.file_type_, new_file_type);

    meta::DatesT dates;
    dates.push_back(meta::Meta::GetDate());
    status = impl.DropPartitionsByDates(table_file.table_id_, dates);
    ASSERT_FALSE(status.ok());

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

    status = impl.DropAll();
    ASSERT_TRUE(status.ok());
}

TEST_F(MySQLTest, ARCHIVE_TEST_DAYS) {
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

    status = impl.DropAll();
    ASSERT_TRUE(status.ok());
}

TEST_F(MySQLTest, ARCHIVE_TEST_DISK) {
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

    meta::TableSchema group;
    group.table_id_ = table_id;
    auto status = impl.CreateTable(group);

    meta::TableFilesSchema files;
    meta::TableFileSchema table_file;
    table_file.table_id_ = group.table_id_;

    auto cnt = 10;
    auto each_size = 2UL;
    std::vector<size_t> ids;
    for (auto i=0; i<cnt; ++i) {
        status = impl.CreateTableFile(table_file);
        table_file.file_type_ = meta::TableFileSchema::NEW;
        table_file.size_ = each_size * meta::G;
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

TEST_F(MySQLTest, TABLE_FILES_TEST) {
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

    meta::TableSchema group;
    group.table_id_ = table_id;
    auto status = impl.CreateTable(group);

    int new_files_cnt = 4;
    int raw_files_cnt = 5;
    int to_index_files_cnt = 6;
    int index_files_cnt = 7;

    meta::TableFileSchema table_file;
    table_file.table_id_ = group.table_id_;

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
    status = impl.FilesToMerge(group.table_id_, dated_files);
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

    status = impl.DropAll();
    ASSERT_TRUE(status.ok());
}
