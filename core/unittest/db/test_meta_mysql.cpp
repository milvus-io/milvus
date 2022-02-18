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

#include <fiu-control.h>
#include <fiu-local.h>
#include <gtest/gtest.h>
#include <mysql++/mysql++.h>
#include <stdlib.h>
#include <time.h>
#include <boost/filesystem/operations.hpp>
#include <iostream>
#include <thread>

const char* FAILED_CONNECT_SQL_SERVER = "Failed to connect to meta server(mysql)";
const char* COLLECTION_ALREADY_EXISTS = "Collection already exists and it is in delete state, please wait a second";

TEST_F(MySqlMetaTest, COLLECTION_TEST) {
    auto collection_id = "meta_test_table";
    fiu_init(0);

    milvus::engine::meta::CollectionSchema collection;
    collection.collection_id_ = collection_id;
    auto status = impl_->CreateCollection(collection);
    ASSERT_TRUE(status.ok());

    auto gid = collection.id_;
    collection.id_ = -1;
    status = impl_->DescribeCollection(collection);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(collection.id_, gid);
    ASSERT_EQ(collection.collection_id_, collection_id);

    collection.collection_id_ = "not_found";
    status = impl_->DescribeCollection(collection);
    ASSERT_TRUE(!status.ok());

    collection.collection_id_ = collection_id;
    status = impl_->CreateCollection(collection);
    ASSERT_EQ(status.code(), milvus::DB_ALREADY_EXIST);

    collection.collection_id_ = "";
    status = impl_->CreateCollection(collection);
    //    ASSERT_TRUE(status.ok());

    collection.collection_id_ = collection_id;
    FIU_ENABLE_FIU("MySQLMetaImpl.CreateCollection.null_connection");
    auto stat = impl_->CreateCollection(collection);
    ASSERT_FALSE(stat.ok());
    ASSERT_EQ(stat.message(), FAILED_CONNECT_SQL_SERVER);
    fiu_disable("MySQLMetaImpl.CreateCollection.null_connection");

    FIU_ENABLE_FIU("MySQLMetaImpl.CreateCollection.throw_exception");
    stat = impl_->CreateCollection(collection);
    ASSERT_FALSE(stat.ok());
    fiu_disable("MySQLMetaImpl.CreateCollection.throw_exception");

    // ensure collection exists
    stat = impl_->CreateCollection(collection);
    FIU_ENABLE_FIU("MySQLMetaImpl.CreateCollection.schema_TO_DELETE");
    stat = impl_->CreateCollection(collection);
    ASSERT_FALSE(stat.ok());
    ASSERT_EQ(stat.message(), COLLECTION_ALREADY_EXISTS);
    fiu_disable("MySQLMetaImpl.CreateCollection.schema_TO_DELETE");

    FIU_ENABLE_FIU("MySQLMetaImpl.DescribeCollection.null_connection");
    stat = impl_->DescribeCollection(collection);
    ASSERT_FALSE(stat.ok());
    fiu_disable("MySQLMetaImpl.DescribeCollection.null_connection");

    FIU_ENABLE_FIU("MySQLMetaImpl.DescribeCollection.throw_exception");
    stat = impl_->DescribeCollection(collection);
    ASSERT_FALSE(stat.ok());
    fiu_disable("MySQLMetaImpl.DescribeCollection.throw_exception");

    bool has_collection = false;
    stat = impl_->HasCollection(collection_id, has_collection);
    ASSERT_TRUE(stat.ok());
    ASSERT_TRUE(has_collection);

    has_collection = false;
    FIU_ENABLE_FIU("MySQLMetaImpl.HasCollection.null_connection");
    stat = impl_->HasCollection(collection_id, has_collection);
    ASSERT_FALSE(stat.ok());
    ASSERT_FALSE(has_collection);
    fiu_disable("MySQLMetaImpl.HasCollection.null_connection");

    FIU_ENABLE_FIU("MySQLMetaImpl.HasCollection.throw_exception");
    stat = impl_->HasCollection(collection_id, has_collection);
    ASSERT_FALSE(stat.ok());
    ASSERT_FALSE(has_collection);
    fiu_disable("MySQLMetaImpl.HasCollection.throw_exception");

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

TEST_F(MySqlMetaTest, COLLECTION_FILE_TEST) {
    auto collection_id = "meta_test_table";
    fiu_init(0);

    uint64_t size = 0;
    auto status = impl_->Size(size);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(size, 0);

    milvus::engine::meta::CollectionSchema collection;
    collection.collection_id_ = collection_id;
    collection.dimension_ = 256;
    status = impl_->CreateCollection(collection);

    // CreateCollectionFile
    milvus::engine::meta::SegmentSchema table_file;
    table_file.collection_id_ = collection.collection_id_;
    status = impl_->CreateCollectionFile(table_file);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(table_file.file_type_, milvus::engine::meta::SegmentSchema::NEW);

    FIU_ENABLE_FIU("MySQLMetaImpl.CreateCollectionFiles.null_connection");
    status = impl_->CreateCollectionFile(table_file);
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.CreateCollectionFiles.null_connection");

    FIU_ENABLE_FIU("MySQLMetaImpl.CreateCollectionFiles.throw_exception");
    status = impl_->CreateCollectionFile(table_file);
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.CreateCollectionFiles.throw_exception");

    FIU_ENABLE_FIU("MySQLMetaImpl.DescribeCollection.throw_exception");
    status = impl_->CreateCollectionFile(table_file);
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.DescribeCollection.throw_exception");

    // Count
    uint64_t cnt = 0;
    status = impl_->Count(collection_id, cnt);
    //    ASSERT_TRUE(status.ok());
    //    ASSERT_EQ(cnt, 0UL);

    FIU_ENABLE_FIU("MySQLMetaImpl.Count.null_connection");
    status = impl_->Count(collection_id, cnt);
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.Count.null_connection");

    FIU_ENABLE_FIU("MySQLMetaImpl.Count.throw_exception");
    status = impl_->Count(collection_id, cnt);
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.Count.throw_exception");
    auto file_id = table_file.file_id_;

    auto new_file_type = milvus::engine::meta::SegmentSchema::INDEX;
    table_file.file_type_ = new_file_type;

    // UpdateCollectionFile
    FIU_ENABLE_FIU("MySQLMetaImpl.UpdateCollectionFile.null_connection");
    status = impl_->UpdateCollectionFile(table_file);
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.UpdateCollectionFile.null_connection");

    FIU_ENABLE_FIU("MySQLMetaImpl.UpdateCollectionFile.throw_exception");
    status = impl_->UpdateCollectionFile(table_file);
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.UpdateCollectionFile.throw_exception");

    status = impl_->UpdateCollectionFile(table_file);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(table_file.file_type_, new_file_type);

    auto no_table_file = table_file;
    no_table_file.collection_id_ = "notexist";
    status = impl_->UpdateCollectionFile(no_table_file);
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

    milvus::engine::meta::SegmentsSchema files_schema;
    FIU_ENABLE_FIU("MySQLMetaImpl.UpdateCollectionFiles.null_connection");
    status = impl_->UpdateCollectionFiles(files_schema);
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.UpdateCollectionFiles.null_connection");

    FIU_ENABLE_FIU("MySQLMetaImpl.UpdateCollectionFiles.throw_exception");
    status = impl_->UpdateCollectionFiles(files_schema);
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.UpdateCollectionFiles.throw_exception");

    status = impl_->UpdateCollectionFiles(files_schema);
    ASSERT_TRUE(status.ok());

    std::vector<size_t> ids = {table_file.id_};
    milvus::engine::meta::FilesHolder files_holder;
    status = impl_->GetCollectionFiles(table_file.collection_id_, ids, files_holder);
    ASSERT_EQ(files_holder.HoldFiles().size(), 0UL);

    FIU_ENABLE_FIU("MySQLMetaImpl.GetCollectionFiles.null_connection");
    status = impl_->GetCollectionFiles(table_file.collection_id_, ids, files_holder);
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.GetCollectionFiles.null_connection");

    FIU_ENABLE_FIU("MySQLMetaImpl.GetCollectionFiles.throw_exception");
    status = impl_->GetCollectionFiles(table_file.collection_id_, ids, files_holder);
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.GetCollectionFiles.throw_exception");

    ids.clear();
    status = impl_->GetCollectionFiles(table_file.collection_id_, ids, files_holder);
    ASSERT_TRUE(status.ok());

    table_file.collection_id_ = collection.collection_id_;
    table_file.file_type_ = milvus::engine::meta::SegmentSchema::RAW;
    status = impl_->CreateCollectionFile(table_file);
    ids = {table_file.id_};
    status = impl_->FilesByID(ids, files_holder);
    ASSERT_EQ(files_holder.HoldFiles().size(), 1UL);

    table_file.collection_id_ = collection.collection_id_;
    table_file.file_type_ = milvus::engine::meta::SegmentSchema::TO_DELETE;
    status = impl_->CreateCollectionFile(table_file);

    files_holder.ReleaseFiles();
    std::vector<int> files_to_delete = {milvus::engine::meta::SegmentSchema::TO_DELETE};
    status = impl_->FilesByType(collection_id, files_to_delete, files_holder);
    ASSERT_TRUE(status.ok());

    table_file.collection_id_ = collection_id;
    table_file.file_type_ = milvus::engine::meta::SegmentSchema::TO_DELETE;
    table_file.file_id_ = files_holder.HoldFiles().front().file_id_;
    status = impl_->CleanUpFilesWithTTL(1UL);
    ASSERT_TRUE(status.ok());

    status = impl_->DropCollections({table_file.collection_id_});
    ASSERT_TRUE(status.ok());
    status = impl_->UpdateCollectionFile(table_file);
    ASSERT_TRUE(status.ok());
}

TEST_F(MySqlMetaTest, COLLECTION_FILE_ROW_COUNT_TEST) {
    auto collection_id = "row_count_test_table";

    milvus::engine::meta::CollectionSchema collection;
    collection.collection_id_ = collection_id;
    collection.dimension_ = 256;
    auto status = impl_->CreateCollection(collection);

    milvus::engine::meta::SegmentSchema table_file;
    table_file.row_count_ = 100;
    table_file.collection_id_ = collection.collection_id_;
    table_file.file_type_ = 1;
    status = impl_->CreateCollectionFile(table_file);

    uint64_t cnt = 0;
    status = impl_->Count(collection_id, cnt);
    ASSERT_EQ(table_file.row_count_, cnt);

    table_file.row_count_ = 99999;
    milvus::engine::meta::SegmentsSchema table_files = {table_file};
    status = impl_->UpdateCollectionFilesRowCount(table_files);
    ASSERT_TRUE(status.ok());

    cnt = 0;
    status = impl_->Count(collection_id, cnt);
    ASSERT_EQ(table_file.row_count_, cnt);

    std::vector<size_t> ids = {table_file.id_};
    milvus::engine::meta::FilesHolder files_holder;
    status = impl_->GetCollectionFiles(collection_id, ids, files_holder);

    milvus::engine::meta::SegmentsSchema& schemas = files_holder.HoldFiles();
    ASSERT_EQ(schemas.size(), 1UL);
    ASSERT_EQ(table_file.row_count_, schemas[0].row_count_);
    ASSERT_EQ(table_file.file_id_, schemas[0].file_id_);
    ASSERT_EQ(table_file.file_type_, schemas[0].file_type_);
    ASSERT_EQ(table_file.segment_id_, schemas[0].segment_id_);
    ASSERT_EQ(table_file.collection_id_, schemas[0].collection_id_);
    ASSERT_EQ(table_file.engine_type_, schemas[0].engine_type_);
    ASSERT_EQ(table_file.dimension_, schemas[0].dimension_);
    ASSERT_EQ(table_file.flush_lsn_, schemas[0].flush_lsn_);
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

    auto collection_id = "meta_test_table";

    milvus::engine::meta::CollectionSchema collection;
    collection.collection_id_ = collection_id;
    auto status = impl.CreateCollection(collection);

    milvus::engine::meta::SegmentsSchema files;
    milvus::engine::meta::SegmentSchema table_file;
    table_file.collection_id_ = collection.collection_id_;

    auto cnt = 100;
    int64_t ts = milvus::engine::utils::GetMicroSecTimeStamp();
    std::vector<int> days;
    std::vector<size_t> ids;
    for (auto i = 0; i < cnt; ++i) {
        status = impl.CreateCollectionFile(table_file);
        table_file.file_type_ = milvus::engine::meta::SegmentSchema::NEW;
        int day = rand_r(&seed) % (days_num * 2);
        table_file.created_on_ = ts - day * milvus::engine::meta::DAY * milvus::engine::meta::US_PS - 10000;
        status = impl.UpdateCollectionFile(table_file);
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

    milvus::engine::meta::FilesHolder files_holder;
    status = impl.GetCollectionFiles(table_file.collection_id_, ids, files_holder);
    ASSERT_TRUE(status.ok());

    for (auto& file : files_holder.HoldFiles()) {
        if (days[i] < days_num) {
            ASSERT_EQ(file.file_type_, milvus::engine::meta::SegmentSchema::NEW);
        }
        i++;
    }

    std::vector<int> file_types = {
        (int)milvus::engine::meta::SegmentSchema::NEW,
    };

    files_holder.ReleaseFiles();
    status = impl.FilesByType(collection_id, file_types, files_holder);
    ASSERT_FALSE(files_holder.HoldFiles().empty());

    FIU_ENABLE_FIU("MySQLMetaImpl.FilesByType.null_connection");
    files_holder.ReleaseFiles();
    status = impl.FilesByType(collection_id, file_types, files_holder);
    ASSERT_FALSE(status.ok());
    ASSERT_TRUE(files_holder.HoldFiles().empty());
    fiu_disable("MySQLMetaImpl.FilesByType.null_connection");

    FIU_ENABLE_FIU("MySQLMetaImpl.FilesByType.throw_exception");
    status = impl.FilesByType(collection_id, file_types, files_holder);
    ASSERT_FALSE(status.ok());
    ASSERT_TRUE(files_holder.HoldFiles().empty());
    fiu_disable("MySQLMetaImpl.FilesByType.throw_exception");

    status = impl.UpdateCollectionFilesToIndex(collection_id);
    ASSERT_TRUE(status.ok());

    FIU_ENABLE_FIU("MySQLMetaImpl.UpdateCollectionFilesToIndex.null_connection");
    status = impl.UpdateCollectionFilesToIndex(collection_id);
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.UpdateCollectionFilesToIndex.null_connection");

    FIU_ENABLE_FIU("MySQLMetaImpl.UpdateCollectionFilesToIndex.throw_exception");
    status = impl.UpdateCollectionFilesToIndex(collection_id);
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.UpdateCollectionFilesToIndex.throw_exception");

    status = impl.DropAll();
    ASSERT_TRUE(status.ok());
}

TEST_F(MySqlMetaTest, ARCHIVE_TEST_DISK) {
    fiu_init(0);
    milvus::engine::DBMetaOptions options = GetOptions().meta_;

    options.archive_conf_ = milvus::engine::ArchiveConf("delete", "disk:11");
    int mode = milvus::engine::DBOptions::MODE::SINGLE;
    milvus::engine::meta::MySQLMetaImpl impl(options, mode);
    auto collection_id = "meta_test_group";

    milvus::engine::meta::CollectionSchema collection;
    collection.collection_id_ = collection_id;
    auto status = impl.CreateCollection(collection);

    milvus::engine::meta::CollectionSchema collection_schema;
    collection_schema.collection_id_ = "";
    status = impl.CreateCollection(collection_schema);

    milvus::engine::meta::SegmentsSchema files;
    milvus::engine::meta::SegmentSchema table_file;
    table_file.collection_id_ = collection.collection_id_;

    auto cnt = 10;
    auto each_size = 2UL;
    std::vector<size_t> ids;
    for (auto i = 0; i < cnt; ++i) {
        status = impl.CreateCollectionFile(table_file);
        table_file.file_type_ = milvus::engine::meta::SegmentSchema::NEW;
        table_file.file_size_ = each_size * milvus::engine::GB;
        status = impl.UpdateCollectionFile(table_file);
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

    milvus::engine::meta::FilesHolder files_holder;
    status = impl.GetCollectionFiles(table_file.collection_id_, ids, files_holder);
    ASSERT_TRUE(status.ok());

    for (auto& file : files_holder.HoldFiles()) {
        if (i >= 5) {
            ASSERT_EQ(file.file_type_, milvus::engine::meta::SegmentSchema::NEW);
        }
        ++i;
    }

    status = impl.GetCollectionFilesBySegmentId(table_file.segment_id_, files_holder);
    ASSERT_TRUE(status.ok());

    status = impl.DropAll();
    ASSERT_TRUE(status.ok());
}

TEST_F(MySqlMetaTest, INVALID_INITILIZE_TEST) {
    fiu_init(0);
    auto collection_id = "meta_test_group";
    milvus::engine::meta::CollectionSchema collection;
    collection.collection_id_ = collection_id;
    milvus::engine::DBMetaOptions meta = GetOptions().meta_;
    {
        FIU_ENABLE_FIU("MySQLMetaImpl.Initialize.fail_create_directory");
        // delete directory created by SetUp
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
        FIU_ENABLE_FIU("MySQLMetaImpl.Initialize.meta_schema_exist");
        FIU_ENABLE_FIU("MySQLMetaImpl.Initialize.fail_create_meta_schema");
        ASSERT_ANY_THROW(milvus::engine::meta::MySQLMetaImpl impl(GetOptions().meta_, GetOptions().mode_));
        fiu_disable("MySQLMetaImpl.Initialize.fail_create_meta_schema");
        fiu_disable("MySQLMetaImpl.Initialize.meta_schema_exist");
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

TEST_F(MySqlMetaTest, COLLECTION_FILES_TEST) {
    auto collection_id = "meta_test_group";
    fiu_init(0);

    milvus::engine::meta::CollectionSchema collection;
    collection.collection_id_ = collection_id;
    auto status = impl_->CreateCollection(collection);

    uint64_t new_merge_files_cnt = 1;
    uint64_t new_index_files_cnt = 2;
    uint64_t backup_files_cnt = 3;
    uint64_t new_files_cnt = 4;
    uint64_t raw_files_cnt = 5;
    uint64_t to_index_files_cnt = 6;
    uint64_t index_files_cnt = 7;

    milvus::engine::meta::SegmentSchema table_file;
    table_file.collection_id_ = collection.collection_id_;

    for (auto i = 0; i < new_merge_files_cnt; ++i) {
        status = impl_->CreateCollectionFile(table_file);
        table_file.file_type_ = milvus::engine::meta::SegmentSchema::NEW_MERGE;
        status = impl_->UpdateCollectionFile(table_file);
    }

    for (auto i = 0; i < new_index_files_cnt; ++i) {
        status = impl_->CreateCollectionFile(table_file);
        table_file.file_type_ = milvus::engine::meta::SegmentSchema::NEW_INDEX;
        status = impl_->UpdateCollectionFile(table_file);
    }

    for (auto i = 0; i < backup_files_cnt; ++i) {
        status = impl_->CreateCollectionFile(table_file);
        table_file.file_type_ = milvus::engine::meta::SegmentSchema::BACKUP;
        table_file.row_count_ = 1;
        status = impl_->UpdateCollectionFile(table_file);
    }

    for (auto i = 0; i < new_files_cnt; ++i) {
        status = impl_->CreateCollectionFile(table_file);
        table_file.file_type_ = milvus::engine::meta::SegmentSchema::NEW;
        status = impl_->UpdateCollectionFile(table_file);
    }

    for (auto i = 0; i < raw_files_cnt; ++i) {
        status = impl_->CreateCollectionFile(table_file);
        table_file.file_type_ = milvus::engine::meta::SegmentSchema::RAW;
        table_file.row_count_ = 1;
        status = impl_->UpdateCollectionFile(table_file);
    }

    for (auto i = 0; i < to_index_files_cnt; ++i) {
        status = impl_->CreateCollectionFile(table_file);
        table_file.file_type_ = milvus::engine::meta::SegmentSchema::TO_INDEX;
        table_file.row_count_ = 1;
        status = impl_->UpdateCollectionFile(table_file);
    }

    for (auto i = 0; i < index_files_cnt; ++i) {
        status = impl_->CreateCollectionFile(table_file);
        table_file.file_type_ = milvus::engine::meta::SegmentSchema::INDEX;
        table_file.row_count_ = 1;
        status = impl_->UpdateCollectionFile(table_file);
    }

    uint64_t total_row_count = 0;
    status = impl_->Count(collection_id, total_row_count);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(total_row_count, raw_files_cnt + to_index_files_cnt + index_files_cnt);

    milvus::engine::meta::FilesHolder files_holder;
    status = impl_->FilesToIndex(files_holder);
    ASSERT_EQ(files_holder.HoldFiles().size(), to_index_files_cnt);

    files_holder.ReleaseFiles();
    status = impl_->FilesToMerge(collection.collection_id_, files_holder);
    ASSERT_EQ(files_holder.HoldFiles().size(), raw_files_cnt);

    files_holder.ReleaseFiles();
    FIU_ENABLE_FIU("MySQLMetaImpl.FilesToMerge.null_connection");
    status = impl_->FilesToMerge(collection.collection_id_, files_holder);
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.FilesToMerge.null_connection");

    files_holder.ReleaseFiles();
    FIU_ENABLE_FIU("MySQLMetaImpl.FilesToMerge.throw_exception");
    status = impl_->FilesToMerge(collection.collection_id_, files_holder);
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.FilesToMerge.throw_exception");

    files_holder.ReleaseFiles();
    status = impl_->FilesToMerge("notexist", files_holder);
    ASSERT_EQ(status.code(), milvus::DB_NOT_FOUND);

    files_holder.ReleaseFiles();
    status = impl_->FilesToSearch(collection_id, files_holder, false);
    ASSERT_EQ(files_holder.HoldFiles().size(), index_files_cnt);

    std::set<std::string> coll_arr;
    coll_arr.insert(collection_id);

    files_holder.ReleaseFiles();
    status = impl_->FilesToSearchEx(collection_id, coll_arr, files_holder, false);
    ASSERT_EQ(files_holder.HoldFiles().size(), index_files_cnt);

    files_holder.ReleaseFiles();
    status = impl_->FilesToSearchEx(collection_id, coll_arr, files_holder);
    ASSERT_EQ(files_holder.HoldFiles().size(), to_index_files_cnt + raw_files_cnt + index_files_cnt);

    table_file.file_type_ = milvus::engine::meta::SegmentSchema::RAW;
    table_file.file_size_ = milvus::engine::GB + 1;
    status = impl_->UpdateCollectionFile(table_file);
    ASSERT_TRUE(status.ok());

    files_holder.ReleaseFiles();
    status = impl_->FilesToIndex(files_holder);
    ASSERT_EQ(files_holder.HoldFiles().size(), to_index_files_cnt);

    FIU_ENABLE_FIU("MySQLMetaImpl.DescribeCollection.throw_exception");
    status = impl_->FilesToIndex(files_holder);
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.DescribeCollection.throw_exception");

    FIU_ENABLE_FIU("MySQLMetaImpl.FilesToIndex.null_connection");
    status = impl_->FilesToIndex(files_holder);
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.FilesToIndex.null_connection");

    FIU_ENABLE_FIU("MySQLMetaImpl.FilesToIndex.throw_exception");
    status = impl_->FilesToIndex(files_holder);
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.FilesToIndex.throw_exception");

    files_holder.ReleaseFiles();
    status = impl_->FilesToSearch(collection_id, files_holder);
    ASSERT_EQ(files_holder.HoldFiles().size(), to_index_files_cnt + raw_files_cnt + index_files_cnt);

    files_holder.ReleaseFiles();
    std::vector<size_t> ids = {9999999999UL};
    status = impl_->FilesByID(ids, files_holder);
    ASSERT_EQ(files_holder.HoldFiles().size(), 0);

    FIU_ENABLE_FIU("MySQLMetaImpl.FilesToSearch.null_connection");
    status = impl_->FilesToSearch(collection_id, files_holder);
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.FilesToSearch.null_connection");

    FIU_ENABLE_FIU("MySQLMetaImpl.FilesToSearch.throw_exception");
    status = impl_->FilesToSearch(collection_id, files_holder);
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.FilesToSearch.throw_exception");

    status = impl_->FilesToSearch("notexist", files_holder);
    ASSERT_EQ(status.code(), milvus::DB_NOT_FOUND);

    files_holder.ReleaseFiles();
    std::vector<int> file_types;
    status = impl_->FilesByType(collection.collection_id_, file_types, files_holder);
    ASSERT_TRUE(files_holder.HoldFiles().empty());
    ASSERT_FALSE(status.ok());

    file_types = {
        milvus::engine::meta::SegmentSchema::NEW,       milvus::engine::meta::SegmentSchema::NEW_MERGE,
        milvus::engine::meta::SegmentSchema::NEW_INDEX, milvus::engine::meta::SegmentSchema::TO_INDEX,
        milvus::engine::meta::SegmentSchema::INDEX,     milvus::engine::meta::SegmentSchema::RAW,
        milvus::engine::meta::SegmentSchema::BACKUP,
    };
    status = impl_->FilesByType(collection.collection_id_, file_types, files_holder);
    ASSERT_TRUE(status.ok());
    uint64_t total_cnt = new_index_files_cnt + new_merge_files_cnt + backup_files_cnt + new_files_cnt + raw_files_cnt +
                         to_index_files_cnt + index_files_cnt;
    ASSERT_EQ(files_holder.HoldFiles().size(), total_cnt);

    std::vector<milvus::engine::meta::CollectionSchema> collection_array;
    milvus::engine::meta::CollectionSchema schema;
    schema.collection_id_ = collection_id;
    status = impl_->FilesByTypeEx(collection_array, file_types, files_holder);
    ASSERT_TRUE(status.ok());

    //    FIU_ENABLE_FIU("MySQLMetaImpl.FilesByTypeEx.throw_exception");
    //    status = impl_->FilesByTypeEx(collection_array, file_types, files_holder);
    //    ASSERT_FALSE(status.ok());
    //    fiu_disable("MySQLMetaImpl.FilesByTypeEx.throw_exception");

    FIU_ENABLE_FIU("MySQLMetaImpl.DeleteCollectionFiles.null_connection");
    status = impl_->DeleteCollectionFiles({collection_id});
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.DeleteCollectionFiles.null_connection");

    FIU_ENABLE_FIU("MySQLMetaImpl.DeleteCollectionFiles.throw_exception");
    status = impl_->DeleteCollectionFiles({collection_id});
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.DeleteCollectionFiles.throw_exception");

    status = impl_->DeleteCollectionFiles({collection_id});
    ASSERT_TRUE(status.ok());

    status = impl_->DropCollections({collection_id});
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

    FIU_ENABLE_FIU("MySQLMetaImpl.CleanUpFilesWithTTL.RemoveToDeleteCollections_NUllConnection");
    status = impl_->CleanUpFilesWithTTL(0UL);
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.CleanUpFilesWithTTL.RemoveToDeleteCollections_NUllConnection");

    FIU_ENABLE_FIU("MySQLMetaImpl.CleanUpFilesWithTTL.RemoveToDeleteCollections_ThrowException");
    status = impl_->CleanUpFilesWithTTL(0UL);
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.CleanUpFilesWithTTL.RemoveToDeleteCollections_ThrowException");

    FIU_ENABLE_FIU("MySQLMetaImpl.CleanUpFilesWithTTL.RemoveDeletedCollectionFolder_NUllConnection");
    status = impl_->CleanUpFilesWithTTL(0UL);
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.CleanUpFilesWithTTL.RemoveDeletedCollectionFolder_NUllConnection");

    FIU_ENABLE_FIU("MySQLMetaImpl.CleanUpFilesWithTTL.RemoveDeletedCollectionFolder_ThrowException");
    status = impl_->CleanUpFilesWithTTL(0UL);
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.CleanUpFilesWithTTL.RemoveDeletedCollectionFolder_ThrowException");
}

TEST_F(MySqlMetaTest, INDEX_TEST) {
    auto collection_id = "index_test";
    fiu_init(0);

    milvus::engine::meta::CollectionSchema collection;
    collection.collection_id_ = collection_id;
    auto status = impl_->CreateCollection(collection);

    milvus::engine::CollectionIndex index;
    index.metric_type_ = 2;
    index.extra_params_ = {{"nlist", 1234}};
    index.engine_type_ = 3;
    status = impl_->UpdateCollectionIndex(collection_id, index);
    ASSERT_TRUE(status.ok());

    FIU_ENABLE_FIU("MySQLMetaImpl.UpdateCollectionIndex.null_connection");
    status = impl_->UpdateCollectionIndex(collection_id, index);
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.UpdateCollectionIndex.null_connection");

    FIU_ENABLE_FIU("MySQLMetaImpl.UpdateCollectionIndex.throw_exception");
    status = impl_->UpdateCollectionIndex(collection_id, index);
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.UpdateCollectionIndex.throw_exception");

    status = impl_->UpdateCollectionIndex("notexist", index);
    ASSERT_EQ(status.code(), milvus::DB_NOT_FOUND);

    int64_t flag = 65536;
    status = impl_->UpdateCollectionFlag(collection_id, flag);
    ASSERT_TRUE(status.ok());

    FIU_ENABLE_FIU("MySQLMetaImpl.UpdateCollectionFlag.null_connection");
    status = impl_->UpdateCollectionFlag(collection_id, flag);
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.UpdateCollectionFlag.null_connection");

    FIU_ENABLE_FIU("MySQLMetaImpl.UpdateCollectionFlag.throw_exception");
    status = impl_->UpdateCollectionFlag(collection_id, flag);
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.UpdateCollectionFlag.throw_exception");

    milvus::engine::meta::CollectionSchema collection_info;
    collection_info.collection_id_ = collection_id;
    status = impl_->DescribeCollection(collection_info);
    ASSERT_EQ(collection_info.flag_, flag);

    milvus::engine::CollectionIndex index_out;
    status = impl_->DescribeCollectionIndex(collection_id, index_out);
    ASSERT_EQ(index_out.metric_type_, index.metric_type_);
    ASSERT_EQ(index_out.extra_params_, index.extra_params_);
    ASSERT_EQ(index_out.engine_type_, index.engine_type_);

    status = impl_->DropCollectionIndex(collection_id);
    ASSERT_TRUE(status.ok());
    status = impl_->DescribeCollectionIndex(collection_id, index_out);
    ASSERT_EQ(index_out.metric_type_, index.metric_type_);
    ASSERT_NE(index_out.engine_type_, index.engine_type_);

    FIU_ENABLE_FIU("MySQLMetaImpl.DescribeCollectionIndex.null_connection");
    status = impl_->DescribeCollectionIndex(collection_id, index_out);
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.DescribeCollectionIndex.null_connection");

    FIU_ENABLE_FIU("MySQLMetaImpl.DescribeCollectionIndex.throw_exception");
    status = impl_->DescribeCollectionIndex(collection_id, index_out);
    ASSERT_FALSE(status.ok());
    fiu_disable("MySQLMetaImpl.DescribeCollectionIndex.throw_exception");

    status = impl_->DescribeCollectionIndex("notexist", index_out);
    ASSERT_EQ(status.code(), milvus::DB_NOT_FOUND);

    status = impl_->UpdateCollectionFilesToIndex(collection_id);
    ASSERT_TRUE(status.ok());
}
