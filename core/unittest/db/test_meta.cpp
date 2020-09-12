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

#include "db/meta/MetaNames.h"
#include "db/meta/backend/MetaContext.h"
#include "db/snapshot/ResourceContext.h"
#include "db/utils.h"
<<<<<<< HEAD
#include "utils/Json.h"

template<typename T>
using ResourceContext = milvus::engine::snapshot::ResourceContext<T>;
template<typename T>
using ResourceContextBuilder = milvus::engine::snapshot::ResourceContextBuilder<T>;

using FType = milvus::engine::DataType;
using FEType = milvus::engine::FieldElementType;
using Op = milvus::engine::meta::MetaContextOp;
using State = milvus::engine::snapshot::State;

TEST_F(MetaTest, ApplyTest) {
    ID_TYPE result_id;

    auto collection = std::make_shared<Collection>("meta_test_c1");
    auto c_ctx = ResourceContextBuilder<Collection>().SetResource(collection).CreatePtr();
    auto status = meta_->Execute<Collection>(c_ctx, result_id);
    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_GT(result_id, 0);
    collection->SetID(result_id);

    collection->Activate();
    auto c2_ctx = ResourceContextBuilder<Collection>().SetResource(collection)
        .SetOp(Op::oUpdate).AddAttr(milvus::engine::meta::F_STATE).CreatePtr();
    status = meta_->Execute<Collection>(c2_ctx, result_id);
    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_GT(result_id, 0);
    ASSERT_EQ(result_id, collection->GetID());

    auto c3_ctx = ResourceContextBuilder<Collection>().SetID(result_id).SetOp(Op::oDelete).CreatePtr();
    status = meta_->Execute<Collection>(c3_ctx, result_id);
    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_GT(result_id, 0);
    ASSERT_EQ(result_id, collection->GetID());
=======

#include <fiu-control.h>
#include <fiu-local.h>
#include <gtest/gtest.h>
#include <stdlib.h>
#include <time.h>
#include <boost/filesystem/operations.hpp>

TEST_F(MetaTest, COLLECTION_TEST) {
    auto collection_id = "meta_test_table";

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

    status = impl_->DropCollections({collection.collection_id_});
    ASSERT_TRUE(status.ok());

    status = impl_->CreateCollection(collection);
    ASSERT_EQ(status.code(), milvus::DB_ERROR);

    collection.collection_id_ = "";
    status = impl_->CreateCollection(collection);
    ASSERT_TRUE(status.ok());
}

TEST_F(MetaTest, FAILED_TEST) {
    fiu_init(0);
    auto options = GetOptions();
    auto collection_id = "meta_test_table";
    milvus::engine::meta::CollectionSchema collection;
    collection.collection_id_ = collection_id;
    milvus::Status status;

    {
        FIU_ENABLE_FIU("SqliteMetaImpl.ValidateMetaSchema.NullConnection");
        ASSERT_ANY_THROW(milvus::engine::meta::SqliteMetaImpl impl(options.meta_));
        fiu_disable("SqliteMetaImpl.ValidateMetaSchema.NullConnection");
    }
    {
        // failed initialize
        auto options_1 = options;
        options_1.meta_.path_ = options.meta_.path_ + "1";
        if (boost::filesystem::is_directory(options_1.meta_.path_)) {
            boost::filesystem::remove_all(options_1.meta_.path_);
        }

        FIU_ENABLE_FIU("SqliteMetaImpl.Initialize.fail_create_directory");
        ASSERT_ANY_THROW(milvus::engine::meta::SqliteMetaImpl impl(options_1.meta_));
        fiu_disable("SqliteMetaImpl.Initialize.fail_create_directory");

        boost::filesystem::remove_all(options_1.meta_.path_);
    }
    {
        FIU_ENABLE_FIU("SqliteMetaImpl.CreateCollection.throw_exception");
        status = impl_->CreateCollection(collection);
        ASSERT_FALSE(status.ok());
        ASSERT_EQ(status.code(), milvus::DB_META_TRANSACTION_FAILED);
        fiu_disable("SqliteMetaImpl.CreateCollection.throw_exception");

        FIU_ENABLE_FIU("SqliteMetaImpl.CreateCollection.insert_throw_exception");
        collection.collection_id_ = "";
        status = impl_->CreateCollection(collection);
        ASSERT_FALSE(status.ok());
        fiu_disable("SqliteMetaImpl.CreateCollection.insert_throw_exception");

        // success create collection
        collection.collection_id_ = collection_id;
        status = impl_->CreateCollection(collection);
        ASSERT_TRUE(status.ok());
    }
    {
        FIU_ENABLE_FIU("SqliteMetaImpl.DescribeCollection.throw_exception");
        status = impl_->DescribeCollection(collection);
        ASSERT_FALSE(status.ok());
        fiu_disable("SqliteMetaImpl.DescribeCollection.throw_exception");
    }
    {
        FIU_ENABLE_FIU("SqliteMetaImpl.HasCollection.throw_exception");
        bool has = false;
        status = impl_->HasCollection(collection.collection_id_, has);
        ASSERT_FALSE(status.ok());
        ASSERT_FALSE(has);
        fiu_disable("SqliteMetaImpl.HasCollection.throw_exception");
    }
    {
        FIU_ENABLE_FIU("SqliteMetaImpl.AllCollections.throw_exception");
        std::vector<milvus::engine::meta::CollectionSchema> table_schema_array;
        status = impl_->AllCollections(table_schema_array);
        ASSERT_FALSE(status.ok());
        fiu_disable("SqliteMetaImpl.AllCollections.throw_exception");
    }
    {
        FIU_ENABLE_FIU("SqliteMetaImpl.DropCollection.throw_exception");
        status = impl_->DropCollections({collection.collection_id_});
        ASSERT_FALSE(status.ok());
        fiu_disable("SqliteMetaImpl.DropCollection.throw_exception");
    }
    {
        milvus::engine::meta::SegmentSchema schema;
        schema.collection_id_ = "notexist";
        status = impl_->CreateCollectionFile(schema);
        ASSERT_FALSE(status.ok());

        FIU_ENABLE_FIU("SqliteMetaImpl.CreateCollectionFile.throw_exception");
        schema.collection_id_ = collection_id;
        status = impl_->CreateCollectionFile(schema);
        ASSERT_FALSE(status.ok());
        fiu_disable("SqliteMetaImpl.CreateCollectionFile.throw_exception");
    }
    {
        FIU_ENABLE_FIU("SqliteMetaImpl.DeleteCollectionFiles.throw_exception");
        status = impl_->DeleteCollectionFiles({collection.collection_id_});
        ASSERT_FALSE(status.ok());
        fiu_disable("SqliteMetaImpl.DeleteCollectionFiles.throw_exception");
    }
    {
        milvus::engine::meta::FilesHolder files_holder;
        std::vector<size_t> ids;
        status = impl_->GetCollectionFiles("notexist", ids, files_holder);
        ASSERT_FALSE(status.ok());

        FIU_ENABLE_FIU("SqliteMetaImpl.GetCollectionFiles.throw_exception");
        status = impl_->GetCollectionFiles(collection_id, ids, files_holder);
        ASSERT_FALSE(status.ok());
        ASSERT_EQ(status.code(), milvus::DB_META_TRANSACTION_FAILED);
        fiu_disable("SqliteMetaImpl.GetCollectionFiles.throw_exception");
    }
    {
        FIU_ENABLE_FIU("SqliteMetaImpl.UpdateCollectionFlag.throw_exception");
        status = impl_->UpdateCollectionFlag(collection_id, 0);
        ASSERT_EQ(status.code(), milvus::DB_META_TRANSACTION_FAILED);
        fiu_disable("SqliteMetaImpl.UpdateCollectionFlag.throw_exception");
    }
    {
        FIU_ENABLE_FIU("SqliteMetaImpl.UpdateCollectionFile.throw_exception");
        milvus::engine::meta::SegmentSchema schema;
        schema.collection_id_ = collection_id;
        status = impl_->UpdateCollectionFile(schema);
        ASSERT_EQ(status.code(), milvus::DB_META_TRANSACTION_FAILED);
        fiu_disable("SqliteMetaImpl.UpdateCollectionFile.throw_exception");

        schema = {};
        schema.collection_id_ = "notexist";
        status = impl_->UpdateCollectionFile(schema);
        ASSERT_TRUE(status.ok());
    }
    {
        milvus::engine::meta::SegmentsSchema schemas;
        milvus::engine::meta::SegmentSchema schema;
        schema.collection_id_ = "notexits";
        schemas.emplace_back(schema);
        status = impl_->UpdateCollectionFiles(schemas);
        ASSERT_TRUE(status.ok());

        FIU_ENABLE_FIU("SqliteMetaImpl.UpdateCollectionFiles.throw_exception");
        status = impl_->UpdateCollectionFiles(schemas);
        ASSERT_EQ(status.code(), milvus::DB_META_TRANSACTION_FAILED);
        fiu_disable("SqliteMetaImpl.UpdateCollectionFiles.throw_exception");

        FIU_ENABLE_FIU("SqliteMetaImpl.UpdateCollectionFiles.fail_commited");
        status = impl_->UpdateCollectionFiles(schemas);
        ASSERT_EQ(status.code(), milvus::DB_META_TRANSACTION_FAILED);
        fiu_disable("SqliteMetaImpl.UpdateCollectionFiles.fail_commited");
    }
    {
        milvus::engine::CollectionIndex index;
        status = impl_->UpdateCollectionIndex("notexist", index);
        ASSERT_EQ(status.code(), milvus::DB_NOT_FOUND);

        FIU_ENABLE_FIU("SqliteMetaImpl.UpdateCollectionIndex.throw_exception");
        status = impl_->UpdateCollectionIndex("notexist", index);
        ASSERT_EQ(status.code(), milvus::DB_META_TRANSACTION_FAILED);
        fiu_disable("SqliteMetaImpl.UpdateCollectionIndex.throw_exception");

        FIU_ENABLE_FIU("SqliteMetaImpl.DescribeCollectionIndex.throw_exception");
        status = impl_->DescribeCollectionIndex(collection_id, index);
        ASSERT_EQ(status.code(), milvus::DB_META_TRANSACTION_FAILED);
        fiu_disable("SqliteMetaImpl.DescribeCollectionIndex.throw_exception");
    }
    {
        FIU_ENABLE_FIU("SqliteMetaImpl.UpdateCollectionFilesToIndex.throw_exception");
        status = impl_->UpdateCollectionFilesToIndex(collection_id);
        ASSERT_EQ(status.code(), milvus::DB_META_TRANSACTION_FAILED);
        fiu_disable("SqliteMetaImpl.UpdateCollectionFilesToIndex.throw_exception");
    }
    {
        FIU_ENABLE_FIU("SqliteMetaImpl.DropCollectionIndex.throw_exception");
        status = impl_->DropCollectionIndex(collection_id);
        ASSERT_EQ(status.code(), milvus::DB_META_TRANSACTION_FAILED);
        fiu_disable("SqliteMetaImpl.DropCollectionIndex.throw_exception");
    }
    {
        std::string partition = "part0";
        std::string partition_tag = "tag0";
        status = impl_->CreatePartition("notexist", partition, partition_tag, 0);
        ASSERT_EQ(status.code(), milvus::DB_NOT_FOUND);

        status = impl_->CreatePartition(collection_id, partition, partition_tag, 0);
        ASSERT_TRUE(status.ok());

        partition_tag = "tag1";
        status = impl_->CreatePartition(collection_id, partition, partition_tag, 0);
        ASSERT_FALSE(status.ok());

        // create empty name partition
        partition = "";
        status = impl_->CreatePartition(collection_id, partition, partition_tag, 0);
        ASSERT_TRUE(status.ok());

        std::vector<milvus::engine::meta::CollectionSchema> partions_schema;
        status = impl_->ShowPartitions(collection_id, partions_schema);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(partions_schema.size(), 2);

        partions_schema.clear();
        FIU_ENABLE_FIU("SqliteMetaImpl.ShowPartitions.throw_exception");
        status = impl_->ShowPartitions(collection_id, partions_schema);
        ASSERT_EQ(status.code(), milvus::DB_META_TRANSACTION_FAILED);
        fiu_disable("SqliteMetaImpl.ShowPartitions.throw_exception");

        std::string partion;
        FIU_ENABLE_FIU("SqliteMetaImpl.GetPartitionName.throw_exception");
        status = impl_->GetPartitionName(collection_id, "tag0", partion);
        ASSERT_EQ(status.code(), milvus::DB_META_TRANSACTION_FAILED);
        fiu_disable("SqliteMetaImpl.GetPartitionName.throw_exception");
    }
    {
        milvus::engine::meta::FilesHolder files_holder;
        status = impl_->FilesToSearch("notexist", files_holder);
        ASSERT_EQ(status.code(), milvus::DB_NOT_FOUND);

        FIU_ENABLE_FIU("SqliteMetaImpl.FilesToSearch.throw_exception");
        status = impl_->FilesToSearch(collection_id, files_holder);
        ASSERT_EQ(status.code(), milvus::DB_META_TRANSACTION_FAILED);
        fiu_disable("SqliteMetaImpl.FilesToSearch.throw_exception");
    }
    {
        milvus::engine::meta::SegmentSchema file;
        file.collection_id_ = collection_id;
        status = impl_->CreateCollectionFile(file);
        ASSERT_TRUE(status.ok());
        file.file_type_ = milvus::engine::meta::SegmentSchema::TO_INDEX;
        impl_->UpdateCollectionFile(file);

        milvus::engine::meta::FilesHolder files_holder;
        FIU_ENABLE_FIU("SqliteMetaImpl_FilesToIndex_CollectionNotFound");
        status = impl_->FilesToIndex(files_holder);
        ASSERT_EQ(status.code(), milvus::DB_NOT_FOUND);
        fiu_disable("SqliteMetaImpl_FilesToIndex_CollectionNotFound");

        FIU_ENABLE_FIU("SqliteMetaImpl.FilesToIndex.throw_exception");
        status = impl_->FilesToIndex(files_holder);
        ASSERT_EQ(status.code(), milvus::DB_META_TRANSACTION_FAILED);
        fiu_disable("SqliteMetaImpl.FilesToIndex.throw_exception");
    }
    {
        milvus::engine::meta::FilesHolder files_holder;
        std::vector<int> file_types;
        file_types.push_back(milvus::engine::meta::SegmentSchema::INDEX);
        FIU_ENABLE_FIU("SqliteMetaImpl.FilesByType.throw_exception");
        status = impl_->FilesByType(collection_id, file_types, files_holder);
        ASSERT_EQ(status.code(), milvus::DB_META_TRANSACTION_FAILED);
        fiu_disable("SqliteMetaImpl.FilesByType.throw_exception");
    }
    {
        milvus::engine::meta::FilesHolder files_holder;
        std::vector<milvus::engine::meta::CollectionSchema> collection_array;
        milvus::engine::meta::CollectionSchema schema;
        schema.collection_id_ = collection_id;
        collection_array.emplace_back(schema);
        std::vector<int> file_types;
        file_types.push_back(milvus::engine::meta::SegmentSchema::INDEX);
        FIU_ENABLE_FIU("SqliteMetaImpl.FilesByTypeEx.throw_exception");
        status = impl_->FilesByTypeEx(collection_array, file_types, files_holder);
        ASSERT_EQ(status.code(), milvus::DB_META_TRANSACTION_FAILED);
        fiu_disable("SqliteMetaImpl.FilesByTypeEx.throw_exception");
        status = impl_->FilesByTypeEx(collection_array, file_types, files_holder);
    }
    {
        uint64_t size = 0;
        FIU_ENABLE_FIU("SqliteMetaImpl.Size.throw_exception");
        status = impl_->Size(size);
        ASSERT_EQ(status.code(), milvus::DB_META_TRANSACTION_FAILED);
        fiu_disable("SqliteMetaImpl.Size.throw_exception");
    }
    {
        FIU_ENABLE_FIU("SqliteMetaImpl.CleanUpShadowFiles.fail_commited");
        status = impl_->CleanUpShadowFiles();
        ASSERT_EQ(status.code(), milvus::DB_META_TRANSACTION_FAILED);
        fiu_disable("SqliteMetaImpl.CleanUpShadowFiles.fail_commited");

        FIU_ENABLE_FIU("SqliteMetaImpl.CleanUpShadowFiles.throw_exception");
        status = impl_->CleanUpShadowFiles();
        ASSERT_EQ(status.code(), milvus::DB_META_TRANSACTION_FAILED);
        fiu_disable("SqliteMetaImpl.CleanUpShadowFiles.throw_exception");
    }
    {
        uint64_t count;
        status = impl_->Count("notexist", count);
        ASSERT_EQ(status.code(), milvus::DB_NOT_FOUND);

        FIU_ENABLE_FIU("SqliteMetaImpl.Count.throw_exception");
        status = impl_->Count("notexist", count);
        ASSERT_EQ(status.code(), milvus::DB_META_TRANSACTION_FAILED);
        fiu_disable("SqliteMetaImpl.Count.throw_exception");
    }
    {
        FIU_ENABLE_FIU("SqliteMetaImpl.CleanUpFilesWithTTL.RemoveFile_ThrowException");
        status = impl_->CleanUpFilesWithTTL(1);
        ASSERT_EQ(status.code(), milvus::DB_META_TRANSACTION_FAILED);
        fiu_disable("SqliteMetaImpl.CleanUpFilesWithTTL.RemoveFile_ThrowException");

        FIU_ENABLE_FIU("SqliteMetaImpl.CleanUpFilesWithTTL.RemoveFile_FailCommited");
        status = impl_->CleanUpFilesWithTTL(1);
        ASSERT_EQ(status.code(), milvus::DB_META_TRANSACTION_FAILED);
        fiu_disable("SqliteMetaImpl.CleanUpFilesWithTTL.RemoveFile_FailCommited");

        FIU_ENABLE_FIU("SqliteMetaImpl.CleanUpFilesWithTTL.RemoveCollection_Failcommited");
        status = impl_->CleanUpFilesWithTTL(1);
        ASSERT_EQ(status.code(), milvus::DB_META_TRANSACTION_FAILED);
        fiu_disable("SqliteMetaImpl.CleanUpFilesWithTTL.RemoveCollection_Failcommited");

        FIU_ENABLE_FIU("SqliteMetaImpl.CleanUpFilesWithTTL.RemoveCollection_ThrowException");
        status = impl_->CleanUpFilesWithTTL(1);
        ASSERT_EQ(status.code(), milvus::DB_META_TRANSACTION_FAILED);
        fiu_disable("SqliteMetaImpl.CleanUpFilesWithTTL.RemoveCollection_ThrowException");

        FIU_ENABLE_FIU("SqliteMetaImpl.CleanUpFilesWithTTL.RemoveCollectionFolder_ThrowException");
        status = impl_->CleanUpFilesWithTTL(1);
        ASSERT_EQ(status.code(), milvus::DB_META_TRANSACTION_FAILED);
        fiu_disable("SqliteMetaImpl.CleanUpFilesWithTTL.RemoveCollectionFolder_ThrowException");
    }
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda
}

TEST_F(MetaTest, SessionTest) {
    ID_TYPE result_id;

    auto collection = std::make_shared<Collection>("meta_test_c1");
    auto c_ctx = ResourceContextBuilder<Collection>().SetResource(collection).CreatePtr();
    auto status = meta_->Execute<Collection>(c_ctx, result_id);
    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_GT(result_id, 0);
    collection->SetID(result_id);

    auto partition = std::make_shared<Partition>("meta_test_p1", result_id);
    auto p_ctx = ResourceContextBuilder<Partition>().SetResource(partition).CreatePtr();
    status = meta_->Execute<Partition>(p_ctx, result_id);
    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_GT(result_id, 0);
    partition->SetID(result_id);

    auto field = std::make_shared<Field>("meta_test_f1", 1, FType::INT64);
    auto f_ctx = ResourceContextBuilder<Field>().SetResource(field).CreatePtr();
    status = meta_->Execute<Field>(f_ctx, result_id);
    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_GT(result_id, 0);
    field->SetID(result_id);

    auto field_element = std::make_shared<FieldElement>(collection->GetID(), field->GetID(),
                                                        "meta_test_f1_fe1", FEType::FET_RAW);
    auto fe_ctx = ResourceContextBuilder<FieldElement>().SetResource(field_element).CreatePtr();
    status = meta_->Execute<FieldElement>(fe_ctx, result_id);
    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_GT(result_id, 0);
    field_element->SetID(result_id);

    auto session = meta_->CreateSession();
    ASSERT_TRUE(collection->Activate());
    auto c2_ctx = ResourceContextBuilder<Collection>().SetResource(collection)
        .SetOp(Op::oUpdate).AddAttr(milvus::engine::meta::F_STATE).CreatePtr();
    status = session->Apply<Collection>(c2_ctx);
    ASSERT_TRUE(status.ok()) << status.ToString();

    ASSERT_TRUE(partition->Activate());
    auto p2_ctx = ResourceContextBuilder<Partition>().SetResource(partition)
        .SetOp(Op::oUpdate).AddAttr(milvus::engine::meta::F_STATE).CreatePtr();
    status = session->Apply<Partition>(p2_ctx);
    ASSERT_TRUE(status.ok()) << status.ToString();

    ASSERT_TRUE(field->Activate());
    auto f2_ctx = ResourceContextBuilder<Field>().SetResource(field)
        .SetOp(Op::oUpdate).AddAttr(milvus::engine::meta::F_STATE).CreatePtr();
    status = session->Apply<Field>(f2_ctx);
    ASSERT_TRUE(status.ok()) << status.ToString();

    ASSERT_TRUE(field_element->Activate());
    auto fe2_ctx = ResourceContextBuilder<FieldElement>().SetResource(field_element)
        .SetOp(Op::oUpdate).AddAttr(milvus::engine::meta::F_STATE).CreatePtr();
    status = session->Apply<FieldElement>(fe2_ctx);
    ASSERT_TRUE(status.ok()) << status.ToString();

    std::vector<ID_TYPE> result_ids;
    status = session->Commit(result_ids);
    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_EQ(result_ids.size(), 4);
    ASSERT_EQ(result_ids.at(0), collection->GetID());
    ASSERT_EQ(result_ids.at(1), partition->GetID());
    ASSERT_EQ(result_ids.at(2), field->GetID());
    ASSERT_EQ(result_ids.at(3), field_element->GetID());
}

TEST_F(MetaTest, SelectTest) {
    ID_TYPE result_id;

    auto collection = std::make_shared<Collection>("meta_test_c1");
    ASSERT_TRUE(collection->Activate());
    auto c_ctx = ResourceContextBuilder<Collection>().SetResource(collection).CreatePtr();
    auto status = meta_->Execute<Collection>(c_ctx, result_id);
    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_GT(result_id, 0);
    collection->SetID(result_id);

    Collection::Ptr return_collection;
    status = meta_->Select<Collection>(collection->GetID(), return_collection);
    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_EQ(collection->GetID(), return_collection->GetID());
    ASSERT_EQ(collection->GetName(), return_collection->GetName());

    auto collection2 = std::make_shared<Collection>("meta_test_c2");
    ASSERT_TRUE(collection2->Activate());
    auto c2_ctx = ResourceContextBuilder<Collection>().SetResource(collection2).CreatePtr();
    status = meta_->Execute<Collection>(c2_ctx, result_id);
    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_GT(result_id, 0);
    collection2->SetID(result_id);

    ASSERT_GT(collection2->GetID(), collection->GetID());

    std::vector<Collection::Ptr> return_collections;
    status = meta_->SelectBy<Collection, ID_TYPE>(milvus::engine::meta::F_ID,
                                                  {collection2->GetID()}, return_collections);
    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_EQ(return_collections.size(), 1);
    ASSERT_EQ(return_collections.at(0)->GetID(), collection2->GetID());
    ASSERT_EQ(return_collections.at(0)->GetName(), collection2->GetName());
    return_collections.clear();

    status = meta_->SelectBy<Collection, State>(milvus::engine::meta::F_STATE, {State::ACTIVE}, return_collections);
    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_EQ(return_collections.size(), 2);

    std::vector<ID_TYPE> ids;
    status = meta_->SelectResourceIDs<Collection, std::string>(ids, "", {""});
    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_EQ(ids.size(), 2);

    ids.clear();
    status = meta_->SelectResourceIDs<Collection, std::string>(ids, milvus::engine::meta::F_NAME,
                                                               {collection->GetName()});
    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_EQ(ids.size(), 1);
    ASSERT_EQ(ids.at(0), collection->GetID());
}

TEST_F(MetaTest, TruncateTest) {
    ID_TYPE result_id;

    auto collection = std::make_shared<Collection>("meta_test_c1");
    ASSERT_TRUE(collection->Activate());
    auto c_ctx = ResourceContextBuilder<Collection>().SetResource(collection).CreatePtr();
    auto status = meta_->Execute<Collection>(c_ctx, result_id);
    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_GT(result_id, 0);
    collection->SetID(result_id);

    status = meta_->TruncateAll();
    ASSERT_TRUE(status.ok()) << status.ToString();

    Collection::Ptr return_collection;
    status = meta_->Select<Collection>(collection->GetID(), return_collection);
    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_EQ(return_collection, nullptr);
}

TEST_F(MetaTest, MultiThreadRequestTest) {
    auto request_worker = [&](size_t i) {
        std::string collection_name_prefix = "meta_test_collection_" + std::to_string(i) + "_";
        int64_t result_id;
        for (size_t ii = 0; ii < 30; ii++) {
            std::string collection_name = collection_name_prefix + std::to_string(ii);
            auto collection = std::make_shared<Collection>(collection_name);
            auto c_ctx = ResourceContextBuilder<Collection>().SetResource(collection).CreatePtr();
            auto status = meta_->Execute<Collection>(c_ctx, result_id);
            ASSERT_TRUE(status.ok()) << status.ToString();
            ASSERT_GT(result_id, 0);

            collection->SetID(result_id);
            collection->Activate();
            auto c_ctx2 = ResourceContextBuilder<Collection>().SetResource(collection)
                .SetOp(Op::oUpdate).AddAttr(milvus::engine::meta::F_STATE).CreatePtr();
            status = meta_->Execute<Collection>(c_ctx2, result_id);
            ASSERT_TRUE(status.ok()) << status.ToString();

            CollectionPtr collection2;
            status = meta_->Select<Collection>(result_id, collection2);
            ASSERT_TRUE(status.ok()) << status.ToString();
            ASSERT_EQ(collection2->GetID(), result_id);
            ASSERT_EQ(collection2->GetState(), State::ACTIVE);
            ASSERT_EQ(collection2->GetName(), collection_name);

            collection->Deactivate();
            auto c_ctx3 = ResourceContextBuilder<Collection>().SetResource(collection)
                .SetOp(Op::oUpdate).AddAttr(milvus::engine::meta::F_STATE).CreatePtr();
            status = meta_->Execute<Collection>(c_ctx3, result_id);
            ASSERT_TRUE(status.ok()) << status.ToString();
            ASSERT_EQ(result_id, collection->GetID());

            auto c_ctx4 = ResourceContextBuilder<Collection>().SetID(result_id)
                .SetOp(Op::oDelete).SetTable(Collection::Name).CreatePtr();
            status = meta_->Execute<Collection>(c_ctx4, result_id);
            ASSERT_TRUE(status.ok()) << status.ToString();
            CollectionPtr collection3;
            status = meta_->Select<Collection>(result_id, collection3);
            ASSERT_TRUE(status.ok()) << status.ToString();
            ASSERT_EQ(collection3, nullptr);
        }
<<<<<<< HEAD
    };
=======
        ++i;
    }

    status = impl.GetCollectionFilesBySegmentId(table_file.segment_id_, files_holder);
    ASSERT_TRUE(status.ok());

    impl.DropAll();
}

TEST_F(MetaTest, COLLECTION_FILES_TEST) {
    auto collection_id = "meta_test_group";

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
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda

    auto cc_task = [&](size_t j) {
        std::string collection_name_prefix = "meta_test_collection_cc_" + std::to_string(j) + "_";
        int64_t result_id;
        Status status;
        for (size_t jj = 0; jj < 20; jj ++) {
            std::string collection_name = collection_name_prefix + std::to_string(jj);
            milvus::json cj{{"segment_row_count", 1024}};
            auto collection = std::make_shared<Collection>(collection_name, cj);
            auto c_ctx = ResourceContextBuilder<Collection>().SetResource(collection).SetOp(Op::oAdd).CreatePtr();
            status = meta_->Execute<Collection>(c_ctx, result_id);
            ASSERT_TRUE(status.ok()) << status.ToString();
            ASSERT_GT(result_id, 0);
            collection->SetID(result_id);

            std::string partition_name = collection_name + "_p_" + std::to_string(jj);
            auto partition = std::make_shared<Partition>(partition_name, collection->GetID());
            auto p_ctx = ResourceContextBuilder<Partition>().SetResource(partition).SetOp(Op::oAdd).CreatePtr();
            status = meta_->Execute<Partition>(p_ctx, result_id);
            ASSERT_TRUE(status.ok()) << status.ToString();
            ASSERT_GT(result_id, 0);
            partition->SetID(result_id);

            std::string segment_name = partition_name + "_s_" + std::to_string(jj);
            auto segment = std::make_shared<Segment>(collection->GetID(), partition->GetID());
            auto s_ctx = ResourceContextBuilder<Segment>().SetResource(segment).SetOp(Op::oAdd).CreatePtr();
            status = meta_->Execute<Segment>(s_ctx, result_id);
            ASSERT_TRUE(status.ok()) << status.ToString();
            ASSERT_GT(result_id, 0);
            segment->SetID(result_id);

            auto session = meta_->CreateSession();

            collection->Activate();
            auto c_ctx2 = ResourceContextBuilder<Collection>().SetResource(collection)
                .SetOp(Op::oUpdate).AddAttr(milvus::engine::meta::F_STATE).CreatePtr();
            ASSERT_TRUE(session->Apply<Collection>(c_ctx2).ok());
            partition->Activate();
            auto p_ctx2 = ResourceContextBuilder<Partition>().SetResource(partition)
                .SetOp(Op::oUpdate).AddAttr(milvus::engine::meta::F_STATE).CreatePtr();
            ASSERT_TRUE(session->Apply<Partition>(p_ctx2).ok());
            segment->Activate();
            auto s_ctx2 = ResourceContextBuilder<Segment>().SetResource(segment)
                .SetOp(Op::oUpdate).AddAttr(milvus::engine::meta::F_STATE).CreatePtr();
            ASSERT_TRUE(session->Apply<Segment>(s_ctx2).ok());
            std::vector<int64_t> ids;
            status = session->Commit(ids);
            ASSERT_TRUE(status.ok()) << status.ToString();
        }
    };

    unsigned int thread_hint = std::thread::hardware_concurrency();
    std::vector<std::thread> request_threads;
    for (size_t i = 0; i < 3 * thread_hint; i++) {
        request_threads.emplace_back(request_worker, i);
    }

    std::vector<std::thread> cc_threads;
    for (size_t j = 0; j < 3 * thread_hint; j++) {
        cc_threads.emplace_back(cc_task, j);
    }

    for (auto& t : request_threads) {
        t.join();
    }

    for (auto& t : cc_threads) {
        t.join();
    }
<<<<<<< HEAD
=======
    size_t cnt = files_holder.HoldFiles().size();
    files_holder.ReleaseFiles();
    status = impl_->FilesByID(ids, files_holder);
    ASSERT_EQ(files_holder.HoldFiles().size(), cnt);

    files_holder.ReleaseFiles();
    ids = {9999999999UL};
    status = impl_->FilesByID(ids, files_holder);
    ASSERT_EQ(files_holder.HoldFiles().size(), 0);

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

    status = impl_->DeleteCollectionFiles({collection_id});
    ASSERT_TRUE(status.ok());

    status = impl_->CreateCollectionFile(table_file);
    table_file.file_type_ = milvus::engine::meta::SegmentSchema::NEW;
    status = impl_->UpdateCollectionFile(table_file);
    status = impl_->CleanUpShadowFiles();
    ASSERT_TRUE(status.ok());

    table_file.collection_id_ = collection.collection_id_;
    table_file.file_type_ = milvus::engine::meta::SegmentSchema::TO_DELETE;
    status = impl_->CreateCollectionFile(table_file);

    std::vector<int> files_to_delete;
    files_holder.ReleaseFiles();
    files_to_delete.push_back(milvus::engine::meta::SegmentSchema::TO_DELETE);
    status = impl_->FilesByType(collection_id, files_to_delete, files_holder);
    ASSERT_TRUE(status.ok());

    table_file.collection_id_ = collection_id;
    table_file.file_type_ = milvus::engine::meta::SegmentSchema::TO_DELETE;
    table_file.file_id_ = files_holder.HoldFiles().front().file_id_;
    status = impl_->CleanUpFilesWithTTL(1UL);
    ASSERT_TRUE(status.ok());

    status = impl_->DropCollections({collection_id});
    ASSERT_TRUE(status.ok());
}

TEST_F(MetaTest, INDEX_TEST) {
    auto collection_id = "index_test";

    milvus::engine::meta::CollectionSchema collection;
    collection.collection_id_ = collection_id;
    auto status = impl_->CreateCollection(collection);

    milvus::engine::CollectionIndex index;
    index.metric_type_ = 2;
    index.extra_params_ = {{"nlist", 1234}};
    index.engine_type_ = 3;
    status = impl_->UpdateCollectionIndex(collection_id, index);
    ASSERT_TRUE(status.ok());

    int64_t flag = 65536;
    status = impl_->UpdateCollectionFlag(collection_id, flag);
    ASSERT_TRUE(status.ok());

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

    status = impl_->UpdateCollectionFilesToIndex(collection_id);
    ASSERT_TRUE(status.ok());
}

TEST_F(MetaTest, LSN_TEST) {
    auto collection_id = "lsn_test";
    uint64_t lsn = 42949672960;

    milvus::engine::meta::CollectionSchema collection;
    collection.collection_id_ = collection_id;
    auto status = impl_->CreateCollection(collection);

    status = impl_->UpdateCollectionFlushLSN(collection_id, lsn);
    ASSERT_TRUE(status.ok());

    uint64_t temp_lsb = 0;
    status = impl_->GetCollectionFlushLSN(collection_id, temp_lsb);
    ASSERT_EQ(temp_lsb, lsn);

    status = impl_->SetGlobalLastLSN(lsn);
    ASSERT_TRUE(status.ok());

    temp_lsb = 0;
    status = impl_->GetGlobalLastLSN(temp_lsb);
    ASSERT_EQ(temp_lsb, lsn);
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda
}
