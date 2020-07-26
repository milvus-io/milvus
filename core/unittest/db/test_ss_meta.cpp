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

template <typename T>
using ResourceContext = milvus::engine::snapshot::ResourceContext<T>;
template <typename T>
using ResourceContextBuilder = milvus::engine::snapshot::ResourceContextBuilder<T>;

using FType = milvus::engine::FieldType;
using FEType = milvus::engine::FieldElementType;
using Op = milvus::engine::meta::MetaContextOp;

TEST_F(MetaTest, ApplyTest) {
    ID_TYPE result_id;

    auto collection = std::make_shared<Collection>("meta_test_c1");
    auto c_ctx = ResourceContextBuilder<Collection>().SetResource(collection).CreatePtr();
    auto status = meta_->Apply<Collection>(c_ctx, result_id);
    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_GT(result_id, 0);
    collection->SetID(result_id);

    collection->Activate();
    auto c2_ctx = ResourceContextBuilder<Collection>().SetResource(collection)
        .SetOp(Op::oUpdate).AddAttr(milvus::engine::meta::F_STATE).CreatePtr();
    status = meta_->Apply<Collection>(c2_ctx, result_id);
    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_GT(result_id, 0);
    ASSERT_EQ(result_id, collection->GetID());

    auto c3_ctx = ResourceContextBuilder<Collection>().SetID(result_id).SetOp(Op::oDelete).CreatePtr();
    status = meta_->Apply<Collection>(c3_ctx, result_id);
    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_GT(result_id, 0);
    ASSERT_EQ(result_id, collection->GetID());
}

TEST_F(MetaTest, SessionTest) {
    ID_TYPE result_id;

    auto collection = std::make_shared<Collection>("meta_test_c1");
    auto c_ctx = ResourceContextBuilder<Collection>().SetResource(collection).CreatePtr();
    auto status = meta_->Apply<Collection>(c_ctx, result_id);
    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_GT(result_id, 0);
    collection->SetID(result_id);

    auto partition = std::make_shared<Partition>("meta_test_p1", result_id);
    auto p_ctx = ResourceContextBuilder<Partition>().SetResource(partition).CreatePtr();
    status = meta_->Apply<Partition>(p_ctx, result_id);
    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_GT(result_id, 0);
    partition->SetID(result_id);

    auto field = std::make_shared<Field>("meta_test_f1", 1, FType::INT64);
    auto f_ctx = ResourceContextBuilder<Field>().SetResource(field).CreatePtr();
    status = meta_->Apply<Field>(f_ctx, result_id);
    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_GT(result_id, 0);
    field->SetID(result_id);

    auto field_element = std::make_shared<FieldElement>(collection->GetID(), field->GetID(),
        "meta_test_f1_fe1", FEType::FET_RAW);
    auto fe_ctx = ResourceContextBuilder<FieldElement>().SetResource(field_element).CreatePtr();
    status = meta_->Apply<FieldElement>(fe_ctx, result_id);
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
    auto status = meta_->Apply<Collection>(c_ctx, result_id);
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
    status = meta_->Apply<Collection>(c2_ctx, result_id);
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
    auto status = meta_->Apply<Collection>(c_ctx, result_id);
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
