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

#include <fiu-control.h>
#include <fiu/fiu-local.h>
#include <gtest/gtest.h>

#include <string>
#include <set>
#include <algorithm>

#include "db/utils.h"
#include "db/snapshot/HandlerFactory.h"

Status
GetFirstCollectionID(ID_TYPE& result_id) {
    std::vector<ID_TYPE> ids;
    auto status = Snapshots::GetInstance().GetCollectionIds(ids);
    if (status.ok()) {
        result_id = ids.at(0);
    }

    return status;
}

TEST_F(SnapshotTest, ResourcesTest) {
    int nprobe = 16;
    milvus::json params = {{"nprobe", nprobe}};
    ParamsField p_field(params);
    ASSERT_EQ(params, p_field.GetParams());

    auto nprobe_real = p_field.GetParams().at("nprobe").get<int>();
    ASSERT_EQ(nprobe, nprobe_real);
}

TEST_F(SnapshotTest, ReferenceProxyTest) {
    std::string status("raw");
    const std::string CALLED = "CALLED";
    auto callback = [&]() {
        status = CALLED;
    };

    auto proxy = ReferenceProxy();
    ASSERT_EQ(proxy.ref_count(), 0);

    int refcnt = 3;
    for (auto i = 0; i < refcnt; ++i) {
        proxy.Ref();
    }
    ASSERT_EQ(proxy.ref_count(), refcnt);

    proxy.RegisterOnNoRefCB(callback);

    for (auto i = 0; i < refcnt; ++i) {
        proxy.UnRef();
    }
    ASSERT_EQ(proxy.ref_count(), 0);
    ASSERT_EQ(status, CALLED);
}

TEST_F(SnapshotTest, ScopedResourceTest) {
    auto inner = std::make_shared<Collection>("c1");
    ASSERT_EQ(inner->ref_count(), 0);

    {
        auto not_scoped = CollectionScopedT(inner, false);
        ASSERT_EQ(not_scoped->ref_count(), 0);
        not_scoped->Ref();
        ASSERT_EQ(not_scoped->ref_count(), 1);
        ASSERT_EQ(inner->ref_count(), 1);

        auto not_scoped_2 = not_scoped;
        ASSERT_EQ(not_scoped_2->ref_count(), 1);
        ASSERT_EQ(not_scoped->ref_count(), 1);
        ASSERT_EQ(inner->ref_count(), 1);

        not_scoped_2->Ref();
        ASSERT_EQ(not_scoped_2->ref_count(), 2);
        ASSERT_EQ(not_scoped->ref_count(), 2);
        ASSERT_EQ(inner->ref_count(), 2);
    }
    inner->UnRef();
    ASSERT_EQ(inner->ref_count(), 1);

    inner->UnRef();
    ASSERT_EQ(inner->ref_count(), 0);

    {
        // Test scoped construct
        auto scoped = CollectionScopedT(inner);
        ASSERT_EQ(scoped->ref_count(), 1);
        ASSERT_EQ(inner->ref_count(), 1);

        {
            // Test bool operator
            CollectionScopedT other_scoped;
            ASSERT_EQ(other_scoped, false);
            // Test operator=
            other_scoped = scoped;
            ASSERT_EQ(other_scoped->ref_count(), 2);
            ASSERT_EQ(scoped->ref_count(), 2);
            ASSERT_EQ(inner->ref_count(), 2);
        }
        ASSERT_EQ(scoped->ref_count(), 1);
        ASSERT_EQ(inner->ref_count(), 1);

        {
            // Test copy
            auto other_scoped(scoped);
            ASSERT_EQ(other_scoped->ref_count(), 2);
            ASSERT_EQ(scoped->ref_count(), 2);
            ASSERT_EQ(inner->ref_count(), 2);
        }
        ASSERT_EQ(scoped->ref_count(), 1);
        ASSERT_EQ(inner->ref_count(), 1);
    }
    ASSERT_EQ(inner->ref_count(), 0);
}

TEST_F(SnapshotTest, ResourceHoldersTest) {
    ID_TYPE collection_id;
    ASSERT_TRUE(GetFirstCollectionID(collection_id).ok());
    auto collection = CollectionsHolder::GetInstance().GetResource(collection_id, false);
    auto prev_cnt = collection->ref_count();
    {
        auto collection_2 = CollectionsHolder::GetInstance().GetResource(collection_id, false);
        ASSERT_EQ(collection_2->GetID(), collection_id);
        ASSERT_EQ(collection_2->ref_count(), prev_cnt);
    }

    {
        auto collection_3 = CollectionsHolder::GetInstance().GetResource(collection_id, true);
        ASSERT_EQ(collection_3->GetID(), collection_id);
        ASSERT_EQ(collection_3->ref_count(), 1+prev_cnt);
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(80));

    if (prev_cnt == 0) {
        auto collection_4 = CollectionsHolder::GetInstance().GetResource(collection_id, false);
        ASSERT_TRUE(!collection_4);
    }
}

TEST_F(SnapshotTest, DeleteOperationTest) {
    std::string collection_name = "test_c1";
    LSN_TYPE lsn = 1;
    auto ss = CreateCollection(collection_name, lsn);
    ASSERT_TRUE(ss);

    auto collection = CollectionsHolder::GetInstance().GetResource(ss->GetCollectionId());
    ASSERT_EQ(collection->GetName(), collection_name);

    {
        auto soft_op = std::make_shared<SoftDeleteCollectionOperation>(collection->GetID());
        auto status = soft_op->Push();
        ASSERT_TRUE(status.ok());
        CollectionPtr soft_deleted;
        status = soft_op->GetResource(soft_deleted);
        ASSERT_TRUE(status.ok());
        ASSERT_TRUE(soft_deleted);
        ASSERT_EQ(soft_deleted->GetID(), collection->GetID());
        ASSERT_TRUE(soft_deleted->IsDeactive());
    }

    {
        CollectionPtr loaded;
        LoadOperationContext context;
        context.id = collection->GetID();
        auto load_op = std::make_shared<milvus::engine::snapshot::LoadOperation<Collection>>(context);
        auto status = load_op->Push();
        ASSERT_TRUE(status.ok());
        status = load_op->GetResource(loaded);
        ASSERT_TRUE(status.ok());
        ASSERT_TRUE(loaded);
        ASSERT_EQ(loaded->GetID(), collection->GetID());
        ASSERT_TRUE(loaded->IsDeactive());
    }

    {
        auto hard_op = std::make_shared<milvus::engine::snapshot::HardDeleteOperation<Collection>
            >(collection->GetID());
        auto status = hard_op->Push();
        ASSERT_TRUE(status.ok());
    }

    {
        CollectionPtr loaded;
        LoadOperationContext context;
        context.id = collection->GetID();
        auto load_op = std::make_shared<milvus::engine::snapshot::LoadOperation<Collection>>(context);
        auto status = load_op->Push();
        if (!status.ok()) {
            std::cout << status.ToString() << std::endl;
        }
        ASSERT_FALSE(status.ok());
    }

    {
        auto soft_op = std::make_shared<SoftDeleteCollectionOperation>(collection->GetID());
        auto status = soft_op->Push();
        if (!status.ok()) {
            std::cout << status.ToString() << std::endl;
        }
        ASSERT_FALSE(status.ok());
    }
}

TEST_F(SnapshotTest, CreateCollectionOperationTest) {
    ScopedSnapshotT expect_null;
    auto status = Snapshots::GetInstance().GetSnapshot(expect_null, 100000);
    ASSERT_FALSE(expect_null);

    std::string collection_name = "test_c1";
    LSN_TYPE lsn = 1;
    auto ss = CreateCollection(collection_name, lsn);
    ASSERT_TRUE(ss);

    ScopedSnapshotT latest_ss;
    status = Snapshots::GetInstance().GetSnapshot(latest_ss, "xxxx");
    ASSERT_FALSE(status.ok());

    status = Snapshots::GetInstance().GetSnapshot(latest_ss, collection_name);
    ASSERT_TRUE(status.ok());
    ASSERT_TRUE(latest_ss);
    ASSERT_TRUE(latest_ss->GetName() == collection_name);

    IDS_TYPE ids;
    status = Snapshots::GetInstance().GetCollectionIds(ids);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(ids.size(), 6);
    ASSERT_EQ(ids.back(), latest_ss->GetCollectionId());

    OperationContext sd_op_ctx;
    sd_op_ctx.collection = latest_ss->GetCollection();
    sd_op_ctx.lsn = latest_ss->GetMaxLsn() + 1;
    ASSERT_TRUE(sd_op_ctx.collection->IsActive());
    auto sd_op = std::make_shared<DropCollectionOperation>(sd_op_ctx, latest_ss);
    status = sd_op->Push();
    ASSERT_TRUE(status.ok());
    ASSERT_TRUE(sd_op->GetStatus().ok());
    ASSERT_FALSE(sd_op_ctx.collection->IsActive());
    ASSERT_FALSE(latest_ss->GetCollection()->IsActive());

    Snapshots::GetInstance().Reset();
}

TEST_F(SnapshotTest, DropCollectionTest) {
    std::string collection_name = "test_c1";
    LSN_TYPE lsn = 1;
    auto ss = CreateCollection(collection_name, lsn);
    ASSERT_TRUE(ss);
    ScopedSnapshotT lss;
    auto status = Snapshots::GetInstance().GetSnapshot(lss, collection_name);
    ASSERT_TRUE(status.ok());
    ASSERT_TRUE(lss);
    ASSERT_EQ(ss->GetID(), lss->GetID());
    auto prev_ss_id = ss->GetID();
    auto prev_c_id = ss->GetCollection()->GetID();
    lsn = ss->GetMaxLsn() + 1;
    status = Snapshots::GetInstance().DropCollection(collection_name, lsn);
    ASSERT_TRUE(status.ok());
    status = Snapshots::GetInstance().GetSnapshot(lss, collection_name);
    ASSERT_FALSE(status.ok());

    auto ss_2 = CreateCollection(collection_name, ++lsn);
    status = Snapshots::GetInstance().GetSnapshot(lss, collection_name);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(ss_2->GetID(), lss->GetID());
    ASSERT_NE(prev_ss_id, ss_2->GetID());
    ASSERT_NE(prev_c_id, ss_2->GetCollection()->GetID());
    status = Snapshots::GetInstance().DropCollection(collection_name, ++lsn);
    ASSERT_TRUE(status.ok());
    status = Snapshots::GetInstance().DropCollection(collection_name, ++lsn);
    ASSERT_FALSE(status.ok());
}

TEST_F(SnapshotTest, ConCurrentCollectionOperation) {
    std::string collection_name("c1");
    LSN_TYPE lsn = 1;

    ID_TYPE stale_ss_id;
    auto worker1 = [&]() {
        Status status;
        auto ss = CreateCollection(collection_name, ++lsn);
        ASSERT_TRUE(ss);
        ASSERT_EQ(ss->GetName(), collection_name);
        stale_ss_id = ss->GetID();
        ScopedSnapshotT a_ss;
        status = Snapshots::GetInstance().GetSnapshot(a_ss, collection_name);
        ASSERT_TRUE(status.ok());
        std::this_thread::sleep_for(std::chrono::milliseconds(80));
        ASSERT_FALSE(ss->GetCollection()->IsActive());
        status = Snapshots::GetInstance().GetSnapshot(a_ss, collection_name);
        ASSERT_FALSE(status.ok());

        auto c_c = CollectionCommitsHolder::GetInstance().GetResource(stale_ss_id, false);
        ASSERT_TRUE(c_c);
        ASSERT_EQ(c_c->GetID(), stale_ss_id);
    };
    auto worker2 = [&] {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        auto status = Snapshots::GetInstance().DropCollection(collection_name, ++lsn);
        ASSERT_TRUE(status.ok());
        ScopedSnapshotT a_ss;
        status = Snapshots::GetInstance().GetSnapshot(a_ss, collection_name);
        ASSERT_FALSE(status.ok());
    };
    auto worker3 = [&] {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        auto ss = CreateCollection(collection_name, ++lsn);
        ASSERT_FALSE(ss);
        std::this_thread::sleep_for(std::chrono::milliseconds(80));
        ss = CreateCollection(collection_name, ++lsn);
        ASSERT_TRUE(ss);
        ASSERT_EQ(ss->GetName(), collection_name);
    };
    std::thread t1 = std::thread(worker1);
    std::thread t2 = std::thread(worker2);
    std::thread t3 = std::thread(worker3);
    t1.join();
    t2.join();
    t3.join();

    auto c_c = CollectionCommitsHolder::GetInstance().GetResource(stale_ss_id, false);
    ASSERT_FALSE(c_c);
}

TEST_F(SnapshotTest, PartitionTest) {
    std::string collection_name("c1");
    LSN_TYPE lsn = 1;
    auto ss = CreateCollection(collection_name, ++lsn);
    ASSERT_TRUE(ss);
    ASSERT_EQ(ss->GetName(), collection_name);
    ASSERT_EQ(ss->NumberOfPartitions(), 1);

    auto partition_iterator = std::make_shared<PartitionCollector>(ss);
    partition_iterator->Iterate();
    ASSERT_TRUE(partition_iterator->GetStatus().ok());
    ASSERT_EQ(partition_iterator->partition_names_.size(), 1);

    OperationContext context;
    context.lsn = ++lsn;
    auto op = std::make_shared<CreatePartitionOperation>(context, ss);

    std::string partition_name("p1");
    PartitionContext p_ctx;
    p_ctx.name = partition_name;
    PartitionPtr partition;
    auto status = op->CommitNewPartition(p_ctx, partition);
    ASSERT_TRUE(status.ok());
    ASSERT_TRUE(partition);
    ASSERT_EQ(partition->GetName(), partition_name);
    ASSERT_FALSE(partition->IsActive());
    ASSERT_TRUE(partition->HasAssigned());

    status = op->Push();
    ASSERT_TRUE(status.ok());
    ScopedSnapshotT curr_ss;
    status = op->GetSnapshot(curr_ss);
    ASSERT_TRUE(status.ok());
    ASSERT_TRUE(curr_ss);
    ASSERT_EQ(curr_ss->GetName(), ss->GetName());
    ASSERT_GT(curr_ss->GetID(), ss->GetID());
    ASSERT_EQ(curr_ss->NumberOfPartitions(), 2);

    partition_iterator = std::make_shared<PartitionCollector>(curr_ss);
    partition_iterator->Iterate();
    ASSERT_TRUE(partition_iterator->GetStatus().ok());
    ASSERT_EQ(partition_iterator->partition_names_.size(), 2);

    p_ctx.lsn = ++lsn;
    auto drop_op = std::make_shared<DropPartitionOperation>(p_ctx, curr_ss);
    status = drop_op->Push();
    ASSERT_TRUE(status.ok());

    ScopedSnapshotT latest_ss;
    status = drop_op->GetSnapshot(latest_ss);
    ASSERT_TRUE(status.ok());
    ASSERT_TRUE(latest_ss);
    ASSERT_EQ(latest_ss->GetName(), ss->GetName());
    ASSERT_TRUE(latest_ss->GetID() > curr_ss->GetID());
    ASSERT_EQ(latest_ss->NumberOfPartitions(), 1);

    p_ctx.lsn = ++lsn;
    drop_op = std::make_shared<DropPartitionOperation>(p_ctx, latest_ss);
    status = drop_op->Push();
    std::cout << status.ToString() << std::endl;
    ASSERT_FALSE(status.ok());

    // TODO: Keep LSN in order
    PartitionContext pp_ctx;
    /* pp_ctx.name = "p2"; */
    /* curr_ss = CreatePartition(collection_name, pp_ctx, lsn-1); */
    /* ASSERT_TRUE(curr_ss); */

    std::stringstream p_name_stream;

    auto num = RandomInt(20, 30);
    for (auto i = 0; i < num; ++i) {
        p_name_stream.str("");
        p_name_stream << "partition_" << i;
        pp_ctx.name = p_name_stream.str();
        curr_ss = CreatePartition(collection_name, pp_ctx, ++lsn);
        ASSERT_TRUE(curr_ss);
        ASSERT_EQ(curr_ss->NumberOfPartitions(), 2 + i);
    }

    auto total_partition_num = curr_ss->NumberOfPartitions();

    ID_TYPE partition_id;
    for (auto i = 0; i < num; ++i) {
        p_name_stream.str("");
        p_name_stream << "partition_" << i;

        status = curr_ss->GetPartitionId(p_name_stream.str(), partition_id);
        ASSERT_TRUE(status.ok());
        status = Snapshots::GetInstance().DropPartition(curr_ss->GetCollectionId(), partition_id, ++lsn);
        ASSERT_TRUE(status.ok());
        status = Snapshots::GetInstance().GetSnapshot(curr_ss, curr_ss->GetCollectionId());
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(curr_ss->NumberOfPartitions(), total_partition_num - i - 1);
    }
}

TEST_F(SnapshotTest, PartitionTest2) {
    std::string collection_name("c1");
    LSN_TYPE lsn = 1;
    milvus::Status status;

    auto ss = CreateCollection(collection_name, ++lsn);
    ASSERT_TRUE(ss);
    ASSERT_EQ(lsn, ss->GetMaxLsn());

    OperationContext context;
    context.lsn = lsn - 1;
    auto cp_op = std::make_shared<CreatePartitionOperation>(context, ss);
    std::string partition_name("p1");
    PartitionContext p_ctx;
    p_ctx.name = partition_name;
    PartitionPtr partition;
    status = cp_op->CommitNewPartition(p_ctx, partition);
    ASSERT_TRUE(status.ok());
    ASSERT_TRUE(partition);
    ASSERT_EQ(partition->GetName(), partition_name);
    ASSERT_FALSE(partition->IsActive());
    ASSERT_TRUE(partition->HasAssigned());

    status = cp_op->Push();
    ASSERT_FALSE(status.ok()) << status.ToString();
}

TEST_F(SnapshotTest, DropSegmentTest){
    LSN_TYPE lsn = 0;
    auto next_lsn = [&]() -> decltype(lsn) {
        return ++lsn;
    };
    auto collection_name = "test";
    ScopedSnapshotT ss;
    ss = CreateCollection(collection_name, ++lsn);


    milvus::Status status;

    PartitionContext pp_ctx;
    std::stringstream p_name_stream;

    auto num = RandomInt(3, 5);
    for (auto i = 0; i < num; ++i) {
        p_name_stream.str("");
        p_name_stream << "partition_" << i;
        pp_ctx.name = p_name_stream.str();
        ss = CreatePartition(ss->GetName(), pp_ctx, next_lsn());
        ASSERT_TRUE(ss);
    }

    ASSERT_EQ(ss->NumberOfPartitions(), num + 1);

    auto total_row_cnt = 0;
    auto partitions = ss->GetResources<Partition>();
    SegmentFileContext sf_context;
    SFContextBuilder(sf_context, ss);

    for (auto& kv : partitions) {
        num = RandomInt(2, 5);
        auto row_cnt = 1024;
        for (auto i = 0; i < num; ++i) {
            ASSERT_TRUE(CreateSegment(ss, kv.first, next_lsn(), sf_context, row_cnt).ok());
            total_row_cnt += row_cnt;
        }
    }

    status = Snapshots::GetInstance().GetSnapshot(ss, collection_name);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(total_row_cnt, ss->GetCollectionCommit()->GetRowCount());

    OperationContext drop_seg_context;
    auto segments = ss->GetResources<Segment>();
    auto prev_partitions = ss->GetResources<Partition>();
    ASSERT_TRUE(!prev_partitions.empty());
    ASSERT_TRUE(!segments.empty());
    for (auto kv:segments){
        milvus::engine::snapshot::ID_TYPE segment_id = kv.first;
        auto seg = ss->GetResource<milvus::engine::snapshot::Segment>(segment_id);
        drop_seg_context.prev_segment = seg;
        auto drop_op = std::make_shared<milvus::engine::snapshot::DropSegmentOperation>(drop_seg_context, ss);
        status = drop_op->Push();
        ASSERT_TRUE(status.ok());
    }
    status = Snapshots::GetInstance().GetSnapshot(ss, collection_name);
    ASSERT_TRUE(status.ok());
    auto result_segments = ss->GetResources<Segment>();
    auto result_partitions = ss->GetResources<Partition>();
    ASSERT_TRUE(!result_partitions.empty());
    ASSERT_TRUE(result_segments.empty());

}

TEST_F(SnapshotTest, IndexTest) {
    LSN_TYPE lsn = 0;
    auto next_lsn = [&]() -> decltype(lsn) {
        return ++lsn;
    };

    std::vector<ID_TYPE> ids;
    auto status = Snapshots::GetInstance().GetCollectionIds(ids);
    ASSERT_TRUE(status.ok()) << status.message();

    auto collection_id = ids.at(0);

    ScopedSnapshotT ss;
    status = Snapshots::GetInstance().GetSnapshot(ss, collection_id);
    ASSERT_TRUE(status.ok()) << status.message();

    SegmentFileContext sf_context;
    SFContextBuilder(sf_context, ss);

    std::cout << ss->ToString() << std::endl;

    OperationContext context;
    context.lsn = next_lsn();
    context.prev_partition = ss->GetResource<Partition>(sf_context.partition_id);
    auto build_op = std::make_shared<ChangeSegmentFileOperation>(context, ss);
    SegmentFilePtr seg_file;
    status = build_op->CommitNewSegmentFile(sf_context, seg_file);
    ASSERT_TRUE(status.ok()) << status.message();
    ASSERT_TRUE(seg_file);
    auto op_ctx = build_op->GetContext();
    ASSERT_EQ(seg_file, op_ctx.new_segment_files[0]);

    build_op->Push();
    status = build_op->GetSnapshot(ss);
    ASSERT_TRUE(status.ok()) << status.message();

    auto filter = [&](SegmentFile::Ptr segment_file) -> bool {
        return segment_file->GetSegmentId() == seg_file->GetSegmentId();
    };

    auto filter2 = [&](SegmentFile::Ptr segment_file) -> bool {
        return true;
    };

    auto sf_collector = std::make_shared<SegmentFileCollector>(ss, filter);
    sf_collector->Iterate();

    auto it_found = sf_collector->segment_files_.find(seg_file->GetID());
    ASSERT_NE(it_found, sf_collector->segment_files_.end());

    status = Snapshots::GetInstance().GetSnapshot(ss, collection_id);
    ASSERT_TRUE(status.ok()) << status.ToString();

    OperationContext drop_ctx;
    drop_ctx.lsn = next_lsn();
    drop_ctx.stale_segment_files.push_back(seg_file);
    auto drop_op = std::make_shared<DropIndexOperation>(drop_ctx, ss);
    status = drop_op->Push();
    ASSERT_TRUE(status.ok());

    status = drop_op->GetSnapshot(ss);
    ASSERT_TRUE(status.ok());

    sf_collector = std::make_shared<SegmentFileCollector>(ss, filter);
    sf_collector->Iterate();

    it_found = sf_collector->segment_files_.find(seg_file->GetID());
    ASSERT_EQ(it_found, sf_collector->segment_files_.end());

    PartitionContext pp_ctx;
    std::stringstream p_name_stream;

    auto num = RandomInt(3, 5);
    for (auto i = 0; i < num; ++i) {
        p_name_stream.str("");
        p_name_stream << "partition_" << i;
        pp_ctx.name = p_name_stream.str();
        ss = CreatePartition(ss->GetName(), pp_ctx, next_lsn());
        ASSERT_TRUE(ss);
    }
    ASSERT_EQ(ss->NumberOfPartitions(), num + 1);

    sf_collector = std::make_shared<SegmentFileCollector>(ss, filter2);
    sf_collector->Iterate();
    auto prev_total = sf_collector->segment_files_.size();

    decltype(sf_context) osf_context;
    SFContextBuilder(osf_context, ss, {sf_context.field_element_name});
    std::vector<SegmentFileContext> sfs_context = {sf_context, osf_context};

    auto new_total = 0;
    auto partitions = ss->GetResources<Partition>();
    for (auto& kv : partitions) {
        num = RandomInt(2, 5);
        auto row_cnt = 1024;
        for (auto i = 0; i < num; ++i) {
            ASSERT_TRUE(CreateSegment(ss, kv.first, next_lsn(), sfs_context, row_cnt).ok());
        }
        new_total += num * sfs_context.size();
    }

    status = Snapshots::GetInstance().GetSnapshot(ss, ss->GetName());
    ASSERT_TRUE(status.ok());
    std::cout << ss->ToString() << std::endl;

    sf_collector = std::make_shared<SegmentFileCollector>(ss, filter2);
    sf_collector->Iterate();
    auto total = sf_collector->segment_files_.size();
    ASSERT_EQ(total, prev_total + new_total);

    std::set<ID_TYPE> fe_ids;

    for (auto ctx : sfs_context) {
        auto field_element_id = ss->GetFieldElementId(ctx.field_name,
                ctx.field_element_name);
        ASSERT_NE(field_element_id, 0);
        fe_ids.insert(field_element_id);
    }

    auto filter3 = [&](SegmentFile::Ptr segment_file) -> bool {
        return fe_ids.find(segment_file->GetFieldElementId()) != fe_ids.end();
    };
    sf_collector = std::make_shared<SegmentFileCollector>(ss, filter3);
    sf_collector->Iterate();
    auto specified_segment_files_cnt = sf_collector->segment_files_.size();

    OperationContext d_a_i_ctx;
    d_a_i_ctx.lsn = next_lsn();
    for (auto fe_id : fe_ids) {
        d_a_i_ctx.stale_field_elements.push_back(ss->GetResource<FieldElement>(fe_id));
    }

    auto drop_all_index_op = std::make_shared<DropAllIndexOperation>(d_a_i_ctx, ss);
    status = drop_all_index_op->Push();
    ASSERT_TRUE(status.ok());

    status = drop_all_index_op->GetSnapshot(ss);
    ASSERT_TRUE(status.ok());

    /* { */
    /*     auto& fields = ss->GetResources<Field>(); */
    /*     for (auto field_kv : fields) { */
    /*         auto field = field_kv.second; */
    /*         std::cout << "field " << field->GetID() << " " << field->GetName() << std::endl; */
    /*         auto& field_elements = ss->GetResources<FieldElement>(); */
    /*         for (auto element_kv : field_elements) { */
    /*             auto element = element_kv.second; */
    /*             if (element->GetFieldId() != field->GetID()) { */
    /*                 continue; */
    /*             } */
    /*             std::cout << "\tfield_element " << element->GetID() << " " << element->GetName() << std::endl; */
    /*         } */
    /*     } */
    /* } */

    std::cout << ss->ToString() << std::endl;
    sf_collector = std::make_shared<SegmentFileCollector>(ss, filter2);
    sf_collector->Iterate();
    ASSERT_EQ(sf_collector->segment_files_.size(), total - specified_segment_files_cnt);
    std::cout << "sf size = " << sf_collector->segment_files_.size() << std::endl;
    std::cout << "total = " << total << std::endl;
    std::cout << "specified_segment_files_cnt = " << specified_segment_files_cnt << std::endl;

    {
        auto& field_elements = ss->GetResources<FieldElement>();
        for (auto& kv : field_elements) {
            ASSERT_EQ(fe_ids.find(kv.second->GetID()), fe_ids.end());
        }
    }
}

TEST_F(SnapshotTest, OperationTest) {
    std::string to_string;
    LSN_TYPE lsn = 0;
    Status status;

    /* ID_TYPE collection_id; */
    /* ASSERT_TRUE(GetFirstCollectionID(collection_id).ok()); */
    std::string collection_name("c1");
    auto ss = CreateCollection(collection_name, ++lsn);
    ASSERT_TRUE(ss);

    SegmentFileContext sf_context;
    SFContextBuilder(sf_context, ss);

    auto& partitions = ss->GetResources<Partition>();
    auto total_row_cnt = 0;
    for (auto& kv : partitions) {
        auto num = RandomInt(2, 5);
        for (auto i = 0; i < num; ++i) {
            auto row_cnt = RandomInt(100, 200);
            ASSERT_TRUE(CreateSegment(ss, kv.first, ++lsn, sf_context, row_cnt).ok());
            total_row_cnt += row_cnt;
        }
    }

    status = Snapshots::GetInstance().GetSnapshot(ss, collection_name);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(total_row_cnt, ss->GetCollectionCommit()->GetRowCount());

    auto total_size = ss->GetCollectionCommit()->GetSize();

    auto ss_id = ss->GetID();
    lsn = ss->GetMaxLsn() + 1;

    // Check snapshot
    {
        auto collection_commit = CollectionCommitsHolder::GetInstance().GetResource(ss_id, false);
        /* snapshot::SegmentCommitsHolder::GetInstance().GetResource(prev_segment_commit->GetID()); */
        ASSERT_TRUE(collection_commit);
        std::cout << collection_commit->ToString() << std::endl;
    }

    OperationContext merge_ctx;
    auto merge_segment_row_cnt = 0;

    std::set<ID_TYPE> stale_segment_commit_ids;
    SFContextBuilder(sf_context, ss);

    std::cout << ss->ToString() << std::endl;

    ID_TYPE new_seg_id;
    ScopedSnapshotT new_ss;
    // Check build operation correctness
    {
        OperationContext context;
        context.lsn = ++lsn;
        auto build_op = std::make_shared<ChangeSegmentFileOperation>(context, ss);
        SegmentFilePtr seg_file;
        status = build_op->CommitNewSegmentFile(sf_context, seg_file);
        std::cout << status.ToString() << std::endl;
        ASSERT_TRUE(status.ok());
        ASSERT_TRUE(seg_file);
        auto prev_segment_commit = ss->GetSegmentCommitBySegmentId(seg_file->GetSegmentId());
        auto prev_segment_commit_mappings = prev_segment_commit->GetMappings();
        ASSERT_FALSE(prev_segment_commit->ToString().empty());

        auto new_size = RandomInt(1000, 20000);
        seg_file->SetSize(new_size);
        total_size += new_size;

        auto delta = prev_segment_commit->GetRowCount() / 2;
        build_op->CommitRowCountDelta(delta);
        total_row_cnt -= delta;

        build_op->Push();
        status = build_op->GetSnapshot(ss);
        ASSERT_TRUE(status.ok());
        ASSERT_GT(ss->GetID(), ss_id);
        ASSERT_EQ(ss->GetCollectionCommit()->GetRowCount(), total_row_cnt);
        ASSERT_EQ(ss->GetCollectionCommit()->GetSize(), total_size);
        std::cout << ss->ToString() << std::endl;

        auto segment_commit = ss->GetSegmentCommitBySegmentId(seg_file->GetSegmentId());
        auto segment_commit_mappings = segment_commit->GetMappings();
        MappingT expected_mappings = prev_segment_commit_mappings;
        expected_mappings.insert(seg_file->GetID());
        ASSERT_EQ(expected_mappings, segment_commit_mappings);

        auto seg = ss->GetResource<Segment>(seg_file->GetSegmentId());
        ASSERT_TRUE(seg);
        merge_ctx.stale_segments.push_back(seg);
        merge_segment_row_cnt += ss->GetSegmentCommitBySegmentId(seg->GetID())->GetRowCount();

        stale_segment_commit_ids.insert(segment_commit->GetID());
    }

//    std::this_thread::sleep_for(std::chrono::milliseconds(100));
//    // Check stale snapshot has been deleted from store
//    {
//        auto collection_commit = CollectionCommitsHolder::GetInstance().GetResource(ss_id, false);
//        ASSERT_FALSE(collection_commit);
//    }

    ss_id = ss->GetID();
    ID_TYPE partition_id;
    {
        std::vector<PartitionPtr> partitions;
        auto executor = [&](const PartitionPtr& partition,
                            PartitionIterator* itr) -> Status {
            if (partition->GetCollectionId() != ss->GetCollectionId()) {
                return Status::OK();
            }
            partitions.push_back(partition);
            return Status::OK();
        };
        auto iterator = std::make_shared<PartitionIterator>(ss, executor);
        iterator->Iterate();

        OperationContext context;
        context.lsn = ++lsn;
        context.prev_partition = ss->GetResource<Partition>(partitions[0]->GetID());
        auto op = std::make_shared<NewSegmentOperation>(context, ss);
        SegmentPtr new_seg;
        status = op->CommitNewSegment(new_seg);
        ASSERT_TRUE(status.ok());
        ASSERT_FALSE(new_seg->ToString().empty());
        SegmentFilePtr seg_file;
        status = op->CommitNewSegmentFile(sf_context, seg_file);
        ASSERT_TRUE(status.ok());

        auto new_segment_row_cnt = RandomInt(100, 200);
        status = op->CommitRowCount(new_segment_row_cnt);
        ASSERT_TRUE(status.ok());
        total_row_cnt += new_segment_row_cnt;

        auto new_size = new_segment_row_cnt * 5;
        seg_file->SetSize(new_size);
        total_size += new_size;

        status = op->Push();
        ASSERT_TRUE(status.ok());

        status = op->GetSnapshot(ss);
        ASSERT_GT(ss->GetID(), ss_id);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(ss->GetCollectionCommit()->GetRowCount(), total_row_cnt);
        ASSERT_EQ(ss->GetCollectionCommit()->GetSize(), total_size);

        auto segment_commit = ss->GetSegmentCommitBySegmentId(seg_file->GetSegmentId());
        auto segment_commit_mappings = segment_commit->GetMappings();
        MappingT expected_segment_mappings;
        expected_segment_mappings.insert(seg_file->GetID());
        ASSERT_EQ(expected_segment_mappings, segment_commit_mappings);

        merge_ctx.stale_segments.push_back(new_seg);
        merge_segment_row_cnt += ss->GetSegmentCommitBySegmentId(new_seg->GetID())->GetRowCount();

        partition_id = segment_commit->GetPartitionId();
        stale_segment_commit_ids.insert(segment_commit->GetID());
        auto partition = ss->GetResource<Partition>(partition_id);
        merge_ctx.prev_partition = partition;
        new_seg_id = seg_file->GetSegmentId();
        new_ss = ss;
    }

    SegmentPtr merge_seg;
    ss_id = ss->GetID();
    {
        auto prev_partition_commit = ss->GetPartitionCommitByPartitionId(partition_id);
        auto expect_null = ss->GetPartitionCommitByPartitionId(11111111);
        ASSERT_FALSE(expect_null);
        ASSERT_FALSE(prev_partition_commit->ToString().empty());
        merge_ctx.lsn = ++lsn;
        auto op = std::make_shared<MergeOperation>(merge_ctx, ss);
        SegmentPtr new_seg;
        status = op->CommitNewSegment(new_seg);
        sf_context.segment_id = new_seg->GetID();
        SegmentFilePtr seg_file;
        status = op->CommitNewSegmentFile(sf_context, seg_file);
        ASSERT_TRUE(status.ok());

        auto new_size = RandomInt(1000, 20000);
        seg_file->SetSize(new_size);
        auto stale_size = 0;
        for (auto& stale_seg : merge_ctx.stale_segments) {
            stale_size += ss->GetSegmentCommitBySegmentId(stale_seg->GetID())->GetSize();
        }
        total_size += new_size;
        total_size -= stale_size;

        status = op->Push();
        ASSERT_TRUE(status.ok()) << status.ToString();
        std::cout << op->ToString() << std::endl;
        status = op->GetSnapshot(ss);
        ASSERT_GT(ss->GetID(), ss_id);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(total_size, ss->GetCollectionCommit()->GetSize());

        auto segment_commit = ss->GetSegmentCommitBySegmentId(new_seg->GetID());
        ASSERT_EQ(segment_commit->GetRowCount(), merge_segment_row_cnt);
        ASSERT_EQ(segment_commit->GetSize(), new_size);
        ASSERT_EQ(ss->GetCollectionCommit()->GetRowCount(), total_row_cnt);

        auto new_partition_commit = ss->GetPartitionCommitByPartitionId(partition_id);
        auto new_mappings = new_partition_commit->GetMappings();
        auto prev_mappings = prev_partition_commit->GetMappings();
        auto expected_mappings = prev_mappings;
        for (auto id : stale_segment_commit_ids) {
            expected_mappings.erase(id);
        }
        expected_mappings.insert(segment_commit->GetID());
        ASSERT_EQ(expected_mappings, new_mappings);

        CollectionCommitsHolder::GetInstance().Dump();
        merge_seg = new_seg;
    }

    // 1. New seg1, seg2
    // 2. Build seg1 start
    // 3. Merge seg1, seg2 to seg3
    // 4. Commit new seg file of build operation -> Stale Segment Found Here!
    {
        OperationContext context;
        context.lsn = ++lsn;
        auto build_op = std::make_shared<ChangeSegmentFileOperation>(context, new_ss);
        SegmentFilePtr seg_file;
        auto new_sf_context = sf_context;
        new_sf_context.segment_id = new_seg_id;
        status = build_op->CommitNewSegmentFile(new_sf_context, seg_file);
        ASSERT_FALSE(status.ok());
    }

    {
        OperationContext context;
        context.lsn = ++lsn;
        auto op = std::make_shared<ChangeSegmentFileOperation>(context, ss);
        SegmentFilePtr seg_file;
        auto new_sf_context = sf_context;
        new_sf_context.segment_id = merge_seg->GetID();
        status = op->CommitNewSegmentFile(new_sf_context, seg_file);
        ASSERT_TRUE(status.ok());
        auto prev_sc = ss->GetSegmentCommitBySegmentId(merge_seg->GetID());
        ASSERT_TRUE(prev_sc);
        auto delta = prev_sc->GetRowCount() + 1;
        op->CommitRowCountDelta(delta);
        status = op->Push();
        std::cout << status.ToString() << std::endl;
        ASSERT_FALSE(status.ok());
    }

    std::string new_fe_name = "fe_index";
    {
        status = Snapshots::GetInstance().GetSnapshot(ss, collection_name);
        ASSERT_TRUE(status.ok());

        auto field = ss->GetField(sf_context.field_name);
        ASSERT_TRUE(field);
        auto new_fe = std::make_shared<FieldElement>(ss->GetCollectionId(),
                field->GetID(), new_fe_name, milvus::engine::FieldElementType::FET_INDEX);

        OperationContext context;
        context.lsn = ++lsn;
        context.new_field_elements.push_back(new_fe);
        auto op = std::make_shared<AddFieldElementOperation>(context, ss);
        status = op->Push();
        ASSERT_TRUE(status.ok());

        status = op->GetSnapshot(ss);
        ASSERT_TRUE(status.ok());

        std::cout << ss->ToString() << std::endl;
    }

    {
        auto snapshot_id = ss->GetID();
        auto field = ss->GetField(sf_context.field_name);
        ASSERT_TRUE(field);
        FieldElementPtr new_fe;
        status = ss->GetFieldElement(sf_context.field_name, new_fe_name, new_fe);
        ASSERT_TRUE(status.ok());
        ASSERT_TRUE(new_fe);

        OperationContext context;
        context.lsn = ++lsn;
        context.new_field_elements.push_back(new_fe);
        auto op = std::make_shared<AddFieldElementOperation>(context, ss);
        status = op->Push();
        ASSERT_FALSE(status.ok());

        status = Snapshots::GetInstance().GetSnapshot(ss, collection_name);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(snapshot_id, ss->GetID());
    }

    // 1. Build start
    // 2. Commit new seg file of build operation
    // 3. Drop collection
    // 4. Commit build operation -> Stale Segment Found Here!
    {
        OperationContext context;
        context.lsn = ++lsn;
        auto build_op = std::make_shared<ChangeSegmentFileOperation>(context, ss);
        SegmentFilePtr seg_file;
        auto new_sf_context = sf_context;
        new_sf_context.segment_id = merge_seg->GetID();
        status = build_op->CommitNewSegmentFile(new_sf_context, seg_file);
        ASSERT_TRUE(status.ok());
        std::cout << build_op->ToString() << std::endl;

        auto status = Snapshots::GetInstance().DropCollection(ss->GetName(), ++lsn);
        ASSERT_TRUE(status.ok());
        status = build_op->Push();
        std::cout << status.ToString() << std::endl;
        ASSERT_FALSE(status.ok());
        ASSERT_FALSE(build_op->GetStatus().ok());
        std::cout << build_op->ToString() << std::endl;
    }

    Snapshots::GetInstance().Reset();
}

TEST_F(SnapshotTest, OperationTest2) {
    LSN_TYPE lsn = 0;
    std::string collection_name("c1");
    auto ss = CreateCollection(collection_name, ++lsn);
    ASSERT_TRUE(ss);

    SegmentFileContext sf_context;
    SFContextBuilder(sf_context, ss);

    auto& partitions = ss->GetResources<Partition>();
    auto total_row_cnt = 0;
    for (auto& kv : partitions) {
        auto num = RandomInt(2, 5);
        for (auto i = 0; i < num; ++i) {
            auto row_cnt = RandomInt(100, 200);
            ASSERT_TRUE(CreateSegment(ss, kv.first, ++lsn, sf_context, row_cnt).ok());
            total_row_cnt += row_cnt;
        }
    }

    auto status = Snapshots::GetInstance().GetSnapshot(ss, collection_name);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(total_row_cnt, ss->GetCollectionCommit()->GetRowCount());

    auto total_size = ss->GetCollectionCommit()->GetSize();

    auto target_segment = ss->GetResources<Segment>().begin()->second.Get();

    auto sf_ids = ss->GetSegmentFileIds(target_segment->GetID());
    ASSERT_GT(sf_ids.size(), 0);
    auto stale_sf = ss->GetResource<SegmentFile>(*(sf_ids.begin()));
    ASSERT_TRUE(stale_sf);

    {
        OperationContext context;
        context.lsn = ++lsn;
        context.stale_segment_files.push_back(stale_sf);
        auto op = std::make_shared<ChangeSegmentFileOperation>(context, ss);
        SegmentFilePtr seg_file;
        sf_context.field_name = "vector";
        sf_context.field_element_name = "_raw";
        sf_context.segment_id = stale_sf->GetSegmentId();
        sf_context.partition_id = stale_sf->GetPartitionId();
        sf_context.collection_id = stale_sf->GetCollectionId();
        status = op->CommitNewSegmentFile(sf_context, seg_file);
        /* std::cout << status.ToString() << std::endl; */
        ASSERT_TRUE(status.ok());
        ASSERT_TRUE(seg_file);

        auto prev_segment_commit = ss->GetSegmentCommitBySegmentId(seg_file->GetSegmentId());
        // auto prev_segment_commit_mappings = prev_segment_commit->GetMappings();
        ASSERT_FALSE(prev_segment_commit->ToString().empty());

        auto new_size = RandomInt(1000, 20000);
        seg_file->SetSize(new_size);
        total_size += new_size;
        total_size -= stale_sf->GetSize();

        auto delta = prev_segment_commit->GetRowCount() / 2;
        op->CommitRowCountDelta(delta);
        total_row_cnt -= delta;

        status = op->Push();
        ASSERT_TRUE(status.ok()) << status.message();
        status = op->GetSnapshot(ss);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(ss->GetCollectionCommit()->GetRowCount(), total_row_cnt);
        ASSERT_EQ(ss->GetCollectionCommit()->GetSize(), total_size);
        std::cout << ss->ToString() << std::endl;
    }

    {
        OperationContext context;
        context.lsn = ++lsn;

        SegmentFile::VecT stale_sfs;
        std::set<ID_TYPE> modified_segments;
        auto executor = [&](const SegmentFilePtr& sf,
                            SegmentFileIterator* itr) -> Status {
            context.stale_segment_files.push_back(sf);
            modified_segments.insert(sf->GetSegmentId());
            return Status::OK();
        };
        auto iterator = std::make_shared<SegmentFileIterator>(ss, executor);
        iterator->Iterate();
        ASSERT_TRUE(iterator->GetStatus().ok());

        {
            auto op = std::make_shared<CompoundSegmentsOperation>(context, ss);

            for (auto seg_id : modified_segments) {
                auto sc = ss->GetSegmentCommitBySegmentId(seg_id);
                ASSERT_TRUE(sc);
                status = op->CommitRowCountDelta(seg_id, sc->GetRowCount() + 1, true);
                ASSERT_TRUE(status.ok()) << status.ToString();
            }

            status = op->Push();
            ASSERT_FALSE(status.ok()) << status.ToString();
        }

        {
            auto op = std::make_shared<CompoundSegmentsOperation>(context, ss);
            status = op->CommitRowCountDelta(100000000, 1, false);
            ASSERT_FALSE(status.ok()) << status.ToString();
        }

        {
            auto segment_cnt = ss->GetResources<Segment>().size();
            context.lsn++;
            auto op = std::make_shared<CompoundSegmentsOperation>(context, ss);
            for (auto seg_id : modified_segments) {
                auto sc = ss->GetSegmentCommitBySegmentId(seg_id);
                ASSERT_TRUE(sc);
                bool is_sub = (RandomInt(1, 10) >= 5);
                auto delta = sc->GetRowCount() / 2;
                status = op->CommitRowCountDelta(seg_id, delta, is_sub);
                ASSERT_TRUE(status.ok()) << status.ToString();
                if (is_sub) {
                    total_row_cnt -= delta;
                } else {
                    total_row_cnt += delta;
                }
            }

            auto new_size = 0;
            for (auto stale_sf : context.stale_segment_files) {
                SegmentFilePtr seg_file;
                sf_context.field_name = "vector";
                sf_context.field_element_name = "_raw";
                sf_context.segment_id = stale_sf->GetSegmentId();
                sf_context.partition_id = stale_sf->GetPartitionId();
                sf_context.collection_id = stale_sf->GetCollectionId();
                status = op->CommitNewSegmentFile(sf_context, seg_file);
                auto size_delta = RandomInt(100, 1000);
                seg_file->SetSize(size_delta);
                new_size += size_delta;
                ASSERT_TRUE(status.ok()) << status.ToString();
            }

            SegmentPtr new_seg;
            OperationContext new_seg_ctx;
            new_seg_ctx.prev_partition = ss->GetResource<Partition>(
                    context.stale_segment_files[0]->GetPartitionId());
            status = op->CommitNewSegment(new_seg_ctx, new_seg);
            ASSERT_TRUE(status.ok()) << status.ToString();
            ASSERT_TRUE(new_seg);

            {
                SegmentFilePtr seg_file;
                sf_context.field_name = "vector";
                sf_context.field_element_name = "_raw";
                sf_context.segment_id = new_seg->GetID();
                sf_context.partition_id = new_seg->GetPartitionId();
                sf_context.collection_id = new_seg->GetCollectionId();
                status = op->CommitNewSegmentFile(sf_context, seg_file);
                ASSERT_TRUE(status.ok()) << status.ToString();
                auto size_delta = RandomInt(100, 1000);
                seg_file->SetSize(size_delta);
                new_size += size_delta;
                ASSERT_TRUE(status.ok()) << status.ToString();
            }

            auto delta_row = RandomInt(100, 1000);
            status = op->CommitRowCountDelta(new_seg->GetID(), delta_row, false);
            ASSERT_TRUE(status.ok());
            total_row_cnt += delta_row;

            status = op->Push();
            ASSERT_TRUE(status.ok()) << status.ToString();
            status = op->GetSnapshot(ss);
            ASSERT_TRUE(status.ok());
            std::cout << ss->ToString() << std::endl;
            ASSERT_EQ(segment_cnt + 1, ss->GetResources<Segment>().size());

            auto expect_size = ss->GetCollectionCommit()->GetSize();
            ASSERT_EQ(expect_size, new_size) << status.ToString();
            auto expect_row_cnt = ss->GetCollectionCommit()->GetRowCount();
            ASSERT_EQ(expect_row_cnt, total_row_cnt) << status.ToString();
        }

        {
            modified_segments.clear();
            context.stale_segment_files.clear();
            iterator = std::make_shared<SegmentFileIterator>(ss, executor);
            iterator->Iterate();
            ASSERT_TRUE(iterator->GetStatus().ok());

            context.lsn++;
            auto op = std::make_shared<CompoundSegmentsOperation>(context, ss);

            for (auto seg_id : modified_segments) {
                auto sc = ss->GetSegmentCommitBySegmentId(seg_id);
                ASSERT_TRUE(sc);
                status = op->CommitRowCountDelta(seg_id, sc->GetRowCount(), true);
                ASSERT_TRUE(status.ok()) << status.ToString();
            }

            status = op->Push();
            ASSERT_TRUE(status.ok()) << status.ToString();

            status = op->GetSnapshot(ss);
            ASSERT_TRUE(status.ok());
            std::cout << ss->ToString() << std::endl;
            ASSERT_EQ(ss->GetCollectionCommit()->GetRowCount(), 0);
            ASSERT_EQ(ss->GetCollectionCommit()->GetSize(), 0);
        }
    }
}

TEST_F(SnapshotTest, DropTest) {
    Status status;
    std::atomic<LSN_TYPE> lsn = 0;
    auto next_lsn = [&]() -> decltype(lsn) {
        return ++lsn;
    };

    std::string collection_name("c1");
    auto ss = CreateCollection(collection_name, next_lsn());
    ASSERT_TRUE(ss);

    SegmentFileContext sf_context;
    SFContextBuilder(sf_context, ss);

    {
        OperationContext context;
        context.lsn = next_lsn();
        context.prev_partition = ss->GetResources<Partition>().begin()->second.Get();
        auto op = std::make_shared<NewSegmentOperation>(context, ss);
        SegmentPtr new_seg;
        status = op->CommitNewSegment(new_seg);
        ASSERT_TRUE(status.ok());
        ASSERT_FALSE(new_seg->ToString().empty());
        SegmentFilePtr seg_file;
        status = op->CommitNewSegmentFile(sf_context, seg_file);
        ASSERT_TRUE(status.ok());

        status = Snapshots::GetInstance().DropCollection(ss->GetName(), next_lsn());
        ASSERT_TRUE(status.ok());
        status = op->Push();
        ASSERT_FALSE(status.ok()) << status.message();
    }

    ss = CreateCollection(collection_name, next_lsn());
    ASSERT_TRUE(ss);

    {
        PartitionContext pp_ctx;
        pp_ctx.name = "p1";
        ss = CreatePartition(ss->GetName(), pp_ctx, next_lsn());
        ASSERT_TRUE(ss);

        OperationContext context;
        context.lsn = next_lsn();
        context.prev_partition = ss->GetPartition(pp_ctx.name);
        ASSERT_TRUE(context.prev_partition);
        auto op = std::make_shared<NewSegmentOperation>(context, ss);
        SegmentPtr new_seg;
        status = op->CommitNewSegment(new_seg);
        ASSERT_TRUE(status.ok());
        ASSERT_FALSE(new_seg->ToString().empty());
        SegmentFilePtr seg_file;
        sf_context.segment_id = new_seg->GetID();
        sf_context.partition_id = new_seg->GetPartitionId();
        sf_context.collection_id = new_seg->GetCollectionId();
        status = op->CommitNewSegmentFile(sf_context, seg_file);
        ASSERT_TRUE(status.ok());

        status = Snapshots::GetInstance().DropPartition(
                context.prev_partition->GetCollectionId(), context.prev_partition->GetID(), next_lsn());
        ASSERT_TRUE(status.ok());

        status = op->Push();
        ASSERT_FALSE(status.ok());

        {
            context.lsn = next_lsn();
            auto c_op = std::make_shared<CompoundSegmentsOperation>(context, ss);
            OperationContext new_seg_ctx;
            new_seg_ctx.prev_partition = context.prev_partition;
            status = c_op->CommitNewSegment(new_seg_ctx, new_seg);
            ASSERT_TRUE(status.ok());
            status = c_op->Push();
            ASSERT_FALSE(status.ok()) << status.ToString();
        }

        {
            status = Snapshots::GetInstance().GetSnapshot(ss, collection_name);
            ASSERT_TRUE(status.ok());
            context.lsn = next_lsn();
            auto c_op = std::make_shared<CompoundSegmentsOperation>(context, ss);
            OperationContext new_seg_ctx;
            new_seg_ctx.prev_partition = context.prev_partition;
            status = c_op->CommitNewSegment(new_seg_ctx, new_seg);
            ASSERT_FALSE(status.ok()) << status.ToString();
        }

    }
}

TEST_F(SnapshotTest, CompoundTest1) {
    Status status;
    std::atomic<LSN_TYPE> lsn = 0;
    auto next_lsn = [&]() -> decltype(lsn) {
        return ++lsn;
    };
    std::atomic<LSN_TYPE> pid = 0;
    auto next_pid = [&]() -> decltype(pid) {
        return ++pid;
    };
    std::string collection_name("c1");
    auto ss = CreateCollection(collection_name, next_lsn());
    ASSERT_TRUE(ss);
    ASSERT_EQ(lsn, ss->GetMaxLsn());

    Queue merge_queue;
    Queue build_queue;

    std::set<ID_TYPE> all_segments;
    std::set<ID_TYPE> segment_in_building;
    std::map<ID_TYPE, std::set<ID_TYPE>> merged_segs_history;
    std::set<ID_TYPE> merged_segs;
    std::set<ID_TYPE> built_segs;
    std::set<ID_TYPE> build_stale_segs;

    std::mutex all_mtx;
    std::mutex merge_mtx;
    std::mutex built_mtx;
    std::mutex partition_mtx;

    WaitableObj merge_waiter;
    WaitableObj build_waiter;

    SegmentFileContext sf_context;
    sf_context.field_name = "vector";
    sf_context.field_element_name = "ivfsq8";
    sf_context.segment_id = 1;
    sf_context.partition_id = 1;

    IDS_TYPE partitions = {ss->GetResources<Partition>().begin()->second->GetID()};

    auto do_build = [&] (const ID_TYPE& seg_id) {
        ScopedSnapshotT latest_ss;
        auto status = Snapshots::GetInstance().GetSnapshot(latest_ss, collection_name);
        ASSERT_TRUE(status.ok());

        auto build_sf_context = sf_context;

        OperationContext context;
        context.lsn = next_lsn();
        auto build_op = std::make_shared<ChangeSegmentFileOperation>(context, latest_ss);
        SegmentFilePtr seg_file;
        build_sf_context.segment_id = seg_id;
        status = build_op->CommitNewSegmentFile(build_sf_context, seg_file);
        if (!status.ok()) {
            std::cout << status.ToString() << std::endl;
            std::unique_lock<std::mutex> lock(merge_mtx);
            auto it = merged_segs.find(seg_id);
            ASSERT_NE(it, merged_segs.end());
            return;
        }
        std::unique_lock<std::mutex> lock(built_mtx);
        status = build_op->Push();
        if (!status.ok()) {
            std::cout << status.ToString() << std::endl;
            std::unique_lock<std::mutex> lock(merge_mtx);
            auto it = merged_segs.find(seg_id);
            ASSERT_NE(it, merged_segs.end());
            return;
        }
        ASSERT_TRUE(status.ok());
        status = build_op->GetSnapshot(latest_ss);
        ASSERT_TRUE(status.ok());

        built_segs.insert(seg_id);
    };

    auto do_merge = [&] (std::set<ID_TYPE>& seg_ids, ID_TYPE& new_seg_id) {
        if (seg_ids.size() == 0) {
            return;
        }
        ScopedSnapshotT latest_ss;
        auto status = Snapshots::GetInstance().GetSnapshot(latest_ss, collection_name);
        ASSERT_TRUE(status.ok());

        PartitionPtr partition;
        OperationContext context;
        for (auto& id : seg_ids) {
            auto seg = latest_ss->GetResource<Segment>(id);
            if (!seg) {
                std::cout << "Error seg=" << id << std::endl;
                ASSERT_TRUE(seg);
            }
            if (!partition) {
                partition = latest_ss->GetResource<Partition>(seg->GetPartitionId());
                ASSERT_TRUE(partition);
            } else {
                ASSERT_EQ(seg->GetPartitionId(), partition->GetID());
            }
            context.stale_segments.push_back(seg);
            if (!context.prev_partition) {
                context.prev_partition = latest_ss->GetResource<Partition>(
                        seg->GetPartitionId());
            }
        }

        context.lsn = next_lsn();
        auto op = std::make_shared<MergeOperation>(context, latest_ss);
        SegmentPtr new_seg;
        status = op->CommitNewSegment(new_seg);
        ASSERT_TRUE(status.ok());
        sf_context.segment_id = new_seg->GetID();
        SegmentFilePtr seg_file;
        status = op->CommitNewSegmentFile(sf_context, seg_file);
        ASSERT_TRUE(status.ok());
        std::unique_lock<std::mutex> lock(merge_mtx);
        status = op->Push();
        if (!status.ok()) {
            lock.unlock();
            std::unique_lock<std::mutex> blk(built_mtx);
            std::cout << status.ToString() << std::endl;
            bool stale_found = false;
            for (auto& seg_id : seg_ids) {
                auto it = built_segs.find(seg_id);
                if (it != built_segs.end()) {
                    stale_found = true;
                    break;
                }
            }
            ASSERT_TRUE(stale_found);
            return;
        }
        ID_TYPE ss_id = latest_ss->GetID();
        status = op->GetSnapshot(latest_ss);
        ASSERT_TRUE(status.ok());
        ASSERT_TRUE(latest_ss->GetID() > ss_id);

        merged_segs_history[new_seg->GetID()] = seg_ids;
        for (auto& seg_id : seg_ids) {
            merged_segs.insert(seg_id);
        }
        new_seg_id = new_seg->GetID();
        ASSERT_EQ(new_seg->GetPartitionId(), partition->GetID());
    };

    // TODO: If any Compound Operation find larger Snapshot. This Operation should be rollback to latest
    auto handler_worker = [&] {
        auto loop_cnt = RandomInt(10, 20);
        ScopedSnapshotT latest_ss;

        auto create_new_segment = [&]() {
            ID_TYPE partition_id;
            {
                std::unique_lock<std::mutex> lock(partition_mtx);
                auto idx = RandomInt(0, partitions.size() - 1);
                partition_id = partitions[idx];
            }
            Snapshots::GetInstance().GetSnapshot(latest_ss, collection_name);
            OperationContext context;
            context.prev_partition = latest_ss->GetResource<Partition>(partition_id);
            context.lsn = next_lsn();
            auto op = std::make_shared<NewSegmentOperation>(context, latest_ss);
            SegmentPtr new_seg;
            auto status = op->CommitNewSegment(new_seg);
            if (!status.ok()) {
                std::cout << status.ToString() << std::endl;
            }
            ASSERT_TRUE(status.ok());
            SegmentFilePtr seg_file;
            sf_context.segment_id = new_seg->GetID();
            op->CommitNewSegmentFile(sf_context, seg_file);
            op->Push();
            status = op->GetSnapshot(latest_ss);
            ASSERT_TRUE(status.ok());

            {
                std::unique_lock<std::mutex> lock(all_mtx);
                all_segments.insert(new_seg->GetID());
            }
            if (RandomInt(0, 10) >= 7) {
                build_queue.Put(new_seg->GetID());
            }
            merge_queue.Put(new_seg->GetID());
        };

        auto create_partition = [&]() {
            std::stringstream ss;
            ss << "fake_partition_" << next_pid();
            PartitionContext context;
            context.name = ss.str();
            std::unique_lock<std::mutex> lock(partition_mtx);
            auto latest_ss = CreatePartition(collection_name, context, next_lsn());
            ASSERT_TRUE(latest_ss);
            auto partition = latest_ss->GetPartition(ss.str());
            partitions.push_back(partition->GetID());
            if (latest_ss->NumberOfPartitions() != partitions.size()) {
                for (auto& pid : partitions) {
                    std::cout << "PartitionId=" << pid << std::endl;
                }
            }
            ASSERT_EQ(latest_ss->NumberOfPartitions(), partitions.size());
        };

        for (auto i = 0; i < loop_cnt; ++i) {
            if (RandomInt(0, 10) > 7) {
                create_partition();
            }
            create_new_segment();
        }
    };

    std::map<ID_TYPE, std::set<ID_TYPE>> merge_segs;
    auto merge_worker = [&] {
        while (true) {
            auto seg_id = merge_queue.Take();
            if (seg_id == 0) {
                std::cout << "Exiting Merge Worker" << std::endl;
                break;
            }
            ScopedSnapshotT latest_ss;
            auto status = Snapshots::GetInstance().GetSnapshot(latest_ss, collection_name);
            ASSERT_TRUE(status.ok());
            auto seg = latest_ss->GetResource<Segment>(seg_id);
            if (!seg) {
                std::cout << "SegID=" << seg_id << std::endl;
                std::cout << latest_ss->ToString() << std::endl;
                ASSERT_TRUE(seg);
            }

            auto it_segs = merge_segs.find(seg->GetPartitionId());
            if (it_segs == merge_segs.end()) {
                merge_segs[seg->GetPartitionId()] = {seg->GetID()};
            } else {
                merge_segs[seg->GetPartitionId()].insert(seg->GetID());
            }
            auto& segs = merge_segs[seg->GetPartitionId()];
            if ((segs.size() >= 2) && (RandomInt(0, 10) >= 2)) {
                std::cout << "Merging partition " << seg->GetPartitionId() << " segs (";
                for (auto seg : segs) {
                    std::cout << seg << ",";
                }
                std::cout << ")" << std::endl;
                ID_TYPE new_seg_id = 0;
                do_merge(segs, new_seg_id);
                segs.clear();
                if (new_seg_id == 0) {
                    continue;
                }
                if (RandomInt(0, 10) >= 6) {
                    build_queue.Put(new_seg_id);
                }

            } else {
                continue;
            }
        }
        merge_waiter.Notify();
        build_queue.Put(0);
        build_waiter.Wait();
    };

    auto build_worker = [&] {
        while (true) {
            auto seg_id = build_queue.Take();
            if (seg_id == 0) {
                std::cout << "Exiting Build Worker" << std::endl;
                break;
            }

            std::cout << "Building " << seg_id << std::endl;
            do_build(seg_id);
        }
        build_waiter.Notify();
    };
    std::vector<std::thread> handlers;
    auto num_handlers = RandomInt(8, 9);
    for (auto i = 0; i < num_handlers; ++i) {
        handlers.emplace_back(handler_worker);
    }
    std::thread t3 = std::thread(merge_worker);
    std::thread t4 = std::thread(build_worker);

    for (auto& handler : handlers) {
        handler.join();
    }

    merge_queue.Put(0);
    t3.join();
    t4.join();

    /* for (auto& kv : merged_segs_history) { */
    /*     std::cout << "merged: ("; */
    /*     for (auto i : kv.second) { */
    /*         std::cout << i << ","; */
    /*     } */
    /*     std::cout << ") -> " << kv.first << std::endl; */
    /* } */

    /* for (auto& id : built_segs) { */
    /*     std::cout << "built: " << id << std::endl; */
    /* } */

    merge_waiter.Wait();

    ScopedSnapshotT latest_ss;
    status = Snapshots::GetInstance().GetSnapshot(latest_ss, collection_name);
    ASSERT_TRUE(status.ok());
    auto expect_segments = all_segments;
    for (auto& kv : merged_segs_history) {
        expect_segments.insert(kv.first);
        for (auto& id : kv.second) {
            expect_segments.erase(id);
        }
    }
    std::set<ID_TYPE> final_segments;
    auto segments = latest_ss->GetResources<Segment>();
    for (auto& kv : segments) {
        final_segments.insert(kv.first);
    }
    ASSERT_EQ(final_segments, expect_segments);

    auto final_segment_file_cnt = latest_ss->GetResources<SegmentFile>().size();

    size_t expect_segment_file_cnt;
    expect_segment_file_cnt = expect_segments.size();
    expect_segment_file_cnt += built_segs.size();
    std::cout << latest_ss->ToString() << std::endl;
    std::vector<int> common_ids;
    std::set_intersection(merged_segs.begin(), merged_segs.end(), built_segs.begin(), built_segs.end(),
            std::back_inserter(common_ids));
    expect_segment_file_cnt -= common_ids.size();
    ASSERT_EQ(expect_segment_file_cnt, final_segment_file_cnt);
}


TEST_F(SnapshotTest, CompoundTest2) {
    Status status;
    LSN_TYPE lsn = 0;
    auto next_lsn = [&]() -> LSN_TYPE& {
        return ++lsn;
    };
    std::atomic<LSN_TYPE> pid = 0;
    auto next_pid = [&]() -> LSN_TYPE {
        return ++pid;
    };
    std::string collection_name("c1");
    auto ss = CreateCollection(collection_name, next_lsn());
    ASSERT_TRUE(ss);
    ASSERT_EQ(lsn, ss->GetMaxLsn());

    TQueue merge_queue;
    TQueue build_queue;

    std::set<ID_TYPE> all_segments;
    std::set<ID_TYPE> segment_in_building;
    std::map<ID_TYPE, std::set<ID_TYPE>> merged_segs_history;
    std::set<ID_TYPE> merged_segs;
    std::set<ID_TYPE> built_segs;
    std::set<ID_TYPE> build_stale_segs;

    std::mutex all_mtx;
    std::mutex merge_mtx;
    std::mutex built_mtx;
    std::mutex partition_mtx;
    std::mutex seg_p_mtx;

    WaitableObj merge_waiter;
    WaitableObj build_waiter;

    SegmentFileContext sf_context;
    sf_context.field_name = "vector";
    sf_context.field_element_name = "ivfsq8";
    sf_context.segment_id = 1;
    sf_context.partition_id = 1;

    IDS_TYPE partitions = {ss->GetResources<Partition>().begin()->second->GetID()};
    std::set<ID_TYPE> stale_partitions;
    std::map<ID_TYPE, ID_TYPE> seg_p_map;

    auto do_build = [&] (const ID_TYPE& seg_id, const ID_TYPE& p_id) {
        ScopedSnapshotT latest_ss;
        auto status = Snapshots::GetInstance().GetSnapshot(latest_ss, collection_name);
        ASSERT_TRUE(status.ok());

        auto build_sf_context = sf_context;

        OperationContext context;
        context.lsn = next_lsn();
        auto build_op = std::make_shared<ChangeSegmentFileOperation>(context, latest_ss);
        SegmentFilePtr seg_file;
        build_sf_context.segment_id = seg_id;
        status = build_op->CommitNewSegmentFile(build_sf_context, seg_file);
        if (!status.ok()) {
            std::cout << status.ToString() << std::endl;
            {
                std::unique_lock<std::mutex> lock(partition_mtx);
                auto it = stale_partitions.find(p_id);
                if (it != stale_partitions.end()) {
                    std::cout << "Segment " << seg_id << " host partition " << p_id << " is stale" << std::endl;
                    return;
                }
            }
            std::unique_lock<std::mutex> lock(merge_mtx);
            auto it = merged_segs.find(seg_id);
            ASSERT_NE(it, merged_segs.end());
            return;
        }
        std::unique_lock<std::mutex> lock(built_mtx);
        status = build_op->Push();
        if (!status.ok()) {
            lock.unlock();
            std::cout << status.ToString() << std::endl;
            {
                std::unique_lock<std::mutex> lock(partition_mtx);
                auto it = stale_partitions.find(p_id);
                if (it != stale_partitions.end()) {
                    std::cout << "Segment " << seg_id << " host partition " << p_id << " is stale" << std::endl;
                    return;
                }
            }
            std::unique_lock<std::mutex> lock(merge_mtx);
            auto it = merged_segs.find(seg_id);
            ASSERT_NE(it, merged_segs.end());
            return;
        }
        ASSERT_TRUE(status.ok());
        status = build_op->GetSnapshot(latest_ss);
        ASSERT_TRUE(status.ok());

        built_segs.insert(seg_id);
    };

    auto do_merge = [&] (std::set<ID_TYPE>& seg_ids, ID_TYPE& new_seg_id, const ID_TYPE& p_id) {
        if (seg_ids.size() == 0) {
            return;
        }
        ScopedSnapshotT latest_ss;
        auto status = Snapshots::GetInstance().GetSnapshot(latest_ss, collection_name);
        ASSERT_TRUE(status.ok());

        PartitionPtr partition;
        OperationContext context;
        for (auto& id : seg_ids) {
            auto seg = latest_ss->GetResource<Segment>(id);
            if (!seg) {
                std::cout << "Stale seg=" << id << std::endl;
                std::unique_lock<std::mutex> lock(partition_mtx);
                std::cout << ((stale_partitions.find(p_id) != stale_partitions.end()) ? " due stale partition"
                        : " unexpected") << std::endl;
                ASSERT_TRUE(stale_partitions.find(p_id) != stale_partitions.end());
                return;
            }
            if (!partition) {
                partition = latest_ss->GetResource<Partition>(seg->GetPartitionId());
                ASSERT_TRUE(partition);
            } else {
                ASSERT_EQ(seg->GetPartitionId(), partition->GetID());
            }
            context.stale_segments.push_back(seg);
            if (!context.prev_partition) {
                context.prev_partition = latest_ss->GetResource<Partition>(
                        seg->GetPartitionId());
            }
        }

        context.lsn = next_lsn();
        auto op = std::make_shared<MergeOperation>(context, latest_ss);
        SegmentPtr new_seg;
        status = op->CommitNewSegment(new_seg);
        ASSERT_TRUE(status.ok());
        sf_context.segment_id = new_seg->GetID();
        SegmentFilePtr seg_file;
        status = op->CommitNewSegmentFile(sf_context, seg_file);
        ASSERT_TRUE(status.ok());
        std::unique_lock<std::mutex> lock(merge_mtx);
        status = op->Push();
        if (!status.ok()) {
            lock.unlock();
            bool stale_found = false;
            {
                std::unique_lock<std::mutex> blk(built_mtx);
                std::cout << status.ToString() << std::endl;
                /* for (auto id : built_segs) { */
                /*     std::cout << "builted " << id << std::endl; */
                /* } */
                /* for (auto id : seg_ids) { */
                /*     std::cout << "to_merge " << id << std::endl; */
                /* } */
                for (auto& seg_id : seg_ids) {
                    auto it = built_segs.find(seg_id);
                    if (it != built_segs.end()) {
                        stale_found = true;
                        break;
                    }
                }
            }
            if (!stale_found) {
                std::unique_lock<std::mutex> lock(partition_mtx);
                auto it = stale_partitions.find(p_id);
                if (it != stale_partitions.end()) {
                    /* std::cout << "Segment " << seg_id << " host partition " << p_id << " is stale" << std::endl; */
                    stale_found = true;
                }
            }

            ASSERT_TRUE(stale_found);
            return;
        }
        ID_TYPE ss_id = latest_ss->GetID();
        status = op->GetSnapshot(latest_ss);
        ASSERT_TRUE(status.ok());
        ASSERT_TRUE(latest_ss->GetID() > ss_id);

        merged_segs_history[new_seg->GetID()] = seg_ids;
        for (auto& seg_id : seg_ids) {
            merged_segs.insert(seg_id);
        }
        new_seg_id = new_seg->GetID();
        seg_p_map[new_seg_id] = new_seg->GetPartitionId();
        ASSERT_EQ(new_seg->GetPartitionId(), partition->GetID());
    };

    auto handler_worker = [&] {
        auto loop_cnt = RandomInt(30, 35);
        ScopedSnapshotT latest_ss;

        auto create_new_segment = [&]() {
            ID_TYPE partition_id;
            {
                std::unique_lock<std::mutex> lock(partition_mtx);
                auto idx = RandomInt(0, partitions.size() - 1);
                partition_id = partitions[idx];
            }
            Snapshots::GetInstance().GetSnapshot(latest_ss, collection_name);
            OperationContext context;
            context.prev_partition = latest_ss->GetResource<Partition>(partition_id);
            context.lsn = next_lsn();
            auto op = std::make_shared<NewSegmentOperation>(context, latest_ss);
            SegmentPtr new_seg;
            auto status = op->CommitNewSegment(new_seg);
            if (!status.ok()) {
                std::cout << status.ToString() << std::endl;
                std::unique_lock<std::mutex> lock(partition_mtx);
                ASSERT_TRUE(stale_partitions.find(partition_id) != stale_partitions.end());
                return;
            }
            ASSERT_TRUE(status.ok());
            SegmentFilePtr seg_file;
            sf_context.segment_id = new_seg->GetID();
            status = op->CommitNewSegmentFile(sf_context, seg_file);
            if (!status.ok()) {
                std::cout << status.ToString() << std::endl;
                ASSERT_TRUE(status.ok());
            }
            status = op->Push();
            if (!status.ok()) {
                std::unique_lock<std::mutex> lock(partition_mtx);
                /* if (stale_partitions.find(partition_id) == stale_partitions.end()) { */
                /*     for (auto p : stale_partitions) { */
                /*         std::cout << "stale p: " << p << std::endl; */
                /*     } */
                /*     ASSERT_TRUE(false); */
                /* } */
                ASSERT_TRUE(stale_partitions.find(partition_id) != stale_partitions.end());
                return;
            }
            status = op->GetSnapshot(latest_ss);
            if (!status.ok()) {
                std::cout << status.ToString() << std::endl;
                std::unique_lock<std::mutex> lock(partition_mtx);
                ASSERT_TRUE(stale_partitions.find(partition_id) != stale_partitions.end());
                return;
            }

            {
                std::unique_lock<std::mutex> lock(all_mtx);
                all_segments.insert(new_seg->GetID());
            }
            if (RandomInt(0, 10) >= 7) {
                build_queue.Put({new_seg->GetID(), new_seg->GetPartitionId()});
            }
            seg_p_map[new_seg->GetID()] = new_seg->GetPartitionId();
            merge_queue.Put({new_seg->GetID(), new_seg->GetPartitionId()});
        };

        auto create_partition = [&]() {
            std::stringstream ss;
            ss << "fake_partition_" << next_pid();
            PartitionContext context;
            context.name = ss.str();
            std::unique_lock<std::mutex> lock(partition_mtx);
            auto latest_ss = CreatePartition(collection_name, context, next_lsn());
            ASSERT_TRUE(latest_ss);
            auto partition = latest_ss->GetPartition(ss.str());
            partitions.push_back(partition->GetID());
            if (latest_ss->NumberOfPartitions() != partitions.size()) {
                for (auto& pid : partitions) {
                    std::cout << "PartitionId=" << pid << std::endl;
                }
            }
            ASSERT_EQ(latest_ss->NumberOfPartitions(), partitions.size());
        };

        auto drop_partition = [&]() {
            std::unique_lock<std::mutex> lock(partition_mtx);
            if (partitions.size() <= 2) {
                return;
            }
            ScopedSnapshotT latest_ss;
            Snapshots::GetInstance().GetSnapshot(latest_ss, collection_name);
            auto index = RandomInt(0, partitions.size() - 1);
            auto pid = partitions[index];
            std::cout << "Dropping partition " << pid << std::endl;
            status = Snapshots::GetInstance().DropPartition(
                    latest_ss->GetCollectionId(), pid, next_lsn());
            ASSERT_TRUE(status.ok());
            stale_partitions.insert(pid);
            partitions.erase(partitions.begin() + index);
        };

        for (auto i = 0; i < loop_cnt; ++i) {
            if (RandomInt(0, 10) > 5) {
                create_partition();
            }
            if (RandomInt(0, 10) > 7) {
                drop_partition();
            }
            create_new_segment();
        }
    };

    std::map<ID_TYPE, std::set<ID_TYPE>> merge_segs;
    auto merge_worker = [&] {
        while (true) {
            auto ids = merge_queue.Take();
            auto seg_id = std::get<0>(ids);
            auto p_id = std::get<1>(ids);
            if (seg_id == 0) {
                std::cout << "Exiting Merge Worker" << std::endl;
                break;
            }
            ScopedSnapshotT latest_ss;
            auto status = Snapshots::GetInstance().GetSnapshot(latest_ss, collection_name);
            ASSERT_TRUE(status.ok());
            auto seg = latest_ss->GetResource<Segment>(seg_id);
            if (!seg) {
                std::cout << "SegID=" << seg_id << std::endl;
                std::cout << latest_ss->ToString() << std::endl;
                std::unique_lock<std::mutex> lock(partition_mtx);
                auto it = stale_partitions.find(p_id);
                if (it != stale_partitions.end()) {
                    std::cout << "Segment " << seg_id << " host partition " << p_id << " is stale" << std::endl;
                    continue;
                }
                ASSERT_TRUE(seg);
            }

            auto it_segs = merge_segs.find(seg->GetPartitionId());
            if (it_segs == merge_segs.end()) {
                merge_segs[seg->GetPartitionId()] = {seg->GetID()};
            } else {
                merge_segs[seg->GetPartitionId()].insert(seg->GetID());
            }
            auto& segs = merge_segs[seg->GetPartitionId()];
            if ((segs.size() >= 2) && (RandomInt(0, 10) >= 2)) {
                std::cout << "Merging partition " << seg->GetPartitionId() << " segs (";
                for (auto seg : segs) {
                    std::cout << seg << ",";
                }
                std::cout << ")" << std::endl;
                ID_TYPE new_seg_id = 0;
                ID_TYPE partition_id = 0;
                do_merge(segs, new_seg_id, p_id);
                segs.clear();
                if (new_seg_id == 0) {
                    continue;
                }
                if (RandomInt(0, 10) >= 6) {
                    build_queue.Put({new_seg_id, p_id});
                }

            } else {
                continue;
            }
        }
        merge_waiter.Notify();
        build_queue.Put({0, 0});
        build_waiter.Wait();
    };

    auto build_worker = [&] {
        while (true) {
            auto ids = build_queue.Take();
            auto seg_id = std::get<0>(ids);
            auto p_id = std::get<1>(ids);
            if (seg_id == 0) {
                std::cout << "Exiting Build Worker" << std::endl;
                break;
            }

            std::cout << "Building " << seg_id << std::endl;
            do_build(seg_id, p_id);
        }
        build_waiter.Notify();
    };
    std::vector<std::thread> handlers;
    auto num_handlers = RandomInt(6, 8);
    for (auto i = 0; i < num_handlers; ++i) {
        handlers.emplace_back(handler_worker);
    }
    std::thread t3 = std::thread(merge_worker);
    std::thread t4 = std::thread(build_worker);

    for (auto& handler : handlers) {
        handler.join();
    }

    merge_queue.Put({0, 0});
    t3.join();
    t4.join();

    /* for (auto& kv : merged_segs_history) { */
    /*     std::cout << "merged: ("; */
    /*     for (auto i : kv.second) { */
    /*         std::cout << i << ","; */
    /*     } */
    /*     std::cout << ") -> " << kv.first << std::endl; */
    /* } */

    /* for (auto& id : built_segs) { */
    /*     std::cout << "built: " << id << std::endl; */
    /* } */

    merge_waiter.Wait();

    ScopedSnapshotT latest_ss;
    status = Snapshots::GetInstance().GetSnapshot(latest_ss, collection_name);
    ASSERT_TRUE(status.ok());

    auto expect_segments = all_segments;
    for (auto& kv : merged_segs_history) {
        expect_segments.insert(kv.first);
        for (auto& id : kv.second) {
            expect_segments.erase(id);
        }
    }

    for (auto& seg_p : seg_p_map) {
        auto it = stale_partitions.find(seg_p.second);
        if (it == stale_partitions.end()) {
            continue;
        }
        expect_segments.erase(seg_p.first);
    }

    std::set<ID_TYPE> final_segments;
    auto segments = latest_ss->GetResources<Segment>();
    for (auto& kv : segments) {
        final_segments.insert(kv.first);
    }
    ASSERT_EQ(final_segments, expect_segments);
    // TODO: Check Total Segment Files Cnt
}

TEST_F(SnapshotTest, MultiSegmentsTest) {
    const int64_t loop = 10;
    std::string collection_name("c1");
    LSN_TYPE lsn = 1;
    auto ss = CreateCollection(collection_name, ++lsn);
    ASSERT_TRUE(ss);
    ASSERT_EQ(ss->GetName(), collection_name);
    ASSERT_EQ(ss->NumberOfPartitions(), 1);

    Partition::VecT partitions;
    auto start_ss = ss;
    for (size_t i = 0; i < loop; i++) {
        OperationContext context;
        context.lsn = ++lsn;
        auto op = std::make_shared<CreatePartitionOperation>(context, start_ss);
        std::string partition_name = "p_" + std::to_string(i);
        PartitionContext p_ctx;
        p_ctx.name = partition_name;
        PartitionPtr partition;
        auto status = op->CommitNewPartition(p_ctx, partition);
        ASSERT_TRUE(status.ok());
        ASSERT_TRUE(partition);
        ASSERT_EQ(partition->GetName(), partition_name);
        ASSERT_FALSE(partition->IsActive());
        ASSERT_TRUE(partition->HasAssigned());

        ASSERT_TRUE(op->Push().ok());
        ASSERT_TRUE(op->GetSnapshot(start_ss).ok());
        ASSERT_TRUE(start_ss);

        partitions.push_back(partition);
    }

    srand(time(nullptr));
    auto r = rand() % 10;
    for (size_t i = 0; i < r; i ++) {
        PartitionContext context;
        context.id = partitions[i]->GetID();
        context.name = partitions[i]->GetName();
        context.lsn = ++lsn;
        auto op = std::make_shared<DropPartitionOperation>(context, start_ss);
        ASSERT_TRUE(op->Push().ok());
        ASSERT_TRUE(op->GetSnapshot(start_ss).ok());
    }

    auto multi_segments_task = [&]() {
        OperationContext context;
        auto op = std::make_shared<MultiSegmentsOperation>(context, start_ss);
        for (size_t i = 0; i < loop; i++) {
            OperationContext oc;
            oc.prev_partition = partitions[i];
            Segment::Ptr segment;
            auto status = op->CommitNewSegment(oc, segment);
            if (status.code() == milvus::SS_STALE_ERROR) {
                continue;
            }

            ASSERT_TRUE(status.ok()) << status.ToString();

            SegmentFileContext sfc;
            sfc.field_name = "vector";
            sfc.field_element_name = "ivfsq8";
            sfc.segment_id = segment->GetID();
            SegmentFile::Ptr sf;
            status = op->CommitNewSegmentFile(sfc, sf);
            ASSERT_TRUE(status.ok()) << status.ToString();

            status = op->CommitRowCount(segment->GetID(), 100);
            ASSERT_TRUE(status.ok()) << status.ToString();
        }

        auto status = op->Push();
        ASSERT_TRUE(status.ok()) << status.ToString();
    };

    auto multi_segments_thread = std::thread(multi_segments_task);
    multi_segments_thread.join();

    ScopedSnapshotT new_ss;
    auto status = Snapshots::GetInstance().GetSnapshot(new_ss, collection_name);
    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_EQ(new_ss->GetCollectionCommit()->GetRowCount(), (loop - r) * 100);
}

struct GCSchedule {
    // static constexpr const char* Name = "GCSchedule";
};

struct FlushSchedule {
    // static constexpr const char* Name = "FlushSchedule";
};

using IEventHandler = milvus::engine::snapshot::IEventHandler;
/* struct SampleHandler : public IEventHandler { */
/*     static constexpr const char* EventName = "SampleHandler"; */
/*     const char* */
/*     GetEventName() const override { */
/*         return EventName; */
/*     } */
/* }; */

REGISTER_HANDLER(GCSchedule, IEventHandler);
/* REGISTER_HANDLER(GCSchedule, SampleHandler); */
REGISTER_HANDLER(FlushSchedule, IEventHandler);
/* REGISTER_HANDLER(FlushSchedule, SampleHandler); */

using GCScheduleFactory = milvus::engine::snapshot::HandlerFactory<GCSchedule>;
using FlushScheduleFactory = milvus::engine::snapshot::HandlerFactory<GCSchedule>;

TEST_F(SnapshotTest, RegistryTest) {
    {
        auto& factory = GCScheduleFactory::GetInstance();
        auto ihandler = factory.GetHandler(IEventHandler::EventName);
        ASSERT_TRUE(ihandler);
        /* auto sihandler = factory.GetHandler(SampleHandler::EventName); */
        /* ASSERT_TRUE(sihandler); */
        /* ASSERT_EQ(SampleHandler::EventName, sihandler->GetEventName()); */
    }
    {
        /* auto& factory = FlushScheduleFactory::GetInstance(); */
        /* auto ihandler = factory.GetHandler(IEventHandler::EventName); */
        /* ASSERT_TRUE(ihandler); */
        /* auto sihandler = factory.GetHandler(SampleHandler::EventName); */
        /* ASSERT_TRUE(sihandler); */
        /* ASSERT_EQ(SampleHandler::EventName, sihandler->GetEventName()); */
    }
}
