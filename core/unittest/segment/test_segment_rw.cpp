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

#include <gtest/gtest.h>
//#include <fiu-local.h>
//#include <fiu-control.h>

#include "db/snapshot/Snapshots.h"
#include "segment/SegmentReader.h"
#include "segment/SegmentWriter.h"
//#include "easyloggingpp/easylogging++.h"
#include "segment/utils.h"
#include "knowhere/index/vector_index/IndexIVF.h"
#include "knowhere/index/vector_index/IndexIVFSQ.h"
#include "knowhere/index/IndexType.h"
#include "index/unittest/Helper.h"
#include "index/unittest/utils.h"

//INITIALIZE_EASYLOGGINGPP

class SegmentIndexTest : public DataGen, public SegmentTest {
 protected:
    void
    SetUp() override {
        SegmentTest::SetUp();
        Generate(DIM, NB, NQ);
    }

    void
    TearDown() override {
        SegmentTest::TearDown();
    }

 protected:
    milvus::knowhere::IndexType index_type_;
    milvus::knowhere::IndexMode index_mode_;
    milvus::knowhere::IVFPtr index_;
};

TEST_F(SegmentIndexTest, SEGMENT_VECTOR_INDEX_RW_TEST) {
    const std::string segment_dir = "/tmp";
    const std::string collection_name = "test_cc1234";
    const int64_t segment_id = 1234;

    const std::string index_location = "/tmp/test_index";

    {
        milvus::engine::snapshot::ScopedSnapshotT ss;
        ASSERT_TRUE(milvus::engine::snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_name).ok());
        ASSERT_TRUE(ss);
        ASSERT_EQ(ss->GetName(), collection_name);
        auto write_visitor = milvus::engine::SegmentVisitor::Build(ss, segment_id);
        ASSERT_TRUE(write_visitor);
        milvus::segment::SegmentWriter segment_writer(segment_dir, write_visitor);

        /* test to write vector index */
        index_type_ = milvus::knowhere::IndexEnum::INDEX_FAISS_IVFSQ8;
        index_mode_ = milvus::knowhere::IndexMode::MODE_CPU;
        index_ = IndexFactory(index_type_, index_mode_);
        ASSERT_TRUE(index_ != nullptr);
        auto conf = ParamGenerator::GetInstance().Gen(index_type_);
        index_->Train(base_dataset, conf);
        index_->Add(base_dataset, conf);
        EXPECT_EQ(index_->Count(), nb);
        EXPECT_EQ(index_->Dim(), dim);

        segment_writer.SetVectorIndex(index_location, index_);
        ASSERT_TRUE(segment_writer.WriteVectorIndex(index_location).ok());
    }

    {
        milvus::engine::snapshot::ScopedSnapshotT ss;
        ASSERT_TRUE(milvus::engine::snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_name).ok());
        ASSERT_TRUE(ss);
        auto read_visitor = milvus::engine::SegmentVisitor::Build(ss, segment_id);
        ASSERT_TRUE(read_visitor);
        milvus::segment::SegmentReader segment_reader(segment_dir, read_visitor);

        /* test to read vector index */
        milvus::knowhere::VecIndexPtr index_ptr;
        ASSERT_TRUE(segment_reader.LoadVectorIndex(index_location, index_ptr).ok());
    }
}

TEST_F(SegmentTest, SEGMENT_DELETEDDOCS_RW_TEST) {
    const std::string segment_dir = "/tmp";
    const std::string collection_name = "test_cc4321";
    const int64_t segment_id = 4321;

    const std::string del_docs_location = "/tmp/del_docs";
    const std::vector<milvus::engine::offset_t> input_data{1, 2, 3};

    {
        milvus::engine::snapshot::ScopedSnapshotT ss;
        ASSERT_TRUE(milvus::engine::snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_name).ok());
        auto write_visitor = milvus::engine::SegmentVisitor::Build(ss, segment_id);
        ASSERT_TRUE(write_visitor != nullptr);
        milvus::segment::SegmentWriter segment_writer(segment_dir, write_visitor);

        /* test to write deleted docs */
        milvus::segment::DeletedDocsPtr deleted_docs_ptr = std::make_shared<milvus::segment::DeletedDocs>();
        deleted_docs_ptr->AddDeletedDoc(input_data.at(0));
        ASSERT_TRUE(segment_writer.WriteDeletedDocs(del_docs_location, deleted_docs_ptr).ok());
        deleted_docs_ptr = std::make_shared<milvus::segment::DeletedDocs>();
        deleted_docs_ptr->AddDeletedDoc(input_data.at(1));
        deleted_docs_ptr->AddDeletedDoc(input_data.at(2));
        ASSERT_TRUE(segment_writer.WriteDeletedDocs(del_docs_location, deleted_docs_ptr).ok());  // write twice so as to test Move()
    }

    {
        milvus::engine::snapshot::ScopedSnapshotT ss;
        ASSERT_TRUE(milvus::engine::snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_name).ok());
        auto read_visitor = milvus::engine::SegmentVisitor::Build(ss, segment_id);
        ASSERT_TRUE(read_visitor != nullptr);
        milvus::segment::SegmentReader segment_reader(segment_dir, read_visitor);

        /* test to read deleted docs */
        milvus::segment::DeletedDocsPtr deleted_docs_ptr;
        ASSERT_TRUE(segment_reader.LoadDeletedDocs(deleted_docs_ptr).ok());
        size_t deleted_docs_size;
        ASSERT_TRUE(segment_reader.ReadDeletedDocsSize(deleted_docs_size).ok());

        const auto& deleted_docs_vec = deleted_docs_ptr->GetDeletedDocs();

        EXPECT_EQ(deleted_docs_size, input_data.size());
        EXPECT_EQ(deleted_docs_vec.size(), deleted_docs_size);
        EXPECT_EQ(deleted_docs_vec.at(0), input_data.at(0));
        EXPECT_EQ(deleted_docs_vec.at(1), input_data.at(1));
        EXPECT_EQ(deleted_docs_vec.at(2), input_data.at(2));
    }
}

//TEST_F(SegmentTest, SEGMENT_VECTOR_RW_TEST) {
//    const std::string segment_dir = "/tmp";
//
//    const std::string vector_name = "test_vector";
//    const std::vector<uint8_t> vector_data{0, 1};
//    const std::vector<milvus::segment::doc_id_t> vector_uids{1234, 5678};
//
//    {
//        milvus::segment::SegmentWriter segment_writer(segment_dir);
//
//        /* test to write vector */
//        ASSERT_TRUE(segment_writer.AddVectors(vector_name, vector_data, vector_uids).ok());
//        ASSERT_TRUE(segment_writer.WriteVectors().ok());
//    }
//
//    {
//        milvus::segment::SegmentReader segment_reader(segment_dir);
//
//        /* test to read vector */
//        off_t offset = 0;
//        size_t num_bytes = vector_data.size() * sizeof(uint8_t);
//        std::vector<uint8_t> raw_vector;
//        ASSERT_TRUE(segment_reader.LoadVectors(offset, num_bytes, raw_vector).ok());
//        std::vector<milvus::segment::doc_id_t> raw_uids;
//        ASSERT_TRUE(segment_reader.LoadUids(raw_uids).ok());
//
//        EXPECT_EQ(raw_vector.size(), vector_data.size());
//        EXPECT_EQ(raw_vector.at(0), vector_data.at(0));
//        EXPECT_EQ(raw_vector.at(1), vector_data.at(1));
//        EXPECT_EQ(raw_uids.size(), vector_uids.size());
//        EXPECT_EQ(raw_uids.at(0), vector_uids.at(0));
//        EXPECT_EQ(raw_uids.at(1), vector_uids.at(1));
//    }
//}

//TEST_F(SegmentTest, SEGMENT_ATTR_RW_TEST) {
//    /* not realize read / write attr with S3, so please set s3_enable option to be false when testing this unit */
//
//    const std::string segment_dir = "/tmp";
//
//    const std::string attr_name = "test_partition";
//    const std::unordered_map<std::string, std::vector<uint8_t>> attr_data{{"test_partition", {0, 1}}};
//    const std::unordered_map<std::string, uint64_t> attr_nbytes{
//        {"test_partition", attr_data.at("test_partition").size() * sizeof(uint8_t)}};
//    const std::vector<milvus::segment::doc_id_t> attr_uids{4321, 8765};
//
//    {
//        milvus::segment::SegmentWriter segment_writer(segment_dir);
//
//        /* test to write attr */
//        ASSERT_TRUE(segment_writer.AddAttrs(attr_name, attr_nbytes, attr_data, attr_uids).ok());
//        ASSERT_TRUE(segment_writer.WriteAttrs().ok());
//    }
//
//    {
//        milvus::segment::SegmentReader segment_reader(segment_dir);
//
//        /* test to read attr */
//        // segment reader does not realize LoadAttrs() yet, waiting for a better solution
//        ASSERT_TRUE(segment_reader.Load().ok());
//    }
//}

//TEST_F(SegmentTest, SEGMENT_BLOOMFILTER_RW_TEST) {
//    /* not realize bloomfilter with S3, so please set s3_enable option to be false when testing this unit */
//
//    const std::string segment_dir = "/tmp";
//
//    {
//        milvus::segment::SegmentWriter segment_writer(segment_dir);
//
//        /* test to write bloomfilter */
//        ASSERT_TRUE(segment_writer.WriteBloomFilter().ok());  // create and write an empty bloomfilter
//    }
//
//    {
//        milvus::segment::SegmentReader segment_reader(segment_dir);
//
//        /* test to read bloomfilter */
//        milvus::segment::IdBloomFilterPtr id_bloom_filter_ptr;
//        ASSERT_TRUE(segment_reader.LoadBloomFilter(id_bloom_filter_ptr).ok());
//    }
//}

//TEST_F(SegmentTest, SEGMENT_SERIALIZE_TEST) {
//    /* not realize attr and bloomfilter with S3, so please set s3_enable option to be false when testing this unit */
//
//    const std::string segment_dir = "/tmp";
//
//    const std::string vector_name = "test_vector_222";
//    const std::vector<uint8_t> vector_data{0, 1};
//    const std::vector<milvus::segment::doc_id_t> vector_uids{1234, 5678};
//
//    const std::string attr_name = "test_partition_222";
//    const std::unordered_map<std::string, std::vector<uint8_t>> attr_data{{"test_partition_222", {0, 1}}};
//    const std::unordered_map<std::string, uint64_t> attr_nbytes{
//        {"test_partition_222", attr_data.at("test_partition_222").size() * sizeof(uint8_t)}};
//    const std::vector<milvus::segment::doc_id_t> attr_uids{4321, 8765};
//
//    {
//        milvus::segment::SegmentWriter segment_writer(segment_dir);
//
//        ASSERT_TRUE(segment_writer.AddVectors(vector_name, vector_data, vector_uids).ok());
//        ASSERT_TRUE(segment_writer.AddAttrs(attr_name, attr_nbytes, attr_data, attr_uids).ok());
//        ASSERT_TRUE(segment_writer.Serialize().ok());
//    }
//}

//TEST_F(SegmentTest, SEGMENT_MERGE_TEST) {
//    /* not realize attr and bloomfilter with S3, so please set s3_enable option to be false when testing this unit */
//
//    const std::string segment_dir_to_merge = "/tmp/dir_to_merge";
//
//    const std::string vector_name = "test_vector";
//    const std::vector<uint8_t> vector_data{0, 1};
//    const std::vector<milvus::segment::doc_id_t> vector_uids{1234, 5678};
//
//    const std::string attr_name = "test_partition";
//    const std::unordered_map<std::string, std::vector<uint8_t>> attr_data{{"test_partition", {0, 1}}};
//    const std::unordered_map<std::string, uint64_t> attr_nbytes{
//        {"test_partition", attr_data.at("test_partition").size() * sizeof(uint8_t)}};
//    const std::vector<milvus::segment::doc_id_t> attr_uids{4321, 8765};
//
//    {
//        milvus::segment::SegmentWriter segment_writer(segment_dir_to_merge);
//
//        ASSERT_TRUE(segment_writer.AddVectors(vector_name, vector_data, vector_uids).ok());
//        ASSERT_TRUE(segment_writer.AddAttrs(attr_name, attr_nbytes, attr_data, attr_uids).ok());
//
//        ASSERT_TRUE(segment_writer.Serialize().ok());
//    }
//
//    const std::string segment_dir = "/tmp";
//
//    {
//        milvus::segment::SegmentWriter segment_writer(segment_dir);
//        ASSERT_TRUE(segment_writer.Merge(segment_dir_to_merge, "test_merge").ok());
//    }
//}
