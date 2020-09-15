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

#include <random>
#include <string>
#include <experimental/filesystem>

#include "codecs/Codec.h"
#include "db/IDGenerator.h"
#include "db/utils.h"
#include "db/SnapshotVisitor.h"
#include "db/Types.h"
#include "db/snapshot/IterateHandler.h"
#include "db/snapshot/Resources.h"
#include "knowhere/index/vector_index/helpers/IndexParameter.h"
#include "segment/SegmentReader.h"
#include "segment/SegmentWriter.h"
#include "segment/IdBloomFilter.h"
#include "segment/Utils.h"
#include "storage/disk/DiskIOReader.h"
#include "storage/disk/DiskIOWriter.h"
#include "utils/Json.h"

using SegmentVisitor = milvus::engine::SegmentVisitor;
using IdBloomFilter = milvus::segment::IdBloomFilter;
using IdBloomFilterPtr = milvus::segment::IdBloomFilterPtr;

namespace {
milvus::Status
CreateCollection(std::shared_ptr<DB> db, const std::string& collection_name, const LSN_TYPE& lsn) {
    CreateCollectionContext context;
    context.lsn = lsn;
    auto collection_schema = std::make_shared<Collection>(collection_name);
    context.collection = collection_schema;

    int64_t collection_id = 0;
    int64_t field_id = 0;
    /* field uid */
    auto uid_field = std::make_shared<Field>(milvus::engine::FIELD_UID,
                                             0,
                                             milvus::engine::DataType::INT64,
                                             milvus::engine::snapshot::JEmpty,
                                             field_id);
    auto uid_field_element_blt = std::make_shared<FieldElement>(collection_id,
                                                                field_id,
                                                                milvus::engine::ELEMENT_BLOOM_FILTER,
                                                                milvus::engine::FieldElementType::FET_BLOOM_FILTER);
    auto uid_field_element_del = std::make_shared<FieldElement>(collection_id,
                                                                field_id,
                                                                milvus::engine::ELEMENT_DELETED_DOCS,
                                                                milvus::engine::FieldElementType::FET_DELETED_DOCS);

    field_id++;
    /* field vector */
    milvus::json vector_param = {{milvus::knowhere::meta::DIM, 4}};
    auto vector_field = std::make_shared<Field>("vector", 0, milvus::engine::DataType::VECTOR_FLOAT, vector_param,
                                                field_id);
    auto vector_field_element_index = std::make_shared<FieldElement>(collection_id,
                                                                     field_id,
                                                                     milvus::knowhere::IndexEnum::INDEX_FAISS_IVFSQ8,
                                                                     milvus::engine::FieldElementType::FET_INDEX);

    context.fields_schema[uid_field] = {uid_field_element_blt, uid_field_element_del};
    context.fields_schema[vector_field] = {vector_field_element_index};

    return db->CreateCollection(context);
}
}  // namespace

TEST_F(SegmentTest, SegmentTest) {
    LSN_TYPE lsn = 0;
    auto next_lsn = [&]() -> decltype(lsn) {
        return ++lsn;
    };

    std::string db_root = "/tmp/milvus_test/db/table";
    std::string c1 = "c1";
    auto status = CreateCollection(db_, c1, next_lsn());
    ASSERT_TRUE(status.ok());

    ScopedSnapshotT ss;
    status = Snapshots::GetInstance().GetSnapshot(ss, c1);
    ASSERT_TRUE(status.ok());
    ASSERT_TRUE(ss);
    ASSERT_EQ(ss->GetName(), c1);

    SegmentFileContext sf_context;
    SFContextBuilder(sf_context, ss);

    std::vector<SegmentFileContext> contexts;
    SFContextsBuilder(contexts, ss);


    // std::cout << ss->ToString() << std::endl;

    auto& partitions = ss->GetResources<Partition>();
    ID_TYPE partition_id;
    for (auto& kv : partitions) {
        /* select the first partition */
        partition_id = kv.first;
        break;
    }

    std::vector<milvus::engine::idx_t> raw_uids = {123};
    std::vector<uint8_t> raw_vectors = {1, 2, 3, 4};

    {
        /* commit new segment */
        OperationContext context;
        context.lsn = next_lsn();
        context.prev_partition = ss->GetResource<Partition>(partition_id);
        auto op = std::make_shared<NewSegmentOperation>(context, ss);
        SegmentPtr new_seg;
        status = op->CommitNewSegment(new_seg);
        ASSERT_TRUE(status.ok());

        /* commit new segment file */
        for (auto& cctx : contexts) {
            SegmentFilePtr seg_file;
            auto nsf_context = cctx;
            nsf_context.segment_id = new_seg->GetID();
            nsf_context.partition_id = new_seg->GetPartitionId();
            status = op->CommitNewSegmentFile(nsf_context, seg_file);
        }

        /* build segment visitor */
        auto ctx = op->GetContext();
        ASSERT_TRUE(ctx.new_segment);
        auto visitor = SegmentVisitor::Build(ss, ctx.new_segment, ctx.new_segment_files);
        ASSERT_TRUE(visitor);
        ASSERT_EQ(visitor->GetSegment(), new_seg);
        ASSERT_FALSE(visitor->GetSegment()->IsActive());
        // std::cout << visitor->ToString() << std::endl;
        // std::cout << ss->ToString() << std::endl;

        /* write data */
        milvus::segment::SegmentWriter segment_writer(db_root, visitor);

//        status = segment_writer.AddChunk("test", raw_vectors, raw_uids);
//        ASSERT_TRUE(status.ok())
//
//        status = segment_writer.Serialize();
//        ASSERT_TRUE(status.ok());

        /* read data */
//        milvus::segment::SSSegmentReader segment_reader(db_root, visitor);
//
//        status = segment_reader.Load();
//        ASSERT_TRUE(status.ok());
//
//        milvus::segment::SegmentPtr segment_ptr;
//        status = segment_reader.GetSegment(segment_ptr);
//        ASSERT_TRUE(status.ok());
//
//        auto& out_uids = segment_ptr->vectors_ptr_->GetUids();
//        ASSERT_EQ(raw_uids.size(), out_uids.size());
//        ASSERT_EQ(raw_uids[0], out_uids[0]);
//        auto& out_vectors = segment_ptr->vectors_ptr_->GetData();
//        ASSERT_EQ(raw_vectors.size(), out_vectors.size());
//        ASSERT_EQ(raw_vectors[0], out_vectors[0]);
    }

    status = db_->DropCollection(c1);
    ASSERT_TRUE(status.ok());
}

TEST(BloomFilterTest, ReadWriteTest) {
    std::string file_path = "/tmp/milvus_bloom.blf";

    milvus::storage::IOReaderPtr reader_ptr = std::make_shared<milvus::storage::DiskIOReader>();
    milvus::storage::IOWriterPtr writer_ptr = std::make_shared<milvus::storage::DiskIOWriter>();
    milvus::storage::OperationPtr operation_ptr = nullptr;
    auto fs_ptr = std::make_shared<milvus::storage::FSHandler>(reader_ptr, writer_ptr, operation_ptr);

    const int64_t id_count = 100000;
    milvus::engine::SafeIDGenerator id_gen;
    std::vector<int64_t> id_array;
    std::vector<int64_t> removed_id_array;

    auto error_rate_check_1 = [&](IdBloomFilter& filter, int64_t repeat) -> void {
        int64_t wrong_check = 0;
        for (int64_t i = 0; i < repeat; ++i) {
            auto id = id_gen.GetNextIDNumber();
            bool res = filter.Check(id);
            if (res) {
                wrong_check++;
            }
        }

        double error_rate = filter.ErrorRate();
        double wrong_rate = (double)wrong_check / id_count;
        ASSERT_LT(wrong_rate, error_rate);
    };

    auto error_rate_check_2 = [&](IdBloomFilter& filter, const std::vector<int64_t>& id_array) -> void {
        int64_t wrong_check = 0;
        for (auto id : id_array) {
            bool res = filter.Check(id);
            if (res) {
                wrong_check++;
            }
        }

        double error_rate = filter.ErrorRate();
        double wrong_rate = (double)wrong_check / id_count;
        ASSERT_LT(wrong_rate, error_rate);
    };

    {
        IdBloomFilter filter(id_count);

        // insert some ids
        for (int64_t i = 0; i < id_count; ++i) {
            auto id = id_gen.GetNextIDNumber();
            filter.Add(id);
            id_array.push_back(id);
        }

        // check inserted ids
        for (auto id : id_array) {
            bool res = filter.Check(id);
            ASSERT_TRUE(res);
        }

        // check non-exist ids
        error_rate_check_1(filter, id_count);

        // remove some ids
        std::vector<int64_t> temp_array;
        for (auto id : id_array) {
            if (id % 7 == 0) {
                filter.Remove(id);
                removed_id_array.push_back(id);
            } else {
                temp_array.push_back(id);
            }
        }
        id_array.swap(temp_array);

        // check removed ids
        error_rate_check_2(filter, removed_id_array);

        fs_ptr->writer_ptr_->Open(file_path);
        auto status = filter.Write(fs_ptr);
        ASSERT_TRUE(status.ok());
        fs_ptr->writer_ptr_->Close();
    }

    {
        IdBloomFilter filter(0);
        fs_ptr->reader_ptr_->Open(file_path);
        auto status = filter.Read(fs_ptr);
        ASSERT_TRUE(status.ok());
        fs_ptr->reader_ptr_->Close();

        // check inserted ids
        for (auto id : id_array) {
            bool res = filter.Check(id);
            ASSERT_TRUE(res);
        }

        // check non-exist ids
        error_rate_check_1(filter, id_count);

        // check removed ids
        error_rate_check_2(filter, removed_id_array);
    }

    std::experimental::filesystem::remove(file_path);
}

TEST(BloomFilterTest, CloneTest) {
    const int64_t id_count = 100000;
    milvus::engine::SafeIDGenerator id_gen;

    std::vector<int64_t> id_array;
    std::vector<int64_t> removed_id_array;

    IdBloomFilterPtr filter = std::make_shared<IdBloomFilter>(id_count);

    // insert some ids
    std::set<int64_t> ids;
    for (int64_t i = 0; i < id_count; ++i) {
        auto id = id_gen.GetNextIDNumber();
        filter->Add(id);
        ids.insert(id);
        id_array.push_back(id);
    }

    // remove some ids
    std::vector<int64_t> temp_array;
    for (auto id : id_array) {
        if (id % 7 == 0) {
            filter->Remove(id);
            removed_id_array.push_back(id);
        } else {
            temp_array.push_back(id);
        }
    }
    id_array.swap(temp_array);

    auto error_rate_check = [&](IdBloomFilterPtr& filter, const std::vector<int64_t>& id_array) -> void {
        int64_t wrong_check = 0;
        for (auto id : id_array) {
            bool res = filter->Check(id);
            if (res) {
                wrong_check++;
            }
        }

        double error_rate = filter->ErrorRate();
        double wrong_rate = (double)wrong_check / id_count;
        ASSERT_LT(wrong_rate, error_rate);
    };

    error_rate_check(filter, removed_id_array);

    IdBloomFilterPtr clone_filter;
    filter->Clone(clone_filter);
    ASSERT_NE(clone_filter, nullptr);

    error_rate_check(clone_filter, removed_id_array);
}

TEST(SegmentUtilTest, CalcCopyRangeTest) {
    // invalid input test
    std::vector<int32_t> offsets;
    int64_t row_count = 0, delete_count = 0;
    milvus::segment::CopyRanges copy_ranges;
    bool res = milvus::segment::CalcCopyRangesWithOffset(offsets, row_count, copy_ranges, delete_count);
    ASSERT_FALSE(res);

    row_count = 100;

    auto compare_result =
        [&](const std::vector<int32_t>& offsets, const milvus::segment::CopyRanges& compare_range) -> void {
            milvus::segment::CopyRanges copy_ranges;
            res = milvus::segment::CalcCopyRangesWithOffset(offsets, row_count, copy_ranges, delete_count);
            ASSERT_TRUE(res);

            int64_t compare_count = 0;
            for (auto offset : offsets) {
                if (offset >= 0 && offset < row_count) {
                    compare_count++;
                }
            }

            ASSERT_EQ(delete_count, compare_count);
            ASSERT_EQ(copy_ranges.size(), compare_range.size());
            for (size_t i = 0; i < copy_ranges.size(); ++i) {
                ASSERT_EQ(copy_ranges[i], compare_range[i]);
            }
        };

    {
        offsets = {0, 1, 2, 99, 100};
        milvus::segment::CopyRanges compare = {
            {3, 99}
        };
        compare_result(offsets, compare);
    }

    {
        offsets = {-1, 5, 4, 3, 90, 91};
        milvus::segment::CopyRanges compare = {
            {0, 3},
            {6, 90},
            {92, 100},
        };
        compare_result(offsets, compare);
    }
}

TEST(SegmentUtilTest, CopyRangeDataTest) {
    auto compare_result = [&](std::vector<uint8_t>& src_data,
                              std::vector<int32_t>& offsets,
                              int64_t row_count,
                              int64_t row_width) -> void {
        int64_t delete_count = 0;
        milvus::segment::CopyRanges copy_ranges;
        auto res = milvus::segment::CalcCopyRangesWithOffset(offsets, row_count, copy_ranges, delete_count);
        ASSERT_TRUE(res);

        if (copy_ranges.empty()) {
            return;
        }

        std::vector<uint8_t> target_data;
        res = milvus::segment::CopyDataWithRanges(src_data, row_width, copy_ranges, target_data);
        ASSERT_TRUE(res);

        // erase element from the largest offset
        std::vector<uint8_t> compare_data = src_data;
        std::set<int32_t> arrange_offsets;
        for (auto offset : offsets) {
            if (offset >= 0 && offset < row_count) {
                arrange_offsets.insert(offset);
            }
        }

        for (auto iter = arrange_offsets.rbegin(); iter != arrange_offsets.rend(); ++iter) {
            auto step = (*iter) * row_width;
            compare_data.erase(compare_data.begin() + step, compare_data.begin() + step + row_width);
        }
        ASSERT_EQ(target_data, compare_data);
    };

    // invalid input test
    std::vector<int32_t> offsets;
    std::vector<uint8_t> src_data;
    int64_t row_width = 0;
    milvus::segment::CopyRanges copy_ranges;
    std::vector<uint8_t> target_data;
    bool res = milvus::segment::CopyDataWithRanges(src_data, row_width, copy_ranges, target_data);
    ASSERT_FALSE(res);

    // construct source data
    row_width = 64;
    int64_t row_count = 100;
    src_data.resize(row_count * row_width);
    for (int64_t i = 0; i < row_count * row_width; ++i) {
        src_data[i] = i % 255;
    }
    {
        offsets = {0, 1, 2, 99, 100};
        compare_result(src_data, offsets, row_count, row_width);
    }

    {
        offsets = {-1, 5, 4, 3, 90, 91};
        compare_result(src_data, offsets, row_count, row_width);
    }

    // random test
    for (int32_t i = 0; i < 10; ++i) {
        std::default_random_engine random;
        row_count = random() % 100 + 1;
        row_width = random() % 8 + 8;

        src_data.resize(row_count * row_width);
        for (int64_t i = 0; i < row_count * row_width; ++i) {
            src_data[i] = i % 255;
        }

        int64_t offset_count = (row_count > 1) ? (random() % row_count + 1) : 1;
        offsets.resize(offset_count);
        for (int64_t k = 0; k < offset_count; ++k) {
            offsets[k] = (random() % row_count) + ((k % 2 == 0) ? 2 : -2);
        }
        compare_result(src_data, offsets, row_count, row_width);
    }
}
