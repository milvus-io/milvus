// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "common/Decimal.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "segcore/SegmentGrowing.h"
#include "segcore/SegmentGrowingImpl.h"
#include "unittest/utils.h"

using namespace milvus;
using namespace milvus::segcore;

TEST(DecimalSegcore, GrowingSegmentInsertAndBulkSubscript) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    auto decimal_fid = schema->AddDebugField("decimal_val", DataType::DECIMAL, true);
    auto vec_fid = schema->AddDebugField("vec", DataType::VECTOR_FLOAT, 4, knowhere::metric::L2);
    schema->set_primary_field_id(pk_fid);

    SegcoreConfig config = SegcoreConfig::default_config();
    auto segment = CreateGrowingSegment(schema, nullptr, config);

    const int64_t num_rows = 3;
    auto insert_record = std::make_unique<segcorepb::InsertRecord>();
    insert_record->set_num_rows(num_rows);

    // Primary key
    auto pk_data = insert_record->add_fields_data();
    pk_data->set_field_id(pk_fid.get());
    pk_data->set_type(segcorepb::DataType::Int64);
    auto pk_scalars = pk_data->mutable_scalars()->mutable_long_data();
    for (int64_t i = 1; i <= num_rows; ++i) {
        pk_scalars->add_data(i);
    }

    // Decimal field
    auto dec_data = insert_record->add_fields_data();
    dec_data->set_field_id(decimal_fid.get());
    dec_data->set_type(segcorepb::DataType::Decimal);
    auto dec_bytes = dec_data->mutable_scalars()->mutable_bytes_data();
    dec_bytes->add_data(EncodeDecimalBytes(199900));  // 19.99
    dec_bytes->add_data("");                           // Null placeholder
    dec_bytes->add_data(EncodeDecimalBytes(-50));      // -0.005
    dec_data->add_valid_data(true);
    dec_data->add_valid_data(false);
    dec_data->add_valid_data(true);

    // Vector field
    auto vec_data = insert_record->add_fields_data();
    vec_data->set_field_id(vec_fid.get());
    vec_data->set_type(segcorepb::DataType::FloatVector);
    vec_data->mutable_vectors()->set_dim(4);
    auto vec_floats = vec_data->mutable_vectors()->mutable_float_vector();
    for (int i = 0; i < num_rows * 4; ++i) {
        vec_floats->add_data(0.1f * i);
    }

    // Timestamps
    for (int i = 0; i < num_rows; ++i) {
        insert_record->add_timestamps(100 + i);
        insert_record->add_row_ids(i);
    }

    auto reserved_offset = segment->PreInsert(num_rows);
    ASSERT_EQ(reserved_offset, 0);
    segment->Insert(reserved_offset,
                    num_rows,
                    insert_record->timestamps().data(),
                    insert_record->row_ids().data(),
                    insert_record.get());

    // Retrieve via bulk_subscript
    SegOffset seg_offsets[] = {SegOffset(0), SegOffset(1), SegOffset(2)};
    auto field_data = segment->bulk_subscript(decimal_fid, seg_offsets, num_rows);
    ASSERT_NE(field_data, nullptr);

    auto result_bytes = field_data->scalars().bytes_data().data();
    ASSERT_EQ(result_bytes.size(), num_rows);
    EXPECT_EQ(result_bytes[0], EncodeDecimalBytes(199900));
    EXPECT_EQ(result_bytes[1], "");
    EXPECT_EQ(result_bytes[2], EncodeDecimalBytes(-50));

    EXPECT_TRUE(field_data->valid_data(0));
    EXPECT_FALSE(field_data->valid_data(1));
    EXPECT_TRUE(field_data->valid_data(2));
}
