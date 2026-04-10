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

#include <cstddef>
#include <string>
#include <vector>

#include "common/Types.h"
#include "../common/mol_c.h"
#include "index/MolPatternIndex.h"

namespace milvus::index {

TEST(MolPatternIndexTest, BuildWithValidAndInvalidRows) {
    auto pickle1 = ConvertSMILESToPickle("CCO");
    ASSERT_EQ(pickle1.error_code, MOL_SUCCESS);
    ASSERT_NE(pickle1.data, nullptr);

    auto pickle2 = ConvertSMILESToPickle("CO");
    ASSERT_EQ(pickle2.error_code, MOL_SUCCESS);
    ASSERT_NE(pickle2.data, nullptr);

    std::vector<std::string> values;
    values.emplace_back(reinterpret_cast<const char*>(pickle1.data),
                        pickle1.size);
    values.emplace_back("");
    values.emplace_back(reinterpret_cast<const char*>(pickle2.data),
                        pickle2.size);

    bool valid_data[] = {true, false, true};

    MolPatternIndex<std::string> index;
    index.Build(values.size(), values.data(), valid_data);

    EXPECT_EQ(index.RowCount(), static_cast<int64_t>(values.size()));
    EXPECT_EQ(index.Dim(), 2048);

    auto fp_data = index.GetFPData();
    ASSERT_NE(fp_data, nullptr);

    auto bytes_per_row = static_cast<size_t>((index.Dim() + 7) / 8);
    auto invalid_row_offset = bytes_per_row;
    for (size_t i = 0; i < bytes_per_row; ++i) {
        EXPECT_EQ(fp_data[invalid_row_offset + i], 0)
            << "invalid row should be zeroed";
    }

    FreeMolDataResult(&pickle2);
    FreeMolDataResult(&pickle1);
}

TEST(MolPatternIndexTest, SerializeLoadWithoutAssemble) {
    auto pickle1 = ConvertSMILESToPickle("CCO");
    ASSERT_EQ(pickle1.error_code, MOL_SUCCESS);
    ASSERT_NE(pickle1.data, nullptr);

    auto pickle2 = ConvertSMILESToPickle("CO");
    ASSERT_EQ(pickle2.error_code, MOL_SUCCESS);
    ASSERT_NE(pickle2.data, nullptr);

    std::vector<std::string> values;
    values.emplace_back(reinterpret_cast<const char*>(pickle1.data),
                        pickle1.size);
    values.emplace_back(reinterpret_cast<const char*>(pickle2.data),
                        pickle2.size);

    MolPatternIndex<std::string> index;
    index.Build(values.size(), values.data(), nullptr);

    Config config;
    auto binary_set = index.Serialize(config);

    MolPatternIndex<std::string> loaded;
    loaded.LoadWithoutAssemble(binary_set, {});

    EXPECT_EQ(loaded.Dim(), index.Dim());
    EXPECT_EQ(loaded.RowCount(), index.RowCount());

    auto data_buf = binary_set.GetByName("mol_pattern_fp_data");
    ASSERT_NE(data_buf, nullptr);
    auto bytes_per_row = static_cast<size_t>((index.Dim() + 7) / 8);
    EXPECT_EQ(data_buf->size,
              static_cast<size_t>(index.RowCount()) * bytes_per_row);

    FreeMolDataResult(&pickle2);
    FreeMolDataResult(&pickle1);
}

TEST(MolPatternIndexTest, AppendMolRowUpdatesPublishedCount) {
    MolPatternIndex<std::string> index(4, 1024);

    auto pickle = ConvertSMILESToPickle("CCO");
    ASSERT_EQ(pickle.error_code, MOL_SUCCESS);
    ASSERT_NE(pickle.data, nullptr);

    std::string pickle_str(reinterpret_cast<const char*>(pickle.data),
                           pickle.size);

    index.AppendMolRow(2, pickle_str, true);
    EXPECT_EQ(index.RowCount(), 3);

    index.AppendMolRow(0, pickle_str, true);
    EXPECT_EQ(index.RowCount(), 3);

    index.AppendMolRow(3, std::string{}, false);
    EXPECT_EQ(index.RowCount(), 4);

    auto bytes_per_row = static_cast<size_t>((index.Dim() + 7) / 8);
    auto fp_data = index.GetFPData();
    ASSERT_NE(fp_data, nullptr);
    auto row3_offset = bytes_per_row * 3;
    for (size_t i = 0; i < bytes_per_row; ++i) {
        EXPECT_EQ(fp_data[row3_offset + i], 0)
            << "invalid row should stay zeroed";
    }

    FreeMolDataResult(&pickle);
}

}  // namespace milvus::index
