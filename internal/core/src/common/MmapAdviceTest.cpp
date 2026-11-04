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

#include <arrow/api.h>
#include <gtest/gtest.h>

#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>

#include "common/ChunkWriter.h"
#include "common/MmapAdvice.h"
#include "storage/storage_c.h"

namespace milvus {
namespace {

TEST(MmapAdvice, ParsePolicies) {
    EXPECT_EQ(ParseMmapAdvice(""), MmapAdvice::None);
    EXPECT_EQ(ParseMmapAdvice("none"), MmapAdvice::None);
    EXPECT_EQ(ParseMmapAdvice("NORMAL"), MmapAdvice::Normal);
    EXPECT_EQ(ParseMmapAdvice("Random"), MmapAdvice::Random);
    EXPECT_EQ(ParseMmapAdvice("sequential"), MmapAdvice::Sequential);
    EXPECT_EQ(ParseMmapAdvice("willneed"), MmapAdvice::WillNeed);
    EXPECT_FALSE(ParseMmapAdvice("unsupported").has_value());
    EXPECT_FALSE(NativeMmapAdvice(MmapAdvice::None).has_value());
}

TEST(MmapAdvice, InvalidConfigRejectedBeforeInitialization) {
    // All other pointers are null: validation must precede any initialization.
    CMmapConfig config{};
    config.vector_field_mmap_advice = "invalid";
    auto status = InitMmapManager(config);
    EXPECT_EQ(status.error_code, ErrorCode::ConfigInvalid);
    ASSERT_NE(status.error_msg, nullptr);
    EXPECT_NE(std::string(status.error_msg).find("vectorFieldAdvice"),
              std::string::npos);
    free(const_cast<char*>(status.error_msg));
}

#ifdef __linux__
struct MappingFlags {
    bool random{false};
    bool sequential{false};
};

std::optional<MappingFlags>
ReadMappingFlags(const void* data) {
    std::ifstream smaps("/proc/self/smaps");
    const auto address = reinterpret_cast<uintptr_t>(data);
    bool matched = false;
    for (std::string line; std::getline(smaps, line);) {
        unsigned long begin = 0, end = 0;
        if (std::sscanf(line.c_str(), "%lx-%lx", &begin, &end) == 2) {
            matched = address >= begin && address < end;
        } else if (matched && line.starts_with("VmFlags:")) {
            MappingFlags result;
            std::istringstream flags(line);
            for (std::string flag; flags >> flag;) {
                result.random |= flag == "rr";
                result.sequential |= flag == "sr";
            }
            return result;
        }
    }
    return std::nullopt;
}

void
ExpectAdvice(const void* data, MmapAdvice advice) {
    const auto flags = ReadMappingFlags(data);
    ASSERT_TRUE(flags.has_value());
    EXPECT_EQ(flags->random, advice == MmapAdvice::Random);
    EXPECT_EQ(flags->sequential, advice == MmapAdvice::Sequential);
}

class VectorFieldMmapAdviceTest : public testing::Test {
 protected:
    void
    SetUp() override {
        std::string pattern =
            (std::filesystem::temp_directory_path() / "vector_mmap_XXXXXX")
                .string();
        ASSERT_NE(mkdtemp(pattern.data()), nullptr);
        directory_ = std::move(pattern);
        arrow::FixedSizeBinaryBuilder builder(arrow::fixed_size_binary(8));
        const float values[] = {1.0F, 2.0F};
        ASSERT_TRUE(
            builder.Append(reinterpret_cast<const uint8_t*>(values)).ok());
        ASSERT_TRUE(builder.Finish(&vectors_).ok());
        arrow::Int64Builder scalar_builder;
        ASSERT_TRUE(scalar_builder.Append(42).ok());
        ASSERT_TRUE(scalar_builder.Finish(&scalars_).ok());
    }

    void
    TearDown() override {
        std::filesystem::remove_all(directory_);
    }

    std::string directory_;
    std::shared_ptr<arrow::Array> vectors_;
    std::shared_ptr<arrow::Array> scalars_;
    const FieldMeta vector_meta_{FieldName("vector"),
                                 FieldId(101),
                                 DataType::VECTOR_FLOAT,
                                 2,
                                 std::nullopt,
                                 false,
                                 std::nullopt};
    const FieldMeta scalar_meta_{FieldName("scalar"),
                                 FieldId(102),
                                 DataType::INT64,
                                 false,
                                 std::nullopt};
};

TEST_F(VectorFieldMmapAdviceTest, FileMappingsRespectPolicyAndPopulate) {
    for (const auto advice : {MmapAdvice::None,
                              MmapAdvice::Normal,
                              MmapAdvice::Random,
                              MmapAdvice::Sequential,
                              MmapAdvice::WillNeed}) {
        for (const bool populate : {false, true}) {
            SCOPED_TRACE(static_cast<int>(advice));
            SCOPED_TRACE(populate);
            const auto path = directory_ + "/vector";
            {
                auto chunk = create_chunk(vector_meta_,
                                          {vectors_},
                                          populate,
                                          path,
                                          proto::common::LoadPriority::HIGH,
                                          MmapChunkWritebackMode::Disabled,
                                          advice);
                ExpectAdvice(chunk->RawData(), advice);
                const auto* values =
                    reinterpret_cast<const float*>(chunk->Data());
                EXPECT_FLOAT_EQ(values[0], 1.0F);
                EXPECT_FLOAT_EQ(values[1], 2.0F);
            }
            EXPECT_FALSE(std::filesystem::exists(path));
        }
    }
}

TEST_F(VectorFieldMmapAdviceTest, ScalarAndAnonymousMappingsAreUnchanged) {
    auto scalar = create_chunk(scalar_meta_,
                               {scalars_},
                               false,
                               directory_ + "/scalar",
                               proto::common::LoadPriority::HIGH,
                               MmapChunkWritebackMode::Disabled,
                               MmapAdvice::Random);
    auto vector = create_chunk(vector_meta_,
                               {vectors_},
                               false,
                               "",
                               proto::common::LoadPriority::HIGH,
                               MmapChunkWritebackMode::Disabled,
                               MmapAdvice::Random);
    ExpectAdvice(scalar->RawData(), MmapAdvice::None);
    ExpectAdvice(vector->RawData(), MmapAdvice::None);
}

TEST_F(VectorFieldMmapAdviceTest, MixedGroupAdvisesOnlyVectorPages) {
    const FieldMeta tail_meta(
        FieldName("tail"), FieldId(103), DataType::INT64, false, std::nullopt);
    for (const auto advice : {MmapAdvice::Random, MmapAdvice::Sequential}) {
        const auto path = directory_ + "/group";
        {
            auto chunks =
                create_group_chunk({scalar_meta_.get_id(),
                                    vector_meta_.get_id(),
                                    tail_meta.get_id()},
                                   {scalar_meta_, vector_meta_, tail_meta},
                                   {{scalars_}, {vectors_}, {scalars_}},
                                   false,
                                   path,
                                   proto::common::LoadPriority::HIGH,
                                   MmapChunkWritebackMode::Disabled,
                                   advice);
            ExpectAdvice(chunks.at(vector_meta_.get_id())->RawData(), advice);
            ExpectAdvice(chunks.at(scalar_meta_.get_id())->RawData(),
                         MmapAdvice::None);
            ExpectAdvice(chunks.at(tail_meta.get_id())->RawData(),
                         MmapAdvice::None);
            // Scalar lifetime keeps the shared mmap alive after dropping the vector.
            chunks.erase(vector_meta_.get_id());
            EXPECT_TRUE(std::filesystem::exists(path));
            EXPECT_EQ(*reinterpret_cast<const int64_t*>(
                          chunks.at(tail_meta.get_id())->Data()),
                      42);
        }
        EXPECT_FALSE(std::filesystem::exists(path));
    }
}

TEST_F(VectorFieldMmapAdviceTest, FailedAdviceKeepsMappingUsable) {
    auto buffer = create_chunk_buffer(
        vector_meta_, {vectors_}, false, directory_ + "/failure");
    // An unaligned address forces EINVAL from the real syscall.
    EXPECT_NO_THROW(
        ApplyMmapAdvice(buffer.data + 1, buffer.size, MmapAdvice::Random));
    ExpectAdvice(buffer.data, MmapAdvice::None);
    EXPECT_FLOAT_EQ(*reinterpret_cast<const float*>(buffer.data), 1.0F);
    ApplyMmapAdvice(buffer.data, buffer.size, MmapAdvice::Sequential);
    ApplyMmapAdvice(buffer.data, buffer.size, MmapAdvice::None);
    ExpectAdvice(buffer.data, MmapAdvice::Sequential);
    ApplyMmapAdvice(buffer.data, buffer.size, MmapAdvice::Normal);
    ExpectAdvice(buffer.data, MmapAdvice::Normal);
}

TEST_F(VectorFieldMmapAdviceTest, CoversAllVectorFieldTypes) {
    std::vector<FieldMeta> metas;
    std::vector<arrow::ArrayVector> arrays;
    std::vector<FieldId> ids;
    for (const auto& [type, dim] :
         std::vector<std::pair<DataType, int>>{{DataType::VECTOR_FLOAT, 2},
                                               {DataType::VECTOR_FLOAT16, 4},
                                               {DataType::VECTOR_BFLOAT16, 4},
                                               {DataType::VECTOR_INT8, 8},
                                               {DataType::VECTOR_BINARY, 64}}) {
        ids.emplace_back(200 + ids.size());
        metas.emplace_back(FieldName("vector"),
                           ids.back(),
                           type,
                           dim,
                           std::nullopt,
                           false,
                           std::nullopt);
        arrays.push_back({vectors_});
    }
    arrow::BinaryBuilder sparse_builder;
    ASSERT_TRUE(sparse_builder.Append(std::string(8, '\0')).ok());
    std::shared_ptr<arrow::Array> sparse;
    ASSERT_TRUE(sparse_builder.Finish(&sparse).ok());
    ids.emplace_back(200 + ids.size());
    metas.emplace_back(FieldName("sparse"),
                       ids.back(),
                       DataType::VECTOR_SPARSE_U32_F32,
                       0,
                       std::nullopt,
                       false,
                       std::nullopt);
    arrays.push_back({sparse});

    auto values = std::make_shared<arrow::FixedSizeBinaryBuilder>(
        arrow::fixed_size_binary(8));
    arrow::ListBuilder list_builder(arrow::default_memory_pool(), values);
    ASSERT_TRUE(list_builder.Append().ok());
    ASSERT_TRUE(values->Append(std::string(8, '\0')).ok());
    std::shared_ptr<arrow::Array> vector_array;
    ASSERT_TRUE(list_builder.Finish(&vector_array).ok());
    ids.emplace_back(200 + ids.size());
    metas.emplace_back(FieldName("vector_array"),
                       ids.back(),
                       DataType::VECTOR_ARRAY,
                       DataType::VECTOR_FLOAT,
                       2,
                       std::nullopt,
                       false,
                       false);
    arrays.push_back({vector_array});

    for (size_t i = 0; i < metas.size(); ++i) {
        SCOPED_TRACE(i);
        auto buffer = create_chunk_buffer(metas[i],
                                          arrays[i],
                                          false,
                                          directory_ + "/single",
                                          proto::common::LoadPriority::HIGH,
                                          MmapChunkWritebackMode::Disabled,
                                          MmapAdvice::Random);
        ExpectAdvice(buffer.data, MmapAdvice::Random);
    }
    auto chunks = create_group_chunk(ids,
                                     metas,
                                     arrays,
                                     false,
                                     directory_ + "/vectors",
                                     proto::common::LoadPriority::HIGH,
                                     MmapChunkWritebackMode::Disabled,
                                     MmapAdvice::Random);
    for (const auto id : ids) {
        ExpectAdvice(chunks.at(id)->RawData(), MmapAdvice::Random);
    }
}
#endif

}  // namespace
}  // namespace milvus
