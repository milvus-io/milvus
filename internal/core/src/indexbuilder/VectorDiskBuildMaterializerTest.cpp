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

#include <array>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <utility>

#include "common/FieldData.h"
#include "indexbuilder/VectorBuildMaterializer.h"
#include "indexbuilder/VectorDiskBuildMaterializer.h"

namespace milvus::indexbuilder {
namespace {

constexpr int64_t kRows = 4;
constexpr int64_t kDim = 2;
constexpr std::array<float, kRows * kDim> kValues{1, 2, 3, 4, 5, 6, 7, 8};
constexpr const char* kFamily = "test_disk_input_options";

class InspectingBuilder
    : public index::IArtifactBuilder<index::PreparedVectorBuildFiles<float>> {
 public:
    explicit InspectingBuilder(bool use_memory) : use_memory_(use_memory) {
    }

    index::BuilderInputSpec
    InputSpec() const override {
        return {.use_memory = use_memory_};
    }

    storage::ArtifactPtr
        Build(const index::PreparedVectorBuildFiles<float>& input) &&
        override {
        std::ifstream file(input.raw_path, std::ios::binary);
        std::array<uint32_t, 2> header{};
        file.read(reinterpret_cast<char*>(header.data()), sizeof(header));
        EXPECT_TRUE(file.good());
        EXPECT_EQ(header[0], kRows);
        EXPECT_EQ(header[1], kDim);
        std::array<float, kRows * kDim> values{};
        if (input.tensor) {
            EXPECT_TRUE(use_memory_);
            std::memcpy(values.data(), input.tensor, sizeof(values));
        } else {
            EXPECT_TRUE(input.raw_file_complete);
            file.read(reinterpret_cast<char*>(values.data()), sizeof(values));
            EXPECT_TRUE(file.good());
        }
        EXPECT_EQ(values, kValues);
        EXPECT_EQ(
            std::filesystem::file_size(input.raw_path),
            sizeof(header) + (input.raw_file_complete ? sizeof(values) : 0));
        return nullptr;
    }

 private:
    bool use_memory_;
};

class VectorDiskBuildMaterializerTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        static const bool registered = [] {
            index::BuilderRegistry<
                index::PreparedVectorBuildFiles<float>>::Instance()
                .Register(kFamily, [](const index::BuildParams& params) {
                    return std::make_unique<InspectingBuilder>(
                        params.value("use_memory", false));
                });
            return true;
        }();
        static_cast<void>(registered);
        files_ = storage::LocalDirectory::CreateOwned(
            "/tmp", "disk_input_options_XXXXXX", "disk input options test");
    }

    VectorDiskBuildMaterializer
    Materializer(bool use_memory = false) {
        return {files_->Path(),
                DataType::VECTOR_FLOAT,
                DataType::VECTOR_FLOAT,
                kDim,
                false,
                kRows,
                kFamily,
                {{"use_memory", use_memory}}};
    }

    void
    AddRows(VectorDiskBuildMaterializer& materializer, size_t offset) {
        FieldDataPtr batch = std::make_shared<FieldData<FloatVector>>(
            kDim, DataType::VECTOR_FLOAT, false);
        batch->FillFieldData(kValues.data() + offset * kDim, 2);
        materializer.Add(batch);
    }

    uintmax_t
    RawFileSize() const {
        for (const auto& entry :
             std::filesystem::recursive_directory_iterator(files_->Path())) {
            if (entry.path().filename() == "raw_data") {
                return entry.file_size();
            }
        }
        ADD_FAILURE() << "raw input file not found";
        return 0;
    }

    std::shared_ptr<storage::LocalDirectory> files_;
};

TEST_F(VectorDiskBuildMaterializerTest, DefaultInputStreamsEachBatchToFile) {
    auto materializer = Materializer();
    EXPECT_FALSE(materializer.InputSpec().use_memory);
    EXPECT_ANY_THROW(materializer.AllocateInput());
    for (size_t offset : {0, 2}) {
        AddRows(materializer, offset);
        EXPECT_EQ(RawFileSize(),
                  2 * sizeof(uint32_t) + (offset + 2) * kDim * sizeof(float));
    }
    materializer.FinishPrimary();
    EXPECT_NO_THROW(std::move(materializer).Build());
}

TEST_F(VectorDiskBuildMaterializerTest, OptedInInputRemainsResident) {
    auto materializer = Materializer(true);
    AddRows(materializer, 0);
    AddRows(materializer, 2);
    materializer.FinishPrimary();
    EXPECT_EQ(RawFileSize(), 2 * sizeof(uint32_t));
    EXPECT_NO_THROW(std::move(materializer).Build());
}

TEST_F(VectorDiskBuildMaterializerTest, DirectInputRemainsResident) {
    auto materializer = Materializer(true);
    std::memcpy(materializer.AllocateInput(), kValues.data(), sizeof(kValues));
    materializer.FinishInput(kRows);
    materializer.FinishPrimary();
    EXPECT_EQ(RawFileSize(), 2 * sizeof(uint32_t));
    EXPECT_NO_THROW(std::move(materializer).Build());
}

}  // namespace
}  // namespace milvus::indexbuilder
