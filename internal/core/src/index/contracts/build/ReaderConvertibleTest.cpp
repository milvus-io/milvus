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
#include <string_view>
#include <utility>

#include "common/EasyAssert.h"
#include "index/contracts/build/IReaderConvertible.h"
#include "index/contracts/query/INullReader.h"
#include "index/test_utils/ScalarReaderFactory.h"
#include "index/test_utils/ScalarTestData.h"

namespace milvus::index::test {
namespace {

class TrackingReader final : public IIndexReaderBase {
 public:
    explicit TrackingReader(std::shared_ptr<int> dependency)
        : dependency_(std::move(dependency)) {
    }

    ReaderCaps
    Caps() const override {
        return {};
    }

    Domain
    CoordDomain() const override {
        return Domain::Row;
    }

    int64_t
    Count() const override {
        return dependency_ == nullptr ? -1 : *dependency_;
    }

    DataType
    ValueType() const override {
        return DataType::INT64;
    }

    int64_t
    MemoryUsage() const override {
        return 0;
    }

    cachinglayer::ResourceUsage
    CellByteSize() const override {
        return {0, 0};
    }

 private:
    std::shared_ptr<int> dependency_;
};

class PlainTrackingArtifact final : public storage::Artifact {
 public:
    PlainTrackingArtifact(int& destroyed, int& serialized)
        : destroyed_(destroyed), serialized_(serialized) {
    }

    ~PlainTrackingArtifact() override {
        ++destroyed_;
    }

    void
    Serialize(storage::FileSink&) const override {
        ++serialized_;
    }

 private:
    int& destroyed_;
    int& serialized_;
};

enum class ConvertBehavior {
    Success,
    Throw,
    Null,
};

class ConvertibleTrackingArtifact final : public storage::Artifact,
                                          public IReaderConvertible {
 public:
    ConvertibleTrackingArtifact(int& destroyed,
                                int& serialized,
                                std::shared_ptr<int> dependency,
                                ConvertBehavior behavior)
        : destroyed_(destroyed),
          serialized_(serialized),
          dependency_(std::move(dependency)),
          behavior_(behavior) {
    }

    ~ConvertibleTrackingArtifact() override {
        ++destroyed_;
    }

    void
    Serialize(storage::FileSink&) const override {
        ++serialized_;
    }

    IIndexReaderBasePtr
        IntoReader() &&
        override {
        if (behavior_ == ConvertBehavior::Throw) {
            ThrowInfo(DataFormatBroken, "tracking conversion failed");
        }
        if (behavior_ == ConvertBehavior::Null) {
            return nullptr;
        }
        return std::make_unique<TrackingReader>(std::move(dependency_));
    }

 private:
    int& destroyed_;
    int& serialized_;
    std::shared_ptr<int> dependency_;
    ConvertBehavior behavior_;
};

void
ExpectConsumeError(storage::ArtifactPtr artifact, ErrorCode expected) {
    try {
        static_cast<void>(IReaderConvertible::FromArtifact(std::move(artifact)));
        FAIL() << "artifact conversion expected an error";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), expected);
    }
}

TEST(ReaderConvertibleTest, NullArtifactIsUnexpectedError) {
    ExpectConsumeError(nullptr, ErrorCode::UnexpectedError);
}

TEST(ReaderConvertibleTest, MissingCapabilityDoesNotSerialize) {
    int destroyed = 0;
    int serialized = 0;
    ExpectConsumeError(
        std::make_unique<PlainTrackingArtifact>(destroyed, serialized),
        ErrorCode::Unsupported);
    EXPECT_EQ(destroyed, 1);
    EXPECT_EQ(serialized, 0);
}

TEST(ReaderConvertibleTest, SuccessTransfersDependencyToReader) {
    int destroyed = 0;
    int serialized = 0;
    auto dependency = std::make_shared<int>(7);
    std::weak_ptr<int> weak = dependency;
    auto artifact = std::make_unique<ConvertibleTrackingArtifact>(
        destroyed, serialized, std::move(dependency), ConvertBehavior::Success);

    auto reader = IReaderConvertible::FromArtifact(std::move(artifact));
    EXPECT_EQ(destroyed, 1);
    EXPECT_EQ(serialized, 0);
    ASSERT_NE(reader, nullptr);
    EXPECT_EQ(reader->Count(), 7);
    EXPECT_FALSE(weak.expired());
    reader.reset();
    EXPECT_TRUE(weak.expired());
}

TEST(ReaderConvertibleTest, ConverterErrorDestroysShell) {
    int destroyed = 0;
    int serialized = 0;
    ExpectConsumeError(
        std::make_unique<ConvertibleTrackingArtifact>(destroyed,
                                                      serialized,
                                                      std::make_shared<int>(1),
                                                      ConvertBehavior::Throw),
        ErrorCode::DataFormatBroken);
    EXPECT_EQ(destroyed, 1);
    EXPECT_EQ(serialized, 0);
}

TEST(ReaderConvertibleTest, NullResultDestroysShell) {
    int destroyed = 0;
    int serialized = 0;
    ExpectConsumeError(
        std::make_unique<ConvertibleTrackingArtifact>(destroyed,
                                                      serialized,
                                                      std::make_shared<int>(1),
                                                      ConvertBehavior::Null),
        ErrorCode::UnexpectedError);
    EXPECT_EQ(destroyed, 1);
    EXPECT_EQ(serialized, 0);
}

TEST(ReaderConvertibleTest, OrdinaryScalarArtifactIsNotConvertible) {
    const auto& backend = ScalarReaderBackends().Get<int64_t>("BitmapInt64");
    ScalarTestData<int64_t> data({1, 2, 3});
    data.validity_present = false;
    const ScalarTestInput<int64_t> input(data);
    auto artifact = backend.Build(input.View(), {.row_count = 3});
    ExpectConsumeError(std::move(artifact), ErrorCode::Unsupported);
}

TEST(ReaderConvertibleTest, TextRamArtifactConvertsWithoutPersistence) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("TextVarcharRamV7");
    ScalarTestData<std::string_view> data({"alpha beta", "gamma"});
    data.validity_present = false;
    const ScalarTestInput<std::string_view> input(data);
    auto artifact = backend.Build(input.View(), {.row_count = 2});

    auto reader = IReaderConvertible::FromArtifact(std::move(artifact));
    ASSERT_NE(reader, nullptr);
    EXPECT_EQ(reader->Count(), 2);
    EXPECT_EQ(reader->CoordDomain(), Domain::Row);
    EXPECT_EQ(reader->ValueType(), DataType::VARCHAR);
    EXPECT_TRUE(reader->Caps().text_match);
    EXPECT_NE(dynamic_cast<const INullReader*>(reader.get()), nullptr);
}

}  // namespace
}  // namespace milvus::index::test
