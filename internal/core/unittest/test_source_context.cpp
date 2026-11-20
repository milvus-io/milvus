// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <arrow/c/bridge.h>
#include <atomic>
#include <cstdlib>
#include <nlohmann/json.hpp>
#include "gtest/gtest.h"
#include "common/common_type_c.h"
#include "milvus-storage/filesystem/azure/source_sas_token_policy.h"
#include "milvus-storage/filesystem/fs.h"
#include "storage/loon_ffi/external_spec_c.h"
#include "storage/loon_ffi/loon_error_code.h"
#include "storage/loon_ffi/source_context.h"
#include "storage/loon_ffi/tracked_batch_reader.h"
#include "storage/loon_ffi/util.h"

namespace {
// Routing tests don't authenticate this synthetic binding. Production obtains
// it from trusted coordinator metadata and validates it through the Go factory.
constexpr auto kSpec =
    R"({"format":"iceberg-table","snapshot_id":7,"credential_binding":{},"extfs":{"cloud_provider":"azure","access_key_id":"account123","region":"usgov"}})";
constexpr auto kRoot = "abfss://data@account123.dfs.core.windows.net/table";

class RejectSourceProvider : public milvus_storage::SourceCredentialProvider {
 public:
    milvus_storage::SourceCredentialResponse
    Fetch(const milvus_storage::SourceCredentialRequest&) const override {
        ++calls;
        milvus_storage::SourceCredentialResponse response;
        response.status = LOON_SOURCE_CREDENTIAL_DENIED;
        return response;
    }
    mutable std::atomic<int> calls{0};
};

TEST(SourceContextRouting, BothInjectionEntrypointsUseExplicitAzureRouting) {
    for (const auto* source :
         {"azure://core.windows.net/data/table",
          "az://core.windows.net/data/table",
          "https://account123.blob.core.windows.net/data/table",
          "https://account123.dfs.core.windows.net/data/table",
          kRoot}) {
        SCOPED_TRACE(source);
        milvus_storage::api::Properties properties;
        ASSERT_NO_THROW(
            InjectExternalSpecProperties(properties, 42, source, kSpec));
        EXPECT_EQ(std::get<std::string>(properties.at("extfs.42.address")),
                  "core.windows.net");
        EXPECT_EQ(std::get<std::string>(properties.at("extfs.42.bucket_name")),
                  "data");
        EXPECT_EQ(std::get<std::string>(properties.at("extfs.42.use_ssl")),
                  "true");

        LoonProperties c_properties{};
        auto result = loon_properties_inject_external_spec(
            &c_properties, 42, source, kSpec, 3000, 0);
        EXPECT_EQ(result.err_code, 0);
        EXPECT_STREQ(loon_properties_get(&c_properties, "extfs.42.address"),
                     "core.windows.net");
        EXPECT_STREQ(loon_properties_get(&c_properties, "extfs.42.bucket_name"),
                     "data");
        EXPECT_STREQ(
            loon_properties_get(&c_properties, "reader.exttable.snapshot_id"),
            "7");
        loon_ffi_free_result(&result);
        loon_properties_free(&c_properties);
    }
}

TEST(SourceContextRouting, InvalidConfigurationCannotFallBackToLegacy) {
    for (const auto* key : {"access_key_value",
                            "credential_endpoint",
                            "azure_credential_endpoint",
                            "use_iam",
                            "endpoint_url",
                            "bucket_name"}) {
        auto spec = nlohmann::json::parse(kSpec);
        spec["extfs"][key] = "";
        LoonProperties properties{};
        auto result = loon_properties_inject_external_spec(
            &properties, 42, kRoot, spec.dump().c_str(), 3000, 0);
        EXPECT_NE(result.err_code, 0) << key;
        EXPECT_EQ(properties.count, 0);
        loon_ffi_free_result(&result);
        loon_properties_free(&properties);
    }
    for (const auto* source :
         {"http://account123.blob.core.windows.net/data/table",
          "https://otheraccount.blob.core.windows.net/data/table",
          "azure://core.windows.net/data/table/%2fescape",
          "azure://core.windows.net/data/table/../other",
          "azure://core.windows.net/data/table?sig=PRIVATE",
          "az://data/table"}) {
        auto result = milvus::storage::SourceContextResource(source, kSpec);
        EXPECT_FALSE(result.ok()) << source;
        EXPECT_EQ(result.status().ToString().find("PRIVATE"),
                  std::string::npos);
    }
}

TEST(SourceContextRouting, NativeFilesystemChecksScopeAfterPropertyInjection) {
    auto provider = std::make_shared<RejectSourceProvider>();
    milvus_storage::SourceReadContextOptions options;
    options.account_name = "account123";
    options.allowed_root_uris = {kRoot};
    auto created = milvus_storage::SourceReadContext::Create(options, provider);
    ASSERT_TRUE(created.ok());
    auto context = *created;
    for (const auto* source :
         {"azure://core.windows.net/data/table",
          kRoot,
          "https://account123.blob.core.windows.net/data/table"}) {
        auto properties = std::make_shared<milvus_storage::api::Properties>();
        InjectExternalSpecProperties(*properties, 42, source, kSpec);
        auto fs =
            milvus_storage::FilesystemCache::getInstance().get_with_context(
                properties, source, context);
        ASSERT_TRUE(fs.ok()) << fs.status();
        EXPECT_EQ(fs->path, "table");
        auto wrong =
            milvus_storage::FilesystemCache::getInstance().get_with_context(
                properties,
                "https://account123.blob.core.windows.net/data/table-other/"
                "file",
                context);
        EXPECT_EQ(
            static_cast<int>(milvus::storage::ArrowStatusToErrorCode(wrong)),
            117);
    }
    // Neither configuration nor a rejected path fetches credentials.
    EXPECT_EQ(provider->calls.load(), 0);
}

class FailingBatchReader : public arrow::RecordBatchReader {
 public:
    FailingBatchReader(LoonSourceCredentialStatus status, bool throws)
        : status_(status), throws_(throws) {
    }
    std::shared_ptr<arrow::Schema>
    schema() const override {
        return arrow::schema({});
    }
    arrow::Status
    ReadNext(std::shared_ptr<arrow::RecordBatch>* batch) override {
        if (throws_) {
            ThrowInfo(static_cast<milvus::ErrorCode>(116 + status_),
                      "PRIVATE provider diagnostic");
        }
        batch->reset();
        return milvus_storage::fs::SourceCredentialArrowStatus(status_);
    }

 private:
    LoonSourceCredentialStatus status_;
    bool throws_;
};

TEST(SourceContextStream, RetainsTypedErrorsAcrossActualArrowCStream) {
    for (int code = 117; code <= 121; ++code) {
        EXPECT_EQ(
            static_cast<int>(milvus::storage::LoonErrCodeToErrorCode(code)),
            code);
        for (bool throws : {false, true}) {
            SCOPED_TRACE(code);
            auto error = std::make_shared<std::atomic<int>>(0);
            auto reader = std::make_shared<milvus::storage::TrackedBatchReader>(
                std::make_shared<FailingBatchReader>(
                    static_cast<LoonSourceCredentialStatus>(code - 116),
                    throws),
                error);
            ArrowArrayStream stream{};
            ASSERT_TRUE(arrow::ExportRecordBatchReader(reader, &stream).ok());
            ArrowArray batch{};
            EXPECT_NE(stream.get_next(&stream, &batch), 0);
            EXPECT_EQ(error->load(), code);
            EXPECT_EQ(
                std::string(stream.get_last_error(&stream)).find("PRIVATE"),
                std::string::npos);
            EXPECT_EQ(batch.release, nullptr);
            stream.release(&stream);

            // Exercise the same real CStatus exception projection used by the
            // engine accessor, not only a numeric Go mapping.
            milvus::SegcoreError native{
                static_cast<milvus::ErrorCode>(error->load()),
                "source read failed"};
            auto status = milvus::FailureCStatus(&native);
            EXPECT_EQ(status.error_code, code);
            free(const_cast<char*>(status.error_msg));
        }
    }
}
}  // namespace
