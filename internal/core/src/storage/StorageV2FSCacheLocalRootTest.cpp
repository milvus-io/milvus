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
#include <folly/ScopeGuard.h>
#include <unistd.h>

#include <filesystem>
#include <string>

#include "common/type_c.h"
#include "milvus-storage/properties.h"
#include "pb/clustering.pb.h"
#include "pb/index_cgo_msg.pb.h"
#include "storage/Util.h"
#include "storage/loon_ffi/property_singleton.h"
#include "storage/loon_ffi/util.h"

namespace {

std::string
MakeTempRoot(const std::string& name) {
    auto root =
        std::filesystem::temp_directory_path() /
        ("milvus_local_root_" + name + "_" + std::to_string(::getpid()));
    std::filesystem::remove_all(root);
    std::filesystem::create_directories(root);
    return root.string();
}

}  // namespace

// Milvus hands complete filesystem paths (already prefixed with
// localStorage.path) to the local Arrow / loon filesystem, so that filesystem
// must be rooted at "/". If it were rooted at localStorage.path again, every
// write would land under <root>/<root>/... (milvus-storage #351, #53051).
TEST(StorageV2FSCacheLocalRootTest, AbsolutePathsAreNotJoinedWithRootTwice) {
    const auto root = MakeTempRoot("fs");

    auto key = std::make_shared<milvus_storage::api::Properties>();
    (*key)[PROPERTY_FS_ROOT_PATH] = root;
    (*key)[PROPERTY_FS_STORAGE_TYPE] = std::string("local");
    const auto cm = milvus::storage::CreateChunkManager(key);
    EXPECT_EQ(cm->GetRootPath(), root);
    auto fs = milvus::storage::InitArrowFileSystem(key);
    ASSERT_NE(fs, nullptr);

    const auto dir = root + "/insert_log/1/2/3/_data";
    const auto file = dir + "/0.parquet";
    ASSERT_TRUE(fs->CreateDir(dir, /*recursive=*/true).ok());
    auto out = fs->OpenOutputStream(file);
    ASSERT_TRUE(out.ok()) << out.status().ToString();
    ASSERT_TRUE(out.ValueOrDie()->Write("x", 1).ok());
    ASSERT_TRUE(out.ValueOrDie()->Close().ok());

    // Physical location is exactly the key.
    EXPECT_TRUE(std::filesystem::exists(file));
    EXPECT_EQ(cm->Size(file), 1);
    char data = 0;
    EXPECT_EQ(cm->Read(file, &data, 1), 1);
    EXPECT_EQ(data, 'x');
    // Filesystem normalization must not mutate the shared ChunkManager config.
    EXPECT_EQ(std::get<std::string>(key->at(PROPERTY_FS_ROOT_PATH)), root);
    // And not the double-joined location.
    EXPECT_FALSE(std::filesystem::exists(root + root + "/insert_log"));

    // Reading back through the same filesystem with the same key works.
    auto in = fs->OpenInputFile(file);
    ASSERT_TRUE(in.ok()) << in.status().ToString();
    auto size = in.ValueOrDie()->GetSize();
    ASSERT_TRUE(size.ok());
    EXPECT_EQ(size.ValueOrDie(), 1);

    std::filesystem::remove_all(root);
}

TEST(StorageV2FSCacheLocalRootTest, LoonFSRootPathRule) {
    EXPECT_EQ(LoonFSRootPath("local", "/var/lib/milvus/data"), "/");
    EXPECT_EQ(LoonFSRootPath("local", ""), "/");
    EXPECT_EQ(LoonFSRootPath("remote", "files"), "files");
    EXPECT_EQ(LoonFSRootPath("minio", "files"), "files");
    EXPECT_EQ(LoonFSRootPath("", "files"), "files");
}

TEST(StorageV2FSCacheLocalRootTest, InternalPropertiesUseSlashForLocal) {
    CStorageConfig c_config{};
    c_config.root_path = "/var/lib/milvus/data";
    c_config.storage_type = "local";
    const auto input = MakeStorageProperties(c_config);
    EXPECT_EQ(std::get<std::string>(input->at(PROPERTY_FS_ROOT_PATH)),
              c_config.root_path);
    auto props = MakeInternalProperties(input);
    EXPECT_EQ(std::get<std::string>(input->at(PROPERTY_FS_ROOT_PATH)),
              c_config.root_path);
    auto root = milvus_storage::api::GetValue<std::string>(
        props, PROPERTY_FS_ROOT_PATH);
    ASSERT_TRUE(root.ok());
    EXPECT_EQ(root.ValueOrDie(), "/");

    CStorageConfig remote{};
    remote.root_path = "files";
    remote.storage_type = "remote";
    auto remote_props = MakeInternalPropertiesFromStorageConfig(remote);
    auto remote_root = milvus_storage::api::GetValue<std::string>(
        remote_props, PROPERTY_FS_ROOT_PATH);
    ASSERT_TRUE(remote_root.ok());
    EXPECT_EQ(remote_root.ValueOrDie(), "files");
    auto use_crt = milvus_storage::api::GetValue<bool>(remote_props,
                                                       PROPERTY_FS_S3_USE_CRT);
    ASSERT_TRUE(use_crt.ok());
    EXPECT_FALSE(*use_crt);
}

TEST(StorageProperties, RemoteBoundaryPreservesOptionsAndOwnsStrings) {
    std::string endpoint = "objects.example:9000";
    CStorageConfig config{};
    config.address = endpoint.c_str();
    config.bucket_name = "bucket";
    config.root_path = "files/prefix";
    config.storage_type = "minio";
    config.cloud_provider = "";
    config.access_key_id = "review-ak";
    config.access_key_value = "review-sk";
    config.region = "region";
    config.iam_endpoint = "http://iam.example";
    config.useIAM = true;
    config.useVirtualHost = true;
    config.sslCACert = "ca.pem";
    config.log_level = "debug";
    config.requestTimeoutMs = 17000;
    config.max_connections = 37;
    config.use_custom_part_upload = true;
    config.use_crc32c_checksum = true;
    config.tls_min_version = "1.3";
    config.crt_queue_sizes = 9;
    const auto properties = MakeStorageProperties(config);
    endpoint.assign("changed-after-conversion");
    const auto normalized = MakeInternalProperties(properties);
    milvus_storage::ArrowFileSystemConfig actual;
    const auto status =
        milvus_storage::ArrowFileSystemConfig::create_file_system_config(
            normalized, actual);
    ASSERT_TRUE(status.ok()) << status.ToString();
    EXPECT_EQ(actual.storage_type, "remote");
    EXPECT_EQ(actual.cloud_provider, "aws");
    EXPECT_EQ(actual.address, "http://objects.example:9000");
    EXPECT_EQ(actual.bucket_name, "bucket");
    EXPECT_EQ(actual.root_path, "files/prefix");
    EXPECT_EQ(actual.access_key_id, "review-ak");
    EXPECT_EQ(actual.access_key_value, "review-sk");
    EXPECT_EQ(actual.region, "region");
    EXPECT_EQ(actual.iam_endpoint, "http://iam.example");
    EXPECT_TRUE(actual.use_iam);
    EXPECT_TRUE(actual.use_virtual_host);
    EXPECT_EQ(actual.ssl_ca_cert, "ca.pem");
    EXPECT_EQ(actual.log_level, "debug");
    EXPECT_EQ(actual.request_timeout_ms, 17000);
    EXPECT_EQ(actual.max_connections, 37);
    EXPECT_TRUE(actual.use_custom_part_upload);
    EXPECT_TRUE(actual.use_crc32c_checksum);
    EXPECT_EQ(actual.tls_min_version, "1.3");
    EXPECT_EQ(actual.crt_queue_sizes, 9);
    EXPECT_FALSE(actual.s3_use_crt);
    EXPECT_EQ(std::get<std::string>(properties->at(PROPERTY_FS_ADDRESS)),
              "objects.example:9000");
    EXPECT_EQ(std::get<std::string>(properties->at(PROPERTY_FS_STORAGE_TYPE)),
              "minio");

    // Azure authorities must not receive the S3 http:// prefix.
    auto azure = std::make_shared<milvus_storage::api::Properties>(*properties);
    (*azure)[PROPERTY_FS_CLOUD_PROVIDER] = std::string("azure");
    const auto azure_properties = MakeInternalProperties(azure);
    EXPECT_EQ(std::get<std::string>(azure_properties->at(PROPERTY_FS_ADDRESS)),
              "objects.example:9000");

    // Both job protobufs must carry the same configuration as the C boundary,
    // including node-local settings absent from the job messages.
    auto& singleton =
        milvus::storage::LoonFFIPropertiesSingleton::GetInstance();
    const auto previous_queue_sizes = singleton.GetS3CrtQueueSizes();
    const auto previous_reader_limits = singleton.GetArrowReaderLimits();
    const auto restore = folly::makeGuard([&] {
        singleton.SetS3CrtQueueSizes(previous_queue_sizes);
        singleton.SetArrowReaderConfig(
            previous_reader_limits.hole_size_limit_bytes,
            previous_reader_limits.range_size_limit_bytes);
    });
    singleton.SetS3CrtQueueSizes(250);
    singleton.SetArrowReaderConfig(1024, 4096);
    config.address = "objects.example:9000";
    config.log_level = "warn";
    config.crt_queue_sizes = 0;
    config.useSSL = true;
    config.gcp_credential_json = "credential-json";
    config.talon_mode = 1;
    config.talon_small_read_threshold = 128;
    config.talon_coordinator = "talon.example:9000";
    config.talon_block_size = 1024;
    config.talon_max_idle_per_addr = 7;
    config.talon_enable_for_external_table = true;
    const auto expected = MakeStorageProperties(config);
    const auto check_proto = [&](auto proto) {
        SCOPED_TRACE(proto.GetTypeName());
        proto.set_address(config.address);
        proto.set_bucket_name(config.bucket_name);
        proto.set_access_keyid(config.access_key_id);
        proto.set_secret_access_key(config.access_key_value);
        proto.set_root_path(config.root_path);
        proto.set_storage_type(config.storage_type);
        proto.set_cloud_provider(config.cloud_provider);
        proto.set_iamendpoint(config.iam_endpoint);
        proto.set_region(config.region);
        proto.set_usessl(config.useSSL);
        proto.set_sslcacert(config.sslCACert);
        proto.set_useiam(config.useIAM);
        proto.set_use_virtual_host(config.useVirtualHost);
        proto.set_request_timeout_ms(config.requestTimeoutMs);
        proto.set_gcpcredentialjson(config.gcp_credential_json);
        proto.set_max_connections(config.max_connections);
        proto.set_ssl_tls_min_version(config.tls_min_version);
        proto.set_use_crc32c_checksum(config.use_crc32c_checksum);
        proto.set_talon_mode(config.talon_mode);
        proto.set_talon_small_read_threshold(config.talon_small_read_threshold);
        proto.set_talon_coordinator(config.talon_coordinator);
        proto.set_talon_block_size(config.talon_block_size);
        proto.set_talon_max_idle_per_addr(config.talon_max_idle_per_addr);
        proto.set_talon_enable_for_external_table(
            config.talon_enable_for_external_table);
        const auto converted = MakeStorageProperties(proto);
        proto.Clear();
        EXPECT_EQ(*converted, *expected);

        const auto defaults =
            MakeInternalProperties(MakeStorageProperties(proto));
        for (const auto* key : {PROPERTY_FS_MAX_CONNECTIONS,
                                PROPERTY_FS_TALON_SMALL_READ_THRESHOLD,
                                PROPERTY_FS_TALON_BLOCK_SIZE,
                                PROPERTY_FS_TALON_MAX_IDLE_PER_ADDR}) {
            EXPECT_EQ(defaults->count(key), 0u) << key;
        }
        EXPECT_EQ(std::get<std::string>(defaults->at(PROPERTY_FS_LOG_LEVEL)),
                  "warn");
        EXPECT_TRUE(
            std::get<bool>(defaults->at(PROPERTY_FS_USE_CUSTOM_PART_UPLOAD)));
        EXPECT_FALSE(std::get<bool>(defaults->at(PROPERTY_FS_S3_USE_CRT)));
        EXPECT_EQ(
            std::get<uint32_t>(defaults->at(PROPERTY_FS_S3_CRT_QUEUE_SIZES)),
            250);
        EXPECT_EQ(std::get<int64_t>(defaults->at(
                      PROPERTY_READER_PARQUET_PREBUFFER_HOLE_SIZE_LIMIT)),
                  1024);
        EXPECT_EQ(std::get<int64_t>(defaults->at(
                      PROPERTY_READER_PARQUET_PREBUFFER_RANGE_SIZE_LIMIT)),
                  4096);
        EXPECT_EQ(
            std::get<std::string>(defaults->at("milvus.talon.external_mode")),
            "0");
    };
    check_proto(milvus::proto::indexcgo::StorageConfig{});
    check_proto(milvus::proto::clustering::StorageConfig{});
}
