// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#pragma once

#include <memory>
#include <optional>
#include <string>

#include "common/type_c.h"
#include "milvus-storage/ffi_c.h"
#include "milvus-storage/manifest.h"
#include "milvus-storage/properties.h"
#include "storage/Types.h"

// Preserve the configured root for ChunkManager key construction.
std::shared_ptr<milvus_storage::api::Properties>
MakeStorageProperties(CStorageConfig c_storage_config);

// Index-build and clustering protobufs share the storage fields. Reuse the C
// boundary for owned strings, optional tuning defaults and node-local policy.
template <typename ProtoStorageConfig>
std::shared_ptr<milvus_storage::api::Properties>
MakeStorageProperties(const ProtoStorageConfig& config) {
    return MakeStorageProperties(CStorageConfig{
        .address = config.address().c_str(),
        .bucket_name = config.bucket_name().c_str(),
        .access_key_id = config.access_keyid().c_str(),
        .access_key_value = config.secret_access_key().c_str(),
        .root_path = config.root_path().c_str(),
        .storage_type = config.storage_type().c_str(),
        .cloud_provider = config.cloud_provider().c_str(),
        .iam_endpoint = config.iamendpoint().c_str(),
        .log_level = "warn",
        .region = config.region().c_str(),
        .useSSL = config.usessl(),
        .sslCACert = config.sslcacert().c_str(),
        .useIAM = config.useiam(),
        .useVirtualHost = config.use_virtual_host(),
        .requestTimeoutMs = config.request_timeout_ms(),
        .gcp_credential_json = config.gcpcredentialjson().c_str(),
        .use_custom_part_upload = true,
        .max_connections = config.max_connections(),
        .tls_min_version = config.ssl_tls_min_version().c_str(),
        .use_crc32c_checksum = config.use_crc32c_checksum(),
        .talon_mode = config.talon_mode(),
        .talon_small_read_threshold = config.talon_small_read_threshold(),
        .talon_coordinator = config.talon_coordinator().c_str(),
        .talon_block_size = config.talon_block_size(),
        .talon_max_idle_per_addr = config.talon_max_idle_per_addr(),
        .talon_enable_for_external_table =
            config.talon_enable_for_external_table(),
        .crt_queue_sizes = 0,
    });
}

// Normalize a private copy for Arrow/loon, without modifying shared properties.
std::shared_ptr<milvus_storage::api::Properties>
MakeInternalProperties(
    const std::shared_ptr<const milvus_storage::api::Properties>& properties);

/**
 * @brief Create internal API Properties from CStorageConfig
 *
 * Creates a Properties object using the internal milvus_storage::api
 * interface. All configuration fields from CStorageConfig are mapped to
 * properties.
 *
 * @param c_storage_config The storage configuration to convert
 * @return Shared pointer to milvus_storage::api::Properties
 */
std::shared_ptr<milvus_storage::api::Properties>
MakeInternalPropertiesFromStorageConfig(CStorageConfig c_storage_config);

/**
 * @brief fs.root_path handed to the loon / Arrow filesystem for a storage
 * config.
 *
 * Milvus keys already carry the storage prefix: localStorage.path for local
 * (`/var/lib/milvus/data/insert_log/...`) and minio.rootPath for remote
 * (`files/insert_log/...`). The filesystem therefore has to be rooted at the
 * namespace root, which is the bucket for remote (milvus-storage never
 * applies root_path there) and "/" for local. Rooting the local
 * SubTreeFileSystem at localStorage.path joins the prefix twice
 * (milvus-storage #351, milvus #53051), making complete keys differ from their
 * physical locations.
 */
inline constexpr const char* kLoonLocalFSRootPath = "/";

std::string
LoonFSRootPath(const std::string& storage_type, const std::string& root_path);

/**
 * @brief Retrieve ColumnGroups metadata from manifest path
 *
 * Parses the manifest path JSON and fetches the latest manifest
 * containing column groups metadata from the storage.
 *
 * @param path JSON string containing "base_path" and "ver" fields
 * @param properties Storage properties for accessing the manifest
 * @return Shared pointer to ColumnGroups metadata
 * @throws std::runtime_error If JSON parsing or manifest fetch fails
 */
std::shared_ptr<milvus_storage::api::Manifest>
GetLoonManifest(
    const std::string& path,
    const std::shared_ptr<const milvus_storage::api::Properties>& properties);

struct LoonManifestPath {
    std::string base_path;
    int64_t version = 0;
    // Byte size of the manifest object of this revision as the path records it,
    // or a non-positive value when unknown. A known size lets the read skip
    // asking storage for it and must match the object exactly. It is not part
    // of the revision identity.
    int64_t size = -1;
};

/**
 * @brief Parse a manifest path JSON
 *
 * @return the revision the path names, or std::nullopt when path is not a JSON
 *         object with a string "base_path" and an integer "ver". An optional
 *         "size" that is not an integer is ignored.
 */
std::optional<LoonManifestPath>
ParseLoonManifestPath(const std::string& path);

/**
 * @brief Read the manifest of the revision a manifest path names
 *
 * Opens the revision with the size the path records, so the read can skip
 * asking storage for it. Error classification is left to the caller.
 */
LoonFFIResult
ReadLoonManifest(const LoonManifestPath& manifest_path,
                 const LoonProperties* properties,
                 LoonManifest** out_manifest);

/**
 * @brief Check whether two manifest paths name the same manifest revision
 *
 * A manifest path is identified by its "base_path" and "ver"; JSON key order
 * and optional fields do not change the revision. Values that are not
 * manifest path JSON are compared as plain strings. Mirrors Go's
 * packed.SameManifestPath.
 */
bool
IsSameLoonManifestPath(const std::string& lhs, const std::string& rhs);

/**
 * @brief Inject every external_spec-derived property for external collections.
 *
 * Emits two property families from a single external_spec JSON:
 *   - extfs.{collectionID}.* — storage-layer: credentials, endpoint, bucket,
 *     region, SSL, IAM. Three-layer model:
 *       Layer 0: zero-init bool fields and apply the process-local External
 *                Table IOPS policy (no fs.* IOPS inheritance).
 *       Layer 1: derive storage_type / use_ssl / bucket_name / address from
 *                the external_source URI (Milvus form by default).
 *       Layer 2: apply external_spec.extfs JSON with allowlist gating.
 *       Post-process: explicit endpoint_url mapping, or the legacy AWS-form
 *       swap + Tier-1/2 endpoint derivation when endpoint_url is absent.
 *   - format-layer — per-format keys derived from spec.format (e.g.
 *     reader.exttable.snapshot_id when format="iceberg-table").
 *
 * The caller MUST have run ValidateExternalSource (Go side) first — this
 * function asserts non-empty scheme and host on external_source.
 */
void
InjectExternalSpecProperties(milvus_storage::api::Properties& properties,
                             int64_t collection_id,
                             const std::string& external_source,
                             const std::string& external_spec);

// Server-owned startup policy. An uninitialized or empty policy denies brokers.
arrow::Result<std::string>
NormalizeBrokerEndpoint(const std::string& raw);
arrow::Status
InitCredentialBrokerPolicy(const std::string& endpoints_json);
arrow::Status
CheckCredentialBrokerEndpoint(const std::string& endpoint);
