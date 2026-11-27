// Copyright 2023 Zilliz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <mutex>
#include <regex>
#include <cctype>
#include <charconv>
#include <arpa/inet.h>
#include "arrow/util/uri.h"
#include "arrow/util/string.h"
#include <nlohmann/json.hpp>
#include <stdint.h>
#include <algorithm>
#include <charconv>
#include <initializer_list>
#include <map>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include "arrow/result.h"
#include "arrow/status.h"
#include "simdjson.h"
#include "common/EasyAssert.h"
#include "common/ExternalSpecKey.h"
#include "log/Log.h"
#include "common/type_c.h"
#include "milvus-storage/common/extend_status.h"
#include "milvus-storage/ffi_c.h"
#include "milvus-storage/ffi_internal/ffi_error_code.h"
#include "milvus-storage/ffi_internal/result.h"
#include "milvus-storage/filesystem/fs.h"
#include "milvus-storage/properties.h"
#include "milvus-storage/transaction/transaction.h"
#include "nlohmann/json_fwd.hpp"
#include "storage/Types.h"
#include "storage/loon_ffi/external_spec_c.h"
#include "storage/loon_ffi/property_singleton.h"
#include "storage/loon_ffi/util.h"

using json = nlohmann::json;

namespace {
std::mutex broker_policy_mutex;
std::optional<std::unordered_set<std::string>> broker_allowed_endpoints;
}  // namespace

std::optional<int64_t>
TryParseCanonicalFieldID(std::string_view value) {
    if (value.empty()) {
        return std::nullopt;
    }
    int64_t field_id = -1;
    const auto parsed =
        std::from_chars(value.data(), value.data() + value.size(), field_id);
    if (parsed.ec != std::errc{} || parsed.ptr != value.data() + value.size() ||
        field_id < 0 || std::to_string(field_id) != value) {
        return std::nullopt;
    }
    return field_id;
}

namespace {
int64_t
ParseCanonicalFieldID(const std::string& value) {
    const auto field_id = TryParseCanonicalFieldID(value);
    if (!field_id.has_value()) {
        ThrowInfo(milvus::ErrorCode::DataFormatBroken,
                  "Invalid field ID mapping value '{}': expected canonical "
                  "nonnegative int64",
                  value);
    }
    return *field_id;
}

json
ParseFieldIDMappingJSON(const std::string& text) {
    std::unordered_set<std::string> keys;
    auto callback = [&keys](
                        int depth, json::parse_event_t event, json& parsed) {
        if (depth == 1 && event == json::parse_event_t::key &&
            !keys.insert(parsed.get<std::string>()).second) {
            ThrowInfo(milvus::ErrorCode::DataFormatBroken,
                      "Duplicate field ID mapping key");
        }
        return true;
    };
    try {
        auto result = json::parse(text, callback);
        if (!result.is_object()) {
            ThrowInfo(milvus::ErrorCode::DataFormatBroken,
                      "Field ID mapping must be an object");
        }
        return result;
    } catch (const nlohmann::json::exception& e) {
        ThrowInfo(milvus::ErrorCode::DataFormatBroken,
                  "Invalid field ID mapping JSON: {}",
                  e.what());
    }
}
}  // namespace

MilvusTableFieldIdMapping
ParseMilvusTableFieldIdMapping(const std::string& encoded) {
    const auto parsed = ParseFieldIDMappingJSON(encoded);
    MilvusTableFieldIdMapping mapping;
    for (auto it = parsed.begin(); it != parsed.end(); ++it) {
        const auto target_id = ParseCanonicalFieldID(it.key());
        if (!it.value().is_string()) {
            ThrowInfo(milvus::ErrorCode::DataFormatBroken,
                      "source field ID for target {} must be a string",
                      it.key());
        }
        const auto& physical = it.value().get_ref<const std::string&>();
        (void)ParseCanonicalFieldID(physical);
        mapping.physical_columns.emplace(target_id, physical);
        if (!mapping.target_fields.emplace(physical, target_id).second) {
            ThrowInfo(milvus::ErrorCode::DataFormatBroken,
                      "duplicate source field ID {}",
                      physical);
        }
    }
    return mapping;
}

std::shared_ptr<const MilvusTableFieldIdMapping>
ResolveMilvusTableFieldIdMapping(
    const milvus_storage::api::ColumnGroups& column_groups) {
    if (auto mapping = ReadMilvusTableFieldIdMapping(column_groups)) {
        return mapping;
    }
    auto identity = std::make_shared<MilvusTableFieldIdMapping>();
    for (const auto& group : column_groups) {
        for (const auto& column : group->columns) {
            const auto id = ParseCanonicalFieldID(column);
            identity->physical_columns.emplace(id, column);
            identity->target_fields.emplace(column, id);
        }
    }
    return identity;
}

std::shared_ptr<const MilvusTableFieldIdMapping>
ReadMilvusTableFieldIdMapping(
    const milvus_storage::api::ColumnGroups& column_groups) {
    std::shared_ptr<MilvusTableFieldIdMapping> mapping;
    bool saw_unmapped_nonempty = false;
    for (const auto& group : column_groups) {
        if (group == nullptr) {
            ThrowInfo(milvus::ErrorCode::DataFormatBroken,
                      "Null column group in field ID mapping");
        }
        std::optional<MilvusTableFieldIdMapping> group_mapping;
        bool any_property = false;
        bool any_missing = false;
        for (const auto& file : group->files) {
            const auto property =
                file.properties.find(kMilvusTableFieldIdMappingProperty);
            if (property == file.properties.end()) {
                any_missing = true;
                continue;
            }
            any_property = true;
            auto parsed = ParseMilvusTableFieldIdMapping(property->second);
            if (group_mapping.has_value() &&
                group_mapping->physical_columns != parsed.physical_columns) {
                ThrowInfo(milvus::ErrorCode::DataFormatBroken,
                          "Column group files disagree on field ID mapping");
            }
            group_mapping = std::move(parsed);
        }
        if (any_property && any_missing) {
            ThrowInfo(milvus::ErrorCode::DataFormatBroken,
                      "Column group files have incomplete field ID mapping "
                      "properties");
        }
        if (!any_property) {
            if (!group->files.empty() || !group->columns.empty()) {
                saw_unmapped_nonempty = true;
            }
            continue;
        }
        if (group_mapping->physical_columns.empty() &&
            !group->columns.empty()) {
            ThrowInfo(milvus::ErrorCode::DataFormatBroken,
                      "Nonempty column group has an empty field ID mapping");
        }
        if (!mapping) {
            mapping = std::make_shared<MilvusTableFieldIdMapping>();
        }
        std::unordered_set<std::string> mapped_group_columns;
        for (const auto& [target_id, physical] :
             group_mapping->physical_columns) {
            if (physical.empty() ||
                std::find(group->columns.begin(),
                          group->columns.end(),
                          physical) == group->columns.end()) {
                ThrowInfo(milvus::ErrorCode::DataFormatBroken,
                          "Mapped physical column '{}' is absent from its "
                          "column group",
                          physical);
            }
            mapped_group_columns.insert(physical);
            auto [physical_it, inserted] =
                mapping->physical_columns.emplace(target_id, physical);
            if (!inserted && physical_it->second != physical) {
                ThrowInfo(milvus::ErrorCode::DataFormatBroken,
                          "Target field {} has conflicting physical columns",
                          target_id);
            }
            auto [target_it, target_inserted] =
                mapping->target_fields.emplace(physical, target_id);
            if (!target_inserted && target_it->second != target_id) {
                ThrowInfo(milvus::ErrorCode::DataFormatBroken,
                          "Physical column '{}' maps to multiple target fields",
                          physical);
            }
        }
        if (mapped_group_columns.size() != group->columns.size()) {
            ThrowInfo(milvus::ErrorCode::DataFormatBroken,
                      "Column group field ID mapping does not cover every "
                      "physical column");
        }
    }
    if (mapping != nullptr && saw_unmapped_nonempty) {
        ThrowInfo(milvus::ErrorCode::DataFormatBroken,
                  "Column group has no field ID mapping");
    }
    return mapping;
}

arrow::Result<std::string>
NormalizeBrokerEndpoint(const std::string& raw) {
    for (unsigned char c : raw) {
        if (c <= 32 || c >= 127 || c == '\\') {
            return arrow::Status::Invalid(
                "Invalid credential broker URL characters");
        }
    }
    // Match Go specutil.NormalizeBrokerEndpoint without decoding the path.
    static const std::regex pattern(
        R"(^([A-Za-z][A-Za-z0-9+.-]*)://([^/?#]+)(/[^?#]*)?(\?[^#]*)?$)");
    std::smatch match;
    if (!std::regex_match(raw, match, pattern)) {
        return arrow::Status::Invalid("Invalid credential broker URL");
    }
    auto lower = [](std::string value) {
        std::transform(
            value.begin(), value.end(), value.begin(), [](unsigned char c) {
                return std::tolower(c);
            });
        return value;
    };
    auto scheme = lower(match[1]);
    auto authority = lower(match[2]);
    if ((scheme != "http" && scheme != "https") ||
        authority.find_first_of("@%*") != std::string::npos) {
        return arrow::Status::Invalid("Invalid credential broker authority");
    }
    std::string host, port;
    if (authority.front() == '[') {
        auto end = authority.find(']');
        if (end == std::string::npos || end == 1) {
            return arrow::Status::Invalid("Invalid broker IPv6 host");
        }
        host = authority.substr(0, end + 1);
        if (end + 1 < authority.size()) {
            if (authority[end + 1] != ':') {
                return arrow::Status::Invalid("Invalid broker port");
            }
            port = authority.substr(end + 2);
            if (port.empty())
                return arrow::Status::Invalid("Empty broker port");
        }
    } else {
        auto colon = authority.find(':');
        host = authority.substr(0, colon);
        if (colon != std::string::npos) {
            port = authority.substr(colon + 1);
            if (port.empty())
                return arrow::Status::Invalid("Empty broker port");
        }
    }
    if (!host.empty() && host.front() == '[') {
        in6_addr parsed_address;
        auto literal = host.substr(1, host.size() - 2);
        if (inet_pton(AF_INET6, literal.c_str(), &parsed_address) != 1) {
            return arrow::Status::Invalid("Invalid broker IPv6 host");
        }
    } else if (host.find_first_not_of(
                   "abcdefghijklmnopqrstuvwxyz0123456789.-_") !=
               std::string::npos) {
        return arrow::Status::Invalid("Invalid broker hostname");
    }
    if (host.empty())
        return arrow::Status::Invalid("Empty broker host");
    if (!port.empty()) {
        if (port.size() > 5 ||
            port.find_first_not_of("0123456789") != std::string::npos) {
            return arrow::Status::Invalid("Invalid broker port");
        }
        int value = std::stoi(port);
        if (value < 1 || value > 65535)
            return arrow::Status::Invalid("Invalid broker port");
        port = (scheme == "http" && value == 80) ||
                       (scheme == "https" && value == 443)
                   ? ""
                   : ":" + std::to_string(value);
    }
    // Arrow validates escapes and IPv6 syntax; its decoded path is never used.
    arrow::util::Uri uri;
    ARROW_RETURN_NOT_OK(uri.Parse(raw));
    auto path = match[3].str();
    if (path.empty())
        path = "/";
    if (path.find("//") != std::string::npos || path == "/." || path == "/.." ||
        path.find("/./") != std::string::npos ||
        path.find("/../") != std::string::npos ||
        (path.size() >= 2 && path.compare(path.size() - 2, 2, "/.") == 0) ||
        (path.size() >= 3 && path.compare(path.size() - 3, 3, "/..") == 0) ||
        path.find_first_not_of("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTU"
                               "VWXYZ0123456789-._~/") != std::string::npos) {
        return arrow::Status::Invalid(
            "Credential broker path must use unescaped unreserved characters, "
            "without repeated separators or dot segments");
    }
    return scheme + "://" + host + port + path + match[4].str();
}

arrow::Status
InitCredentialBrokerPolicy(const std::string& endpoints_json) {
    std::unordered_set<std::string> endpoints;
    try {
        for (const auto& value :
             json::parse(endpoints_json).get<std::vector<std::string>>()) {
            ARROW_ASSIGN_OR_RAISE(auto endpoint,
                                  NormalizeBrokerEndpoint(value));
            endpoints.insert(std::move(endpoint));
        }
    } catch (const json::exception&) {
        return arrow::Status::Invalid(
            "Invalid credential broker allowlist JSON");
    }
    std::lock_guard lock(broker_policy_mutex);
    if (broker_allowed_endpoints.has_value() &&
        *broker_allowed_endpoints != endpoints) {
        return arrow::Status::Invalid(
            "Credential broker policy is immutable; restart required");
    }
    broker_allowed_endpoints = std::move(endpoints);
    return arrow::Status::OK();
}

arrow::Status
CheckCredentialBrokerEndpoint(const std::string& endpoint) {
    std::lock_guard lock(broker_policy_mutex);
    if (!broker_allowed_endpoints.has_value() ||
        broker_allowed_endpoints->count(endpoint) == 0) {
        return arrow::Status::IOError(
            "Credential broker endpoint denied by server policy");
    }
    return arrow::Status::OK();
}

std::shared_ptr<milvus_storage::api::Properties>
MakeInternalPropertiesFromStorageConfig(CStorageConfig c_storage_config) {
    auto properties_map = std::make_shared<milvus_storage::api::Properties>();

    // Add non-null string fields
    if (c_storage_config.address != nullptr) {
        milvus_storage::api::SetValue(
            *properties_map, PROPERTY_FS_ADDRESS, c_storage_config.address);
    }
    if (c_storage_config.bucket_name != nullptr) {
        milvus_storage::api::SetValue(*properties_map,
                                      PROPERTY_FS_BUCKET_NAME,
                                      c_storage_config.bucket_name);
    }
    if (c_storage_config.access_key_id != nullptr) {
        milvus_storage::api::SetValue(*properties_map,
                                      PROPERTY_FS_ACCESS_KEY_ID,
                                      c_storage_config.access_key_id);
    }
    if (c_storage_config.access_key_value != nullptr) {
        milvus_storage::api::SetValue(*properties_map,
                                      PROPERTY_FS_ACCESS_KEY_VALUE,
                                      c_storage_config.access_key_value);
    }
    const std::string storage_type = c_storage_config.storage_type != nullptr
                                         ? c_storage_config.storage_type
                                         : "";
    if (c_storage_config.root_path != nullptr || storage_type == "local") {
        const std::string root_path = c_storage_config.root_path != nullptr
                                          ? c_storage_config.root_path
                                          : "";
        milvus_storage::api::SetValue(
            *properties_map,
            PROPERTY_FS_ROOT_PATH,
            LoonFSRootPath(storage_type, root_path).c_str());
    }
    if (c_storage_config.storage_type != nullptr) {
        milvus_storage::api::SetValue(*properties_map,
                                      PROPERTY_FS_STORAGE_TYPE,
                                      c_storage_config.storage_type);
    }
    if (c_storage_config.cloud_provider != nullptr) {
        milvus_storage::api::SetValue(*properties_map,
                                      PROPERTY_FS_CLOUD_PROVIDER,
                                      c_storage_config.cloud_provider);
    }
    if (c_storage_config.iam_endpoint != nullptr) {
        milvus_storage::api::SetValue(*properties_map,
                                      PROPERTY_FS_IAM_ENDPOINT,
                                      c_storage_config.iam_endpoint);
    }
    if (c_storage_config.log_level != nullptr) {
        milvus_storage::api::SetValue(
            *properties_map, PROPERTY_FS_LOG_LEVEL, "Warn");
    }
    if (c_storage_config.region != nullptr) {
        milvus_storage::api::SetValue(
            *properties_map, PROPERTY_FS_REGION, c_storage_config.region);
    }
    if (c_storage_config.sslCACert != nullptr) {
        milvus_storage::api::SetValue(*properties_map,
                                      PROPERTY_FS_SSL_CA_CERT,
                                      c_storage_config.sslCACert);
    }
    if (c_storage_config.gcp_credential_json != nullptr) {
        milvus_storage::api::SetValue(*properties_map,
                                      PROPERTY_FS_GCP_CREDENTIAL_JSON,
                                      c_storage_config.gcp_credential_json);
    }

    // Add boolean fields
    milvus_storage::api::SetValue(*properties_map,
                                  PROPERTY_FS_USE_SSL,
                                  c_storage_config.useSSL ? "true" : "false");
    milvus_storage::api::SetValue(*properties_map,
                                  PROPERTY_FS_USE_IAM,
                                  c_storage_config.useIAM ? "true" : "false");
    milvus_storage::api::SetValue(
        *properties_map,
        PROPERTY_FS_USE_VIRTUAL_HOST,
        c_storage_config.useVirtualHost ? "true" : "false");
    milvus_storage::api::SetValue(
        *properties_map,
        PROPERTY_FS_USE_CUSTOM_PART_UPLOAD,
        c_storage_config.use_custom_part_upload ? "true" : "false");

    // Add integer fields
    milvus_storage::api::SetValue(
        *properties_map,
        PROPERTY_FS_REQUEST_TIMEOUT_MS,
        std::to_string(c_storage_config.requestTimeoutMs).c_str());
    // Absent when unset: leave the key absent so milvus-storage applies its
    // registered default instead of taking an explicit 0.
    if (c_storage_config.max_connections > 0) {
        milvus_storage::api::SetValue(
            *properties_map,
            PROPERTY_FS_MAX_CONNECTIONS,
            std::to_string(c_storage_config.max_connections).c_str());
    }

    (*properties_map)[PROPERTY_FS_S3_CRT_QUEUE_SIZES] =
        c_storage_config.crt_queue_sizes > 0
            ? c_storage_config.crt_queue_sizes
            : milvus::storage::LoonFFIPropertiesSingleton::GetInstance()
                  .GetS3CrtQueueSizes();

    if (c_storage_config.tls_min_version != nullptr) {
        std::string tls_ver(c_storage_config.tls_min_version);
        if (!tls_ver.empty() && tls_ver != "default") {
            milvus_storage::api::SetValue(*properties_map,
                                          PROPERTY_FS_TLS_MIN_VERSION,
                                          c_storage_config.tls_min_version);
        }
    }

    milvus_storage::api::SetValue(
        *properties_map,
        PROPERTY_FS_USE_CRC32C_CHECKSUM,
        c_storage_config.use_crc32c_checksum ? "true" : "false");

    const bool is_local =
        c_storage_config.storage_type != nullptr &&
        std::string_view(c_storage_config.storage_type) == "local";
    // Default properties serve arbitrary operations. Only unrestricted policy
    // enables their FS; load readers select their own mode and file policy later.
    const uint32_t data_mode = (c_storage_config.talon_enabled_stages == 0 ||
                                c_storage_config.talon_enabled_stages ==
                                    milvus::storage::kTalonStagesAll)
                                   ? c_storage_config.talon_mode
                                   : 0;
    const std::pair<const char*, uint32_t> talon_values[] = {
        {PROPERTY_FS_TALON_MODE, data_mode},
        {PROPERTY_FS_TALON_SMALL_READ_THRESHOLD,
         c_storage_config.talon_small_read_threshold},
        {PROPERTY_FS_TALON_BLOCK_SIZE, c_storage_config.talon_block_size},
        {PROPERTY_FS_TALON_MAX_IDLE_PER_ADDR,
         c_storage_config.talon_max_idle_per_addr}};
    // Ordinary callers retain unrestricted file behavior; a QN load copy may
    // select the immutable open-only filesystem policy below.
    (*properties_map)[PROPERTY_FS_TALON_OPEN_ONLY] = false;
    // Keep the External Table policy alongside ordinary filesystem properties.
    // This Milvus-only key is consumed below, including for local primary storage.
    (*properties_map)["milvus.talon.external_mode"] = std::to_string(
        c_storage_config.talon_enable_for_external_table ? data_mode : 0);
    for (const auto& [key, value] : talon_values) {
        const bool is_mode = std::string_view(key) == PROPERTY_FS_TALON_MODE;
        if (!is_mode && value == 0) {
            continue;  // Unset in older StorageConfig messages: use storage defaults.
        }
        (*properties_map)[key] = is_mode && is_local ? uint32_t{0} : value;
    }
    if (c_storage_config.talon_coordinator != nullptr) {
        (*properties_map)[PROPERTY_FS_TALON_COORDINATOR] =
            std::string(c_storage_config.talon_coordinator);
    }
    if (c_storage_config.talon_namespace != nullptr &&
        c_storage_config.talon_namespace[0] != '\0') {
        (*properties_map)[PROPERTY_FS_TALON_NAMESPACE] =
            std::string(c_storage_config.talon_namespace);
    }

    // Carry the configured arrow reader prebuffer limits into every
    // freshly-built properties map. Index build (index_c.cpp) and the FFI
    // readers construct properties per task through this helper instead of
    // going through LoonFFIPropertiesSingleton's cached map, so without this
    // the common.arrow.reader.* configuration never reaches those reads and
    // external-table index builds are stuck with arrow's default range
    // coalescing (effectively one in-flight S3 range per file).
    milvus::storage::LoonFFIPropertiesSingleton::GetInstance()
        .ApplyArrowReaderConfig(*properties_map);

    return properties_map;
}

// kAllowedExtfsSpecKeys is the C++-side defense-in-depth allowlist for extfs
// keys that a user-supplied external_spec.extfs JSON object is allowed to
// override. This MUST stay in lock-step with allowedExtfsKeys in
// pkg/util/externalspec/external_spec.go. The Go side is the primary filter;
// this set exists so that any future bypass of the Go validation (new
// internal callers, bugs, admin tools) cannot turn external_spec into an
// arbitrary property-injection point into milvus-storage.
//
// Any key outside this set is dropped with a warning — we do not fail the
// request because the Go side should have rejected it already, so reaching
// here means either a programming error or an attempted bypass. A key that
// differs from one in this set only by case, or a duplicate key, fails the
// request instead (CheckExtfsSpecKey).
static const std::unordered_set<std::string> kAllowedExtfsSpecKeys = {
    "use_iam",
    "use_ssl",
    "use_virtual_host",
    "region",
    "cloud_provider",
    "endpoint_url",
    "iam_endpoint",
    "storage_type",
    "ssl_ca_cert",
    "access_key_id",
    "access_key_value",
    "role_arn",
    "session_name",
    "external_id",
    "load_frequency",
    "gcp_target_service_account",
    "bucket_name",
    "anonymous",
    "azure_client_id",
    "azure_tenant_id",
    "azure_credential_endpoint",
    "credential_endpoint",
};

// kExternalSpecKeys are the top-level external_spec keys Go decodes into
// specutil.ExternalSpec. Keep in sync with specKeys in
// pkg/util/externalspec/specutil/spec.go.
static const std::unordered_set<std::string> kExternalSpecKeys = {
    "format",
    "columns",
    "extfs",
    "snapshot_id",
};

// CheckTopLevelSpecKey rejects a repeated top-level key, and a second key that
// Go's encoding/json reads as the same ExternalSpec field, such as "Extfs"
// after "extfs". A lone non-canonical spelling such as "Format" is accepted:
// FindSpecField reads it as the canonical key, as Go does. Other keys are
// unknown to ExternalSpec, so only their exact repeats are rejected.
static arrow::Status
CheckTopLevelSpecKey(
    const std::string& key,
    std::unordered_set<std::string>& seen,
    std::unordered_map<std::string, std::string>& first_by_name) {
    if (!seen.emplace(key).second) {
        return arrow::Status::Invalid(
            "external_spec key '", key, "' is duplicated");
    }
    auto name = milvus::FoldExternalSpecKey(key);
    if (kExternalSpecKeys.count(name) == 0) {
        return arrow::Status::OK();
    }
    const auto [first, inserted] = first_by_name.emplace(name, key);
    if (!inserted) {
        return arrow::Status::Invalid(
            "external_spec keys '",
            first->second,
            "' and '",
            key,
            "' differ only in case; set the key once");
    }
    return arrow::Status::OK();
}

// CheckExtfsSpecKey rejects a repeated extfs key, and a key that differs from
// an allowlisted one only by case. Go keeps such a key as written and its
// allowlist rejects it, while this file would drop it.
static arrow::Status
CheckExtfsSpecKey(std::string_view key, std::unordered_set<std::string>& seen) {
    if (!seen.emplace(key).second) {
        return arrow::Status::Invalid("extfs key '", key, "' is duplicated");
    }
    if (kAllowedExtfsSpecKeys.count(std::string(key)) > 0) {
        return arrow::Status::OK();
    }
    const auto folded = milvus::FoldExternalSpecKey(key);
    if (kAllowedExtfsSpecKeys.count(folded) > 0) {
        return arrow::Status::Invalid(
            "extfs key '", key, "' must be written as '", folded, "'");
    }
    return arrow::Status::OK();
}

// CheckExternalSpecKeys rejects a spec that sets one key twice. Go's
// encoding/json reads a top-level key that equals an ExternalSpec field
// ignoring case as that field, lets a later occurrence override an earlier one
// and merges two extfs objects, while this file reads one occurrence, so the
// two sides would read different settings or credentials. Go rejects these
// specs first (specutil.CheckSpecKeys); this check covers specs persisted
// before Go did. Keys are unescaped with allow_replacement, as encoding/json
// unescapes them: an escaped lone surrogate such as "\ud800" becomes U+FFFD
// instead of failing a spec Go accepts. A spec, or an extfs value, that is not
// a JSON object has no keys to check; any other error, such as invalid UTF-8,
// fails the request, and a parse error met while iterating throws
// simdjson_error.
static arrow::Status
CheckExternalSpecKeys(const simdjson::padded_string& external_spec) {
    // A separate parser: a failed lookup can leave a document unusable, and
    // the caller iterates the spec again.
    simdjson::ondemand::parser parser;
    auto doc = parser.iterate(external_spec);
    simdjson::ondemand::object spec;
    if (const auto error = doc.get_object().get(spec);
        error != simdjson::SUCCESS) {
        if (error == simdjson::INCORRECT_TYPE) {
            return arrow::Status::OK();
        }
        return arrow::Status::Invalid("external_spec cannot be parsed: ",
                                      simdjson::error_message(error));
    }
    std::unordered_set<std::string> seen;
    std::unordered_map<std::string, std::string> first_by_name;
    for (auto field : spec) {
        const auto key = std::string(
            field.unescaped_key(/*allow_replacement=*/true).value());
        ARROW_RETURN_NOT_OK(CheckTopLevelSpecKey(key, seen, first_by_name));
        if (milvus::FoldExternalSpecKey(key) != "extfs") {
            continue;
        }
        simdjson::ondemand::object extfs;
        if (const auto error = field.value().get_object().get(extfs);
            error != simdjson::SUCCESS) {
            if (error == simdjson::INCORRECT_TYPE) {
                continue;
            }
            return arrow::Status::Invalid(
                "external_spec extfs cannot be parsed: ",
                simdjson::error_message(error));
        }
        std::unordered_set<std::string> seen_extfs;
        for (auto extfs_field : extfs) {
            ARROW_RETURN_NOT_OK(CheckExtfsSpecKey(
                extfs_field.unescaped_key(/*allow_replacement=*/true).value(),
                seen_extfs));
        }
    }
    return arrow::Status::OK();
}

// FindSpecField returns the top-level field that Go's encoding/json reads as
// name, a key in kExternalSpecKeys: its unescaped key folds to name, so a lone
// "Format" or "\u0065xtfs" is found as format or extfs. simdjson's find_field
// compares raw bytes and would miss both. CheckExternalSpecKeys has rejected a
// spec with two keys that fold to the same name, so at most one field matches.
// doc must be freshly iterated: rewinding a document after a parse error
// dereferences a released parser.
static simdjson::simdjson_result<simdjson::ondemand::value>
FindSpecField(simdjson::simdjson_result<simdjson::ondemand::document>& doc,
              std::string_view name) {
    simdjson::ondemand::object spec;
    if (const auto error = doc.get_object().get(spec);
        error != simdjson::SUCCESS) {
        return error;
    }
    for (auto field : spec) {
        std::string_view key;
        if (const auto error =
                field.unescaped_key(/*allow_replacement=*/true).get(key);
            error != simdjson::SUCCESS) {
            return error;
        }
        if (milvus::FoldExternalSpecKey(key) == name) {
            return field.value();
        }
    }
    return simdjson::NO_SUCH_FIELD;
}

// kExtfsFields is the contract with Go extfsFields in
// internal/storagev2/packed/ffi_common.go. Keep in lockstep.
static const std::vector<std::pair<std::string, bool /*is_bool*/>>
    kExtfsFields = {
        {"storage_type", false},
        {"bucket_name", false},
        {"address", false},
        {"root_path", false},
        {"access_key_id", false},
        {"access_key_value", false},
        {"cloud_provider", false},
        {"iam_endpoint", false},
        {"region", false},
        {"ssl_ca_cert", false},
        {"gcp_target_service_account", false},
        {"use_ssl", true},
        {"use_iam", true},
        {"use_virtual_host", true},
        {"anonymous", true},
};

static std::string
ExternalFsPropertyKey(const std::string& extfs_prefix,
                      const char* fs_property) {
    constexpr std::string_view fs_prefix = PROPERTY_FS_PREFIX;
    std::string_view property(fs_property);
    AssertInfo(property.substr(0, fs_prefix.size()) == fs_prefix,
               "Invalid filesystem property key: {}",
               std::string(property));
    return extfs_prefix + std::string(property.substr(fs_prefix.size()));
}

// kExtfsInheritedFsFields lists fs.* properties that extfs.{collectionID}.*
// inherits verbatim from the process-level storage config.
//
// milvus-storage builds each external filesystem purely from its own
// extfs.<name>.* keys (ExtractExternalFsProperties) — there is no fallback to
// fs.*, and a missing key silently takes the library's built-in default. So
// anything that is server-side capacity/tuning config rather than part of the
// external source's identity has to be copied across explicitly, or it is a
// no-op on every external read.
//
// These are deliberately NOT in kAllowedExtfsSpecKeys: they come from
// milvus.yaml, not from a user-supplied external_spec.
static const std::vector<const char*> kExtfsInheritedFsFields = {
    PROPERTY_FS_MAX_CONNECTIONS,
    PROPERTY_FS_S3_CRT_QUEUE_SIZES,
};

// PropertyValueAsString renders a PropertyVariant back to the string form
// SetValue accepts. Both variant shapes reach here: the C++ index-build path
// goes through SetValue, which stores registry-typed values (max_connections
// is UINT32), while loon_properties_inject_external_spec rebuilds the map
// from a flat LoonProperties and stores everything as std::string.
static std::optional<std::string>
PropertyValueAsString(const milvus_storage::api::Properties& properties,
                      const char* key) {
    auto it = properties.find(key);
    if (it == properties.end()) {
        return std::nullopt;
    }
    return std::visit(
        [](const auto& v) -> std::optional<std::string> {
            using T = std::decay_t<decltype(v)>;
            if constexpr (std::is_same_v<T, std::string>) {
                return v.empty() ? std::nullopt : std::optional<std::string>(v);
            } else if constexpr (std::is_same_v<T, bool>) {
                return std::optional<std::string>(v ? "true" : "false");
            } else if constexpr (std::is_integral_v<T>) {
                return std::optional<std::string>(std::to_string(v));
            } else {
                // vector<string> / nullptr_t: no scalar rendering, and no
                // inherited field uses them.
                return std::nullopt;
            }
        },
        it->second);
}

static std::string
DeriveUseSSLFromScheme(const std::string& scheme) {
    if (scheme == "minio") {
        return "false";
    }
    return "true";
}

// DeriveEndpoint: mirror of Go externalspec.DeriveEndpoint. Keep in lockstep.
static std::string
DeriveEndpoint(const std::string& cloud_provider, const std::string& region) {
    std::string cp = cloud_provider;
    std::transform(cp.begin(), cp.end(), cp.begin(), [](unsigned char c) {
        return std::tolower(c);
    });
    if (cp == "aws") {
        if (region.empty()) {
            return "";
        }
        if (region.rfind("cn-", 0) == 0) {
            return "https://s3." + region + ".amazonaws.com.cn";
        }
        return "https://s3." + region + ".amazonaws.com";
    }
    if (cp == "gcp") {
        return "https://storage.googleapis.com";
    }
    if (cp == "aliyun") {
        if (region.empty()) {
            return "";
        }
        return "https://oss-" + region + ".aliyuncs.com";
    }
    if (cp == "tencent") {
        if (region.empty()) {
            return "";
        }
        return "https://cos." + region + ".myqcloud.com";
    }
    if (cp == "huawei") {
        if (region.empty()) {
            return "";
        }
        return "https://obs." + region + ".myhuaweicloud.com";
    }
    if (cp == "azure") {
        // Azure endpoint derivation: "region" slot selects sovereign cloud.
        // AzureFileSystemProducer concatenates ".blob."/".dfs." onto
        // config_.address verbatim, so the returned value is a bare authority.
        //
        // Azurite / Azure Stack Hub: use Milvus-form URI
        // (azure://<custom-host>/<container>/<blob>) instead of derive.
        std::string r = region;
        std::transform(r.begin(), r.end(), r.begin(), [](unsigned char c) {
            return std::tolower(c);
        });
        if (r.empty()) {
            // Empty region → not derivable. Caller must supply Milvus-form URI.
            return "";
        }
        if (r.rfind("china", 0) == 0) {
            return "core.chinacloudapi.cn";
        }
        if (r.rfind("usgov", 0) == 0 || r.rfind("usdod", 0) == 0) {
            return "core.usgovcloudapi.net";
        }
        // Microsoft Cloud Germany (core.cloudapi.de) closed in October 2021;
        // germanywestcentral and germanynorth are public Azure regions.
        return "core.windows.net";
    }
    return "";
}

static std::string
StripURIScheme(const std::string& addr) {
    auto pos = addr.find("://");
    return (pos != std::string::npos) ? addr.substr(pos + 3) : addr;
}

// IsAzureStorageAuthority returns true for the bare Azure Storage
// authorities that azure://<authority>/<container>/<blob> names. An Azure
// container name cannot contain a dot, so such a host is never a container.
static bool
IsAzureStorageAuthority(const std::string& host) {
    std::string h = host;
    std::transform(h.begin(), h.end(), h.begin(), [](unsigned char c) {
        return std::tolower(c);
    });
    return h == "core.windows.net" || h == "core.chinacloudapi.cn" ||
           h == "core.usgovcloudapi.net" || h == "core.cloudapi.de";
}

// Keep the endpoint domain families in sync with Go externalspec.IsCloudEndpointHost.
static std::string_view
CloudEndpointProvider(const std::string& host) {
    const auto h = arrow::internal::AsciiToLower(host);
    if (h.ends_with(".amazonaws.com") || h.ends_with(".amazonaws.com.cn")) {
        return "aws";
    }
    if (h.ends_with(".googleapis.com")) {
        return "gcp";
    }
    if (h.ends_with(".aliyuncs.com")) {
        return "aliyun";
    }
    if (h.ends_with(".myqcloud.com")) {
        return "tencent";
    }
    if (h.ends_with(".myhuaweicloud.com")) {
        return "huawei";
    }
    if (h.ends_with(".core.windows.net") ||
        h.ends_with(".core.chinacloudapi.cn") ||
        h.ends_with(".core.usgovcloudapi.net") ||
        h.ends_with(".core.cloudapi.de")) {
        return "azure";
    }
    return {};
}

static arrow::Status
InjectExternalSpecProperties(
    milvus_storage::api::Properties& properties,
    int64_t collection_id,
    const std::string& external_source,
    const std::string& external_spec,
    const milvus::storage::ExternalIopsConfig& iops_config) {
    std::string extfs_prefix = std::string(PROPERTY_EXTFS_PREFIX) +
                               std::to_string(collection_id) + ".";

    // Only set registered boolean defaults. Storage has no fs.anonymous key.
    for (const auto& [name, is_bool] : kExtfsFields) {
        if (is_bool && name != "anonymous") {
            properties[extfs_prefix + name] = std::string("false");
        }
    }

    // Absent keys default to minioadmin in storage and conflict with GCP tokens.
    properties[extfs_prefix + "access_key_id"] = std::string();
    properties[extfs_prefix + "access_key_value"] = std::string();

    // IOPS limiting is an External Table read policy, not an attribute of the
    // internal filesystem. Keep it out of fs.* and stamp it only into the
    // per-collection extfs namespace. These keys are intentionally absent
    // from kAllowedExtfsSpecKeys, so external_spec cannot override them.
    const auto initial_rate_key =
        ExternalFsPropertyKey(extfs_prefix, PROPERTY_FS_IOPS_INITIAL_RATE);
    const auto max_rate_key =
        ExternalFsPropertyKey(extfs_prefix, PROPERTY_FS_IOPS_MAX_RATE);
    milvus_storage::api::SetValue(
        properties,
        initial_rate_key.c_str(),
        std::to_string(iops_config.initial_rate).c_str());
    milvus_storage::api::SetValue(properties,
                                  max_rate_key.c_str(),
                                  std::to_string(iops_config.max_rate).c_str());

    // Layer 0b: inherit process-level fs.* tuning. Position relative to the
    // later layers is arbitrary — none of these keys is in
    // kAllowedExtfsSpecKeys nor derived from the URI, so no other layer
    // writes them; it sits next to Layer 0 because both are defaults rather
    // than external-source-derived values.
    // "0" is treated as unset, like the empty string: for every fs.* tunable
    // inherited here, 0 means "producer did not set it" (the convention in
    // ChunkManager.cpp, MinioChunkManager.cpp and the Go producer). Mirroring
    // it would be worse than not mirroring at all — before this key existed on
    // the extfs side, ExtractExternalFsProperties fell back to the library
    // default (100 connections), whereas an explicit "0" overrides it and
    // caps the external S3 client at max(io_capacity, 25). The producers are
    // guarded too; this is the last line of defence for the read path this
    // whole change exists to widen.
    for (const auto* fs_key : kExtfsInheritedFsFields) {
        auto value = PropertyValueAsString(properties, fs_key);
        if (!value.has_value() || *value == "0") {
            continue;
        }
        const auto extfs_key = ExternalFsPropertyKey(extfs_prefix, fs_key);
        milvus_storage::api::SetValue(
            properties, extfs_key.c_str(), value->c_str());
    }

    // Layer 1: derive bucket / address / storage_type / use_ssl from URI.
    // Caller must have run Go ValidateExternalSource; malformed here
    // signals etcd corruption or bypass — fail adjacent to bad input.
    auto scheme_end = external_source.find("://");
    AssertInfo(scheme_end != std::string::npos,
               "external_source for collection {} missing scheme: {}",
               collection_id,
               external_source);

    std::string scheme = external_source.substr(0, scheme_end);
    auto rest = external_source.substr(scheme_end + 3);
    rest = rest.substr(0, rest.find_first_of("?#"));
    auto slash_pos = rest.find('/');
    // No slash after authority (e.g. "s3://mybucket"): entire rest is host,
    // path is empty. Only assert against the pathological "scheme://" case
    // where rest itself is empty.
    std::string host =
        (slash_pos != std::string::npos) ? rest.substr(0, slash_pos) : rest;
    AssertInfo(!host.empty(),
               "external_source for collection {} has empty host: {}",
               collection_id,
               external_source);

    std::string path_part =
        (slash_pos != std::string::npos) ? rest.substr(slash_pos + 1) : "";
    auto bucket_end = path_part.find('/');
    std::string path_bucket = (bucket_end != std::string::npos)
                                  ? path_part.substr(0, bucket_end)
                                  : path_part;

    milvus_storage::api::SetValue(
        properties, (extfs_prefix + "storage_type").c_str(), "remote");
    milvus_storage::api::SetValue(properties,
                                  (extfs_prefix + "use_ssl").c_str(),
                                  DeriveUseSSLFromScheme(scheme).c_str());

    // Default to Milvus-form (URI.host as endpoint). Layer 3 may rewrite
    // to AWS-form when DeriveEndpoint(cp, region) differs from URI.host.
    std::string address = host;
    if (address.find("://") == std::string::npos &&
        DeriveUseSSLFromScheme(scheme) == "false") {
        address = "http://" + address;
    }
    milvus_storage::api::SetValue(
        properties, (extfs_prefix + "address").c_str(), address.c_str());
    if (!path_bucket.empty()) {
        milvus_storage::api::SetValue(properties,
                                      (extfs_prefix + "bucket_name").c_str(),
                                      path_bucket.c_str());
    }

    // Layer 2: apply spec.extfs JSON. Gate every key through
    // kAllowedExtfsSpecKeys — defense-in-depth against property injection
    // if a caller bypasses Go ParseExternalSpec. Note: "address" is NOT in
    // the allowlist. endpoint_url is captured below and translated to the
    // internal address property instead of being forwarded verbatim.
    std::string spec_cloud_provider;
    std::string spec_region;
    std::string spec_endpoint_url;
    std::optional<std::string> spec_bucket_name;
    bool has_spec_endpoint_url = false;
    std::string broker_endpoint;
    bool legacy_broker = false;
    if (!external_spec.empty()) {
        try {
            simdjson::ondemand::parser parser;
            simdjson::padded_string padded(external_spec);
            ARROW_RETURN_NOT_OK(CheckExternalSpecKeys(padded));
            auto doc = parser.iterate(padded);
            auto extfs_obj = FindSpecField(doc, "extfs");
            // NO_SUCH_FIELD means no extfs, and INCORRECT_TYPE that the spec
            // is not a JSON object (Go reads null as a spec without extfs).
            // Any other error must not silently drop the spec's extfs.
            if (const auto error = extfs_obj.error();
                error != simdjson::SUCCESS &&
                error != simdjson::NO_SUCH_FIELD &&
                error != simdjson::INCORRECT_TYPE) {
                return arrow::Status::Invalid(
                    "external_spec extfs cannot be read: ",
                    simdjson::error_message(error));
            }
            if (extfs_obj.error() == simdjson::SUCCESS) {
                for (auto field : extfs_obj.get_object()) {
                    try {
                        auto key = std::string(
                            field.unescaped_key(/*allow_replacement=*/true)
                                .value());
                        if (kAllowedExtfsSpecKeys.find(key) ==
                            kAllowedExtfsSpecKeys.end()) {
                            LOG_WARN(
                                "extfs key '{}' not in allowlist, dropped "
                                "(collection_id={}); Go-side validation "
                                "should have already rejected this",
                                key,
                                collection_id);
                            continue;
                        }
                        // Match Go's map[string]string: null is an empty string.
                        auto json_value = field.value();
                        std::string_view text;
                        if (!json_value.is_null().value() &&
                            json_value.get_string().get(text) !=
                                simdjson::SUCCESS) {
                            return arrow::Status::Invalid(
                                "extfs.", key, " must be a JSON string");
                        }
                        auto val = std::string(text);
                        if (key == "endpoint_url") {
                            has_spec_endpoint_url = true;
                            spec_endpoint_url = val;
                            continue;
                        }
                        if (key == "bucket_name") {
                            spec_bucket_name = std::move(val);
                            continue;
                        }
                        if (val.empty()) {
                            continue;
                        }
                        if (key == "credential_endpoint" ||
                            key == "azure_credential_endpoint") {
                            ARROW_ASSIGN_OR_RAISE(auto endpoint,
                                                  NormalizeBrokerEndpoint(val));
                            if (!broker_endpoint.empty() &&
                                broker_endpoint != endpoint) {
                                return arrow::Status::Invalid(
                                    "Conflicting credential broker endpoints");
                            }
                            broker_endpoint = std::move(endpoint);
                            legacy_broker = legacy_broker ||
                                            key == "azure_credential_endpoint";
                            continue;
                        }
                        if (key == "anonymous" && val == "false") {
                            // Explicit false clears the override. The storage
                            // property registry has no fs.anonymous entry.
                            properties.erase(extfs_prefix + key);
                            continue;
                        }
                        if (key == "cloud_provider") {
                            spec_cloud_provider = val;
                        } else if (key == "region") {
                            spec_region = val;
                        }
                        // "minio" is a Milvus-only cloud_provider sentinel
                        // meaning "self-hosted S3-compatible, do not derive
                        // endpoint and do not swap host". loon's allowlist
                        // does not include it, so propagate the value only
                        // for our swap-decision logic above and drop it
                        // before reaching loon.
                        if (key == "cloud_provider" && val == "minio") {
                            continue;
                        }
                        milvus_storage::api::SetValue(
                            properties,
                            (extfs_prefix + key).c_str(),
                            val.c_str());
                    } catch (const simdjson::simdjson_error& per_field) {
                        // A field that cannot be read would leave the spec
                        // partially applied.
                        return arrow::Status::Invalid(
                            "extfs field parse failed: ", per_field.what());
                    }
                }
            }
        } catch (const simdjson::simdjson_error& e) {
            LOG_ERROR(
                "Failed to parse external_spec for extfs overrides "
                "(collection_id={}): {}",
                collection_id,
                e.what());
            ThrowInfo(milvus::ErrorCode::UnexpectedError,
                      "external_spec parse failed for collection {}: {}",
                      collection_id,
                      e.what());
        }
    }

    const auto value = [&](const char* key) {
        return PropertyValueAsString(properties, (extfs_prefix + key).c_str())
            .value_or("");
    };
    const bool use_iam =
        arrow::internal::AsciiEqualsCaseInsensitive(value("use_iam"), "true");
    const bool has_target = !value("gcp_target_service_account").empty();
    const auto validate_token_lifetime = [&](const char* mode) {
        const auto lifetime = value("load_frequency");
        if (lifetime.empty()) {
            return arrow::Status::OK();
        }
        uint64_t seconds = 0;
        const auto* end = lifetime.data() + lifetime.size();
        const auto [ptr, error] =
            std::from_chars(lifetime.data(), end, seconds);
        if (error != std::errc{} || ptr != end) {
            return arrow::Status::Invalid(mode,
                                          " has an invalid token lifetime");
        }
        if (seconds < 900 || seconds > 3600) {
            return arrow::Status::Invalid(
                mode, " requires load_frequency in [900, 3600]");
        }
        return arrow::Status::OK();
    };
    if (!broker_endpoint.empty()) {
        if (legacy_broker && spec_cloud_provider != "azure") {
            return arrow::Status::Invalid(
                "azure_credential_endpoint requires cloud_provider=azure");
        }
        if (spec_cloud_provider != "aws" && spec_cloud_provider != "aliyun" &&
            spec_cloud_provider != "tencent" &&
            spec_cloud_provider != "azure" && spec_cloud_provider != "gcp") {
            return arrow::Status::Invalid(
                "Unsupported credential broker cloud provider");
        }
        const auto reject_fields =
            [&](const char* mode, std::initializer_list<const char*> keys) {
                for (const auto* key : keys) {
                    if (!value(key).empty()) {
                        return arrow::Status::Invalid(
                            "Inactive credential field in ", mode, ": ", key);
                    }
                }
                return arrow::Status::OK();
            };
        if (spec_cloud_provider == "azure") {
            if (value("access_key_id").empty()) {
                return arrow::Status::Invalid(
                    "Azure broker requires access_key_id");
            }
            if (use_iam) {
                // Existing Azure target specs may carry unused S3 session fields.
                ARROW_RETURN_NOT_OK(reject_fields("Azure broker IAM",
                                                  {"azure_client_id",
                                                   "azure_tenant_id",
                                                   "external_id",
                                                   "session_name"}));
                ARROW_RETURN_NOT_OK(
                    validate_token_lifetime("Azure broker IAM"));
            } else if (value("azure_client_id").empty() ||
                       value("azure_tenant_id").empty() ||
                       value("region").empty()) {
                return arrow::Status::Invalid(
                    "Azure broker requires azure_client_id, azure_tenant_id "
                    "and region without use_iam=true");
            }
            ARROW_RETURN_NOT_OK(reject_fields("Azure broker",
                                              {"access_key_value",
                                               "role_arn",
                                               "gcp_target_service_account"}));
            if (value("anonymous") == "true") {
                return arrow::Status::Invalid(
                    "anonymous conflicts with Azure broker");
            }
        } else if (spec_cloud_provider == "gcp" || use_iam) {
            const bool is_gcp = spec_cloud_provider == "gcp";
            if (is_gcp && use_iam == has_target) {
                return arrow::Status::Invalid(
                    "GCP broker requires exactly one of use_iam=true and "
                    "gcp_target_service_account");
            }
            if (!is_gcp && has_target) {
                return arrow::Status::Invalid(
                    "Inactive credential field in broker IAM: "
                    "gcp_target_service_account");
            }
            ARROW_RETURN_NOT_OK(
                reject_fields(is_gcp ? "GCP broker" : "broker IAM",
                              {"role_arn",
                               "external_id",
                               "session_name",
                               "access_key_id",
                               "access_key_value",
                               "azure_client_id",
                               "azure_tenant_id"}));
        }
        ARROW_RETURN_NOT_OK(CheckCredentialBrokerEndpoint(broker_endpoint));
        // The legacy Azure key delegates to a target identity on storage 3.0.
        // Broker IAM must use the generic endpoint to select the broker identity.
        const bool azure_target = spec_cloud_provider == "azure" && !use_iam;
        properties.erase(extfs_prefix + (azure_target
                                             ? "credential_endpoint"
                                             : "azure_credential_endpoint"));
        milvus_storage::api::SetValue(
            properties,
            (extfs_prefix + (azure_target ? "azure_credential_endpoint"
                                          : "credential_endpoint"))
                .c_str(),
            broker_endpoint.c_str());
    }

    // Layer 3: resolve the effective endpoint.
    //
    // endpoint_url selects explicit-endpoint mode. In this mode
    // external_source has the standard scheme://bucket/key shape: URI.host is
    // the bucket, while endpoint_url is the physical S3-compatible address.
    // The Go API boundary validates the full URL contract. The checks here are
    // defense in depth for corrupted metadata or callers that bypass Go.
    if (has_spec_endpoint_url) {
        if (spec_bucket_name.has_value()) {
            return arrow::Status::Invalid(
                "extfs.bucket_name cannot be set with extfs.endpoint_url; "
                "the bucket comes from external_source");
        }
        std::string effective_endpoint = spec_endpoint_url;
        if (!effective_endpoint.empty() && effective_endpoint.back() == '/') {
            effective_endpoint.pop_back();
        }
        auto endpoint_scheme_end = effective_endpoint.find("://");
        AssertInfo(endpoint_scheme_end != std::string::npos,
                   "extfs.endpoint_url for collection {} missing scheme",
                   collection_id);
        std::string endpoint_scheme =
            effective_endpoint.substr(0, endpoint_scheme_end);
        std::transform(endpoint_scheme.begin(),
                       endpoint_scheme.end(),
                       endpoint_scheme.begin(),
                       [](unsigned char c) { return std::tolower(c); });
        AssertInfo(endpoint_scheme == "http" || endpoint_scheme == "https",
                   "extfs.endpoint_url for collection {} must use http or "
                   "https",
                   collection_id);
        std::string endpoint_authority =
            effective_endpoint.substr(endpoint_scheme_end + 3);
        AssertInfo(
            !endpoint_authority.empty() &&
                endpoint_authority.find_first_of("/@?#") == std::string::npos,
            "extfs.endpoint_url for collection {} must contain only "
            "scheme and authority",
            collection_id);
        AssertInfo(
            host.find(':') == std::string::npos,
            "external_source host for collection {} must be a bucket name "
            "without a port when extfs.endpoint_url is set",
            collection_id);

        effective_endpoint = endpoint_scheme + "://" + endpoint_authority;
        milvus_storage::api::SetValue(properties,
                                      (extfs_prefix + "address").c_str(),
                                      effective_endpoint.c_str());
        milvus_storage::api::SetValue(
            properties, (extfs_prefix + "bucket_name").c_str(), host.c_str());
        milvus_storage::api::SetValue(
            properties,
            (extfs_prefix + "use_ssl").c_str(),
            endpoint_scheme == "https" ? "true" : "false");
    } else {
        // AWS-form disambiguation via derived-endpoint vs URI.host.
        //   Compute DeriveEndpoint(cp, region). If non-empty and different
        //   from URI.host, source is AWS-form (scheme://bucket/key) — swap
        //   URI.host into bucket_name and write derived endpoint as address.
        //   Otherwise Milvus-form (URI.host == endpoint) or unresolvable —
        //   keep Layer1 values.
        //
        //   Path-segment heuristics are unreliable: AWS-form S3 keys may
        //   contain '/' (`s3://bucket/deep/key.parquet`) and Milvus-form URIs
        //   may omit keys (`s3://endpoint/bucket`). Comparing derive result
        //   with host handles both cases correctly — the derived endpoint
        //   string is a stable cloud-provider identifier, not dependent on
        //   path structure.
        // Swap decision uses ONLY the user-supplied cloud_provider, never a
        // scheme-inferred fallback. Self-hosted MinIO with
        // `s3://localhost:9000/bucket/...` is Milvus-form; inferring
        // cloud_provider=aws from scheme=s3 would produce
        // derived=https://s3.<region>.amazonaws.com, falsely classify host
        // as a bucket, and swap. When the user does not declare
        // cloud_provider, DeriveEndpoint returns empty and the URI is
        // treated as Milvus-form (host is endpoint).
        std::string derived = DeriveEndpoint(spec_cloud_provider, spec_region);
        // Cloud-family host suffix check: if URI.host ends with a known cloud
        // provider domain (AWS, GCP, Aliyun, Tencent, Huawei, Azure — all
        // endpoint variants including global/accelerate/dualstack/FIPS/VPC),
        // the URI is Milvus-form and URI.host is authoritative. Skip swap
        // regardless of whether DeriveEndpoint string-matches. This prevents
        // misclassifying `s3://s3.amazonaws.com/bucket/key` as AWS-form when
        // spec.region is a regional value.
        // A bare Azure Storage authority is the endpoint the source names, so
        // it also wins over the region-derived authority.
        std::string spec_cp = spec_cloud_provider;
        std::transform(spec_cp.begin(),
                       spec_cp.end(),
                       spec_cp.begin(),
                       [](unsigned char c) { return std::tolower(c); });
        const auto colon = host.find(':');
        const auto provider = CloudEndpointProvider(host.substr(0, colon));
        bool uri_host_is_cloud_endpoint =
            (colon == std::string::npos && !provider.empty()) ||
            (spec_cp == "azure" && IsAzureStorageAuthority(host));
        if (colon != std::string::npos && provider == spec_cp &&
            (provider == "aws" || provider == "aliyun" ||
             provider == "tencent" ||
             (provider == "gcp" &&
              (arrow::internal::AsciiEqualsCaseInsensitive(scheme, "gs") ||
               arrow::internal::AsciiEqualsCaseInsensitive(scheme, "gcs"))))) {
            // Classify the DNS hostname, but keep the complete address for
            // matching the storage URI and connecting to its explicit port.
            arrow::util::Uri endpoint;
            if (!endpoint.Parse("https://" + host).ok() ||
                endpoint.port() <= 0 || endpoint.port() > 65535 ||
                !endpoint.username().empty() || !endpoint.password().empty()) {
                return arrow::Status::Invalid(
                    "Cloud endpoint requires a port between 1 and 65535");
            }
            uri_host_is_cloud_endpoint = true;
        }
        const bool bucket_in_host = !uri_host_is_cloud_endpoint &&
                                    !derived.empty() &&
                                    StripURIScheme(derived) != host;
        // StorageUri::Parse decodes the path before splitting out the bucket.
        auto source_bucket =
            bucket_in_host ? host : arrow::util::UriUnescape(path_bucket);
        source_bucket = source_bucket.substr(0, source_bucket.find('/'));
        if (spec_bucket_name && !spec_bucket_name->empty() &&
            *spec_bucket_name != source_bucket) {
            return arrow::Status::Invalid(
                "extfs.bucket_name must match the bucket in "
                "external_source");
        }
        if (!bucket_in_host && !path_bucket.empty()) {
            if (spec_cp == "azure" && !broker_endpoint.empty() && use_iam &&
                path_bucket != source_bucket) {
                return arrow::Status::Invalid(
                    "Azure broker IAM requires an unescaped source "
                    "container");
            }
            milvus_storage::api::SetValue(
                properties,
                (extfs_prefix + "bucket_name").c_str(),
                source_bucket.c_str());
        }
        if (bucket_in_host) {
            milvus_storage::api::SetValue(
                properties,
                (extfs_prefix + "bucket_name").c_str(),
                host.c_str());
            milvus_storage::api::SetValue(properties,
                                          (extfs_prefix + "address").c_str(),
                                          derived.c_str());
        }
    }

    // Reconcile bare address with final use_ssl. Addresses that already
    // carry a scheme win. Azure is exempt: AzureFileSystemProducer does
    // `options.blob_storage_authority = ".blob." + config_.address`, so the
    // address must be a bare authority (e.g. "core.windows.net"); prepending
    // a scheme would break the concatenation.
    auto final_address = value("address");
    const bool use_ssl = value("use_ssl") == "true";
    if (value("cloud_provider") != "azure" && !final_address.empty() &&
        final_address.find("://") == std::string::npos) {
        final_address = (use_ssl ? "https://" : "http://") + final_address;
        milvus_storage::api::SetValue(properties,
                                      (extfs_prefix + "address").c_str(),
                                      final_address.c_str());
    }

    // Target impersonation and Broker tokens have the same endpoint contract
    // in Go admission and native readers, including persisted specs. Plain IAM
    // retains storage's operator-trusted endpoint policy.
    if (spec_cloud_provider == "gcp" &&
        (has_target || !broker_endpoint.empty())) {
        arrow::util::Uri endpoint;
        ARROW_RETURN_NOT_OK(endpoint.Parse(final_address));
        const auto hostname = arrow::internal::AsciiToLower(endpoint.host());
        constexpr std::string_view suffix = ".googleapis.com";
        if (!use_ssl || endpoint.scheme() != "https" ||
            hostname.size() <= suffix.size() || !hostname.ends_with(suffix)) {
            return arrow::Status::Invalid(
                "GCP Bearer credentials require HTTPS and a *.googleapis.com "
                "endpoint");
        }
        ARROW_RETURN_NOT_OK(validate_token_lifetime("GCP Bearer credentials"));
    }

    const bool origin_only = value("storage_type") == "local";
    for (const auto* key : {PROPERTY_FS_TALON_MODE,
                            PROPERTY_FS_TALON_OPEN_ONLY,
                            PROPERTY_FS_TALON_SMALL_READ_THRESHOLD,
                            PROPERTY_FS_TALON_COORDINATOR,
                            PROPERTY_FS_TALON_NAMESPACE,
                            PROPERTY_FS_TALON_BLOCK_SIZE,
                            PROPERTY_FS_TALON_MAX_IDLE_PER_ADDR}) {
        const auto* source_key = std::string_view(key) == PROPERTY_FS_TALON_MODE
                                     ? "milvus.talon.external_mode"
                                     : key;
        const auto value = PropertyValueAsString(properties, source_key);
        const auto extfs_key = ExternalFsPropertyKey(extfs_prefix, key);
        if (origin_only) {
            properties.erase(extfs_key);
        } else if (value.has_value()) {
            properties[extfs_key] = *value;
        }
    }
    // Format-layer: emit per-format properties derived from spec.format.
    // Lance and Iceberg use reader.exttable.snapshot_id.
    if (external_spec.empty()) {
        return arrow::Status::OK();
    }
    try {
        simdjson::ondemand::parser parser;
        simdjson::padded_string padded(external_spec);
        auto doc = parser.iterate(padded);
        std::string_view format_view;
        if (FindSpecField(doc, "format").get_string().get(format_view) ==
            simdjson::SUCCESS) {
            std::string format{format_view};
            if (format == "lance-table" || format == "iceberg-table") {
                // JSON fields may appear before or after "format", so
                // search a fresh iteration.
                doc = parser.iterate(padded);
                auto snapshot_field = FindSpecField(doc, "snapshot_id");
                int64_t snapshot_id = 0;
                auto int_err = snapshot_field.get_int64().get(snapshot_id);
                if (int_err == simdjson::INCORRECT_TYPE) {
                    std::string_view snapshot_view;
                    if (snapshot_field.get_string().get(snapshot_view) ==
                        simdjson::SUCCESS) {
                        try {
                            snapshot_id =
                                std::stoll(std::string(snapshot_view));
                            int_err = simdjson::SUCCESS;
                        } catch (const std::exception& e) {
                            LOG_WARN(
                                "iceberg snapshot_id parse failed "
                                "(collection_id={}): {}",
                                collection_id,
                                e.what());
                        }
                    }
                }
                if (int_err == simdjson::SUCCESS) {
                    const auto error = milvus_storage::api::SetValue(
                        properties,
                        PROPERTY_READER_EXTTABLE_SNAPSHOT_ID,
                        std::to_string(snapshot_id).c_str());
                    if (error.has_value()) {
                        // An invalid snapshot must not fall back to latest.
                        return arrow::Status::Invalid(
                            PROPERTY_READER_EXTTABLE_SNAPSHOT_ID, ": ", *error);
                    }
                }
            }
        }
    } catch (const simdjson::simdjson_error& e) {
        // Top-level parse failure already caught by the extfs block above;
        // this second pass only runs after the first succeeded.
        LOG_WARN("format-props parse failed (collection_id={}): {}",
                 collection_id,
                 e.what());
    }
    return arrow::Status::OK();
}

void
InjectExternalSpecProperties(milvus_storage::api::Properties& properties,
                             int64_t collection_id,
                             const std::string& external_source,
                             const std::string& external_spec) {
    // Native C++ callers rely on the process policy initialized during
    // component startup.
    const auto iops_config =
        milvus::storage::LoonFFIPropertiesSingleton::GetInstance()
            .GetExternalIopsConfig();
    const auto status = InjectExternalSpecProperties(
        properties, collection_id, external_source, external_spec, iops_config);
    if (!status.ok()) {
        ThrowInfo(milvus::InvalidParameter, "{}", status.message());
    }
}

std::string
LoonFSRootPath(const std::string& storage_type, const std::string& root_path) {
    if (storage_type == "local") {
        return kLoonLocalFSRootPath;
    }
    return root_path;
}

CStorageConfig
ToCStorageConfig(const milvus::storage::StorageConfig& config) {
    return CStorageConfig{config.address.c_str(),
                          config.bucket_name.c_str(),
                          config.access_key_id.c_str(),
                          config.access_key_value.c_str(),
                          config.root_path.c_str(),
                          config.storage_type.c_str(),
                          config.cloud_provider.c_str(),
                          config.iam_endpoint.c_str(),
                          config.log_level.c_str(),
                          config.region.c_str(),
                          config.useSSL,
                          config.sslCACert.c_str(),
                          config.useIAM,
                          config.useVirtualHost,
                          config.requestTimeoutMs,
                          config.gcp_credential_json.c_str(),
                          false,  // this field does not exist in StorageConfig
                          config.max_connections,
                          config.tls_min_version.c_str(),
                          config.use_crc32c_checksum,
                          0,  // use the process-wide CRT queue size
                          config.talon_mode,
                          config.talon_small_read_threshold,
                          config.talon_coordinator.c_str(),
                          config.talon_block_size,
                          config.talon_max_idle_per_addr,
                          config.talon_enable_for_external_table,
                          config.talon_enabled_stages,
                          config.talon_namespace.c_str()};
}

std::shared_ptr<milvus_storage::api::Manifest>
GetLoonManifest(
    const std::string& path,
    const std::shared_ptr<const milvus_storage::api::Properties>& properties) {
    try {
        // Parse the JSON string
        json j = json::parse(path);

        // Extract base_path & version
        std::string base_path = j.at("base_path").get<std::string>();
        int64_t version = j.at("ver").get<int64_t>();

        auto fs_result =
            milvus_storage::FilesystemCache::getInstance().get(*properties);
        if (!fs_result.ok()) {
            auto error = milvus_storage::ToSegcoreError(fs_result.status());
            ThrowInfo(error.get_error_code(),
                      "Failed to get filesystem: {}",
                      error.what());
        }
        auto fs = std::move(fs_result.ValueOrDie());

        auto transaction_result =
            milvus_storage::api::transaction::Transaction::Open(
                fs,
                base_path,
                version,
                milvus_storage::api::transaction::FailResolver,
                1);
        if (!transaction_result.ok()) {
            auto error =
                milvus_storage::ToSegcoreError(transaction_result.status());
            ThrowInfo(error.get_error_code(),
                      "Failed to open transaction: {}",
                      error.what());
        }
        auto transaction = std::move(transaction_result.ValueOrDie());
        auto manifest_result = transaction->GetManifest();
        if (!manifest_result.ok()) {
            auto error =
                milvus_storage::ToSegcoreError(manifest_result.status());
            ThrowInfo(error.get_error_code(),
                      "Failed to get manifest: {}",
                      error.what());
        }
        auto current_manifest = manifest_result.ValueOrDie();
        return current_manifest;
    } catch (const json::parse_error& e) {
        throw std::runtime_error(
            std::string("Failed to parse manifest JSON: ") + e.what());
    } catch (const json::out_of_range& e) {
        throw std::runtime_error(
            std::string("Missing required field in manifest: ") + e.what());
    } catch (const json::type_error& e) {
        throw std::runtime_error(
            std::string("Invalid field type in manifest: ") + e.what());
    }
}

// ==================== C-ABI: external spec injection ====================
//
// Bridges Go callers into the C++ InjectExternalSpecProperties pipeline so
// that URI parsing, endpoint derivation, AWS-form rewriting, allowlist
// enforcement, and format-property derivation (reader.exttable.snapshot_id etc.)
// all live in a single implementation driven by the raw external_spec JSON.

extern "C" LoonFFIResult
loon_properties_inject_external_spec(LoonProperties* properties,
                                     int64_t collection_id,
                                     const char* external_source,
                                     const char* external_spec,
                                     uint32_t iops_initial_rate,
                                     uint32_t iops_max_rate) {
    if (properties == nullptr) {
        RETURN_ERROR(LOON_INVALID_ARGS, "properties is null");
    }
    if (external_source == nullptr || external_source[0] == '\0') {
        RETURN_SUCCESS();  // no-op for internal (non-external) collections
    }
    if (iops_initial_rate == 0) {
        RETURN_ERROR(LOON_INVALID_ARGS,
                     "external IOPS initial rate must be greater than zero");
    }
    try {
        // Load existing flat LoonProperties into a typed Properties map.
        milvus_storage::api::Properties props;
        for (size_t i = 0; i < properties->count; ++i) {
            const auto& p = properties->properties[i];
            if (p.key != nullptr && p.value != nullptr) {
                props[p.key] = std::string(p.value);
            }
        }

        // Keep Go-provided values call-local. Publishing them to the singleton
        // would couple concurrent FFI and native reads through global state.
        const milvus::storage::ExternalIopsConfig iops_config{iops_initial_rate,
                                                              iops_max_rate};
        const auto status =
            InjectExternalSpecProperties(props,
                                         collection_id,
                                         external_source,
                                         external_spec ? external_spec : "",
                                         iops_config);
        if (!status.ok()) {
            RETURN_ERROR(LOON_INVALID_PROPERTIES, status.message());
        }

        // Rebuild LoonProperties from the merged map. Free old entries first
        // so ownership stays with loon_properties_free.
        for (size_t i = 0; i < properties->count; ++i) {
            free(properties->properties[i].key);
            free(properties->properties[i].value);
        }
        free(properties->properties);
        properties->properties = nullptr;
        properties->count = 0;

        size_t n = props.size();
        if (n == 0) {
            RETURN_SUCCESS();
        }
        auto* arr =
            static_cast<LoonProperty*>(malloc(sizeof(LoonProperty) * n));
        if (arr == nullptr) {
            RETURN_ERROR(LOON_MEMORY_ERROR,
                         "failed to malloc LoonProperty array of size ",
                         n);
        }
        size_t i = 0;
        for (const auto& kv : props) {
            arr[i].key = strdup(kv.first.c_str());
            // Registered properties such as snapshot_id are stored as INT64.
            const auto value = PropertyValueAsString(props, kv.first.c_str());
            arr[i].value = strdup(value ? value->c_str() : "");
            ++i;
        }
        properties->properties = arr;
        properties->count = n;
        RETURN_SUCCESS();
    } catch (std::exception& e) {
        RETURN_EXCEPTION(e.what());
    }
}
