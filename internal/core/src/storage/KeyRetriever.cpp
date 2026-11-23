#include "storage/KeyRetriever.h"

#include <exception>
#include <mutex>

#include "arrow/io/caching.h"
#include "common/EasyAssert.h"
#include "fmt/format.h"
#include "log/Log.h"
#include "parquet/properties.h"
#include "storage/PluginLoader.h"

namespace milvus::storage {
namespace {
std::mutex arrow_reader_properties_mutex;
parquet::ArrowReaderProperties arrow_reader_properties =
    parquet::default_arrow_reader_properties();
}  // namespace

std::string
KeyRetriever::GetKey(const std::string& key_metadata) {
    auto plugin = PluginLoader::GetInstance().getCipherPlugin();
    AssertInfo(plugin != nullptr, "cipher plugin not found");
    auto context = DecodeKeyMetadata(key_metadata);
    AssertInfo(context != nullptr, "invalid encryption metadata");
    auto decryptor = plugin->GetDecryptor(
        context->ez_id, context->collection_id, context->key);
    AssertInfo(decryptor != nullptr, "cipher plugin returned no decryptor");
    return decryptor->GetKey();
}

parquet::ReaderProperties
GetReaderProperties() {
    parquet::ReaderProperties reader_properties =
        parquet::default_reader_properties();
    std::shared_ptr<milvus::storage::KeyRetriever> key_retriever =
        std::make_shared<milvus::storage::KeyRetriever>();
    parquet::FileDecryptionProperties::Builder builder;
    reader_properties.file_decryption_properties(
        builder.key_retriever(key_retriever)
            ->plaintext_files_allowed()
            ->build());
    return reader_properties;
}

void
RegisterReaderKeys(const CPluginContext* context) {
    if (!context) {
        return;
    }
    auto cipher = PluginLoader::GetInstance().getCipherPlugin();
    if (!cipher) {
        ThrowInfo(Unsupported, "missing cipher plugin");
    }
    AssertInfo(context->key && *context->key, "missing reader keyset");
    cipher->RegisterReadKeyset(
        context->ez_id, context->collection_id, context->key);
}

std::pair<std::string, std::string>
GetWriterEncryptionParams(const CPluginContext* context) {
    AssertInfo(context != nullptr, "c_plugin_context is nullptr");
    auto cipher = PluginLoader::GetInstance().getCipherPlugin();
    if (!cipher) {
        ThrowInfo(Unsupported, "missing cipher plugin");
    }
    AssertInfo(context->key && *context->key, "missing writer keyset");
    auto keys = cipher->GetEncryptorForKeyset(
        context->ez_id, context->collection_id, context->key);
    AssertInfo(keys.first != nullptr, "cipher plugin returned no encryptor");
    auto footer_key = keys.first->GetKey();
    auto length = footer_key.size();
    AssertInfo(!keys.second.empty() &&
                   keys.second.find('\0') == std::string::npos &&
                   (length == 16 || length == 24 || length == 32),
               "cipher plugin returned unusable encryption parameters");
    auto metadata =
        EncodeKeyMetadata(context->ez_id, context->collection_id, keys.second);
    return {std::move(footer_key), std::move(metadata)};
}

parquet::ArrowReaderProperties
GetArrowReaderProperties() {
    std::lock_guard<std::mutex> lock(arrow_reader_properties_mutex);
    return arrow_reader_properties;
}

void
ConfigureArrowReaderProperties(int64_t hole_size_limit_bytes,
                               int64_t range_size_limit_bytes) {
    auto properties = parquet::default_arrow_reader_properties();
    auto cache_options = properties.cache_options();
    if (hole_size_limit_bytes > 0) {
        cache_options.hole_size_limit = hole_size_limit_bytes;
    }
    if (range_size_limit_bytes > 0) {
        cache_options.range_size_limit = range_size_limit_bytes;
    }
    AssertInfo(cache_options.range_size_limit > cache_options.hole_size_limit,
               "arrow reader range size limit must be greater than hole size "
               "limit, range_size_limit={}, hole_size_limit={}",
               cache_options.range_size_limit,
               cache_options.hole_size_limit);
    properties.set_cache_options(cache_options);

    std::lock_guard<std::mutex> lock(arrow_reader_properties_mutex);
    arrow_reader_properties = properties;
}

std::string
EncodeKeyMetadata(int64_t ez_id, int64_t collection_id, std::string key) {
    return fmt::format("{}_{}_{}", ez_id, collection_id, key);
}

std::shared_ptr<CPluginContext>
DecodeKeyMetadata(const std::string& key_metadata) {
    // Keep the wrapped key alive for as long as its C context is used.
    struct OwnedContext : CPluginContext {
        std::string wrapped_key;
    };
    auto context = std::make_shared<OwnedContext>();
    try {
        auto first_pos = key_metadata.find("_");
        if (first_pos == std::string::npos) {
            return nullptr;
        }

        auto second_pos = key_metadata.find("_", first_pos + 1);
        if (second_pos == std::string::npos) {
            return nullptr;
        }

        context->ez_id = std::stoll(key_metadata.substr(0, first_pos));
        context->collection_id = std::stoll(
            key_metadata.substr(first_pos + 1, second_pos - (first_pos + 1)));
        context->wrapped_key = key_metadata.substr(second_pos + 1);
        context->key = context->wrapped_key.c_str();
    } catch (const std::exception& e) {
        LOG_WARN("failed to decode key metadata, reason: {}", e.what());
        return nullptr;
    }
    return context;
}

}  // namespace milvus::storage
