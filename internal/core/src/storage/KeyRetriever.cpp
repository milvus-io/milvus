#include "storage/KeyRetriever.h"

#include <cstdlib>
#include <cstring>
#include <exception>
#include <mutex>

#include "arrow/io/caching.h"
#include "common/EasyAssert.h"
#include "fmt/format.h"
#include "glog/logging.h"
#include "log/Log.h"
#include "parquet/properties.h"
#include "storage/PluginLoader.h"
#include "storage/plugin/PluginInterface.h"

namespace milvus::storage {
namespace {
std::mutex arrow_reader_properties_mutex;
parquet::ArrowReaderProperties arrow_reader_properties =
    parquet::default_arrow_reader_properties();

}  // namespace

std::pair<std::string, std::string>
GetWriterEncryptionParams(const CPluginContext* context) {
    if (context == nullptr) {
        ThrowInfo(ErrorCode::InvalidParameter, "c_plugin_context is nullptr");
    }
    if (!context->key || !*context->key) {
        ThrowInfo(ErrorCode::InvalidParameter, "missing writer keyset");
    }
    auto plugin = PluginLoader::GetInstance().getCipherPlugin();
    if (!plugin) {
        ThrowInfo(ErrorCode::Unsupported, "missing cipher plugin");
    }
    auto result = plugin->GetEncryptorForKeyset(
        context->ez_id, context->collection_id, context->key);
    if (!result.first) {
        ThrowInfo(ErrorCode::InvalidParameter,
                  "cipher plugin returned no encryptor");
    }
    auto key = result.first->GetKey();
    if ((key.size() != 16 && key.size() != 24 && key.size() != 32) ||
        result.second.empty() ||
        result.second.find('\0') != std::string::npos) {
        ThrowInfo(ErrorCode::InvalidParameter,
                  "cipher plugin returned unusable encryption parameters");
    }
    return {
        std::move(key),
        EncodeKeyMetadata(
            context->ez_id, context->collection_id, std::move(result.second))};
}

KeyRetriever::KeyRetriever(const CPluginContext* context) {
    if (context == nullptr) {
        return;
    }
    if (!context->key || !*context->key) {
        ThrowInfo(ErrorCode::InvalidParameter, "missing reader keyset");
    }
    auto cipher = PluginLoader::GetInstance().getCipherPlugin();
    if (!cipher) {
        ThrowInfo(ErrorCode::Unsupported, "missing cipher plugin");
    }
    has_keyset_ = true;
    bound_ez_id_ = context->ez_id;
    bound_keyset_ = context->key;
}

std::string
KeyRetriever::GetKey(const std::string& key_metadata) {
    auto context = DecodeKeyMetadata(key_metadata);
    if (!context) {
        ThrowInfo(ErrorCode::DataFormatBroken, "invalid encryption metadata");
    }
    auto plugin = PluginLoader::GetInstance().getCipherPlugin();
    AssertInfo(plugin != nullptr, "cipher plugin not found");
    auto decryptor =
        (has_keyset_ && context->ez_id == bound_ez_id_)
            ? plugin->GetDecryptorForKeyset(context->ez_id,
                                            context->collection_id,
                                            bound_keyset_,
                                            std::string(context->key))
            : plugin->GetDecryptor(context->ez_id,
                                   context->collection_id,
                                   std::string(context->key));
    if (!decryptor) {
        ThrowInfo(ErrorCode::Unsupported,
                  "cipher plugin returned no decryptor");
    }
    return decryptor->GetKey();
}

parquet::ReaderProperties
GetReaderProperties(const CPluginContext* context) {
    parquet::ReaderProperties reader_properties =
        parquet::default_reader_properties();
    parquet::FileDecryptionProperties::Builder builder;
    reader_properties.file_decryption_properties(
        builder.key_retriever(std::make_shared<KeyRetriever>(context))
            ->plaintext_files_allowed()
            ->build());
    return reader_properties;
}

parquet::ArrowReaderProperties
GetArrowReaderProperties() {
    std::lock_guard<std::mutex> lock(arrow_reader_properties_mutex);
    return arrow_reader_properties;
}

parquet::ArrowReaderProperties
GetArrowReaderProperties(bool eager_prebuffer) {
    auto properties = GetArrowReaderProperties();
    if (!eager_prebuffer) {
        return properties;
    }
    // Only when the ranges are fetched changes. The coalescing limits stay as
    // configured, so an eager reader issues the same requests as a lazy one.
    auto cache_options = properties.cache_options();
    cache_options.lazy = false;
    cache_options.prefetch_limit = 0;
    properties.set_pre_buffer(true);
    properties.set_cache_options(cache_options);
    return properties;
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
