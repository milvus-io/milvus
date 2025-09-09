#include "storage/KeyRetriever.h"
#include "common/EasyAssert.h"
#include "log/Log.h"
#include "parquet/properties.h"
#include "storage/PluginLoader.h"

namespace milvus::storage {

std::string
KeyRetriever::GetKey(const std::string& key_metadata) {
    auto plugin = PluginLoader::GetInstance().getCipherPlugin();
    AssertInfo(plugin != nullptr, "cipher plugin not found");
    auto context = DecodeKeyMetadata(key_metadata);
    AssertInfo(context != nullptr, "invalid key metadata: {}", key_metadata);
    auto decryptor = plugin->GetDecryptor(
        context->ez_id, context->collection_id, std::string(context->key));
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

std::string
EncodeKeyMetadata(int64_t ez_id, int64_t collection_id, std::string key) {
    return std::to_string(ez_id) + "_" + std::to_string(collection_id) + "_" +
           key;
}

std::shared_ptr<CPluginContext>
DecodeKeyMetadata(std::string key_metadata) {
    auto context = std::make_shared<CPluginContext>();
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
        context->key = key_metadata.substr(second_pos + 1).c_str();
    } catch (const std::exception& e) {
        LOG_WARN("failed to decode key metadata, reason: {}", e.what());
        return nullptr;
    }
    return context;
}

}  // namespace milvus::storage
