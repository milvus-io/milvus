#include "common/type_c.h"
#include "parquet/encryption/encryption.h"

namespace milvus::storage {

class KeyRetriever : public parquet::DecryptionKeyRetriever{
public:
   std::string GetKey(const std::string& key_metadata) override;
};

parquet::ReaderProperties
GetReaderProperties();

std::string
EncodeKeyMetadata(int64_t ez_id, int64_t collection_id, std::string key) ;

std::shared_ptr<CPluginContext>
DecodeKeyMetadata(std::string key_metadata);

}// namespace milvus::storage
