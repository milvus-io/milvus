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

#include "common/type_c.h"
#include "parquet/encryption/encryption.h"

namespace milvus::storage {

class KeyRetriever : public parquet::DecryptionKeyRetriever {
 public:
    std::string
    GetKey(const std::string& key_metadata) override;
};

parquet::ReaderProperties
GetReaderProperties();

std::string
EncodeKeyMetadata(int64_t ez_id, int64_t collection_id, std::string key);

std::shared_ptr<CPluginContext>
DecodeKeyMetadata(std::string key_metadata);

}  // namespace milvus::storage
