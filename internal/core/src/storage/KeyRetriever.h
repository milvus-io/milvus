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

#include "common/type_c.h"
#include "parquet/encryption/encryption.h"
#include "parquet/properties.h"

namespace milvus::storage {

class KeyRetriever : public parquet::DecryptionKeyRetriever {
 public:
    std::string
    GetKey(const std::string& key_metadata) override;
};

parquet::ReaderProperties
GetReaderProperties();

parquet::ArrowReaderProperties
GetArrowReaderProperties();

// Reader properties for a caller that reads a whole file set at once and wants
// every byte range of a read in flight together.
//
// The configured properties read lazily: arrow fetches a coalesced range only
// when the decoder first touches it, so one file has one request in flight.
// With eager_range_size_bytes > 0 the ranges are instead cut at that size and
// all submitted to arrow's IO pool when the read starts. The bytes a read
// holds are the same either way; only the number of requests carrying them
// changes. eager_range_size_bytes <= 0 returns the configured properties
// unchanged.
parquet::ArrowReaderProperties
GetArrowReaderProperties(int64_t eager_range_size_bytes);

void
ConfigureArrowReaderProperties(int64_t hole_size_limit_bytes,
                               int64_t range_size_limit_bytes);

std::string
EncodeKeyMetadata(int64_t ez_id, int64_t collection_id, std::string key);

std::shared_ptr<CPluginContext>
DecodeKeyMetadata(const std::string& key_metadata);

}  // namespace milvus::storage
