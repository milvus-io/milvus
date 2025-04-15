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

#pragma once

#include <vector>
#include <memory>

#include "storage/DataCodec.h"
#include "storage/PayloadReader.h"

namespace milvus::storage {

class InsertData : public DataCodec {
 public:
    explicit InsertData(std::shared_ptr<PayloadReader>& payload_reader)
        : DataCodec(payload_reader, CodecType::InsertDataType) {
    }

    std::vector<uint8_t>
    Serialize(StorageType medium) override;

    void
    SetFieldDataMeta(const FieldDataMeta& meta) override;

 public:
    std::vector<uint8_t>
    serialize_to_remote_file();

    std::vector<uint8_t>
    serialize_to_local_file();

 private:
    std::optional<FieldDataMeta> field_data_meta_;
};

}  // namespace milvus::storage
