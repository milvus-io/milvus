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

#include "index/CreateIndexResult.h"

namespace milvus::index {

CreateIndexResultPtr
CreateIndexResult::New(
    int64_t mem_size,
    std::vector<SerializedIndexFileInfo>&& serialized_index_infos) {
    return std::unique_ptr<CreateIndexResult>(
        new CreateIndexResult(mem_size, std::move(serialized_index_infos)));
}

CreateIndexResult::CreateIndexResult(
    int64_t mem_size,
    std::vector<SerializedIndexFileInfo>&& serialized_index_infos)
    : mem_size_(mem_size), serialized_index_infos_(serialized_index_infos) {
}

void
CreateIndexResult::AppendSerializedIndexFileInfo(
    SerializedIndexFileInfo&& info) {
    serialized_index_infos_.push_back(std::move(info));
}

void
CreateIndexResult::SerializeAt(milvus::ProtoLayout* layout) {
    milvus::proto::cgo::CreateIndexResult result;
    result.set_mem_size(mem_size_);
    for (auto& info : serialized_index_infos_) {
        auto serialized_info = result.add_serialized_index_infos();
        serialized_info->set_file_name(info.file_name);
        serialized_info->set_file_size(info.file_size);
    }
    AssertInfo(layout->SerializeAndHoldProto(result),
               "marshal CreateIndexResult failed");
}

std::vector<std::string>
CreateIndexResult::GetIndexFiles() const {
    std::vector<std::string> files;
    for (auto& info : serialized_index_infos_) {
        files.push_back(info.file_name);
    }
    return files;
}

int64_t
CreateIndexResult::GetMemSize() const {
    return mem_size_;
}

int64_t
CreateIndexResult::GetSerializedSize() const {
    int64_t size = 0;
    for (auto& info : serialized_index_infos_) {
        size += info.file_size;
    }
    return size;
}

}  // namespace milvus::index