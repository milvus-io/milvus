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

#include <stddef.h>
#include <algorithm>
#include <cstdint>
#include <iostream>
#include <string>
#include <vector>

#include "common/Types.h"
#include "fmt/core.h"
#include "index/IndexStats.h"
#include "index/Meta.h"
#include "index/ScalarIndex.h"
#include "index/Utils.h"
#include "knowhere/dataset.h"
#include "log/Log.h"
#include "pb/schema.pb.h"
#include "storage/FileManager.h"
#include "storage/IndexEntryReader.h"
#include "storage/IndexEntryWriter.h"
#include "storage/Util.h"

namespace milvus::index {
template <typename T>
const TargetBitmap
ScalarIndex<T>::Query(const DatasetPtr& dataset) {
    auto op = dataset->Get<OpType>(OPERATOR_TYPE);
    switch (op) {
        case OpType::LessThan:
        case OpType::LessEqual:
        case OpType::GreaterThan:
        case OpType::GreaterEqual: {
            auto value = dataset->Get<T>(RANGE_VALUE);
            return Range(value, op);
        }

        case OpType::Range: {
            auto lower_bound_value = dataset->Get<T>(LOWER_BOUND_VALUE);
            auto upper_bound_value = dataset->Get<T>(UPPER_BOUND_VALUE);
            auto lower_bound_inclusive =
                dataset->Get<bool>(LOWER_BOUND_INCLUSIVE);
            auto upper_bound_inclusive =
                dataset->Get<bool>(UPPER_BOUND_INCLUSIVE);
            return Range(lower_bound_value,
                         lower_bound_inclusive,
                         upper_bound_value,
                         upper_bound_inclusive);
        }

        case OpType::In: {
            auto n = dataset->GetRows();
            auto values = dataset->GetTensor();
            return In(n, reinterpret_cast<const T*>(values));
        }

        case OpType::NotIn: {
            auto n = dataset->GetRows();
            auto values = dataset->GetTensor();
            return NotIn(n, reinterpret_cast<const T*>(values));
        }

        case OpType::PrefixMatch:
        case OpType::PostfixMatch:
        default:
            ThrowInfo(OpTypeInvalid,
                      fmt::format("unsupported operator type: {}", op));
    }
}

template <>
void
ScalarIndex<std::string>::BuildWithRawDataForUT(size_t n,
                                                const void* values,
                                                const Config& config) {
    proto::schema::StringArray arr;
    auto ok = arr.ParseFromArray(values, n);
    Assert(ok);

    // TODO :: optimize here. avoid memory copy.
    std::vector<std::string> vecs{arr.data().begin(), arr.data().end()};
    Build(arr.data_size(), vecs.data());
}

template <>
void
ScalarIndex<bool>::BuildWithRawDataForUT(size_t n,
                                         const void* values,
                                         const Config& config) {
    proto::schema::BoolArray arr;
    auto ok = arr.ParseFromArray(values, n);
    Assert(ok);
    Build(arr.data_size(), arr.data().data());
}

template <>
void
ScalarIndex<int8_t>::BuildWithRawDataForUT(size_t n,
                                           const void* values,
                                           const Config& config) {
    auto data = reinterpret_cast<int8_t*>(const_cast<void*>(values));
    Build(n, data);
}

template <>
void
ScalarIndex<int16_t>::BuildWithRawDataForUT(size_t n,
                                            const void* values,
                                            const Config& config) {
    auto data = reinterpret_cast<int16_t*>(const_cast<void*>(values));
    Build(n, data);
}

template <>
void
ScalarIndex<int32_t>::BuildWithRawDataForUT(size_t n,
                                            const void* values,
                                            const Config& config) {
    auto data = reinterpret_cast<int32_t*>(const_cast<void*>(values));
    Build(n, data);
}

template <>
void
ScalarIndex<int64_t>::BuildWithRawDataForUT(size_t n,
                                            const void* values,
                                            const Config& config) {
    auto data = reinterpret_cast<int64_t*>(const_cast<void*>(values));
    Build(n, data);
}

template <>
void
ScalarIndex<float>::BuildWithRawDataForUT(size_t n,
                                          const void* values,
                                          const Config& config) {
    auto data = reinterpret_cast<float*>(const_cast<void*>(values));
    Build(n, data);
}

template <>
void
ScalarIndex<double>::BuildWithRawDataForUT(size_t n,
                                           const void* values,
                                           const Config& config) {
    auto data = reinterpret_cast<double*>(const_cast<void*>(values));
    Build(n, data);
}

template <typename T>
IndexStatsPtr
ScalarIndex<T>::UploadV3(const Config& config) {
    AssertInfo(file_manager_ != nullptr,
               "file_manager_ is null, UploadV3 requires a valid file manager");

    // Build filename: milvus_packed_<type>_index.v3
    auto type_str = ToString(GetIndexType());
    std::transform(
        type_str.begin(), type_str.end(), type_str.begin(), ::tolower);
    auto filename = "milvus_packed_" + type_str + "_index.v3";

    // Create the IndexEntryWriter
    auto writer =
        file_manager_->CreateIndexEntryWriterV3(filename, is_index_file_);
    AssertInfo(writer != nullptr,
               "failed to create IndexEntryWriter for V3 format");

    // Call subclass implementation to write all entries.
    // Subclasses use writer->PutMeta() to add their metadata.
    WriteEntries(writer.get());

    // Finish writing - writes __meta__ entry, Directory Table and Footer
    writer->Finish();

    // Get actual file size from writer
    auto file_size = writer->GetTotalBytesWritten();

    LOG_INFO("UploadV3 completed for index type: {}, file size: {}",
             index_type_,
             file_size);

    // Return IndexStats with the single packed file (full remote path)
    std::vector<SerializedIndexFileInfo> index_files;
    auto remote_prefix = is_index_file_
                             ? file_manager_->GetRemoteIndexObjectPrefixV2()
                             : file_manager_->GetRemoteTextLogPrefixV2();
    auto remote_path = remote_prefix + "/" + filename;
    index_files.emplace_back(remote_path, file_size);

    return IndexStats::New(file_size, std::move(index_files));
}

template <typename T>
void
ScalarIndex<T>::LoadV3(const Config& config) {
    AssertInfo(file_manager_ != nullptr,
               "file_manager_ is null, LoadV3 requires a valid file manager");

    // Get the packed index file path from config
    auto index_files =
        GetValueFromConfig<std::vector<std::string>>(config, INDEX_FILES);
    AssertInfo(index_files.has_value() && !index_files.value().empty(),
               "index_files is required for LoadV3");

    // For V3 format, there should be exactly one packed file
    AssertInfo(index_files.value().size() == 1,
               "LoadV3 expects exactly one packed index file, got: {}",
               index_files.value().size());
    const auto& packed_file = index_files.value()[0];

    LOG_INFO("LoadV3: loading packed index file: {}", packed_file);

    // Open the file using the file manager
    auto input = file_manager_->OpenInputStream(packed_file, is_index_file_);
    AssertInfo(input != nullptr,
               "failed to open input stream for packed index file: {}",
               packed_file);

    size_t file_size = input->Size();

    auto collection_id =
        GetValueFromConfig<int64_t>(config, COLLECTION_ID).value_or(0);

    auto reader =
        storage::IndexEntryReader::Open(input, file_size, collection_id);
    AssertInfo(reader != nullptr, "failed to create IndexEntryReader");

    LoadEntries(*reader, config);

    LOG_INFO("LoadV3 completed for index type: {}", index_type_);
}

template class ScalarIndex<bool>;
template class ScalarIndex<int8_t>;
template class ScalarIndex<int16_t>;
template class ScalarIndex<int32_t>;
template class ScalarIndex<int64_t>;
template class ScalarIndex<float>;
template class ScalarIndex<double>;
template class ScalarIndex<std::string>;
}  // namespace milvus::index
