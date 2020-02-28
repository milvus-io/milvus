// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "segment/Vectors.h"

#include <algorithm>
#include <chrono>
#include <iostream>
#include <utility>
#include <vector>

#include "utils/Log.h"

namespace milvus {
namespace segment {

Vectors::Vectors(std::vector<uint8_t> data, std::vector<doc_id_t> uids, const std::string& name)
    : data_(std::move(data)), uids_(std::move(uids)), name_(name) {
}

void
Vectors::AddData(const std::vector<uint8_t>& data) {
    data_.reserve(data_.size() + data.size());
    data_.insert(data_.end(), std::make_move_iterator(data.begin()), std::make_move_iterator(data.end()));
}

void
Vectors::AddUids(const std::vector<doc_id_t>& uids) {
    uids_.reserve(uids_.size() + uids.size());
    uids_.insert(uids_.end(), std::make_move_iterator(uids.begin()), std::make_move_iterator(uids.end()));
}

void
Vectors::Erase(int32_t offset) {
    auto code_length = GetCodeLength();
    if (code_length != 0) {
        auto step = offset * code_length;
        data_.erase(data_.begin() + step, data_.begin() + step + code_length);
        uids_.erase(uids_.begin() + offset, uids_.begin() + offset + 1);
    }
}

void
Vectors::Erase(std::vector<int32_t>& offsets) {
    if (offsets.empty()) {
        return;
    }

    // Sort and remove duplicates
    auto start = std::chrono::high_resolution_clock::now();

    std::sort(offsets.begin(), offsets.end());

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> diff = end - start;
    ENGINE_LOG_DEBUG << "Sorting " << offsets.size() << " offsets to delete took " << diff.count() << " s";

    start = std::chrono::high_resolution_clock::now();

    offsets.erase(std::unique(offsets.begin(), offsets.end()), offsets.end());

    end = std::chrono::high_resolution_clock::now();
    diff = end - start;
    ENGINE_LOG_DEBUG << "Deduplicating " << offsets.size() << " offsets to delete took " << diff.count() << " s";

    // Reconstruct raw vectors and uids
    ENGINE_LOG_DEBUG << "Begin erasing...";

    size_t new_size = uids_.size() - offsets.size();
    std::vector<doc_id_t> new_uids(new_size);
    auto code_length = GetCodeLength();
    std::vector<uint8_t> new_data(new_size * code_length);

    auto count = 0;
    auto skip = offsets.cbegin();
    auto loop_size = uids_.size();

    for (size_t i = 0; i < loop_size;) {
        while (i == *skip && skip != offsets.cend()) {
            ++i;
            ++skip;
        }

        if (i == loop_size) {
            break;
        }

        new_uids[count] = uids_[i];

        for (size_t j = 0; j < code_length; ++j) {
            new_data[count * code_length + j] = data_[i * code_length + j];
        }

        ++count;
        ++i;
    }

    data_.clear();
    uids_.clear();
    data_.swap(new_data);
    uids_.swap(new_uids);

    end = std::chrono::high_resolution_clock::now();
    diff = end - start;
    ENGINE_LOG_DEBUG << "Erasing " << offsets.size() << " vectors out of " << loop_size << " vectors took "
                     << diff.count() << " s";
}

const std::vector<uint8_t>&
Vectors::GetData() const {
    return data_;
}

const std::vector<doc_id_t>&
Vectors::GetUids() const {
    return uids_;
}

size_t
Vectors::GetCount() const {
    return uids_.size();
}

size_t
Vectors::GetCodeLength() const {
    return uids_.empty() ? 0 : data_.size() / uids_.size();
}

size_t
Vectors::Size() {
    return data_.size() + uids_.size() * sizeof(doc_id_t);
}

void
Vectors::SetName(const std::string& name) {
    name_ = name;
}

const std::string&
Vectors::GetName() const {
    return name_;
}

void
Vectors::Clear() {
    data_.clear();
    data_.shrink_to_fit();
    uids_.clear();
    uids_.shrink_to_fit();
}

}  // namespace segment
}  // namespace milvus
