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

#include <algorithm>
#include <chrono>
#include <cstring>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "Vectors.h"
#include "segment/Attr.h"
#include "utils/Log.h"

namespace milvus {
namespace segment {

Attr::Attr(const std::vector<uint8_t>& data, size_t nbytes, const std::vector<int64_t> uids, const std::string& name)
    : data_(std::move(data)), nbytes_(nbytes), uids_(std::move(uids)), name_(name) {
}

void
Attr::AddAttr(const std::vector<uint8_t>& data, size_t nbytes) {
    data_.reserve(data_.size() + data.size());
    data_.insert(data_.end(), std::make_move_iterator(data.begin()), std::make_move_iterator(data.end()));
    nbytes_ += nbytes;
}

void
Attr::AddUids(const std::vector<int64_t>& uids) {
    uids_.reserve(uids_.size() + uids.size());
    uids_.insert(uids_.end(), std::make_move_iterator(uids.begin()), std::make_move_iterator(uids.end()));
}

void
Attr::SetName(const std::string& name) {
    name_ = name;
}

const std::vector<uint8_t>&
Attr::GetData() const {
    return data_;
}

const std::string&
Attr::GetName() const {
    return name_;
}

const std::string&
Attr::GetCollectionId() {
    return collection_id_;
}

const size_t&
Attr::GetNbytes() const {
    return nbytes_;
}

const std::vector<int64_t>&
Attr::GetUids() const {
    return uids_;
}

size_t
Attr::GetCount() const {
    return uids_.size();
}

size_t
Attr::GetCodeLength() const {
    return uids_.size() == 0 ? 0 : nbytes_ / uids_.size();
}

void
Attr::Erase(int32_t offset) {
    auto code_length = GetCodeLength();
    if (code_length != 0) {
        auto step = offset * code_length;
        data_.erase(data_.begin() + step, data_.begin() + step + code_length);
        uids_.erase(uids_.begin() + offset, uids_.begin() + offset + 1);
    }
}

void
Attr::Erase(std::vector<int32_t>& offsets) {
    if (offsets.empty()) {
        return;
    }

    // Sort and remove duplicates
    auto start = std::chrono::high_resolution_clock::now();

    std::sort(offsets.begin(), offsets.end());

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> diff = end - start;
    LOG_ENGINE_DEBUG_ << "Sorting " << offsets.size() << " offsets to delete took " << diff.count() << " s";

    start = std::chrono::high_resolution_clock::now();

    offsets.erase(std::unique(offsets.begin(), offsets.end()), offsets.end());

    end = std::chrono::high_resolution_clock::now();
    diff = end - start;
    LOG_ENGINE_DEBUG_ << "Deduplicating " << offsets.size() << " offsets to delete took " << diff.count() << " s";

    LOG_ENGINE_DEBUG_ << "Attributes begin erasing...";

    size_t new_size = uids_.size() - offsets.size();
    std::vector<doc_id_t> new_uids(new_size);
    auto code_length = GetCodeLength();
    std::vector<uint8_t> new_data(new_size * code_length);

    auto count = 0;
    auto skip = offsets.cbegin();
    auto loop_size = uids_.size();

    for (size_t i = 0; i < loop_size;) {
        while (skip != offsets.cend() && i == *skip) {
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
    LOG_ENGINE_DEBUG_ << "Erasing " << offsets.size() << " vectors out of " << loop_size << " vectors took "
                      << diff.count() << " s";
}

}  // namespace segment
}  // namespace milvus
