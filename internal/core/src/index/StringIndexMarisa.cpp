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

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <stdlib.h>
#include <stdio.h>
#include <fcntl.h>

#include "index/StringIndexMarisa.h"
#include "index/Utils.h"
#include "index/Index.h"
#include "common/Utils.h"
#include "common/Slice.h"

namespace milvus::index {

#if defined(__linux__) || defined(__APPLE__)

class UnistdException : public std::runtime_error {
 public:
    explicit UnistdException(const std::string& msg) : std::runtime_error(msg) {
    }

    virtual ~UnistdException() {
    }
};

StringIndexMarisa::StringIndexMarisa(storage::FileManagerImplPtr file_manager) {
    if (file_manager != nullptr) {
        file_manager_ = std::dynamic_pointer_cast<storage::MemFileManagerImpl>(
            file_manager);
    }
}

int64_t
StringIndexMarisa::Size() {
    return trie_.size();
}

bool
valid_str_id(size_t str_id) {
    return str_id >= 0 && str_id != MARISA_INVALID_KEY_ID;
}

void
StringIndexMarisa::Build(const Config& config) {
    if (built_) {
        throw std::runtime_error("index has been built");
    }

    auto insert_files =
        GetValueFromConfig<std::vector<std::string>>(config, "insert_files");
    AssertInfo(insert_files.has_value(),
               "insert file paths is empty when build index");
    auto field_datas =
        file_manager_->CacheRawDataToMemory(insert_files.value());
    int64_t total_num_rows = 0;

    // fill key set.
    marisa::Keyset keyset;
    for (auto data : field_datas) {
        auto slice_num = data->get_num_rows();
        for (size_t i = 0; i < slice_num; ++i) {
            keyset.push_back(
                (*static_cast<const std::string*>(data->RawValue(i))).c_str());
        }
        total_num_rows += slice_num;
    }
    trie_.build(keyset);

    // fill str_ids_
    str_ids_.resize(total_num_rows);
    int64_t offset = 0;
    for (auto data : field_datas) {
        auto slice_num = data->get_num_rows();
        for (size_t i = 0; i < slice_num; ++i) {
            auto str_id =
                lookup(*static_cast<const std::string*>(data->RawValue(i)));
            AssertInfo(valid_str_id(str_id), "invalid marisa key");
            str_ids_[offset++] = str_id;
        }
    }

    // fill str_ids_to_offsets_
    fill_offsets();

    built_ = true;
}

void
StringIndexMarisa::Build(size_t n, const std::string* values) {
    if (built_) {
        throw std::runtime_error("index has been built");
    }

    marisa::Keyset keyset;
    {
        // fill key set.
        for (size_t i = 0; i < n; i++) {
            keyset.push_back(values[i].c_str());
        }
    }

    trie_.build(keyset);
    fill_str_ids(n, values);
    fill_offsets();

    built_ = true;
}

BinarySet
StringIndexMarisa::Serialize(const Config& config) {
    auto uuid = boost::uuids::random_generator()();
    auto uuid_string = boost::uuids::to_string(uuid);
    auto file = std::string("/tmp/") + uuid_string;

    auto fd = open(
        file.c_str(), O_RDWR | O_CREAT | O_EXCL, S_IRUSR | S_IWUSR | S_IXUSR);
    trie_.write(fd);

    auto size = get_file_size(fd);
    auto index_data = std::shared_ptr<uint8_t[]>(new uint8_t[size]);

    lseek(fd, 0, SEEK_SET);
    auto status = read(fd, index_data.get(), size);

    close(fd);
    remove(file.c_str());
    if (status != size) {
        throw UnistdException("read index from fd error, errorCode is " +
                              std::to_string(status));
    }

    auto str_ids_len = str_ids_.size() * sizeof(size_t);
    std::shared_ptr<uint8_t[]> str_ids(new uint8_t[str_ids_len]);
    memcpy(str_ids.get(), str_ids_.data(), str_ids_len);

    BinarySet res_set;
    res_set.Append(MARISA_TRIE_INDEX, index_data, size);
    res_set.Append(MARISA_STR_IDS, str_ids, str_ids_len);

    Disassemble(res_set);

    return res_set;
}

BinarySet
StringIndexMarisa::Upload(const Config& config) {
    auto binary_set = Serialize(config);
    file_manager_->AddFile(binary_set);

    auto remote_paths_to_size = file_manager_->GetRemotePathsToFileSize();
    BinarySet ret;
    for (auto& file : remote_paths_to_size) {
        ret.Append(file.first, nullptr, file.second);
    }

    return ret;
}

void
StringIndexMarisa::LoadWithoutAssemble(const BinarySet& set,
                                       const Config& config) {
    auto uuid = boost::uuids::random_generator()();
    auto uuid_string = boost::uuids::to_string(uuid);
    auto file = std::string("/tmp/") + uuid_string;

    auto index = set.GetByName(MARISA_TRIE_INDEX);
    auto len = index->size;

    auto fd = open(
        file.c_str(), O_RDWR | O_CREAT | O_EXCL, S_IRUSR | S_IWUSR | S_IXUSR);
    lseek(fd, 0, SEEK_SET);

    auto status = write(fd, index->data.get(), len);
    if (status != len) {
        close(fd);
        remove(file.c_str());
        throw UnistdException("write index to fd error, errorCode is " +
                              std::to_string(status));
    }

    lseek(fd, 0, SEEK_SET);
    trie_.read(fd);
    close(fd);
    remove(file.c_str());

    auto str_ids = set.GetByName(MARISA_STR_IDS);
    auto str_ids_len = str_ids->size;
    str_ids_.resize(str_ids_len / sizeof(size_t));
    memcpy(str_ids_.data(), str_ids->data.get(), str_ids_len);

    fill_offsets();
}

void
StringIndexMarisa::Load(const BinarySet& set, const Config& config) {
    milvus::Assemble(const_cast<BinarySet&>(set));
    LoadWithoutAssemble(set, config);
}

void
StringIndexMarisa::Load(const Config& config) {
    auto index_files =
        GetValueFromConfig<std::vector<std::string>>(config, "index_files");
    AssertInfo(index_files.has_value(),
               "index file paths is empty when load index");
    auto index_datas = file_manager_->LoadIndexToMemory(index_files.value());
    AssembleIndexDatas(index_datas);
    BinarySet binary_set;
    for (auto& [key, data] : index_datas) {
        auto size = data->Size();
        auto deleter = [&](uint8_t*) {};  // avoid repeated deconstruction
        auto buf = std::shared_ptr<uint8_t[]>(
            (uint8_t*)const_cast<void*>(data->Data()), deleter);
        binary_set.Append(key, buf, size);
    }

    LoadWithoutAssemble(binary_set, config);
}

const TargetBitmap
StringIndexMarisa::In(size_t n, const std::string* values) {
    TargetBitmap bitset(str_ids_.size());
    for (size_t i = 0; i < n; i++) {
        auto str = values[i];
        auto str_id = lookup(str);
        if (valid_str_id(str_id)) {
            auto offsets = str_ids_to_offsets_[str_id];
            for (auto offset : offsets) {
                bitset[offset] = true;
            }
        }
    }
    return bitset;
}

const TargetBitmap
StringIndexMarisa::NotIn(size_t n, const std::string* values) {
    TargetBitmap bitset(str_ids_.size(), true);
    for (size_t i = 0; i < n; i++) {
        auto str = values[i];
        auto str_id = lookup(str);
        if (valid_str_id(str_id)) {
            auto offsets = str_ids_to_offsets_[str_id];
            for (auto offset : offsets) {
                bitset[offset] = false;
            }
        }
    }
    return bitset;
}

const TargetBitmap
StringIndexMarisa::Range(std::string value, OpType op) {
    auto count = Count();
    TargetBitmap bitset(count);
    marisa::Agent agent;
    for (size_t offset = 0; offset < count; ++offset) {
        agent.set_query(str_ids_[offset]);
        trie_.reverse_lookup(agent);
        std::string raw_data(agent.key().ptr(), agent.key().length());
        bool set = false;
        switch (op) {
            case OpType::LessThan:
                set = raw_data.compare(value) < 0;
                break;
            case OpType::LessEqual:
                set = raw_data.compare(value) <= 0;
                break;
            case OpType::GreaterThan:
                set = raw_data.compare(value) > 0;
                break;
            case OpType::GreaterEqual:
                set = raw_data.compare(value) >= 0;
                break;
            default:
                throw std::invalid_argument(
                    std::string("Invalid OperatorType: ") +
                    std::to_string((int)op) + "!");
        }
        if (set) {
            bitset[offset] = true;
        }
    }
    return bitset;
}

const TargetBitmap
StringIndexMarisa::Range(std::string lower_bound_value,
                         bool lb_inclusive,
                         std::string upper_bound_value,
                         bool ub_inclusive) {
    auto count = Count();
    TargetBitmap bitset(count);
    if (lower_bound_value.compare(upper_bound_value) > 0 ||
        (lower_bound_value.compare(upper_bound_value) == 0 &&
         !(lb_inclusive && ub_inclusive))) {
        return bitset;
    }
    marisa::Agent agent;
    for (size_t offset = 0; offset < count; ++offset) {
        agent.set_query(str_ids_[offset]);
        trie_.reverse_lookup(agent);
        std::string raw_data(agent.key().ptr(), agent.key().length());
        bool set = true;
        if (lb_inclusive) {
            set &= raw_data.compare(lower_bound_value) >= 0;
        } else {
            set &= raw_data.compare(lower_bound_value) > 0;
        }
        if (ub_inclusive) {
            set &= raw_data.compare(upper_bound_value) <= 0;
        } else {
            set &= raw_data.compare(upper_bound_value) < 0;
        }
        if (set) {
            bitset[offset] = true;
        }
    }
    return bitset;
}

const TargetBitmap
StringIndexMarisa::PrefixMatch(std::string_view prefix) {
    TargetBitmap bitset(str_ids_.size());
    auto matched = prefix_match(prefix);
    for (const auto str_id : matched) {
        auto offsets = str_ids_to_offsets_[str_id];
        for (auto offset : offsets) {
            bitset[offset] = true;
        }
    }
    return bitset;
}

void
StringIndexMarisa::fill_str_ids(size_t n, const std::string* values) {
    str_ids_.resize(n);
    for (size_t i = 0; i < n; i++) {
        auto str = values[i];
        auto str_id = lookup(str);
        AssertInfo(valid_str_id(str_id), "invalid marisa key");
        str_ids_[i] = str_id;
    }
}

void
StringIndexMarisa::fill_offsets() {
    for (size_t offset = 0; offset < str_ids_.size(); offset++) {
        auto str_id = str_ids_[offset];
        if (str_ids_to_offsets_.find(str_id) == str_ids_to_offsets_.end()) {
            str_ids_to_offsets_[str_id] = std::vector<size_t>{};
        }
        str_ids_to_offsets_[str_id].push_back(offset);
    }
}

size_t
StringIndexMarisa::lookup(const std::string_view str) {
    marisa::Agent agent;
    agent.set_query(str.data());
    if (trie_.lookup(agent)) {
        return agent.key().id();
    }

    // not found the string in trie
    return MARISA_INVALID_KEY_ID;
}

std::vector<size_t>
StringIndexMarisa::prefix_match(const std::string_view prefix) {
    std::vector<size_t> ret;
    marisa::Agent agent;
    agent.set_query(prefix.data());
    while (trie_.predictive_search(agent)) {
        ret.push_back(agent.key().id());
    }
    return ret;
}

std::string
StringIndexMarisa::Reverse_Lookup(size_t offset) const {
    AssertInfo(offset < str_ids_.size(), "out of range of total count");
    marisa::Agent agent;
    agent.set_query(str_ids_[offset]);
    trie_.reverse_lookup(agent);
    return std::string(agent.key().ptr(), agent.key().length());
}

#endif

}  // namespace milvus::index
