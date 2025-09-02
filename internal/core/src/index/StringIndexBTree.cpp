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

#include "index/StringIndexBTree.h"

#include <algorithm>
#include <cstring>
#include <numeric>

#include "common/EasyAssert.h"
#include "common/Exception.h"
#include "common/Utils.h"
#include "common/Slice.h"
#include "index/Utils.h"
#include "storage/Util.h"
#include "storage/FileWriter.h"
#include "common/File.h"
#include "index/Meta.h"
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

namespace milvus::index {

constexpr size_t NULL_OFFSET = std::numeric_limits<size_t>::max();
constexpr const char* BTREE_INDEX_DATA = "btree_index_data";
constexpr const char* BTREE_POSTING_LISTS = "btree_posting_lists";
constexpr const char* BTREE_REVERSE_INDEX = "btree_reverse_index";

// PostingListManager Implementation
PostingListManager::PostingListManager() = default;

PostingListManager::~PostingListManager() = default;

size_t
PostingListManager::Store(std::vector<size_t>&& posting_list) {
    size_t id = posting_lists_.size();
    posting_lists_.emplace_back(std::move(posting_list));
    return id;
}

const std::vector<size_t>&
PostingListManager::Get(size_t id) const {
    AssertInfo(id < posting_lists_.size(), "Invalid posting list ID");
    return posting_lists_[id];
}

std::vector<size_t>&
PostingListManager::GetMutable(size_t id) {
    AssertInfo(id < posting_lists_.size(), "Invalid posting list ID");
    return posting_lists_[id];
}

size_t
PostingListManager::TotalSize() const {
    size_t total = 0;
    for (const auto& list : posting_lists_) {
        total += list.size() * sizeof(size_t);
    }
    return total;
}

size_t
PostingListManager::SerializedSize() const {
    size_t size = sizeof(size_t);  // Number of lists
    for (const auto& list : posting_lists_) {
        size += sizeof(size_t);                // List size
        size += list.size() * sizeof(size_t);  // List data
    }
    return size;
}

void
PostingListManager::Serialize(uint8_t* buffer, size_t& offset) const {
    size_t num_lists = posting_lists_.size();
    memcpy(buffer + offset, &num_lists, sizeof(size_t));
    offset += sizeof(size_t);

    for (const auto& list : posting_lists_) {
        size_t list_size = list.size();
        memcpy(buffer + offset, &list_size, sizeof(size_t));
        offset += sizeof(size_t);

        if (list_size > 0) {
            memcpy(buffer + offset, list.data(), list_size * sizeof(size_t));
            offset += list_size * sizeof(size_t);
        }
    }
}

void
PostingListManager::Deserialize(const uint8_t* buffer, size_t& offset) {
    size_t num_lists;
    memcpy(&num_lists, buffer + offset, sizeof(size_t));
    offset += sizeof(size_t);

    posting_lists_.clear();
    posting_lists_.reserve(num_lists);

    for (size_t i = 0; i < num_lists; ++i) {
        size_t list_size;
        memcpy(&list_size, buffer + offset, sizeof(size_t));
        offset += sizeof(size_t);

        std::vector<size_t> list(list_size);
        if (list_size > 0) {
            memcpy(list.data(), buffer + offset, list_size * sizeof(size_t));
            offset += list_size * sizeof(size_t);
        }
        posting_lists_.emplace_back(std::move(list));
    }
}

// StringIndexBTree Implementation
StringIndexBTree::StringIndexBTree(
    const storage::FileManagerContext& file_manager_context,
    size_t inline_threshold)
    : StringIndex(BTREE_INDEX_TYPE),
      inline_threshold_(inline_threshold),
      total_rows_(0),
      built_(false),
      num_inline_lists_(0),
      num_external_lists_(0),
      total_posting_size_(0) {
    field_id_ = file_manager_context.fieldDataMeta.field_id;
    if (file_manager_context.Valid()) {
        file_manager_ =
            std::make_shared<storage::MemFileManagerImpl>(file_manager_context);
    }
    posting_manager_ = std::make_unique<PostingListManager>();
}

StringIndexBTree::~StringIndexBTree() = default;

int64_t
StringIndexBTree::Size() {
    return btree_.size();
}

void
StringIndexBTree::Build(size_t n,
                        const std::string* values,
                        const bool* valid_data) {
    if (built_) {
        ThrowInfo(IndexAlreadyBuild, "index has been built");
    }

    total_rows_ = n;
    offset_to_string_.resize(n);

    // Build the index
    for (size_t i = 0; i < n; ++i) {
        if (valid_data != nullptr && !valid_data[i]) {
            offset_to_string_[i] = "";  // Null marker
            continue;
        }

        const std::string& value = values[i];
        offset_to_string_[i] = value;
        AddPosting(value, i);
    }

    // Optimize posting lists after building
    OptimizePostingLists();

    built_ = true;
}

void
StringIndexBTree::Build(const Config& config) {
    if (built_) {
        ThrowInfo(IndexAlreadyBuild, "index has been built");
    }
    auto field_datas =
        storage::CacheRawDataAndFillMissing(file_manager_, config);

    BuildWithFieldData(field_datas);
}

void
StringIndexBTree::BuildWithFieldData(
    const std::vector<FieldDataPtr>& field_datas) {
    LOG_INFO("Start to build btree index, field id: {}", field_id_);
    index_build_begin_ = std::chrono::system_clock::now();

    int64_t total_num_rows = 0;
    // Count total rows
    for (const auto& data : field_datas) {
        total_num_rows += data->get_num_rows();
    }

    total_rows_ = total_num_rows;
    offset_to_string_.resize(total_rows_);

    // Build index
    int64_t offset = 0;
    for (const auto& data : field_datas) {
        auto slice_num = data->get_num_rows();
        for (int64_t i = 0; i < slice_num; ++i) {
            if (data->is_valid(i)) {
                const std::string& value =
                    *static_cast<const std::string*>(data->RawValue(i));
                offset_to_string_[offset] = value;
                AddPosting(value, offset);
            } else {
                offset_to_string_[offset] = "";  // Null marker
            }
            offset++;
        }
    }

    // Optimize posting lists after building
    OptimizePostingLists();

    built_ = true;
}

void
StringIndexBTree::AddPosting(const std::string& key, size_t offset) {
    auto it = btree_.find(key);
    if (it == btree_.end()) {
        // New key, create inline posting list
        PostingListPtr ptr;
        ptr.type = PostingListPtr::INLINE_STORAGE;
        ptr.count = 1;
        ptr.inline_data.push_back(offset);
        btree_[key] = std::move(ptr);
        num_inline_lists_++;
    } else {
        // Existing key, add to posting list
        PostingListPtr& ptr = it->second;
        ptr.count++;

        if (ptr.type == PostingListPtr::INLINE_STORAGE) {
            ptr.inline_data.push_back(offset);
            // Check if we should externalize
            MaybeExternalize(ptr);
        } else {
            // External storage, update the external list
            auto& list = posting_manager_->GetMutable(ptr.external_id);
            list.push_back(offset);
        }
    }
    total_posting_size_++;
}

void
StringIndexBTree::MaybeExternalize(PostingListPtr& ptr) {
    if (ptr.type == PostingListPtr::INLINE_STORAGE &&
        ptr.inline_data.size() > inline_threshold_) {
        // Convert to external storage
        size_t external_id =
            posting_manager_->Store(std::move(ptr.inline_data));
        ptr.type = PostingListPtr::EXTERNAL_STORAGE;
        ptr.external_id = external_id;
        ptr.inline_data.clear();

        num_inline_lists_--;
        num_external_lists_++;
    }
}

void
StringIndexBTree::OptimizePostingLists() {
    // Sort posting lists for better cache locality and binary search
    for (auto& [key, ptr] : btree_) {
        if (ptr.type == PostingListPtr::INLINE_STORAGE) {
            std::sort(ptr.inline_data.begin(), ptr.inline_data.end());
        } else {
            auto& list = posting_manager_->GetMutable(ptr.external_id);
            std::sort(list.begin(), list.end());
        }
    }
}

std::vector<size_t>
StringIndexBTree::GetPostingList(const std::string& key) const {
    auto it = btree_.find(key);
    if (it == btree_.end()) {
        return {};
    }
    return GetPostingList(it->second);
}

std::vector<size_t>
StringIndexBTree::GetPostingList(const PostingListPtr& ptr) const {
    if (ptr.type == PostingListPtr::INLINE_STORAGE) {
        return ptr.inline_data;
    } else {
        return posting_manager_->Get(ptr.external_id);
    }
}

const TargetBitmap
StringIndexBTree::In(size_t n, const std::string* values) {
    TargetBitmap bitset(total_rows_);

    for (size_t i = 0; i < n; ++i) {
        auto posting_list = GetPostingList(values[i]);
        for (size_t offset : posting_list) {
            bitset[offset] = true;
        }
    }

    return bitset;
}

const TargetBitmap
StringIndexBTree::NotIn(size_t n, const std::string* values) {
    TargetBitmap bitset(total_rows_, true);

    for (size_t i = 0; i < n; ++i) {
        auto posting_list = GetPostingList(values[i]);
        for (size_t offset : posting_list) {
            bitset[offset] = false;
        }
    }

    // Handle nulls - NotIn(null) is false
    for (size_t i = 0; i < total_rows_; ++i) {
        if (offset_to_string_[i].empty()) {
            bitset[i] = false;
        }
    }

    return bitset;
}

const TargetBitmap
StringIndexBTree::IsNull() {
    TargetBitmap bitset(total_rows_);

    for (size_t i = 0; i < total_rows_; ++i) {
        if (offset_to_string_[i].empty()) {
            bitset[i] = true;
        }
    }

    return bitset;
}

const TargetBitmap
StringIndexBTree::IsNotNull() {
    TargetBitmap bitset(total_rows_, true);

    // for (size_t i = 0; i < total_rows_; ++i) {
    //     if (!offset_to_string_[i].empty()) {
    //         bitset[i] = true;
    //     }
    // }

    return bitset;
}

const TargetBitmap
StringIndexBTree::Range(std::string value, OpType op) {
    TargetBitmap bitset(total_rows_);

    switch (op) {
        case OpType::GreaterThan: {
            auto it = btree_.upper_bound(value);
            while (it != btree_.end()) {
                auto posting_list = GetPostingList(it->second);
                for (size_t offset : posting_list) {
                    bitset[offset] = true;
                }
                ++it;
            }
            break;
        }
        case OpType::GreaterEqual: {
            auto it = btree_.lower_bound(value);
            while (it != btree_.end()) {
                auto posting_list = GetPostingList(it->second);
                for (size_t offset : posting_list) {
                    bitset[offset] = true;
                }
                ++it;
            }
            break;
        }
        case OpType::LessThan: {
            auto it = btree_.begin();
            auto end = btree_.lower_bound(value);
            while (it != end) {
                auto posting_list = GetPostingList(it->second);
                for (size_t offset : posting_list) {
                    bitset[offset] = true;
                }
                ++it;
            }
            break;
        }
        case OpType::LessEqual: {
            auto it = btree_.begin();
            auto end = btree_.upper_bound(value);
            while (it != end) {
                auto posting_list = GetPostingList(it->second);
                for (size_t offset : posting_list) {
                    bitset[offset] = true;
                }
                ++it;
            }
            break;
        }
        default:
            ThrowInfo(
                OpTypeInvalid,
                fmt::format("Invalid OperatorType: {}", static_cast<int>(op)));
    }

    return bitset;
}

const TargetBitmap
StringIndexBTree::Range(std::string lower_bound_value,
                        bool lb_inclusive,
                        std::string upper_bound_value,
                        bool ub_inclusive) {
    TargetBitmap bitset(total_rows_);

    if (lower_bound_value > upper_bound_value ||
        (lower_bound_value == upper_bound_value &&
         !(lb_inclusive && ub_inclusive))) {
        return bitset;
    }

    // Find range boundaries
    auto begin = lb_inclusive ? btree_.lower_bound(lower_bound_value)
                              : btree_.upper_bound(lower_bound_value);
    auto end = ub_inclusive ? btree_.upper_bound(upper_bound_value)
                            : btree_.lower_bound(upper_bound_value);

    // Iterate through range
    for (auto it = begin; it != end; ++it) {
        auto posting_list = GetPostingList(it->second);
        for (size_t offset : posting_list) {
            bitset[offset] = true;
        }
    }

    return bitset;
}

const TargetBitmap
StringIndexBTree::PrefixMatch(const std::string_view prefix) {
    TargetBitmap bitset(total_rows_);

    // Find the first key >= prefix
    auto it = btree_.lower_bound(std::string(prefix));

    // Iterate while keys have the prefix
    while (it != btree_.end()) {
        if (!milvus::PrefixMatch(it->first, prefix)) {
            break;
        }

        auto posting_list = GetPostingList(it->second);
        for (size_t offset : posting_list) {
            bitset[offset] = true;
        }
        ++it;
    }

    return bitset;
}

std::optional<std::string>
StringIndexBTree::Reverse_Lookup(size_t offset) const {
    AssertInfo(offset < total_rows_, "Offset out of range");

    const std::string& value = offset_to_string_[offset];
    if (value.empty()) {
        return std::nullopt;  // Null value
    }

    return value;
}

BinarySet
StringIndexBTree::Serialize(const Config& config) {
    BinarySet res_set;

    // Serialize BTree structure
    size_t btree_size = sizeof(size_t);  // Number of entries
    for (const auto& [key, ptr] : btree_) {
        btree_size += sizeof(size_t);  // Key length
        btree_size += key.size();      // Key data
        btree_size += sizeof(PostingListPtr::StorageType);
        btree_size += sizeof(size_t);  // Count

        if (ptr.type == PostingListPtr::INLINE_STORAGE) {
            btree_size += sizeof(size_t);  // Inline data size
            btree_size += ptr.inline_data.size() * sizeof(size_t);
        } else {
            btree_size += sizeof(size_t);  // External ID
        }
    }

    std::shared_ptr<uint8_t[]> btree_data(new uint8_t[btree_size]);
    size_t offset = 0;

    size_t num_entries = btree_.size();
    memcpy(btree_data.get() + offset, &num_entries, sizeof(size_t));
    offset += sizeof(size_t);

    for (const auto& [key, ptr] : btree_) {
        size_t key_len = key.size();
        memcpy(btree_data.get() + offset, &key_len, sizeof(size_t));
        offset += sizeof(size_t);

        memcpy(btree_data.get() + offset, key.data(), key_len);
        offset += key_len;

        memcpy(btree_data.get() + offset,
               &ptr.type,
               sizeof(PostingListPtr::StorageType));
        offset += sizeof(PostingListPtr::StorageType);

        memcpy(btree_data.get() + offset, &ptr.count, sizeof(size_t));
        offset += sizeof(size_t);

        if (ptr.type == PostingListPtr::INLINE_STORAGE) {
            size_t inline_size = ptr.inline_data.size();
            memcpy(btree_data.get() + offset, &inline_size, sizeof(size_t));
            offset += sizeof(size_t);

            if (inline_size > 0) {
                memcpy(btree_data.get() + offset,
                       ptr.inline_data.data(),
                       inline_size * sizeof(size_t));
                offset += inline_size * sizeof(size_t);
            }
        } else {
            memcpy(btree_data.get() + offset, &ptr.external_id, sizeof(size_t));
            offset += sizeof(size_t);
        }
    }

    res_set.Append(BTREE_INDEX_DATA, btree_data, btree_size);

    // Serialize external posting lists
    size_t posting_size = posting_manager_->SerializedSize();
    std::shared_ptr<uint8_t[]> posting_data(new uint8_t[posting_size]);
    offset = 0;
    posting_manager_->Serialize(posting_data.get(), offset);
    res_set.Append(BTREE_POSTING_LISTS, posting_data, posting_size);

    // Serialize reverse index
    size_t reverse_size = sizeof(size_t) + total_rows_ * sizeof(size_t);
    for (const auto& str : offset_to_string_) {
        reverse_size += sizeof(size_t) + str.size();
    }

    std::shared_ptr<uint8_t[]> reverse_data(new uint8_t[reverse_size]);
    offset = 0;

    size_t num_rows = total_rows_;
    memcpy(reverse_data.get() + offset, &num_rows, sizeof(size_t));
    offset += sizeof(size_t);

    for (const auto& str : offset_to_string_) {
        size_t str_len = str.size();
        memcpy(reverse_data.get() + offset, &str_len, sizeof(size_t));
        offset += sizeof(size_t);

        if (str_len > 0) {
            memcpy(reverse_data.get() + offset, str.data(), str_len);
            offset += str_len;
        }
    }

    res_set.Append(BTREE_REVERSE_INDEX, reverse_data, reverse_size);

    milvus::Disassemble(res_set);

    return res_set;
}

void
StringIndexBTree::LoadWithoutAssemble(const BinarySet& binary_set,
                                      const Config& config) {
    // Check if we should use mmap
    use_mmap_ = config.contains(MMAP_FILE_PATH);

    if (use_mmap_) {
        // Create temporary files for mmap
        auto uuid = boost::uuids::random_generator()();
        auto uuid_string = boost::uuids::to_string(uuid);
        auto btree_file = std::string("/tmp/btree_") + uuid_string;
        auto posting_file = std::string("/tmp/posting_") + uuid_string;
        auto reverse_file = std::string("/tmp/reverse_") + uuid_string;

        // Write btree data to file
        auto btree_data = binary_set.GetByName(BTREE_INDEX_DATA);
        {
            storage::FileWriter writer(btree_file);
            writer.Write(btree_data->data.get(), btree_data->size);
            writer.Finish();
        }

        // Write posting data to file
        auto posting_data = binary_set.GetByName(BTREE_POSTING_LISTS);
        {
            storage::FileWriter writer(posting_file);
            writer.Write(posting_data->data.get(), posting_data->size);
            writer.Finish();
        }

        // Write reverse index data to file
        auto reverse_data = binary_set.GetByName(BTREE_REVERSE_INDEX);
        {
            storage::FileWriter writer(reverse_file);
            writer.Write(reverse_data->data.get(), reverse_data->size);
            writer.Finish();
        }

        // Mmap all files
        int btree_fd = open(btree_file.c_str(), O_RDONLY);
        AssertInfo(btree_fd != -1, "Failed to open btree file for mmap");
        struct stat st;
        fstat(btree_fd, &st);
        size_t btree_size = st.st_size;

        const uint8_t* btree_mmap_data = static_cast<const uint8_t*>(
            mmap(nullptr, btree_size, PROT_READ, MAP_PRIVATE, btree_fd, 0));
        AssertInfo(btree_mmap_data != MAP_FAILED, "Failed to mmap btree file");
        close(btree_fd);

        int posting_fd = open(posting_file.c_str(), O_RDONLY);
        AssertInfo(posting_fd != -1, "Failed to open posting file for mmap");
        fstat(posting_fd, &st);
        size_t posting_size = st.st_size;

        const uint8_t* posting_mmap_data = static_cast<const uint8_t*>(
            mmap(nullptr, posting_size, PROT_READ, MAP_PRIVATE, posting_fd, 0));
        AssertInfo(posting_mmap_data != MAP_FAILED,
                   "Failed to mmap posting file");
        close(posting_fd);

        int reverse_fd = open(reverse_file.c_str(), O_RDONLY);
        AssertInfo(reverse_fd != -1, "Failed to open reverse file for mmap");
        fstat(reverse_fd, &st);
        size_t reverse_size = st.st_size;

        const uint8_t* reverse_mmap_data = static_cast<const uint8_t*>(
            mmap(nullptr, reverse_size, PROT_READ, MAP_PRIVATE, reverse_fd, 0));
        AssertInfo(reverse_mmap_data != MAP_FAILED,
                   "Failed to mmap reverse file");
        close(reverse_fd);

        // Clean up temp files
        mmap_file_raii_ = std::make_unique<MmapFileRAII>(btree_file);
        unlink(posting_file.c_str());
        unlink(reverse_file.c_str());

        // Load from mmap data
        LoadFromMmapData(btree_mmap_data,
                         btree_size,
                         posting_mmap_data,
                         posting_size,
                         reverse_mmap_data,
                         reverse_size);
    } else {
        // Original loading logic
        auto btree_data = binary_set.GetByName(BTREE_INDEX_DATA);
        auto posting_data = binary_set.GetByName(BTREE_POSTING_LISTS);
        auto reverse_data = binary_set.GetByName(BTREE_REVERSE_INDEX);

        LoadFromMemoryData(btree_data->data.get(),
                           btree_data->size,
                           posting_data->data.get(),
                           posting_data->size,
                           reverse_data->data.get(),
                           reverse_data->size);
    }
}

void
StringIndexBTree::LoadFromMmapData(const uint8_t* btree_data,
                                   size_t btree_size,
                                   const uint8_t* posting_data,
                                   size_t posting_size,
                                   const uint8_t* reverse_data,
                                   size_t reverse_size) {
    LoadFromMemoryData(btree_data,
                       btree_size,
                       posting_data,
                       posting_size,
                       reverse_data,
                       reverse_size);
}

void
StringIndexBTree::LoadFromMemoryData(const uint8_t* btree_data,
                                     size_t btree_size,
                                     const uint8_t* posting_data,
                                     size_t posting_size,
                                     const uint8_t* reverse_data,
                                     size_t reverse_size) {
    // Load BTree structure
    size_t offset = 0;

    size_t num_entries;
    memcpy(&num_entries, btree_data + offset, sizeof(size_t));
    offset += sizeof(size_t);

    btree_.clear();

    for (size_t i = 0; i < num_entries; ++i) {
        size_t key_len;
        memcpy(&key_len, btree_data + offset, sizeof(size_t));
        offset += sizeof(size_t);

        std::string key(reinterpret_cast<const char*>(btree_data + offset),
                        key_len);
        offset += key_len;

        PostingListPtr ptr;
        memcpy(&ptr.type,
               btree_data + offset,
               sizeof(PostingListPtr::StorageType));
        offset += sizeof(PostingListPtr::StorageType);

        memcpy(&ptr.count, btree_data + offset, sizeof(size_t));
        offset += sizeof(size_t);

        if (ptr.type == PostingListPtr::INLINE_STORAGE) {
            size_t inline_size;
            memcpy(&inline_size, btree_data + offset, sizeof(size_t));
            offset += sizeof(size_t);

            ptr.inline_data.resize(inline_size);
            if (inline_size > 0) {
                memcpy(ptr.inline_data.data(),
                       btree_data + offset,
                       inline_size * sizeof(size_t));
                offset += inline_size * sizeof(size_t);
            }
            num_inline_lists_++;
        } else {
            memcpy(&ptr.external_id, btree_data + offset, sizeof(size_t));
            offset += sizeof(size_t);
            num_external_lists_++;
        }

        btree_[key] = std::move(ptr);
    }

    // Load external posting lists
    offset = 0;
    posting_manager_->Deserialize(posting_data, offset);

    // Load reverse index
    offset = 0;

    memcpy(&total_rows_, reverse_data + offset, sizeof(size_t));
    offset += sizeof(size_t);

    offset_to_string_.resize(total_rows_);

    for (size_t i = 0; i < total_rows_; ++i) {
        size_t str_len;
        memcpy(&str_len, reverse_data + offset, sizeof(size_t));
        offset += sizeof(size_t);

        if (str_len > 0) {
            offset_to_string_[i] = std::string(
                reinterpret_cast<const char*>(reverse_data + offset), str_len);
            offset += str_len;
        } else {
            offset_to_string_[i] = "";  // Null marker
        }
    }

    built_ = true;
}

void
StringIndexBTree::Load(const BinarySet& set, const Config& config) {
    milvus::Assemble(const_cast<BinarySet&>(set));
    LoadWithoutAssemble(set, config);
}

void
StringIndexBTree::Load(milvus::tracer::TraceContext ctx, const Config& config) {
    auto index_files =
        GetValueFromConfig<std::vector<std::string>>(config, "index_files");
    AssertInfo(index_files.has_value(),
               "index file paths is empty when load index");
    auto index_datas = file_manager_->LoadIndexToMemory(
        index_files.value(), milvus::proto::common::LoadPriority::LOW);
    BinarySet binary_set;
    AssembleIndexDatas(index_datas, binary_set);
    LoadWithoutAssemble(binary_set, config);
}

IndexStatsPtr
StringIndexBTree::Upload(const Config& config) {
    auto binary_set = Serialize(config);
    file_manager_->AddFile(binary_set);

    auto remote_paths_to_size = file_manager_->GetRemotePathsToFileSize();

    auto index_build_end = std::chrono::system_clock::now();
    auto index_build_duration =
        std::chrono::duration<double>(index_build_end - index_build_begin_)
            .count();
    LOG_INFO("index build done for btree index, field id: {}, duration: {}s",
             field_id_,
             index_build_duration);

    return IndexStats::NewFromSizeMap(file_manager_->GetAddedTotalMemSize(),
                                      remote_paths_to_size);
}

}  // namespace milvus::index