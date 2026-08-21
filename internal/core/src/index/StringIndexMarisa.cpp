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

#include <boost/uuid/uuid_io.hpp>
#include <arrow/status.h>
#include "common/FastMem.h"
#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>
#include <algorithm>
#include <cstdint>
#include <cstring>
#include <exception>
#include <filesystem>
#include <iosfwd>
#include <limits>
#include <memory>
#include <optional>
#include <system_error>
#include <type_traits>

#include "bitset/bitset.h"
#include "boost/uuid/random_generator.hpp"
#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/FieldDataInterface.h"
#include "common/File.h"
#include "common/Slice.h"
#include "common/Tracer.h"
#include "common/RegexQuery.h"
#include "common/Types.h"
#include "common/Utils.h"
#include "fmt/core.h"
#include "folly/ScopeGuard.h"
#include "index/Meta.h"
#include "index/StringIndexMarisa.h"
#include "index/Utils.h"
#include "knowhere/binaryset.h"
#include "marisa/agent.h"
#include "marisa/base.h"
#include "marisa/key.h"
#include "marisa/keyset.h"
#include "nlohmann/json.hpp"
#include "pb/common.pb.h"
#include "storage/FileWriter.h"
#include "storage/LocalChunkManager.h"
#include "storage/IndexEntryReader.h"
#include "storage/IndexEntryWriter.h"
#include "storage/MemFileManagerImpl.h"
#include "storage/ThreadPools.h"
#include "storage/Util.h"

namespace milvus::index {

namespace {

constexpr uint32_t MARISA_CSR_FORMAT_VERSION = 1;
constexpr const char* MARISA_CSR_FORMAT_VERSION_META =
    "marisa_csr_format_version";

struct MarisaLoadContext {
    bool is_mmap{false};
    bool has_csr{false};
    std::string file_name;
    size_t str_ids_bytes{0};
    size_t csr_index_bytes{0};
    size_t csr_offsets_bytes{0};
    size_t csr_num_keys{0};

    std::shared_ptr<std::vector<int64_t>> str_ids;
    std::shared_ptr<std::vector<uint32_t>> csr_index;
    std::shared_ptr<std::vector<uint32_t>> csr_offsets;
    std::shared_ptr<storage::MmapFileTarget> trie_file;
    std::shared_ptr<storage::MmapFileTarget> str_ids_file;
    std::shared_ptr<storage::MmapFileTarget> csr_file;
};

}  // namespace

StringIndexMarisa::StringIndexMarisa(
    const storage::FileManagerContext& file_manager_context)
    : StringIndex(MARISA_TRIE) {
    if (file_manager_context.Valid()) {
        this->file_manager_ =
            std::make_shared<storage::MemFileManagerImpl>(file_manager_context);
    }
}

int64_t
StringIndexMarisa::Size() {
    return total_size_;
}

void
StringIndexMarisa::ComputeByteSize() {
    StringIndex::ComputeByteSize();
    int64_t total = cached_byte_size_;

    // Size of the trie structure (marisa trie uses io_size() for serialized/memory size)
    total += trie_.io_size();

    // str_ids
    if (str_ids_mmap_data_ != nullptr && str_ids_mmap_data_ != MAP_FAILED) {
        total += str_ids_mmap_size_;
    } else {
        total += str_ids_.capacity() * sizeof(int64_t);
    }

    // CSR index + offsets
    if (csr_mmap_data_ != nullptr && csr_mmap_data_ != MAP_FAILED) {
        total += csr_mmap_size_;
    } else {
        total += csr_index_.capacity() * sizeof(uint32_t);
        total += csr_offsets_.capacity() * sizeof(uint32_t);
    }

    cached_byte_size_ = total;
}

int64_t
StringIndexMarisa::CalculateTotalSize() const {
    int64_t size = 0;

    // Size of the trie structure
    // marisa trie uses io_size() to get the serialized size
    // which approximates the memory usage
    size += trie_.io_size();

    // Size of str_ids (vector owner or mmap region)
    if (str_ids_mmap_data_ != nullptr && str_ids_mmap_data_ != MAP_FAILED) {
        size += str_ids_mmap_size_;
    } else {
        size += str_ids_.size() * sizeof(int64_t);
    }

    // CSR index + offsets
    if (csr_mmap_data_ != nullptr && csr_mmap_data_ != MAP_FAILED) {
        size += csr_mmap_size_;
    } else {
        size += csr_index_.size() * sizeof(uint32_t);
        size += csr_offsets_.size() * sizeof(uint32_t);
    }

    return size;
}

bool
valid_str_id(size_t str_id) {
    return str_id != MARISA_NULL_KEY_ID && str_id != MARISA_INVALID_KEY_ID;
}

void
ValidateMarisaEntryElementSize(const char* entry_name,
                               size_t bytes,
                               size_t element_size) {
    AssertInfo(bytes % element_size == 0,
               "invalid {} size: expected multiple of {}, got {}",
               entry_name,
               element_size,
               bytes);
}

void
StringIndexMarisa::Build(const Config& config) {
    if (built_) {
        ThrowInfo(IndexAlreadyBuild, "index has been built");
    }
    auto field_datas = storage::CacheRawDataAndFillMissing(
        std::static_pointer_cast<storage::MemFileManagerImpl>(
            this->file_manager_),
        config);

    BuildWithFieldData(field_datas);
}

void
StringIndexMarisa::BuildWithFieldData(
    const std::vector<FieldDataPtr>& field_datas) {
    int64_t total_num_rows = 0;

    // fill key set.
    marisa::Keyset keyset;
    for (const auto& data : field_datas) {
        auto slice_num = data->get_num_rows();
        for (int64_t i = 0; i < slice_num; ++i) {
            if (data->is_valid(i)) {
                keyset.push_back(
                    (*static_cast<const std::string*>(data->RawValue(i)))
                        .c_str());
            }
        }
        total_num_rows += slice_num;
    }
    trie_.build(keyset, MARISA_LABEL_ORDER);

    // fill str_ids_
    str_ids_.resize(total_num_rows, MARISA_NULL_KEY_ID);
    int64_t offset = 0;
    for (const auto& data : field_datas) {
        auto slice_num = data->get_num_rows();
        for (int64_t i = 0; i < slice_num; ++i) {
            if (data->is_valid(i)) {
                auto str_id =
                    lookup(*static_cast<const std::string*>(data->RawValue(i)));
                AssertInfo(valid_str_id(str_id), "invalid marisa key");
                str_ids_[offset] = str_id;
            }
            offset++;
        }
    }

    str_ids_ptr_ = str_ids_.data();
    str_ids_size_ = str_ids_.size();
    fill_offsets();

    built_ = true;
    total_size_ = CalculateTotalSize();
    ComputeByteSize();
}

void
StringIndexMarisa::Build(size_t n,
                         const std::string* values,
                         const bool* valid_data) {
    if (built_) {
        ThrowInfo(IndexAlreadyBuild, "index has been built");
    }

    marisa::Keyset keyset;
    {
        // fill key set.
        for (size_t i = 0; i < n; i++) {
            if (valid_data == nullptr || valid_data[i]) {
                keyset.push_back(values[i].c_str());
            }
        }
    }

    trie_.build(keyset, MARISA_LABEL_ORDER);
    fill_str_ids(n, values, valid_data);
    str_ids_ptr_ = str_ids_.data();
    str_ids_size_ = str_ids_.size();
    fill_offsets();

    built_ = true;
    total_size_ = CalculateTotalSize();
    ComputeByteSize();
}

BinarySet
StringIndexMarisa::Serialize(const Config& config) {
    auto uuid = boost::uuids::random_generator()();
    auto uuid_string = boost::uuids::to_string(uuid);
    auto file = std::string("/tmp/") + uuid_string;

    auto fd = open(
        file.c_str(), O_RDWR | O_CREAT | O_EXCL, S_IRUSR | S_IWUSR | S_IXUSR);
    AssertInfo(fd != -1, "open file failed");
    trie_.write(fd);

    auto size = get_file_size(fd);
    auto index_data = std::shared_ptr<uint8_t[]>(new uint8_t[size]);
    ReadDataFromFD(fd, index_data.get(), size);

    close(fd);
    remove(file.c_str());

    auto str_ids_len = str_ids_.size() * sizeof(int64_t);
    std::shared_ptr<uint8_t[]> str_ids(new uint8_t[str_ids_len]);
    milvus::fastmem::FastMemcpy(str_ids.get(), str_ids_.data(), str_ids_len);

    BinarySet res_set;
    res_set.Append(MARISA_TRIE_INDEX, index_data, size);
    res_set.Append(MARISA_STR_IDS, str_ids, str_ids_len);

    Disassemble(res_set);

    return res_set;
}

IndexStatsPtr
StringIndexMarisa::Upload(const Config& config) {
    auto binary_set = Serialize(config);
    this->file_manager_->AddFile(binary_set);

    auto remote_paths_to_size = this->file_manager_->GetRemotePathsToFileSize();
    return IndexStats::NewFromSizeMap(
        this->file_manager_->GetAddedTotalMemSize(), remote_paths_to_size);
}

void
StringIndexMarisa::LoadWithoutAssemble(const BinarySet& set,
                                       const Config& config) {
    auto uuid = boost::uuids::random_generator()();
    auto uuid_string = boost::uuids::to_string(uuid);
    // TODO: change the mmap path of marisa index
    auto file_name = std::string("/tmp/") + uuid_string;

    auto index = set.GetByName(MARISA_TRIE_INDEX);
    auto len = index->size;
    auto load_priority =
        GetValueFromConfig<milvus::proto::common::LoadPriority>(
            config, milvus::LOAD_PRIORITY)
            .value_or(milvus::proto::common::LoadPriority::HIGH);

    {
        auto file_writer = storage::FileWriter(
            file_name, storage::io::GetPriorityFromLoadPriority(load_priority));
        file_writer.Write(index->data.get(), len);
        file_writer.Finish();
    }

    if (config.contains(MMAP_FILE_PATH)) {
        auto trie_file_raii = std::make_unique<MmapFileRAII>(file_name);
        trie_.mmap(file_name.c_str());
        mmap_file_raii_ = std::move(trie_file_raii);
    } else {
        auto file = File::Open(file_name, O_RDONLY);
        trie_.read(file.Descriptor());
        mmap_file_raii_ = nullptr;
    }

    if (!config.contains(MMAP_FILE_PATH)) {
        unlink(file_name.c_str());
    }

    auto str_ids = set.GetByName(MARISA_STR_IDS);
    auto str_ids_len = str_ids->size;
    ValidateMarisaEntryElementSize(
        MARISA_STR_IDS, str_ids_len, sizeof(int64_t));
    str_ids_.resize(str_ids_len / sizeof(int64_t), MARISA_NULL_KEY_ID);
    milvus::fastmem::FastMemcpy(
        str_ids_.data(), str_ids->data.get(), str_ids_len);
    str_ids_ptr_ = str_ids_.data();
    str_ids_size_ = str_ids_.size();

    fill_offsets();
    built_ = true;
    total_size_ = CalculateTotalSize();
    ComputeByteSize();
}

void
StringIndexMarisa::Load(const BinarySet& set, const Config& config) {
    milvus::Assemble(const_cast<BinarySet&>(set));
    LoadWithoutAssemble(set, config);
}

void
StringIndexMarisa::Load(milvus::tracer::TraceContext ctx,
                        const Config& config) {
    auto index_files =
        GetValueFromConfig<std::vector<std::string>>(config, "index_files");
    AssertInfo(index_files.has_value(),
               "index file paths is empty when load index");
    auto load_priority =
        GetValueFromConfig<milvus::proto::common::LoadPriority>(
            config, milvus::LOAD_PRIORITY)
            .value_or(milvus::proto::common::LoadPriority::HIGH);
    auto index_datas = this->file_manager_->LoadIndexToMemory(
        index_files.value(), load_priority);
    BinarySet binary_set;
    AssembleIndexDatas(index_datas, binary_set);
    // clear index_datas to free memory early
    index_datas.clear();
    LoadWithoutAssemble(binary_set, config);
}

const TargetBitmap
StringIndexMarisa::In(size_t n, const std::string* values) {
    tracer::AutoSpan span("StringIndexMarisa::In", tracer::GetRootSpan());
    TargetBitmap bitset(str_ids_size_);
    for (size_t i = 0; i < n; i++) {
        const auto& str = values[i];
        auto str_id = lookup(str);
        if (valid_str_id(str_id)) {
            for (size_t j = csr_index_ptr_[str_id];
                 j < csr_index_ptr_[str_id + 1];
                 j++) {
                auto offset = csr_offsets_ptr_[j];
                bitset[offset] = true;
            }
        }
    }
    return bitset;
}

const TargetBitmap
StringIndexMarisa::NotIn(size_t n, const std::string* values) {
    tracer::AutoSpan span("StringIndexMarisa::NotIn", tracer::GetRootSpan());
    TargetBitmap bitset(str_ids_size_, true);
    for (size_t i = 0; i < n; i++) {
        const auto& str = values[i];
        auto str_id = lookup(str);
        if (valid_str_id(str_id)) {
            for (size_t j = csr_index_ptr_[str_id];
                 j < csr_index_ptr_[str_id + 1];
                 j++) {
                auto offset = csr_offsets_ptr_[j];
                bitset[offset] = false;
            }
        }
    }
    // NotIn(null) and In(null) is both false, need to mask with IsNotNull operate
    ResetNull(bitset);
    return bitset;
}

const TargetBitmap
StringIndexMarisa::IsNull() {
    tracer::AutoSpan span("StringIndexMarisa::IsNull", tracer::GetRootSpan());
    TargetBitmap bitset(str_ids_size_);
    SetNull(bitset);
    return bitset;
}

void
StringIndexMarisa::SetNull(TargetBitmap& bitset) {
    tracer::AutoSpan span("StringIndexMarisa::SetNull", tracer::GetRootSpan());
    for (size_t i = 0; i < bitset.size(); i++) {
        if (str_ids_ptr_[i] == MARISA_NULL_KEY_ID) {
            bitset.set(i);
        }
    }
}

void
StringIndexMarisa::ResetNull(TargetBitmap& bitset) {
    tracer::AutoSpan span("StringIndexMarisa::ResetNull",
                          tracer::GetRootSpan());
    for (size_t i = 0; i < bitset.size(); i++) {
        if (str_ids_ptr_[i] == MARISA_NULL_KEY_ID) {
            bitset.reset(i);
        }
    }
}

TargetBitmap
StringIndexMarisa::IsNotNull() {
    tracer::AutoSpan span("StringIndexMarisa::IsNotNull",
                          tracer::GetRootSpan());
    TargetBitmap bitset(str_ids_size_);
    for (size_t i = 0; i < bitset.size(); i++) {
        if (str_ids_ptr_[i] != MARISA_NULL_KEY_ID) {
            bitset.set(i);
        }
    }
    return bitset;
}

const TargetBitmap
StringIndexMarisa::Range(const std::string& value, OpType op) {
    tracer::AutoSpan span("StringIndexMarisa::Range", tracer::GetRootSpan());
    auto count = Count();
    TargetBitmap bitset(count);
    std::vector<size_t> ids;
    marisa::Agent agent;
    bool in_lexico_order = in_lexicographic_order();
    switch (op) {
        case OpType::GreaterThan: {
            if (in_lexico_order) {
                while (trie_.predictive_search(agent)) {
                    std::string_view key(agent.key().ptr(),
                                         agent.key().length());
                    if (key > value) {
                        ids.push_back(agent.key().id());
                        break;
                    }
                };
                // since in lexicographic order, all following nodes is greater than value
                while (trie_.predictive_search(agent)) {
                    ids.push_back(agent.key().id());
                }
            } else {
                // lexicographic order is not guaranteed, check all values
                while (trie_.predictive_search(agent)) {
                    std::string_view key(agent.key().ptr(),
                                         agent.key().length());
                    if (key > value) {
                        ids.push_back(agent.key().id());
                    }
                };
            }
            break;
        }
        case OpType::GreaterEqual: {
            if (in_lexico_order) {
                while (trie_.predictive_search(agent)) {
                    std::string_view key(agent.key().ptr(),
                                         agent.key().length());
                    if (key >= value) {
                        ids.push_back(agent.key().id());
                        break;
                    }
                };
                // since in lexicographic order, all following nodes is greater than or equal value
                while (trie_.predictive_search(agent)) {
                    ids.push_back(agent.key().id());
                }
            } else {
                // lexicographic order is not guaranteed, check all values
                while (trie_.predictive_search(agent)) {
                    std::string_view key(agent.key().ptr(),
                                         agent.key().length());
                    if (key >= value) {
                        ids.push_back(agent.key().id());
                    }
                };
            }
            break;
        }
        case OpType::LessThan: {
            if (in_lexico_order) {
                while (trie_.predictive_search(agent)) {
                    std::string_view key(agent.key().ptr(),
                                         agent.key().length());
                    if (key >= value) {
                        break;
                    }
                    ids.push_back(agent.key().id());
                }
            } else {
                // lexicographic order is not guaranteed, check all values
                while (trie_.predictive_search(agent)) {
                    std::string_view key(agent.key().ptr(),
                                         agent.key().length());
                    if (key < value) {
                        ids.push_back(agent.key().id());
                    }
                };
            }
            break;
        }
        case OpType::LessEqual: {
            if (in_lexico_order) {
                while (trie_.predictive_search(agent)) {
                    std::string_view key(agent.key().ptr(),
                                         agent.key().length());
                    if (key > value) {
                        break;
                    }
                    ids.push_back(agent.key().id());
                }
            } else {
                // lexicographic order is not guaranteed, check all values
                while (trie_.predictive_search(agent)) {
                    std::string_view key(agent.key().ptr(),
                                         agent.key().length());
                    if (key <= value) {
                        ids.push_back(agent.key().id());
                    }
                };
            }
            break;
        }
        default:
            ThrowInfo(
                OpTypeInvalid,
                fmt::format("Invalid OperatorType: {}", static_cast<int>(op)));
    }

    for (const auto str_id : ids) {
        for (size_t j = csr_index_ptr_[str_id]; j < csr_index_ptr_[str_id + 1];
             j++) {
            auto offset = csr_offsets_ptr_[j];
            bitset[offset] = true;
        }
    }
    return bitset;
}

const TargetBitmap
StringIndexMarisa::Range(const std::string& lower_bound_value,
                         bool lb_inclusive,
                         const std::string& upper_bound_value,
                         bool ub_inclusive) {
    tracer::AutoSpan span("StringIndexMarisa::Range", tracer::GetRootSpan());
    auto count = Count();
    TargetBitmap bitset(count);
    if (lower_bound_value.compare(upper_bound_value) > 0 ||
        (lower_bound_value.compare(upper_bound_value) == 0 &&
         !(lb_inclusive && ub_inclusive))) {
        return bitset;
    }

    bool in_lexico_oder = in_lexicographic_order();

    auto common_prefix = GetCommonPrefix(lower_bound_value, upper_bound_value);
    marisa::Agent agent;
    agent.set_query(common_prefix.c_str());
    std::vector<size_t> ids;
    while (trie_.predictive_search(agent)) {
        std::string_view val =
            std::string_view(agent.key().ptr(), agent.key().length());
        if (val > upper_bound_value ||
            (!ub_inclusive && val == upper_bound_value)) {
            // we could only break when trie in lexicographic order.
            if (in_lexico_oder) {
                break;
            } else {
                continue;
            }
        }

        if (val < lower_bound_value ||
            (!lb_inclusive && val == lower_bound_value)) {
            continue;
        }

        if (((lb_inclusive && lower_bound_value <= val) ||
             (!lb_inclusive && lower_bound_value < val)) &&
            ((ub_inclusive && val <= upper_bound_value) ||
             (!ub_inclusive && val < upper_bound_value))) {
            ids.push_back(agent.key().id());
        }
    }
    for (const auto str_id : ids) {
        for (size_t j = csr_index_ptr_[str_id]; j < csr_index_ptr_[str_id + 1];
             j++) {
            auto offset = csr_offsets_ptr_[j];
            bitset[offset] = true;
        }
    }

    return bitset;
}

const TargetBitmap
StringIndexMarisa::PrefixMatch(std::string_view prefix) {
    tracer::AutoSpan span("StringIndexMarisa::PrefixMatch",
                          tracer::GetRootSpan());
    TargetBitmap bitset(str_ids_size_);
    auto matched = prefix_match(prefix);
    for (const auto str_id : matched) {
        for (size_t j = csr_index_ptr_[str_id]; j < csr_index_ptr_[str_id + 1];
             j++) {
            auto offset = csr_offsets_ptr_[j];
            bitset[offset] = true;
        }
    }
    return bitset;
}

const TargetBitmap
StringIndexMarisa::PatternMatch(const std::string& pattern,
                                proto::plan::OpType op) {
    if (op == proto::plan::OpType::PrefixMatch) {
        return PrefixMatch(pattern);
    }

    if (op != proto::plan::OpType::Match &&
        op != proto::plan::OpType::PostfixMatch &&
        op != proto::plan::OpType::InnerMatch &&
        op != proto::plan::OpType::RegexMatch) {
        ThrowInfo(
            Unsupported,
            "StringIndexMarisa::PatternMatch only supports Match, "
            "PrefixMatch, PostfixMatch, InnerMatch, RegexMatch, got op: {}",
            static_cast<int>(op));
    }

    // For Match/PostfixMatch/InnerMatch/RegexMatch, iterate over unique trie keys
    // instead of all rows to avoid redundant matching on duplicate values.
    TargetBitmap bitset(str_ids_size_);

    auto match_fn = [&](const std::string& val) -> bool {
        switch (op) {
            case proto::plan::OpType::PostfixMatch:
                return PostfixMatch(val, pattern);
            case proto::plan::OpType::InnerMatch:
                return InnerMatch(val, pattern);
            default:
                return false;
        }
    };

    if (op == proto::plan::OpType::RegexMatch) {
        PartialRegexMatcher matcher(pattern);
        for (size_t kid = 0; kid < csr_num_keys_; kid++) {
            auto start = csr_index_ptr_[kid];
            auto end = csr_index_ptr_[kid + 1];
            if (start == end)
                continue;
            auto val = Reverse_Lookup(csr_offsets_ptr_[start]);
            if (val.has_value() && matcher(val.value())) {
                for (size_t j = start; j < end; j++) {
                    bitset[csr_offsets_ptr_[j]] = true;
                }
            }
        }
    } else if (op == proto::plan::OpType::Match) {
        LikePatternMatcher matcher(pattern);
        for (size_t kid = 0; kid < csr_num_keys_; kid++) {
            auto start = csr_index_ptr_[kid];
            auto end = csr_index_ptr_[kid + 1];
            if (start == end)
                continue;
            auto val = Reverse_Lookup(csr_offsets_ptr_[start]);
            if (val.has_value() && matcher(val.value())) {
                for (size_t j = start; j < end; j++) {
                    bitset[csr_offsets_ptr_[j]] = true;
                }
            }
        }
    } else {
        for (size_t kid = 0; kid < csr_num_keys_; kid++) {
            auto start = csr_index_ptr_[kid];
            auto end = csr_index_ptr_[kid + 1];
            if (start == end)
                continue;
            auto val = Reverse_Lookup(csr_offsets_ptr_[start]);
            if (val.has_value() && match_fn(val.value())) {
                for (size_t j = start; j < end; j++) {
                    bitset[csr_offsets_ptr_[j]] = true;
                }
            }
        }
    }
    return bitset;
}

void
StringIndexMarisa::fill_str_ids(size_t n,
                                const std::string* values,
                                const bool* valid_data) {
    str_ids_.resize(n, MARISA_NULL_KEY_ID);
    for (size_t i = 0; i < n; i++) {
        if (valid_data != nullptr && !valid_data[i]) {
            continue;
        }
        const auto& str = values[i];
        auto str_id = lookup(str);
        AssertInfo(valid_str_id(str_id), "invalid marisa key");
        str_ids_[i] = str_id;
    }
}

void
StringIndexMarisa::fill_offsets() {
    csr_num_keys_ = trie_.num_keys();

    AssertInfo(str_ids_size_ <= std::numeric_limits<uint32_t>::max(),
               "segment row count {} exceeds uint32_t capacity for CSR",
               str_ids_size_);
    AssertInfo(csr_num_keys_ < std::numeric_limits<uint32_t>::max(),
               "trie key count {} exceeds uint32_t capacity for CSR",
               csr_num_keys_);

    // Pass 1: count occurrences per key_id
    csr_index_.resize(csr_num_keys_ + 1, 0);
    for (size_t offset = 0; offset < str_ids_size_; offset++) {
        auto str_id = str_ids_ptr_[offset];
        if (valid_str_id(str_id)) {
            csr_index_[str_id + 1]++;
        }
    }

    // Prefix sum to get CSR index
    for (size_t i = 1; i <= csr_num_keys_; i++) {
        csr_index_[i] += csr_index_[i - 1];
    }

    // Pass 2: fill offsets
    csr_offsets_.resize(csr_index_[csr_num_keys_]);
    // Use a temporary copy of starts for scatter
    std::vector<uint32_t> write_pos(csr_index_.begin(),
                                    csr_index_.begin() + csr_num_keys_);
    for (size_t offset = 0; offset < str_ids_size_; offset++) {
        auto str_id = str_ids_ptr_[offset];
        if (valid_str_id(str_id)) {
            csr_offsets_[write_pos[str_id]++] = offset;
        }
    }

    csr_index_ptr_ = csr_index_.data();
    csr_offsets_ptr_ = csr_offsets_.data();
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
    tracer::AutoSpan span("StringIndexMarisa::prefix_match",
                          tracer::GetRootSpan());
    std::vector<size_t> ret;
    marisa::Agent agent;
    agent.set_query(prefix.data());
    while (trie_.predictive_search(agent)) {
        ret.push_back(agent.key().id());
    }
    return ret;
}
std::optional<std::string>
StringIndexMarisa::Reverse_Lookup(size_t offset) const {
    tracer::AutoSpan span("StringIndexMarisa::Reverse_Lookup",
                          tracer::GetRootSpan());
    AssertInfo(offset < str_ids_size_, "out of range of total count");
    marisa::Agent agent;
    if (str_ids_ptr_[offset] < 0) {
        return std::nullopt;
    }
    agent.set_query(str_ids_ptr_[offset]);
    trie_.reverse_lookup(agent);
    return std::string(agent.key().ptr(), agent.key().length());
}

bool
StringIndexMarisa::in_lexicographic_order() {
    // by default, marisa trie uses `MARISA_WEIGHT_ORDER` to build trie
    // so `predictive_search` will not iterate in lexicographic order
    // now we build trie using `MARISA_LABEL_ORDER` and also handle old index in weight order.
    if (trie_.node_order() == MARISA_LABEL_ORDER) {
        return true;
    }

    return false;
}

void
StringIndexMarisa::WriteEntries(storage::IndexEntryWriter* writer) {
    // Write trie data via temp file (marisa trie writes to file descriptor)
    auto local_cm =
        storage::LocalChunkManagerSingleton::GetInstance().GetChunkManager();
    std::string tmp_dir =
        local_cm ? local_cm->GetRootPath() : std::string("/tmp");
    std::error_code ec;
    std::filesystem::create_directories(tmp_dir, ec);
    AssertInfo(!ec,
               "failed to create string index temp directory: {}, error: {}",
               tmp_dir,
               ec.message());
    auto uuid = boost::uuids::random_generator()();
    auto uuid_string = boost::uuids::to_string(uuid);
    auto file = tmp_dir + "/" + uuid_string;

    auto fd = open(file.c_str(),
                   O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC,
                   S_IRUSR | S_IWUSR | S_IXUSR);
    AssertInfo(fd != -1, "open file failed: {}", file);
    auto close_fd = folly::makeGuard([fd]() { close(fd); });

    // Immediately unlink the file so it will be deleted when fd is closed,
    // even if an exception occurs or the process crashes
    unlink(file.c_str());

    trie_.write(fd);

    auto size = get_file_size(fd);
    lseek(fd, 0, SEEK_SET);
    writer->WriteEntry(MARISA_TRIE_INDEX, fd, size);

    // Write str_ids
    auto str_ids_len = str_ids_.size() * sizeof(int64_t);
    writer->WriteEntry(MARISA_STR_IDS, str_ids_.data(), str_ids_len);

    // Persist CSR index + offsets
    writer->WriteEntry(MARISA_CSR_INDEX,
                       csr_index_.data(),
                       csr_index_.size() * sizeof(uint32_t));
    writer->WriteEntry(MARISA_CSR_OFFSETS,
                       csr_offsets_.data(),
                       csr_offsets_.size() * sizeof(uint32_t));
    writer->PutMeta(MARISA_CSR_FORMAT_VERSION_META, MARISA_CSR_FORMAT_VERSION);
    writer->PutMeta("csr_num_keys", csr_num_keys_);
}

storage::IndexLoadPlan
StringIndexMarisa::PlanLoad(const storage::IndexEntryCatalog& catalog,
                            const Config& config) {
    auto context = std::make_shared<MarisaLoadContext>();
    context->is_mmap = config.contains(MMAP_FILE_PATH);

    auto local_cm =
        storage::LocalChunkManagerSingleton::GetInstance().GetChunkManager();
    auto tmp_dir = local_cm ? local_cm->GetRootPath() : std::string("/tmp");
    context->file_name =
        tmp_dir + "/" +
        boost::uuids::to_string(boost::uuids::random_generator()());

    auto trie_bytes = catalog.At(MARISA_TRIE_INDEX).plaintext_size;
    context->trie_file =
        std::make_shared<storage::MmapFileTarget>(storage::MmapFileTarget{
            context->file_name, trie_bytes, context->is_mmap, nullptr});

    context->str_ids_bytes = catalog.At(MARISA_STR_IDS).plaintext_size;
    ValidateMarisaEntryElementSize(
        MARISA_STR_IDS, context->str_ids_bytes, sizeof(int64_t));

    auto has_csr_index = catalog.HasEntry(MARISA_CSR_INDEX);
    auto has_csr_offsets = catalog.HasEntry(MARISA_CSR_OFFSETS);
    auto has_csr_num_keys = catalog.HasMeta("csr_num_keys");
    auto has_csr_version = catalog.HasMeta(MARISA_CSR_FORMAT_VERSION_META);
    auto has_any_csr =
        has_csr_index || has_csr_offsets || has_csr_num_keys || has_csr_version;
    if (has_any_csr) {
        AssertInfo(has_csr_index && has_csr_offsets && has_csr_num_keys &&
                       has_csr_version,
                   "incomplete marisa CSR side entries: index {}, offsets {}, "
                   "num_keys {}, version {}",
                   has_csr_index,
                   has_csr_offsets,
                   has_csr_num_keys,
                   has_csr_version);
        auto csr_format_version =
            catalog.GetMeta<uint32_t>(MARISA_CSR_FORMAT_VERSION_META);
        AssertInfo(csr_format_version == MARISA_CSR_FORMAT_VERSION,
                   "unsupported marisa CSR format version: expected {}, got {}",
                   MARISA_CSR_FORMAT_VERSION,
                   csr_format_version);
        context->has_csr = true;
        context->csr_num_keys = catalog.GetMeta<size_t>("csr_num_keys");
        AssertInfo(
            context->csr_num_keys <=
                std::numeric_limits<size_t>::max() / sizeof(uint32_t) - 1,
            "marisa CSR key count {} is too large",
            context->csr_num_keys);
        context->csr_index_bytes = catalog.At(MARISA_CSR_INDEX).plaintext_size;
        context->csr_offsets_bytes =
            catalog.At(MARISA_CSR_OFFSETS).plaintext_size;
        ValidateMarisaEntryElementSize(
            MARISA_CSR_INDEX, context->csr_index_bytes, sizeof(uint32_t));
        ValidateMarisaEntryElementSize(
            MARISA_CSR_OFFSETS, context->csr_offsets_bytes, sizeof(uint32_t));
        auto expected_index_bytes =
            (context->csr_num_keys + 1) * sizeof(uint32_t);
        AssertInfo(context->csr_index_bytes == expected_index_bytes,
                   "invalid marisa CSR index size: expected {}, got {}",
                   expected_index_bytes,
                   context->csr_index_bytes);
        AssertInfo(
            context->csr_offsets_bytes <=
                std::numeric_limits<size_t>::max() - context->csr_index_bytes,
            "marisa CSR file size overflow");
    }

    storage::IndexLoadPlan plan;
    plan.finalize_context = context;
    auto slice_size = storage::DefaultEntryStreamSliceSize();
    plan.entries.push_back(storage::MakePlainEntryLoadPlan(
        catalog,
        MARISA_TRIE_INDEX,
        storage::MmapEntryTarget{context->trie_file, 0, trie_bytes},
        slice_size));

    if (context->is_mmap) {
        context->str_ids_file = std::make_shared<storage::MmapFileTarget>(
            storage::MmapFileTarget{context->file_name + ".str_ids",
                                    context->str_ids_bytes,
                                    true,
                                    nullptr});
        plan.entries.push_back(storage::MakePlainEntryLoadPlan(
            catalog,
            MARISA_STR_IDS,
            storage::MmapEntryTarget{
                context->str_ids_file, 0, context->str_ids_bytes},
            slice_size));
    } else {
        context->str_ids = std::make_shared<std::vector<int64_t>>(
            context->str_ids_bytes / sizeof(int64_t), MARISA_NULL_KEY_ID);
        plan.entries.push_back(storage::MakePlainEntryLoadPlan(
            catalog,
            MARISA_STR_IDS,
            storage::MemoryEntryTarget{
                context->str_ids,
                reinterpret_cast<uint8_t*>(context->str_ids->data()),
                context->str_ids_bytes},
            slice_size));
    }

    if (!context->has_csr) {
        return plan;
    }
    if (context->is_mmap) {
        auto csr_file_size =
            context->csr_index_bytes + context->csr_offsets_bytes;
        context->csr_file =
            std::make_shared<storage::MmapFileTarget>(storage::MmapFileTarget{
                context->file_name + ".csr", csr_file_size, true, nullptr});
        plan.entries.push_back(storage::MakePlainEntryLoadPlan(
            catalog,
            MARISA_CSR_INDEX,
            storage::MmapEntryTarget{
                context->csr_file, 0, context->csr_index_bytes},
            slice_size));
        plan.entries.push_back(storage::MakePlainEntryLoadPlan(
            catalog,
            MARISA_CSR_OFFSETS,
            storage::MmapEntryTarget{context->csr_file,
                                     context->csr_index_bytes,
                                     context->csr_offsets_bytes},
            slice_size));
    } else {
        context->csr_index = std::make_shared<std::vector<uint32_t>>(
            context->csr_index_bytes / sizeof(uint32_t));
        context->csr_offsets = std::make_shared<std::vector<uint32_t>>(
            context->csr_offsets_bytes / sizeof(uint32_t));
        plan.entries.push_back(storage::MakePlainEntryLoadPlan(
            catalog,
            MARISA_CSR_INDEX,
            storage::MemoryEntryTarget{
                context->csr_index,
                reinterpret_cast<uint8_t*>(context->csr_index->data()),
                context->csr_index_bytes},
            slice_size));
        plan.entries.push_back(storage::MakePlainEntryLoadPlan(
            catalog,
            MARISA_CSR_OFFSETS,
            storage::MemoryEntryTarget{
                context->csr_offsets,
                reinterpret_cast<uint8_t*>(context->csr_offsets->data()),
                context->csr_offsets_bytes},
            slice_size));
    }
    return plan;
}

void
StringIndexMarisa::FinalizeLoad(storage::IndexLoadArtifact&& artifact,
                                const Config& config) {
    auto context =
        artifact.FinalizeContext<std::shared_ptr<MarisaLoadContext>>();
    AssertInfo(context != nullptr,
               "StringIndexMarisa FinalizeLoad context is null");

    marisa::Trie new_trie;
    std::unique_ptr<MmapFileRAII> new_trie_file_raii;
    if (context->is_mmap) {
        new_trie_file_raii =
            std::make_unique<MmapFileRAII>(context->trie_file->path);
        new_trie.mmap(context->trie_file->path.c_str());
    } else {
        auto trie_file = File::Open(context->trie_file->path, O_RDONLY);
        new_trie.read(trie_file.Descriptor());
        trie_file.Close();
    }

    char* new_str_ids_mmap_data = nullptr;
    char* new_csr_mmap_data = nullptr;
    auto str_ids_mmap_guard = folly::makeGuard([&]() {
        if (new_str_ids_mmap_data != nullptr &&
            new_str_ids_mmap_data != MAP_FAILED) {
            munmap(new_str_ids_mmap_data, context->str_ids_bytes);
        }
    });
    auto csr_mmap_guard = folly::makeGuard([&]() {
        if (new_csr_mmap_data != nullptr && new_csr_mmap_data != MAP_FAILED) {
            munmap(new_csr_mmap_data,
                   context->csr_index_bytes + context->csr_offsets_bytes);
        }
    });
    std::unique_ptr<MmapFileRAII> new_str_ids_file_raii;
    std::unique_ptr<MmapFileRAII> new_csr_file_raii;
    std::vector<int64_t> new_str_ids;
    std::vector<uint32_t> new_csr_index;
    std::vector<uint32_t> new_csr_offsets;

    if (context->is_mmap) {
        new_str_ids_file_raii =
            std::make_unique<MmapFileRAII>(context->str_ids_file->path);
        auto file = File::Open(context->str_ids_file->path, O_RDONLY);
        new_str_ids_mmap_data = static_cast<char*>(mmap(nullptr,
                                                        context->str_ids_bytes,
                                                        PROT_READ,
                                                        MAP_PRIVATE,
                                                        file.Descriptor(),
                                                        0));
        file.Close();
        AssertInfo(new_str_ids_mmap_data != MAP_FAILED,
                   "failed to mmap marisa str_ids: {}",
                   strerror(errno));
    } else {
        new_str_ids = std::move(*context->str_ids);
    }
    auto new_str_ids_size = context->str_ids_bytes / sizeof(int64_t);
    AssertInfo(new_str_ids_size <= std::numeric_limits<uint32_t>::max(),
               "segment row count {} exceeds uint32_t capacity for CSR",
               new_str_ids_size);

    if (context->has_csr) {
        AssertInfo(context->csr_num_keys == new_trie.num_keys(),
                   "invalid marisa CSR key count: expected {}, got {}",
                   new_trie.num_keys(),
                   context->csr_num_keys);
        if (context->is_mmap) {
            new_csr_file_raii =
                std::make_unique<MmapFileRAII>(context->csr_file->path);
            auto file = File::Open(context->csr_file->path, O_RDONLY);
            auto csr_size =
                context->csr_index_bytes + context->csr_offsets_bytes;
            new_csr_mmap_data = static_cast<char*>(mmap(nullptr,
                                                        csr_size,
                                                        PROT_READ,
                                                        MAP_PRIVATE,
                                                        file.Descriptor(),
                                                        0));
            file.Close();
            AssertInfo(new_csr_mmap_data != MAP_FAILED,
                       "failed to mmap marisa CSR: {}",
                       strerror(errno));
        } else {
            new_csr_index = std::move(*context->csr_index);
            new_csr_offsets = std::move(*context->csr_offsets);
        }
    }

    config_ = config;
    trie_.swap(new_trie);
    mmap_file_raii_ = std::move(new_trie_file_raii);
    str_ids_size_ = new_str_ids_size;
    if (context->is_mmap) {
        AssertInfo(context->str_ids_bytes <=
                       static_cast<size_t>(std::numeric_limits<int64_t>::max()),
                   "marisa str_ids mmap size exceeds int64 range");
        str_ids_mmap_data_ = new_str_ids_mmap_data;
        new_str_ids_mmap_data = nullptr;
        str_ids_mmap_size_ = static_cast<int64_t>(context->str_ids_bytes);
        str_ids_mmap_raii_ = std::move(new_str_ids_file_raii);
        str_ids_ptr_ = reinterpret_cast<const int64_t*>(str_ids_mmap_data_);
    } else {
        str_ids_ = std::move(new_str_ids);
        str_ids_ptr_ = str_ids_.data();
    }

    if (context->has_csr) {
        csr_num_keys_ = context->csr_num_keys;
        if (context->is_mmap) {
            auto csr_size =
                context->csr_index_bytes + context->csr_offsets_bytes;
            AssertInfo(csr_size <= static_cast<size_t>(
                                       std::numeric_limits<int64_t>::max()),
                       "marisa CSR mmap size exceeds int64 range");
            csr_mmap_data_ = new_csr_mmap_data;
            new_csr_mmap_data = nullptr;
            csr_mmap_size_ = static_cast<int64_t>(csr_size);
            csr_mmap_raii_ = std::move(new_csr_file_raii);
            csr_index_ptr_ = reinterpret_cast<const uint32_t*>(csr_mmap_data_);
            csr_offsets_ptr_ = reinterpret_cast<const uint32_t*>(
                csr_mmap_data_ + context->csr_index_bytes);
        } else {
            csr_index_ = std::move(new_csr_index);
            csr_offsets_ = std::move(new_csr_offsets);
            csr_index_ptr_ = csr_index_.data();
            csr_offsets_ptr_ = csr_offsets_.data();
        }
    } else {
        fill_offsets();
    }

    built_ = true;
    total_size_ = CalculateTotalSize();
    ComputeByteSize();
    LOG_INFO("FinalizeLoad StringIndexMarisa done");
}

void
StringIndexMarisa::LoadEntries(storage::IndexEntryReader& reader,
                               const Config& config) {
    auto local_cm =
        storage::LocalChunkManagerSingleton::GetInstance().GetChunkManager();
    std::string tmp_dir =
        local_cm ? local_cm->GetRootPath() : std::string("/tmp");
    std::error_code ec;
    std::filesystem::create_directories(tmp_dir, ec);
    AssertInfo(!ec,
               "failed to create string index temp directory: {}, error: {}",
               tmp_dir,
               ec.message());
    auto uuid = boost::uuids::random_generator()();
    auto uuid_string = boost::uuids::to_string(uuid);
    auto file_name = tmp_dir + "/" + uuid_string;

    auto load_priority =
        GetValueFromConfig<milvus::proto::common::LoadPriority>(
            config, milvus::LOAD_PRIORITY)
            .value_or(milvus::proto::common::LoadPriority::HIGH);

    // Stream trie entry directly to temp file (no full-size temp buffer)
    {
        auto file_writer = storage::FileWriter(
            file_name, storage::io::GetPriorityFromLoadPriority(load_priority));
        reader.ReadEntryStream(MARISA_TRIE_INDEX,
                               [&](const uint8_t* data, size_t len) {
                                   file_writer.Write(data, len);
                               });
        file_writer.Finish();
    }

    if (config.contains(MMAP_FILE_PATH)) {
        auto trie_file_raii = std::make_unique<MmapFileRAII>(file_name);
        trie_.mmap(file_name.c_str());
        mmap_file_raii_ = std::move(trie_file_raii);
    } else {
        auto file = File::Open(file_name, O_RDONLY);
        trie_.read(file.Descriptor());
        mmap_file_raii_ = nullptr;
    }

    if (!config.contains(MMAP_FILE_PATH)) {
        unlink(file_name.c_str());
    }

    // Stream str_ids entry
    auto str_ids_bytes = reader.GetEntrySize(MARISA_STR_IDS);
    ValidateMarisaEntryElementSize(
        MARISA_STR_IDS, str_ids_bytes, sizeof(int64_t));
    if (config.contains(MMAP_FILE_PATH)) {
        // mmap path: stream to disk file, then mmap
        auto str_ids_path = file_name + ".str_ids";
        {
            auto fw = storage::FileWriter(
                str_ids_path,
                storage::io::GetPriorityFromLoadPriority(load_priority));
            size_t written = 0;
            reader.ReadEntryStream(MARISA_STR_IDS,
                                   [&](const uint8_t* d, size_t len) {
                                       fw.Write(d, len);
                                       written += len;
                                   });
            AssertInfo(written == str_ids_bytes,
                       "str_ids stream read size mismatch: got {}, expected {}",
                       written,
                       str_ids_bytes);
            fw.Finish();
        }
        auto str_ids_file_raii = std::make_unique<MmapFileRAII>(str_ids_path);
        auto str_ids_file = File::Open(str_ids_path, O_RDONLY);
        auto* mapped = mmap(NULL,
                            str_ids_bytes,
                            PROT_READ,
                            MAP_PRIVATE,
                            str_ids_file.Descriptor(),
                            0);
        if (mapped == MAP_FAILED) {
            str_ids_file.Close();
            ThrowInfo(ErrorCode::UnexpectedError,
                      "failed to mmap str_ids: {}",
                      strerror(errno));
        }
        str_ids_file.Close();
        str_ids_mmap_data_ = static_cast<char*>(mapped);
        str_ids_mmap_size_ = str_ids_bytes;
        str_ids_mmap_raii_ = std::move(str_ids_file_raii);
        str_ids_ptr_ = reinterpret_cast<const int64_t*>(str_ids_mmap_data_);
        str_ids_size_ = str_ids_bytes / sizeof(int64_t);
    } else {
        // memory path: stream to pre-allocated vector
        str_ids_.resize(str_ids_bytes / sizeof(int64_t), MARISA_NULL_KEY_ID);
        size_t write_offset = 0;
        reader.ReadEntryStream(
            MARISA_STR_IDS, [&](const uint8_t* d, size_t len) {
                milvus::fastmem::FastMemcpy(
                    reinterpret_cast<uint8_t*>(str_ids_.data()) + write_offset,
                    d,
                    len);
                write_offset += len;
            });
        AssertInfo(write_offset == str_ids_bytes,
                   "str_ids stream read size mismatch: got {}, expected {}",
                   write_offset,
                   str_ids_bytes);
        str_ids_ptr_ = str_ids_.data();
        str_ids_size_ = str_ids_.size();
    }

    // Load persisted CSR or rebuild from str_ids
    auto has_csr_index = reader.HasEntry(MARISA_CSR_INDEX);
    auto has_csr_offsets = reader.HasEntry(MARISA_CSR_OFFSETS);
    auto has_csr_num_keys = reader.HasMeta("csr_num_keys");
    auto has_csr_version = reader.HasMeta(MARISA_CSR_FORMAT_VERSION_META);
    auto has_any_csr =
        has_csr_index || has_csr_offsets || has_csr_num_keys || has_csr_version;

    if (has_any_csr) {
        AssertInfo(has_csr_index && has_csr_offsets && has_csr_num_keys &&
                       has_csr_version,
                   "incomplete marisa CSR side entries: index {}, offsets {}, "
                   "num_keys {}, version {}",
                   has_csr_index,
                   has_csr_offsets,
                   has_csr_num_keys,
                   has_csr_version);
        auto csr_format_version =
            reader.GetMeta<uint32_t>(MARISA_CSR_FORMAT_VERSION_META);
        AssertInfo(csr_format_version == MARISA_CSR_FORMAT_VERSION,
                   "unsupported marisa CSR format version: expected {}, got {}",
                   MARISA_CSR_FORMAT_VERSION,
                   csr_format_version);

        csr_num_keys_ = reader.GetMeta<size_t>("csr_num_keys");
        AssertInfo(csr_num_keys_ == trie_.num_keys(),
                   "invalid marisa CSR key count: expected {}, got {}",
                   trie_.num_keys(),
                   csr_num_keys_);
        AssertInfo(
            csr_num_keys_ <=
                std::numeric_limits<size_t>::max() / sizeof(uint32_t) - 1,
            "marisa CSR key count {} is too large",
            csr_num_keys_);

        // csr_offsets_ is indexed with values up to str_ids_size_; uint32_t
        // slots require the segment to fit below 2^32 rows. Guard the load
        // path too so files written by a future build with a larger cap can
        // be rejected loudly instead of silently truncated.
        AssertInfo(str_ids_size_ <= std::numeric_limits<uint32_t>::max(),
                   "segment row count {} exceeds uint32_t capacity for CSR",
                   str_ids_size_);

        size_t idx_bytes = reader.GetEntrySize(MARISA_CSR_INDEX);
        size_t off_bytes = reader.GetEntrySize(MARISA_CSR_OFFSETS);
        ValidateMarisaEntryElementSize(
            MARISA_CSR_INDEX, idx_bytes, sizeof(uint32_t));
        ValidateMarisaEntryElementSize(
            MARISA_CSR_OFFSETS, off_bytes, sizeof(uint32_t));
        AssertInfo(idx_bytes == (csr_num_keys_ + 1) * sizeof(uint32_t),
                   "invalid marisa CSR index size: expected {}, got {}",
                   (csr_num_keys_ + 1) * sizeof(uint32_t),
                   idx_bytes);
        auto csr_offsets_count = off_bytes / sizeof(uint32_t);

        if (config.contains(MMAP_FILE_PATH)) {
            // mmap path: stream csr_index + csr_offsets to disk, then mmap
            auto csr_path = file_name + ".csr";

            {
                auto fw = storage::FileWriter(
                    csr_path,
                    storage::io::GetPriorityFromLoadPriority(load_priority));
                size_t written = 0;
                reader.ReadEntryStream(MARISA_CSR_INDEX,
                                       [&](const uint8_t* d, size_t len) {
                                           fw.Write(d, len);
                                           written += len;
                                       });
                reader.ReadEntryStream(MARISA_CSR_OFFSETS,
                                       [&](const uint8_t* d, size_t len) {
                                           fw.Write(d, len);
                                           written += len;
                                       });
                AssertInfo(written == idx_bytes + off_bytes,
                           "CSR stream read size mismatch: got {}, expected {}",
                           written,
                           idx_bytes + off_bytes);
                fw.Finish();
            }

            auto csr_file_raii = std::make_unique<MmapFileRAII>(csr_path);
            csr_mmap_size_ = idx_bytes + off_bytes;
            auto csr_file = File::Open(csr_path, O_RDONLY);
            auto* mapped = mmap(NULL,
                                csr_mmap_size_,
                                PROT_READ,
                                MAP_PRIVATE,
                                csr_file.Descriptor(),
                                0);
            if (mapped == MAP_FAILED) {
                csr_file.Close();
                ThrowInfo(ErrorCode::UnexpectedError,
                          "failed to mmap CSR: {}",
                          strerror(errno));
            }
            csr_file.Close();
            csr_mmap_data_ = static_cast<char*>(mapped);
            csr_mmap_raii_ = std::move(csr_file_raii);

            csr_index_ptr_ = reinterpret_cast<const uint32_t*>(csr_mmap_data_);
            csr_offsets_ptr_ =
                reinterpret_cast<const uint32_t*>(csr_mmap_data_ + idx_bytes);
        } else {
            // memory path: stream into vectors
            csr_index_.resize(idx_bytes / sizeof(uint32_t));
            size_t wo = 0;
            reader.ReadEntryStream(
                MARISA_CSR_INDEX, [&](const uint8_t* d, size_t len) {
                    milvus::fastmem::FastMemcpy(
                        reinterpret_cast<uint8_t*>(csr_index_.data()) + wo,
                        d,
                        len);
                    wo += len;
                });
            AssertInfo(wo == idx_bytes,
                       "CSR index stream read size mismatch: got {}, expected "
                       "{}",
                       wo,
                       idx_bytes);

            csr_offsets_.resize(off_bytes / sizeof(uint32_t));
            wo = 0;
            reader.ReadEntryStream(
                MARISA_CSR_OFFSETS, [&](const uint8_t* d, size_t len) {
                    milvus::fastmem::FastMemcpy(
                        reinterpret_cast<uint8_t*>(csr_offsets_.data()) + wo,
                        d,
                        len);
                    wo += len;
                });
            AssertInfo(wo == off_bytes,
                       "CSR offsets stream read size mismatch: got {}, "
                       "expected {}",
                       wo,
                       off_bytes);

            csr_index_ptr_ = csr_index_.data();
            csr_offsets_ptr_ = csr_offsets_.data();
        }
    } else {
        // Backward compat: rebuild CSR from str_ids
        fill_offsets();
    }

    built_ = true;
    total_size_ = CalculateTotalSize();
    ComputeByteSize();

    LOG_INFO("LoadEntries StringIndexMarisa done");
}

}  // namespace milvus::index
