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

#include <absl/container/btree_map.h>
#include <memory>
#include <string>
#include <vector>
#include <optional>

#include "index/StringIndex.h"
#include "storage/MemFileManagerImpl.h"
#include "common/File.h"

namespace milvus::index {

// Forward declaration
class PostingListManager;

// Posting list metadata stored in BTree
// If posting list is small, it is stored inline in the BTree. Otherwise, it is
// stored externally.
struct PostingListPtr {
    enum StorageType {
        // Small lists stored directly
        INLINE_STORAGE,
        // Large lists stored externally
        EXTERNAL_STORAGE
    };

    StorageType type;
    // Number of elements
    size_t count;

    // For inline storage (small lists)
    std::vector<size_t> inline_data;

    // For external storage (large lists)
    size_t external_id;

    PostingListPtr() : type(INLINE_STORAGE), count(0), external_id(0) {
    }
};

class StringIndexBTree : public StringIndex {
 public:
    explicit StringIndexBTree(
        const storage::FileManagerContext& file_manager_context =
            storage::FileManagerContext(),
        size_t inline_threshold = 20);  // Lists smaller than this are inlined

    ~StringIndexBTree() override;

    int64_t
    Size() override;

    BinarySet
    Serialize(const Config& config) override;

    void
    Load(const BinarySet& set, const Config& config = {}) override;

    void
    Load(milvus::tracer::TraceContext ctx, const Config& config = {}) override;

    int64_t
    Count() override {
        return total_rows_;
    }

    ScalarIndexType
    GetIndexType() const override {
        return ScalarIndexType::BTREE;
    }

    void
    Build(size_t n,
          const std::string* values,
          const bool* valid_data = nullptr) override;

    void
    Build(const Config& config = {}) override;

    void
    BuildWithFieldData(const std::vector<FieldDataPtr>& field_datas) override;

    const TargetBitmap
    In(size_t n, const std::string* values) override;

    const TargetBitmap
    NotIn(size_t n, const std::string* values) override;

    const TargetBitmap
    IsNull() override;

    const TargetBitmap
    IsNotNull() override;

    const TargetBitmap
    Range(std::string value, OpType op) override;

    const TargetBitmap
    Range(std::string lower_bound_value,
          bool lb_inclusive,
          std::string upper_bound_value,
          bool ub_inclusive) override;

    const TargetBitmap
    PrefixMatch(const std::string_view prefix) override;

    std::optional<std::string>
    Reverse_Lookup(size_t offset) const override;

    IndexStatsPtr
    Upload(const Config& config = {}) override;

    const bool
    HasRawData() const override {
        return true;
    }

 private:
    // Helper methods
    void
    AddPosting(const std::string& key, size_t offset);

    std::vector<size_t>
    GetPostingList(const std::string& key) const;

    std::vector<size_t>
    GetPostingList(const PostingListPtr& ptr) const;

    void
    LoadWithoutAssemble(const BinarySet& binary_set,
                        const Config& config) override;

    void
    OptimizePostingLists();

    // Convert inline storage to external when it grows too large
    void
    MaybeExternalize(PostingListPtr& ptr);

    void
    LoadFromMmapData(const uint8_t* btree_data,
                     size_t btree_size,
                     const uint8_t* posting_data,
                     size_t posting_size,
                     const uint8_t* reverse_data,
                     size_t reverse_size);

    void
    LoadFromMemoryData(const uint8_t* btree_data,
                       size_t btree_size,
                       const uint8_t* posting_data,
                       size_t posting_size,
                       const uint8_t* reverse_data,
                       size_t reverse_size);

 private:
    int64_t field_id_{0};

    // Main BTree index: string -> posting list pointer
    absl::btree_map<std::string, PostingListPtr> btree_;

    // External posting list storage
    std::unique_ptr<PostingListManager> posting_manager_;

    // Reverse index for Reverse_Lookup: offset -> string
    // Only populated if needed for reverse lookups
    std::vector<std::string> offset_to_string_;

    // Configuration
    size_t inline_threshold_;  // Max size for inline storage
    size_t total_rows_;
    bool built_;

    // File management
    std::shared_ptr<storage::MemFileManagerImpl> file_manager_;

    // Statistics
    size_t num_inline_lists_;
    size_t num_external_lists_;
    size_t total_posting_size_;

    // Mmap support
    std::unique_ptr<MmapFileRAII> mmap_file_raii_;
    const uint8_t* mmap_data_ = nullptr;
    size_t mmap_size_ = 0;
    bool use_mmap_ = false;

    std::chrono::time_point<std::chrono::system_clock> index_build_begin_;
};

// Manages external posting lists to avoid BTree node bloat
class PostingListManager {
 public:
    PostingListManager();
    ~PostingListManager();

    // Store a posting list and return its ID
    size_t
    Store(std::vector<size_t>&& posting_list);

    // Retrieve a posting list by ID
    const std::vector<size_t>&
    Get(size_t id) const;

    // Get mutable reference for updates
    std::vector<size_t>&
    GetMutable(size_t id);

    // Serialization
    void
    Serialize(uint8_t* buffer, size_t& offset) const;
    void
    Deserialize(const uint8_t* buffer, size_t& offset);

    size_t
    SerializedSize() const;

    // Statistics
    size_t
    NumLists() const {
        return posting_lists_.size();
    }
    size_t
    TotalSize() const;

 private:
    // Simple vector storage, could be optimized with memory pooling
    std::vector<std::vector<size_t>> posting_lists_;
};

using StringIndexBTreePtr = std::unique_ptr<StringIndexBTree>;

inline StringIndexPtr
CreateStringIndexBTree(const storage::FileManagerContext& file_manager_context =
                           storage::FileManagerContext()) {
    return std::make_unique<StringIndexBTree>(file_manager_context);
}

}  // namespace milvus::index