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

#include <memory>
#include <string>
#include <vector>

#include "cachinglayer/Translator.h"
#include "common/Types.h"
#include "mmap/ChunkedColumnInterface.h"
#include "segcore/InsertRecord.h"
#include "segcore/TimestampIndex.h"

namespace milvus::segcore::storagev2translator {

class TimestampIndexCell {
 public:
    explicit TimestampIndexCell(TimestampIndex timestamp_index,
                                int64_t num_rows);

    const TimestampIndex&
    timestamp_index() const {
        return timestamp_index_;
    }

    cachinglayer::ResourceUsage
    CellByteSize() const {
        return byte_size_;
    }

 private:
    TimestampIndex timestamp_index_;
    cachinglayer::ResourceUsage byte_size_;
};

class PkIndexCell {
 public:
    PkIndexCell(std::unique_ptr<OffsetMap> pk2offset,
                std::unique_ptr<CompressedInt64PkArray> offset2pk,
                bool is_int64_pk);

    bool
    has_pk2offset() const {
        return pk2offset_ != nullptr;
    }

    bool
    empty_pks() const {
        return pk2offset_ == nullptr || pk2offset_->empty();
    }

    bool
    contain(const PkType& pk) const {
        if (pk2offset_ == nullptr) {
            return false;
        }
        return pk2offset_->contain(pk);
    }

    const OffsetMap&
    pk2offset() const {
        AssertInfo(pk2offset_ != nullptr,
                   "pk2offset not built (sorted-by-pk segment)");
        return *pk2offset_;
    }

    bool
    has_int64_pk_index() const {
        return is_int64_pk_ && offset2pk_ != nullptr;
    }

    void
    bulk_get_int64_pks_by_offsets(const int64_t* offsets,
                                  int64_t count,
                                  int64_t* output) const;

    cachinglayer::ResourceUsage
    CellByteSize() const {
        return byte_size_;
    }

 private:
    std::unique_ptr<OffsetMap> pk2offset_;
    std::unique_ptr<CompressedInt64PkArray> offset2pk_;
    bool is_int64_pk_{false};
    cachinglayer::ResourceUsage byte_size_;
};

class TimestampIndexTranslator
    : public milvus::cachinglayer::Translator<TimestampIndexCell> {
 public:
    TimestampIndexTranslator(int64_t segment_id,
                             std::shared_ptr<ChunkedColumnInterface> column,
                             int64_t num_rows,
                             const std::string& warmup_policy = "");

    ~TimestampIndexTranslator() override = default;

    size_t
    num_cells() const override;

    milvus::cachinglayer::cid_t
    cell_id_of(milvus::cachinglayer::uid_t uid) const override;

    std::pair<milvus::cachinglayer::ResourceUsage,
              milvus::cachinglayer::ResourceUsage>
    estimated_byte_size_of_cell(milvus::cachinglayer::cid_t cid) const override;

    const std::string&
    key() const override;

    std::vector<std::pair<milvus::cachinglayer::cid_t,
                          std::unique_ptr<TimestampIndexCell>>>
    get_cells(milvus::OpContext* ctx,
              const std::vector<milvus::cachinglayer::cid_t>& cids) override;

    Meta*
    meta() override;

    int64_t
    cells_storage_bytes(
        const std::vector<milvus::cachinglayer::cid_t>& cids) const override {
        return 0;
    }

 private:
    int64_t segment_id_;
    std::shared_ptr<ChunkedColumnInterface> column_;
    int64_t num_rows_;
    std::string key_;
    milvus::cachinglayer::Meta meta_;
};

class PkIndexTranslator : public milvus::cachinglayer::Translator<PkIndexCell> {
 public:
    PkIndexTranslator(int64_t segment_id,
                      std::shared_ptr<ChunkedColumnInterface> column,
                      DataType data_type,
                      bool is_sorted_by_pk);

    ~PkIndexTranslator() override = default;

    size_t
    num_cells() const override;

    milvus::cachinglayer::cid_t
    cell_id_of(milvus::cachinglayer::uid_t uid) const override;

    std::pair<milvus::cachinglayer::ResourceUsage,
              milvus::cachinglayer::ResourceUsage>
    estimated_byte_size_of_cell(milvus::cachinglayer::cid_t cid) const override;

    const std::string&
    key() const override;

    std::vector<
        std::pair<milvus::cachinglayer::cid_t, std::unique_ptr<PkIndexCell>>>
    get_cells(milvus::OpContext* ctx,
              const std::vector<milvus::cachinglayer::cid_t>& cids) override;

    Meta*
    meta() override;

    int64_t
    cells_storage_bytes(
        const std::vector<milvus::cachinglayer::cid_t>& cids) const override {
        return 0;
    }

 private:
    int64_t segment_id_;
    std::shared_ptr<ChunkedColumnInterface> column_;
    DataType data_type_;
    bool is_sorted_by_pk_;
    std::string key_;
    milvus::cachinglayer::Meta meta_;
};

}  // namespace milvus::segcore::storagev2translator
