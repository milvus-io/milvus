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

#include "common/Slice.h"
#include "common/BitsetView.h"
#include "index/VectorMemNMIndex.h"
#include "log/Log.h"

#include "knowhere/index/VecIndexFactory.h"
#include "knowhere/common/Timer.h"
#include "knowhere/index/vector_index/ConfAdapterMgr.h"
#include "knowhere/index/vector_index/adapter/VectorAdapter.h"

namespace milvus::index {

BinarySet
VectorMemNMIndex::Serialize(const Config& config) {
    knowhere::Config serialize_config = config;
    parse_config(serialize_config);

    auto ret = index_->Serialize(serialize_config);
    auto deleter = [&](uint8_t*) {};  // avoid repeated deconstruction
    auto raw_data = std::shared_ptr<uint8_t[]>(static_cast<uint8_t*>(raw_data_.data()), deleter);
    ret.Append(RAW_DATA, raw_data, raw_data_.size());
    milvus::Disassemble(ret);

    return ret;
}

void
VectorMemNMIndex::BuildWithDataset(const DatasetPtr& dataset, const Config& config) {
    VectorMemIndex::BuildWithDataset(dataset, config);
    knowhere::TimeRecorder rc("store_raw_data", 1);
    store_raw_data(dataset);
    rc.ElapseFromBegin("Done");
}

void
VectorMemNMIndex::Load(const BinarySet& binary_set, const Config& config) {
    VectorMemIndex::Load(binary_set, config);
    if (binary_set.Contains(RAW_DATA)) {
        std::call_once(raw_data_loaded_, [&]() { LOG_SEGCORE_INFO_C << "NM index load raw data done!"; });
    }
}

std::unique_ptr<SearchResult>
VectorMemNMIndex::Query(const DatasetPtr dataset, const SearchInfo& search_info, const BitsetView& bitset) {
    auto load_raw_data_closure = [&]() { LoadRawData(); };  // hide this pointer
    // load -> query, raw data has been loaded
    // build -> query, this case just for test, should load raw data before query
    std::call_once(raw_data_loaded_, load_raw_data_closure);

    return VectorMemIndex::Query(dataset, search_info, bitset);
}

void
VectorMemNMIndex::store_raw_data(const knowhere::DatasetPtr& dataset) {
    auto index_type = GetIndexType();
    auto tensor = knowhere::GetDatasetTensor(dataset);
    auto row_num = knowhere::GetDatasetRows(dataset);
    auto dim = knowhere::GetDatasetDim(dataset);
    int64_t data_size;
    if (is_in_bin_list(index_type)) {
        data_size = dim / 8 * row_num;
    } else {
        data_size = dim * row_num * sizeof(float);
    }
    raw_data_.resize(data_size);
    memcpy(raw_data_.data(), tensor, data_size);
}

void
VectorMemNMIndex::LoadRawData() {
    auto bs = index_->Serialize(Config{});
    auto bptr = std::make_shared<knowhere::Binary>();
    auto deleter = [&](uint8_t*) {};  // avoid repeated deconstruction
    bptr->data = std::shared_ptr<uint8_t[]>(static_cast<uint8_t*>(raw_data_.data()), deleter);
    bptr->size = raw_data_.size();
    bs.Append(RAW_DATA, bptr);
    index_->Load(bs);
}

}  // namespace milvus::index
