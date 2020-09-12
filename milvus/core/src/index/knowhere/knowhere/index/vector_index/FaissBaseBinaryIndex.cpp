// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#include <faiss/index_io.h>

#include "knowhere/common/Exception.h"
#include "knowhere/index/vector_index/FaissBaseBinaryIndex.h"
#include "knowhere/index/vector_index/helpers/FaissIO.h"

namespace milvus {
namespace knowhere {

BinarySet
FaissBaseBinaryIndex::SerializeImpl(const IndexType& type) {
    try {
        faiss::IndexBinary* index = index_.get();

        MemoryIOWriter writer;
        faiss::write_index_binary(index, &writer);
        std::shared_ptr<uint8_t[]> data(writer.data_);

        BinarySet res_set;
        res_set.Append("BinaryIVF", data, writer.rp);
        return res_set;
    } catch (std::exception& e) {
        KNOWHERE_THROW_MSG(e.what());
    }
}

void
FaissBaseBinaryIndex::LoadImpl(const BinarySet& index_binary, const IndexType& type) {
    auto binary = index_binary.GetByName("BinaryIVF");

    MemoryIOReader reader;
    reader.total = binary->size;
    reader.data_ = binary->data.get();

    faiss::IndexBinary* index = faiss::read_index_binary(&reader);
    index_.reset(index);
}

}  // namespace knowhere
}  // namespace milvus
