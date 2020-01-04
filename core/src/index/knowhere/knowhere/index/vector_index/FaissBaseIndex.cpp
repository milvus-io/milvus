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

#include <faiss/index_io.h>
#include <utility>

#include "knowhere/common/Exception.h"
#include "knowhere/index/vector_index/FaissBaseIndex.h"
#include "knowhere/index/vector_index/IndexIVF.h"
#include "knowhere/index/vector_index/helpers/FaissIO.h"

namespace knowhere {

FaissBaseIndex::FaissBaseIndex(std::shared_ptr<faiss::Index> index) : index_(std::move(index)) {
}

BinarySet
FaissBaseIndex::SerializeImpl() {
    try {
        faiss::Index* index = index_.get();

        // SealImpl();

        MemoryIOWriter writer;
        faiss::write_index(index, &writer);
        auto data = std::make_shared<uint8_t>();
        data.reset(writer.data_);

        BinarySet res_set;
        // TODO(linxj): use virtual func Name() instead of raw string.
        res_set.Append("IVF", data, writer.rp);
        return res_set;
    } catch (std::exception& e) {
        KNOWHERE_THROW_MSG(e.what());
    }
}

void
FaissBaseIndex::LoadImpl(const BinarySet& index_binary) {
    auto binary = index_binary.GetByName("IVF");

    MemoryIOReader reader;
    reader.total = binary->size;
    reader.data_ = binary->data.get();

    faiss::Index* index = faiss::read_index(&reader);

    index_.reset(index);

    SealImpl();
}

void
FaissBaseIndex::SealImpl() {
#ifdef CUSTOMIZATION
    faiss::Index* index = index_.get();
    auto idx = dynamic_cast<faiss::IndexIVF*>(index);
    if (idx != nullptr) {
        // To be deleted
        KNOWHERE_LOG_DEBUG << "Test before to_readonly:"
                           << " IVF READONLY " << std::boolalpha << idx->is_readonly();
        idx->to_readonly();
    }
#endif
}

}  // namespace knowhere
