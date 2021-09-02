//  Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
//  with the License. You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software distributed under the License
//  is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
//  or implied. See the License for the specific language governing permissions and limitations under the License.
//

#ifdef MILVUS_APU_VERSION

#include "knowhere/index/vector_index/fpga/ApuUtils.h"

void
getDeletedDocs(std::string location, std::vector<milvus::segment::offset_t>& delete_docs) {
    std::string segment_dir;
    milvus::engine::utils::GetParentPath(location, segment_dir);
    auto segment_reader_ptr = std::make_shared<milvus::segment::SegmentReader>(segment_dir);

    milvus::segment::DeletedDocsPtr deleted_docs_ptr;
    auto status = segment_reader_ptr->LoadDeletedDocs(deleted_docs_ptr);
    if (!status.ok()) {
        std::string msg = "Failed to load deleted docs from " + location;
    }
    delete_docs = deleted_docs_ptr->GetDeletedDocs();
}

int
getOffsetByValue(std::shared_ptr<std::vector<milvus::knowhere::IDType>> inp_vec,
                 milvus::engine::IDNumbers value_to_look, std::vector<uint32_t>& out_offsets) {
    int found_counter = 0;
    for (int i = 0; i < value_to_look.size(); ++i) {
        auto itr = std::find(inp_vec->begin(), inp_vec->end(), value_to_look.at(i));
        if (itr != inp_vec->end()) {
            out_offsets.at(found_counter) = std::distance(inp_vec->begin(), itr);
            found_counter++;
        } else {
            out_offsets.pop_back();
        }
    }
    return found_counter;
}

#endif
