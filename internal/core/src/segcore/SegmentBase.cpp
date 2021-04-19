// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include "segcore/SegmentBase.h"
#include "segcore/SegmentNaive.h"
#include "segcore/SegmentSmallIndex.h"

namespace milvus::segcore {

// seems to be deprecated
struct ColumnBasedDataChunk {
    std::vector<std::vector<float>> entity_vecs;

    static ColumnBasedDataChunk
    from(const RowBasedRawData& source, const Schema& schema) {
        ColumnBasedDataChunk dest;
        auto count = source.count;
        auto raw_data = reinterpret_cast<const char*>(source.raw_data);
        auto align = source.sizeof_per_row;
        for (auto& field : schema) {
            auto len = field.get_sizeof();
            Assert(len % sizeof(float) == 0);
            std::vector<float> new_col(len * count / sizeof(float));
            for (int64_t i = 0; i < count; ++i) {
                memcpy(new_col.data() + i * len / sizeof(float), raw_data + i * align, len);
            }
            dest.entity_vecs.push_back(std::move(new_col));
            // offset the raw_data
            raw_data += len / sizeof(float);
        }
        return dest;
    }
};

int
TestABI() {
    return 42;
}

std::unique_ptr<SegmentBase>
CreateSegment(SchemaPtr schema) {
    auto segment = std::make_unique<SegmentSmallIndex>(schema);
    return segment;
}
}  // namespace milvus::segcore
