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

#include "common/protobuf_utils.h"
#include "common/protobuf_utils_c.h"

// Make a static_assert to ensure that the size and alignment of the C++ and C
static_assert(
    sizeof(milvus::ProtoLayout) == sizeof(ProtoLayout),
    "Size of milvus::ProtoLayout is not equal to size of ProtoLayoutInterface");

// Make a static_assert to ensure that the size and alignment of the C++ and C
static_assert(alignof(milvus::ProtoLayout) == alignof(ProtoLayout),
              "Alignment of milvus::ProtoLayout is not equal to alignment of "
              "ProtoLayoutInterface");

ProtoLayoutInterface
CreateProtoLayout() {
    auto ptr = new milvus::ProtoLayout();
    return reinterpret_cast<ProtoLayoutInterface>(ptr);
}

void
ReleaseProtoLayout(ProtoLayoutInterface proto) {
    delete reinterpret_cast<milvus::ProtoLayout*>(proto);
}

namespace milvus {
ProtoLayout::ProtoLayout() : blob_(nullptr), size_(0) {
}

ProtoLayout::~ProtoLayout() {
    if (blob_ != nullptr) {
        delete[] static_cast<uint8_t*>(blob_);
    }
}
}  // namespace milvus
