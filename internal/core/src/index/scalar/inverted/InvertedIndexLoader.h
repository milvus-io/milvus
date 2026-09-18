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

#include <string_view>

#include "index/Families.h"
#include "index/contracts/query/IIndexReaderBase.h"
#include "storage/artifact/FileSource.h"
#include "storage/artifact/LoadOptions.h"

// The LOADER of the inverted family.

namespace milvus::index {

class InvertedIndexLoader final {
 public:
    static constexpr std::string_view kFamily = families::kInverted;

    // Inverted's historical wire format stores neither logical value type nor
    // nested mode. Both are normalized runtime parameters, so caps remain
    // available without opening a payload while the persisted bytes stay
    // unchanged.
    static ReaderCaps
    DeriveCaps(const Config& index_meta);

    static IIndexReaderBasePtr
    Open(storage::FileSource& source,
         const storage::LoadOptions& opts);
};

}  // namespace milvus::index
