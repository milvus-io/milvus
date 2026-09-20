// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <utility>

#include "cachinglayer/Translator.h"
#include "common/Types.h"
#include "common/common_type_c.h"
#include "index/contracts/Registry.h"
#include "storage/artifact/LoadOptions.h"

namespace milvus::segcore::storagev1translator {

// Common cache translator contract for readers selected through the index
// family registry. Loading, accounting, and cache metadata remain translator
// specific.
class IndexReaderTranslator
    : public cachinglayer::Translator<index::IIndexReaderBase> {
 public:
    const index::IndexFamily&
    Family() const noexcept;

    DataType
    ValueType() const noexcept;

    const index::ReaderCaps&
    Caps() const noexcept;

 protected:
    void
    SetReaderContract(index::IndexFamily family,
                      DataType value_type,
                      index::ReaderCaps caps) {
        family_ = std::move(family);
        value_type_ = value_type;
        caps_ = std::move(caps);
    }

 private:
    index::IndexFamily family_;
    DataType value_type_{DataType::NONE};
    index::ReaderCaps caps_;
};

storage::WarmupPolicy
ToStorageWarmup(CacheWarmupPolicy policy);

}  // namespace milvus::segcore::storagev1translator
