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

// Stateless loader for in-memory knowhere families. Materialized loading reads
// logical entries into a BinarySet; mmap loading streams ordered engine entries
// to a local file through FileSource::ReadEntriesToLocalFile.
// Embedding-list sidecars remain separate files, and validity/empty-list
// metadata is decoded separately rather than concatenated into the engine file.
// The source is borrowed during Open only; local backing files must remain
// owned for the resulting reader's lifetime.

namespace milvus::index {

class VectorMemLoader final {
 public:
    static constexpr std::string_view kFamily = families::kVectorMem;

    // The concrete knowhere index type remains runtime load metadata parsed by
    // this provider; it is not a dynamic registry family and is not persisted
    // under a new key.

    // Derive capabilities before payload open. ReaderCaps currently contains scalar
    // query bits, not vector raw-value or refinement availability. A vector reader
    // returns those bits false and exact true; consumers must not treat that as a
    // complete vector capability description. TODO: represent load-time-derivable
    // vector capabilities without opening a cold reader just to inspect them.
    static ReaderCaps
    DeriveCaps(const Config& index_meta);

    static IIndexReaderBasePtr
    Open(storage::FileSource& source,
         const storage::LoadOptions& opts);
};

}  // namespace milvus::index
