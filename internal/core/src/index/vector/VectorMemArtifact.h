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
#include <vector>

#include "index/contracts/build/IReaderConvertible.h"
#include "index/vector/KnowhereEngine.h"
#include "storage/artifact/Artifact.h"
#include "storage/artifact/FileSink.h"

// Memory-shaped knowhere artifact: serialize the engine and the validity
// metadata its knowhere IdMap owns as logical named BinarySet entries through
// FileSink. Slice assembly and
// transport naming belong to the sink/source, not the artifact. This artifact
// does not implement a packed V3 vector format and rejects a V3 sink.

namespace milvus::index {

class VectorMemArtifact final : public storage::Artifact,
                                public IReaderConvertible {
 public:
    explicit VectorMemArtifact(
        KnowhereEngine engine,
        std::vector<size_t> empty_emb_list_offsets = {});

    ~VectorMemArtifact() override = default;

    // Emit logical entries to the sink. The build service orchestrates upload;
    // FileSink::Finish returns publication statistics.
    void
    Serialize(storage::FileSink& sink) const override;

    IIndexReaderBasePtr
    IntoReader() && override;

 private:
    KnowhereEngine engine_;
    // All-null nullable and empty-embedding-list artifacts can contain only an
    // id map and/or offsets, with no knowhere index payload. These states must
    // remain serializable. The offsets live in the engine's immutable shared
    // generation so readers observe the same state without an O(rows) copy.
};

}  // namespace milvus::index
