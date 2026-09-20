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

#include <utility>

#include "common/EasyAssert.h"
#include "index/contracts/query/IIndexReaderBase.h"
#include "storage/artifact/Artifact.h"

namespace milvus::index {

// Optional consuming capability for a completed artifact whose state can
// directly become a query reader without a storage round trip. A capability
// pointer obtained from an artifact is borrowed; conversion must go through
// FromArtifact so the artifact shell stays alive. Once IntoReader is invoked,
// neither conversion nor serialization may be retried.
class IReaderConvertible {
 public:
    virtual ~IReaderConvertible() = default;

    // Takes ownership before checking the optional capability and destroys the
    // artifact shell on success, Unsupported, or conversion failure. Callers
    // that also need persistence must Serialize before entering. Missing
    // capability never triggers an implicit serialize/load or other IO fallback.
    static IIndexReaderBasePtr
    FromArtifact(storage::ArtifactPtr artifact) {
        if (artifact == nullptr) {
            ThrowInfo(UnexpectedError, "cannot consume a null index artifact");
        }

        auto* convertible = dynamic_cast<IReaderConvertible*>(artifact.get());
        if (convertible == nullptr) {
            ThrowInfo(
                Unsupported,
                "artifact does not support conversion to an index reader");
        }

        auto reader = std::move(*convertible).IntoReader();
        if (reader == nullptr) {
            ThrowInfo(UnexpectedError,
                      "artifact conversion returned a null index reader");
        }
        return reader;
    }

    virtual IIndexReaderBasePtr
    IntoReader() && = 0;
};

}  // namespace milvus::index
