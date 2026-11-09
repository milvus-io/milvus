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

#include "storage/artifact/FileSink.h"
#include "storage/IndexEntryWriter.h"

// Result of a completed build. Every artifact exposes Serialize with an injected
// FileSink or the V3 IndexEntryWriter; families select supported formats. Large local
// files are streamed by path, not copied into one resident buffer. Loader opens
// persisted bytes independently, without retaining the builder. Upload
// orchestration remains with the caller.
namespace milvus::storage {

class Artifact {
 public:
    virtual ~Artifact() = default;

    // Hand the materialized bytes to the sink. No upload here: the sink decides
    // where bytes go, and upload orchestration is the indexbuilder service's.
    virtual void
    Serialize(FileSink& sink) const = 0;

    // Write the existing packed V3 format; caller owns Finish and publication.
    virtual void
    Serialize(IndexEntryWriter& writer) const {
        ThrowInfo(Unsupported, "artifact has no V3 packed representation");
    }
};

using ArtifactPtr = std::unique_ptr<Artifact>;

}  // namespace milvus::storage
