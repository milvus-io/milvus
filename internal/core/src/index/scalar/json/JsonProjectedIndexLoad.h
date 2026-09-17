// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include "common/JsonCastType.h"
#include "common/Types.h"
#include "index/contracts/query/IIndexReaderBase.h"
#include "storage/artifact/FileSource.h"
#include "storage/artifact/LoadOptions.h"

namespace milvus::index {

enum class JsonProjectionCompleteness {
    NotProjected,
    LegacyUnknown,
    Complete,
};

// Values only: this plan never retains FileSource, LoadOptions, or OpContext.
struct JsonProjectedOpenPlan {
    JsonProjectionCompleteness completeness{
        JsonProjectionCompleteness::NotProjected};
    std::string json_path;
    std::optional<JsonCastType> cast_type;
    bool has_non_exist_entry{false};
    std::optional<size_t> declared_non_exist_bytes;
    std::string staging_parent;
};

// Called by load planning before DeriveCaps. Any caller-provided value under
// the private runtime key is discarded and replaced with metadata observed
// from this source. The annotation is runtime-only and is never persisted.
Config
AnnotateJsonProjectionCompleteness(Config params, storage::FileSource& source);

ReaderCaps
DeriveJsonProjectedCaps(std::string_view family,
                        const Config& annotated_params,
                        ReaderCaps inner_caps);

// Must run before the family reads engine payload. It validates normalized
// outer JSON parameters, re-observes source metadata, and checks the cold-plan
// annotation. The returned object owns no load collaborator.
JsonProjectedOpenPlan
PrepareJsonProjectedOpen(std::string_view family,
                         storage::FileSource& source,
                         const storage::LoadOptions& opts);

// Legacy artifacts without completeness metadata return `inner` unchanged.
// Complete artifacts read the native-size_t sidecar directly into its final
// vector and return one JsonPathIndexReader wrapper.
std::unique_ptr<IIndexReaderBase>
FinishJsonProjectedOpen(JsonProjectedOpenPlan plan,
                        storage::FileSource& source,
                        std::unique_ptr<IIndexReaderBase> inner);

}  // namespace milvus::index
