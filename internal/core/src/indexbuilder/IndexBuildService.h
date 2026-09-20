// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License
// at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <cstddef>
#include <memory>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "common/Types.h"
#include "index/contracts/Registry.h"
#include "storage/FileManager.h"
#include "storage/artifact/Artifact.h"
#include "storage/artifact/ArtifactStats.h"
#include "storage/artifact/FileSink.h"

namespace milvus::indexbuilder {

// Native projection of the field schema needed by build orchestration.
// Protobuf/default-value interpretation remains inside IndexBuildService.
struct BuildFieldSpec {
    // TODO: Replace these flat types with TypeSchema when recursive schema
    // integration reaches build orchestration.
    DataType field_type{DataType::NONE};
    DataType element_type{DataType::NONE};
    bool nullable{false};
    size_t estimated_missing_row_bytes{1};
};

struct V1BinlogBuildSource {
    std::vector<std::string> files;
};

struct StorageV2BuildSource {
    std::vector<std::vector<std::string>> files;
};

struct ManifestBuildSource {
    std::string manifest_path;
};

using BuildSource = std::
    variant<V1BinlogBuildSource, StorageV2BuildSource, ManifestBuildSource>;

struct BuildOutputSpec {
    storage::Generation generation{storage::Generation::V1V2};
    storage::ArtifactStoragePath storage_path{
        storage::ArtifactStoragePath::Index};

    // Required for V3 and unused for V1/V2. Publication owns remote naming.
    std::string packed_file_name;
};

// Native request assembled by the C-ABI adapter in a later migration step.
// The source shape and output transport are explicit; neither is inferred from
// a family or engine version here.
struct BuildRequest {
    index::IndexFamily family;
    index::BuildParams params;

    // Actual value type indexed by the selected builder, independent of the
    // source transport. A JSON column cast to DOUBLE uses DOUBLE; an
    // ARRAY<INT64> field uses INT64.
    DataType value_type{DataType::NONE};
    FieldId field_id;

    // Binlog files, column groups, or a manifest only describe how source
    // field data is transported; they do not determine value_type.
    BuildSource source{V1BinlogBuildSource{}};

    // Final logical row count for the field after any absent leading prefix
    // has been filled.
    int64_t expected_rows{0};

    // Number of rows in the absent leading prefix [0, missing_rows), such as
    // historical rows written before the field was added. This is not a
    // recovery path for unreadable or failed source files. The service fills a
    // supported schema default when present, otherwise null for a nullable
    // field, and rejects a non-nullable field without a default. The V1 adapter
    // obtains missing_rows from lack_binlog_rows; columnar sources derive the
    // missing count independently from expected_rows and the rows visited.
    int64_t missing_rows{0};

    // Borrowed parent for family-owned build staging. The service injects it
    // into normalized builder parameters but never removes it.
    std::string staging_parent;
    BuildOutputSpec output;

    // Authoritative JSON root for this request. Empty denotes the document
    // root; any path aliases already present in params must match it.
    std::string json_path;
};

// Explicit Artifact-or-SkippedEmpty outcome of a build, not an artifact base
// abstraction. SkippedEmpty means the build produced no Artifact.
class BuildProduct {
 public:
    enum class Kind {
        Artifact,
        SkippedEmpty,
    };

    static BuildProduct
    FromArtifact(storage::ArtifactPtr artifact);

    static BuildProduct
    SkippedEmpty();

    BuildProduct(BuildProduct&&) noexcept = default;
    BuildProduct&
    operator=(BuildProduct&&) noexcept = default;

    BuildProduct(const BuildProduct&) = delete;
    BuildProduct&
    operator=(const BuildProduct&) = delete;

    bool
    IsSkippedEmpty() const {
        return kind_ == Kind::SkippedEmpty;
    }

    const storage::Artifact&
    GetArtifact() const;

 private:
    BuildProduct(Kind kind, storage::ArtifactPtr artifact)
        : kind_(kind), artifact_(std::move(artifact)) {
    }

    Kind kind_;
    storage::ArtifactPtr artifact_;
};

// L5 orchestration: materialize one declared source into one stable complete
// input, invoke the selected typed builder once, and publish its artifact.
class IndexBuildService {
 public:
    IndexBuildService(BuildRequest request,
                      const storage::FileManagerContext& file_manager_context);

    BuildProduct
    RunToArtifact();

    // Publish the explicit build outcome. An Artifact is serialized through
    // FileSink (legacy) or IndexEntryWriter (V3) to the configured remote object
    // store; SkippedEmpty returns empty file stats. Writes may upload
    // incrementally; Finish finalizes publication and does not necessarily
    // start the upload.
    storage::ArtifactStats
    Publish(const BuildProduct& product) const;

    const BuildRequest&
    Request() const noexcept;

    const storage::FileManagerContext&
    Context() const noexcept;

 private:
    void
    NormalizeAndValidateRequest();

    void
    ValidateInputSpec(const index::BuilderInputSpec& spec) const;

    BuildRequest req_;
    storage::FileManagerContext file_manager_context_;
    BuildFieldSpec field_spec_;
    bool run_started_{false};
};

}  // namespace milvus::indexbuilder
