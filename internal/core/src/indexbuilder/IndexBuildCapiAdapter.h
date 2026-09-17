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

#include "indexbuilder/IndexBuildService.h"
#include "pb/cgo_msg.pb.h"
#include "pb/index_cgo_msg.pb.h"

namespace milvus::indexbuilder {

// Selects the production request shape. Scalar, text, and vector requests are
// normalized once here before the unified BuildSession owns their lifecycle.
enum class BuildPurpose {
    ScalarIndex,
    TextIndex,
    VectorIndex,
};

// The protobuf adapter produces only the two native values whose ownership is
// needed by IndexBuildService. Family/value/type normalization is stored once
// in request; there is no parallel AdaptedIndexType copy to drift.
struct PreparedBuild {
    BuildRequest request;
    storage::FileManagerContext file_manager_context;
};

PreparedBuild
AdaptBuildIndexInfo(const proto::indexcgo::BuildIndexInfo& info,
                    BuildPurpose purpose);

// Existing C-ABI projection of published artifact statistics. Text and scalar
// share the same native ArtifactStats; storage-path selection already made the
// returned file names relative to their correct remote namespace.
proto::cgo::IndexStats
AdaptArtifactStats(const storage::ArtifactStats& stats);

}  // namespace milvus::indexbuilder
