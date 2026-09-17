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

#include <memory>
#include <optional>

#include "indexbuilder/IndexBuildService.h"

namespace milvus::indexbuilder {

// Native lifecycle holder for one source-backed index build. Source
// materialization and artifact publication remain in IndexBuildService; this
// class preserves the built artifact for publication retries. It intentionally
// has no protobuf dependency.
class BuildSession final {
 public:
    BuildSession(BuildRequest request,
                 storage::FileManagerContext file_manager_context);

    BuildSession(const BuildSession&) = delete;
    BuildSession&
    operator=(const BuildSession&) = delete;

    BuildSession(BuildSession&&) = delete;
    BuildSession&
    operator=(BuildSession&&) = delete;

    ~BuildSession();

    // Reads and materializes the configured source exactly once. Any exception
    // poisons this session; a successfully built artifact remains available
    // for Publish.
    void
    BuildFromSource();

    // Only a source-backed session has publication context and may publish its
    // retained build outcome. An Artifact is serialized through the
    // generation-specific FileSink to the configured remote object store;
    // SkippedEmpty returns empty file stats. Sink writes may upload
    // incrementally, and Finish finalizes the publication. A sink failure does
    // not discard or poison the retained build product, so Publish remains
    // retryable.
    storage::ArtifactStats
    Publish() const;

 private:
    enum class State {
        Ready,
        Artifact,
        SkippedEmpty,
        Failed,
    };

    std::unique_ptr<IndexBuildService> service_;
    std::optional<BuildProduct> product_;
    State state_{State::Ready};
};

}  // namespace milvus::indexbuilder
