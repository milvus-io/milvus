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

#include "indexbuilder/BuildSession.h"

#include <utility>

#include "common/EasyAssert.h"

namespace milvus::indexbuilder {

BuildSession::BuildSession(BuildRequest request,
                           storage::FileManagerContext file_manager_context)
    : service_(std::make_unique<IndexBuildService>(std::move(request),
                                                   file_manager_context)) {
}

BuildSession::~BuildSession() = default;

void
BuildSession::BuildFromSource() {
    try {
        AssertInfo(state_ == State::Ready,
                   "index-build session cannot build more than once");
        AssertInfo(service_ != nullptr,
                   "index-build session has no build service");
        product_.emplace(service_->RunToArtifact());
        state_ =
            product_->IsSkippedEmpty() ? State::SkippedEmpty : State::Artifact;
    } catch (...) {
        product_.reset();
        state_ = State::Failed;
        throw;
    }
}

storage::ArtifactStats
BuildSession::Publish() const {
    AssertInfo(service_ != nullptr,
               "index-build session has no publish service");
    AssertInfo(state_ == State::Artifact || state_ == State::SkippedEmpty,
               "index-build session has no publishable result");
    AssertInfo(product_.has_value(),
               "publishable index-build session has no build product");
    return service_->Publish(*product_);
}

}  // namespace milvus::indexbuilder
