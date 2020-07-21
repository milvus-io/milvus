// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#pragma once

#include "db/Types.h"
#include "db/meta/FilesHolder.h"
#include "db/snapshot/IterateHandler.h"
#include "db/snapshot/Snapshot.h"
#include "segment/Segment.h"
#include "segment/Types.h"
#include "server/context/Context.h"
#include "utils/Log.h"

#include <memory>
#include <string>
#include <vector>

namespace milvus {
namespace engine {

struct LoadVectorFieldElementHandler : public snapshot::IterateHandler<snapshot::FieldElement> {
    using ResourceT = snapshot::FieldElement;
    using BaseT = snapshot::IterateHandler<ResourceT>;
    LoadVectorFieldElementHandler(const server::ContextPtr& context, snapshot::ScopedSnapshotT ss,
                                  const snapshot::FieldPtr& field);

    Status
    Handle(const typename ResourceT::Ptr&) override;

    const server::ContextPtr context_;
    const snapshot::FieldPtr field_;
};

struct LoadVectorFieldHandler : public snapshot::IterateHandler<snapshot::Field> {
    using ResourceT = snapshot::Field;
    using BaseT = snapshot::IterateHandler<ResourceT>;
    LoadVectorFieldHandler(const server::ContextPtr& context, snapshot::ScopedSnapshotT ss);

    Status
    Handle(const typename ResourceT::Ptr&) override;

    const server::ContextPtr context_;
};

struct SegmentsToSearchCollector : public snapshot::IterateHandler<snapshot::SegmentCommit> {
    using ResourceT = snapshot::SegmentCommit;
    using BaseT = snapshot::IterateHandler<ResourceT>;
    SegmentsToSearchCollector(snapshot::ScopedSnapshotT ss, meta::FilesHolder& holder);

    Status
    Handle(const typename ResourceT::Ptr&) override;

    meta::FilesHolder& holder_;
};

///////////////////////////////////////////////////////////////////////////////
struct GetEntityByIdSegmentHandler : public snapshot::IterateHandler<snapshot::Segment> {
    using ResourceT = snapshot::Segment;
    using BaseT = snapshot::IterateHandler<ResourceT>;
    GetEntityByIdSegmentHandler(const server::ContextPtr& context, snapshot::ScopedSnapshotT ss,
                                const std::string& dir_root, const IDNumbers& ids,
                                const std::vector<std::string>& field_names);

    Status
    Handle(const typename ResourceT::Ptr&) override;

    const server::ContextPtr context_;
    const std::string dir_root_;
    const engine::IDNumbers ids_;
    const std::vector<std::string> field_names_;
    engine::DataChunkPtr data_chunk_;
};

///////////////////////////////////////////////////////////////////////////////

}  // namespace engine
}  // namespace milvus
