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

#include "db/SnapshotVisitor.h"
#include <sstream>
#include "db/SnapshotHandlers.h"
#include "db/meta/MetaTypes.h"
#include "db/snapshot/Snapshots.h"

namespace milvus {
namespace engine {

SnapshotVisitor::SnapshotVisitor(snapshot::ScopedSnapshotT ss) : ss_(ss) {
}

SnapshotVisitor::SnapshotVisitor(const std::string& collection_name) {
    status_ = snapshot::Snapshots::GetInstance().GetSnapshot(ss_, collection_name);
}

SnapshotVisitor::SnapshotVisitor(snapshot::ID_TYPE collection_id) {
    status_ = snapshot::Snapshots::GetInstance().GetSnapshot(ss_, collection_id);
}

Status
SnapshotVisitor::SegmentsToSearch(meta::FilesHolder& files_holder) {
    STATUS_CHECK(status_);

    auto handler = std::make_shared<SegmentsToSearchCollector>(ss_, files_holder);
    handler->Iterate();

    return handler->GetStatus();
}

SegmentFileVisitor::Ptr
SegmentFileVisitor::Build(snapshot::ScopedSnapshotT ss, snapshot::ID_TYPE segment_file_id) {
    if (!ss) {
        return nullptr;
    }

    auto file = ss->GetResource<snapshot::SegmentFile>(segment_file_id);
    if (!file) {
        return nullptr;
    }

    auto visitor = std::make_shared<SegmentFileVisitor>();
    visitor->SetFile(file);
    auto field_element = ss->GetResource<snapshot::FieldElement>(file->GetFieldElementId());
    auto field = ss->GetResource<snapshot::Field>(field_element->GetFieldId());
    visitor->SetField(field);
    visitor->SetFieldElement(field_element);
    return visitor;
}

SegmentVisitor::Ptr
SegmentVisitor::Build(snapshot::ScopedSnapshotT ss, snapshot::ID_TYPE segment_id) {
    if (!ss) {
        return nullptr;
    }
    auto segment = ss->GetResource<snapshot::Segment>(segment_id);
    if (!segment) {
        return nullptr;
    }

    auto visitor = std::make_shared<SegmentVisitor>();
    visitor->SetSegment(segment);

    auto& file_ids = ss->GetSegmentFileIds(segment_id);
    for (auto id : file_ids) {
        auto file_visitor = SegmentFileVisitor::Build(ss, id);
        if (!file_visitor) {
            return nullptr;
        }
        visitor->InsertSegmentFile(file_visitor);
    }

    return visitor;
}

}  // namespace engine
}  // namespace milvus
