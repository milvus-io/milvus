#pragma once

#include "Resources.h"
#include <string>
#include <iostream>
#include <vector>

struct SegmentFileContext {
    std::string field_name;
    std::string field_element_name;
    ID_TYPE segment_id;
    ID_TYPE partition_id;
};

struct OperationContext {
    SegmentPtr new_segment = nullptr;
    SegmentCommitPtr new_segment_commit = nullptr;
    PartitionCommitPtr new_partition_commit = nullptr;
    SchemaCommitPtr new_schema_commit = nullptr;

    SegmentFilePtr stale_segment_file = nullptr;
    std::vector<SegmentPtr> stale_segments;

    FieldPtr prev_field = nullptr;
    FieldElementPtr prev_field_element = nullptr;

    SegmentPtr prev_segment = nullptr;
    SegmentCommitPtr prev_segment_commit = nullptr;
    PartitionPtr prev_partition = nullptr;
    PartitionCommitPtr prev_partition_commit = nullptr;
    CollectionCommitPtr prev_collection_commit = nullptr;

    SegmentFile::VecT new_segment_files;
};
