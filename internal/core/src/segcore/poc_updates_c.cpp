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

#include "segcore/poc_updates_c.h"
#include "segcore/SealedUpdatesRecord.h"
#include "segcore/ChunkedSegmentSealedImpl.h"
#include "common/CGoCatch.h"
#include <nlohmann/json.hpp>
#include <limits>
using namespace milvus;
using namespace milvus::segcore;
using namespace milvus::segcore::experimental;
namespace {
UpdateValue
ReadValue(const nlohmann::json& value) {
    if (value.is_null())
        return std::monostate{};
    if (value.is_string())
        return value.get<std::string>();
    if (value.is_number_unsigned()) {
        auto n = value.get<uint64_t>();
        AssertInfo(n <= uint64_t(std::numeric_limits<int64_t>::max()),
                   "POC integer exceeds INT64");
        return int64_t(n);
    }
    AssertInfo(value.is_number_integer(),
               "POC values must be INT64, VARCHAR or NULL");
    return value.get<int64_t>();
}
}  // namespace
CStatus
BeginPOCUpdates(CSegmentInterface segment,
                const int64_t* fields,
                int64_t field_count,
                int64_t row_count,
                bool enabled_temp_index,
                void** session,
                uint64_t* base_timestamp) {
    try {
        AssertInfo(session && base_timestamp && fields && field_count > 0,
                   "Invalid POC initialization");
        *session = nullptr;
        auto* sealed = dynamic_cast<ChunkedSegmentSealedImpl*>(
            static_cast<SegmentInterface*>(segment));
        AssertInfo(sealed && sealed->get_row_count() == row_count,
                   "POC requires the declared single sealed segment row count");
        AssertInfo(!sealed->GetExperimentalUpdates(),
                   "POC updates already attached");
        // Go holds the segment pin; this adapter must not delete the segment.
        auto source =
            std::shared_ptr<ChunkedSegmentSealedImpl>(sealed, [](auto*) {});
        auto adapter = SealedUpdatesRecord::Build(
            source, {fields, fields + field_count}, enabled_temp_index);
        *base_timestamp = adapter->LatestInsertionTimestamp();
        *session = adapter.release();
        return SuccessCStatus();
    }
    CGO_CATCH_AND_RETURN_CSTATUS
}
CStatus
ApplyPOCUpdates(void* session,
                const char* json,
                int64_t length,
                int64_t* matched_rows) {
    try {
        AssertInfo(session && json && length > 0 && matched_rows,
                   "Invalid POC update batch");
        auto op = nlohmann::json::parse(json, json + length);
        std::vector<UpdateValue> pks;
        for (const auto& pk : op.at("primary_keys"))
            pks.push_back(ReadValue(pk));
        std::vector<UpdateAssignment> assignments;
        for (const auto& a : op.at("assignments"))
            assignments.push_back(
                {a.at("field_id").get<int64_t>(), ReadValue(a.at("value"))});
        AssertInfo(!pks.empty() && !assignments.empty(), "Empty POC update");
        *matched_rows = static_cast<SealedUpdatesRecord*>(session)->Apply(
            pks, op.at("timestamp").get<uint64_t>(), assignments);
        return SuccessCStatus();
    }
    CGO_CATCH_AND_RETURN_CSTATUS
}
CStatus
CommitPOCUpdates(void* session) {
    try {
        AssertInfo(session, "Missing POC session");
        static_cast<SealedUpdatesRecord*>(session)->EnableExprEvaluation();
        return SuccessCStatus();
    }
    CGO_CATCH_AND_RETURN_CSTATUS
}
void
DestroyPOCUpdates(void* session) {
    delete static_cast<SealedUpdatesRecord*>(session);
}
CStatus
GetPOCReadCounts(CSegmentInterface segment,
                 uint64_t* index_reads,
                 uint64_t* value_reads) {
    try {
        auto records = static_cast<SegmentInternalInterface*>(segment)
                           ->GetExperimentalUpdates();
        AssertInfo(records && index_reads && value_reads,
                   "POC store is not ready");
        auto counts = records->ReadCounts();
        *index_reads = counts.first;
        *value_reads = counts.second;
        return SuccessCStatus();
    }
    CGO_CATCH_AND_RETURN_CSTATUS
}
