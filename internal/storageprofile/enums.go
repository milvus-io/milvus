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

package storageprofile

import "strings"

const (
	SchemaVersionV1       uint32 = 1
	LatencyBucketSchemaV1 uint32 = 1
	SizeBucketSchemaV1    uint32 = 1
	LatencyBucketCount           = 20
	SizeBucketCount              = 13
)

type AccessLayer int32

const (
	AccessLayerUnknown  AccessLayer = 0
	AccessLayerMilvus   AccessLayer = 1
	AccessLayerProvider AccessLayer = 2
)

type StorageOperation int32

const (
	StorageOperationUnknown StorageOperation = iota
	StorageOperationRead
	StorageOperationRangeRead
	StorageOperationWrite
	StorageOperationStat
	StorageOperationList
	StorageOperationDelete
	StorageOperationCopy
	StorageOperationMultipartCreate
	StorageOperationMultipartWrite
	StorageOperationMultipartComplete
	StorageOperationMultipartAbort
	StorageOperationCount
)

var storageOperationNames = [...]string{
	"unknown", "read", "range_read", "write", "stat", "list", "delete", "copy",
	"multipart_create", "multipart_write", "multipart_complete", "multipart_abort",
}

func (v StorageOperation) Valid() bool { return v >= 0 && v < StorageOperationCount }
func (v StorageOperation) String() string {
	if !v.Valid() {
		return "unknown"
	}
	return storageOperationNames[v]
}

type OperationOutcome int32

const (
	OutcomeSuccess OperationOutcome = iota
	OutcomeFailure
	OutcomeCanceled
	OutcomeTimeout
	OutcomeCount
)

var outcomeNames = [...]string{"success", "failure", "canceled", "timeout"}

func (v OperationOutcome) Valid() bool { return v >= 0 && v < OutcomeCount }
func (v OperationOutcome) String() string {
	if !v.Valid() {
		return "failure"
	}
	return outcomeNames[v]
}

type ErrorCategory int32

const (
	ErrorCategoryNone ErrorCategory = iota
	ErrorCategoryNotFound
	ErrorCategoryThrottled
	ErrorCategoryPermissionDenied
	ErrorCategoryInvalidCredentials
	ErrorCategoryBucketNotFound
	ErrorCategoryInvalidArgument
	ErrorCategoryInvalidRange
	ErrorCategoryEntityTooLarge
	ErrorCategoryUnexpectedEOF
	ErrorCategoryTimeout
	ErrorCategoryCanceled
	ErrorCategoryIOFailed
	ErrorCategoryUnknown
	ErrorCategoryCount
)

var errorCategoryNames = [...]string{
	"none", "not_found", "throttled", "permission_denied", "invalid_credentials",
	"bucket_not_found", "invalid_argument", "invalid_range", "entity_too_large",
	"unexpected_eof", "timeout", "canceled", "io_failed", "unknown",
}

func (v ErrorCategory) Valid() bool { return v >= 0 && v < ErrorCategoryCount }
func (v ErrorCategory) String() string {
	if !v.Valid() {
		return "unknown"
	}
	return errorCategoryNames[v]
}

type ScopeType int32

const (
	ScopeTypeUnknown ScopeType = iota
	ScopeTypeRequest
	ScopeTypeTask
	ScopeTypeCount
)

var scopeTypeNames = [...]string{"unknown", "request", "task"}

func (v ScopeType) Valid() bool { return v >= 0 && v < ScopeTypeCount }
func (v ScopeType) String() string {
	if !v.Valid() {
		return "unknown"
	}
	return scopeTypeNames[v]
}

type WorkloadClass int32

const (
	WorkloadClassUnknown WorkloadClass = iota
	WorkloadClassRequestPath
	WorkloadClassBackground
	WorkloadClassRecovery
	WorkloadClassSystem
	WorkloadClassCount
)

var workloadClassNames = [...]string{"unknown", "request_path", "background", "recovery", "system"}

func (v WorkloadClass) Valid() bool { return v >= 0 && v < WorkloadClassCount }
func (v WorkloadClass) String() string {
	if !v.Valid() {
		return "unknown"
	}
	return workloadClassNames[v]
}

type WorkloadKind int32

const (
	WorkloadKindUnknown WorkloadKind = iota
	WorkloadKindSearch
	WorkloadKindQuery
	WorkloadKindInsert
	WorkloadKindDelete
	WorkloadKindUpsert
	WorkloadKindFlush
	WorkloadKindImport
	WorkloadKindCompaction
	WorkloadKindIndex
	WorkloadKindLoad
	WorkloadKindRecovery
	WorkloadKindSnapshot
	WorkloadKindGC
	WorkloadKindExternalSync
	WorkloadKindReplication
	WorkloadKindCount
)

var workloadKindNames = [...]string{
	"unknown", "search", "query", "insert", "delete", "upsert", "flush", "import",
	"compaction", "index", "load", "recovery", "snapshot", "gc", "external_sync", "replication",
}

func (v WorkloadKind) Valid() bool { return v >= 0 && v < WorkloadKindCount }
func (v WorkloadKind) String() string {
	if !v.Valid() {
		return "unknown"
	}
	return workloadKindNames[v]
}

func ParseWorkloadKind(value string) (WorkloadKind, bool) {
	value = strings.TrimSpace(strings.ToLower(value))
	for valueIndex, name := range workloadKindNames {
		if value == name {
			return WorkloadKind(valueIndex), true
		}
	}
	return WorkloadKindUnknown, false
}

type WorkloadSubtype int32

const (
	WorkloadSubtypeUnknown WorkloadSubtype = iota
	WorkloadSubtypeAuto
	WorkloadSubtypeManual
	WorkloadSubtypeStreaming
	WorkloadSubtypePreImport
	WorkloadSubtypeIngest
	WorkloadSubtypeL0PreImport
	WorkloadSubtypeL0Ingest
	WorkloadSubtypeCopySegment
	WorkloadSubtypeMix
	WorkloadSubtypeLevel0Delete
	WorkloadSubtypeClustering
	WorkloadSubtypeSort
	WorkloadSubtypePartitionKeySort
	WorkloadSubtypeBumpSchemaVersion
	WorkloadSubtypeSingle
	WorkloadSubtypeMinor
	WorkloadSubtypeMajor
	WorkloadSubtypeBuild
	WorkloadSubtypeAnalyze
	WorkloadSubtypeStatsText
	WorkloadSubtypeStatsBM25
	WorkloadSubtypeStatsJSONKey
	WorkloadSubtypeSegmentLoad
	WorkloadSubtypeIndexLoad
	WorkloadSubtypeSyncWarmup
	WorkloadSubtypeAsyncWarmup
	WorkloadSubtypeCacheFill
	WorkloadSubtypeCreate
	WorkloadSubtypeRestore
	WorkloadSubtypePin
	WorkloadSubtypeUnpin
	WorkloadSubtypeCopy
	WorkloadSubtypeCount
)

var workloadSubtypeNames = [...]string{
	"unknown", "auto", "manual", "streaming", "preimport", "ingest", "l0_preimport", "l0_ingest",
	"copy_segment", "mix", "level0_delete", "clustering", "sort", "partition_key_sort",
	"bump_schema_version", "single", "minor", "major", "build", "analyze", "stats_text",
	"stats_bm25", "stats_json_key", "segment_load", "index_load", "sync_warmup", "async_warmup",
	"cache_fill", "create", "restore", "pin", "unpin", "copy",
}

func (v WorkloadSubtype) Valid() bool { return v >= 0 && v < WorkloadSubtypeCount }
func (v WorkloadSubtype) String() string {
	if !v.Valid() {
		return "unknown"
	}
	return workloadSubtypeNames[v]
}

type WorkloadPhase int32

const (
	WorkloadPhaseUnknown WorkloadPhase = iota
	WorkloadPhaseReadSource
	WorkloadPhaseReadMetadata
	WorkloadPhaseWriteOutput
	WorkloadPhaseWriteMetadata
	WorkloadPhaseCopyObject
	WorkloadPhaseWarmup
	WorkloadPhaseCacheLookup
	WorkloadPhaseCacheFill
	WorkloadPhaseCleanup
	WorkloadPhaseCount
)

var workloadPhaseNames = [...]string{
	"unknown", "read_source", "read_metadata", "write_output", "write_metadata",
	"copy_object", "warmup", "cache_lookup", "cache_fill", "cleanup",
}

func (v WorkloadPhase) Valid() bool { return v >= 0 && v < WorkloadPhaseCount }
func (v WorkloadPhase) String() string {
	if !v.Valid() {
		return "unknown"
	}
	return workloadPhaseNames[v]
}

type StorageRole int32

const (
	StorageRoleUnknown StorageRole = iota
	StorageRoleSource
	StorageRolePersistent
	StorageRoleCount
)

var storageRoleNames = [...]string{"unknown", "source", "persistent"}

func (v StorageRole) Valid() bool { return v >= 0 && v < StorageRoleCount }
func (v StorageRole) String() string {
	if !v.Valid() {
		return "unknown"
	}
	return storageRoleNames[v]
}

type BackendKind int32

const (
	BackendKindUnknown BackendKind = iota
	BackendKindS3Compatible
	BackendKindAzure
	BackendKindGCP
	BackendKindLocal
	BackendKindCount
)

var backendKindNames = [...]string{"unknown", "s3_compatible", "azure", "gcp", "local"}

func (v BackendKind) Valid() bool { return v >= 0 && v < BackendKindCount }
func (v BackendKind) String() string {
	if !v.Valid() {
		return "unknown"
	}
	return backendKindNames[v]
}

type CoverageState int32

const (
	CoverageNotApplicable CoverageState = iota
	CoverageInstrumented
	CoveragePartial
	CoverageUnavailable
)

type StorageProfileLevel int32

const (
	StorageProfileDisabled StorageProfileLevel = iota
	StorageProfileSummary
	StorageProfileDetailed
)

var profileLevelNames = [...]string{"disabled", "summary", "detailed"}

func (v StorageProfileLevel) String() string {
	if v < StorageProfileDisabled || v > StorageProfileDetailed {
		return "disabled"
	}
	return profileLevelNames[v]
}

func ParseProfileLevel(value string) StorageProfileLevel {
	switch strings.TrimSpace(strings.ToLower(value)) {
	case "summary":
		return StorageProfileSummary
	case "detailed":
		return StorageProfileDetailed
	default:
		return StorageProfileDisabled
	}
}

type ProfileSource int32

const (
	ProfileSourceDefault ProfileSource = iota
	ProfileSourceAdminRule
	ProfileSourceTaskConfig
	ProfileSourceExplicit
)

var profileSourceNames = [...]string{"default", "admin_rule", "task_config", "explicit"}

func (v ProfileSource) String() string {
	if v < ProfileSourceDefault || int(v) >= len(profileSourceNames) {
		return "default"
	}
	return profileSourceNames[v]
}

type ProfileDecisionReason int32

const (
	ProfileReasonAllowed ProfileDecisionReason = iota
	ProfileReasonGlobalDisabled
	ProfileReasonNotRequested
	ProfileReasonExplicitDisabled
	ProfileReasonTaskDisabled
	ProfileReasonTaskNotSelected
	ProfileReasonPermissionDenied
	ProfileReasonActiveLimit
	ProfileReasonRateLimit
	ProfileReasonInvalidLevel
	ProfileReasonSerialization
)

var profileDecisionReasonNames = [...]string{
	"allowed", "global_disabled", "not_requested", "explicit_disabled", "task_disabled",
	"task_not_selected", "permission_denied", "active_limit", "rate_limit", "invalid_level", "serialization",
}

func (v ProfileDecisionReason) String() string {
	if v < ProfileReasonAllowed || int(v) >= len(profileDecisionReasonNames) {
		return "invalid_level"
	}
	return profileDecisionReasonNames[v]
}

type CacheTier int32

const (
	CacheTierUnknown CacheTier = iota
	CacheTierMemory
	CacheTierDisk
	CacheTierTiered
)

var cacheTierNames = [...]string{"unknown", "memory", "disk", "tiered"}

func (v CacheTier) String() string {
	if v < CacheTierUnknown || int(v) >= len(cacheTierNames) {
		return "unknown"
	}
	return cacheTierNames[v]
}

type CacheResult int32

const (
	CacheResultUnknown CacheResult = iota
	CacheResultHit
	CacheResultMiss
)

var cacheResultNames = [...]string{"unknown", "hit", "miss"}

func (v CacheResult) String() string {
	if v < CacheResultUnknown || int(v) >= len(cacheResultNames) {
		return "unknown"
	}
	return cacheResultNames[v]
}

type CacheAction int32

const (
	CacheActionRequested CacheAction = iota
	CacheActionServed
	CacheActionCold
	CacheActionFilled
	CacheActionEvicted
)

var cacheActionNames = [...]string{"requested", "served", "cold", "filled", "evicted"}

func (v CacheAction) String() string {
	if v < CacheActionRequested || int(v) >= len(cacheActionNames) {
		return "requested"
	}
	return cacheActionNames[v]
}
