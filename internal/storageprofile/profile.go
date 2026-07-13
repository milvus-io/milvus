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

import "time"

type Attribution struct {
	ScopeType ScopeType

	TenantID string
	UserID   string

	RequestID   string
	RequestType string
	TraceID     string

	TaskID      string
	TaskAttempt uint32

	Component    string
	NodeID       int64
	CollectionID int64

	WorkloadClass   WorkloadClass
	WorkloadKind    WorkloadKind
	WorkloadSubtype WorkloadSubtype
	Phase           WorkloadPhase
	StorageRole     StorageRole
	BackendKind     BackendKind
}

func (a Attribution) Bounded() Attribution {
	if !a.ScopeType.Valid() {
		a.ScopeType = ScopeTypeUnknown
	}
	if !a.WorkloadClass.Valid() {
		a.WorkloadClass = WorkloadClassUnknown
	}
	if !a.WorkloadKind.Valid() {
		a.WorkloadKind = WorkloadKindUnknown
	}
	if !a.WorkloadSubtype.Valid() {
		a.WorkloadSubtype = WorkloadSubtypeUnknown
	}
	if !a.Phase.Valid() {
		a.Phase = WorkloadPhaseUnknown
	}
	if !a.StorageRole.Valid() {
		a.StorageRole = StorageRoleUnknown
	}
	if !a.BackendKind.Valid() {
		a.BackendKind = BackendKindUnknown
	}
	a.Component = boundedComponent(a.Component)
	return a
}

func boundedComponent(component string) string {
	switch component {
	case "proxy", "querynode", "datanode", "datacoord", "streamingnode", "rootcoord",
		"querycoord", "mixcoord", "indexnode", "standalone", "embedded":
		return component
	default:
		return "unknown"
	}
}

type ProfileCoverage struct {
	GoStorageOperations  CoverageState
	CppStorageOperations CoverageState
	StorageBytes         CoverageState
	StreamingTTFB        CoverageState
	TieredCacheUsage     CoverageState
	CacheWait            CoverageState
	ProviderAccess       CoverageState
}

type OperationStats struct {
	Count     uint64
	Success   uint64
	Failed    uint64
	Canceled  uint64
	TimedOut  uint64
	Retried   uint64
	Throttled uint64

	BytesRequested uint64
	BytesCompleted uint64

	Duration LatencyDistribution
	TTFB     LatencyDistribution
	Size     SizeDistribution

	Errors [ErrorCategoryCount]uint64
}

const MaxOperationBreakdownEntries = 64

type OperationBreakdownEntry struct {
	Operation   StorageOperation
	Phase       WorkloadPhase
	StorageRole StorageRole
	Stats       OperationStats
}

type CacheStats struct {
	LookupsHit  uint64
	LookupsMiss uint64

	RequestedBytes uint64
	ServedBytes    uint64
	ColdBytes      uint64
	FilledBytes    uint64
	EvictedBytes   uint64

	LookupDuration LatencyDistribution
	FillDuration   LatencyDistribution
	WaitDuration   LatencyDistribution
}

type StorageProfile struct {
	SchemaVersion uint32
	BucketSchema  uint32

	Attribution Attribution

	Operations                [StorageOperationCount]OperationStats
	OperationBreakdown        [MaxOperationBreakdownEntries]OperationBreakdownEntry
	OperationBreakdownCount   uint32
	OperationBreakdownDropped uint64
	Cache                     CacheStats
	Coverage                  ProfileCoverage

	StartedAtUnixNano  int64
	FinishedAtUnixNano int64

	QuantilesComplete bool
}

func NewProfile(attribution Attribution, now time.Time) StorageProfile {
	return StorageProfile{
		SchemaVersion:     SchemaVersionV1,
		BucketSchema:      LatencyBucketSchemaV1,
		Attribution:       attribution.Bounded(),
		StartedAtUnixNano: now.UnixNano(),
		QuantilesComplete: true,
		Coverage: ProfileCoverage{
			GoStorageOperations:  CoverageUnavailable,
			CppStorageOperations: CoverageUnavailable,
			StorageBytes:         CoverageUnavailable,
			StreamingTTFB:        CoverageUnavailable,
			TieredCacheUsage:     CoverageUnavailable,
			CacheWait:            CoverageUnavailable,
			ProviderAccess:       CoverageUnavailable,
		},
	}
}

func (p *StorageProfile) Merge(other *StorageProfile) {
	if other == nil {
		return
	}
	if p.SchemaVersion == 0 {
		*p = *other
		return
	}
	knownBuckets := p.BucketSchema == LatencyBucketSchemaV1 && other.BucketSchema == LatencyBucketSchemaV1
	if p.SchemaVersion != other.SchemaVersion || !knownBuckets {
		p.QuantilesComplete = false
	}
	for operationIndex := StorageOperationUnknown; operationIndex < StorageOperationCount; operationIndex++ {
		mergeOperationStats(&p.Operations[operationIndex], other.Operations[operationIndex], knownBuckets)
	}
	for breakdownIndex := uint32(0); breakdownIndex < other.OperationBreakdownCount; breakdownIndex++ {
		right := other.OperationBreakdown[breakdownIndex]
		left := p.findOrCreateBreakdown(right.Operation, right.Phase, right.StorageRole)
		if left == nil {
			p.OperationBreakdownDropped += right.Stats.Count
			continue
		}
		mergeOperationStats(left, right.Stats, knownBuckets)
	}
	p.OperationBreakdownDropped += other.OperationBreakdownDropped
	mergeCacheStats(&p.Cache, other.Cache, knownBuckets)
	p.Coverage = mergeCoverage(p.Coverage, other.Coverage)
	if p.StartedAtUnixNano == 0 || other.StartedAtUnixNano < p.StartedAtUnixNano {
		p.StartedAtUnixNano = other.StartedAtUnixNano
	}
	if other.FinishedAtUnixNano > p.FinishedAtUnixNano {
		p.FinishedAtUnixNano = other.FinishedAtUnixNano
	}
}

func (p *StorageProfile) findOrCreateBreakdown(operation StorageOperation, phase WorkloadPhase, role StorageRole) *OperationStats {
	for breakdownIndex := uint32(0); breakdownIndex < p.OperationBreakdownCount; breakdownIndex++ {
		entry := &p.OperationBreakdown[breakdownIndex]
		if entry.Operation == operation && entry.Phase == phase && entry.StorageRole == role {
			return &entry.Stats
		}
	}
	if p.OperationBreakdownCount >= MaxOperationBreakdownEntries {
		return nil
	}
	entry := &p.OperationBreakdown[p.OperationBreakdownCount]
	p.OperationBreakdownCount++
	entry.Operation = operation
	entry.Phase = phase
	entry.StorageRole = role
	return &entry.Stats
}

func mergeOperationStats(left *OperationStats, right OperationStats, mergeBuckets bool) {
	left.Count += right.Count
	left.Success += right.Success
	left.Failed += right.Failed
	left.Canceled += right.Canceled
	left.TimedOut += right.TimedOut
	left.Retried += right.Retried
	left.Throttled += right.Throttled
	left.BytesRequested += right.BytesRequested
	left.BytesCompleted += right.BytesCompleted
	for categoryIndex := range left.Errors {
		left.Errors[categoryIndex] += right.Errors[categoryIndex]
	}
	if mergeBuckets {
		left.Duration.Merge(right.Duration)
		left.TTFB.Merge(right.TTFB)
		left.Size.Merge(right.Size)
	} else {
		mergeLatencyScalars(&left.Duration, right.Duration)
		mergeLatencyScalars(&left.TTFB, right.TTFB)
		mergeSizeScalars(&left.Size, right.Size)
	}
}

func mergeLatencyScalars(left *LatencyDistribution, right LatencyDistribution) {
	if right.Count == 0 {
		return
	}
	if left.Count == 0 || right.MinNanos < left.MinNanos {
		left.MinNanos = right.MinNanos
	}
	if right.MaxNanos > left.MaxNanos {
		left.MaxNanos = right.MaxNanos
	}
	left.Count += right.Count
	left.SumNanos += right.SumNanos
}

func mergeSizeScalars(left *SizeDistribution, right SizeDistribution) {
	if right.Count == 0 {
		return
	}
	if left.Count == 0 || right.Min < left.Min {
		left.Min = right.Min
	}
	if right.Max > left.Max {
		left.Max = right.Max
	}
	left.Count += right.Count
	left.Sum += right.Sum
}

func mergeCacheStats(left *CacheStats, right CacheStats, mergeBuckets bool) {
	left.LookupsHit += right.LookupsHit
	left.LookupsMiss += right.LookupsMiss
	left.RequestedBytes += right.RequestedBytes
	left.ServedBytes += right.ServedBytes
	left.ColdBytes += right.ColdBytes
	left.FilledBytes += right.FilledBytes
	left.EvictedBytes += right.EvictedBytes
	if mergeBuckets {
		left.LookupDuration.Merge(right.LookupDuration)
		left.FillDuration.Merge(right.FillDuration)
		left.WaitDuration.Merge(right.WaitDuration)
	} else {
		mergeLatencyScalars(&left.LookupDuration, right.LookupDuration)
		mergeLatencyScalars(&left.FillDuration, right.FillDuration)
		mergeLatencyScalars(&left.WaitDuration, right.WaitDuration)
	}
}

func mergeCoverage(left, right ProfileCoverage) ProfileCoverage {
	return ProfileCoverage{
		GoStorageOperations:  mergeCoverageState(left.GoStorageOperations, right.GoStorageOperations),
		CppStorageOperations: mergeCoverageState(left.CppStorageOperations, right.CppStorageOperations),
		StorageBytes:         mergeCoverageState(left.StorageBytes, right.StorageBytes),
		StreamingTTFB:        mergeCoverageState(left.StreamingTTFB, right.StreamingTTFB),
		TieredCacheUsage:     mergeCoverageState(left.TieredCacheUsage, right.TieredCacheUsage),
		CacheWait:            mergeCoverageState(left.CacheWait, right.CacheWait),
		ProviderAccess:       mergeCoverageState(left.ProviderAccess, right.ProviderAccess),
	}
}

func mergeCoverageState(left, right CoverageState) CoverageState {
	if left == right {
		return left
	}
	if left == CoverageNotApplicable {
		return right
	}
	if right == CoverageNotApplicable {
		return left
	}
	if left == CoverageUnavailable && right == CoverageUnavailable {
		return CoverageUnavailable
	}
	return CoveragePartial
}

type ContributionIdentity struct {
	ClusterID   string
	NodeID      int64
	ScopeID     string
	TaskAttempt uint32
	ExecutionID string
}

type Contribution struct {
	Identity ContributionIdentity
	Profile  *StorageProfile
}

func MergeContributions(contributions []Contribution) *StorageProfile {
	seen := make(map[ContributionIdentity]struct{}, len(contributions))
	var merged *StorageProfile
	for _, contribution := range contributions {
		if contribution.Profile == nil {
			continue
		}
		if _, duplicated := seen[contribution.Identity]; duplicated {
			continue
		}
		seen[contribution.Identity] = struct{}{}
		if merged == nil {
			copyProfile := *contribution.Profile
			merged = &copyProfile
			continue
		}
		merged.Merge(contribution.Profile)
	}
	return merged
}
