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

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestLatencyDistribution(t *testing.T) {
	var distribution LatencyDistribution
	for _, duration := range []time.Duration{time.Millisecond, 2 * time.Millisecond, 200 * time.Millisecond, time.Second} {
		distribution.Observe(duration)
	}

	assert.Equal(t, uint64(4), distribution.Count)
	assert.Equal(t, time.Millisecond, time.Duration(distribution.MinNanos))
	assert.Equal(t, time.Second, time.Duration(distribution.MaxNanos))
	average, ok := distribution.Average()
	require.True(t, ok)
	assert.Equal(t, 300_750*time.Microsecond, average)
	p50, ok := distribution.Quantile(0.5)
	require.True(t, ok)
	assert.Equal(t, 2*time.Millisecond, p50)
	p99, ok := distribution.Quantile(0.99)
	require.True(t, ok)
	assert.Equal(t, time.Second, p99)
	_, ok = (LatencyDistribution{}).Quantile(0.5)
	assert.False(t, ok)
}

func TestDistributionMergeAssociativeAndCommutative(t *testing.T) {
	makeDistribution := func(values ...time.Duration) LatencyDistribution {
		var result LatencyDistribution
		for _, value := range values {
			result.Observe(value)
		}
		return result
	}
	a := makeDistribution(time.Millisecond, 5*time.Millisecond)
	b := makeDistribution(10*time.Millisecond, time.Second)
	c := makeDistribution(250 * time.Millisecond)

	left := a
	left.Merge(b)
	left.Merge(c)
	right := b
	right.Merge(c)
	right2 := a
	right2.Merge(right)
	assert.Equal(t, left, right2)

	commuted := b
	commuted.Merge(a)
	ab := a
	ab.Merge(b)
	assert.Equal(t, ab, commuted)
}

func TestRecorderConcurrentAndFinishOnce(t *testing.T) {
	attribution := Attribution{
		ScopeType:     ScopeTypeRequest,
		Component:     "querynode",
		WorkloadClass: WorkloadClassRequestPath,
		WorkloadKind:  WorkloadKindSearch,
		StorageRole:   StorageRolePersistent,
		BackendKind:   BackendKindS3Compatible,
	}
	recorder := NewRecorder(attribution)
	ctx := WithRecorder(WithAttribution(context.Background(), attribution), recorder)

	const operations = 64
	var waitGroup sync.WaitGroup
	for operationIndex := 0; operationIndex < operations; operationIndex++ {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			op := BeginOperation(ctx, OperationMeta{Operation: StorageOperationRead, AccessLayer: AccessLayerMilvus})
			op.AddCompletedBytes(128)
			op.FirstByte()
			op.Finish(OperationResult{SizeKnown: true})
			op.Finish(OperationResult{Err: errors.New("must be ignored")})
		}()
	}
	waitGroup.Wait()

	profile := recorder.Snapshot()
	require.NotNil(t, profile)
	stats := profile.Operations[StorageOperationRead]
	assert.Equal(t, uint64(operations), stats.Count)
	assert.Equal(t, uint64(operations), stats.Success)
	assert.Equal(t, uint64(operations*128), stats.BytesCompleted)
	assert.Equal(t, uint64(operations), stats.Duration.Count)
	assert.Equal(t, uint64(operations), stats.TTFB.Count)
}

func TestRecorderKeepsBoundedPhaseAndRoleBreakdown(t *testing.T) {
	attribution := Attribution{
		ScopeType:       ScopeTypeTask,
		Component:       "datanode",
		WorkloadClass:   WorkloadClassBackground,
		WorkloadKind:    WorkloadKindImport,
		WorkloadSubtype: WorkloadSubtypeIngest,
		Phase:           WorkloadPhaseReadSource,
		StorageRole:     StorageRoleSource,
	}
	recorder := NewRecorder(attribution)
	ctx := WithRecorder(WithAttribution(context.Background(), attribution), recorder)

	sourceRead := BeginOperation(ctx, OperationMeta{Operation: StorageOperationRead})
	sourceRead.AddCompletedBytes(64)
	sourceRead.Finish(OperationResult{SizeKnown: true})

	outputCtx := WithPhase(ctx, WorkloadPhaseWriteOutput, StorageRolePersistent)
	outputWrite := BeginOperation(outputCtx, OperationMeta{Operation: StorageOperationWrite})
	outputWrite.AddCompletedBytes(128)
	outputWrite.Finish(OperationResult{SizeKnown: true})

	profile := recorder.Snapshot()
	require.Equal(t, uint32(2), profile.OperationBreakdownCount)
	assert.Equal(t, WorkloadPhaseReadSource, profile.OperationBreakdown[0].Phase)
	assert.Equal(t, StorageRoleSource, profile.OperationBreakdown[0].StorageRole)
	assert.Equal(t, WorkloadPhaseWriteOutput, profile.OperationBreakdown[1].Phase)
	assert.Equal(t, StorageRolePersistent, profile.OperationBreakdown[1].StorageRole)
	assert.Equal(t, uint64(1), profile.Operations[StorageOperationRead].Count)
	assert.Equal(t, uint64(1), profile.Operations[StorageOperationWrite].Count)
}

func TestErrorClassification(t *testing.T) {
	testCases := []struct {
		err      error
		expected ErrorCategory
	}{
		{nil, ErrorCategoryNone},
		{context.Canceled, ErrorCategoryCanceled},
		{context.DeadlineExceeded, ErrorCategoryTimeout},
		{merr.WrapErrIoKeyNotFound("x"), ErrorCategoryNotFound},
		{merr.WrapErrIoTooManyRequests("x", errors.New("slow down")), ErrorCategoryThrottled},
		{merr.WrapErrIoPermissionDenied("x", errors.New("denied")), ErrorCategoryPermissionDenied},
		{merr.WrapErrIoInvalidCredentials("x", errors.New("credential")), ErrorCategoryInvalidCredentials},
		{merr.WrapErrIoBucketNotFound("x", errors.New("bucket")), ErrorCategoryBucketNotFound},
		{merr.WrapErrIoInvalidArgument("x", errors.New("argument")), ErrorCategoryInvalidArgument},
		{merr.WrapErrIoInvalidRange("x", errors.New("range")), ErrorCategoryInvalidRange},
		{merr.WrapErrIoEntityTooLarge("x", errors.New("large")), ErrorCategoryEntityTooLarge},
		{merr.WrapErrIoUnexpectEOF("x", errors.New("eof")), ErrorCategoryUnexpectedEOF},
		{merr.WrapErrIoFailed("x", errors.New("io")), ErrorCategoryIOFailed},
		{errors.New("opaque"), ErrorCategoryUnknown},
	}
	for _, testCase := range testCases {
		assert.Equal(t, testCase.expected, ClassifyError(testCase.err))
	}
}

func TestOperationUsesContextOutcomeWhenStorageMappingMasksCause(t *testing.T) {
	attribution := Attribution{Component: "querynode", WorkloadKind: WorkloadKindQuery}
	recorder := NewRecorder(attribution)
	ctx, cancel := context.WithCancel(context.Background())
	ctx = WithRecorder(WithAttribution(ctx, attribution), recorder)
	operation := BeginOperation(ctx, OperationMeta{Operation: StorageOperationRead})
	cancel()
	operation.Finish(OperationResult{Err: merr.WrapErrIoFailed("key", errors.New("mapped cancellation"))})

	profile := recorder.Snapshot()
	stats := profile.Operations[StorageOperationRead]
	assert.Equal(t, uint64(1), stats.Canceled)
	assert.Equal(t, uint64(1), stats.Errors[ErrorCategoryCanceled])
}

func TestObserveCppReadProfileFoldsBoundedHandoff(t *testing.T) {
	attribution := Attribution{
		Component:     "querynode",
		WorkloadClass: WorkloadClassRequestPath,
		WorkloadKind:  WorkloadKindSearch,
		StorageRole:   StorageRolePersistent,
	}
	recorder := NewRecorder(attribution)
	ctx := WithRecorder(WithAttribution(context.Background(), attribution), recorder)

	ObserveCppReadProfile(ctx, []uint64{1_000, 2_000}, 128, 3)

	profile := recorder.Snapshot()
	stats := profile.Operations[StorageOperationRead]
	assert.Equal(t, uint64(2), stats.Count)
	assert.Equal(t, uint64(2), stats.Success)
	assert.Equal(t, uint64(128), stats.BytesCompleted)
	assert.Equal(t, CoveragePartial, profile.Coverage.CppStorageOperations)
	assert.Equal(t, CoveragePartial, profile.Coverage.StorageBytes)
	assert.Equal(t, uint64(3), profile.OperationBreakdownDropped)
}

func TestObservabilityCategoryPreservesOriginalErrorContract(t *testing.T) {
	original := merr.WrapErrIoPermissionDenied("key", errors.New("invalid credential"))
	categorized := WithErrorCategory(original, ErrorCategoryInvalidCredentials)

	assert.ErrorIs(t, categorized, merr.ErrIoPermissionDenied)
	assert.False(t, errors.Is(categorized, merr.ErrIoInvalidCredentials))
	assert.Equal(t, original.Error(), categorized.Error())
	assert.Equal(t, ErrorCategoryInvalidCredentials, ClassifyError(categorized))
}

func TestMergeContributionsDeduplicatesAndMarksUnknownBuckets(t *testing.T) {
	first := NewProfile(Attribution{Component: "querynode"}, time.Now())
	first.Operations[StorageOperationRead].Count = 1
	first.Operations[StorageOperationRead].Duration.Observe(time.Millisecond)
	second := NewProfile(Attribution{Component: "querynode"}, time.Now())
	second.Operations[StorageOperationRead].Count = 1
	second.Operations[StorageOperationRead].Duration.Observe(2 * time.Millisecond)
	second.BucketSchema = 999

	identity := ContributionIdentity{ClusterID: "c", NodeID: 1, ScopeID: "s", ExecutionID: "e"}
	merged := MergeContributions([]Contribution{
		{Identity: identity, Profile: &first},
		{Identity: identity, Profile: &first},
		{Identity: ContributionIdentity{ClusterID: "c", NodeID: 2, ScopeID: "s", ExecutionID: "e"}, Profile: &second},
	})
	require.NotNil(t, merged)
	assert.Equal(t, uint64(2), merged.Operations[StorageOperationRead].Count)
	assert.False(t, merged.QuantilesComplete)
}

func TestContributionEncodingIsSparseAndRoundTrips(t *testing.T) {
	profile := NewProfile(Attribution{Component: "querynode"}, time.Now())
	profile.Operations[StorageOperationRead].Count = 1
	profile.Operations[StorageOperationRead].Success = 1
	profile.Operations[StorageOperationRead].BytesCompleted = 1024
	profile.Operations[StorageOperationRead].Duration.Observe(time.Millisecond)
	breakdown := profile.findOrCreateBreakdown(StorageOperationRead, WorkloadPhaseReadSource, StorageRolePersistent)
	require.NotNil(t, breakdown)
	*breakdown = profile.Operations[StorageOperationRead]

	encoded, err := MarshalContribution(Contribution{
		Identity: ContributionIdentity{ClusterID: "cluster", NodeID: 1, ScopeID: "scope", ExecutionID: "execution"},
		Profile:  &profile,
	})
	require.NoError(t, err)
	assert.Less(t, len(encoded), 16<<10)

	decoded, err := UnmarshalContribution(encoded)
	require.NoError(t, err)
	require.NotNil(t, decoded.Profile)
	assert.Equal(t, profile.Operations[StorageOperationRead], decoded.Profile.Operations[StorageOperationRead])
	assert.Equal(t, profile.OperationBreakdownCount, decoded.Profile.OperationBreakdownCount)
}

func TestMergeContributionPayloadsMarksRollingUpgradeGap(t *testing.T) {
	profile := NewProfile(Attribution{Component: "querynode"}, time.Now())
	profile.Coverage.GoStorageOperations = CoverageInstrumented
	profile.Coverage.CppStorageOperations = CoverageUnavailable
	profile.Coverage.StorageBytes = CoverageInstrumented
	payload, err := MarshalContribution(Contribution{
		Identity: ContributionIdentity{ClusterID: "cluster", NodeID: 1, ScopeID: "scope", ExecutionID: "execution"},
		Profile:  &profile,
	})
	require.NoError(t, err)

	mergedPayload, err := MergeContributionPayloads(payload, nil)
	require.NoError(t, err)
	contributions, err := UnmarshalContributions(mergedPayload)
	require.NoError(t, err)
	require.Len(t, contributions, 1)
	merged := contributions[0].Profile
	require.NotNil(t, merged)
	assert.False(t, merged.QuantilesComplete)
	assert.Equal(t, CoveragePartial, merged.Coverage.GoStorageOperations)
	assert.Equal(t, CoverageUnavailable, merged.Coverage.CppStorageOperations)
	assert.Equal(t, CoveragePartial, merged.Coverage.StorageBytes)
}

func TestBoundedLabelsAndCardinality(t *testing.T) {
	attribution := Attribution{
		Component:       "attacker-controlled-component",
		WorkloadClass:   WorkloadClass(999),
		WorkloadKind:    WorkloadKind(999),
		WorkloadSubtype: WorkloadSubtype(999),
		Phase:           WorkloadPhase(999),
		StorageRole:     StorageRole(999),
		BackendKind:     BackendKind(999),
	}.Bounded()
	assert.Equal(t, "unknown", attribution.Component)
	assert.Equal(t, "unknown", attribution.WorkloadClass.String())
	assert.Equal(t, "unknown", attribution.WorkloadKind.String())
	assert.Equal(t, "unknown", attribution.StorageRole.String())
	assert.Equal(t, "unknown", attribution.BackendKind.String())

	const boundedComponents = 12
	operationSeries := boundedComponents * int(WorkloadClassCount) * int(WorkloadKindCount) * int(StorageOperationCount) * int(OutcomeCount) * int(StorageRoleCount) * int(BackendKindCount)
	errorSeries := boundedComponents * int(WorkloadClassCount) * int(WorkloadKindCount) * int(StorageOperationCount) * int(ErrorCategoryCount) * int(StorageRoleCount) * int(BackendKindCount)
	// This is the closed-world worst-case across all component registries. A single
	// process normally emits one component value, reducing the local bound by 12x.
	assert.Equal(t, 691_200, operationSeries)
	// Error-category series are emitted only for non-successful operations and
	// use a separate counter so latency/size histograms do not multiply by this bound.
	assert.Equal(t, 2_419_200, errorSeries)
}

func TestPolicy(t *testing.T) {
	decider := NewProfileDecider(PolicyConfig{
		Enabled:                      true,
		Level:                        StorageProfileSummary,
		RequestAllowExplicit:         true,
		TaskEnabled:                  true,
		TaskTypes:                    []WorkloadKind{WorkloadKindImport},
		MaxProfiledRequestsPerSecond: 1,
	}, nil)

	requestMeta := ScopeMeta{
		Attribution: Attribution{ScopeType: ScopeTypeRequest},
		Requested:   StorageProfileSummary,
		Explicit:    true,
		Authorized:  true,
	}
	assert.Equal(t, StorageProfileSummary, decider.Decide(context.Background(), requestMeta).Effective)
	assert.Equal(t, ProfileReasonRateLimit, decider.Decide(context.Background(), requestMeta).Reason)

	taskDecision := decider.Decide(context.Background(), ScopeMeta{Attribution: Attribution{ScopeType: ScopeTypeTask, WorkloadKind: WorkloadKindImport}})
	assert.Equal(t, StorageProfileSummary, taskDecision.Effective)
	otherTask := decider.Decide(context.Background(), ScopeMeta{Attribution: Attribution{ScopeType: ScopeTypeTask, WorkloadKind: WorkloadKindGC}})
	assert.Equal(t, ProfileReasonTaskNotSelected, otherTask.Reason)
}

func TestScopeBindPropagatesProfileLevel(t *testing.T) {
	attribution := Attribution{
		ScopeType:     ScopeTypeRequest,
		RequestID:     "scope-id",
		Component:     "proxy",
		WorkloadClass: WorkloadClassRequestPath,
		WorkloadKind:  WorkloadKindSearch,
	}
	scope := &Scope{
		ctx: WithProfileLevel(
			WithAttribution(context.Background(), attribution),
			StorageProfileSummary,
		),
		recorder: NoopRecorder(),
	}

	bound := scope.Bind(context.Background())
	assert.Equal(t, StorageProfileSummary, ProfileLevelFromContext(bound))
	assert.Equal(t, "scope-id", AttributionFromContext(bound).RequestID)
}
