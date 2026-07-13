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
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type ByteStats struct {
	Requested uint64
	Completed uint64
}

type OperationMeta struct {
	AccessLayer AccessLayer
	Operation   StorageOperation
	Phase       WorkloadPhase
	StorageRole StorageRole
	CppBoundary bool

	StreamingTTFBObservable bool

	BytesRequested      uint64
	RequestedBytesKnown bool
}

type OperationResult struct {
	Err         error
	Outcome     OperationOutcome
	Category    ErrorCategory
	RetryCount  uint64
	RetryReason ErrorCategory
	SizeKnown   bool
	Ignored     bool
}

type CacheEvent struct {
	Tier     CacheTier
	Result   CacheResult
	Action   CacheAction
	Bytes    uint64
	Duration time.Duration
	Outcome  OperationOutcome
	IsWait   bool
	IsLoad   bool
	IsLookup bool
}

type categorizedError struct {
	err      error
	category ErrorCategory
}

func (e categorizedError) Error() string { return e.err.Error() }
func (e categorizedError) Unwrap() error { return e.err }
func (e categorizedError) StorageProfileErrorCategory() ErrorCategory {
	return e.category
}

// WithErrorCategory attaches an observability-only category while preserving
// the original error text, errors.Is behavior, wire code, and retry policy.
func WithErrorCategory(err error, category ErrorCategory) error {
	if err == nil || !category.Valid() || category == ErrorCategoryNone {
		return err
	}
	return categorizedError{err: err, category: category}
}

type Recorder interface {
	BeginOperation(meta OperationMeta) OperationRecorder
	ObserveCache(event CacheEvent)
	MergeProfile(profile *StorageProfile)
	Snapshot() *StorageProfile
}

type OperationRecorder interface {
	AddCompletedBytes(n uint64)
	FirstByte()
	Finish(result OperationResult)
}

type noopRecorder struct{}
type noopOperationRecorder struct{}

var (
	globalNoopRecorder          Recorder          = noopRecorder{}
	globalNoopOperationRecorder OperationRecorder = noopOperationRecorder{}
)

func NoopRecorder() Recorder { return globalNoopRecorder }

func (noopRecorder) BeginOperation(OperationMeta) OperationRecorder {
	return globalNoopOperationRecorder
}
func (noopRecorder) ObserveCache(CacheEvent)           {}
func (noopRecorder) MergeProfile(*StorageProfile)      {}
func (noopRecorder) Snapshot() *StorageProfile         { return nil }
func (noopOperationRecorder) AddCompletedBytes(uint64) {}
func (noopOperationRecorder) FirstByte()               {}
func (noopOperationRecorder) Finish(OperationResult)   {}

type activeRecorder struct {
	mu           sync.Mutex
	profile      StorageProfile
	cacheEnabled bool
	bytesKnown   bool
	bytesUnknown bool
	finished     sync.Once
}

func NewRecorder(attribution Attribution) Recorder {
	return NewRecorderWithCache(attribution, true)
}

func NewRecorderWithCache(attribution Attribution, cacheEnabled bool) Recorder {
	metrics.StorageProfileActiveScopes.WithLabelValues(attribution.ScopeType.String()).Inc()
	return &activeRecorder{profile: NewProfile(attribution, time.Now()), cacheEnabled: cacheEnabled}
}

func (r *activeRecorder) BeginOperation(meta OperationMeta) OperationRecorder {
	if !meta.Operation.Valid() {
		meta.Operation = StorageOperationUnknown
	}
	if meta.AccessLayer == AccessLayerUnknown {
		meta.AccessLayer = AccessLayerMilvus
	}
	return &activeOperationRecorder{
		owner: r,
		meta:  meta,
		start: time.Now(),
	}
}

func (r *activeRecorder) ObserveCache(event CacheEvent) {
	if !r.cacheEnabled {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	observeCacheStats(&r.profile.Cache, event)
	r.profile.Coverage.TieredCacheUsage = CoverageInstrumented
	if event.IsWait {
		r.profile.Coverage.CacheWait = CoverageInstrumented
	}
}

func (r *activeRecorder) MergeProfile(profile *StorageProfile) {
	if profile == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if profile.Coverage.StorageBytes == CoverageInstrumented {
		r.bytesKnown = true
	} else if profile.Coverage.StorageBytes == CoveragePartial {
		r.bytesKnown = true
		r.bytesUnknown = true
	}
	r.profile.Merge(profile)
}

func observeCacheStats(stats *CacheStats, event CacheEvent) {
	if event.IsLookup {
		switch event.Result {
		case CacheResultHit:
			stats.LookupsHit++
		case CacheResultMiss:
			stats.LookupsMiss++
		}
		stats.LookupDuration.Observe(event.Duration)
	}
	switch event.Action {
	case CacheActionRequested:
		stats.RequestedBytes += event.Bytes
	case CacheActionServed:
		stats.ServedBytes += event.Bytes
	case CacheActionCold:
		stats.ColdBytes += event.Bytes
	case CacheActionFilled:
		stats.FilledBytes += event.Bytes
	case CacheActionEvicted:
		stats.EvictedBytes += event.Bytes
	}
	if event.IsWait {
		stats.WaitDuration.Observe(event.Duration)
	}
	if event.IsLoad {
		stats.FillDuration.Observe(event.Duration)
	}
}

func (r *activeRecorder) Snapshot() *StorageProfile {
	start := time.Now()
	r.finished.Do(func() {
		r.mu.Lock()
		r.profile.FinishedAtUnixNano = time.Now().UnixNano()
		r.mu.Unlock()
		metrics.StorageProfileActiveScopes.WithLabelValues(r.profile.Attribution.ScopeType.String()).Dec()
	})
	r.mu.Lock()
	copyProfile := r.profile
	r.mu.Unlock()
	metrics.StorageProfileSnapshotDuration.WithLabelValues(copyProfile.Attribution.Component).Observe(time.Since(start).Seconds())
	return &copyProfile
}

type activeOperationRecorder struct {
	owner *activeRecorder
	meta  OperationMeta
	start time.Time

	completedBytes atomic.Uint64
	firstByteNanos atomic.Int64
	finished       sync.Once
}

func (r *activeOperationRecorder) AddCompletedBytes(n uint64) {
	r.completedBytes.Add(n)
}

func (r *activeOperationRecorder) FirstByte() {
	elapsed := time.Since(r.start).Nanoseconds()
	if elapsed <= 0 {
		elapsed = 1
	}
	r.firstByteNanos.CompareAndSwap(0, elapsed)
}

func (r *activeOperationRecorder) Finish(result OperationResult) {
	r.finished.Do(func() {
		duration := time.Since(r.start)
		outcome, category := normalizeResult(result)
		completedBytes := r.completedBytes.Load()
		var observation OperationStats
		observation.Count = 1
		switch outcome {
		case OutcomeSuccess:
			observation.Success = 1
		case OutcomeCanceled:
			observation.Canceled = 1
		case OutcomeTimeout:
			observation.TimedOut = 1
		default:
			observation.Failed = 1
		}
		observation.Retried = result.RetryCount
		if result.RetryReason == ErrorCategoryThrottled || category == ErrorCategoryThrottled {
			observation.Throttled = 1
		}
		observation.Errors[category] = 1
		if r.meta.RequestedBytesKnown {
			observation.BytesRequested = r.meta.BytesRequested
		}
		observation.BytesCompleted = completedBytes
		observation.Duration.Observe(duration)
		if firstByteNanos := r.firstByteNanos.Load(); firstByteNanos > 0 {
			observation.TTFB.ObserveNanos(uint64(firstByteNanos))
		}
		if result.SizeKnown {
			observation.Size.Observe(completedBytes)
		}

		r.owner.mu.Lock()
		mergeOperationStats(&r.owner.profile.Operations[r.meta.Operation], observation, true)
		breakdown := r.owner.profile.findOrCreateBreakdown(r.meta.Operation, r.meta.Phase, r.meta.StorageRole)
		if breakdown == nil {
			r.owner.profile.OperationBreakdownDropped++
		} else {
			mergeOperationStats(breakdown, observation, true)
		}
		if r.meta.CppBoundary {
			r.owner.profile.Coverage.CppStorageOperations = CoverageInstrumented
		} else {
			r.owner.profile.Coverage.GoStorageOperations = CoverageInstrumented
		}
		if r.meta.StreamingTTFBObservable {
			r.owner.profile.Coverage.StreamingTTFB = CoverageInstrumented
		}
		if operationCarriesBytes(r.meta.Operation) {
			if result.SizeKnown {
				r.owner.bytesKnown = true
			} else {
				r.owner.bytesUnknown = true
			}
			switch {
			case r.owner.bytesKnown && r.owner.bytesUnknown:
				r.owner.profile.Coverage.StorageBytes = CoveragePartial
			case r.owner.bytesKnown:
				r.owner.profile.Coverage.StorageBytes = CoverageInstrumented
			default:
				r.owner.profile.Coverage.StorageBytes = CoverageUnavailable
			}
		}
		r.owner.mu.Unlock()
	})
}

type observedOperation struct {
	inner       OperationRecorder
	attribution Attribution
	meta        OperationMeta
	ctx         context.Context
	start       time.Time

	completedBytes atomic.Uint64
	firstByteOnce  sync.Once
	finishOnce     sync.Once
}

func BeginOperation(ctx context.Context, meta OperationMeta) OperationRecorder {
	if ctx == nil || IsSuppressed(ctx) {
		return globalNoopOperationRecorder
	}
	if !meta.Operation.Valid() {
		meta.Operation = StorageOperationUnknown
	}
	attribution := AttributionFromContext(ctx).Bounded()
	if meta.Phase.Valid() && meta.Phase != WorkloadPhaseUnknown {
		attribution.Phase = meta.Phase
	} else {
		meta.Phase = attribution.Phase
	}
	if meta.StorageRole.Valid() && meta.StorageRole != StorageRoleUnknown {
		attribution.StorageRole = meta.StorageRole
	} else {
		meta.StorageRole = attribution.StorageRole
	}
	metrics.StorageOperationsInflight.WithLabelValues(attribution.Component, attribution.WorkloadKind.String(), meta.Operation.String()).Inc()
	return &observedOperation{
		inner:       RecorderFromContext(ctx).BeginOperation(meta),
		attribution: attribution,
		meta:        meta,
		ctx:         ctx,
		start:       time.Now(),
	}
}

func (r *observedOperation) AddCompletedBytes(n uint64) {
	r.completedBytes.Add(n)
	r.inner.AddCompletedBytes(n)
}

func (r *observedOperation) FirstByte() {
	r.firstByteOnce.Do(func() { r.inner.FirstByte() })
}

func (r *observedOperation) Finish(result OperationResult) {
	r.finishOnce.Do(func() {
		if result.Ignored {
			metrics.StorageOperationsInflight.WithLabelValues(r.attribution.Component, r.attribution.WorkloadKind.String(), r.meta.Operation.String()).Dec()
			return
		}
		if result.Err != nil && r.ctx != nil {
			switch r.ctx.Err() {
			case context.Canceled:
				result.Outcome = OutcomeCanceled
				result.Category = ErrorCategoryCanceled
			case context.DeadlineExceeded:
				result.Outcome = OutcomeTimeout
				result.Category = ErrorCategoryTimeout
			}
		}
		outcome, category := normalizeResult(result)
		duration := time.Since(r.start)
		completedBytes := r.completedBytes.Load()
		labels := []string{
			r.attribution.Component,
			r.attribution.WorkloadClass.String(),
			r.attribution.WorkloadKind.String(),
			r.meta.Operation.String(),
			outcome.String(),
			r.attribution.StorageRole.String(),
			r.attribution.BackendKind.String(),
		}
		metrics.StorageOperations.WithLabelValues(labels...).Inc()
		metrics.StorageOperationDuration.WithLabelValues(labels...).Observe(duration.Seconds())
		if result.SizeKnown {
			metrics.StorageOperationSize.WithLabelValues(labels...).Observe(float64(completedBytes))
		}
		if completedBytes > 0 {
			metrics.StorageBytes.WithLabelValues(
				r.attribution.Component,
				r.attribution.WorkloadClass.String(),
				r.attribution.WorkloadKind.String(),
				operationDirection(r.meta.Operation),
				r.attribution.StorageRole.String(),
				r.attribution.BackendKind.String(),
			).Add(float64(completedBytes))
		}
		if result.RetryCount > 0 {
			retryReason := result.RetryReason
			if retryReason == ErrorCategoryNone {
				retryReason = category
			}
			metrics.StorageRetries.WithLabelValues(
				r.attribution.Component,
				r.attribution.WorkloadClass.String(),
				r.attribution.WorkloadKind.String(),
				r.meta.Operation.String(),
				retryReason.String(),
				r.attribution.StorageRole.String(),
				r.attribution.BackendKind.String(),
			).Add(float64(result.RetryCount))
		}
		if category != ErrorCategoryNone {
			metrics.StorageOperationErrors.WithLabelValues(
				r.attribution.Component,
				r.attribution.WorkloadClass.String(),
				r.attribution.WorkloadKind.String(),
				r.meta.Operation.String(),
				category.String(),
				r.attribution.StorageRole.String(),
				r.attribution.BackendKind.String(),
			).Inc()
		}
		metrics.StorageOperationsInflight.WithLabelValues(r.attribution.Component, r.attribution.WorkloadKind.String(), r.meta.Operation.String()).Dec()
		result.Outcome = outcome
		result.Category = category
		r.inner.Finish(result)
	})
}

func normalizeResult(result OperationResult) (OperationOutcome, ErrorCategory) {
	if result.Err == nil {
		if result.Outcome.Valid() && result.Outcome != OutcomeFailure {
			return result.Outcome, ErrorCategoryNone
		}
		return OutcomeSuccess, ErrorCategoryNone
	}
	category := result.Category
	if category == ErrorCategoryNone || !category.Valid() {
		category = ClassifyError(result.Err)
	}
	switch {
	case result.Outcome == OutcomeCanceled:
		return OutcomeCanceled, ErrorCategoryCanceled
	case result.Outcome == OutcomeTimeout:
		return OutcomeTimeout, ErrorCategoryTimeout
	case errors.Is(result.Err, context.Canceled):
		return OutcomeCanceled, ErrorCategoryCanceled
	case errors.Is(result.Err, context.DeadlineExceeded):
		return OutcomeTimeout, ErrorCategoryTimeout
	default:
		return OutcomeFailure, category
	}
}

func ClassifyError(err error) ErrorCategory {
	var categorized interface {
		StorageProfileErrorCategory() ErrorCategory
	}
	if errors.As(err, &categorized) {
		if category := categorized.StorageProfileErrorCategory(); category.Valid() {
			return category
		}
	}
	switch {
	case err == nil:
		return ErrorCategoryNone
	case errors.Is(err, context.Canceled):
		return ErrorCategoryCanceled
	case errors.Is(err, context.DeadlineExceeded):
		return ErrorCategoryTimeout
	case errors.Is(err, merr.ErrIoKeyNotFound):
		return ErrorCategoryNotFound
	case errors.Is(err, merr.ErrIoTooManyRequests):
		return ErrorCategoryThrottled
	case errors.Is(err, merr.ErrIoPermissionDenied):
		return ErrorCategoryPermissionDenied
	case errors.Is(err, merr.ErrIoInvalidCredentials):
		return ErrorCategoryInvalidCredentials
	case errors.Is(err, merr.ErrIoBucketNotFound):
		return ErrorCategoryBucketNotFound
	case errors.Is(err, merr.ErrIoInvalidArgument):
		return ErrorCategoryInvalidArgument
	case errors.Is(err, merr.ErrIoInvalidRange):
		return ErrorCategoryInvalidRange
	case errors.Is(err, merr.ErrIoEntityTooLarge):
		return ErrorCategoryEntityTooLarge
	case errors.Is(err, merr.ErrIoUnexpectEOF), errors.Is(err, io.ErrUnexpectedEOF):
		return ErrorCategoryUnexpectedEOF
	case errors.Is(err, merr.ErrIoFailed):
		return ErrorCategoryIOFailed
	default:
		return ErrorCategoryUnknown
	}
}

func operationDirection(operation StorageOperation) string {
	switch operation {
	case StorageOperationRead, StorageOperationRangeRead:
		return "read"
	case StorageOperationCopy:
		return "copy"
	default:
		return "write"
	}
}

func operationCarriesBytes(operation StorageOperation) bool {
	switch operation {
	case StorageOperationRead, StorageOperationRangeRead, StorageOperationWrite, StorageOperationCopy,
		StorageOperationMultipartWrite:
		return true
	default:
		return false
	}
}

func ObserveCache(ctx context.Context, event CacheEvent) {
	if ctx == nil {
		return
	}
	attribution := AttributionFromContext(ctx).Bounded()
	outcome := event.Outcome
	if !outcome.Valid() {
		outcome = OutcomeSuccess
	}
	if event.IsLookup {
		metrics.StorageCacheLookups.WithLabelValues(attribution.Component, attribution.WorkloadKind.String(), event.Tier.String(), event.Result.String()).Inc()
	}
	if event.Bytes > 0 {
		metrics.StorageCacheBytes.WithLabelValues(attribution.Component, attribution.WorkloadKind.String(), event.Tier.String(), event.Action.String()).Add(float64(event.Bytes))
	}
	if event.IsWait {
		metrics.StorageCacheWaitDuration.WithLabelValues(attribution.Component, attribution.WorkloadKind.String(), event.Tier.String(), outcome.String()).Observe(event.Duration.Seconds())
	}
	if event.IsLoad {
		metrics.StorageCacheLoadDuration.WithLabelValues(attribution.Component, attribution.WorkloadKind.String(), event.Tier.String(), outcome.String()).Observe(event.Duration.Seconds())
	}
	RecorderFromContext(ctx).ObserveCache(event)
}

func ObserveCppReadProfile(ctx context.Context, durationsNanos []uint64, completedBytes, dropped uint64) {
	if ctx == nil || len(durationsNanos) == 0 {
		return
	}
	attribution := AttributionFromContext(ctx).Bounded()
	profile := NewProfile(attribution, time.Now())
	profile.FinishedAtUnixNano = time.Now().UnixNano()
	profile.Coverage.GoStorageOperations = CoverageNotApplicable
	profile.Coverage.CppStorageOperations = CoverageInstrumented
	profile.Coverage.StorageBytes = CoveragePartial
	if dropped > 0 {
		profile.Coverage.CppStorageOperations = CoveragePartial
		profile.OperationBreakdownDropped = dropped
	}
	stats := &profile.Operations[StorageOperationRead]
	stats.Count = uint64(len(durationsNanos))
	stats.Success = stats.Count
	stats.BytesCompleted = completedBytes
	stats.Errors[ErrorCategoryNone] = stats.Count
	for _, nanos := range durationsNanos {
		stats.Duration.ObserveNanos(max(nanos, 1))
		labels := []string{
			attribution.Component,
			attribution.WorkloadClass.String(),
			attribution.WorkloadKind.String(),
			StorageOperationRead.String(),
			OutcomeSuccess.String(),
			attribution.StorageRole.String(),
			attribution.BackendKind.String(),
		}
		metrics.StorageOperations.WithLabelValues(labels...).Inc()
		metrics.StorageOperationDuration.WithLabelValues(labels...).Observe(float64(max(nanos, 1)) / float64(time.Second))
	}
	if completedBytes > 0 {
		metrics.StorageBytes.WithLabelValues(
			attribution.Component,
			attribution.WorkloadClass.String(),
			attribution.WorkloadKind.String(),
			"read",
			attribution.StorageRole.String(),
			attribution.BackendKind.String(),
		).Add(float64(completedBytes))
	}
	breakdown := profile.findOrCreateBreakdown(StorageOperationRead, attribution.Phase, attribution.StorageRole)
	if breakdown != nil {
		*breakdown = *stats
	}
	RecorderFromContext(ctx).MergeProfile(&profile)
}
