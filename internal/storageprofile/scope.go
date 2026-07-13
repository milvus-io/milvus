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
	"sync"
	"sync/atomic"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"google.golang.org/grpc/metadata"
)

var (
	activeProfileScopes atomic.Int64
	activeTaskScopes    atomic.Int64
	requestRateMu       sync.Mutex
	requestRateWindow   time.Time
	requestRateCount    int64
)

const ExplicitRequestHeader = "x-milvus-storage-profile"

type Scope struct {
	ctx      context.Context
	recorder Recorder
	sink     SummarySink
	active   bool
	task     bool
	finish   sync.Once
}

func NewTaskScope(attribution Attribution) *Scope {
	attribution.ScopeType = ScopeTypeTask
	ctx := WithAttribution(context.Background(), attribution)
	config, err := PolicyConfigFromParamTable(paramtable.Get())
	if err != nil {
		return &Scope{ctx: WithRecorder(ctx, NoopRecorder()), recorder: NoopRecorder(), sink: NoopSummarySink{}}
	}
	decision := NewProfileDecider(config, nil).Decide(ctx, ScopeMeta{Attribution: attribution})
	reserveReason := ProfileReasonAllowed
	if decision.Effective != StorageProfileDisabled {
		reserveReason = reserveScope(config, true)
	}
	if decision.Effective == StorageProfileDisabled || reserveReason != ProfileReasonAllowed {
		if decision.Effective != StorageProfileDisabled && reserveReason != ProfileReasonAllowed {
			metrics.StorageProfileDroppedSummaries.WithLabelValues(reserveReason.String()).Inc()
		}
		return &Scope{ctx: WithRecorder(ctx, NoopRecorder()), recorder: NoopRecorder(), sink: NoopSummarySink{}}
	}
	recorder := NewRecorderWithCache(attribution, config.CacheEnabled)
	return &Scope{
		ctx:      WithRecorder(ctx, recorder),
		recorder: recorder,
		sink:     NoopSummarySink{},
		active:   true,
		task:     true,
	}
}

func RequestedLevelFromContext(ctx context.Context) (StorageProfileLevel, bool) {
	metadataValues, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return StorageProfileDisabled, false
	}
	values := metadataValues.Get(ExplicitRequestHeader)
	if len(values) == 0 {
		return StorageProfileDisabled, false
	}
	return ParseProfileLevel(values[0]), true
}

func NewRequestScope(ctx context.Context, attribution Attribution, requested StorageProfileLevel, explicit, authorized bool) (*Scope, ProfileDecision) {
	attribution.ScopeType = ScopeTypeRequest
	ctx = WithProfileLevel(WithAttribution(ctx, attribution), requested)
	config, err := PolicyConfigFromParamTable(paramtable.Get())
	if err != nil {
		decision := ProfileDecision{Requested: requested, Effective: StorageProfileDisabled, Source: ProfileSourceExplicit, Reason: ProfileReasonInvalidLevel}
		return &Scope{ctx: WithRecorder(ctx, NoopRecorder()), recorder: NoopRecorder(), sink: NoopSummarySink{}}, decision
	}
	decision := NewProfileDecider(config, nil).Decide(ctx, ScopeMeta{
		Attribution: attribution,
		Requested:   requested,
		Explicit:    explicit,
		Authorized:  authorized,
	})
	reserveReason := ProfileReasonAllowed
	if decision.Effective != StorageProfileDisabled {
		reserveReason = reserveRequestScope(config)
	}
	if decision.Effective == StorageProfileDisabled || reserveReason != ProfileReasonAllowed {
		if decision.Effective != StorageProfileDisabled && reserveReason != ProfileReasonAllowed {
			decision.Effective = StorageProfileDisabled
			decision.Reason = reserveReason
			metrics.StorageProfileDroppedSummaries.WithLabelValues(reserveReason.String()).Inc()
		}
		ctx = WithProfileLevel(ctx, StorageProfileDisabled)
		return &Scope{ctx: WithRecorder(ctx, NoopRecorder()), recorder: NoopRecorder(), sink: NoopSummarySink{}}, decision
	}
	ctx = WithProfileLevel(ctx, decision.Effective)
	recorder := NewRecorderWithCache(attribution, config.CacheEnabled)
	return &Scope{
		ctx:      WithRecorder(ctx, recorder),
		recorder: recorder,
		sink:     NoopSummarySink{},
		active:   true,
	}, decision
}

func NewContributionScope(ctx context.Context, attribution Attribution, effective StorageProfileLevel) *Scope {
	attribution.ScopeType = ScopeTypeRequest
	ctx = WithProfileLevel(WithAttribution(ctx, attribution), effective)
	config, err := PolicyConfigFromParamTable(paramtable.Get())
	if err != nil || !config.Enabled || effective == StorageProfileDisabled || reserveScope(config, false) != ProfileReasonAllowed {
		return &Scope{ctx: WithRecorder(ctx, NoopRecorder()), recorder: NoopRecorder(), sink: NoopSummarySink{}}
	}
	recorder := NewRecorderWithCache(attribution, config.CacheEnabled)
	return &Scope{
		ctx:      WithRecorder(ctx, recorder),
		recorder: recorder,
		sink:     NoopSummarySink{},
		active:   true,
	}
}

func reserveRequestScope(config PolicyConfig) ProfileDecisionReason {
	if reason := reserveScope(config, false); reason != ProfileReasonAllowed {
		return reason
	}
	if config.MaxProfiledRequestsPerSecond <= 0 {
		return ProfileReasonAllowed
	}
	requestRateMu.Lock()
	defer requestRateMu.Unlock()
	now := time.Now()
	if requestRateWindow.IsZero() || now.Sub(requestRateWindow) >= time.Second {
		requestRateWindow = now
		requestRateCount = 0
	}
	if requestRateCount >= config.MaxProfiledRequestsPerSecond {
		activeProfileScopes.Add(-1)
		return ProfileReasonRateLimit
	}
	requestRateCount++
	return ProfileReasonAllowed
}

func reserveScope(config PolicyConfig, task bool) ProfileDecisionReason {
	for {
		active := activeProfileScopes.Load()
		if config.MaxActiveScopes > 0 && active >= config.MaxActiveScopes {
			return ProfileReasonActiveLimit
		}
		if activeProfileScopes.CompareAndSwap(active, active+1) {
			break
		}
	}
	if !task {
		return ProfileReasonAllowed
	}
	for {
		active := activeTaskScopes.Load()
		if config.MaxProfiledTasks > 0 && active >= config.MaxProfiledTasks {
			activeProfileScopes.Add(-1)
			return ProfileReasonActiveLimit
		}
		if activeTaskScopes.CompareAndSwap(active, active+1) {
			return ProfileReasonAllowed
		}
	}
}

func (s *Scope) Context() context.Context {
	if s == nil || s.ctx == nil {
		return context.Background()
	}
	return s.ctx
}

func (s *Scope) Bind(ctx context.Context) context.Context {
	if s == nil {
		return ctx
	}
	ctx = WithDefaultAttribution(ctx, AttributionFromContext(s.ctx))
	ctx = WithProfileLevel(ctx, ProfileLevelFromContext(s.ctx))
	return WithRecorder(ctx, s.recorder)
}

func (s *Scope) Snapshot() *StorageProfile {
	if s == nil || s.recorder == nil {
		return nil
	}
	return s.recorder.Snapshot()
}

func (s *Scope) Finish() {
	if s == nil {
		return
	}
	s.finish.Do(func() {
		profile := s.recorder.Snapshot()
		if profile != nil {
			_ = s.sink.Publish(context.Background(), profile)
		}
		if s.active {
			activeProfileScopes.Add(-1)
			if s.task {
				activeTaskScopes.Add(-1)
			}
		}
	})
}
