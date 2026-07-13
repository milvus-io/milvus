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
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
)

type ScopeMeta struct {
	Attribution Attribution
	Requested   StorageProfileLevel
	Explicit    bool
	Authorized  bool
}

type ProfileDecision struct {
	Requested StorageProfileLevel
	Effective StorageProfileLevel
	Source    ProfileSource
	Reason    ProfileDecisionReason
}

type ProfileDecider interface {
	Decide(ctx context.Context, meta ScopeMeta) ProfileDecision
}

type ProfileSelector struct {
	TenantID        string
	CollectionID    int64
	WorkloadKind    WorkloadKind
	WorkloadSubtype WorkloadSubtype
	TaskID          string
	RequestType     string
	NodeID          int64
}

type ProfileRule struct {
	ID        string
	Level     StorageProfileLevel
	ExpiresAt time.Time
	Selector  ProfileSelector
}

type ProfileRuleProvider interface {
	Match(meta ScopeMeta) *ProfileRule
}

type noopRuleProvider struct{}

func (noopRuleProvider) Match(ScopeMeta) *ProfileRule { return nil }

type PolicyConfig struct {
	Enabled                      bool
	Level                        StorageProfileLevel
	RequestAllowExplicit         bool
	TaskEnabled                  bool
	TaskTypes                    []WorkloadKind
	CacheEnabled                 bool
	MaxActiveScopes              int64
	MaxProfiledRequestsPerSecond int64
	MaxProfiledTasks             int64
}

type policyDecider struct {
	config PolicyConfig
	rules  ProfileRuleProvider

	activeRequests atomic.Int64
	activeTasks    atomic.Int64

	rateMu          sync.Mutex
	rateWindowStart time.Time
	rateCount       int64
}

func NewProfileDecider(config PolicyConfig, rules ProfileRuleProvider) ProfileDecider {
	if rules == nil {
		rules = noopRuleProvider{}
	}
	return &policyDecider{config: config, rules: rules, rateWindowStart: time.Now()}
}

func (d *policyDecider) Decide(_ context.Context, meta ScopeMeta) ProfileDecision {
	decision := d.decide(meta)
	metrics.StorageProfileDecisions.WithLabelValues(
		decision.Source.String(), decision.Requested.String(), decision.Effective.String(), decision.Reason.String(),
	).Inc()
	return decision
}

func (d *policyDecider) decide(meta ScopeMeta) ProfileDecision {
	decision := ProfileDecision{Requested: meta.Requested, Effective: StorageProfileDisabled, Source: ProfileSourceDefault}
	if !d.config.Enabled {
		decision.Reason = ProfileReasonGlobalDisabled
		return decision
	}
	if rule := d.rules.Match(meta); rule != nil && (rule.ExpiresAt.IsZero() || time.Now().Before(rule.ExpiresAt)) {
		decision.Source = ProfileSourceAdminRule
		decision.Requested = rule.Level
		decision.Effective = min(rule.Level, StorageProfileSummary)
		decision.Reason = ProfileReasonAllowed
		return decision
	}
	if meta.Attribution.ScopeType == ScopeTypeTask {
		decision.Source = ProfileSourceTaskConfig
		if !d.config.TaskEnabled {
			decision.Reason = ProfileReasonTaskDisabled
			return decision
		}
		if !slices.Contains(d.config.TaskTypes, meta.Attribution.WorkloadKind) {
			decision.Reason = ProfileReasonTaskNotSelected
			return decision
		}
		if d.config.MaxProfiledTasks > 0 && d.activeTasks.Load() >= d.config.MaxProfiledTasks {
			decision.Reason = ProfileReasonActiveLimit
			return decision
		}
		decision.Requested = d.config.Level
		decision.Effective = min(d.config.Level, StorageProfileSummary)
		decision.Reason = ProfileReasonAllowed
		return decision
	}
	if !meta.Explicit || meta.Requested == StorageProfileDisabled {
		decision.Reason = ProfileReasonNotRequested
		return decision
	}
	decision.Source = ProfileSourceExplicit
	if !d.config.RequestAllowExplicit {
		decision.Reason = ProfileReasonExplicitDisabled
		return decision
	}
	if !meta.Authorized {
		decision.Reason = ProfileReasonPermissionDenied
		return decision
	}
	if d.config.MaxActiveScopes > 0 && d.activeRequests.Load()+d.activeTasks.Load() >= d.config.MaxActiveScopes {
		decision.Reason = ProfileReasonActiveLimit
		return decision
	}
	if !d.allowRequestByRate() {
		decision.Reason = ProfileReasonRateLimit
		return decision
	}
	decision.Effective = min(meta.Requested, StorageProfileSummary)
	decision.Reason = ProfileReasonAllowed
	return decision
}

func (d *policyDecider) allowRequestByRate() bool {
	if d.config.MaxProfiledRequestsPerSecond <= 0 {
		return true
	}
	d.rateMu.Lock()
	defer d.rateMu.Unlock()
	now := time.Now()
	if now.Sub(d.rateWindowStart) >= time.Second {
		d.rateWindowStart = now
		d.rateCount = 0
	}
	if d.rateCount >= d.config.MaxProfiledRequestsPerSecond {
		return false
	}
	d.rateCount++
	return true
}

func ValidateTaskTypes(values []string) ([]WorkloadKind, bool) {
	result := make([]WorkloadKind, 0, len(values))
	for _, value := range values {
		kind, ok := ParseWorkloadKind(value)
		if !ok || kind < WorkloadKindFlush || kind > WorkloadKindExternalSync {
			return nil, false
		}
		if !slices.Contains(result, kind) {
			result = append(result, kind)
		}
	}
	return result, true
}
