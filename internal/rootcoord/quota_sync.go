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

package rootcoord

import (
	"context"

	"go.uber.org/multierr"

	"github.com/milvus-io/milvus/internal/util/proxyutil"
	"github.com/milvus-io/milvus/internal/util/quotasync"
	rlinternal "github.com/milvus-io/milvus/internal/util/ratelimitutil"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func (q *QuotaCenter) publishQuotaRules() error {
	proxyCount := q.proxies.GetProxyCount()
	if proxyCount == 0 {
		return nil
	}
	epoch := q.publishedRules.Version.Epoch
	if epoch == 0 {
		var err error
		epoch, err = q.tsoAllocator.GenerateTSO(1)
		if err != nil {
			return err
		}
	}
	if epoch == 0 {
		return merr.WrapErrServiceNotReadyMsg("quota publisher epoch is not initialized")
	}
	policies := make(map[quotasync.Key]*proxypb.Limiter)
	if q.localQuota != nil {
		proxyCount = q.localQuota.calculationProxyCount
	}
	var visit func(quotasync.Key, *rlinternal.RateLimiterNode)
	visit = func(key quotasync.Key, node *rlinternal.RateLimiterNode) {
		policies[key] = quotasync.Canonical(q.toRequestLimiterForProxyCount(node, proxyCount))
		node.GetChildren().Range(func(id int64, child *rlinternal.RateLimiterNode) bool {
			next := key
			next.Scope = child.Level()
			switch next.Scope {
			case internalpb.RateScope_Database:
				next.DB = id
			case internalpb.RateScope_Collection:
				next.Collection = id
			case internalpb.RateScope_Partition:
				next.Partition = id
			}
			visit(next, child)
			return true
		})
	}
	if q.localQuota != nil && q.publishedRules.Version.Valid() {
		q.visitLocalQuotaNodes(func(key quotasync.Key, node *rlinternal.RateLimiterNode) {
			policies[key] = quotasync.Canonical(q.toRequestLimiterForProxyCount(node, proxyCount))
		})
	} else {
		visit(quotasync.Key{Scope: internalpb.RateScope_Cluster}, q.rateLimiter.GetRootLimiters())
	}
	defaults := quotasync.ScopeDefaults()
	if err := q.validateLocalQuotaInputs(); err != nil {
		return err
	}
	if q.localQuota != nil && q.publishedRules.Version.Valid() {
		q.publishedRules.PublishPatch(epoch, policies, q.localQuota.deleted, defaults)
	} else {
		q.publishedRules.Publish(epoch, policies, defaults)
	}
	if q.localQuota != nil {
		q.localQuota.proxyCount = proxyCount
		q.localQuota.dirty = make(map[int64]bool)
		q.localQuota.deleted = make(map[quotasync.Key]bool)
	}
	return nil
}

func (q *QuotaCenter) reconcilePublishedRates() error {
	if !q.publishedRules.Version.Valid() {
		return nil
	}
	manager, ok := q.proxies.(interface {
		SyncQuota(context.Context, *quotasync.Publisher, bool, func() *proxypb.SetRatesRequest) map[int64]proxyutil.QuotaSyncResult
	})
	if !ok {
		return merr.WrapErrServiceInternalMsg("proxy manager does not support quota synchronization")
	}
	ctx, cancel := context.WithTimeout(q.ctx, SetRatesTimeout)
	defer cancel()
	legacy := func() *proxypb.SetRatesRequest {
		return &proxypb.SetRatesRequest{Base: commonpbutil.NewMsgBase(), RootLimiter: q.publishedRules.Full().RootLimiter}
	}
	results := manager.SyncQuota(ctx, &q.publishedRules, Params.QuotaConfig.IncrementalRatesEnabled.GetAsBool(), legacy)
	errs := make([]error, 0)
	for id, result := range results {
		mode := result.Mode
		if mode == "" {
			mode = "none"
		}
		outcome := result.Result
		if result.Err != nil {
			outcome = "failure"
		} else if outcome != "applied" && outcome != "already_applied" && outcome != "full" {
			outcome = "other"
		}
		metrics.RootCoordQuotaSync.WithLabelValues(mode, outcome).Inc()
		if result.Err != nil {
			errs = append(errs, merr.Wrapf(result.Err, "quota sync proxy %d", id))
		}
	}
	metrics.RootCoordQuotaUnsynced.Set(float64(len(errs)))
	return multierr.Combine(errs...)
}

// A failed calculation leaves the last complete publication available to a
// Proxy that missed an update or restarted since the previous round.
func (q *QuotaCenter) retryPublishedRates() {
	if q.versionedRates {
		if err := q.reconcilePublishedRates(); err != nil {
			mlog.Warn(q.ctx, "quota reconciliation failed", mlog.Err(err))
		}
	}
}
