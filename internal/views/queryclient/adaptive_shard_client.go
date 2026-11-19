// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package queryclient

import (
	"context"
	"maps"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/util/streamingutil/service/balancer/picker"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func (s *shardViewQueryClient) replicaSelectionConfig() ReplicaSelectionConfig {
	if s.selectionConfig == nil {
		return ReplicaSelectionConfig{}
	}
	return s.selectionConfig().normalized()
}

// adaptiveShardAttempt owns everything that may be mutated by one replica.
// Only the winning attempt's commit can reach the request reducer.
type adaptiveShardAttempt struct {
	params *shardExecParams
	commit func(qviews.ShardID) error
}

func (s *shardViewQueryClient) searchWithReplicaSelection(ctx context.Context, req *ShardSearchRequest, cfg ReplicaSelectionConfig) (*ShardPlan, error) {
	key := replicaPolicyKey{channel: req.VChannel, class: "search/" + req.Req.ConsistencyLevel.String()}
	return s.executeAdaptiveShard(ctx, key, req.Req.CollectionID, cfg, func(observation *replicaObservation) *adaptiveShardAttempt {
		var mu sync.Mutex
		var responses []*viewpb.SearchOnViewResponse
		var stage twoStageSearch
		client := *s
		client.queryServiceClient = &observedViewQueryService{ViewQueryServiceClient: s.queryServiceClient, observation: observation}
		return &adaptiveShardAttempt{
			params: &shardExecParams{
				collectionID: req.Req.CollectionID, consistencyLevel: req.Req.ConsistencyLevel,
				mvcc: req.MVCC, barrierMVCC: req.BarrierMVCC,
				buildPlanReq: func(shard qviews.ShardID) *viewpb.GetQueryPlanRequest {
					request := proto.Clone(req.Req).(*internalpb.SearchRequest)
					return &viewpb.GetQueryPlanRequest{
						CollectionId: request.CollectionID, ShardId: shard.IntoProto(), PartitionIds: request.PartitionIDs,
						Request: &viewpb.GetQueryPlanRequest_LegacySearchRequest{LegacySearchRequest: request},
					}
				},
				beforeDispatch: func(ctx context.Context, response *viewpb.GetQueryPlanResponse, shard qviews.ShardID) error {
					return client.prepareTwoStageSearch(ctx, response, shard, req.TotalChannelNum, &stage)
				},
				dispatchNode: func(ctx context.Context, node qviews.WorkNode, plan *viewpb.QueryPlan, shard qviews.ShardID) error {
					response, err := client.queryServiceClient.SearchOnView(ctx, node, &viewpb.SearchOnViewRequest{
						ExecutionOptions: stage.options, LegacyReq: legacySearchRequestForNode(plan, node),
						SegmentSelection: qviews.WorkNodeSelection(plan, node).GetSegmentSelection(),
						SubSelections:    qviews.WorkNodeSelection(plan, node).GetSubSelections(),
						ShardId:          shard.IntoProto(), Version: plan.Version, Mvcc: plan.GetMvcc(),
					})
					if err != nil {
						return err
					}
					if response.GetLegacyResults() == nil {
						return merr.WrapErrServiceInternalMsg("missing legacy search result for shard %s", shard.String())
					}
					if err := merr.Error(response.GetLegacyResults().GetStatus()); err != nil {
						return err
					}
					stage.mergeFeatures(node, response)
					mu.Lock()
					responses = append(responses, response)
					mu.Unlock()
					return nil
				},
			},
			commit: func(shard qviews.ShardID) error {
				for _, response := range responses {
					if err := req.Reducer.Add(shard, response); err != nil {
						req.Reducer.ResetShard(shard)
						return err
					}
				}
				return nil
			},
		}
	})
}

func (s *shardViewQueryClient) queryWithReplicaSelection(ctx context.Context, req *ShardQueryRequest, cfg ReplicaSelectionConfig) (*ShardPlan, error) {
	key := replicaPolicyKey{channel: req.VChannel, class: "query/" + req.Req.ConsistencyLevel.String()}
	return s.executeAdaptiveShard(ctx, key, req.Req.CollectionID, cfg, func(observation *replicaObservation) *adaptiveShardAttempt {
		var mu sync.Mutex
		var responses []*viewpb.QueryOnViewResponse
		service := &observedViewQueryService{ViewQueryServiceClient: s.queryServiceClient, observation: observation}
		return &adaptiveShardAttempt{
			params: &shardExecParams{
				collectionID: req.Req.CollectionID, consistencyLevel: req.Req.ConsistencyLevel,
				mvcc: req.MVCC, barrierMVCC: req.BarrierMVCC,
				buildPlanReq: func(shard qviews.ShardID) *viewpb.GetQueryPlanRequest {
					request := proto.Clone(req.Req).(*internalpb.RetrieveRequest)
					return &viewpb.GetQueryPlanRequest{
						CollectionId: request.CollectionID, ShardId: shard.IntoProto(), PartitionIds: request.PartitionIDs,
						Request: &viewpb.GetQueryPlanRequest_LegacyRetrieveRequest{LegacyRetrieveRequest: request},
					}
				},
				dispatchNode: func(ctx context.Context, node qviews.WorkNode, plan *viewpb.QueryPlan, shard qviews.ShardID) error {
					response, err := service.QueryOnView(ctx, node, &viewpb.QueryOnViewRequest{
						LegacyReq:        legacyRetrieveRequestForNode(plan, node),
						SegmentSelection: qviews.WorkNodeSelection(plan, node).GetSegmentSelection(),
						ShardId:          shard.IntoProto(), Version: plan.Version, Mvcc: plan.GetMvcc(),
					})
					if err != nil {
						return err
					}
					if response.GetLegacyResults() == nil {
						return merr.WrapErrServiceInternalMsg("missing legacy query result for shard %s", shard.String())
					}
					if err := merr.Error(response.GetLegacyResults().GetStatus()); err != nil {
						return err
					}
					if !isEmptySuccessfulQueryResponse(response) {
						mu.Lock()
						responses = append(responses, response)
						mu.Unlock()
					}
					return nil
				},
			},
			commit: func(shard qviews.ShardID) error {
				for _, response := range responses {
					if err := req.Reducer.Add(shard, response); err != nil {
						req.Reducer.ResetShard(shard)
						return err
					}
				}
				return nil
			},
		}
	})
}

// observedViewQueryService tracks only QN work. A shared WAL-owner planning or
// growing-side wait alone must not penalize a particular replica.
type observedViewQueryService struct {
	ViewQueryServiceClient
	observation *replicaObservation
}

func (s *observedViewQueryService) SearchOnView(ctx context.Context, node qviews.WorkNode, req *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
	defer s.observation.workerStarted(node)()
	return s.ViewQueryServiceClient.SearchOnView(ctx, node, req)
}

func (s *observedViewQueryService) QueryOnView(ctx context.Context, node qviews.WorkNode, req *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error) {
	defer s.observation.workerStarted(node)()
	return s.ViewQueryServiceClient.QueryOnView(ctx, node, req)
}

type adaptiveReplicaResult struct {
	candidate qviews.Replica
	attempt   *adaptiveShardAttempt
	plan      *ShardPlan
	latency   time.Duration
	blocked   bool
	err       error
}

func (s *shardViewQueryClient) executeAdaptiveShard(ctx context.Context, key replicaPolicyKey, collectionID int64, cfg ReplicaSelectionConfig, newAttempt func(*replicaObservation) *adaptiveShardAttempt) (*ShardPlan, error) {
	route, err := s.replicas.route(key.channel)
	if err != nil {
		return nil, err
	}
	var loadCheck *collectionLoadCheck
	defer func() {
		if loadCheck != nil {
			loadCheck.release()
		}
	}()
	checkLoad := func(ctx context.Context) (time.Time, error) {
		if loadCheck == nil {
			loadCheck = s.loadChecks.acquire(collectionID)
		}
		return loadCheck.check(ctx)
	}
	var choose func([]qviews.Replica) qviews.Replica
	if cfg.LatencyWeighted {
		choose = func(candidates []qviews.Replica) qviews.Replica {
			return s.selection.selectReplica(key, candidates, cfg)
		}
	}
	tried := make(map[qviews.Replica]bool)
	deadNodes := make(map[int64]struct{})
	var sessionChanged <-chan struct{}
	var lastErr error
	hedgedOnce := false
	for attempts := 0; ; {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if s.maxRetries > 0 && attempts >= s.maxRetries && !route.hasUntried(tried) {
			return nil, lastErr
		}
		primary, sessionsUpdated, err := route.nextReplicaWithSelector(ctx, tried, sessionChanged, checkLoad, choose)
		if sessionsUpdated {
			clear(tried)
			clear(deadNodes)
			sessionChanged = nil
			continue
		}
		if err != nil {
			return nil, err
		}
		started := time.Now()
		results := make(chan adaptiveReplicaResult, 2)
		launch := func(candidate qviews.Replica) *replicaObservation {
			tried[candidate] = false
			attempts++
			var observation *replicaObservation
			if cfg.LatencyWeighted {
				observation = s.selection.begin(key, candidate.ReplicaID, cfg)
			}
			attempt := newAttempt(observation)
			knownDead := maps.Clone(deadNodes)
			attemptStarted := time.Now()
			go func() {
				plan, blocked, err := s.executeReplica(ctx, qviews.ShardID{ReplicaID: candidate.ReplicaID, VChannel: key.channel}, attempt.params, knownDead)
				var failure *workNodeFailure
				qnFailure := errors.As(err, &failure) && failure.node.NodeType() == qviews.NodeTypeQueryNode
				observation.finish(ctx, err, qnFailure, err == nil && !blocked)
				results <- adaptiveReplicaResult{candidate: candidate, attempt: attempt, plan: plan, latency: time.Since(attemptStarted), blocked: blocked, err: err}
			}()
			return observation
		}
		primaryObservation := launch(primary)
		pending := 1
		var terminalErr error
		hedgeSent := false
		var hedgeTimer *time.Timer
		var hedgeReady <-chan time.Time
		if cfg.HedgeEnabled && !hedgedOnce {
			hedgeTimer = time.NewTimer(s.selection.hedgeDelay(key, cfg))
			hedgeReady = hedgeTimer.C
		}
		stopTimer := func() {
			if hedgeTimer != nil {
				hedgeTimer.Stop()
			}
		}
		for pending > 0 {
			select {
			case <-ctx.Done():
				stopTimer()
				s.selection.completed(key, time.Since(started), false, hedgeSent, false)
				return nil, ctx.Err()
			case <-hedgeReady:
				hedgeReady = nil
				if !s.selection.allowHedge(key) {
					metrics.ProxyReplicaHedgeCount.WithLabelValues(paramtable.GetStringNodeID(), "skipped_low_win_rate").Inc()
					continue
				}
				if candidate, ok := route.alternative(primary.ReplicaID, tried, choose); ok {
					launch(candidate)
					pending++
					hedgeSent = true
					hedgedOnce = true
					metrics.ProxyReplicaHedgeCount.WithLabelValues(paramtable.GetStringNodeID(), "sent").Inc()
				} else {
					metrics.ProxyReplicaHedgeCount.WithLabelValues(paramtable.GetStringNodeID(), "skipped_no_replica").Inc()
				}
			case result := <-results:
				pending--
				if result.blocked {
					tried[result.candidate] = true
					continue
				}
				if result.err == nil {
					stopTimer()
					if err := result.attempt.commit(result.plan.ShardID); err != nil {
						s.selection.completed(key, time.Since(started), false, hedgeSent, false)
						return nil, err
					}
					hedgeWon := result.candidate.ReplicaID != primary.ReplicaID
					if hedgeWon {
						metrics.ProxyReplicaHedgeCount.WithLabelValues(paramtable.GetStringNodeID(), "won").Inc()
						primaryObservation.overtaken(ctx)
					}
					s.selection.completed(key, result.latency, true, hedgeSent, hedgeWon)
					return result.plan, nil
				}
				lastErr = result.err
				if merr.GetErrorType(toMilvusError(result.err)) == merr.InputError {
					// Stop retrying, but allow an already dispatched sibling to succeed.
					terminalErr = result.err
					continue
				}
				if !retryableQueryError(result.err) {
					// This error prevents another retry round, but an already
					// dispatched sibling can still succeed. Preserve it even if
					// the sibling subsequently returns a retryable error.
					if terminalErr == nil {
						terminalErr = result.err
					}
					continue
				}
				var failure *workNodeFailure
				var missing *viewerror.NodeSessionUnavailable
				missingSession := errors.As(result.err, &missing)
				if errors.As(result.err, &failure) && (missingSession || picker.IsErrSubConnNoExist(failure.err)) {
					if node, ok := failure.node.(qviews.QueryNode); ok {
						deadNodes[node.ID] = struct{}{}
						tried[result.candidate] = true
						if missingSession && sessionChanged == nil {
							sessionChanged = missing.Changed
						}
					}
				}
			}
		}
		stopTimer()
		s.selection.completed(key, time.Since(started), false, hedgeSent, false)
		if terminalErr != nil {
			return nil, terminalErr
		}
	}
}
