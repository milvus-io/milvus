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

// delegator package contains the logic of shard delegator.
package delegator

import (
	"context"
	"fmt"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.opentelemetry.io/otel"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querynodev2/cluster"
	"github.com/milvus-io/milvus/internal/querynodev2/delegator/deletebuffer"
	"github.com/milvus-io/milvus/internal/querynodev2/optimizers"
	"github.com/milvus-io/milvus/internal/querynodev2/pkoracle"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/querynodev2/tsafe"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/streamrpc"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/lifetime"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metautil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// ShardDelegator is the interface definition.
type ShardDelegator interface {
	Collection() int64
	Version() int64
	GetSegmentInfo(readable bool) (sealed []SnapshotItem, growing []SegmentEntry)
	SyncDistribution(ctx context.Context, entries ...SegmentEntry)
	SyncPartitionStats(ctx context.Context, partVersions map[int64]int64)
	GetPartitionStatsVersions(ctx context.Context) map[int64]int64
	Search(ctx context.Context, req *querypb.SearchRequest) ([]*internalpb.SearchResults, error)
	Query(ctx context.Context, req *querypb.QueryRequest) ([]*internalpb.RetrieveResults, error)
	QueryStream(ctx context.Context, req *querypb.QueryRequest, srv streamrpc.QueryStreamServer) error
	GetStatistics(ctx context.Context, req *querypb.GetStatisticsRequest) ([]*internalpb.GetStatisticsResponse, error)

	// data
	ProcessInsert(insertRecords map[int64]*InsertData)
	ProcessDelete(deleteData []*DeleteData, ts uint64)
	LoadGrowing(ctx context.Context, infos []*querypb.SegmentLoadInfo, version int64) error
	LoadSegments(ctx context.Context, req *querypb.LoadSegmentsRequest) error
	ReleaseSegments(ctx context.Context, req *querypb.ReleaseSegmentsRequest, force bool) error
	SyncTargetVersion(newVersion int64, growingInTarget []int64, sealedInTarget []int64, droppedInTarget []int64, checkpoint *msgpb.MsgPosition)
	GetTargetVersion() int64

	// manage exclude segments
	AddExcludedSegments(excludeInfo map[int64]uint64)
	VerifyExcludedSegments(segmentID int64, ts uint64) bool
	TryCleanExcludedSegments(ts uint64)

	// control
	Serviceable() bool
	Start()
	Close()
}

var _ ShardDelegator = (*shardDelegator)(nil)

// shardDelegator maintains the shard distribution and streaming part of the data.
type shardDelegator struct {
	// shard information attributes
	collectionID int64
	replicaID    int64
	vchannelName string
	version      int64
	// collection schema
	collection *segments.Collection

	workerManager cluster.Manager

	lifetime lifetime.Lifetime[lifetime.State]

	distribution   *distribution
	segmentManager segments.SegmentManager
	tsafeManager   tsafe.Manager
	pkOracle       pkoracle.PkOracle
	level0Mut      sync.RWMutex
	// stream delete buffer
	deleteMut    sync.RWMutex
	deleteBuffer deletebuffer.DeleteBuffer[*deletebuffer.Item]
	// dispatcherClient msgdispatcher.Client
	factory msgstream.Factory

	sf          conc.Singleflight[struct{}]
	loader      segments.Loader
	tsCond      *sync.Cond
	latestTsafe *atomic.Uint64
	// queryHook
	queryHook      optimizers.QueryHook
	partitionStats map[UniqueID]*storage.PartitionStatsSnapshot
	chunkManager   storage.ChunkManager

	excludedSegments *ExcludedSegments
	// cause growing segment meta has been stored in segmentManager/distribution/pkOracle/excludeSegments
	// in order to make add/remove growing be atomic, need lock before modify these meta info
	growingSegmentLock sync.RWMutex
	partitionStatsMut  sync.RWMutex
}

// getLogger returns the zap logger with pre-defined shard attributes.
func (sd *shardDelegator) getLogger(ctx context.Context) *log.MLogger {
	return log.Ctx(ctx).With(
		zap.Int64("collectionID", sd.collectionID),
		zap.String("channel", sd.vchannelName),
		zap.Int64("replicaID", sd.replicaID),
	)
}

// Serviceable returns whether delegator is serviceable now.
func (sd *shardDelegator) Serviceable() bool {
	return lifetime.IsWorking(sd.lifetime.GetState()) == nil
}

func (sd *shardDelegator) Stopped() bool {
	return lifetime.NotStopped(sd.lifetime.GetState()) != nil
}

// Start sets delegator to working state.
func (sd *shardDelegator) Start() {
	sd.lifetime.SetState(lifetime.Working)
}

// Collection returns delegator collection id.
func (sd *shardDelegator) Collection() int64 {
	return sd.collectionID
}

// Version returns delegator version.
func (sd *shardDelegator) Version() int64 {
	return sd.version
}

// GetSegmentInfo returns current segment distribution snapshot.
func (sd *shardDelegator) GetSegmentInfo(readable bool) ([]SnapshotItem, []SegmentEntry) {
	return sd.distribution.PeekSegments(readable)
}

// SyncDistribution revises distribution.
func (sd *shardDelegator) SyncDistribution(ctx context.Context, entries ...SegmentEntry) {
	log := sd.getLogger(ctx)

	log.Info("sync distribution", zap.Any("entries", entries))

	sd.distribution.AddDistributions(entries...)
}

// SyncDistribution revises distribution.
func (sd *shardDelegator) SyncPartitionStats(ctx context.Context, partVersions map[int64]int64) {
	log := sd.getLogger(ctx)
	log.RatedInfo(60, "update partition stats versions")
	sd.loadPartitionStats(ctx, partVersions)
}

func (sd *shardDelegator) GetPartitionStatsVersions(ctx context.Context) map[int64]int64 {
	sd.partitionStatsMut.RLock()
	defer sd.partitionStatsMut.RUnlock()
	partStatMap := make(map[int64]int64)
	for partID, partStats := range sd.partitionStats {
		partStatMap[partID] = partStats.GetVersion()
	}
	return partStatMap
}

func (sd *shardDelegator) modifySearchRequest(req *querypb.SearchRequest, scope querypb.DataScope, segmentIDs []int64, targetID int64) *querypb.SearchRequest {
	nodeReq := proto.Clone(req).(*querypb.SearchRequest)
	nodeReq.Scope = scope
	nodeReq.Req.Base.TargetID = targetID
	nodeReq.SegmentIDs = segmentIDs
	nodeReq.DmlChannels = []string{sd.vchannelName}
	return nodeReq
}

func (sd *shardDelegator) modifyQueryRequest(req *querypb.QueryRequest, scope querypb.DataScope, segmentIDs []int64, targetID int64) *querypb.QueryRequest {
	nodeReq := proto.Clone(req).(*querypb.QueryRequest)
	nodeReq.Scope = scope
	nodeReq.Req.Base.TargetID = targetID
	nodeReq.SegmentIDs = segmentIDs
	nodeReq.DmlChannels = []string{sd.vchannelName}
	return nodeReq
}

// Search preforms search operation on shard.
func (sd *shardDelegator) search(ctx context.Context, req *querypb.SearchRequest, sealed []SnapshotItem, growing []SegmentEntry) ([]*internalpb.SearchResults, error) {
	log := sd.getLogger(ctx)
	if req.Req.IgnoreGrowing {
		growing = []SegmentEntry{}
	}

	if paramtable.Get().QueryNodeCfg.EnableSegmentPrune.GetAsBool() {
		func() {
			sd.partitionStatsMut.RLock()
			defer sd.partitionStatsMut.RUnlock()
			PruneSegments(ctx, sd.partitionStats, req.GetReq(), nil, sd.collection.Schema(), sealed,
				PruneInfo{filterRatio: paramtable.Get().QueryNodeCfg.DefaultSegmentFilterRatio.GetAsFloat()})
		}()
	}

	// get final sealedNum after possible segment prune
	sealedNum := lo.SumBy(sealed, func(item SnapshotItem) int { return len(item.Segments) })
	log.Debug("search segments...",
		zap.Int("sealedNum", sealedNum),
		zap.Int("growingNum", len(growing)),
	)

	req, err := optimizers.OptimizeSearchParams(ctx, req, sd.queryHook, sealedNum)
	if err != nil {
		log.Warn("failed to optimize search params", zap.Error(err))
		return nil, err
	}
	tasks, err := organizeSubTask(ctx, req, sealed, growing, sd, sd.modifySearchRequest)
	if err != nil {
		log.Warn("Search organizeSubTask failed", zap.Error(err))
		return nil, err
	}
	results, err := executeSubTasks(ctx, tasks, func(ctx context.Context, req *querypb.SearchRequest, worker cluster.Worker) (*internalpb.SearchResults, error) {
		return worker.SearchSegments(ctx, req)
	}, "Search", log)
	if err != nil {
		log.Warn("Delegator search failed", zap.Error(err))
		return nil, err
	}

	log.Debug("Delegator search done")

	return results, nil
}

// Search preforms search operation on shard.
func (sd *shardDelegator) Search(ctx context.Context, req *querypb.SearchRequest) ([]*internalpb.SearchResults, error) {
	log := sd.getLogger(ctx)
	if err := sd.lifetime.Add(lifetime.IsWorking); err != nil {
		return nil, err
	}
	defer sd.lifetime.Done()

	if !funcutil.SliceContain(req.GetDmlChannels(), sd.vchannelName) {
		log.Warn("deletgator received search request not belongs to it",
			zap.Strings("reqChannels", req.GetDmlChannels()),
		)
		return nil, fmt.Errorf("dml channel not match, delegator channel %s, search channels %v", sd.vchannelName, req.GetDmlChannels())
	}

	partitions := req.GetReq().GetPartitionIDs()
	if !sd.collection.ExistPartition(partitions...) {
		return nil, merr.WrapErrPartitionNotLoaded(partitions)
	}

	// wait tsafe
	waitTr := timerecord.NewTimeRecorder("wait tSafe")
	tSafe, err := sd.waitTSafe(ctx, req.Req.GuaranteeTimestamp)
	if err != nil {
		log.Warn("delegator search failed to wait tsafe", zap.Error(err))
		return nil, err
	}
	if req.GetReq().GetMvccTimestamp() == 0 {
		req.Req.MvccTimestamp = tSafe
	}
	metrics.QueryNodeSQLatencyWaitTSafe.WithLabelValues(
		fmt.Sprint(paramtable.GetNodeID()), metrics.SearchLabel).
		Observe(float64(waitTr.ElapseSpan().Milliseconds()))

	sealed, growing, version, err := sd.distribution.PinReadableSegments(req.GetReq().GetPartitionIDs()...)
	if err != nil {
		log.Warn("delegator failed to search, current distribution is not serviceable")
		return nil, merr.WrapErrChannelNotAvailable(sd.vchannelName, "distribution is not servcieable")
	}
	defer sd.distribution.Unpin(version)
	existPartitions := sd.collection.GetPartitions()
	growing = lo.Filter(growing, func(segment SegmentEntry, _ int) bool {
		return funcutil.SliceContain(existPartitions, segment.PartitionID)
	})

	if req.GetReq().GetIsAdvanced() {
		futures := make([]*conc.Future[*internalpb.SearchResults], len(req.GetReq().GetSubReqs()))
		for index, subReq := range req.GetReq().GetSubReqs() {
			newRequest := &internalpb.SearchRequest{
				Base:               req.GetReq().GetBase(),
				ReqID:              req.GetReq().GetReqID(),
				DbID:               req.GetReq().GetDbID(),
				CollectionID:       req.GetReq().GetCollectionID(),
				PartitionIDs:       subReq.GetPartitionIDs(),
				Dsl:                subReq.GetDsl(),
				PlaceholderGroup:   subReq.GetPlaceholderGroup(),
				DslType:            subReq.GetDslType(),
				SerializedExprPlan: subReq.GetSerializedExprPlan(),
				OutputFieldsId:     req.GetReq().GetOutputFieldsId(),
				MvccTimestamp:      req.GetReq().GetMvccTimestamp(),
				GuaranteeTimestamp: req.GetReq().GetGuaranteeTimestamp(),
				TimeoutTimestamp:   req.GetReq().GetTimeoutTimestamp(),
				Nq:                 subReq.GetNq(),
				Topk:               subReq.GetTopk(),
				MetricType:         subReq.GetMetricType(),
				IgnoreGrowing:      req.GetReq().GetIgnoreGrowing(),
				Username:           req.GetReq().GetUsername(),
				IsAdvanced:         false,
			}
			future := conc.Go(func() (*internalpb.SearchResults, error) {
				searchReq := &querypb.SearchRequest{
					Req:             newRequest,
					DmlChannels:     req.GetDmlChannels(),
					TotalChannelNum: req.GetTotalChannelNum(),
				}
				searchReq.Req.GuaranteeTimestamp = req.GetReq().GetGuaranteeTimestamp()
				searchReq.Req.TimeoutTimestamp = req.GetReq().GetTimeoutTimestamp()
				if searchReq.GetReq().GetMvccTimestamp() == 0 {
					searchReq.GetReq().MvccTimestamp = tSafe
				}

				results, err := sd.search(ctx, searchReq, sealed, growing)
				if err != nil {
					return nil, err
				}

				return segments.ReduceSearchResults(ctx,
					results,
					segments.NewReduceInfo(searchReq.Req.GetNq(),
						searchReq.Req.GetTopk(),
						searchReq.Req.GetExtraSearchParam().GetGroupByFieldId(),
						searchReq.Req.GetExtraSearchParam().GetGroupSize(),
						searchReq.Req.GetMetricType()),
				)
			})
			futures[index] = future
		}

		err = conc.AwaitAll(futures...)
		if err != nil {
			return nil, err
		}
		results := make([]*internalpb.SearchResults, len(futures))
		for i, future := range futures {
			result := future.Value()
			if result.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
				log.Debug("delegator hybrid search failed",
					zap.String("reason", result.GetStatus().GetReason()))
				return nil, merr.Error(result.GetStatus())
			}
			results[i] = result
		}
		var ret *internalpb.SearchResults
		ret, err = segments.MergeToAdvancedResults(ctx, results)
		if err != nil {
			return nil, err
		}
		return []*internalpb.SearchResults{ret}, nil
	}
	return sd.search(ctx, req, sealed, growing)
}

func (sd *shardDelegator) QueryStream(ctx context.Context, req *querypb.QueryRequest, srv streamrpc.QueryStreamServer) error {
	log := sd.getLogger(ctx)
	if !sd.Serviceable() {
		return errors.New("delegator is not serviceable")
	}

	if !funcutil.SliceContain(req.GetDmlChannels(), sd.vchannelName) {
		log.Warn("deletgator received query request not belongs to it",
			zap.Strings("reqChannels", req.GetDmlChannels()),
		)
		return fmt.Errorf("dml channel not match, delegator channel %s, search channels %v", sd.vchannelName, req.GetDmlChannels())
	}

	partitions := req.GetReq().GetPartitionIDs()
	if !sd.collection.ExistPartition(partitions...) {
		return merr.WrapErrPartitionNotLoaded(partitions)
	}

	// wait tsafe
	waitTr := timerecord.NewTimeRecorder("wait tSafe")
	tSafe, err := sd.waitTSafe(ctx, req.Req.GuaranteeTimestamp)
	if err != nil {
		log.Warn("delegator query failed to wait tsafe", zap.Error(err))
		return err
	}
	if req.GetReq().GetMvccTimestamp() == 0 {
		req.Req.MvccTimestamp = tSafe
	}
	metrics.QueryNodeSQLatencyWaitTSafe.WithLabelValues(
		fmt.Sprint(paramtable.GetNodeID()), metrics.QueryLabel).
		Observe(float64(waitTr.ElapseSpan().Milliseconds()))

	sealed, growing, version, err := sd.distribution.PinReadableSegments(req.GetReq().GetPartitionIDs()...)
	if err != nil {
		log.Warn("delegator failed to query, current distribution is not serviceable")
		return merr.WrapErrChannelNotAvailable(sd.vchannelName, "distribution is not servcieable")
	}
	defer sd.distribution.Unpin(version)
	existPartitions := sd.collection.GetPartitions()
	growing = lo.Filter(growing, func(segment SegmentEntry, _ int) bool {
		return funcutil.SliceContain(existPartitions, segment.PartitionID)
	})
	if req.Req.IgnoreGrowing {
		growing = []SegmentEntry{}
	}

	log.Info("query stream segments...",
		zap.Int("sealedNum", len(sealed)),
		zap.Int("growingNum", len(growing)),
	)
	tasks, err := organizeSubTask(ctx, req, sealed, growing, sd, sd.modifyQueryRequest)
	if err != nil {
		log.Warn("query organizeSubTask failed", zap.Error(err))
		return err
	}

	_, err = executeSubTasks(ctx, tasks, func(ctx context.Context, req *querypb.QueryRequest, worker cluster.Worker) (*internalpb.RetrieveResults, error) {
		return nil, worker.QueryStreamSegments(ctx, req, srv)
	}, "Query", log)
	if err != nil {
		log.Warn("Delegator query failed", zap.Error(err))
		return err
	}

	log.Info("Delegator Query done")

	return nil
}

// Query performs query operation on shard.
func (sd *shardDelegator) Query(ctx context.Context, req *querypb.QueryRequest) ([]*internalpb.RetrieveResults, error) {
	log := sd.getLogger(ctx)
	if err := sd.lifetime.Add(lifetime.IsWorking); err != nil {
		return nil, err
	}
	defer sd.lifetime.Done()

	if !funcutil.SliceContain(req.GetDmlChannels(), sd.vchannelName) {
		log.Warn("delegator received query request not belongs to it",
			zap.Strings("reqChannels", req.GetDmlChannels()),
		)
		return nil, fmt.Errorf("dml channel not match, delegator channel %s, search channels %v", sd.vchannelName, req.GetDmlChannels())
	}

	partitions := req.GetReq().GetPartitionIDs()
	if !sd.collection.ExistPartition(partitions...) {
		return nil, merr.WrapErrPartitionNotLoaded(partitions)
	}

	// wait tsafe
	waitTr := timerecord.NewTimeRecorder("wait tSafe")
	tSafe, err := sd.waitTSafe(ctx, req.Req.GuaranteeTimestamp)
	if err != nil {
		log.Warn("delegator query failed to wait tsafe", zap.Error(err))
		return nil, err
	}
	if req.GetReq().GetMvccTimestamp() == 0 {
		req.Req.MvccTimestamp = tSafe
	}
	metrics.QueryNodeSQLatencyWaitTSafe.WithLabelValues(
		fmt.Sprint(paramtable.GetNodeID()), metrics.QueryLabel).
		Observe(float64(waitTr.ElapseSpan().Milliseconds()))

	sealed, growing, version, err := sd.distribution.PinReadableSegments(req.GetReq().GetPartitionIDs()...)
	if err != nil {
		log.Warn("delegator failed to query, current distribution is not serviceable")
		return nil, merr.WrapErrChannelNotAvailable(sd.vchannelName, "distribution is not servcieable")
	}
	defer sd.distribution.Unpin(version)
	if req.Req.IgnoreGrowing {
		growing = []SegmentEntry{}
	} else {
		existPartitions := sd.collection.GetPartitions()
		growing = lo.Filter(growing, func(segment SegmentEntry, _ int) bool {
			return funcutil.SliceContain(existPartitions, segment.PartitionID)
		})
	}

	if paramtable.Get().QueryNodeCfg.EnableSegmentPrune.GetAsBool() {
		func() {
			sd.partitionStatsMut.RLock()
			defer sd.partitionStatsMut.RUnlock()
			PruneSegments(ctx, sd.partitionStats, nil, req.GetReq(), sd.collection.Schema(), sealed, PruneInfo{paramtable.Get().QueryNodeCfg.DefaultSegmentFilterRatio.GetAsFloat()})
		}()
	}

	sealedNum := lo.SumBy(sealed, func(item SnapshotItem) int { return len(item.Segments) })
	log.Debug("query segments...",
		zap.Int("sealedNum", sealedNum),
		zap.Int("growingNum", len(growing)),
	)
	tasks, err := organizeSubTask(ctx, req, sealed, growing, sd, sd.modifyQueryRequest)
	if err != nil {
		log.Warn("query organizeSubTask failed", zap.Error(err))
		return nil, err
	}

	results, err := executeSubTasks(ctx, tasks, func(ctx context.Context, req *querypb.QueryRequest, worker cluster.Worker) (*internalpb.RetrieveResults, error) {
		return worker.QuerySegments(ctx, req)
	}, "Query", log)
	if err != nil {
		log.Warn("Delegator query failed", zap.Error(err))
		return nil, err
	}

	log.Debug("Delegator Query done")

	return results, nil
}

// GetStatistics returns statistics aggregated by delegator.
func (sd *shardDelegator) GetStatistics(ctx context.Context, req *querypb.GetStatisticsRequest) ([]*internalpb.GetStatisticsResponse, error) {
	log := sd.getLogger(ctx)
	if err := sd.lifetime.Add(lifetime.IsWorking); err != nil {
		return nil, err
	}
	defer sd.lifetime.Done()

	if !funcutil.SliceContain(req.GetDmlChannels(), sd.vchannelName) {
		log.Warn("delegator received GetStatistics request not belongs to it",
			zap.Strings("reqChannels", req.GetDmlChannels()),
		)
		return nil, fmt.Errorf("dml channel not match, delegator channel %s, GetStatistics channels %v", sd.vchannelName, req.GetDmlChannels())
	}

	// wait tsafe
	_, err := sd.waitTSafe(ctx, req.Req.GuaranteeTimestamp)
	if err != nil {
		log.Warn("delegator GetStatistics failed to wait tsafe", zap.Error(err))
		return nil, err
	}

	sealed, growing, version, err := sd.distribution.PinReadableSegments(req.Req.GetPartitionIDs()...)
	if err != nil {
		log.Warn("delegator failed to GetStatistics, current distribution is not servicable")
		return nil, merr.WrapErrChannelNotAvailable(sd.vchannelName, "distribution is not serviceable")
	}
	defer sd.distribution.Unpin(version)

	tasks, err := organizeSubTask(ctx, req, sealed, growing, sd, func(req *querypb.GetStatisticsRequest, scope querypb.DataScope, segmentIDs []int64, targetID int64) *querypb.GetStatisticsRequest {
		nodeReq := proto.Clone(req).(*querypb.GetStatisticsRequest)
		nodeReq.GetReq().GetBase().TargetID = targetID
		nodeReq.Scope = scope
		nodeReq.SegmentIDs = segmentIDs
		nodeReq.FromShardLeader = true
		return nodeReq
	})
	if err != nil {
		log.Warn("Get statistics organizeSubTask failed", zap.Error(err))
		return nil, err
	}

	results, err := executeSubTasks(ctx, tasks, func(ctx context.Context, req *querypb.GetStatisticsRequest, worker cluster.Worker) (*internalpb.GetStatisticsResponse, error) {
		return worker.GetStatistics(ctx, req)
	}, "GetStatistics", log)
	if err != nil {
		log.Warn("Delegator get statistics failed", zap.Error(err))
		return nil, err
	}

	return results, nil
}

type subTask[T any] struct {
	req      T
	targetID int64
	worker   cluster.Worker
}

func organizeSubTask[T any](ctx context.Context, req T, sealed []SnapshotItem, growing []SegmentEntry, sd *shardDelegator, modify func(T, querypb.DataScope, []int64, int64) T) ([]subTask[T], error) {
	log := sd.getLogger(ctx)
	result := make([]subTask[T], 0, len(sealed)+1)

	packSubTask := func(segments []SegmentEntry, workerID int64, scope querypb.DataScope) error {
		segmentIDs := lo.Map(segments, func(item SegmentEntry, _ int) int64 {
			return item.SegmentID
		})
		if len(segmentIDs) == 0 {
			return nil
		}
		// update request
		req := modify(req, scope, segmentIDs, workerID)

		worker, err := sd.workerManager.GetWorker(ctx, workerID)
		if err != nil {
			log.Warn("failed to get worker",
				zap.Int64("nodeID", workerID),
				zap.Error(err),
			)
			return fmt.Errorf("failed to get worker %d, %w", workerID, err)
		}

		result = append(result, subTask[T]{
			req:      req,
			targetID: workerID,
			worker:   worker,
		})
		return nil
	}

	for _, entry := range sealed {
		err := packSubTask(entry.Segments, entry.NodeID, querypb.DataScope_Historical)
		if err != nil {
			return nil, err
		}
	}

	packSubTask(growing, paramtable.GetNodeID(), querypb.DataScope_Streaming)

	return result, nil
}

func executeSubTasks[T any, R interface {
	GetStatus() *commonpb.Status
}](ctx context.Context, tasks []subTask[T], execute func(context.Context, T, cluster.Worker) (R, error), taskType string, log *log.MLogger,
) ([]R, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(len(tasks))

	resultCh := make(chan R, len(tasks))
	errCh := make(chan error, 1)
	for _, task := range tasks {
		go func(task subTask[T]) {
			defer wg.Done()
			result, err := execute(ctx, task.req, task.worker)
			if result.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
				err = fmt.Errorf("worker(%d) query failed: %s", task.targetID, result.GetStatus().GetReason())
			}
			if err != nil {
				log.Warn("failed to execute sub task",
					zap.String("taskType", taskType),
					zap.Int64("nodeID", task.targetID),
					zap.Error(err),
				)
				select {
				case errCh <- err: // must be the first
				default: // skip other errors
				}
				cancel()
				return
			}
			resultCh <- result
		}(task)
	}

	wg.Wait()
	close(resultCh)
	select {
	case err := <-errCh:
		log.Warn("Delegator execute subTask failed",
			zap.String("taskType", taskType),
			zap.Error(err),
		)
		return nil, err
	default:
	}

	results := make([]R, 0, len(tasks))
	for result := range resultCh {
		results = append(results, result)
	}
	return results, nil
}

// waitTSafe returns when tsafe listener notifies a timestamp which meet the guarantee ts.
func (sd *shardDelegator) waitTSafe(ctx context.Context, ts uint64) (uint64, error) {
	ctx, sp := otel.Tracer(typeutil.QueryNodeRole).Start(ctx, "Delegator-waitTSafe")
	defer sp.End()
	log := sd.getLogger(ctx)
	// already safe to search
	latestTSafe := sd.latestTsafe.Load()
	if latestTSafe >= ts {
		return latestTSafe, nil
	}
	// check lag duration too large
	st, _ := tsoutil.ParseTS(latestTSafe)
	gt, _ := tsoutil.ParseTS(ts)
	lag := gt.Sub(st)
	maxLag := paramtable.Get().QueryNodeCfg.MaxTimestampLag.GetAsDuration(time.Second)
	if lag > maxLag {
		log.Warn("guarantee and serviceable ts larger than MaxLag",
			zap.Time("guaranteeTime", gt),
			zap.Time("serviceableTime", st),
			zap.Duration("lag", lag),
			zap.Duration("maxTsLag", maxLag),
		)
		return 0, WrapErrTsLagTooLarge(lag, maxLag)
	}

	ch := make(chan struct{})
	go func() {
		sd.tsCond.L.Lock()
		defer sd.tsCond.L.Unlock()

		for sd.latestTsafe.Load() < ts &&
			ctx.Err() == nil &&
			sd.Serviceable() {
			sd.tsCond.Wait()
		}
		close(ch)
	}()

	for {
		select {
		// timeout
		case <-ctx.Done():
			// notify wait goroutine to quit
			sd.tsCond.Broadcast()
			return 0, ctx.Err()
		case <-ch:
			if !sd.Serviceable() {
				return 0, merr.WrapErrChannelNotAvailable(sd.vchannelName, "delegator closed during wait tsafe")
			}
			return sd.latestTsafe.Load(), nil
		}
	}
}

// watchTSafe is the worker function to update serviceable timestamp.
func (sd *shardDelegator) watchTSafe() {
	defer sd.lifetime.Done()
	listener := sd.tsafeManager.WatchChannel(sd.vchannelName)
	sd.updateTSafe()
	log := sd.getLogger(context.Background())
	for {
		select {
		case _, ok := <-listener.On():
			if !ok {
				// listener close
				log.Warn("tsafe listener closed")
				return
			}
			sd.updateTSafe()
		case <-sd.lifetime.CloseCh():
			log.Info("updateTSafe quit")
			// shard delegator closed
			return
		}
	}
}

// updateTSafe read current tsafe value from tsafeManager.
func (sd *shardDelegator) updateTSafe() {
	sd.tsCond.L.Lock()
	tsafe, err := sd.tsafeManager.Get(sd.vchannelName)
	if err != nil {
		log.Warn("tsafeManager failed to get lastest", zap.Error(err))
	}
	if tsafe > sd.latestTsafe.Load() {
		sd.latestTsafe.Store(tsafe)
		sd.tsCond.Broadcast()
	}
	sd.tsCond.L.Unlock()
}

// Close closes the delegator.
func (sd *shardDelegator) Close() {
	sd.lifetime.SetState(lifetime.Stopped)
	sd.lifetime.Close()
	// broadcast to all waitTsafe goroutine to quit
	sd.tsCond.Broadcast()
	sd.lifetime.Wait()
}

// As partition stats is an optimization for search/query which is not mandatory for milvus instance,
// loading partitionStats will be a try-best process and will skip+logError when running across errors rather than
// return an error status
func (sd *shardDelegator) loadPartitionStats(ctx context.Context, partStatsVersions map[int64]int64) {
	colID := sd.Collection()
	log := log.Ctx(ctx)
	for partID, newVersion := range partStatsVersions {
		var curStats *storage.PartitionStatsSnapshot
		var exist bool
		func() {
			sd.partitionStatsMut.RLock()
			defer sd.partitionStatsMut.RUnlock()
			curStats, exist = sd.partitionStats[partID]
		}()
		if exist && curStats != nil && curStats.Version >= newVersion {
			log.RatedWarn(60, "Input partition stats' version is less or equal than current partition stats, skip",
				zap.Int64("partID", partID),
				zap.Int64("curVersion", curStats.Version),
				zap.Int64("inputVersion", newVersion),
			)
			continue
		}
		idPath := metautil.JoinIDPath(colID, partID)
		idPath = path.Join(idPath, sd.vchannelName)
		statsFilePath := path.Join(sd.chunkManager.RootPath(), common.PartitionStatsPath, idPath, strconv.FormatInt(newVersion, 10))
		statsBytes, err := sd.chunkManager.Read(ctx, statsFilePath)
		if err != nil {
			log.Error("failed to read stats file from object storage", zap.String("path", statsFilePath))
			continue
		}
		partStats, err := storage.DeserializePartitionsStatsSnapshot(statsBytes)
		if err != nil {
			log.Error("failed to parse partition stats from bytes",
				zap.Int("bytes_length", len(statsBytes)), zap.Error(err))
			continue
		}
		partStats.SetVersion(newVersion)
		func() {
			sd.partitionStatsMut.Lock()
			defer sd.partitionStatsMut.Unlock()
			sd.partitionStats[partID] = partStats
		}()
		log.Info("Updated partitionStats for partition", zap.Int64("collectionID", sd.collectionID), zap.Int64("partitionID", partID),
			zap.Int64("newVersion", newVersion), zap.Int64("oldVersion", curStats.GetVersion()))
	}
}

// NewShardDelegator creates a new ShardDelegator instance with all fields initialized.
func NewShardDelegator(ctx context.Context, collectionID UniqueID, replicaID UniqueID, channel string, version int64,
	workerManager cluster.Manager, manager *segments.Manager, tsafeManager tsafe.Manager, loader segments.Loader,
	factory msgstream.Factory, startTs uint64, queryHook optimizers.QueryHook, chunkManager storage.ChunkManager,
) (ShardDelegator, error) {
	log := log.Ctx(ctx).With(zap.Int64("collectionID", collectionID),
		zap.Int64("replicaID", replicaID),
		zap.String("channel", channel),
		zap.Int64("version", version),
		zap.Uint64("startTs", startTs),
	)

	collection := manager.Collection.Get(collectionID)
	if collection == nil {
		return nil, fmt.Errorf("collection(%d) not found in manager", collectionID)
	}

	sizePerBlock := paramtable.Get().QueryNodeCfg.DeleteBufferBlockSize.GetAsInt64()
	log.Info("Init delete cache with list delete buffer", zap.Int64("sizePerBlock", sizePerBlock), zap.Time("startTime", tsoutil.PhysicalTime(startTs)))

	excludedSegments := NewExcludedSegments(paramtable.Get().QueryNodeCfg.CleanExcludeSegInterval.GetAsDuration(time.Second))

	sd := &shardDelegator{
		collectionID:     collectionID,
		replicaID:        replicaID,
		vchannelName:     channel,
		version:          version,
		collection:       collection,
		segmentManager:   manager.Segment,
		workerManager:    workerManager,
		lifetime:         lifetime.NewLifetime(lifetime.Initializing),
		distribution:     NewDistribution(),
		deleteBuffer:     deletebuffer.NewListDeleteBuffer[*deletebuffer.Item](startTs, sizePerBlock),
		pkOracle:         pkoracle.NewPkOracle(),
		tsafeManager:     tsafeManager,
		latestTsafe:      atomic.NewUint64(startTs),
		loader:           loader,
		factory:          factory,
		queryHook:        queryHook,
		chunkManager:     chunkManager,
		partitionStats:   make(map[UniqueID]*storage.PartitionStatsSnapshot),
		excludedSegments: excludedSegments,
	}
	m := sync.Mutex{}
	sd.tsCond = sync.NewCond(&m)
	if sd.lifetime.Add(lifetime.NotStopped) == nil {
		go sd.watchTSafe()
	}
	log.Info("finish build new shardDelegator")
	return sd, nil
}
