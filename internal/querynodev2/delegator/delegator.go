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
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.opentelemetry.io/otel"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/querynodev2/cluster"
	"github.com/milvus-io/milvus/internal/querynodev2/delegator/deletebuffer"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/internal/util/reduce"
	"github.com/milvus-io/milvus/internal/util/searchutil/optimizers"
	"github.com/milvus-io/milvus/internal/util/streamrpc"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/conc"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/lifetime"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metautil"
	"github.com/milvus-io/milvus/pkg/v2/util/metric"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
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
	UpdateSchema(ctx context.Context, sch *schemapb.CollectionSchema, version uint64) error

	// data
	ProcessInsert(insertRecords map[int64]*InsertData)
	ProcessDelete(deleteData []*DeleteData, ts uint64)
	LoadGrowing(ctx context.Context, infos []*querypb.SegmentLoadInfo, version int64) error
	LoadL0(ctx context.Context, infos []*querypb.SegmentLoadInfo, version int64) error
	LoadSegments(ctx context.Context, req *querypb.LoadSegmentsRequest) error
	ReleaseSegments(ctx context.Context, req *querypb.ReleaseSegmentsRequest, force bool) error
	SyncTargetVersion(action *querypb.SyncAction, partitions []int64)
	GetChannelQueryView() *channelQueryView
	GetDeleteBufferSize() (entryNum int64, memorySize int64)
	DropIndex(ctx context.Context, req *querypb.DropIndexRequest) error

	// manage exclude segments
	AddExcludedSegments(excludeInfo map[int64]uint64)
	VerifyExcludedSegments(segmentID int64, ts uint64) bool
	TryCleanExcludedSegments(ts uint64)

	// tsafe
	GetLatestRequiredMVCCTimeTick() uint64
	UpdateTSafe(ts uint64)
	GetTSafe() uint64

	// analyzer
	RunAnalyzer(ctx context.Context, req *querypb.RunAnalyzerRequest) ([]*milvuspb.AnalyzerResult, error)
	GetHighlight(ctx context.Context, req *querypb.GetHighlightRequest) ([]*querypb.HighlightResult, error)

	// control
	Serviceable() bool
	CatchingUpStreamingData() bool
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

	distribution *distribution
	idfOracle    IDFOracle

	segmentManager segments.SegmentManager
	// stream delete buffer
	deleteMut    sync.RWMutex
	deleteBuffer deletebuffer.DeleteBuffer[*deletebuffer.Item]

	sf          conc.Singleflight[struct{}]
	loader      segments.Loader
	tsCond      *sync.Cond
	latestTsafe *atomic.Uint64
	// queryHook
	queryHook      optimizers.QueryHook
	partitionStats map[UniqueID]*storage.PartitionStatsSnapshot
	chunkManager   storage.ChunkManager

	excludedSegments *ExcludedSegments
	// cause growing segment meta has been stored in segmentManager/distribution/excludeSegments
	// in order to make add/remove growing be atomic, need lock before modify these meta info
	growingSegmentLock sync.RWMutex
	partitionStatsMut  sync.RWMutex

	// outputFieldId -> functionRunner map for search function field
	functionRunners map[UniqueID]function.FunctionRunner
	isBM25Field     map[UniqueID]bool

	// analyzerFieldID -> analyzerRunner map for run analyzer.
	analyzerRunners map[UniqueID]function.Analyzer

	// current forward policy
	l0ForwardPolicy string

	// schema version
	schemaChangeMutex sync.RWMutex
	schemaVersion     uint64

	// streaming data catch-up state
	catchingUpStreamingData *atomic.Bool

	// latest required mvcc timestamp for the delegator
	// for slow down the delegator consumption and reduce the timetick dispatch frequency.
	latestRequiredMVCCTimeTick *atomic.Uint64
}

// getLogger returns the zap logger with pre-defined shard attributes.
func (sd *shardDelegator) getLogger(ctx context.Context) *log.MLogger {
	return log.Ctx(ctx).With(
		zap.Int64("collectionID", sd.collectionID),
		zap.String("channel", sd.vchannelName),
		zap.Int64("replicaID", sd.replicaID),
	)
}

func (sd *shardDelegator) NotStopped(state lifetime.State) error {
	if state != lifetime.Stopped {
		return nil
	}
	return merr.WrapErrChannelNotAvailable(sd.vchannelName, fmt.Sprintf("delegator is not ready, state: %s", state.String()))
}

func (sd *shardDelegator) IsWorking(state lifetime.State) error {
	if state == lifetime.Working {
		return nil
	}
	return merr.WrapErrChannelNotAvailable(sd.vchannelName, fmt.Sprintf("delegator is not ready, state: %s", state.String()))
}

// Serviceable returns whether delegator is serviceable now.
func (sd *shardDelegator) Serviceable() bool {
	return sd.IsWorking(sd.lifetime.GetState()) == nil
}

func (sd *shardDelegator) Stopped() bool {
	return sd.NotStopped(sd.lifetime.GetState()) != nil
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

func (sd *shardDelegator) shallowCopySearchRequest(req *internalpb.SearchRequest, targetID int64) *internalpb.SearchRequest {
	// Create a new SearchRequest with the same fields
	nodeReq := &internalpb.SearchRequest{
		Base:                    &commonpb.MsgBase{TargetID: targetID},
		ReqID:                   req.ReqID,
		DbID:                    req.DbID,
		CollectionID:            req.CollectionID,
		PartitionIDs:            req.PartitionIDs, // Shallow copy: Same underlying slice
		Dsl:                     req.Dsl,
		PlaceholderGroup:        req.PlaceholderGroup, // Shallow copy: Same underlying byte slice
		DslType:                 req.DslType,
		SerializedExprPlan:      req.SerializedExprPlan, // Shallow copy: Same underlying byte slice
		OutputFieldsId:          req.OutputFieldsId,     // Shallow copy: Same underlying slice
		MvccTimestamp:           req.MvccTimestamp,
		GuaranteeTimestamp:      req.GuaranteeTimestamp,
		TimeoutTimestamp:        req.TimeoutTimestamp,
		Nq:                      req.Nq,
		Topk:                    req.Topk,
		MetricType:              req.MetricType,
		IgnoreGrowing:           req.IgnoreGrowing,
		Username:                req.Username,
		SubReqs:                 req.SubReqs, // Shallow copy: Same underlying slice of pointers
		IsAdvanced:              req.IsAdvanced,
		Offset:                  req.Offset,
		ConsistencyLevel:        req.ConsistencyLevel,
		GroupByFieldId:          req.GroupByFieldId,
		GroupSize:               req.GroupSize,
		FieldId:                 req.FieldId,
		IsTopkReduce:            req.IsTopkReduce,
		IsRecallEvaluation:      req.IsRecallEvaluation,
		CollectionTtlTimestamps: req.CollectionTtlTimestamps,
	}

	return nodeReq
}

func (sd *shardDelegator) modifySearchRequest(req *querypb.SearchRequest, scope querypb.DataScope, segmentIDs []int64, targetID int64) *querypb.SearchRequest {
	nodeReq := &querypb.SearchRequest{
		DmlChannels:     []string{sd.vchannelName},
		SegmentIDs:      segmentIDs,
		Scope:           scope,
		Req:             sd.shallowCopySearchRequest(req.GetReq(), targetID),
		FromShardLeader: req.FromShardLeader,
		TotalChannelNum: req.TotalChannelNum,
	}
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
func (sd *shardDelegator) search(ctx context.Context, req *querypb.SearchRequest, sealed []SnapshotItem, growing []SegmentEntry, sealedRowCount map[int64]int64) ([]*internalpb.SearchResults, error) {
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

	searchAgainstBM25Field := sd.isBM25Field[req.GetReq().GetFieldId()]

	if searchAgainstBM25Field {
		if req.GetReq().GetMetricType() != metric.BM25 && req.GetReq().GetMetricType() != metric.EMPTY {
			return nil, merr.WrapErrParameterInvalid("BM25", req.GetReq().GetMetricType(), "must use BM25 metric type when searching against BM25 Function output field")
		}
		// build idf for bm25 search
		avgdl, err := sd.buildBM25IDF(req.GetReq())
		if err != nil {
			return nil, err
		}

		if avgdl <= 0 {
			log.Warn("search bm25 from empty data, skip search", zap.String("channel", sd.vchannelName), zap.Float64("avgdl", avgdl))
			return []*internalpb.SearchResults{}, nil
		}
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
	tasks, err := organizeSubTask(ctx, req, sealed, growing, sd, true, sd.modifySearchRequest)
	if err != nil {
		log.Warn("Search organizeSubTask failed", zap.Error(err))
		return nil, err
	}
	results, err := executeSubTasks(ctx, tasks, NewRowCountBasedEvaluator(sealedRowCount), func(ctx context.Context, req *querypb.SearchRequest, worker cluster.Worker) (*internalpb.SearchResults, error) {
		resp, err := worker.SearchSegments(ctx, req)
		status, ok := status.FromError(err)
		if ok && status.Code() == codes.Unavailable {
			sd.markSegmentOffline(req.GetSegmentIDs()...)
		}
		return resp, err
	}, "Search", log)
	if err != nil {
		log.Warn("Delegator search failed", zap.Error(err))
		return nil, err
	}

	log.Debug("Delegator search done", zap.Int("results", len(results)))

	return results, nil
}

// Search preforms search operation on shard.
func (sd *shardDelegator) Search(ctx context.Context, req *querypb.SearchRequest) ([]*internalpb.SearchResults, error) {
	log := sd.getLogger(ctx)
	if err := sd.lifetime.Add(sd.IsWorking); err != nil {
		return nil, err
	}
	defer sd.lifetime.Done()

	if !funcutil.SliceContain(req.GetDmlChannels(), sd.vchannelName) {
		log.Warn("delegator received search request not belongs to it",
			zap.Strings("reqChannels", req.GetDmlChannels()),
		)
		return nil, fmt.Errorf("dml channel not match, delegator channel %s, search channels %v", sd.vchannelName, req.GetDmlChannels())
	}

	req.Req.GuaranteeTimestamp = sd.speedupGuranteeTS(
		ctx,
		req.Req.GetConsistencyLevel(),
		req.Req.GetGuaranteeTimestamp(),
		req.Req.GetMvccTimestamp(),
		req.Req.GetIsIterator(),
	)

	partialResultRequiredDataRatio := paramtable.Get().QueryNodeCfg.PartialResultRequiredDataRatio.GetAsFloat()
	// wait tsafe
	waitTr := timerecord.NewTimeRecorder("wait tSafe")
	var tSafe uint64
	var err error
	if partialResultRequiredDataRatio >= 1.0 {
		tSafe, err = sd.waitTSafe(ctx, req.Req.GuaranteeTimestamp)
		if err != nil {
			log.Warn("delegator search failed to wait tsafe", zap.Error(err))
			return nil, err
		}
		if req.GetReq().GetMvccTimestamp() == 0 {
			req.Req.MvccTimestamp = tSafe
		}
	} else {
		tSafe = sd.GetTSafe()
		if req.GetReq().GetMvccTimestamp() == 0 {
			req.Req.MvccTimestamp = tSafe
		}
	}

	metrics.QueryNodeSQLatencyWaitTSafe.WithLabelValues(
		paramtable.GetStringNodeID(), metrics.SearchLabel).
		Observe(float64(waitTr.ElapseSpan().Milliseconds()))

	sealed, growing, sealedRowCount, version, err := sd.distribution.PinReadableSegments(partialResultRequiredDataRatio, req.GetReq().GetPartitionIDs()...)
	if err != nil {
		log.Warn("delegator failed to search, current distribution is not serviceable", zap.Error(err))
		return nil, err
	}
	defer sd.distribution.Unpin(version)

	if req.GetReq().GetIsAdvanced() {
		futures := make([]*conc.Future[*internalpb.SearchResults], len(req.GetReq().GetSubReqs()))
		for index, subReq := range req.GetReq().GetSubReqs() {
			newRequest := &internalpb.SearchRequest{
				Base:                    req.GetReq().GetBase(),
				ReqID:                   req.GetReq().GetReqID(),
				DbID:                    req.GetReq().GetDbID(),
				CollectionID:            req.GetReq().GetCollectionID(),
				PartitionIDs:            subReq.GetPartitionIDs(),
				Dsl:                     subReq.GetDsl(),
				PlaceholderGroup:        subReq.GetPlaceholderGroup(),
				DslType:                 subReq.GetDslType(),
				SerializedExprPlan:      subReq.GetSerializedExprPlan(),
				OutputFieldsId:          req.GetReq().GetOutputFieldsId(),
				MvccTimestamp:           req.GetReq().GetMvccTimestamp(),
				GuaranteeTimestamp:      req.GetReq().GetGuaranteeTimestamp(),
				TimeoutTimestamp:        req.GetReq().GetTimeoutTimestamp(),
				Nq:                      subReq.GetNq(),
				Topk:                    subReq.GetTopk(),
				MetricType:              subReq.GetMetricType(),
				IgnoreGrowing:           subReq.GetIgnoreGrowing(),
				Username:                req.GetReq().GetUsername(),
				IsAdvanced:              false,
				GroupByFieldId:          subReq.GetGroupByFieldId(),
				GroupSize:               subReq.GetGroupSize(),
				FieldId:                 subReq.GetFieldId(),
				IsTopkReduce:            req.GetReq().GetIsTopkReduce(),
				IsIterator:              req.GetReq().GetIsIterator(),
				CollectionTtlTimestamps: req.GetReq().GetCollectionTtlTimestamps(),
				AnalyzerName:            subReq.GetAnalyzerName(),
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
				searchReq.Req.CollectionTtlTimestamps = req.GetReq().GetCollectionTtlTimestamps()
				results, err := sd.search(ctx, searchReq, sealed, growing, sealedRowCount)
				if err != nil {
					return nil, err
				}

				return segments.ReduceSearchOnQueryNode(ctx,
					results,
					reduce.NewReduceSearchResultInfo(searchReq.GetReq().GetNq(),
						searchReq.GetReq().GetTopk()).WithMetricType(searchReq.GetReq().GetMetricType()).
						WithGroupByField(searchReq.GetReq().GetGroupByFieldId()).
						WithGroupSize(searchReq.GetReq().GetGroupSize()))
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
		return results, nil
	}
	return sd.search(ctx, req, sealed, growing, sealedRowCount)
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

	req.Req.GuaranteeTimestamp = sd.speedupGuranteeTS(
		ctx,
		req.Req.GetConsistencyLevel(),
		req.Req.GetGuaranteeTimestamp(),
		req.Req.GetMvccTimestamp(),
		req.Req.GetIsIterator(),
	)

	// wait tsafe
	waitTr := timerecord.NewTimeRecorder("wait tSafe")
	tSafe, err := sd.waitTSafe(ctx, req.Req.GetGuaranteeTimestamp())
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

	sealed, growing, sealedRowCount, version, err := sd.distribution.PinReadableSegments(float64(1.0), req.GetReq().GetPartitionIDs()...)
	if err != nil {
		log.Warn("delegator failed to query, current distribution is not serviceable", zap.Error(err))
		return err
	}
	defer sd.distribution.Unpin(version)

	if req.Req.IgnoreGrowing {
		growing = []SegmentEntry{}
	}

	log.Info("query stream segments...",
		zap.Int("sealedNum", len(sealed)),
		zap.Int("growingNum", len(growing)),
	)
	tasks, err := organizeSubTask(ctx, req, sealed, growing, sd, true, sd.modifyQueryRequest)
	if err != nil {
		log.Warn("query organizeSubTask failed", zap.Error(err))
		return err
	}

	_, err = executeSubTasks(ctx, tasks, NewRowCountBasedEvaluator(sealedRowCount), func(ctx context.Context, req *querypb.QueryRequest, worker cluster.Worker) (*internalpb.RetrieveResults, error) {
		err := worker.QueryStreamSegments(ctx, req, srv)
		status, ok := status.FromError(err)
		if ok && status.Code() == codes.Unavailable {
			sd.markSegmentOffline(req.GetSegmentIDs()...)
		}
		return nil, err
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
	if err := sd.lifetime.Add(sd.IsWorking); err != nil {
		return nil, err
	}
	defer sd.lifetime.Done()

	if !funcutil.SliceContain(req.GetDmlChannels(), sd.vchannelName) {
		log.Warn("delegator received query request not belongs to it",
			zap.Strings("reqChannels", req.GetDmlChannels()),
		)
		return nil, fmt.Errorf("dml channel not match, delegator channel %s, search channels %v", sd.vchannelName, req.GetDmlChannels())
	}

	req.Req.GuaranteeTimestamp = sd.speedupGuranteeTS(
		ctx,
		req.Req.GetConsistencyLevel(),
		req.Req.GetGuaranteeTimestamp(),
		req.Req.GetMvccTimestamp(),
		req.Req.GetIsIterator(),
	)

	partialResultRequiredDataRatio := paramtable.Get().QueryNodeCfg.PartialResultRequiredDataRatio.GetAsFloat()
	// wait tsafe
	waitTr := timerecord.NewTimeRecorder("wait tSafe")
	var tSafe uint64
	var err error
	if partialResultRequiredDataRatio >= 1.0 {
		tSafe, err = sd.waitTSafe(ctx, req.Req.GuaranteeTimestamp)
		if err != nil {
			log.Warn("delegator search failed to wait tsafe", zap.Error(err))
			return nil, err
		}
		if req.GetReq().GetMvccTimestamp() == 0 {
			req.Req.MvccTimestamp = tSafe
		}
	} else {
		if req.GetReq().GetMvccTimestamp() == 0 {
			req.Req.MvccTimestamp = sd.GetTSafe()
		}
	}

	metrics.QueryNodeSQLatencyWaitTSafe.WithLabelValues(
		fmt.Sprint(paramtable.GetNodeID()), metrics.QueryLabel).
		Observe(float64(waitTr.ElapseSpan().Milliseconds()))

	sealed, growing, sealedRowCount, version, err := sd.distribution.PinReadableSegments(partialResultRequiredDataRatio, req.GetReq().GetPartitionIDs()...)
	if err != nil {
		log.Warn("delegator failed to query, current distribution is not serviceable", zap.Error(err))
		return nil, err
	}
	defer sd.distribution.Unpin(version)

	if req.Req.IgnoreGrowing {
		growing = []SegmentEntry{}
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
		zap.Uint64("mvcc", req.GetReq().GetMvccTimestamp()),
		zap.Int("sealedNum", sealedNum),
		zap.Int("growingNum", len(growing)),
	)
	tasks, err := organizeSubTask(ctx, req, sealed, growing, sd, true, sd.modifyQueryRequest)
	if err != nil {
		log.Warn("query organizeSubTask failed", zap.Error(err))
		return nil, err
	}

	results, err := executeSubTasks(ctx, tasks, NewRowCountBasedEvaluator(sealedRowCount), func(ctx context.Context, req *querypb.QueryRequest, worker cluster.Worker) (*internalpb.RetrieveResults, error) {
		resp, err := worker.QuerySegments(ctx, req)
		status, ok := status.FromError(err)
		if ok && status.Code() == codes.Unavailable {
			sd.markSegmentOffline(req.GetSegmentIDs()...)
		}
		return resp, err
	}, "Query", log)
	if err != nil {
		log.Warn("Delegator query failed", zap.Error(err))
		return nil, err
	}

	log.Debug("Delegator Query done")
	if log.Core().Enabled(zap.DebugLevel) {
		sealedIDs := lo.FlatMap(sealed, func(item SnapshotItem, _ int) []int64 {
			return lo.Map(item.Segments, func(segment SegmentEntry, _ int) int64 {
				return segment.SegmentID
			})
		})
		slices.Sort(sealedIDs)
		growingIDs := lo.Map(growing, func(item SegmentEntry, _ int) int64 {
			return item.SegmentID
		})
		slices.Sort(growingIDs)
		log.Debug("execute count on segments...",
			zap.Int64s("sealedIDs", sealedIDs),
			zap.Int64s("growingIDs", growingIDs),
		)
	}

	return results, nil
}

// GetStatistics returns statistics aggregated by delegator.
func (sd *shardDelegator) GetStatistics(ctx context.Context, req *querypb.GetStatisticsRequest) ([]*internalpb.GetStatisticsResponse, error) {
	log := sd.getLogger(ctx)
	if err := sd.lifetime.Add(sd.IsWorking); err != nil {
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
	sd.updateLatestRequiredMVCCTimestamp(req.Req.GuaranteeTimestamp)
	_, err := sd.waitTSafe(ctx, req.Req.GuaranteeTimestamp)
	if err != nil {
		log.Warn("delegator GetStatistics failed to wait tsafe", zap.Error(err))
		return nil, err
	}

	sealed, growing, sealedRowCount, version, err := sd.distribution.PinReadableSegments(1.0, req.Req.GetPartitionIDs()...)
	if err != nil {
		log.Warn("delegator failed to GetStatistics, current distribution is not servicable")
		return nil, merr.WrapErrChannelNotAvailable(sd.vchannelName, "distribution is not serviceable")
	}
	defer sd.distribution.Unpin(version)

	tasks, err := organizeSubTask(ctx, req, sealed, growing, sd, true, func(req *querypb.GetStatisticsRequest, scope querypb.DataScope, segmentIDs []int64, targetID int64) *querypb.GetStatisticsRequest {
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

	results, err := executeSubTasks(ctx, tasks, NewRowCountBasedEvaluator(sealedRowCount), func(ctx context.Context, req *querypb.GetStatisticsRequest, worker cluster.Worker) (*internalpb.GetStatisticsResponse, error) {
		return worker.GetStatistics(ctx, req)
	}, "GetStatistics", log)
	if err != nil {
		log.Warn("Delegator get statistics failed", zap.Error(err))
		return nil, err
	}

	return results, nil
}

func (sd *shardDelegator) GetDeleteBufferSize() (entryNum int64, memorySize int64) {
	return sd.deleteBuffer.Size()
}

type subTask[T any] struct {
	req      T
	targetID int64
	worker   cluster.Worker
}

func organizeSubTask[T any](ctx context.Context,
	req T,
	sealed []SnapshotItem,
	growing []SegmentEntry,
	sd *shardDelegator,
	skipEmpty bool,
	modify func(T, querypb.DataScope, []int64, int64) T,
) ([]subTask[T], error) {
	log := sd.getLogger(ctx)
	result := make([]subTask[T], 0, len(sealed)+1)

	packSubTask := func(segments []SegmentEntry, workerID int64, scope querypb.DataScope) error {
		segmentIDs := lo.Map(segments, func(item SegmentEntry, _ int) int64 {
			return item.SegmentID
		})
		if skipEmpty && len(segmentIDs) == 0 {
			return nil
		}
		// update request
		req := modify(req, scope, segmentIDs, workerID)

		// for partial search, tolerate some worker are offline
		worker, err := sd.workerManager.GetWorker(ctx, workerID)
		if err != nil {
			log.Warn("failed to get worker for sub task",
				zap.Int64("nodeID", workerID),
				zap.Int64s("segments", segmentIDs),
				zap.Error(err),
			)
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
}](ctx context.Context, tasks []subTask[T], evaluator PartialResultEvaluator, execute func(context.Context, T, cluster.Worker) (R, error), taskType string, log *log.MLogger,
) ([]R, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var partialResultRequiredDataRatio float64
	if taskType == "Query" || taskType == "Search" {
		partialResultRequiredDataRatio = paramtable.Get().QueryNodeCfg.PartialResultRequiredDataRatio.GetAsFloat()
	} else {
		partialResultRequiredDataRatio = 1.0
	}

	wg, ctx := errgroup.WithContext(ctx)
	type channelResult struct {
		nodeID   int64
		segments []int64
		result   R
		err      error
	}
	// Buffered channel to collect results from all goroutines
	resultCh := make(chan channelResult, len(tasks))
	for _, task := range tasks {
		task := task // capture loop variable
		wg.Go(func() error {
			var result R
			var err error
			if task.targetID == -1 || task.worker == nil {
				var segments []int64
				if req, ok := any(task.req).(interface{ GetSegmentIDs() []int64 }); ok {
					segments = req.GetSegmentIDs()
				} else {
					segments = []int64{}
				}
				err = fmt.Errorf("segments not loaded in any worker: %v", segments[:min(len(segments), 10)])
			} else {
				result, err = execute(ctx, task.req, task.worker)
				if result.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
					err = fmt.Errorf("worker(%d) query failed: %s", task.targetID, result.GetStatus().GetReason())
				}
			}

			if err != nil {
				log.Warn("failed to execute sub task",
					zap.String("taskType", taskType),
					zap.Int64("nodeID", task.targetID),
					zap.Error(err),
				)
				// check if partial result is disabled, if so, let all sub tasks fail fast
				if partialResultRequiredDataRatio == 1 {
					return err
				}
			}

			taskResult := channelResult{
				nodeID: task.targetID,
				result: result,
				err:    err,
			}
			if req, ok := any(task.req).(interface{ GetSegmentIDs() []int64 }); ok {
				taskResult.segments = req.GetSegmentIDs()
			}
			resultCh <- taskResult
			return nil
		})
	}

	// Wait for all tasks to complete
	if err := wg.Wait(); err != nil {
		log.Warn("some tasks failed to complete",
			zap.String("taskType", taskType),
			zap.Error(err),
		)
		return nil, err
	}
	close(resultCh)

	successSegmentList := typeutil.NewSet[int64]()
	failureSegmentList := make([]int64, 0)
	var errors []error

	// Collect results
	results := make([]R, 0, len(tasks))
	for item := range resultCh {
		if item.err == nil {
			successSegmentList.Insert(item.segments...)
			results = append(results, item.result)
		} else {
			failureSegmentList = append(failureSegmentList, item.segments...)
			errors = append(errors, item.err)
		}
	}

	if len(errors) == 0 {
		return results, nil
	}

	// Use evaluator to determine if partial results should be returned
	if evaluator != nil {
		shouldReturnPartial, accessedDataRatio := evaluator(taskType, successSegmentList, failureSegmentList, errors)
		if shouldReturnPartial {
			log.Info("partial result executed successfully",
				zap.String("taskType", taskType),
				zap.Float64("accessedDataRatio", accessedDataRatio),
				zap.Int64s("failureSegmentList", failureSegmentList),
			)
			return results, nil
		}
	}

	return nil, merr.Combine(errors...)
}

// speedupGuranteeTS returns the guarantee timestamp for strong consistency search.
// TODO: we just make a speedup right now, but in the future, we will make the mvcc and guarantee timestamp same.
func (sd *shardDelegator) speedupGuranteeTS(
	ctx context.Context,
	cl commonpb.ConsistencyLevel,
	guaranteeTS uint64,
	mvccTS uint64,
	isIterator bool,
) uint64 {
	// because the mvcc speed up will make the guarantee timestamp smaller.
	// and the update latest required mvcc timestamp and mvcc speed up are executed concurrently.
	// so we update the latest required mvcc timestamp first, then the mvcc speed up will not affect the latest required mvcc timestamp.
	// to make the new incoming mvcc can be seen by the timetick_slowdowner.
	sd.updateLatestRequiredMVCCTimestamp(guaranteeTS)

	// when 1. streaming service is disable,
	// 2. consistency level is not strong,
	// 3. cannot speed iterator, because current client of milvus doesn't support shard level mvcc.
	if isIterator || cl != commonpb.ConsistencyLevel_Strong || mvccTS != 0 {
		return guaranteeTS
	}
	// use the mvcc timestamp of the wal as the guarantee timestamp to make fast strong consistency search.
	if mvcc, err := streaming.WAL().Local().GetLatestMVCCTimestampIfLocal(ctx, sd.vchannelName); err == nil && mvcc < guaranteeTS {
		return mvcc
	}
	return guaranteeTS
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
	// check whethertsafe downgraded
	if paramtable.Get().QueryNodeCfg.DowngradeTsafe.GetAsBool() {
		log.WithRateGroup("downgradeTsafe", 1, 60).RatedWarn(10, "downgrade tsafe", zap.Uint64("latestTSafe", latestTSafe), zap.Uint64("ts", ts))
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

// GetLatestRequiredMVCCTimeTick returns the latest required mvcc timestamp for the delegator.
func (sd *shardDelegator) GetLatestRequiredMVCCTimeTick() uint64 {
	if sd.catchingUpStreamingData.Load() {
		// delegator need to catch up the streaming data when startup,
		// If the empty timetick is filtered, the load operation will be blocked.
		// We want the delegator to catch up the streaming data, and load done as soon as possible,
		// so we always return the current time as the latest required mvcc timestamp.
		return tsoutil.GetCurrentTime()
	}
	return sd.latestRequiredMVCCTimeTick.Load()
}

// updateLatestRequiredMVCCTimestamp updates the latest required mvcc timestamp for the delegator.
func (sd *shardDelegator) updateLatestRequiredMVCCTimestamp(ts uint64) {
	for {
		previousTs := sd.latestRequiredMVCCTimeTick.Load()
		if ts <= previousTs {
			return
		}
		if sd.latestRequiredMVCCTimeTick.CompareAndSwap(previousTs, ts) {
			return
		}
	}
}

// updateTSafe read current tsafe value from tsafeManager.
func (sd *shardDelegator) UpdateTSafe(tsafe uint64) {
	log := sd.getLogger(context.Background()).WithRateGroup(fmt.Sprintf("UpdateTSafe-%s", sd.vchannelName), 1, 60)
	sd.tsCond.L.Lock()
	if tsafe > sd.latestTsafe.Load() {
		sd.latestTsafe.Store(tsafe)
		sd.tsCond.Broadcast()

		// Check if caught up with streaming data
		if sd.catchingUpStreamingData.Load() {
			lagThreshold := paramtable.Get().QueryNodeCfg.CatchUpStreamingDataTsLag.GetAsDurationByParse()
			if lagThreshold > 0 {
				tsafeTime := tsoutil.PhysicalTime(tsafe)
				lag := time.Since(tsafeTime)
				caughtUp := lag <= lagThreshold
				log.RatedInfo(10, "delegator catching up streaming data progress",
					zap.String("channel", sd.vchannelName),
					zap.Duration("lag", lag),
					zap.Duration("threshold", lagThreshold),
					zap.Bool("caughtUp", caughtUp))
				if caughtUp {
					sd.catchingUpStreamingData.Store(false)
				}
			}
		}
	}
	sd.tsCond.L.Unlock()
}

func (sd *shardDelegator) GetTSafe() uint64 {
	return sd.latestTsafe.Load()
}

// CatchingUpStreamingData returns true if delegator is still catching up with streaming data.
func (sd *shardDelegator) CatchingUpStreamingData() bool {
	return sd.catchingUpStreamingData.Load()
}

func (sd *shardDelegator) UpdateSchema(ctx context.Context, schema *schemapb.CollectionSchema, schVersion uint64) error {
	log := sd.getLogger(ctx)
	if err := sd.lifetime.Add(sd.IsWorking); err != nil {
		return err
	}
	defer sd.lifetime.Done()

	log.Info("delegator received update schema event")

	sd.schemaChangeMutex.Lock()
	defer sd.schemaChangeMutex.Unlock()

	// set updated schema version as load barrier
	// prevent concurrent load segment with old schema
	sd.schemaVersion = schVersion

	sealed, growing, version := sd.distribution.PinOnlineSegments()
	defer sd.distribution.Unpin(version)

	log.Info("update schema targets...",
		zap.Int("sealedNum", len(sealed)),
		zap.Int("growingNum", len(growing)),
	)

	tasks, err := organizeSubTask(ctx, &querypb.UpdateSchemaRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithSourceID(paramtable.GetNodeID()),
		),
		CollectionID: sd.collectionID,
		Schema:       schema,
		Version:      schVersion,
	},
		sealed,
		growing,
		sd,
		false, // don't skip empty
		func(req *querypb.UpdateSchemaRequest, scope querypb.DataScope, segmentIDs []int64, targetID int64) *querypb.UpdateSchemaRequest {
			nodeReq := typeutil.Clone(req)
			nodeReq.GetBase().TargetID = targetID
			return nodeReq
		})
	if err != nil {
		return err
	}

	_, err = executeSubTasks(ctx, tasks, nil, func(ctx context.Context, req *querypb.UpdateSchemaRequest, worker cluster.Worker) (*StatusWrapper, error) {
		status, err := worker.UpdateSchema(ctx, req)
		return (*StatusWrapper)(status), err
	}, "UpdateSchema", log)

	return err
}

type StatusWrapper commonpb.Status

func (w *StatusWrapper) GetStatus() *commonpb.Status {
	return (*commonpb.Status)(w)
}

// Close closes the delegator.
func (sd *shardDelegator) Close() {
	sd.lifetime.SetState(lifetime.Stopped)
	sd.lifetime.Close()
	// broadcast to all waitTsafe goroutine to quit
	sd.tsCond.Broadcast()
	sd.lifetime.Wait()

	// Refund all sealed segment candidates in distribution
	sd.distribution.RefundAllCandidates()

	// clean idf oracle
	if sd.idfOracle != nil {
		sd.idfOracle.Close()
	}

	if sd.functionRunners != nil {
		for _, function := range sd.functionRunners {
			function.Close()
		}
	}

	// clean up l0 segment in delete buffer
	start := time.Now()
	sd.deleteBuffer.Clear()
	log.Info("unregister all l0 segments", zap.Duration("cost", time.Since(start)))

	metrics.QueryNodeDeleteBufferSize.DeleteLabelValues(fmt.Sprint(paramtable.GetNodeID()), sd.vchannelName)
	metrics.QueryNodeDeleteBufferRowNum.DeleteLabelValues(fmt.Sprint(paramtable.GetNodeID()), sd.vchannelName)
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
	workerManager cluster.Manager, manager *segments.Manager, loader segments.Loader, startTs uint64, queryHook optimizers.QueryHook, chunkManager storage.ChunkManager,
	queryView *channelQueryView,
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

	policy := paramtable.Get().QueryNodeCfg.LevelZeroForwardPolicy.GetValue()
	log.Info("shard delegator setup l0 forward policy", zap.String("policy", policy))

	sd := &shardDelegator{
		collectionID:   collectionID,
		replicaID:      replicaID,
		vchannelName:   channel,
		version:        version,
		collection:     collection,
		segmentManager: manager.Segment,
		workerManager:  workerManager,
		lifetime:       lifetime.NewLifetime(lifetime.Initializing),
		distribution:   NewDistribution(channel, queryView),
		deleteBuffer: deletebuffer.NewListDeleteBuffer[*deletebuffer.Item](startTs, sizePerBlock,
			[]string{fmt.Sprint(paramtable.GetNodeID()), channel}),
		latestTsafe:                atomic.NewUint64(startTs),
		loader:                     loader,
		queryHook:                  queryHook,
		chunkManager:               chunkManager,
		partitionStats:             make(map[UniqueID]*storage.PartitionStatsSnapshot),
		excludedSegments:           excludedSegments,
		functionRunners:            make(map[int64]function.FunctionRunner),
		analyzerRunners:            make(map[UniqueID]function.Analyzer),
		isBM25Field:                make(map[int64]bool),
		l0ForwardPolicy:            policy,
		catchingUpStreamingData:    atomic.NewBool(true),
		latestRequiredMVCCTimeTick: atomic.NewUint64(0),
	}

	for _, tf := range collection.Schema().GetFunctions() {
		if tf.GetType() == schemapb.FunctionType_BM25 {
			functionRunner, err := function.NewFunctionRunner(collection.Schema(), tf)
			if err != nil {
				return nil, err
			}
			sd.functionRunners[tf.OutputFieldIds[0]] = functionRunner
			// bm25 input field could use same runner between function and analyzer.
			sd.analyzerRunners[tf.InputFieldIds[0]] = functionRunner.(function.Analyzer)
			if tf.GetType() == schemapb.FunctionType_BM25 {
				sd.isBM25Field[tf.OutputFieldIds[0]] = true
			}
		}
	}

	for _, field := range collection.Schema().GetFields() {
		helper := typeutil.CreateFieldSchemaHelper(field)
		if helper.EnableAnalyzer() && sd.analyzerRunners[field.GetFieldID()] == nil {
			analyzerRunner, err := function.NewAnalyzerRunner(field)
			if err != nil {
				return nil, err
			}
			sd.analyzerRunners[field.GetFieldID()] = analyzerRunner
		}
	}

	if len(sd.isBM25Field) > 0 {
		sd.idfOracle = NewIDFOracle(sd.vchannelName, collection.Schema().GetFunctions())
		sd.distribution.SetIDFOracle(sd.idfOracle)
		sd.idfOracle.Start()
	}

	m := sync.Mutex{}
	sd.tsCond = sync.NewCond(&m)
	log.Info("finish build new shardDelegator")
	return sd, nil
}

func (sd *shardDelegator) RunAnalyzer(ctx context.Context, req *querypb.RunAnalyzerRequest) ([]*milvuspb.AnalyzerResult, error) {
	analyzer, ok := sd.analyzerRunners[req.GetFieldId()]
	if !ok {
		return nil, fmt.Errorf("analyzer runner for field %d not exist, now only support run analyzer by field if field was bm25 input field", req.GetFieldId())
	}

	var result [][]*milvuspb.AnalyzerToken
	texts := lo.Map(req.GetPlaceholder(), func(bytes []byte, _ int) string {
		return string(bytes)
	})

	var err error
	if len(analyzer.GetInputFields()) == 1 {
		result, err = analyzer.BatchAnalyze(req.WithDetail, req.WithHash, texts)
	} else {
		analyzerNames := req.GetAnalyzerNames()
		if len(analyzerNames) == 0 {
			return nil, merr.WrapErrAsInputError(fmt.Errorf("analyzer names must be set for multi analyzer"))
		}

		if len(analyzerNames) == 1 && len(texts) > 1 {
			analyzerNames = make([]string, len(texts))
			for i := range analyzerNames {
				analyzerNames[i] = req.AnalyzerNames[0]
			}
		}
		result, err = analyzer.BatchAnalyze(req.WithDetail, req.WithHash, texts, analyzerNames)
	}

	if err != nil {
		return nil, err
	}

	return lo.Map(result, func(tokens []*milvuspb.AnalyzerToken, _ int) *milvuspb.AnalyzerResult {
		return &milvuspb.AnalyzerResult{
			Tokens: tokens,
		}
	}), nil
}

// PartialResultEvaluator evaluates whether partial results should be returned
// Parameters:
//   - taskType: the type of task being executed (Search, Query, etc.)
//   - successSegments: list of segments that were successfully processed
//   - failureSegments: list of segments that failed to process
//   - errors: list of errors that occurred
//
// Returns:
//   - bool: whether to return partial results
//   - float64: actual accessed data ratio (for logging)
type PartialResultEvaluator func(taskType string, successSegments typeutil.Set[int64], failureSegments []int64, errors []error) (bool, float64)

// NewRowCountBasedEvaluator creates a PartialResultEvaluator based on row count
func NewRowCountBasedEvaluator(sealedRowCount map[int64]int64) PartialResultEvaluator {
	return func(taskType string, successSegments typeutil.Set[int64], failureSegments []int64, errors []error) (bool, float64) {
		var partialResultRequiredDataRatio float64
		if taskType == "Query" || taskType == "Search" {
			partialResultRequiredDataRatio = paramtable.Get().QueryNodeCfg.PartialResultRequiredDataRatio.GetAsFloat()
		} else {
			partialResultRequiredDataRatio = 1.0
		}

		if partialResultRequiredDataRatio >= 1.0 || len(sealedRowCount) == 0 {
			return false, 0.0
		}

		// Calculate accessed data ratio for partial result
		successRowCount := int64(0)
		totalRowCount := int64(0)
		for sid, rowCount := range sealedRowCount {
			if successSegments.Contain(sid) {
				successRowCount += rowCount
			}
			totalRowCount += rowCount
		}

		if totalRowCount == 0 {
			return false, 1.0
		}

		accessedDataRatio := float64(successRowCount) / float64(totalRowCount)
		return accessedDataRatio > partialResultRequiredDataRatio, accessedDataRatio
	}
}
