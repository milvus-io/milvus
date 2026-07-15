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
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/querynodev2/cluster"
	"github.com/milvus-io/milvus/internal/querynodev2/delegator/deletebuffer"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/internal/util/grpcclient"
	"github.com/milvus-io/milvus/internal/util/reduce"
	"github.com/milvus-io/milvus/internal/util/searchutil/optimizers"
	"github.com/milvus-io/milvus/internal/util/shallowcopy"
	"github.com/milvus-io/milvus/internal/util/streamrpc"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/config"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/contextutil"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/lifetime"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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
	UpdateSchema(ctx context.Context, sch *schemapb.CollectionSchema, schemaBarrierTs uint64) error

	// data
	ProcessInsert(insertRecords map[int64]*InsertData)
	ProcessDelete(deleteData []*DeleteData, ts uint64)
	ProcessDeleteBatches(batches []DeleteBatch)
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

type idfOracleHolder struct {
	oracle IDFOracle
}

// shardDelegator maintains the shard distribution and streaming part of the data.
type shardDelegator struct {
	// shard information attributes
	collectionID int64
	replicaID    int64
	vchannelName string
	version      int64
	// collection schema
	collection *segments.Collection

	collectionManager segments.CollectionManager

	workerManager cluster.Manager

	lifetime lifetime.Lifetime[lifetime.State]

	distribution *distribution
	// idfOracle is published once after full initialization and is never replaced.
	idfOracle atomic.Pointer[idfOracleHolder]

	segmentManager segments.SegmentManager
	// stream delete buffer
	deleteMut    sync.RWMutex
	deleteBuffer deletebuffer.DeleteBuffer[*deletebuffer.Item]

	sf          conc.Singleflight[struct{}]
	loader      segments.Loader
	tsCond      *syncutil.ContextCond
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

	functionState *functionRuntimeState

	// current forward policy
	l0ForwardPolicy string

	// schemaBarrierTs fences load results started before the latest schema update.
	schemaChangeMutex sync.RWMutex
	schemaBarrierTs   uint64

	// limits delegator-side post-load work after worker LoadSegments returns.
	postLoadSem           *syncutil.Semaphore
	postLoadConfigHandler config.EventHandler

	// streaming data catch-up state
	catchingUpStreamingData *atomic.Bool

	// latest required mvcc timestamp for the delegator
	// for slow down the delegator consumption and reduce the timetick dispatch frequency.
	latestRequiredMVCCTimeTick *atomic.Uint64

	// growingSourceRegistration is the process-local registry lease for this
	// delegator's optional growing-source source.
	growingSourceRegistration *syncmgr.GrowingSourceRegistration
	growingSourceProvider     *delegatorGrowingSourceProvider
}

// getLogger returns the logger with pre-defined shard attributes.
func (sd *shardDelegator) getLogger(ctx context.Context) *mlog.Logger {
	return mlog.With(
		mlog.Int64("collectionID", sd.collectionID),
		mlog.String("channel", sd.vchannelName),
		mlog.Int64("replicaID", sd.replicaID),
	)
}

func (sd *shardDelegator) getIDFOracle() IDFOracle {
	holder := sd.idfOracle.Load()
	if holder == nil {
		return nil
	}
	return holder.oracle
}

func (sd *shardDelegator) publishIDFOracle(idfOracle IDFOracle) {
	sd.idfOracle.Store(&idfOracleHolder{oracle: idfOracle})
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

func (sd *shardDelegator) prepareSearchFunction(ctx context.Context, req *internalpb.SearchRequest) (float64, bool, error) {
	var avgdl float64
	isBM25 := false
	err := sd.functionState.withSearchFunction(req.GetFieldId(), func(functionType schemapb.FunctionType) error {
		switch functionType {
		case schemapb.FunctionType_BM25:
			isBM25 = true
			if req.GetMetricType() != metric.BM25 && req.GetMetricType() != metric.EMPTY {
				return merr.WrapErrParameterInvalid("BM25", req.GetMetricType(), "must use BM25 metric type when searching against BM25 Function output field")
			}
			var buildErr error
			avgdl, buildErr = sd.buildBM25IDF(ctx, req)
			return buildErr
		case schemapb.FunctionType_MinHash:
			if req.GetMetricType() != metric.MHJACCARD && req.GetMetricType() != metric.EMPTY {
				return merr.WrapErrParameterInvalid("MHJACCARD", req.GetMetricType(), "must use MHJACCARD metric type when searching against MinHash Function output field")
			}
			return sd.parseMinHash(ctx, req)
		default:
			return nil
		}
	})
	return avgdl, isBM25 && avgdl <= 0, err
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
	log.RatedInfo(ctx, rate.Limit(60), "update partition stats versions")
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
	nodeReq := &querypb.SearchRequest{
		DmlChannels:     []string{sd.vchannelName},
		SegmentIDs:      segmentIDs,
		Scope:           scope,
		Req:             shallowcopy.ShallowCopySearchRequest(req.GetReq(), targetID),
		FromShardLeader: req.FromShardLeader,
		TotalChannelNum: req.TotalChannelNum,
		FilterOnly:      req.FilterOnly,
		EnableExprCache: req.EnableExprCache,
	}
	return nodeReq
}

func (sd *shardDelegator) modifyQueryRequest(req *querypb.QueryRequest, scope querypb.DataScope, segmentIDs []int64, targetID int64) *querypb.QueryRequest {
	return &querypb.QueryRequest{
		Req:             shallowcopy.ShallowCopyRetrieveRequest(req.GetReq(), targetID),
		DmlChannels:     []string{sd.vchannelName},
		SegmentIDs:      segmentIDs,
		FromShardLeader: req.FromShardLeader,
		Scope:           scope,
	}
}

// executeSearchSubTasks is a helper that encapsulates the common pattern of
// organizeSubTask + executeSubTasks for search operations.
// Used by both normal search and two-stage search to reduce code duplication.
func (sd *shardDelegator) executeSearchSubTasks(
	ctx context.Context,
	req *querypb.SearchRequest,
	sealed []SnapshotItem,
	growing []SegmentEntry,
	sealedRowCount map[int64]int64,
) ([]*internalpb.SearchResults, error) {
	log := sd.getLogger(ctx)
	tasks, err := organizeSubTask(ctx, req, sealed, growing, sd, true, sd.modifySearchRequest)
	if err != nil {
		mlog.Warn(ctx, "Search organizeSubTask failed", mlog.Err(err))
		return nil, err
	}

	results, err := executeSubTasks(ctx, tasks, NewRowCountBasedEvaluator(sealedRowCount),
		func(ctx context.Context, req *querypb.SearchRequest, worker cluster.Worker) (*internalpb.SearchResults, error) {
			ctx = retry.WithMaxAttemptsContext(ctx, 3)
			resp, err := worker.SearchSegments(ctx, req)
			if errors.Is(err, merr.ErrNodeNotFound) || grpcclient.IsServerIDMismatchErr(err) {
				sd.markSegmentOffline(req.GetSegmentIDs()...)
			}
			return resp, err
		}, "Search", log)
	if err != nil {
		mlog.Warn(ctx, "Delegator search failed", mlog.Err(err))
		return nil, err
	}

	mlog.Debug(ctx, "Delegator search done", mlog.Int("results", len(results)))
	return results, nil
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

	if paramtable.Get().QueryNodeCfg.EnableSegmentFilter.GetAsBool() {
		PruneSealedSegmentsByPKFilter(ctx,
			req.GetReq().GetSerializedExprPlan(),
			req.GetReq().GetPkFilter(),
			sealed,
			req.GetReq().GetCollectionID(),
			metrics.SearchLabel,
		)
	}

	avgdl, skipSearch, err := sd.prepareSearchFunction(ctx, req.GetReq())
	if err != nil {
		return nil, err
	}
	if skipSearch {
		mlog.Warn(ctx, "search bm25 from empty data, skip search", mlog.String("channel", sd.vchannelName), mlog.Float64("avgdl", avgdl))
		return []*internalpb.SearchResults{}, nil
	}

	// get final sealedNum after possible segment prune
	sealedNum := lo.SumBy(sealed, func(item SnapshotItem) int { return len(item.Segments) })

	rowCounts := make([]int64, 0, sealedNum)
	for _, item := range sealed {
		for _, seg := range item.Segments {
			rowCounts = append(rowCounts, sealedRowCount[seg.SegmentID])
		}
	}
	effectiveSegmentNum := optimizers.CalculateEffectiveSegmentNum(sd.queryHook, rowCounts, req.GetReq().GetTopk())

	log.Debug(ctx, "search segments...",
		mlog.Int("sealedNum", sealedNum),
		mlog.Int("growingNum", len(growing)),
		mlog.Int("effectiveSegmentNum", effectiveSegmentNum),
	)

	if optimizers.ShouldUseTwoStageSearch(req, effectiveSegmentNum) {
		results, fallback, err := sd.twoStageSearch(ctx, req, sealed, growing, sealedRowCount)
		if err != nil {
			return nil, err
		}
		if !fallback {
			return results, nil
		}
		// fallback: continue with normal single-stage search below
		mlog.Debug(ctx, "Two-stage search requested fallback, continuing with normal search")
	}

	const isSecondStageSearch = false
	req, err = optimizers.OptimizeSearchParams(ctx, req, sd.queryHook, effectiveSegmentNum, isSecondStageSearch, sd.getVectorFieldDim)
	if err != nil {
		mlog.Warn(ctx, "failed to optimize search params", mlog.Err(err))
		return nil, err
	}
	return sd.executeSearchSubTasks(ctx, req, sealed, growing, sealedRowCount)
}

// getVectorFieldDim returns the dimension of the vector field with the given field ID.
// Returns 0 if the field is not found or dim cannot be determined.
func (sd *shardDelegator) getVectorFieldDim(fieldID int64) int64 {
	field := typeutil.GetFieldByID(sd.collection.Schema(), fieldID)
	if field == nil {
		return 0
	}
	dim, err := typeutil.GetDim(field)
	if err != nil {
		return 0
	}
	return dim
}

// Search preforms search operation on shard.
func (sd *shardDelegator) Search(ctx context.Context, req *querypb.SearchRequest) ([]*internalpb.SearchResults, error) {
	log := sd.getLogger(ctx)
	if err := sd.lifetime.Add(sd.IsWorking); err != nil {
		return nil, err
	}
	defer sd.lifetime.Done()

	if !funcutil.SliceContain(req.GetDmlChannels(), sd.vchannelName) {
		log.Warn(ctx, "delegator received search request not belongs to it",
			mlog.Strings("reqChannels", req.GetDmlChannels()),
		)
		return nil, merr.WrapErrChannelMisrouted(sd.vchannelName, fmt.Sprintf("request channels %v", req.GetDmlChannels()))
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
	} else {
		// partial search enabled, could ignore streaming data
		tSafe = sd.GetTSafe()
	}

	metrics.QueryNodeSQLatencyWaitTSafe.WithLabelValues(
		paramtable.GetStringNodeID(), metrics.SearchLabel).
		Observe(float64(waitTr.ElapseSpan().Milliseconds()))

	if err != nil {
		mlog.Warn(ctx, "delegator search failed to wait tsafe", mlog.Err(err))
		return nil, err
	}

	// use tsafe as mvcc timestamp if request not provide it
	if req.GetReq().GetMvccTimestamp() == 0 {
		req.Req.MvccTimestamp = tSafe
	}

	sealed, growing, sealedRowCount, version, err := sd.distribution.PinReadableSegments(partialResultRequiredDataRatio, req.GetReq().GetPartitionIDs()...)
	if err != nil {
		mlog.Warn(ctx, "delegator failed to search, current distribution is not serviceable", mlog.Err(err))
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
				GroupByFieldIds:         req.GetReq().GetGroupByFieldIds(),
				IsTopkReduce:            req.GetReq().GetIsTopkReduce(),
				IsIterator:              req.GetReq().GetIsIterator(),
				CollectionTtlTimestamps: req.GetReq().GetCollectionTtlTimestamps(),
				EntityTtlPhysicalTime:   req.GetReq().GetEntityTtlPhysicalTime(),
				AnalyzerName:            subReq.GetAnalyzerName(),
				PkFilter:                common.PkFilterNoPkFilter, // hybrid search sub-requests rarely have PK predicates, skip unmarshal
				SearchType:              subReq.GetSearchType(),
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
						WithGroupSize(searchReq.GetReq().GetGroupSize()).
						WithGroupByFieldIdsFromProto(searchReq.GetReq().GetGroupByFieldId(), searchReq.GetReq().GetGroupByFieldIds()))
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
				mlog.Debug(ctx, "delegator hybrid search failed",
					mlog.String("reason", result.GetStatus().GetReason()))
				return nil, merr.Error(result.GetStatus())
			}
			results[i] = result
		}
		return results, nil
	}

	results, err := sd.search(ctx, req, sealed, growing, sealedRowCount)
	if err != nil {
		mlog.Warn(ctx, "delegator common search failed", mlog.Err(err))
		return nil, err
	}
	return results, nil
}

func (sd *shardDelegator) QueryStream(ctx context.Context, req *querypb.QueryRequest, srv streamrpc.QueryStreamServer) error {
	log := sd.getLogger(ctx)
	if !sd.Serviceable() {
		return merr.WrapErrServiceUnavailable("delegator", "not serviceable")
	}

	if !funcutil.SliceContain(req.GetDmlChannels(), sd.vchannelName) {
		mlog.Warn(ctx, "deletgator received query request not belongs to it",
			mlog.Strings("reqChannels", req.GetDmlChannels()),
		)
		return merr.WrapErrChannelMisrouted(sd.vchannelName, fmt.Sprintf("request channels %v", req.GetDmlChannels()))
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
	metrics.QueryNodeSQLatencyWaitTSafe.WithLabelValues(
		paramtable.GetStringNodeID(), contextutil.GetQueryLabel(ctx)).
		Observe(float64(waitTr.ElapseSpan().Milliseconds()))
	if err != nil {
		mlog.Warn(ctx, "delegator query failed to wait tsafe", mlog.Err(err))
		return err
	}

	// use tsafe as mvcc timestamp if request not provide it
	if req.GetReq().GetMvccTimestamp() == 0 {
		req.Req.MvccTimestamp = tSafe
	}

	sealed, growing, sealedRowCount, version, err := sd.distribution.PinReadableSegments(float64(1.0), req.GetReq().GetPartitionIDs()...)
	if err != nil {
		mlog.Warn(ctx, "delegator failed to query, current distribution is not serviceable", mlog.Err(err))
		return err
	}
	defer sd.distribution.Unpin(version)

	if req.Req.IgnoreGrowing {
		growing = []SegmentEntry{}
	}

	if paramtable.Get().QueryNodeCfg.EnableSegmentFilter.GetAsBool() {
		PruneSealedSegmentsByPKFilter(ctx,
			req.GetReq().GetSerializedExprPlan(),
			req.GetReq().GetPkFilter(),
			sealed,
			req.GetReq().GetCollectionID(),
			metrics.QueryLabel,
		)
	}

	mlog.Info(ctx, "query stream segments...",
		mlog.Int("sealedNum", len(sealed)),
		mlog.Int("growingNum", len(growing)),
	)
	tasks, err := organizeSubTask(ctx, req, sealed, growing, sd, true, sd.modifyQueryRequest)
	if err != nil {
		mlog.Warn(ctx, "query organizeSubTask failed", mlog.Err(err))
		return err
	}

	_, err = executeSubTasks(ctx, tasks, NewRowCountBasedEvaluator(sealedRowCount), func(ctx context.Context, req *querypb.QueryRequest, worker cluster.Worker) (*internalpb.RetrieveResults, error) {
		ctx = retry.WithMaxAttemptsContext(ctx, 3)
		err := worker.QueryStreamSegments(ctx, req, srv)
		if errors.Is(err, merr.ErrNodeNotFound) || grpcclient.IsServerIDMismatchErr(err) {
			sd.markSegmentOffline(req.GetSegmentIDs()...)
		}
		return nil, err
	}, "QueryStream", log)
	if err != nil {
		mlog.Warn(ctx, "Delegator query stream failed", mlog.Err(err))
		return err
	}

	mlog.Info(ctx, "Delegator QueryStream done")

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
		mlog.Warn(ctx, "delegator received query request not belongs to it",
			mlog.Strings("reqChannels", req.GetDmlChannels()),
		)
		return nil, merr.WrapErrChannelMisrouted(sd.vchannelName, fmt.Sprintf("request channels %v", req.GetDmlChannels()))
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
	tSafe, err := sd.waitTSafe(ctx, req.Req.GuaranteeTimestamp)

	metrics.QueryNodeSQLatencyWaitTSafe.WithLabelValues(
		paramtable.GetStringNodeID(), contextutil.GetQueryLabel(ctx)).
		Observe(float64(waitTr.ElapseSpan().Milliseconds()))

	if err != nil {
		mlog.Warn(ctx, "delegator query failed to wait tsafe", mlog.Err(err))
		return nil, err
	}

	// use tsafe as mvcc timestamp if request not provide it
	if req.GetReq().GetMvccTimestamp() == 0 {
		req.Req.MvccTimestamp = tSafe
	}

	sealed, growing, sealedRowCount, version, err := sd.distribution.PinReadableSegments(float64(1.0), req.GetReq().GetPartitionIDs()...)
	if err != nil {
		mlog.Warn(ctx, "delegator failed to query, current distribution is not serviceable", mlog.Err(err))
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

	if paramtable.Get().QueryNodeCfg.EnableSegmentFilter.GetAsBool() {
		PruneSealedSegmentsByPKFilter(ctx,
			req.GetReq().GetSerializedExprPlan(),
			req.GetReq().GetPkFilter(),
			sealed,
			req.GetReq().GetCollectionID(),
			metrics.QueryLabel,
		)
	}

	sealedNum := lo.SumBy(sealed, func(item SnapshotItem) int { return len(item.Segments) })
	mlog.Debug(ctx, "query segments...",
		mlog.Uint64("mvcc", req.GetReq().GetMvccTimestamp()),
		mlog.Int("sealedNum", sealedNum),
		mlog.Int("growingNum", len(growing)),
	)
	tasks, err := organizeSubTask(ctx, req, sealed, growing, sd, true, sd.modifyQueryRequest)
	if err != nil {
		mlog.Warn(ctx, "query organizeSubTask failed", mlog.Err(err))
		return nil, err
	}

	results, err := executeSubTasks(ctx, tasks, NewRowCountBasedEvaluator(sealedRowCount), func(ctx context.Context, req *querypb.QueryRequest, worker cluster.Worker) (*internalpb.RetrieveResults, error) {
		ctx = retry.WithMaxAttemptsContext(ctx, 3)
		resp, err := worker.QuerySegments(ctx, req)
		if errors.Is(err, merr.ErrNodeNotFound) || grpcclient.IsServerIDMismatchErr(err) {
			sd.markSegmentOffline(req.GetSegmentIDs()...)
		}
		return resp, err
	}, "Query", log)
	if err != nil {
		mlog.Warn(ctx, "Delegator query failed", mlog.Err(err))
		return nil, err
	}

	mlog.Debug(ctx, "Delegator Query done")
	if mlog.LevelEnabled(mlog.DebugLevel) {
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
		mlog.Debug(ctx, "execute count on segments...",
			mlog.Int64s("sealedIDs", sealedIDs),
			mlog.Int64s("growingIDs", growingIDs),
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
		mlog.Warn(ctx, "delegator received GetStatistics request not belongs to it",
			mlog.Strings("reqChannels", req.GetDmlChannels()),
		)
		return nil, merr.WrapErrChannelMisrouted(sd.vchannelName, fmt.Sprintf("GetStatistics channels %v", req.GetDmlChannels()))
	}

	// wait tsafe
	sd.updateLatestRequiredMVCCTimestamp(req.Req.GuaranteeTimestamp)
	_, err := sd.waitTSafe(ctx, req.Req.GuaranteeTimestamp)
	if err != nil {
		mlog.Warn(ctx, "delegator GetStatistics failed to wait tsafe", mlog.Err(err))
		return nil, err
	}

	sealed, growing, sealedRowCount, version, err := sd.distribution.PinReadableSegments(1.0, req.Req.GetPartitionIDs()...)
	if err != nil {
		mlog.Warn(ctx, "delegator failed to GetStatistics, current distribution is not servicable")
		return nil, merr.WrapErrChannelNotAvailable(sd.vchannelName, "distribution is not serviceable")
	}
	defer sd.distribution.Unpin(version)

	tasks, err := organizeSubTask(ctx, req, sealed, growing, sd, true, func(req *querypb.GetStatisticsRequest, scope querypb.DataScope, segmentIDs []int64, targetID int64) *querypb.GetStatisticsRequest {
		// Shallow copy inner request with new Base (each copy needs different TargetID)
		innerReq := req.GetReq()
		return &querypb.GetStatisticsRequest{
			Req: &internalpb.GetStatisticsRequest{
				Base:               &commonpb.MsgBase{TargetID: targetID},
				DbID:               innerReq.GetDbID(),
				CollectionID:       innerReq.GetCollectionID(),
				PartitionIDs:       innerReq.GetPartitionIDs(), // Shallow copy: Same underlying slice
				TravelTimestamp:    innerReq.GetTravelTimestamp(),
				GuaranteeTimestamp: innerReq.GetGuaranteeTimestamp(),
				TimeoutTimestamp:   innerReq.GetTimeoutTimestamp(),
			},
			DmlChannels:     req.GetDmlChannels(), // Shallow copy: Same underlying slice
			SegmentIDs:      segmentIDs,
			FromShardLeader: true,
			Scope:           scope,
		}
	})
	if err != nil {
		mlog.Warn(ctx, "Get statistics organizeSubTask failed", mlog.Err(err))
		return nil, err
	}

	results, err := executeSubTasks(ctx, tasks, NewRowCountBasedEvaluator(sealedRowCount), func(ctx context.Context, req *querypb.GetStatisticsRequest, worker cluster.Worker) (*internalpb.GetStatisticsResponse, error) {
		ctx = retry.WithMaxAttemptsContext(ctx, 1)
		return worker.GetStatistics(ctx, req)
	}, "GetStatistics", log)
	if err != nil {
		mlog.Warn(ctx, "Delegator get statistics failed", mlog.Err(err))
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
			log.Warn(ctx, "failed to get worker for sub task",
				mlog.Int64("nodeID", workerID),
				mlog.Int64s("segments", segmentIDs),
				mlog.Err(err),
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
}](ctx context.Context, tasks []subTask[T], evaluator PartialResultEvaluator, execute func(context.Context, T, cluster.Worker) (R, error), taskType string, log *mlog.Logger,
) ([]R, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var partialResultRequiredDataRatio float64
	if taskType == "Search" {
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
				err = merr.WrapErrServiceInternalMsg("segments not loaded in any worker: %v", segments[:min(len(segments), 10)])
			} else {
				result, err = execute(ctx, task.req, task.worker)
				if result.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
					err = merr.Wrapf(merr.Error(result.GetStatus()), "worker(%d) query failed", task.targetID)
				}
			}

			if err != nil {
				mlog.Warn(ctx, "failed to execute sub task",
					mlog.String("taskType", taskType),
					mlog.Int64("nodeID", task.targetID),
					mlog.Err(err),
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
		mlog.Warn(ctx, "some tasks failed to complete",
			mlog.String("taskType", taskType),
			mlog.Err(err),
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
			mlog.Info(ctx, "partial result executed successfully",
				mlog.String("taskType", taskType),
				mlog.Float64("accessedDataRatio", accessedDataRatio),
				mlog.Int64s("failureSegmentList", failureSegmentList),
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

	// Fast path: tSafe already meets the guarantee timestamp.
	latestTSafe := sd.latestTsafe.Load()
	if latestTSafe >= ts {
		return latestTSafe, nil
	}

	// Slow path: tSafe has not yet reached the guarantee timestamp,
	// need to wait for tSafe to advance via condition variable.

	// check whether tsafe downgraded
	if paramtable.Get().QueryNodeCfg.DowngradeTsafe.GetAsBool() {
		log.RatedWarn(ctx, rate.Limit(10), "downgrade tsafe", mlog.Uint64("latestTSafe", latestTSafe), mlog.Uint64("ts", ts))
		return latestTSafe, nil
	}
	// check lag duration too large
	st, _ := tsoutil.ParseTS(latestTSafe)
	gt, _ := tsoutil.ParseTS(ts)
	lag := gt.Sub(st)
	maxLag := paramtable.Get().QueryNodeCfg.MaxTimestampLag.GetAsDuration(time.Second)
	if lag > maxLag {
		log.Warn(ctx, "guarantee and serviceable ts larger than MaxLag",
			mlog.Time("guaranteeTime", gt),
			mlog.Time("serviceableTime", st),
			mlog.Duration("lag", lag),
			mlog.Duration("maxTsLag", maxLag),
		)
		return 0, WrapErrTsLagTooLarge(sd.vchannelName, lag, maxLag)
	}

	// Stall detection: if tSafe does not advance within stallTimeout, return
	// a retryable error so the proxy can failover to another replica.
	// This handles cases where the WAL consumption pipeline is blocked
	// (e.g. forward delete retrying against a dead QueryNode).
	stallTimeout := paramtable.Get().QueryNodeCfg.WaitTsafeStallTimeout.GetAsDurationByParse()

	// Standard condition variable pattern: Lock → for !condition { Wait } → Unlock
	sd.tsCond.L.Lock()
	for sd.latestTsafe.Load() < ts && sd.Serviceable() {
		stallCtx, stallCancel := context.WithTimeout(ctx, stallTimeout)
		err := sd.tsCond.Wait(stallCtx)
		stallCancel()
		if err != nil {
			// Wait returned without holding the lock.
			if ctx.Err() != nil {
				return 0, ctx.Err()
			}
			// No broadcast within stallTimeout — tSafe is stalled.
			log.Warn(ctx, "tsafe stall detected, fast-fail to allow proxy failover",
				mlog.Uint64("currentTsafe", sd.latestTsafe.Load()),
				mlog.Uint64("targetTs", ts),
				mlog.Duration("stallTimeout", stallTimeout),
			)
			return 0, WrapErrTsLagTooLarge(sd.vchannelName, gt.Sub(st), stallTimeout)
		}
		// Woken by broadcast with lock re-acquired, loop back to re-check condition.
	}
	current := sd.latestTsafe.Load()
	serviceable := sd.Serviceable()
	sd.tsCond.L.Unlock()

	if !serviceable {
		return 0, merr.WrapErrChannelNotAvailable(sd.vchannelName, "delegator closed during wait tsafe")
	}
	return current, nil
}

// GetLatestRequiredMVCCTimeTick returns the latest required mvcc timestamp for the delegator.
func (sd *shardDelegator) GetLatestRequiredMVCCTimeTick() uint64 {
	if sd.catchingUpStreamingData.Load() {
		// delegator need to catch up the streaming data when startup,
		// If the empty timetick is filtered, the load operation will be blocked.
		// We want the delegator to catch up the streaming data, and load done as soon as possible,
		// so we always return the current time as the latest required mvcc timestamp.
		return tsoutil.ComposeTSByTime(time.Now())
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
	ctx := context.TODO()
	log := sd.getLogger(ctx)
	log.RatedInfo(ctx, rate.Limit(10), "update tsafe",
		mlog.Int64("collectionID", sd.collectionID),
		mlog.String("vchannel", sd.vchannelName),
		mlog.Time("tsafe", tsoutil.PhysicalTime(tsafe)),
		mlog.Time("latestTSafe", tsoutil.PhysicalTime(sd.latestTsafe.Load())))
	if tsafe <= sd.latestTsafe.Load() {
		return
	}

	// Store and broadcast under lock to prevent lost wakeups:
	// without the lock, a waiter could observe the old tSafe, then enter
	// Wait() after the broadcast has already fired, missing the notification.
	sd.tsCond.LockAndBroadcast()
	sd.latestTsafe.Store(tsafe)
	sd.tsCond.L.Unlock()

	// Check if caught up with streaming data (all fields are atomic, no lock needed)
	if sd.catchingUpStreamingData.Load() {
		lagThreshold := paramtable.Get().QueryNodeCfg.CatchUpStreamingDataTsLag.GetAsDurationByParse()
		if lagThreshold > 0 {
			tsafeTime := tsoutil.PhysicalTime(tsafe)
			lag := time.Since(tsafeTime)
			caughtUp := lag <= lagThreshold
			log.RatedInfo(ctx, rate.Limit(10), "delegator catching up streaming data progress",
				mlog.String("channel", sd.vchannelName),
				mlog.Duration("lag", lag),
				mlog.Duration("threshold", lagThreshold),
				mlog.Bool("caughtUp", caughtUp))
			if caughtUp {
				sd.catchingUpStreamingData.Store(false)
			}
		}
	}
}

func (sd *shardDelegator) GetTSafe() uint64 {
	return sd.latestTsafe.Load()
}

// CatchingUpStreamingData returns true if delegator is still catching up with streaming data.
func (sd *shardDelegator) CatchingUpStreamingData() bool {
	return sd.catchingUpStreamingData.Load()
}

func (sd *shardDelegator) UpdateSchema(ctx context.Context, schema *schemapb.CollectionSchema, schemaBarrierTs uint64) error {
	log := sd.getLogger(ctx)
	if err := sd.lifetime.Add(sd.IsWorking); err != nil {
		return err
	}
	defer sd.lifetime.Done()

	schemaVersion := uint64(schema.GetVersion())
	mlog.Info(ctx, "delegator received update schema event",
		mlog.Uint64("schemaVersion", schemaVersion),
		mlog.Uint64("schemaBarrierTs", schemaBarrierTs),
	)

	sd.schemaChangeMutex.Lock()
	defer sd.schemaChangeMutex.Unlock()

	// This pre-check is a best-effort guard for delegator side effects. Load
	// paths can still call collectionManager.PutOrRef under collectionManager's
	// own lock and advance the collection snapshot before the final
	// collectionManager.UpdateSchema below. The collection manager remains the
	// source-of-truth freshness gate and will skip that stale final apply.
	if !segments.ShouldUpdateCollectionSchema(sd.collection, schema, schemaBarrierTs) {
		mlog.Info(ctx, "delegator skip stale or no-op schema event",
			mlog.Uint64("schemaVersion", schemaVersion),
			mlog.Uint64("schemaBarrierTs", schemaBarrierTs),
		)
		return nil
	}

	newFunctionState, err := buildFunctionRuntimeState(schema)
	if err != nil {
		return err
	}

	oldSet := newBM25FunctionSet(sd.collection.Schema())
	newSet := newBM25FunctionSet(schema)
	idfOracle := sd.getIDFOracle()
	if idfOracle != nil && newSet.HasIncompatibleCommonFunction(oldSet) {
		newFunctionState.Close()
		return merr.WrapErrServiceInternal("unsupported incompatible BM25 function schema change on loaded collection")
	}

	// Keep the load barrier monotonic. A higher logical schema version can be
	// replayed with a smaller barrier than an earlier same-version property
	// refresh, but that must not reopen older load results.
	if sd.schemaBarrierTs < schemaBarrierTs {
		sd.schemaBarrierTs = schemaBarrierTs
	}

	sealed, growing, version := sd.distribution.PinOnlineSegments()
	defer sd.distribution.Unpin(version)

	mlog.Info(ctx, "update schema targets...",
		mlog.Int("sealedNum", len(sealed)),
		mlog.Int("growingNum", len(growing)),
	)

	tasks, err := organizeSubTask(ctx, &querypb.UpdateSchemaRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithSourceID(paramtable.GetNodeID()),
		),
		CollectionID: sd.collectionID,
		Schema:       schema,
		// SchemaBarrierTs fences stale load results and lets QueryNode refresh
		// same-version schema payloads such as collection properties. Logical
		// schema freshness is still guarded by schema.version in collectionManager.
		SchemaBarrierTs: schemaBarrierTs,
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
		newFunctionState.Close()
		return err
	}

	_, err = executeSubTasks(ctx, tasks, nil, func(ctx context.Context, req *querypb.UpdateSchemaRequest, worker cluster.Worker) (*StatusWrapper, error) {
		ctx = retry.WithMaxAttemptsContext(ctx, 1)
		status, err := worker.UpdateSchema(ctx, req)
		return (*StatusWrapper)(status), err
	}, "UpdateSchema", log)
	if err != nil {
		newFunctionState.Close()
		return err
	}

	// Apply the local collection update with the same barrier used for remote
	// workers. collectionManager keeps schema.Version as the logical freshness key.
	if err := sd.collectionManager.UpdateSchema(sd.collectionID, schema, schemaBarrierTs); err != nil {
		newFunctionState.Close()
		return err
	}
	sd.updateFunctionRunners(schema)
	if idfOracle == nil && len(newSet) > 0 {
		// StreamingNode schema changes flush and fence old growings before UpdateSchema and new-schema inserts.
		// Old growings cannot receive stats for newly added BM25 fields; ProcessInsert registers only new growings.
		idfOracle = NewIDFOracle(sd.vchannelName, schema.GetFunctions())
		idfOracle.Start()
		sd.distribution.SetIDFOracle(idfOracle)
		if current := sd.distribution.current.Load(); current != nil {
			idfOracle.SetNext(current)
		}
		sd.publishIDFOracle(idfOracle)
	} else if idfOracle != nil && !newSet.Equal(oldSet) {
		if err := idfOracle.SyncFunctions(schema.GetFunctions()); err != nil {
			newFunctionState.Close()
			return err
		}
	}
	sd.functionState.swap(newFunctionState).Close()
	mlog.Info(ctx, "delegator finished update schema event",
		mlog.Uint64("schemaVersion", schemaVersion),
		mlog.Uint64("schemaBarrierTs", schemaBarrierTs),
		mlog.Uint64("loadBarrierTs", sd.schemaBarrierTs),
		mlog.Int("sealedNum", len(sealed)),
		mlog.Int("growingNum", len(growing)),
		mlog.Int("bm25FunctionNum", len(newSet)),
	)
	return nil
}

type StatusWrapper commonpb.Status

func (w *StatusWrapper) GetStatus() *commonpb.Status {
	return (*commonpb.Status)(w)
}

// Close closes the delegator.
func (sd *shardDelegator) Close() {
	sd.lifetime.SetState(lifetime.Stopped)
	sd.lifetime.Close()
	// broadcast to all waitTsafe to quit
	sd.tsCond.LockAndBroadcast()
	sd.tsCond.L.Unlock()
	sd.lifetime.Wait()

	if sd.growingSourceProvider != nil {
		sd.growingSourceProvider.Deactivate()
	}

	// Stop background snapshot loop before refunding candidates
	sd.distribution.Close()

	// Refund all sealed segment candidates in distribution
	sd.distribution.RefundAllCandidates()

	// clean idf oracle
	if idfOracle := sd.getIDFOracle(); idfOracle != nil {
		idfOracle.Close()
	}

	sd.functionState.Close()
	sd.releaseFunctionRunners()

	// clean up l0 segment in delete buffer
	start := time.Now()
	sd.deleteBuffer.Clear()
	mlog.Info(context.TODO(), "unregister all l0 segments", mlog.Duration("cost", time.Since(start)))

	metrics.QueryNodeDeleteBufferSize.DeleteLabelValues(paramtable.GetStringNodeID(), sd.vchannelName)
	metrics.QueryNodeDeleteBufferRowNum.DeleteLabelValues(paramtable.GetStringNodeID(), sd.vchannelName)
	if sd.postLoadConfigHandler != nil {
		paramtable.Get().Unwatch(paramtable.Get().QueryNodeCfg.DelegatorPostLoadConcurrencyFactor.Key, sd.postLoadConfigHandler)
	}
}

func (sd *shardDelegator) allocFunctionRunners(schema *schemapb.CollectionSchema) {
	if err := function.AllocFunctionRunners(sd.collectionID, delegatorFunctionRunnerKey(sd.vchannelName), schema); err != nil {
		sd.warnFunctionRunnerInit(err, "allocate", schema)
	}
}

func (sd *shardDelegator) updateFunctionRunners(schema *schemapb.CollectionSchema) {
	if err := function.UpdateFunctionRunners(sd.collectionID, delegatorFunctionRunnerKey(sd.vchannelName), schema); err != nil {
		sd.warnFunctionRunnerInit(err, "update", schema)
	}
}

func (sd *shardDelegator) warnFunctionRunnerInit(err error, operation string, schema *schemapb.CollectionSchema) {
	schemaVersion := function.LatestFunctionRunnerVersion
	if schema != nil {
		schemaVersion = schema.GetVersion()
	}
	mlog.Warn(context.TODO(), "failed to initialize delegator function runners",
		mlog.Int64("collectionID", sd.collectionID),
		mlog.String("vchannel", sd.vchannelName),
		mlog.String("operation", operation),
		mlog.Int32("schemaVersion", schemaVersion),
		mlog.Err(err))
}

func (sd *shardDelegator) releaseFunctionRunners() {
	function.ReleaseFunctionRunners(sd.collectionID, delegatorFunctionRunnerKey(sd.vchannelName))
}

func delegatorFunctionRunnerKey(vchannel string) string {
	return "DELEGATOR-" + vchannel
}

// As partition stats is an optimization for search/query which is not mandatory for milvus instance,
// loading partitionStats will be a try-best process and will skip+logError when running across errors rather than
// return an error status
func (sd *shardDelegator) loadPartitionStats(ctx context.Context, partStatsVersions map[int64]int64) {
	colID := sd.Collection()
	for partID, newVersion := range partStatsVersions {
		var curStats *storage.PartitionStatsSnapshot
		var exist bool
		func() {
			sd.partitionStatsMut.RLock()
			defer sd.partitionStatsMut.RUnlock()
			curStats, exist = sd.partitionStats[partID]
		}()
		if exist && curStats != nil && curStats.Version >= newVersion {
			mlog.RatedWarn(ctx, rate.Limit(60), "Input partition stats' version is less or equal than current partition stats, skip",
				mlog.Int64("partID", partID),
				mlog.Int64("curVersion", curStats.Version),
				mlog.Int64("inputVersion", newVersion),
			)
			continue
		}
		idPath := metautil.JoinIDPath(colID, partID)
		idPath = path.Join(idPath, sd.vchannelName)
		statsFilePath := path.Join(sd.chunkManager.RootPath(), common.PartitionStatsPath, idPath, strconv.FormatInt(newVersion, 10))
		statsBytes, err := sd.chunkManager.Read(ctx, statsFilePath)
		if err != nil {
			mlog.Error(ctx, "failed to read stats file from object storage", mlog.String("path", statsFilePath))
			continue
		}
		partStats, err := storage.DeserializePartitionsStatsSnapshot(statsBytes)
		if err != nil {
			mlog.Error(ctx, "failed to parse partition stats from bytes",
				mlog.Int("bytes_length", len(statsBytes)), mlog.Err(err))
			continue
		}
		partStats.SetVersion(newVersion)
		func() {
			sd.partitionStatsMut.Lock()
			defer sd.partitionStatsMut.Unlock()
			sd.partitionStats[partID] = partStats
		}()
		mlog.Info(ctx, "Updated partitionStats for partition", mlog.Int64("collectionID", sd.collectionID), mlog.Int64("partitionID", partID),
			mlog.Int64("newVersion", newVersion), mlog.Int64("oldVersion", curStats.GetVersion()))
	}
}

// NewShardDelegator creates a new ShardDelegator instance with all fields initialized.
func NewShardDelegator(ctx context.Context, collectionID UniqueID, replicaID UniqueID, channel string, version int64,
	workerManager cluster.Manager, manager *segments.Manager, loader segments.Loader, startTs uint64, queryHook optimizers.QueryHook, chunkManager storage.ChunkManager,
	queryView *channelQueryView,
	binlogSaver segments.BinlogSaver,
) (ShardDelegator, error) {
	log := mlog.With(mlog.Int64("collectionID", collectionID),
		mlog.Int64("replicaID", replicaID),
		mlog.String("channel", channel),
		mlog.Int64("version", version),
		mlog.Uint64("startTs", startTs),
	)

	collection := manager.Collection.Get(collectionID)
	if collection == nil {
		return nil, merr.WrapErrCollectionNotFound(collectionID, "not in delegator manager")
	}

	sizePerBlock := paramtable.Get().QueryNodeCfg.DeleteBufferBlockSize.GetAsInt64()
	log.Info(ctx, "Init delete cache with list delete buffer", mlog.Int64("sizePerBlock", sizePerBlock), mlog.Time("startTime", tsoutil.PhysicalTime(startTs)))

	excludedSegments := NewExcludedSegments(paramtable.Get().QueryNodeCfg.CleanExcludeSegInterval.GetAsDuration(time.Second))

	policy := paramtable.Get().QueryNodeCfg.LevelZeroForwardPolicy.GetValue()
	log.Info(ctx, "shard delegator setup l0 forward policy", mlog.String("policy", policy))
	postLoadSem := syncutil.NewSemaphore(paramtable.Get().QueryNodeCfg.DelegatorPostLoadConcurrencyFactor.GetAsInt())
	postLoadConfigHandler := config.NewHandler(fmt.Sprintf("qn.delegator.postload.%s.%p", channel, postLoadSem), func(event *config.Event) {
		if event.HasUpdated {
			concurrency := paramtable.Get().QueryNodeCfg.DelegatorPostLoadConcurrencyFactor.GetAsInt()
			postLoadSem.SetCapacity(concurrency)
			log.Info(ctx, "resize delegator post-load concurrency", mlog.Int("concurrency", concurrency))
		}
	})

	sd := &shardDelegator{
		collectionID:      collectionID,
		replicaID:         replicaID,
		vchannelName:      channel,
		version:           version,
		collection:        collection,
		collectionManager: manager.Collection,
		segmentManager:    manager.Segment,
		workerManager:     workerManager,
		lifetime:          lifetime.NewLifetime(lifetime.Initializing),
		distribution:      NewDistribution(channel, queryView),
		deleteBuffer: deletebuffer.NewListDeleteBuffer[*deletebuffer.Item](startTs, sizePerBlock,
			[]string{paramtable.GetStringNodeID(), channel}),
		latestTsafe:                atomic.NewUint64(startTs),
		loader:                     loader,
		queryHook:                  queryHook,
		chunkManager:               chunkManager,
		partitionStats:             make(map[UniqueID]*storage.PartitionStatsSnapshot),
		excludedSegments:           excludedSegments,
		l0ForwardPolicy:            policy,
		postLoadSem:                postLoadSem,
		postLoadConfigHandler:      postLoadConfigHandler,
		catchingUpStreamingData:    atomic.NewBool(true),
		latestRequiredMVCCTimeTick: atomic.NewUint64(0),
	}

	functionState, err := buildFunctionRuntimeState(collection.Schema())
	if err != nil {
		return nil, err
	}
	sd.functionState = functionState
	sd.allocFunctionRunners(collection.Schema())

	hasBM25Field := lo.ContainsBy(collection.Schema().GetFunctions(), func(tf *schemapb.FunctionSchema) bool {
		return tf.GetType() == schemapb.FunctionType_BM25
	})

	if hasBM25Field {
		idfOracle := NewIDFOracle(sd.vchannelName, collection.Schema().GetFunctions())
		idfOracle.Start()
		sd.distribution.SetIDFOracle(idfOracle)
		sd.publishIDFOracle(idfOracle)
	}

	// Register growing-source segments as optional local flush sources. Metadata
	// commit is still owned by WAL flusher / WriteBuffer.
	if sd.useGrowingSourceFlush() {
		sd.growingSourceProvider = newDelegatorGrowingSourceProvider(manager.Segment, func(ctx context.Context, fenceTs uint64) error {
			_, err := sd.waitTSafe(ctx, fenceTs)
			return err
		}, sd.GetTSafe)
		sd.growingSourceProvider.SetChannelName(sd.vchannelName)
		sd.growingSourceRegistration = syncmgr.DefaultGrowingSourceRegistry().Register(sd.vchannelName, sd.growingSourceProvider)
		sd.growingSourceProvider.SetRegistration(sd.growingSourceRegistration)
		log.Info(ctx, "registered growing-source source support")
	}

	sd.tsCond = syncutil.NewContextCond(&sync.Mutex{})
	paramtable.Get().Watch(paramtable.Get().QueryNodeCfg.DelegatorPostLoadConcurrencyFactor.Key, postLoadConfigHandler)
	log.Info(ctx, "finish build new shardDelegator")
	return sd, nil
}

// useGrowingSourceFlush returns true when the collection should expose growing segments as a flush source.
func (sd *shardDelegator) useGrowingSourceFlush() bool {
	if sd == nil || sd.collection == nil {
		return false
	}
	return typeutil.UseGrowingSourceFlush(sd.collection.Schema(),
		paramtable.Get().CommonCfg.UseLoonFFI.GetAsBool(),
		paramtable.Get().CommonCfg.EnableGrowingSourceFlush.GetAsBool())
}

func (sd *shardDelegator) runWithAnalyzer(ctx context.Context, fieldID int64, run func(function.Analyzer) error) (bool, error) {
	schema := sd.collection.Schema()
	ok, err := function.RunWithAnalyzer(ctx, sd.collectionID, schema.GetVersion(), fieldID, run)
	if ok || err != nil {
		return ok, err
	}
	if fieldHasBM25Analyzer(schema, fieldID) {
		return false, nil
	}

	field := typeutil.GetField(schema, fieldID)
	if field == nil || !typeutil.CreateFieldSchemaHelper(field).EnableAnalyzer() {
		return false, nil
	}

	analyzer, err := function.NewAnalyzerRunner(field)
	if err != nil {
		return false, err
	}
	if runner, ok := analyzer.(function.FunctionRunner); ok {
		defer runner.Close()
	}
	return true, run(analyzer)
}

func fieldHasBM25Analyzer(schema *schemapb.CollectionSchema, fieldID int64) bool {
	for _, fn := range schema.GetFunctions() {
		if fn.GetType() == schemapb.FunctionType_BM25 && slices.Contains(fn.GetInputFieldIds(), fieldID) {
			return true
		}
	}
	return false
}

func (sd *shardDelegator) RunAnalyzer(ctx context.Context, req *querypb.RunAnalyzerRequest) ([]*milvuspb.AnalyzerResult, error) {
	var result [][]*milvuspb.AnalyzerToken
	var analyzeErr error
	texts := lo.Map(req.GetPlaceholder(), func(bytes []byte, _ int) string {
		return string(bytes)
	})

	ok, err := sd.runWithAnalyzer(ctx, req.GetFieldId(), func(analyzer function.Analyzer) error {
		if len(analyzer.GetInputFields()) == 1 {
			result, analyzeErr = analyzer.BatchAnalyze(req.WithDetail, req.WithHash, texts)
			return analyzeErr
		}

		analyzerNames, err := normalizeAnalyzerNames(req.GetAnalyzerNames(), len(texts))
		if err != nil {
			return err
		}
		result, analyzeErr = analyzer.BatchAnalyze(req.WithDetail, req.WithHash, texts, analyzerNames)
		return analyzeErr
	})
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, merr.WrapErrParameterInvalidMsg("analyzer runner for field %d not exist, now only support run analyzer by field if field was bm25/minhash input field", req.GetFieldId())
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
		if taskType == "Search" {
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
		return accessedDataRatio >= partialResultRequiredDataRatio, accessedDataRatio
	}
}
