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

package querynodev2

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/querynodev2/delegator"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/querynodev2/tasks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2"
	"github.com/milvus-io/milvus/internal/streamingnode/client/handler"
	"github.com/milvus-io/milvus/internal/streamingnode/client/handler/registry"
	"github.com/milvus-io/milvus/internal/util/analyzer"
	"github.com/milvus-io/milvus/internal/util/fileresource"
	"github.com/milvus-io/milvus/internal/util/searchutil/scheduler"
	streamingstatus "github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/internal/util/streamrpc"
	"github.com/milvus-io/milvus/internal/util/textmatch"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/contextutil"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// legacyLoadScopeIndex is the retired wire value emitted by QueryCoord
// versions that predate LoadScope_Reopen. Keep the value out of the proto enum
// so it cannot be reused, while recognizing it here to return a precise
// mixed-version protocol error.
const legacyLoadScopeIndex = querypb.LoadScope(2)

type segmentDetacher interface {
	DetachStreaming(ctx context.Context, segmentID typeutil.UniqueID) int
}

// GetComponentStates returns information about whether the node is healthy
func (node *QueryNode) GetComponentStates(ctx context.Context, req *milvuspb.GetComponentStatesRequest) (*milvuspb.ComponentStates, error) {
	stats := &milvuspb.ComponentStates{
		Status: merr.Success(),
	}

	code := node.lifetime.GetState()
	nodeID := common.NotRegisteredID

	if node.session != nil && node.session.Registered() {
		nodeID = node.GetNodeID()
	}
	mlog.Debug(ctx, "QueryNode current state", mlog.Int64("NodeID", nodeID), mlog.String("StateCode", code.String()))

	info := &milvuspb.ComponentInfo{
		NodeID:    nodeID,
		Role:      typeutil.QueryNodeRole,
		StateCode: code,
	}
	stats.State = info
	return stats, nil
}

// GetTimeTickChannel returns the time tick channel
// TimeTickChannel contains many time tick messages, which will be sent by query nodes
func (node *QueryNode) GetTimeTickChannel(ctx context.Context, req *internalpb.GetTimeTickChannelRequest) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: merr.Success(),
		Value:  paramtable.Get().CommonCfg.QueryCoordTimeTick.GetValue(),
	}, nil
}

// GetStatisticsChannel returns the statistics channel
// Statistics channel contains statistics infos of query nodes, such as segment infos, memory infos
func (node *QueryNode) GetStatisticsChannel(ctx context.Context, req *internalpb.GetStatisticsChannelRequest) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: merr.Success(),
	}, nil
}

// GetStatistics returns loaded statistics of collection.
func (node *QueryNode) GetStatistics(ctx context.Context, req *querypb.GetStatisticsRequest) (*internalpb.GetStatisticsResponse, error) {
	log := mlog.With(
		mlog.Int64("msgID", req.GetReq().GetBase().GetMsgID()),
		mlog.Strings("vChannels", req.GetDmlChannels()),
		mlog.Int64s("segmentIDs", req.GetSegmentIDs()),
		mlog.Uint64("guaranteeTimestamp", req.GetReq().GetGuaranteeTimestamp()),
		mlog.Uint64("timeTravel", req.GetReq().GetTravelTimestamp()),
	)
	log.Debug(ctx, "received GetStatisticsRequest")

	if err := node.lifetime.Add(merr.IsHealthy); err != nil {
		return &internalpb.GetStatisticsResponse{
			Status: merr.Status(err),
		}, nil
	}
	defer node.lifetime.Done()

	failRet := &internalpb.GetStatisticsResponse{
		Status: merr.Success(),
	}

	var toReduceResults []*internalpb.GetStatisticsResponse
	var mu sync.Mutex
	runningGp, runningCtx := errgroup.WithContext(ctx)
	for _, ch := range req.GetDmlChannels() {
		ch := ch
		req := &querypb.GetStatisticsRequest{
			Req:             req.Req,
			DmlChannels:     []string{ch},
			SegmentIDs:      req.SegmentIDs,
			FromShardLeader: req.FromShardLeader,
			Scope:           req.Scope,
		}
		runningGp.Go(func() error {
			ret, err := node.getChannelStatistics(runningCtx, req, ch)
			if err == nil {
				err = merr.Error(ret.GetStatus())
			}

			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				failRet.Status = merr.Status(err)
				return err
			}
			toReduceResults = append(toReduceResults, ret)
			return nil
		})
	}
	if err := runningGp.Wait(); err != nil {
		return failRet, nil
	}

	ret, err := reduceStatisticResponse(toReduceResults)
	if err != nil {
		failRet.Status = merr.Status(err)
		return failRet, nil
	}
	log.Debug(ctx, "reduce statistic result done")

	return ret, nil
}

// WatchDmChannels create consumers on dmChannels to receive Incremental data，which is the important part of real-time query
func (node *QueryNode) WatchDmChannels(ctx context.Context, req *querypb.WatchDmChannelsRequest) (status *commonpb.Status, e error) {
	defer node.updateDistributionModifyTS()

	channel := req.GetInfos()[0]
	log := mlog.With(
		mlog.Int64("collectionID", req.GetCollectionID()),
		mlog.String("channel", channel.GetChannelName()),
		mlog.Int64("currentNodeID", node.GetNodeID()),
	)

	log.Info(ctx, "received watch channel request",
		mlog.Int64("version", req.GetVersion()),
	)

	// check node healthy
	if err := node.lifetime.Add(merr.IsHealthy); err != nil {
		return merr.Status(err), nil
	}
	defer node.lifetime.Done()

	// check index
	if len(req.GetIndexInfoList()) == 0 {
		err := merr.WrapErrIndexNotFoundForCollection(req.GetSchema().GetName())
		return merr.Status(err), nil
	}

	if !node.subscribingChannels.Insert(channel.GetChannelName()) {
		msg := "channel subscribing..."
		log.Warn(ctx,
			msg)
		return merr.Success(), nil
	}
	defer node.subscribingChannels.Remove(channel.GetChannelName())

	// to avoid concurrent watch/unwatch
	if node.unsubscribingChannels.Contain(channel.GetChannelName()) {
		err := merr.WrapErrChannelReduplicate(channel.GetChannelName(), "the other same channel is unsubscribing")
		log.Warn(ctx, "failed to unsubscribe channel", mlog.Err(err))
		return merr.Status(err), nil
	}

	_, exist := node.delegators.Get(channel.GetChannelName())
	if exist {
		log.Info(ctx, "channel already subscribed")
		return merr.Success(), nil
	}

	err := node.manager.Collection.PutOrRef(req.GetCollectionID(), req.GetSchema(),
		segments.ComposeIndexMeta(ctx, req.GetIndexInfoList(), req.Schema), req.GetLoadMeta())
	if err != nil {
		log.Warn(ctx, "failed to ref collection", mlog.Err(err))
		return merr.Status(err), nil
	}
	defer func() {
		if !merr.Ok(status) {
			node.manager.Collection.Unref(req.GetCollectionID(), 1)
		}
	}()

	queryView := delegator.NewChannelQueryView(
		channel.GetUnflushedSegmentIds(),
		req.GetSealedSegmentRowCount(),
		req.GetPartitionIDs(),
		req.GetTargetVersion(),
	)

	delegator, err := delegator.NewShardDelegator(
		ctx,
		req.GetCollectionID(),
		req.GetReplicaID(),
		channel.GetChannelName(),
		req.GetVersion(),
		node.clusterManager,
		node.manager,
		node.loader,
		channel.GetSeekPosition().GetTimestamp(),
		node.queryHook,
		node.chunkManager,
		queryView,
		node.binlogSaver,
	)
	if err != nil {
		log.Warn(ctx, "failed to create shard delegator", mlog.Err(err))
		return merr.Status(err), nil
	}
	node.delegators.Insert(channel.GetChannelName(), delegator)
	defer func() {
		if err != nil {
			node.delegators.GetAndRemove(channel.GetChannelName())
			delegator.Close()
		}
	}()

	pipeline, err := node.pipelineManager.Add(req.GetCollectionID(), channel.GetChannelName())
	if err != nil {
		msg := "failed to create pipeline"
		log.Warn(ctx,
			msg, mlog.Err(err))
		return merr.Status(err), nil
	}
	defer func() {
		if err != nil {
			node.pipelineManager.Remove(channel.GetChannelName())
		}
	}()

	growingInfo := lo.SliceToMap(channel.GetUnflushedSegmentIds(), func(id int64) (int64, uint64) {
		info := req.GetSegmentInfos()[id]
		return id, info.GetDmlPosition().GetTimestamp()
	})
	delegator.AddExcludedSegments(growingInfo)

	flushedInfo := lo.SliceToMap(channel.GetFlushedSegmentIds(), func(id int64) (int64, uint64) {
		return id, typeutil.MaxTimestamp
	})
	delegator.AddExcludedSegments(flushedInfo)

	droppedInfo := lo.SliceToMap(channel.GetDroppedSegmentIds(), func(id int64) (int64, uint64) {
		return id, typeutil.MaxTimestamp
	})
	delegator.AddExcludedSegments(droppedInfo)

	defer func() {
		if err != nil {
			// remove legacy growing
			node.manager.Segment.RemoveBy(ctx, segments.WithChannel(channel.GetChannelName()),
				segments.WithType(segments.SegmentTypeGrowing))
		}
	}()

	err = loadL0Segments(ctx, delegator, req)
	if err != nil {
		log.Warn(ctx, "failed to load l0 segments", mlog.Err(err))
		return merr.Status(err), nil
	}
	err = loadGrowingSegments(ctx, delegator, req)
	if err != nil {
		msg := "failed to load growing segments"
		log.Warn(ctx,
			msg, mlog.Err(err))
		return merr.Status(err), nil
	}

	// Use seekPosition directly to start consuming the message stream.
	//
	// Background:
	// - seekPosition: channel checkpoint from DataCoord, represents the position where data has been persisted
	// - deleteCheckpoint: the minimum startPosition among all L0 segments, indicates where unpersisted
	//   delete records begin
	//
	// Why we can use seekPosition directly:
	// - L0 segments have already been loaded above (loadL0Segments), which contain delete records
	//   from [deleteCheckpoint, L0.endPosition]
	// - The message stream will capture new delete records from [seekPosition, ∞)
	// - DataCoord ensures that seekPosition is calculated based on channel checkpoint, which is updated
	//   after data (including deletes) is flushed, so L0 segments should cover up to seekPosition
	// - Using seekPosition avoids redundant message consumption when seekPosition > deleteCheckpoint
	log.Info(ctx, "use channel seek position to seek",
		mlog.Time("seekPosition", tsoutil.PhysicalTime(channel.GetSeekPosition().GetTimestamp())),
		mlog.Time("deleteCheckpoint", tsoutil.PhysicalTime(channel.GetDeleteCheckpoint().GetTimestamp())),
	)
	err = pipeline.ConsumeMsgStream(ctx, channel.GetSeekPosition())
	if err != nil {
		err = merr.WrapErrServiceUnavailable(err.Error(), "InitPipelineFailed")
		log.Warn(ctx,
			err.Error(),
			mlog.Int64("collectionID", channel.CollectionID),
			mlog.String("channel", channel.ChannelName),
		)
		return merr.Status(err), nil
	}

	// start pipeline
	pipeline.Start()
	// delegator after all steps done
	delegator.Start()
	log.Info(ctx, "watch dml channel success")
	return merr.Success(), nil
}

func (node *QueryNode) UnsubDmChannel(ctx context.Context, req *querypb.UnsubDmChannelRequest) (*commonpb.Status, error) {
	defer node.updateDistributionModifyTS()
	log := mlog.With(
		mlog.Int64("collectionID", req.GetCollectionID()),
		mlog.String("channel", req.GetChannelName()),
		mlog.Int64("currentNodeID", node.GetNodeID()),
	)

	log.Info(ctx, "received unsubscribe channel request")

	// check node healthy
	if err := node.lifetime.Add(merr.IsHealthy); err != nil {
		return merr.Status(err), nil
	}
	defer node.lifetime.Done()

	node.unsubscribingChannels.Insert(req.GetChannelName())
	defer node.unsubscribingChannels.Remove(req.GetChannelName())
	_, ok := node.delegators.Get(req.GetChannelName())
	if ok {
		growingSegmentIDs := node.localGrowingSegmentIDs(req.GetChannelName(), nil)
		prepared, err := node.prepareReleaseManualFlush(ctx, req.GetCollectionID(), req.GetChannelName(), growingSegmentIDs)
		prepareSkipped := false
		if err != nil {
			if isReleaseManualFlushPrepareUnavailable(err) {
				log.Warn(ctx, "release manual flush prepare unavailable before unsubscribing channel, continue unsubscribe",
					mlog.Int64s("segmentIDs", growingSegmentIDs),
					mlog.Err(err))
				prepared = false
				prepareSkipped = true
			} else {
				log.Warn(ctx, "failed to prepare release manual flush before unsubscribing channel",
					mlog.Int64s("segmentIDs", growingSegmentIDs),
					mlog.Err(err))
				return merr.Status(err), nil
			}
		}
		if prepareSkipped {
			log.Info(ctx, "release manual flush prepare skipped before unsubscribing channel",
				mlog.Int64s("segmentIDs", growingSegmentIDs),
				mlog.Bool("prepared", prepared))
		} else {
			log.Info(ctx, "release manual flush prepare result before unsubscribing channel",
				mlog.Int64s("segmentIDs", growingSegmentIDs),
				mlog.Bool("prepared", prepared))
		}

		delegator, ok := node.delegators.GetAndRemove(req.GetChannelName())
		if !ok {
			log.Info(ctx, "channel already unsubscribed")
			return merr.Success(), nil
		}
		node.pipelineManager.Remove(req.GetChannelName())
		preparedGrowingSourceSegments := syncmgr.DefaultGrowingSourceRegistry().ReleasePreparedSegments(req.GetChannelName())
		// close the delegator first to block all coming query/search requests
		delegator.Close()

		if detacher, ok := node.manager.Segment.(segmentDetacher); ok {
			for _, segmentID := range preparedGrowingSourceSegments {
				detacher.DetachStreaming(ctx, segmentID)
				syncmgr.DefaultGrowingSourceRegistry().MarkReleaseDetached(req.GetChannelName(), segmentID)
				syncmgr.DefaultGrowingSourceRegistry().ClearReleasePrepared(req.GetChannelName(), segmentID)
			}
		}
		node.manager.Segment.RemoveBy(ctx, segments.WithChannel(req.GetChannelName()), segments.WithType(segments.SegmentTypeGrowing))
		node.manager.Collection.Unref(req.GetCollectionID(), 1)
	}
	log.Info(ctx, "unsubscribed channel")

	return merr.Success(), nil
}

func (node *QueryNode) LoadPartitions(ctx context.Context, req *querypb.LoadPartitionsRequest) (*commonpb.Status, error) {
	log := mlog.With(
		mlog.Int64("collectionID", req.GetCollectionID()),
		mlog.Int64s("partitionIDs", req.GetPartitionIDs()),
	)

	log.Info(ctx, "received load partitions request")
	// check node healthy
	if err := node.lifetime.Add(merr.IsHealthyOrStopping); err != nil {
		return merr.Status(err), nil
	}
	defer node.lifetime.Done()

	collection := node.manager.Collection.Get(req.GetCollectionID())
	if collection != nil {
		collection.AddPartition(req.GetPartitionIDs()...)
	}

	log.Info(ctx, "load partitions done")
	return merr.Success(), nil
}

// LoadSegments load historical data into query node, historical data can be vector data or index
func (node *QueryNode) LoadSegments(ctx context.Context, req *querypb.LoadSegmentsRequest) (*commonpb.Status, error) {
	defer node.updateDistributionModifyTS()
	segment := req.GetInfos()[0]

	log := mlog.With(
		mlog.Int64("collectionID", segment.GetCollectionID()),
		mlog.Int64("partitionID", segment.GetPartitionID()),
		mlog.String("shard", segment.GetInsertChannel()),
		mlog.Int64("segmentID", segment.GetSegmentID()),
		mlog.String("level", segment.GetLevel().String()),
		mlog.Int64("currentNodeID", node.GetNodeID()),
		mlog.Int64("dstNodeID", req.GetDstNodeID()),
	)

	log.Info(ctx, "received load segments request",
		mlog.Int64("version", req.GetVersion()),
		mlog.Bool("needTransfer", req.GetNeedTransfer()),
		mlog.String("loadScope", req.GetLoadScope().String()))
	// check node healthy
	if err := node.lifetime.Add(merr.IsHealthy); err != nil {
		return merr.Status(err), nil
	}
	defer node.lifetime.Done()

	// check index
	if len(req.GetIndexInfoList()) == 0 {
		err := merr.WrapErrIndexNotFoundForCollection(req.GetSchema().GetName())
		return merr.Status(err), nil
	}

	// fallback binlog memory size to log size when it is zero
	fallbackBinlogMemorySize := func(binlogs []*datapb.FieldBinlog) {
		for _, insertBinlogs := range binlogs {
			for _, b := range insertBinlogs.GetBinlogs() {
				if b.GetMemorySize() == 0 {
					b.MemorySize = b.GetLogSize()
				}
			}
		}
	}
	for _, s := range req.GetInfos() {
		fallbackBinlogMemorySize(s.GetBinlogPaths())
		fallbackBinlogMemorySize(s.GetStatslogs())
		fallbackBinlogMemorySize(s.GetDeltalogs())
	}

	// Delegates request to workers
	if req.GetNeedTransfer() {
		delegator, ok := node.delegators.Get(segment.GetInsertChannel())
		if !ok {
			msg := "failed to load segments, delegator not found"
			log.Warn(ctx,
				msg)
			err := merr.WrapErrChannelNotFound(segment.GetInsertChannel())
			return merr.Status(err), nil
		}

		if len(req.GetInfos()) > 0 && req.GetInfos()[0].Level == datapb.SegmentLevel_L0 {
			// force l0 segment to load on delegator
			if req.DstNodeID != node.GetNodeID() {
				log.Info(ctx, "unexpected L0 segment load on non-delegator node, force to load on delegator")
				req.DstNodeID = node.GetNodeID()
			}
		}

		req.NeedTransfer = false
		err := delegator.LoadSegments(ctx, req)
		if err != nil {
			log.Warn(ctx, "delegator failed to load segments", mlog.Err(err))
			return merr.Status(err), nil
		}

		return merr.Success(), nil
	}

	err := node.manager.Collection.PutOrRef(req.GetCollectionID(), req.GetSchema(),
		segments.ComposeIndexMeta(ctx, req.GetIndexInfoList(), req.GetSchema()), req.GetLoadMeta())
	if err != nil {
		log.Warn(ctx, "failed to ref collection", mlog.Err(err))
		return merr.Status(err), nil
	}
	defer node.manager.Collection.Unref(req.GetCollectionID(), 1)

	switch req.GetLoadScope() {
	case querypb.LoadScope_Delta:
		return node.loadDeltaLogs(ctx, req), nil
	case querypb.LoadScope_Stats:
		return node.reopenSegments(ctx, req), nil
	case querypb.LoadScope_Reopen:
		return node.reopenSegments(ctx, req), nil
	case querypb.LoadScope_Full:
		// Continue with the full segment load below.
	case legacyLoadScopeIndex:
		err := merr.WrapErrServiceInternalMsg(
			"legacy segment index load scope %d is no longer supported; upgrade QueryCoord to use LoadScope_Reopen before routing requests to this QueryNode",
			req.GetLoadScope())
		return merr.Status(err), nil
	default:
		err := merr.WrapErrServiceInternalMsg(
			"unsupported segment load scope %d", req.GetLoadScope())
		return merr.Status(err), nil
	}

	// Actual load segment
	log.Info(ctx, "start to load segments...")
	loaded, err := node.loader.Load(ctx,
		req.GetCollectionID(),
		segments.SegmentTypeSealed,
		req.GetVersion(),
		req.GetInfos()...,
	)
	if err != nil {
		return merr.Status(err), nil
	}

	node.manager.Collection.Ref(req.GetCollectionID(), uint32(len(loaded)))

	log.Info(ctx, "load segments done...",
		mlog.Int64s("segments", lo.Map(loaded, func(s segments.Segment, _ int) int64 { return s.ID() })))

	// Publish filesystem metrics after load task completion
	// Use default filesystem (empty path) for load tasks
	storagev2.PublishDefaultFilesystemMetrics()

	return merr.Success(), nil
}

// UpdateSchema updates the schema of the collection on the querynode.
func (node *QueryNode) UpdateSchema(ctx context.Context, req *querypb.UpdateSchemaRequest) (*commonpb.Status, error) {
	defer node.updateDistributionModifyTS()

	// check node healthy
	if err := node.lifetime.Add(merr.IsHealthy); err != nil {
		return merr.Status(err), nil
	}
	defer node.lifetime.Done()

	log := mlog.With(
		mlog.Int64("collectionID", req.GetCollectionID()),
		mlog.Uint64("schemaBarrierTs", req.GetSchemaBarrierTs()),
		mlog.Int32("schemaVersion", req.GetSchema().GetVersion()),
	)

	log.Info(ctx, "querynode received update schema request")

	// Pass the barrier timestamp through; collectionManager derives the logical
	// schema version from the schema payload when it is present.
	err := node.manager.Collection.UpdateSchema(req.GetCollectionID(), req.GetSchema(), req.GetSchemaBarrierTs())
	if err != nil {
		log.Warn(ctx, "failed to update schema", mlog.Err(err))
	}

	return merr.Status(err), nil
}

// ReleaseCollection clears all data related to this collection on the querynode
func (node *QueryNode) ReleaseCollection(ctx context.Context, in *querypb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	if err := node.lifetime.Add(merr.IsHealthyOrStopping); err != nil {
		return merr.Status(err), nil
	}
	defer node.lifetime.Done()

	return merr.Success(), nil
}

// ReleasePartitions clears all data related to this partition on the querynode
func (node *QueryNode) ReleasePartitions(ctx context.Context, req *querypb.ReleasePartitionsRequest) (*commonpb.Status, error) {
	log := mlog.With(
		mlog.Int64("collection", req.GetCollectionID()),
		mlog.Int64s("partitions", req.GetPartitionIDs()),
	)

	log.Info(ctx, "received release partitions request")

	// check node healthy
	if err := node.lifetime.Add(merr.IsHealthy); err != nil {
		return merr.Status(err), nil
	}
	defer node.lifetime.Done()

	collection := node.manager.Collection.Get(req.GetCollectionID())
	if collection != nil {
		for _, partition := range req.GetPartitionIDs() {
			collection.RemovePartition(partition)
		}
	}

	log.Info(ctx, "release partitions done")
	return merr.Success(), nil
}

// ReleaseSegments remove the specified segments from query node according segmentIDs, partitionIDs, and collectionID
func (node *QueryNode) ReleaseSegments(ctx context.Context, req *querypb.ReleaseSegmentsRequest) (*commonpb.Status, error) {
	defer node.updateDistributionModifyTS()
	log := mlog.With(
		mlog.Int64("collectionID", req.GetCollectionID()),
		mlog.String("shard", req.GetShard()),
		mlog.Int64s("segmentIDs", req.GetSegmentIDs()),
		mlog.Int64("currentNodeID", node.GetNodeID()),
	)

	log.Info(ctx, "received release segment request",
		mlog.String("scope", req.GetScope().String()),
		mlog.Bool("needTransfer", req.GetNeedTransfer()),
	)

	// check node healthy
	if err := node.lifetime.Add(merr.IsHealthyOrStopping); err != nil {
		return merr.Status(err), nil
	}
	defer node.lifetime.Done()

	if req.GetNeedTransfer() {
		shardDelegator, ok := node.delegators.Get(req.GetShard())
		if !ok {
			msg := "failed to release segment, delegator not found"
			log.Warn(ctx,
				msg)
			err := merr.WrapErrChannelNotFound(req.GetShard())
			return merr.Status(err), nil
		}

		req.NeedTransfer = false
		err := shardDelegator.ReleaseSegments(ctx, req, false)
		if err != nil {
			log.Warn(ctx, "delegator failed to release segment", mlog.Err(err))
			return merr.Status(err), nil
		}

		return merr.Success(), nil
	}

	log.Info(ctx, "start to release segments")
	sealedCount := 0
	for _, id := range req.GetSegmentIDs() {
		_, count := node.manager.Segment.Remove(ctx, id, req.GetScope())
		sealedCount += count
	}
	node.manager.Collection.Unref(req.GetCollectionID(), uint32(sealedCount))

	return merr.Success(), nil
}

func (node *QueryNode) localGrowingSegmentIDs(channel string, segmentIDs []int64) []int64 {
	filters := []segments.SegmentFilter{
		segments.WithChannel(channel),
		segments.WithType(segments.SegmentTypeGrowing),
	}
	if len(segmentIDs) > 0 {
		filters = append(filters, segments.WithIDs(segmentIDs...))
	}
	return lo.Map(node.manager.Segment.GetBy(filters...), func(segment segments.Segment, _ int) int64 {
		return segment.ID()
	})
}

func (node *QueryNode) prepareReleaseManualFlush(ctx context.Context, collectionID int64, channel string, segmentIDs []int64) (bool, error) {
	segmentIDs = lo.Uniq(lo.Filter(segmentIDs, func(segmentID int64, _ int) bool {
		return segmentID > 0 && !syncmgr.DefaultGrowingSourceRegistry().IsReleasePrepared(channel, segmentID, 0)
	}))
	wal := streaming.WAL()
	if wal == nil {
		return false, merr.WrapErrServiceUnavailable("streaming WAL is not initialized")
	}
	return wal.Local().PrepareReleaseManualFlushIfLocal(ctx, collectionID, channel, segmentIDs)
}

func isReleaseManualFlushPrepareUnavailable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, merr.ErrServiceUnavailable) ||
		errors.Is(err, merr.ErrChannelNotAvailable) ||
		errors.Is(err, handler.ErrClientClosed) ||
		errors.Is(err, handler.ErrReadOnlyWAL) ||
		errors.Is(err, registry.ErrNoStreamingNodeDeployed) ||
		errors.Is(err, registry.ErrNoReleaseManualFlushPreparer) {
		return true
	}
	streamingErr := streamingstatus.AsStreamingError(err)
	return streamingErr.IsOnShutdown() || streamingErr.IsWrongStreamingNode()
}

// GetSegmentInfo returns segment information of the collection on the queryNode, and the information includes memSize, numRow, indexName, indexID ...
func (node *QueryNode) GetSegmentInfo(ctx context.Context, in *querypb.GetSegmentInfoRequest) (*querypb.GetSegmentInfoResponse, error) {
	if err := node.lifetime.Add(merr.IsHealthy); err != nil {
		return &querypb.GetSegmentInfoResponse{
			Status: merr.Status(err),
		}, nil
	}
	defer node.lifetime.Done()

	var segmentInfos []*querypb.SegmentInfo
	for _, segmentID := range in.GetSegmentIDs() {
		segment := node.manager.Segment.Get(segmentID)
		if segment == nil {
			continue
		}
		collection := node.manager.Collection.Get(segment.Collection())
		if collection == nil {
			continue
		}

		// TODO(yah01): now Milvus supports only 1 vector field
		vecFields := funcutil.GetVecFieldIDs(collection.Schema())
		var (
			indexName  string
			indexID    int64
			indexInfos []*querypb.FieldIndexInfo
		)
		for _, field := range vecFields {
			indexes := segment.GetIndex(field)
			if indexes != nil {
				if len(indexes) != 1 {
					mlog.Error(ctx, "only support one index for vector field", mlog.Int64("fieldID", field), mlog.Int("index count", len(indexes)))
					return &querypb.GetSegmentInfoResponse{
						Status: merr.Status(merr.WrapErrServiceInternal("only support one index for vector field")),
					}, nil
				}
				index := indexes[0]
				indexName = index.IndexInfo.GetIndexName()
				indexID = index.IndexInfo.GetIndexID()
				indexInfos = append(indexInfos, index.IndexInfo)
			}
		}

		info := &querypb.SegmentInfo{
			SegmentID:      segment.ID(),
			SegmentState:   segment.Type(),
			DmChannel:      segment.Shard().VirtualName(),
			PartitionID:    segment.Partition(),
			CollectionID:   segment.Collection(),
			NodeID:         node.GetNodeID(),
			NodeIds:        []int64{node.GetNodeID()},
			MemSize:        segment.MemSize(),
			NumRows:        segment.InsertCount(),
			IndexName:      indexName,
			IndexID:        indexID,
			IndexInfos:     indexInfos,
			StorageVersion: segment.LoadInfo().GetStorageVersion(),
		}
		segmentInfos = append(segmentInfos, info)
	}

	return &querypb.GetSegmentInfoResponse{
		Status: merr.Success(),
		Infos:  segmentInfos,
	}, nil
}

// SearchSegments performs search on segments.
// If req.FilterOnly is true, only executes filter and returns valid count per segment (Stage 1 of two-stage search).
// If req.FilterOnly is false, performs normal vector search and returns search results.
func (node *QueryNode) SearchSegments(ctx context.Context, req *querypb.SearchRequest) (*internalpb.SearchResults, error) {
	channel := req.GetDmlChannels()[0]
	log := mlog.With(
		mlog.Int64("msgID", req.GetReq().GetBase().GetMsgID()),
		mlog.Int64("collectionID", req.Req.GetCollectionID()),
		mlog.String("channel", channel),
		mlog.String("scope", req.GetScope().String()),
	)
	channelsMvcc := make(map[string]uint64)
	for _, ch := range req.GetDmlChannels() {
		channelsMvcc[ch] = req.GetReq().GetMvccTimestamp()
	}
	resp := &internalpb.SearchResults{
		ChannelsMvcc: channelsMvcc,
	}
	if err := node.lifetime.Add(merr.IsHealthy); err != nil {
		resp.Status = merr.Status(err)
		return resp, nil
	}
	defer node.lifetime.Done()

	nodeIDStr := paramtable.GetStringNodeID()
	collIDStr := strconv.FormatInt(req.GetReq().GetCollectionID(), 10)
	metrics.QueryNodeSQCount.WithLabelValues(nodeIDStr, metrics.SearchLabel, metrics.TotalLabel, metrics.FromLeader, collIDStr).Inc()
	defer func() {
		if !merr.Ok(resp.GetStatus()) {
			metrics.QueryNodeSQCount.WithLabelValues(nodeIDStr, metrics.SearchLabel, metrics.FailLabel, metrics.FromLeader, collIDStr).Inc()
		}
	}()

	log.Debug(ctx, "start to search segments on worker",
		mlog.Int64s("segmentIDs", req.GetSegmentIDs()),
	)
	searchCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	tr := timerecord.NewTimeRecorder("searchSegments")
	log.Debug(ctx, "search segments...")

	if !node.manager.Collection.Ref(req.Req.GetCollectionID(), 1) {
		err := merr.WrapErrCollectionNotLoaded(req.GetReq().GetCollectionID())
		log.Warn(ctx, "failed to search segments", mlog.Err(err))
		resp.Status = merr.Status(err)
		return resp, nil
	}
	collection := node.manager.Collection.Get(req.Req.GetCollectionID())
	defer func() {
		node.manager.Collection.Unref(req.GetReq().GetCollectionID(), 1)
	}()

	task := tasks.NewSearchTask(searchCtx, collection, node.manager, req, node.serverID)

	if err := node.scheduler.Add(task); err != nil {
		log.Warn(ctx, "failed to search channel", mlog.Err(err))
		resp.Status = merr.Status(err)
		return resp, nil
	}

	err := task.Wait()
	if err != nil {
		log.Warn(ctx, "failed to search segments", mlog.Err(err))
		resp.Status = merr.Status(err)
		return resp, nil
	}

	tr.CtxElapse(ctx, "search segments done")

	latency := tr.ElapseSpan()
	metrics.QueryNodeSQReqLatency.WithLabelValues(nodeIDStr, metrics.SearchLabel, metrics.FromLeader).Observe(float64(latency.Milliseconds()))
	metrics.QueryNodeSQCount.WithLabelValues(nodeIDStr, metrics.SearchLabel, metrics.SuccessLabel, metrics.FromLeader, collIDStr).Inc()

	resp = task.SearchResult()
	resp.GetCostAggregation().ResponseTime = tr.ElapseSpan().Milliseconds()
	resp.GetCostAggregation().TotalNQ = node.scheduler.GetWaitingTaskTotalNQ()
	if req.GetReq().GetIsTopkReduce() {
		resp.IsTopkReduce = true
	}
	resp.IsRecallEvaluation = req.GetReq().GetIsRecallEvaluation()
	return resp, nil
}

// Search performs replica search tasks.
func (node *QueryNode) Search(ctx context.Context, req *querypb.SearchRequest) (*internalpb.SearchResults, error) {
	log := mlog.With(
		mlog.Int64("collectionID", req.GetReq().GetCollectionID()),
		mlog.Strings("channels", req.GetDmlChannels()),
		mlog.Int64("nq", req.GetReq().GetNq()),
	)

	log.Debug(ctx, "Received SearchRequest",
		mlog.Int64s("segmentIDs", req.GetSegmentIDs()),
		mlog.Uint64("guaranteeTimestamp", req.GetReq().GetGuaranteeTimestamp()),
		mlog.Uint64("mvccTimestamp", req.GetReq().GetMvccTimestamp()))

	tr := timerecord.NewTimeRecorderWithTrace(ctx, "SearchRequest")

	if err := node.lifetime.Add(merr.IsHealthy); err != nil {
		return &internalpb.SearchResults{
			Status: merr.Status(err),
		}, nil
	}
	defer node.lifetime.Done()

	resp := &internalpb.SearchResults{
		Status: merr.Success(),
	}
	collection := node.manager.Collection.Get(req.GetReq().GetCollectionID())
	if collection == nil {
		resp.Status = merr.Status(merr.WrapErrCollectionNotLoaded(req.GetReq().GetCollectionID()))
		return resp, nil
	}

	if len(req.GetDmlChannels()) != 1 {
		// internal protocol assertion: the proxy always targets exactly one
		// channel per request, so a violation is a Milvus bug, not user input
		err := merr.WrapErrServiceInternalMsg("count of channels to be searched should only be 1, got %d, wrong code", len(req.GetDmlChannels()))
		resp.Status = merr.Status(err)
		log.Warn(ctx, "got wrong number of channels to be searched", mlog.Err(err))
		return resp, nil
	}

	ch := req.GetDmlChannels()[0]
	channelReq := &querypb.SearchRequest{
		Req:             req.Req,
		DmlChannels:     []string{ch},
		SegmentIDs:      req.SegmentIDs,
		Scope:           req.Scope,
		TotalChannelNum: req.TotalChannelNum,
	}
	ret, err := node.searchChannel(ctx, channelReq, ch)
	if err != nil {
		resp.Status = merr.Status(err)
		return resp, nil
	}

	ret.Status = merr.Success()

	metrics.QueryNodeExecuteCounter.WithLabelValues(strconv.FormatInt(node.GetNodeID(), 10), metrics.SearchLabel).
		Add(float64(proto.Size(req)))

	if ret.GetCostAggregation() != nil {
		ret.GetCostAggregation().ResponseTime = tr.ElapseSpan().Milliseconds()
	}
	return ret, nil
}

// only used for delegator query segments from worker
func (node *QueryNode) QuerySegments(ctx context.Context, req *querypb.QueryRequest) (*internalpb.RetrieveResults, error) {
	resp := &internalpb.RetrieveResults{
		Status: merr.Success(),
	}
	msgID := req.Req.Base.GetMsgID()
	traceID := trace.SpanFromContext(ctx).SpanContext().TraceID()
	channel := req.GetDmlChannels()[0]

	queryLabel := req.GetReq().GetQueryLabel()
	if queryLabel == "" {
		queryLabel = metrics.QueryLabel
	}
	ctx = contextutil.WithQueryLabel(ctx, queryLabel)

	log := mlog.With(
		mlog.Int64("msgID", msgID),
		mlog.Int64("collectionID", req.GetReq().GetCollectionID()),
		mlog.String("channel", channel),
		mlog.String("scope", req.GetScope().String()),
		mlog.String("queryLabel", queryLabel),
	)

	if err := node.lifetime.Add(merr.IsHealthy); err != nil {
		resp.Status = merr.Status(err)
		return resp, nil
	}
	defer node.lifetime.Done()

	metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(node.GetNodeID()), queryLabel, metrics.TotalLabel, metrics.FromLeader, fmt.Sprint(req.GetReq().GetCollectionID())).Inc()
	defer func() {
		if resp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
			metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(node.GetNodeID()), queryLabel, metrics.FailLabel, metrics.FromLeader, fmt.Sprint(req.GetReq().GetCollectionID())).Inc()
		}
	}()

	log.Debug(ctx, "start do query segments", mlog.Int64s("segmentIDs", req.GetSegmentIDs()))
	// add cancel when error occurs
	queryCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	tr := timerecord.NewTimeRecorder("querySegments")
	if !node.manager.Collection.Ref(req.Req.GetCollectionID(), 1) {
		err := merr.WrapErrCollectionNotLoaded(req.GetReq().GetCollectionID())
		log.Warn(ctx, "failed to query segments", mlog.Err(err))
		resp.Status = merr.Status(err)
		return resp, nil
	}
	collection := node.manager.Collection.Get(req.Req.GetCollectionID())
	defer func() {
		node.manager.Collection.Unref(req.GetReq().GetCollectionID(), 1)
	}()
	// Send task to scheduler and wait until it finished.
	task := tasks.NewQueryTask(queryCtx, collection, node.manager, req)
	if err := node.scheduler.Add(task); err != nil {
		log.Warn(ctx, "failed to add query task into scheduler", mlog.Err(err))
		resp.Status = merr.Status(err)
		return resp, nil
	}
	err := task.Wait()
	if err != nil {
		log.Warn(ctx, "failed to query channel", mlog.Err(err))
		resp.Status = merr.Status(err)
		return resp, nil
	}

	tr.CtxElapse(ctx, fmt.Sprintf("do query done, traceID = %s,  vChannel = %s, segmentIDs = %v",
		traceID,
		channel,
		req.GetSegmentIDs(),
	))

	// TODO QueryNodeSQLatencyInQueue QueryNodeReduceLatency
	latency := tr.ElapseSpan()
	metrics.QueryNodeSQReqLatency.WithLabelValues(fmt.Sprint(node.GetNodeID()), queryLabel, metrics.FromLeader).Observe(float64(latency.Milliseconds()))
	metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(node.GetNodeID()), queryLabel, metrics.SuccessLabel, metrics.FromLeader, fmt.Sprint(req.GetReq().GetCollectionID())).Inc()
	result := task.Result()
	result.GetCostAggregation().ResponseTime = latency.Milliseconds()
	result.GetCostAggregation().TotalNQ = node.scheduler.GetWaitingTaskTotalNQ()
	return result, nil
}

// Query performs replica query tasks.
func (node *QueryNode) Query(ctx context.Context, req *querypb.QueryRequest) (*internalpb.RetrieveResults, error) {
	log := mlog.With(
		mlog.Int64("collectionID", req.GetReq().GetCollectionID()),
		mlog.Strings("shards", req.GetDmlChannels()),
	)
	log.Debug(ctx, "received query request",
		mlog.Int64s("outputFields", req.GetReq().GetOutputFieldsId()),
		mlog.Int64s("segmentIDs", req.GetSegmentIDs()), // should be empty
		mlog.Uint64("guaranteeTimestamp", req.GetReq().GetGuaranteeTimestamp()),
		mlog.Uint64("mvccTimestamp", req.GetReq().GetMvccTimestamp()),
	)

	if err := node.lifetime.Add(merr.IsHealthy); err != nil {
		return &internalpb.RetrieveResults{
			Status: merr.Status(err),
		}, nil
	}
	defer node.lifetime.Done()
	if !node.manager.Collection.Ref(req.GetReq().GetCollectionID(), 1) {
		err := merr.WrapErrCollectionNotLoaded(req.GetReq().GetCollectionID())
		log.Warn(ctx, "failed to query collection", mlog.Err(err))
		return &internalpb.RetrieveResults{
			Status: merr.Status(err),
		}, nil
	}
	defer func() {
		node.manager.Collection.Unref(req.GetReq().GetCollectionID(), 1)
	}()
	if len(req.GetDmlChannels()) != 1 {
		// internal protocol assertion: the proxy always targets exactly one
		// channel per request, so a violation is a Milvus bug, not user input
		return &internalpb.RetrieveResults{
			Status: merr.Status(merr.WrapErrServiceInternalMsg("query request to querynode should "+
				"only target at one channel, but got:%d", len(req.GetDmlChannels()))),
		}, nil
	}
	tr := timerecord.NewTimeRecorderWithTrace(ctx, "QueryRequest")
	defer tr.CtxElapse(ctx, fmt.Sprintf("do query with channel done, vChannel = %s, segmentIDs = %v",
		req.GetDmlChannels()[0],
		req.GetSegmentIDs(),
	))
	res, err := node.queryChannel(ctx, req, req.GetDmlChannels()[0])
	if err != nil {
		return &internalpb.RetrieveResults{
			Status: merr.Status(err),
		}, nil
	}
	return res, nil
}

func (node *QueryNode) QueryStream(req *querypb.QueryRequest, srv querypb.QueryNode_QueryStreamServer) error {
	ctx := srv.Context()
	queryLabel := req.GetReq().GetQueryLabel()
	if queryLabel == "" {
		queryLabel = metrics.QueryLabel
	}
	log := mlog.With(
		mlog.Int64("collectionID", req.GetReq().GetCollectionID()),
		mlog.Strings("shards", req.GetDmlChannels()),
		mlog.String("queryLabel", queryLabel),
	)
	concurrentSrv := streamrpc.NewConcurrentQueryStreamServer(srv)

	log.Debug(ctx, "received query stream request",
		mlog.Int64s("outputFields", req.GetReq().GetOutputFieldsId()),
		mlog.Int64s("segmentIDs", req.GetSegmentIDs()),
		mlog.Uint64("guaranteeTimestamp", req.GetReq().GetGuaranteeTimestamp()),
		mlog.Uint64("mvccTimestamp", req.GetReq().GetMvccTimestamp()),
	)

	if err := node.lifetime.Add(merr.IsHealthy); err != nil {
		concurrentSrv.Send(&internalpb.RetrieveResults{Status: merr.Status(err)})
		return nil
	}
	defer node.lifetime.Done()

	runningGp, runningCtx := errgroup.WithContext(ctx)

	for _, ch := range req.GetDmlChannels() {
		ch := ch
		req := &querypb.QueryRequest{
			Req:         req.Req,
			DmlChannels: []string{ch},
			SegmentIDs:  req.SegmentIDs,
			Scope:       req.Scope,
		}

		runningGp.Go(func() error {
			err := node.queryChannelStream(runningCtx, req, ch, concurrentSrv)
			if err != nil {
				return err
			}
			return nil
		})
	}

	if err := runningGp.Wait(); err != nil {
		concurrentSrv.Send(&internalpb.RetrieveResults{
			Status: merr.Status(err),
		})
		return nil
	}

	metrics.QueryNodeExecuteCounter.WithLabelValues(strconv.FormatInt(node.GetNodeID(), 10), queryLabel).Add(float64(proto.Size(req)))
	return nil
}

func (node *QueryNode) QueryStreamSegments(req *querypb.QueryRequest, srv querypb.QueryNode_QueryStreamSegmentsServer) error {
	ctx := srv.Context()
	msgID := req.Req.Base.GetMsgID()
	traceID := trace.SpanFromContext(ctx).SpanContext().TraceID()
	channel := req.GetDmlChannels()[0]
	concurrentSrv := streamrpc.NewConcurrentQueryStreamServer(srv)

	queryLabel := req.GetReq().GetQueryLabel()
	if queryLabel == "" {
		queryLabel = metrics.QueryLabel
	}

	log := mlog.With(
		mlog.Int64("msgID", msgID),
		mlog.Int64("collectionID", req.GetReq().GetCollectionID()),
		mlog.String("channel", channel),
		mlog.String("scope", req.GetScope().String()),
		mlog.String("queryLabel", queryLabel),
	)

	resp := &internalpb.RetrieveResults{}
	metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(node.GetNodeID()), queryLabel, metrics.TotalLabel, metrics.FromLeader, fmt.Sprint(req.GetReq().GetCollectionID())).Inc()
	defer func() {
		if resp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
			metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(node.GetNodeID()), queryLabel, metrics.FailLabel, metrics.FromLeader, fmt.Sprint(req.GetReq().GetCollectionID())).Inc()
		}
	}()

	if err := node.lifetime.Add(merr.IsHealthy); err != nil {
		resp.Status = merr.Status(err)
		concurrentSrv.Send(resp)
		return nil
	}
	defer node.lifetime.Done()

	log.Debug(ctx, "start do query with channel", mlog.Int64s("segmentIDs", req.GetSegmentIDs()))

	tr := timerecord.NewTimeRecorder("queryChannel")

	err := node.queryStreamSegments(ctx, req, concurrentSrv)
	if err != nil {
		resp.Status = merr.Status(err)
		concurrentSrv.Send(resp)
		return nil
	}

	tr.CtxElapse(ctx, fmt.Sprintf("do query done, traceID = %s,  vChannel = %s, segmentIDs = %v",
		traceID,
		channel,
		req.GetSegmentIDs(),
	))

	// TODO QueryNodeSQLatencyInQueue QueryNodeReduceLatency
	latency := tr.ElapseSpan()
	metrics.QueryNodeSQReqLatency.WithLabelValues(fmt.Sprint(node.GetNodeID()), queryLabel, metrics.FromLeader).Observe(float64(latency.Milliseconds()))
	metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(node.GetNodeID()), queryLabel, metrics.SuccessLabel, metrics.FromLeader, fmt.Sprint(req.GetReq().GetCollectionID())).Inc()
	return nil
}

// SyncReplicaSegments syncs replica node & segments states
func (node *QueryNode) SyncReplicaSegments(ctx context.Context, req *querypb.SyncReplicaSegmentsRequest) (*commonpb.Status, error) {
	return merr.Success(), nil
}

// ShowConfigurations returns the configurations of queryNode matching req.Pattern
func (node *QueryNode) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest) (*internalpb.ShowConfigurationsResponse, error) {
	if err := node.lifetime.Add(merr.IsHealthy); err != nil {
		mlog.Warn(ctx, "QueryNode.ShowConfigurations failed",
			mlog.Int64("nodeId", node.GetNodeID()),
			mlog.String("req", req.Pattern),
			mlog.Err(err))

		return &internalpb.ShowConfigurationsResponse{
			Status:        merr.Status(err),
			Configuations: nil,
		}, nil
	}
	defer node.lifetime.Done()

	configList := make([]*commonpb.KeyValuePair, 0)
	for key, value := range paramtable.Get().GetComponentConfigurations("querynode", req.Pattern) {
		configList = append(configList,
			&commonpb.KeyValuePair{
				Key:   key,
				Value: value,
			})
	}

	return &internalpb.ShowConfigurationsResponse{
		Status:        merr.Success(),
		Configuations: configList,
	}, nil
}

// GetMetrics return system infos of the query node, such as total memory, memory usage, cpu usage ...
func (node *QueryNode) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	if err := node.lifetime.Add(merr.IsHealthy); err != nil {
		mlog.Warn(ctx, "QueryNode.GetMetrics failed",
			mlog.Int64("nodeId", node.GetNodeID()),
			mlog.String("req", req.Request),
			mlog.Err(err))

		return &milvuspb.GetMetricsResponse{
			Status:   merr.Status(err),
			Response: "",
		}, nil
	}
	defer node.lifetime.Done()

	resp := &milvuspb.GetMetricsResponse{
		Status:        merr.Success(),
		ComponentName: metricsinfo.ConstructComponentName(typeutil.QueryNodeRole, paramtable.GetNodeID()),
	}

	ret, err := node.metricsRequest.ExecuteMetricsRequest(ctx, req)
	if err != nil {
		resp.Status = merr.Status(err)
		return resp, nil
	}

	resp.Response = ret
	return resp, nil
}

func (node *QueryNode) GetDataDistribution(ctx context.Context, req *querypb.GetDataDistributionRequest) (*querypb.GetDataDistributionResponse, error) {
	log := mlog.With(
		mlog.Int64("msgID", req.GetBase().GetMsgID()),
		mlog.Int64("nodeID", node.GetNodeID()),
	)
	if err := node.lifetime.Add(merr.IsHealthy); err != nil {
		log.Warn(ctx, "QueryNode.GetDataDistribution failed",
			mlog.Err(err))

		return &querypb.GetDataDistributionResponse{
			Status: merr.Status(err),
		}, nil
	}
	defer node.lifetime.Done()

	lastModifyTs := node.getDistributionModifyTS()
	distributionChange := func() bool {
		if req.GetLastUpdateTs() == 0 {
			return true
		}

		return req.GetLastUpdateTs() < lastModifyTs
	}

	if !distributionChange() {
		return &querypb.GetDataDistributionResponse{
			Status:       merr.Success(),
			NodeID:       node.GetNodeID(),
			LastModifyTs: lastModifyTs,
		}, nil
	}

	sealedSegments := node.manager.Segment.GetBy(segments.WithType(commonpb.SegmentState_Sealed))
	segmentVersionInfos := make([]*querypb.SegmentVersionInfo, 0, len(sealedSegments))
	for _, s := range sealedSegments {
		segmentVersionInfos = append(segmentVersionInfos, &querypb.SegmentVersionInfo{
			ID:                 s.ID(),
			Collection:         s.Collection(),
			Partition:          s.Partition(),
			Channel:            s.Shard().VirtualName(),
			Version:            s.Version(),
			Level:              s.Level(),
			IsSorted:           s.IsSorted(),
			LastDeltaTimestamp: s.LastDeltaTimestamp(),
			IndexInfo: lo.SliceToMap(s.Indexes(), func(info *segments.IndexedFieldInfo) (int64, *querypb.FieldIndexInfo) {
				return info.IndexInfo.IndexID, info.IndexInfo
			}),
			JsonStatsInfo: s.GetFieldJSONIndexStats(),
			ManifestPath:  s.LoadInfo().GetManifestPath(),
			DataVersion:   proto.Int32(s.LoadInfo().GetDataVersion()),
		})
	}

	channelVersionInfos := make([]*querypb.ChannelVersionInfo, 0)
	leaderViews := make([]*querypb.LeaderView, 0)

	node.delegators.Range(func(key string, delegator delegator.ShardDelegator) bool {
		if !delegator.Serviceable() {
			return true
		}
		channelVersionInfos = append(channelVersionInfos, &querypb.ChannelVersionInfo{
			Channel:    key,
			Collection: delegator.Collection(),
			Version:    delegator.Version(),
		})

		sealed, growing := delegator.GetSegmentInfo(false)
		sealedSegments := make(map[int64]*querypb.SegmentDist)
		for _, item := range sealed {
			for _, segment := range item.Segments {
				sealedSegments[segment.SegmentID] = &querypb.SegmentDist{
					NodeID:  item.NodeID,
					Version: segment.Version,
				}
			}
		}

		numOfGrowingRows := int64(0)
		growingSegments := make(map[int64]*msgpb.MsgPosition)
		for _, entry := range growing {
			segment := node.manager.Segment.GetWithType(entry.SegmentID, segments.SegmentTypeGrowing)
			if segment == nil {
				log.Warn(ctx, "leader view growing not found", mlog.String("channel", key), mlog.Int64("segmentID", entry.SegmentID))
				growingSegments[entry.SegmentID] = &msgpb.MsgPosition{}
				continue
			}
			growingSegments[entry.SegmentID] = segment.StartPosition()
			numOfGrowingRows += segment.InsertCount()
		}

		queryView := delegator.GetChannelQueryView()
		leaderViews = append(leaderViews, &querypb.LeaderView{
			Collection:             delegator.Collection(),
			Channel:                key,
			SegmentDist:            sealedSegments,
			GrowingSegments:        growingSegments,
			NumOfGrowingRows:       numOfGrowingRows,
			PartitionStatsVersions: delegator.GetPartitionStatsVersions(ctx),
			TargetVersion:          queryView.GetVersion(),
			Status: &querypb.LeaderViewStatus{
				Serviceable:             queryView.Serviceable(),
				CatchingUpStreamingData: delegator.CatchingUpStreamingData(),
			},
		})
		return true
	})

	return &querypb.GetDataDistributionResponse{
		Status:          merr.Success(),
		NodeID:          node.GetNodeID(),
		Segments:        segmentVersionInfos,
		Channels:        channelVersionInfos,
		LeaderViews:     leaderViews,
		LastModifyTs:    lastModifyTs,
		MemCapacityInMB: float64(hardware.GetMemoryCount() / 1024 / 1024),
		CpuNum:          int64(hardware.GetCPUNum()),
	}, nil
}

func (node *QueryNode) SyncDistribution(ctx context.Context, req *querypb.SyncDistributionRequest) (*commonpb.Status, error) {
	defer node.updateDistributionModifyTS()

	log := mlog.With(mlog.Int64("collectionID", req.GetCollectionID()),
		mlog.String("channel", req.GetChannel()), mlog.Int64("currentNodeID", node.GetNodeID()))
	// check node healthy
	if err := node.lifetime.Add(merr.IsHealthy); err != nil {
		return merr.Status(err), nil
	}
	defer node.lifetime.Done()

	// get shard delegator
	shardDelegator, ok := node.delegators.Get(req.GetChannel())
	if !ok {
		err := merr.WrapErrChannelNotFound(req.GetChannel())
		log.Warn(ctx, "failed to find shard cluster when sync")
		return merr.Status(err), nil
	}

	// translate segment action
	removeActions := make([]*querypb.SyncAction, 0)
	group, ctx := errgroup.WithContext(ctx)
	for _, action := range req.GetActions() {
		log := mlog.With(mlog.String("Action",
			action.GetType().String()))
		switch action.GetType() {
		case querypb.SyncType_Remove:
			log.Info(ctx, "sync action", mlog.Int64("segmentID", action.SegmentID))
			removeActions = append(removeActions, action)
		case querypb.SyncType_Set:
			log.Info(ctx, "sync action", mlog.Int64("segmentID", action.SegmentID))
			if action.GetInfo() == nil {
				log.Warn(ctx, "sync request from legacy querycoord without load info, skip")
				continue
			}

			// to pass segment'version, we call load segment one by one
			action := action
			group.Go(func() error {
				return shardDelegator.LoadSegments(ctx, &querypb.LoadSegmentsRequest{
					Base: commonpbutil.NewMsgBase(
						commonpbutil.WithMsgType(commonpb.MsgType_LoadSegments),
						commonpbutil.WithMsgID(req.Base.GetMsgID()),
					),
					Infos:         []*querypb.SegmentLoadInfo{action.GetInfo()},
					Schema:        req.GetSchema(),
					LoadMeta:      req.GetLoadMeta(),
					CollectionID:  req.GetCollectionID(),
					ReplicaID:     req.GetReplicaID(),
					DstNodeID:     action.GetNodeID(),
					Version:       action.GetVersion(),
					NeedTransfer:  false,
					LoadScope:     querypb.LoadScope_Delta,
					IndexInfoList: req.GetIndexInfoList(),
				})
			})
		case querypb.SyncType_UpdateVersion:
			// Version compatibility check: reject messages with inconsistent sealed segment fields
			// In v2.6, SealedInTarget and SealedSegmentRowCount have consistent keys (same length)
			// A mismatch indicates the message is from v2.5 which lacks SealedSegmentRowCount
			if len(action.GetSealedInTarget()) != len(action.GetSealedSegmentRowCount()) {
				log.Warn(ctx, "Reject syncTargetVersion from older version Coordinator",
					mlog.String("channel", req.GetChannel()),
					mlog.Int("sealedInTarget", len(action.GetSealedInTarget())),
					mlog.Int("sealedSegmentRowCount", len(action.GetSealedSegmentRowCount())),
				)
				continue
			}

			log.Info(ctx, "sync action",
				mlog.Int64("TargetVersion", action.GetTargetVersion()),
				mlog.Time("checkPoint", tsoutil.PhysicalTime(action.GetCheckpoint().GetTimestamp())),
				mlog.Time("deleteCP", tsoutil.PhysicalTime(action.GetDeleteCP().GetTimestamp())),
				mlog.Int64s("partitions", req.GetLoadMeta().GetPartitionIDs()))
			droppedInfos := lo.SliceToMap(action.GetDroppedInTarget(), func(id int64) (int64, uint64) {
				if action.GetCheckpoint() == nil {
					return id, typeutil.MaxTimestamp
				}
				return id, action.GetCheckpoint().Timestamp
			})
			shardDelegator.AddExcludedSegments(droppedInfos)
			flushedInfo := lo.SliceToMap(action.GetSealedInTarget(), func(id int64) (int64, uint64) {
				if action.GetCheckpoint() == nil {
					return id, typeutil.MaxTimestamp
				}
				return id, action.GetCheckpoint().Timestamp
			})
			shardDelegator.AddExcludedSegments(flushedInfo)
			shardDelegator.SyncTargetVersion(action, req.GetLoadMeta().GetPartitionIDs())
		case querypb.SyncType_UpdatePartitionStats:
			log.Info(ctx, "sync update partition stats versions")
			shardDelegator.SyncPartitionStats(ctx, action.PartitionStatsVersions)
		default:
			return merr.Status(merr.WrapErrServiceInternal("unknown action type", action.GetType().String())), nil
		}
	}

	err := group.Wait()
	if err != nil {
		log.Warn(ctx, "failed to sync distribution", mlog.Err(err))
		return merr.Status(err), nil
	}

	// in case of target node offline, when try to remove segment from leader's distribution, use wildcardNodeID(-1) to skip nodeID check
	for _, action := range removeActions {
		shardDelegator.ReleaseSegments(ctx, &querypb.ReleaseSegmentsRequest{
			NodeID:       -1,
			SegmentIDs:   []int64{action.GetSegmentID()},
			Scope:        querypb.DataScope_Historical,
			CollectionID: req.GetCollectionID(),
		}, true)
	}

	return merr.Success(), nil
}

// Delete is used to forward delete message between delegator and workers.
func (node *QueryNode) Delete(ctx context.Context, req *querypb.DeleteRequest) (*commonpb.Status, error) {
	log := mlog.With(
		mlog.Int64("collectionID", req.GetCollectionId()),
		mlog.String("channel", req.GetVchannelName()),
		mlog.Int64("segmentID", req.GetSegmentId()),
		mlog.String("scope", req.GetScope().String()),
	)

	// check node healthy
	if err := node.lifetime.Add(merr.IsHealthy); err != nil {
		return merr.Status(err), nil
	}
	defer node.lifetime.Done()

	log.Debug(ctx, "QueryNode received worker delete detail", mlog.Stringer("info", &deleteRequestStringer{DeleteRequest: req}))

	filters := []segments.SegmentFilter{
		segments.WithID(req.GetSegmentId()),
	}

	// do not add filter for Unknown & All scope, for backward cap
	switch req.GetScope() {
	case querypb.DataScope_Historical:
		filters = append(filters, segments.WithType(segments.SegmentTypeSealed))
	case querypb.DataScope_Streaming:
		filters = append(filters, segments.WithType(segments.SegmentTypeGrowing))
	}

	segments := node.manager.Segment.GetBy(filters...)
	if len(segments) == 0 {
		err := merr.WrapErrSegmentNotFound(req.GetSegmentId())
		log.Warn(ctx, "segment not found for delete")
		return merr.Status(err), nil
	}

	pks := storage.ParseIDs2PrimaryKeysBatch(req.GetPrimaryKeys())
	var err error
	for _, segment := range segments {
		if req.GetUseLoad() {
			var dd *storage.DeltaData
			dd, err = storage.NewDeltaDataWithData(pks, req.GetTimestamps())
			if err == nil {
				err = segment.LoadDeltaData(ctx, dd)
			}
		} else {
			err = segment.Delete(ctx, pks, req.GetTimestamps())
		}
		if err != nil {
			log.Warn(ctx, "segment delete failed", mlog.Err(err))
			return merr.Status(err), nil
		}
	}

	return merr.Success(), nil
}

// DeleteBatch is the API to apply same delete data into multiple segments.
// it's basically same as `Delete` but cost less memory pressure.
func (node *QueryNode) DeleteBatch(ctx context.Context, req *querypb.DeleteBatchRequest) (*querypb.DeleteBatchResponse, error) {
	log := mlog.With(
		mlog.Int64("collectionID", req.GetCollectionId()),
		mlog.String("channel", req.GetVchannelName()),
		mlog.Int64s("segmentIDs", req.GetSegmentIds()),
		mlog.String("scope", req.GetScope().String()),
	)

	// check node healthy
	if err := node.lifetime.Add(merr.IsHealthy); err != nil {
		return &querypb.DeleteBatchResponse{
			Status: merr.Status(err),
		}, nil
	}
	defer node.lifetime.Done()

	// log.Debug(ctx, "QueryNode received worker delete detail", mlog.Stringer("info", &deleteRequestStringer{DeleteRequest: req}))

	filters := []segments.SegmentFilter{
		segments.WithIDs(req.GetSegmentIds()...),
	}

	// do not add filter for Unknown & All scope, for backward cap
	switch req.GetScope() {
	case querypb.DataScope_Historical:
		filters = append(filters, segments.WithType(segments.SegmentTypeSealed))
	case querypb.DataScope_Streaming:
		filters = append(filters, segments.WithType(segments.SegmentTypeGrowing))
	}

	segs := node.manager.Segment.GetBy(filters...)

	hitIDs := lo.Map(segs, func(segment segments.Segment, _ int) int64 {
		return segment.ID()
	})
	// calculate missing ids, continue to delete existing ones.
	missingIDs := typeutil.NewSet(req.GetSegmentIds()...).Complement(typeutil.NewSet(hitIDs...))
	if missingIDs.Len() > 0 {
		log.Warn(ctx, "Delete batch find missing ids", mlog.Int64s("missing_ids", missingIDs.Collect()))
	}

	pks := storage.ParseIDs2PrimaryKeysBatch(req.GetPrimaryKeys())

	// control the execution batch parallel with P number
	// maybe it shall be lower in case of heavy CPU usage may impacting search/query
	pool := segments.GetDeletePool()
	futures := make([]*conc.Future[struct{}], 0, len(segs))
	errSet := typeutil.NewConcurrentSet[int64]()

	for _, segment := range segs {
		segment := segment
		futures = append(futures, pool.Submit(func() (struct{}, error) {
			// TODO @silverxia, add interface to use same data struct for segment delete
			// current implementation still copys pks into protobuf(or arrow) struct
			err := segment.Delete(ctx, pks, req.GetTimestamps())
			if err != nil {
				errSet.Insert(segment.ID())
				log.Warn(ctx, "segment delete failed",
					mlog.Int64("segmentID", segment.ID()),
					mlog.Err(err))
				return struct{}{}, err
			}
			return struct{}{}, nil
		}))
	}

	// ignore error returned, since error segment is recorded into error set
	_ = conc.AwaitAll(futures...)

	// return merr.Success(), nil
	return &querypb.DeleteBatchResponse{
		Status:    merr.Success(),
		FailedIds: errSet.Collect(),
	}, nil
}

func (node *QueryNode) runAnalyzer(req *querypb.RunAnalyzerRequest) ([]*milvuspb.AnalyzerResult, error) {
	tokenizer, err := analyzer.NewAnalyzer(req.GetAnalyzerParams(), "")
	if err != nil {
		return nil, err
	}

	defer tokenizer.Destroy()

	results := make([]*milvuspb.AnalyzerResult, len(req.GetPlaceholder()))
	for i, text := range req.GetPlaceholder() {
		stream := tokenizer.NewTokenStream(string(text))

		results[i] = &milvuspb.AnalyzerResult{
			Tokens: make([]*milvuspb.AnalyzerToken, 0),
		}

		for stream.Advance() {
			var token *milvuspb.AnalyzerToken
			if req.GetWithDetail() {
				token = stream.DetailedToken()
			} else {
				token = &milvuspb.AnalyzerToken{Token: stream.Token()}
			}

			if req.GetWithHash() {
				token.Hash = typeutil.HashString2LessUint32(token.GetToken())
			}
			results[i].Tokens = append(results[i].Tokens, token)
		}
		stream.Destroy()
	}
	return results, nil
}

func (node *QueryNode) RunAnalyzer(ctx context.Context, req *querypb.RunAnalyzerRequest) (*milvuspb.RunAnalyzerResponse, error) {
	// check node healthy
	if err := node.lifetime.Add(merr.IsHealthy); err != nil {
		return &milvuspb.RunAnalyzerResponse{
			Status: merr.Status(err),
		}, nil
	}
	defer node.lifetime.Done()

	// build and run analyzer by analyzer params
	// if channel not set
	if req.GetChannel() == "" {
		result, err := node.runAnalyzer(req)
		return &milvuspb.RunAnalyzerResponse{
			Status:  merr.Status(err),
			Results: result,
		}, nil
	}

	// run analyzer by delegator
	// get delegator
	sd, ok := node.delegators.Get(req.GetChannel())
	if !ok {
		err := merr.WrapErrChannelNotFound(req.GetChannel())
		mlog.Warn(ctx, "RunAnalyzer failed, failed to get shard delegator", mlog.Err(err))
		return &milvuspb.RunAnalyzerResponse{
			Status: merr.Status(err),
		}, nil
	}
	// run analyzer
	results, err := sd.RunAnalyzer(ctx, req)
	if err != nil {
		mlog.Warn(ctx, "failed to search on delegator", mlog.Err(err))
		return &milvuspb.RunAnalyzerResponse{
			Status: merr.Status(err),
		}, nil
	}

	return &milvuspb.RunAnalyzerResponse{
		Status:  merr.Status(nil),
		Results: results,
	}, nil
}

func (node *QueryNode) ValidateAnalyzer(ctx context.Context, req *querypb.ValidateAnalyzerRequest) (*querypb.ValidateAnalyzerResponse, error) {
	// check node healthy
	if err := node.lifetime.Add(merr.IsHealthy); err != nil {
		return &querypb.ValidateAnalyzerResponse{Status: merr.Status(err)}, nil
	}
	defer node.lifetime.Done()

	resourceSet := typeutil.NewSet[int64]()

	for _, info := range req.AnalyzerInfos {
		ids, err := analyzer.ValidateAnalyzer(info.GetParams(), "")
		if err != nil {
			if info.GetName() != "" {
				return &querypb.ValidateAnalyzerResponse{Status: merr.Status(merr.WrapErrParameterInvalidMsg("validate analyzer failed for field: %s, name: %s, error: %v", info.GetField(), info.GetName(), err))}, nil
			}
			return &querypb.ValidateAnalyzerResponse{Status: merr.Status(merr.WrapErrParameterInvalidMsg("validate analyzer failed for field: %s, error: %v", info.GetField(), err))}, nil
		}
		resourceSet.Insert(ids...)
	}

	return &querypb.ValidateAnalyzerResponse{Status: merr.Status(nil), ResourceIds: resourceSet.Collect()}, nil
}

type deleteRequestStringer struct {
	*querypb.DeleteRequest
}

func (req *deleteRequestStringer) String() string {
	var pkInfo string
	switch {
	case req.GetPrimaryKeys().GetIntId() != nil:
		ids := req.GetPrimaryKeys().GetIntId().GetData()
		pkInfo = fmt.Sprintf("Pks range[%d-%d], len: %d", ids[0], ids[len(ids)-1], len(ids))
	case req.GetPrimaryKeys().GetStrId() != nil:
		ids := req.GetPrimaryKeys().GetStrId().GetData()
		pkInfo = fmt.Sprintf("Pks range[%s-%s], len: %d", ids[0], ids[len(ids)-1], len(ids))
	}
	tss := req.GetTimestamps()
	return fmt.Sprintf("%s, timestamp range: [%d-%d]", pkInfo, tss[0], tss[len(tss)-1])
}

func (node *QueryNode) updateDistributionModifyTS() {
	node.lastModifyLock.Lock()
	defer node.lastModifyLock.Unlock()

	node.lastModifyTs = time.Now().UnixNano()
}

func (node *QueryNode) getDistributionModifyTS() int64 {
	node.lastModifyLock.RLock()
	defer node.lastModifyLock.RUnlock()
	return node.lastModifyTs
}

func (node *QueryNode) UpdateIndex(ctx context.Context, req *querypb.UpdateIndexRequest) (*commonpb.Status, error) {
	defer node.updateDistributionModifyTS()
	// UpdateIndex is currently a placeholder implementation
	// The actual logic should handle AddIndex and DropIndex actions
	// For now, return success to satisfy the interface
	return merr.Success(), nil
}

func (node *QueryNode) GetHighlight(ctx context.Context, req *querypb.GetHighlightRequest) (*querypb.GetHighlightResponse, error) {
	// check node healthy
	if err := node.lifetime.Add(merr.IsHealthy); err != nil {
		return &querypb.GetHighlightResponse{
			Status: merr.Status(err),
		}, nil
	}
	defer node.lifetime.Done()

	// get delegator
	sd, ok := node.delegators.Get(req.GetChannel())
	if !ok {
		err := merr.WrapErrChannelNotFound(req.GetChannel())
		mlog.Warn(ctx, "GetHighlight failed, failed to get shard delegator", mlog.Err(err))
		return &querypb.GetHighlightResponse{
			Status: merr.Status(err),
		}, nil
	}

	results, err := sd.GetHighlight(ctx, req)
	if err != nil {
		mlog.Warn(ctx, "GetHighlight failed, delegator run failed", mlog.Err(err))
		return &querypb.GetHighlightResponse{
			Status: merr.Status(err),
		}, nil
	}

	return &querypb.GetHighlightResponse{
		Status:  merr.Success(),
		Results: results,
	}, nil
}

func (node *QueryNode) SyncFileResource(ctx context.Context, req *internalpb.SyncFileResourceRequest) (*commonpb.Status, error) {
	log := mlog.With(mlog.Uint64("version", req.GetVersion()))
	log.Info(ctx, "sync file resource")

	if err := node.lifetime.Add(merr.IsHealthy); err != nil {
		log.Warn(ctx, "failed to sync file resource, QueryNode is not healthy")
		return merr.Status(err), nil
	}
	defer node.lifetime.Done()

	err := fileresource.Sync(req.GetVersion(), req.GetResources())
	if err != nil {
		return merr.Status(err), nil
	}
	return merr.Success(), nil
}

func (node *QueryNode) ClearReadTaskQueue(ctx context.Context, req *internalpb.ClearReadTaskQueueRequest) (*internalpb.ClearReadTaskQueueResponse, error) {
	resp := &internalpb.ClearReadTaskQueueResponse{Status: merr.Success()}
	if err := node.lifetime.Add(merr.IsHealthy); err != nil {
		resp.Status = merr.Status(err)
		return resp, nil
	}
	defer node.lifetime.Done()

	filter, err := queryNodeReadTaskFilter(req.GetTaskType())
	if err != nil {
		resp.Status = merr.Status(err)
		return resp, nil
	}
	result, err := node.scheduler.ClearQueued(ctx, filter, req.GetReason())
	if err != nil {
		resp.Status = merr.Status(err)
		return resp, nil
	}

	resp.QuerynodeQueuedCleared = result.QueuedCleared
	resp.QueuedNqCleared = result.QueuedNQCleared
	resp.Results = append(resp.Results, &internalpb.ClearReadTaskQueueComponentResult{
		Status:          merr.Success(),
		Role:            typeutil.QueryNodeRole,
		NodeID:          node.GetNodeID(),
		QueuedCleared:   result.QueuedCleared,
		QueuedNqCleared: result.QueuedNQCleared,
	})
	mlog.Info(ctx, "cleared querynode read task queue",
		mlog.String("taskType", req.GetTaskType()),
		mlog.String("reason", req.GetReason()),
		mlog.Int64("queuedCleared", result.QueuedCleared),
		mlog.Int64("queuedNQCleared", result.QueuedNQCleared))
	return resp, nil
}

func queryNodeReadTaskFilter(taskType string) (scheduler.TaskFilter, error) {
	switch taskType {
	case "", "all":
		return nil, nil
	case "search":
		return func(task scheduler.Task) bool {
			_, ok := task.(*tasks.SearchTask)
			return ok
		}, nil
	case "query":
		return func(task scheduler.Task) bool {
			_, ok := task.(*tasks.QueryTask)
			return ok
		}, nil
	case "query_stream":
		return func(task scheduler.Task) bool {
			_, ok := task.(*tasks.QueryStreamTask)
			return ok
		}, nil
	default:
		return nil, merr.WrapErrParameterInvalidMsg("unsupported task_type %q", taskType)
	}
}

func (node *QueryNode) ComputePhraseMatchSlop(ctx context.Context, req *querypb.ComputePhraseMatchSlopRequest) (*querypb.ComputePhraseMatchSlopResponse, error) {
	// check node healthy
	if err := node.lifetime.Add(merr.IsHealthy); err != nil {
		return &querypb.ComputePhraseMatchSlopResponse{
			Status: merr.Status(err),
		}, nil
	}
	defer node.lifetime.Done()

	return node.computePhraseMatchSlopByParams(req)
}

func (node *QueryNode) computePhraseMatchSlopByParams(req *querypb.ComputePhraseMatchSlopRequest) (*querypb.ComputePhraseMatchSlopResponse, error) {
	query := req.GetQueryText()
	datas := req.GetDataTexts()

	if query == "" || len(datas) == 0 {
		return &querypb.ComputePhraseMatchSlopResponse{
			Status: merr.Success(), // Empty result
		}, nil
	}

	isMatches := make([]bool, len(datas))
	slops := make([]int64, len(datas))

	for i, data := range datas {
		slop, err := textmatch.ComputePhraseMatchSlop(req.GetAnalyzerParams(), query, data)
		if err != nil {
			isMatches[i] = false
			slops[i] = -1
		} else {
			isMatches[i] = true
			slops[i] = int64(slop)
		}
	}

	return &querypb.ComputePhraseMatchSlopResponse{
		Status:  merr.Success(),
		IsMatch: isMatches,
		Slops:   slops,
	}, nil
}
