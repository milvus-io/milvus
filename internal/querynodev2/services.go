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

	"github.com/golang/protobuf/proto"
	"github.com/samber/lo"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
	"github.com/milvus-io/milvus/internal/querynodev2/collector"
	"github.com/milvus-io/milvus/internal/querynodev2/delegator"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/querynodev2/tasks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/streamrpc"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// GetComponentStates returns information about whether the node is healthy
func (node *QueryNode) GetComponentStates(ctx context.Context, req *milvuspb.GetComponentStatesRequest) (*milvuspb.ComponentStates, error) {
	stats := &milvuspb.ComponentStates{
		Status: merr.Success(),
	}

	code := node.lifetime.GetState()
	nodeID := common.NotRegisteredID

	if node.session != nil && node.session.Registered() {
		nodeID = paramtable.GetNodeID()
	}
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
	log := log.Ctx(ctx).With(
		zap.Int64("msgID", req.GetReq().GetBase().GetMsgID()),
		zap.Strings("vChannels", req.GetDmlChannels()),
		zap.Int64s("segmentIDs", req.GetSegmentIDs()),
		zap.Uint64("guaranteeTimestamp", req.GetReq().GetGuaranteeTimestamp()),
		zap.Uint64("timeTravel", req.GetReq().GetTravelTimestamp()),
	)
	log.Debug("received GetStatisticsRequest")

	if err := node.lifetime.Add(merr.IsHealthy); err != nil {
		return &internalpb.GetStatisticsResponse{
			Status: merr.Status(err),
		}, nil
	}
	defer node.lifetime.Done()

	err := merr.CheckTargetID(req.GetReq().GetBase())
	if err != nil {
		log.Warn("target ID check failed", zap.Error(err))
		return &internalpb.GetStatisticsResponse{
			Status: merr.Status(err),
		}, nil
	}
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
	log.Debug("reduce statistic result done")

	return ret, nil
}

func (node *QueryNode) composeIndexMeta(indexInfos []*indexpb.IndexInfo, schema *schemapb.CollectionSchema) *segcorepb.CollectionIndexMeta {
	fieldIndexMetas := make([]*segcorepb.FieldIndexMeta, 0)
	for _, info := range indexInfos {
		fieldIndexMetas = append(fieldIndexMetas, &segcorepb.FieldIndexMeta{
			CollectionID:    info.GetCollectionID(),
			FieldID:         info.GetFieldID(),
			IndexName:       info.GetIndexName(),
			TypeParams:      info.GetTypeParams(),
			IndexParams:     info.GetIndexParams(),
			IsAutoIndex:     info.GetIsAutoIndex(),
			UserIndexParams: info.GetUserIndexParams(),
		})
	}
	sizePerRecord, err := typeutil.EstimateSizePerRecord(schema)
	maxIndexRecordPerSegment := int64(0)
	if err != nil || sizePerRecord == 0 {
		log.Warn("failed to transfer segment size to collection, because failed to estimate size per record", zap.Error(err))
	} else {
		threshold := paramtable.Get().DataCoordCfg.SegmentMaxSize.GetAsFloat() * 1024 * 1024
		proportion := paramtable.Get().DataCoordCfg.SegmentSealProportion.GetAsFloat()
		maxIndexRecordPerSegment = int64(threshold * proportion / float64(sizePerRecord))
	}

	return &segcorepb.CollectionIndexMeta{
		IndexMetas:       fieldIndexMetas,
		MaxIndexRowCount: maxIndexRecordPerSegment,
	}
}

// WatchDmChannels create consumers on dmChannels to receive Incremental data，which is the important part of real-time query
func (node *QueryNode) WatchDmChannels(ctx context.Context, req *querypb.WatchDmChannelsRequest) (*commonpb.Status, error) {
	channel := req.GetInfos()[0]
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", req.GetCollectionID()),
		zap.String("channel", channel.GetChannelName()),
		zap.Int64("currentNodeID", paramtable.GetNodeID()),
	)

	log.Info("received watch channel request",
		zap.Int64("version", req.GetVersion()),
		zap.String("metricType", req.GetLoadMeta().GetMetricType()),
	)

	// check node healthy
	if err := node.lifetime.Add(merr.IsHealthy); err != nil {
		return merr.Status(err), nil
	}
	defer node.lifetime.Done()

	// check target matches
	if err := merr.CheckTargetID(req.GetBase()); err != nil {
		return merr.Status(err), nil
	}

	// check metric type
	if req.GetLoadMeta().GetMetricType() == "" {
		err := fmt.Errorf("empty metric type, collection = %d", req.GetCollectionID())
		return merr.Status(err), nil
	}

	// check index
	if len(req.GetIndexInfoList()) == 0 {
		err := merr.WrapErrIndexNotFoundForCollection(req.GetSchema().GetName())
		return merr.Status(err), nil
	}

	if !node.subscribingChannels.Insert(channel.GetChannelName()) {
		msg := "channel subscribing..."
		log.Warn(msg)
		return merr.Success(), nil
	}
	defer node.subscribingChannels.Remove(channel.GetChannelName())

	// to avoid concurrent watch/unwatch
	if node.unsubscribingChannels.Contain(channel.GetChannelName()) {
		err := merr.WrapErrChannelReduplicate(channel.GetChannelName(), "the other same channel is unsubscribing")
		log.Warn("failed to unsubscribe channel", zap.Error(err))
		return merr.Status(err), nil
	}

	_, exist := node.delegators.Get(channel.GetChannelName())
	if exist {
		log.Info("channel already subscribed")
		return merr.Success(), nil
	}

	node.manager.Collection.PutOrRef(req.GetCollectionID(), req.GetSchema(),
		node.composeIndexMeta(req.GetIndexInfoList(), req.Schema), req.GetLoadMeta())
	collection := node.manager.Collection.Get(req.GetCollectionID())
	collection.SetMetricType(req.GetLoadMeta().GetMetricType())
	delegator, err := delegator.NewShardDelegator(ctx, req.GetCollectionID(), req.GetReplicaID(), channel.GetChannelName(), req.GetVersion(),
		node.clusterManager, node.manager, node.tSafeManager, node.loader, node.factory, channel.GetSeekPosition().GetTimestamp())
	if err != nil {
		log.Warn("failed to create shard delegator", zap.Error(err))
		return merr.Status(err), nil
	}
	node.delegators.Insert(channel.GetChannelName(), delegator)
	defer func() {
		if err != nil {
			node.delegators.GetAndRemove(channel.GetChannelName())
		}
	}()

	// create tSafe
	node.tSafeManager.Add(ctx, channel.ChannelName, channel.GetSeekPosition().GetTimestamp())
	defer func() {
		if err != nil {
			node.tSafeManager.Remove(ctx, channel.ChannelName)
		}
	}()

	pipeline, err := node.pipelineManager.Add(req.GetCollectionID(), channel.GetChannelName())
	if err != nil {
		msg := "failed to create pipeline"
		log.Warn(msg, zap.Error(err))
		return merr.Status(err), nil
	}
	defer func() {
		if err != nil {
			node.pipelineManager.Remove(channel.GetChannelName())
		}
	}()

	flushedSet := typeutil.NewSet(channel.GetFlushedSegmentIds()...)
	infos := lo.Map(lo.Values(req.GetSegmentInfos()), func(info *datapb.SegmentInfo, _ int) *datapb.SegmentInfo {
		if flushedSet.Contain(info.GetID()) {
			// for flushed segments, exclude all insert data
			info = typeutil.Clone(info)
			info.DmlPosition = &msgpb.MsgPosition{
				Timestamp: typeutil.MaxTimestamp,
			}
		}
		return info
	})
	pipeline.ExcludedSegments(infos...)
	for _, channelInfo := range req.GetInfos() {
		droppedInfos := lo.Map(channelInfo.GetDroppedSegmentIds(), func(id int64, _ int) *datapb.SegmentInfo {
			return &datapb.SegmentInfo{
				ID: id,
				DmlPosition: &msgpb.MsgPosition{
					Timestamp: typeutil.MaxTimestamp,
				},
			}
		})
		pipeline.ExcludedSegments(droppedInfos...)
	}

	err = loadGrowingSegments(ctx, delegator, req)
	if err != nil {
		msg := "failed to load growing segments"
		log.Warn(msg, zap.Error(err))
		return merr.Status(err), nil
	}
	position := &msgpb.MsgPosition{
		ChannelName: channel.SeekPosition.ChannelName,
		MsgID:       channel.SeekPosition.MsgID,
		Timestamp:   channel.SeekPosition.Timestamp,
	}
	err = pipeline.ConsumeMsgStream(position)
	if err != nil {
		err = merr.WrapErrServiceUnavailable(err.Error(), "InitPipelineFailed")
		log.Warn(err.Error(),
			zap.Int64("collectionID", channel.CollectionID),
			zap.String("channel", channel.ChannelName),
		)
		return merr.Status(err), nil
	}

	// start pipeline
	pipeline.Start()
	// delegator after all steps done
	delegator.Start()
	log.Info("watch dml channel success")
	return merr.Success(), nil
}

func (node *QueryNode) UnsubDmChannel(ctx context.Context, req *querypb.UnsubDmChannelRequest) (*commonpb.Status, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", req.GetCollectionID()),
		zap.String("channel", req.GetChannelName()),
		zap.Int64("currentNodeID", paramtable.GetNodeID()),
	)

	log.Info("received unsubscribe channel request")

	// check node healthy
	if err := node.lifetime.Add(merr.IsHealthy); err != nil {
		return merr.Status(err), nil
	}
	defer node.lifetime.Done()

	// check target matches
	if err := merr.CheckTargetID(req.GetBase()); err != nil {
		return merr.Status(err), nil
	}

	node.unsubscribingChannels.Insert(req.GetChannelName())
	defer node.unsubscribingChannels.Remove(req.GetChannelName())
	delegator, ok := node.delegators.GetAndRemove(req.GetChannelName())
	if ok {
		// close the delegator first to block all coming query/search requests
		delegator.Close()

		node.pipelineManager.Remove(req.GetChannelName())
		node.manager.Segment.RemoveBy(segments.WithChannel(req.GetChannelName()), segments.WithType(segments.SegmentTypeGrowing))
		node.tSafeManager.Remove(ctx, req.GetChannelName())

		node.manager.Collection.Unref(req.GetCollectionID(), 1)
	}
	log.Info("unsubscribed channel")

	return merr.Success(), nil
}

func (node *QueryNode) LoadPartitions(ctx context.Context, req *querypb.LoadPartitionsRequest) (*commonpb.Status, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", req.GetCollectionID()),
		zap.Int64s("partitionIDs", req.GetPartitionIDs()),
	)

	log.Info("received load partitions request")
	// check node healthy
	if err := node.lifetime.Add(merr.IsHealthyOrStopping); err != nil {
		return merr.Status(err), nil
	}
	defer node.lifetime.Done()

	collection := node.manager.Collection.Get(req.GetCollectionID())
	if collection != nil {
		collection.AddPartition(req.GetPartitionIDs()...)
	}

	log.Info("load partitions done")
	return merr.Success(), nil
}

// LoadSegments load historical data into query node, historical data can be vector data or index
func (node *QueryNode) LoadSegments(ctx context.Context, req *querypb.LoadSegmentsRequest) (*commonpb.Status, error) {
	segment := req.GetInfos()[0]

	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", segment.GetCollectionID()),
		zap.Int64("partitionID", segment.GetPartitionID()),
		zap.String("shard", segment.GetInsertChannel()),
		zap.Int64("segmentID", segment.GetSegmentID()),
		zap.Int64("currentNodeID", paramtable.GetNodeID()),
	)

	log.Info("received load segments request",
		zap.Int64("version", req.GetVersion()),
		zap.Bool("needTransfer", req.GetNeedTransfer()),
	)
	// check node healthy
	if err := node.lifetime.Add(merr.IsHealthy); err != nil {
		return merr.Status(err), nil
	}
	node.lifetime.Done()

	// check target matches
	if err := merr.CheckTargetID(req.GetBase()); err != nil {
		return merr.Status(err), nil
	}

	// Delegates request to workers
	if req.GetNeedTransfer() {
		delegator, ok := node.delegators.Get(segment.GetInsertChannel())
		if !ok {
			msg := "failed to load segments, delegator not found"
			log.Warn(msg)
			err := merr.WrapErrChannelNotFound(segment.GetInsertChannel())
			return merr.Status(err), nil
		}

		req.NeedTransfer = false
		err := delegator.LoadSegments(ctx, req)
		if err != nil {
			log.Warn("delegator failed to load segments", zap.Error(err))
			return merr.Status(err), nil
		}

		return merr.Success(), nil
	}

	if req.GetLoadScope() == querypb.LoadScope_Delta {
		return node.loadDeltaLogs(ctx, req), nil
	}
	if req.GetLoadScope() == querypb.LoadScope_Index {
		return node.loadIndex(ctx, req), nil
	}

	node.manager.Collection.PutOrRef(req.GetCollectionID(), req.GetSchema(),
		node.composeIndexMeta(req.GetIndexInfoList(), req.GetSchema()), req.GetLoadMeta())
	defer node.manager.Collection.Unref(req.GetCollectionID(), 1)

	// Actual load segment
	log.Info("start to load segments...")
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

	log.Info("load segments done...",
		zap.Int64s("segments", lo.Map(loaded, func(s segments.Segment, _ int) int64 { return s.ID() })))

	return merr.Success(), nil
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
	log := log.Ctx(ctx).With(
		zap.Int64("collection", req.GetCollectionID()),
		zap.Int64s("partitions", req.GetPartitionIDs()),
	)

	log.Info("received release partitions request")

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

	log.Info("release partitions done")
	return merr.Success(), nil
}

// ReleaseSegments remove the specified segments from query node according segmentIDs, partitionIDs, and collectionID
func (node *QueryNode) ReleaseSegments(ctx context.Context, req *querypb.ReleaseSegmentsRequest) (*commonpb.Status, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", req.GetCollectionID()),
		zap.String("shard", req.GetShard()),
		zap.Int64s("segmentIDs", req.GetSegmentIDs()),
		zap.Int64("currentNodeID", paramtable.GetNodeID()),
	)

	log.Info("received release segment request",
		zap.String("scope", req.GetScope().String()),
		zap.Bool("needTransfer", req.GetNeedTransfer()),
	)

	// check node healthy
	if err := node.lifetime.Add(merr.IsHealthyOrStopping); err != nil {
		return merr.Status(err), nil
	}
	defer node.lifetime.Done()

	// check target matches
	if err := merr.CheckTargetID(req.GetBase()); err != nil {
		return merr.Status(err), nil
	}

	if req.GetNeedTransfer() {
		delegator, ok := node.delegators.Get(req.GetShard())
		if !ok {
			msg := "failed to release segment, delegator not found"
			log.Warn(msg)
			err := merr.WrapErrChannelNotFound(req.GetShard())
			return merr.Status(err), nil
		}

		// when we try to release a segment, add it to pipeline's exclude list first
		// in case of consumed it's growing segment again
		pipeline := node.pipelineManager.Get(req.GetShard())
		if pipeline != nil {
			droppedInfos := lo.Map(req.GetSegmentIDs(), func(id int64, _ int) *datapb.SegmentInfo {
				return &datapb.SegmentInfo{
					ID: id,
					DmlPosition: &msgpb.MsgPosition{
						Timestamp: typeutil.MaxTimestamp,
					},
				}
			})
			pipeline.ExcludedSegments(droppedInfos...)
		}

		req.NeedTransfer = false
		err := delegator.ReleaseSegments(ctx, req, false)
		if err != nil {
			log.Warn("delegator failed to release segment", zap.Error(err))
			return merr.Status(err), nil
		}

		return merr.Success(), nil
	}

	log.Info("start to release segments")
	sealedCount := 0
	for _, id := range req.GetSegmentIDs() {
		_, count := node.manager.Segment.Remove(id, req.GetScope())
		sealedCount += count
	}
	node.manager.Collection.Unref(req.GetCollectionID(), uint32(sealedCount))

	return merr.Success(), nil
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
			index := segment.GetIndex(field)
			if index != nil {
				indexName = index.IndexInfo.GetIndexName()
				indexID = index.IndexInfo.GetIndexID()
				indexInfos = append(indexInfos, index.IndexInfo)
			}
		}

		info := &querypb.SegmentInfo{
			SegmentID:    segment.ID(),
			SegmentState: segment.Type(),
			DmChannel:    segment.Shard(),
			PartitionID:  segment.Partition(),
			CollectionID: segment.Collection(),
			NodeID:       paramtable.GetNodeID(),
			NodeIds:      []int64{paramtable.GetNodeID()},
			MemSize:      segment.MemSize(),
			NumRows:      segment.InsertCount(),
			IndexName:    indexName,
			IndexID:      indexID,
			IndexInfos:   indexInfos,
		}
		segmentInfos = append(segmentInfos, info)
	}

	return &querypb.GetSegmentInfoResponse{
		Status: merr.Success(),
		Infos:  segmentInfos,
	}, nil
}

// only used for shard delegator search segments from worker
func (node *QueryNode) SearchSegments(ctx context.Context, req *querypb.SearchRequest) (*internalpb.SearchResults, error) {
	channel := req.GetDmlChannels()[0]
	log := log.Ctx(ctx).With(
		zap.Int64("msgID", req.GetReq().GetBase().GetMsgID()),
		zap.Int64("collectionID", req.Req.GetCollectionID()),
		zap.String("channel", channel),
		zap.String("scope", req.GetScope().String()),
	)

	resp := &internalpb.SearchResults{}
	if err := node.lifetime.Add(merr.IsHealthy); err != nil {
		resp.Status = merr.Status(err)
		return resp, nil
	}
	defer node.lifetime.Done()

	metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.SearchLabel, metrics.TotalLabel, metrics.FromLeader).Inc()
	defer func() {
		if resp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
			metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.SearchLabel, metrics.FailLabel, metrics.FromLeader).Inc()
		}
	}()

	log.Debug("start to search segments on worker",
		zap.Int64s("segmentIDs", req.GetSegmentIDs()),
	)
	searchCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	tr := timerecord.NewTimeRecorder("searchSegments")
	log.Debug("search segments...")

	collection := node.manager.Collection.Get(req.Req.GetCollectionID())
	if collection == nil {
		err := merr.WrapErrCollectionNotLoaded(req.GetReq().GetCollectionID())
		log.Warn("failed to search segments", zap.Error(err))
		resp.Status = merr.Status(err)
		return resp, nil
	}

	task := tasks.NewSearchTask(searchCtx, collection, node.manager, req)
	if err := node.scheduler.Add(task); err != nil {
		log.Warn("failed to search channel", zap.Error(err))
		resp.Status = merr.Status(err)
		return resp, nil
	}

	err := task.Wait()
	if err != nil {
		log.Warn("failed to search segments", zap.Error(err))
		resp.Status = merr.Status(err)
		return resp, nil
	}

	tr.CtxElapse(ctx, fmt.Sprintf("search segments done, channel = %s, segmentIDs = %v",
		channel,
		req.GetSegmentIDs(),
	))

	latency := tr.ElapseSpan()
	metrics.QueryNodeSQReqLatency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.SearchLabel, metrics.FromLeader).Observe(float64(latency.Milliseconds()))
	metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.SearchLabel, metrics.SuccessLabel, metrics.FromLeader).Inc()

	resp = task.Result()
	resp.GetCostAggregation().ResponseTime = tr.ElapseSpan().Milliseconds()
	resp.GetCostAggregation().TotalNQ = node.scheduler.GetWaitingTaskTotalNQ()
	return resp, nil
}

// Search performs replica search tasks.
func (node *QueryNode) Search(ctx context.Context, req *querypb.SearchRequest) (*internalpb.SearchResults, error) {
	if req.FromShardLeader {
		// for compatible with rolling upgrade from version before v2.2.9
		return node.SearchSegments(ctx, req)
	}

	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", req.GetReq().GetCollectionID()),
		zap.Strings("channels", req.GetDmlChannels()),
		zap.Bool("fromShardLeader", req.GetFromShardLeader()),
	)

	log.Debug("Received SearchRequest",
		zap.Int64s("segmentIDs", req.GetSegmentIDs()),
		zap.Uint64("guaranteeTimestamp", req.GetReq().GetGuaranteeTimestamp()))

	tr := timerecord.NewTimeRecorderWithTrace(ctx, "SearchRequest")

	if err := node.lifetime.Add(merr.IsHealthy); err != nil {
		return &internalpb.SearchResults{
			Status: merr.Status(err),
		}, nil
	}
	defer node.lifetime.Done()

	err := merr.CheckTargetID(req.GetReq().GetBase())
	if err != nil {
		log.Warn("target ID check failed", zap.Error(err))
		return &internalpb.SearchResults{
			Status: merr.Status(err),
		}, nil
	}

	failRet := &internalpb.SearchResults{
		Status: merr.Success(),
	}
	collection := node.manager.Collection.Get(req.GetReq().GetCollectionID())
	if collection == nil {
		failRet.Status = merr.Status(merr.WrapErrCollectionNotFound(req.GetReq().GetCollectionID()))
		return failRet, nil
	}

	// Check if the metric type specified in search params matches the metric type in the index info.
	if !req.GetFromShardLeader() && req.GetReq().GetMetricType() != "" {
		if req.GetReq().GetMetricType() != collection.GetMetricType() {
			failRet.Status = merr.Status(merr.WrapErrParameterInvalid(collection.GetMetricType(), req.GetReq().GetMetricType(),
				fmt.Sprintf("collection:%d, metric type not match", collection.ID())))
			return failRet, nil
		}
	}

	// Define the metric type when it has not been explicitly assigned by the user.
	if !req.GetFromShardLeader() && req.GetReq().GetMetricType() == "" {
		req.Req.MetricType = collection.GetMetricType()
	}

	var toReduceResults []*internalpb.SearchResults
	var mu sync.Mutex
	runningGp, runningCtx := errgroup.WithContext(ctx)
	for _, ch := range req.GetDmlChannels() {
		ch := ch
		req := &querypb.SearchRequest{
			Req:             req.Req,
			DmlChannels:     []string{ch},
			SegmentIDs:      req.SegmentIDs,
			FromShardLeader: req.FromShardLeader,
			Scope:           req.Scope,
			TotalChannelNum: req.TotalChannelNum,
		}

		runningGp.Go(func() error {
			ret, err := node.searchChannel(runningCtx, req, ch)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				failRet.Status = merr.Status(err)
				return err
			}
			if ret.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
				return merr.Error(failRet.GetStatus())
			}
			toReduceResults = append(toReduceResults, ret)
			return nil
		})
	}
	if err := runningGp.Wait(); err != nil {
		return failRet, nil
	}

	tr.RecordSpan()
	result, err := segments.ReduceSearchResults(ctx, toReduceResults, req.Req.GetNq(), req.Req.GetTopk(), req.Req.GetMetricType())
	if err != nil {
		log.Warn("failed to reduce search results", zap.Error(err))
		failRet.Status = merr.Status(err)
		return failRet, nil
	}
	reduceLatency := tr.RecordSpan()
	metrics.QueryNodeReduceLatency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.SearchLabel, metrics.ReduceShards).
		Observe(float64(reduceLatency.Milliseconds()))

	collector.Rate.Add(metricsinfo.NQPerSecond, float64(req.GetReq().GetNq()))
	collector.Rate.Add(metricsinfo.SearchThroughput, float64(proto.Size(req)))
	metrics.QueryNodeExecuteCounter.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), metrics.SearchLabel).
		Add(float64(proto.Size(req)))

	if result.GetCostAggregation() != nil {
		result.GetCostAggregation().ResponseTime = tr.ElapseSpan().Milliseconds()
	}
	return result, nil
}

// only used for delegator query segments from worker
func (node *QueryNode) QuerySegments(ctx context.Context, req *querypb.QueryRequest) (*internalpb.RetrieveResults, error) {
	resp := &internalpb.RetrieveResults{
		Status: merr.Success(),
	}
	msgID := req.Req.Base.GetMsgID()
	traceID := trace.SpanFromContext(ctx).SpanContext().TraceID()
	channel := req.GetDmlChannels()[0]
	log := log.Ctx(ctx).With(
		zap.Int64("msgID", msgID),
		zap.Int64("collectionID", req.GetReq().GetCollectionID()),
		zap.String("channel", channel),
		zap.String("scope", req.GetScope().String()),
	)

	if err := node.lifetime.Add(merr.IsHealthy); err != nil {
		resp.Status = merr.Status(err)
		return resp, nil
	}
	defer node.lifetime.Done()

	metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.QueryLabel, metrics.TotalLabel, metrics.FromLeader).Inc()
	defer func() {
		if resp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
			metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.QueryLabel, metrics.FailLabel, metrics.FromLeader).Inc()
		}
	}()

	log.Debug("start do query segments",
		zap.Bool("fromShardLeader", req.GetFromShardLeader()),
		zap.Int64s("segmentIDs", req.GetSegmentIDs()),
	)
	// add cancel when error occurs
	queryCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	tr := timerecord.NewTimeRecorder("querySegments")
	collection := node.manager.Collection.Get(req.Req.GetCollectionID())
	if collection == nil {
		resp.Status = merr.Status(merr.WrapErrCollectionNotLoaded(req.Req.GetCollectionID()))
		return resp, nil
	}

	// Send task to scheduler and wait until it finished.
	task := tasks.NewQueryTask(queryCtx, collection, node.manager, req)
	if err := node.scheduler.Add(task); err != nil {
		log.Warn("failed to add query task into scheduler", zap.Error(err))
		resp.Status = merr.Status(err)
		return resp, nil
	}
	err := task.Wait()
	if err != nil {
		log.Warn("failed to query channel", zap.Error(err))
		resp.Status = merr.Status(err)
		return resp, nil
	}

	tr.CtxElapse(ctx, fmt.Sprintf("do query done, traceID = %s, fromSharedLeader = %t, vChannel = %s, segmentIDs = %v",
		traceID,
		req.GetFromShardLeader(),
		channel,
		req.GetSegmentIDs(),
	))

	// TODO QueryNodeSQLatencyInQueue QueryNodeReduceLatency
	latency := tr.ElapseSpan()
	metrics.QueryNodeSQReqLatency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.QueryLabel, metrics.FromLeader).Observe(float64(latency.Milliseconds()))
	metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.QueryLabel, metrics.SuccessLabel, metrics.FromLeader).Inc()
	result := task.Result()
	result.GetCostAggregation().ResponseTime = latency.Milliseconds()
	result.GetCostAggregation().TotalNQ = node.scheduler.GetWaitingTaskTotalNQ()
	return result, nil
}

// Query performs replica query tasks.
func (node *QueryNode) Query(ctx context.Context, req *querypb.QueryRequest) (*internalpb.RetrieveResults, error) {
	if req.FromShardLeader {
		// for compatible with rolling upgrade from version before v2.2.9
		return node.QuerySegments(ctx, req)
	}

	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", req.GetReq().GetCollectionID()),
		zap.Strings("shards", req.GetDmlChannels()),
	)

	log.Debug("received query request",
		zap.Int64s("outputFields", req.GetReq().GetOutputFieldsId()),
		zap.Int64s("segmentIDs", req.GetSegmentIDs()),
		zap.Uint64("guaranteeTimestamp", req.GetReq().GetGuaranteeTimestamp()),
		zap.Uint64("mvccTimestamp", req.GetReq().GetMvccTimestamp()),
		zap.Bool("isCount", req.GetReq().GetIsCount()),
	)
	tr := timerecord.NewTimeRecorderWithTrace(ctx, "QueryRequest")

	if err := node.lifetime.Add(merr.IsHealthy); err != nil {
		return &internalpb.RetrieveResults{
			Status: merr.Status(err),
		}, nil
	}
	defer node.lifetime.Done()

	err := merr.CheckTargetID(req.GetReq().GetBase())
	if err != nil {
		log.Warn("target ID check failed", zap.Error(err))
		return &internalpb.RetrieveResults{
			Status: merr.Status(err),
		}, nil
	}

	toMergeResults := make([]*internalpb.RetrieveResults, len(req.GetDmlChannels()))
	runningGp, runningCtx := errgroup.WithContext(ctx)

	for i, ch := range req.GetDmlChannels() {
		ch := ch
		req := &querypb.QueryRequest{
			Req:             req.Req,
			DmlChannels:     []string{ch},
			SegmentIDs:      req.SegmentIDs,
			FromShardLeader: req.FromShardLeader,
			Scope:           req.Scope,
		}

		idx := i
		runningGp.Go(func() error {
			ret, err := node.queryChannel(runningCtx, req, ch)
			if err == nil {
				err = merr.Error(ret.GetStatus())
			}
			if err != nil {
				return err
			}
			toMergeResults[idx] = ret
			return nil
		})
	}
	if err := runningGp.Wait(); err != nil {
		return &internalpb.RetrieveResults{
			Status: merr.Status(err),
		}, nil
	}

	tr.RecordSpan()
	reducer := segments.CreateInternalReducer(req, node.manager.Collection.Get(req.GetReq().GetCollectionID()).Schema())
	ret, err := reducer.Reduce(ctx, toMergeResults)
	if err != nil {
		return &internalpb.RetrieveResults{
			Status: merr.Status(err),
		}, nil
	}
	reduceLatency := tr.RecordSpan()
	metrics.QueryNodeReduceLatency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.QueryLabel, metrics.ReduceShards).
		Observe(float64(reduceLatency.Milliseconds()))

	if !req.FromShardLeader {
		collector.Rate.Add(metricsinfo.NQPerSecond, 1)
		metrics.QueryNodeExecuteCounter.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), metrics.QueryLabel).Add(float64(proto.Size(req)))
	}

	if ret.GetCostAggregation() != nil {
		ret.GetCostAggregation().ResponseTime = tr.ElapseSpan().Milliseconds()
	}
	return ret, nil
}

func (node *QueryNode) QueryStream(req *querypb.QueryRequest, srv querypb.QueryNode_QueryStreamServer) error {
	ctx := srv.Context()
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", req.GetReq().GetCollectionID()),
		zap.Strings("shards", req.GetDmlChannels()),
	)
	concurrentSrv := streamrpc.NewConcurrentQueryStreamServer(srv)

	log.Debug("received query stream request",
		zap.Int64s("outputFields", req.GetReq().GetOutputFieldsId()),
		zap.Int64s("segmentIDs", req.GetSegmentIDs()),
		zap.Uint64("guaranteeTimestamp", req.GetReq().GetGuaranteeTimestamp()),
		zap.Uint64("mvccTimestamp", req.GetReq().GetMvccTimestamp()),
		zap.Bool("isCount", req.GetReq().GetIsCount()),
	)

	if err := node.lifetime.Add(merr.IsHealthy); err != nil {
		concurrentSrv.Send(&internalpb.RetrieveResults{Status: merr.Status(err)})
		return nil
	}
	defer node.lifetime.Done()

	err := merr.CheckTargetID(req.GetReq().GetBase())
	if err != nil {
		log.Warn("target ID check failed", zap.Error(err))
		return err
	}

	runningGp, runningCtx := errgroup.WithContext(ctx)

	for _, ch := range req.GetDmlChannels() {
		ch := ch
		req := &querypb.QueryRequest{
			Req:             req.Req,
			DmlChannels:     []string{ch},
			SegmentIDs:      req.SegmentIDs,
			FromShardLeader: req.FromShardLeader,
			Scope:           req.Scope,
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

	collector.Rate.Add(metricsinfo.NQPerSecond, 1)
	metrics.QueryNodeExecuteCounter.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), metrics.QueryLabel).Add(float64(proto.Size(req)))
	return nil
}

func (node *QueryNode) QueryStreamSegments(req *querypb.QueryRequest, srv querypb.QueryNode_QueryStreamSegmentsServer) error {
	ctx := srv.Context()
	msgID := req.Req.Base.GetMsgID()
	traceID := trace.SpanFromContext(ctx).SpanContext().TraceID()
	channel := req.GetDmlChannels()[0]
	concurrentSrv := streamrpc.NewConcurrentQueryStreamServer(srv)

	log := log.Ctx(ctx).With(
		zap.Int64("msgID", msgID),
		zap.Int64("collectionID", req.GetReq().GetCollectionID()),
		zap.String("channel", channel),
		zap.String("scope", req.GetScope().String()),
	)

	resp := &internalpb.RetrieveResults{}
	metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.QueryLabel, metrics.TotalLabel, metrics.FromLeader).Inc()
	defer func() {
		if resp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
			metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.QueryLabel, metrics.FailLabel, metrics.FromLeader).Inc()
		}
	}()

	if err := node.lifetime.Add(merr.IsHealthy); err != nil {
		resp.Status = merr.Status(err)
		concurrentSrv.Send(resp)
		return nil
	}
	defer node.lifetime.Done()

	log.Debug("start do query with channel",
		zap.Bool("fromShardLeader", req.GetFromShardLeader()),
		zap.Int64s("segmentIDs", req.GetSegmentIDs()),
	)

	tr := timerecord.NewTimeRecorder("queryChannel")

	err := node.queryStreamSegments(ctx, req, concurrentSrv)
	if err != nil {
		resp.Status = merr.Status(err)
		concurrentSrv.Send(resp)
		return nil
	}

	tr.CtxElapse(ctx, fmt.Sprintf("do query done, traceID = %s, fromSharedLeader = %t, vChannel = %s, segmentIDs = %v",
		traceID,
		req.GetFromShardLeader(),
		channel,
		req.GetSegmentIDs(),
	))

	// TODO QueryNodeSQLatencyInQueue QueryNodeReduceLatency
	latency := tr.ElapseSpan()
	metrics.QueryNodeSQReqLatency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.QueryLabel, metrics.FromLeader).Observe(float64(latency.Milliseconds()))
	metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.QueryLabel, metrics.SuccessLabel, metrics.FromLeader).Inc()
	return nil
}

// SyncReplicaSegments syncs replica node & segments states
func (node *QueryNode) SyncReplicaSegments(ctx context.Context, req *querypb.SyncReplicaSegmentsRequest) (*commonpb.Status, error) {
	return merr.Success(), nil
}

// ShowConfigurations returns the configurations of queryNode matching req.Pattern
func (node *QueryNode) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest) (*internalpb.ShowConfigurationsResponse, error) {
	if err := node.lifetime.Add(merr.IsHealthy); err != nil {
		log.Warn("QueryNode.ShowConfigurations failed",
			zap.Int64("nodeId", paramtable.GetNodeID()),
			zap.String("req", req.Pattern),
			zap.Error(err))

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
		log.Warn("QueryNode.GetMetrics failed",
			zap.Int64("nodeId", paramtable.GetNodeID()),
			zap.String("req", req.Request),
			zap.Error(err))

		return &milvuspb.GetMetricsResponse{
			Status:   merr.Status(err),
			Response: "",
		}, nil
	}
	defer node.lifetime.Done()

	metricType, err := metricsinfo.ParseMetricType(req.Request)
	if err != nil {
		log.Warn("QueryNode.GetMetrics failed to parse metric type",
			zap.Int64("nodeId", paramtable.GetNodeID()),
			zap.String("req", req.Request),
			zap.Error(err))

		return &milvuspb.GetMetricsResponse{
			Status: merr.Status(err),
		}, nil
	}

	if metricType == metricsinfo.SystemInfoMetrics {
		queryNodeMetrics, err := getSystemInfoMetrics(ctx, req, node)
		if err != nil {
			log.Warn("QueryNode.GetMetrics failed",
				zap.Int64("nodeId", paramtable.GetNodeID()),
				zap.String("req", req.Request),
				zap.String("metricType", metricType),
				zap.Error(err))
			return &milvuspb.GetMetricsResponse{
				Status: merr.Status(err),
			}, nil
		}
		log.RatedDebug(50, "QueryNode.GetMetrics",
			zap.Int64("nodeID", paramtable.GetNodeID()),
			zap.String("req", req.Request),
			zap.String("metricType", metricType),
			zap.Any("queryNodeMetrics", queryNodeMetrics))

		return queryNodeMetrics, nil
	}

	log.Debug("QueryNode.GetMetrics failed, request metric type is not implemented yet",
		zap.Int64("nodeID", paramtable.GetNodeID()),
		zap.String("req", req.Request),
		zap.String("metricType", metricType))

	return &milvuspb.GetMetricsResponse{
		Status: merr.Status(merr.WrapErrMetricNotFound(metricType)),
	}, nil
}

func (node *QueryNode) GetDataDistribution(ctx context.Context, req *querypb.GetDataDistributionRequest) (*querypb.GetDataDistributionResponse, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("msgID", req.GetBase().GetMsgID()),
		zap.Int64("nodeID", paramtable.GetNodeID()),
	)
	if err := node.lifetime.Add(merr.IsHealthy); err != nil {
		log.Warn("QueryNode.GetDataDistribution failed",
			zap.Error(err))

		return &querypb.GetDataDistributionResponse{
			Status: merr.Status(err),
		}, nil
	}
	defer node.lifetime.Done()

	// check target matches
	if err := merr.CheckTargetID(req.GetBase()); err != nil {
		return &querypb.GetDataDistributionResponse{
			Status: merr.Status(err),
		}, nil
	}

	sealedSegments := node.manager.Segment.GetBy(segments.WithType(commonpb.SegmentState_Sealed))
	segmentVersionInfos := make([]*querypb.SegmentVersionInfo, 0, len(sealedSegments))
	for _, s := range sealedSegments {
		segmentVersionInfos = append(segmentVersionInfos, &querypb.SegmentVersionInfo{
			ID:                 s.ID(),
			Collection:         s.Collection(),
			Partition:          s.Partition(),
			Channel:            s.Shard(),
			Version:            s.Version(),
			LastDeltaTimestamp: s.LastDeltaTimestamp(),
			IndexInfo: lo.SliceToMap(s.Indexes(), func(info *segments.IndexedFieldInfo) (int64, *querypb.FieldIndexInfo) {
				return info.IndexInfo.FieldID, info.IndexInfo
			}),
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

		growingSegments := make(map[int64]*msgpb.MsgPosition)
		for _, entry := range growing {
			segment := node.manager.Segment.GetWithType(entry.SegmentID, segments.SegmentTypeGrowing)
			if segment == nil {
				log.Warn("leader view growing not found", zap.String("channel", key), zap.Int64("segmentID", entry.SegmentID))
				growingSegments[entry.SegmentID] = &msgpb.MsgPosition{}
				continue
			}
			growingSegments[entry.SegmentID] = segment.StartPosition()
		}

		leaderViews = append(leaderViews, &querypb.LeaderView{
			Collection:      delegator.Collection(),
			Channel:         key,
			SegmentDist:     sealedSegments,
			GrowingSegments: growingSegments,
			TargetVersion:   delegator.GetTargetVersion(),
		})
		return true
	})

	return &querypb.GetDataDistributionResponse{
		Status:      merr.Success(),
		NodeID:      paramtable.GetNodeID(),
		Segments:    segmentVersionInfos,
		Channels:    channelVersionInfos,
		LeaderViews: leaderViews,
	}, nil
}

func (node *QueryNode) SyncDistribution(ctx context.Context, req *querypb.SyncDistributionRequest) (*commonpb.Status, error) {
	log := log.Ctx(ctx).With(zap.Int64("collectionID", req.GetCollectionID()),
		zap.String("channel", req.GetChannel()), zap.Int64("currentNodeID", paramtable.GetNodeID()))
	// check node healthy
	if err := node.lifetime.Add(merr.IsHealthy); err != nil {
		return merr.Status(err), nil
	}
	defer node.lifetime.Done()

	// check target matches
	if err := merr.CheckTargetID(req.GetBase()); err != nil {
		return merr.Status(err), nil
	}

	// get shard delegator
	shardDelegator, ok := node.delegators.Get(req.GetChannel())
	if !ok {
		err := merr.WrapErrChannelNotFound(req.GetChannel())
		log.Warn("failed to find shard cluster when sync")
		return merr.Status(err), nil
	}

	// translate segment action
	removeActions := make([]*querypb.SyncAction, 0)
	group, ctx := errgroup.WithContext(ctx)
	for _, action := range req.GetActions() {
		log := log.With(zap.String("Action",
			action.GetType().String()))
		switch action.GetType() {
		case querypb.SyncType_Remove:
			log.Info("sync action", zap.Int64("segmentID", action.SegmentID))
			removeActions = append(removeActions, action)
		case querypb.SyncType_Set:
			log.Info("sync action", zap.Int64("segmentID", action.SegmentID))
			if action.GetInfo() == nil {
				log.Warn("sync request from legacy querycoord without load info, skip")
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
					Infos:        []*querypb.SegmentLoadInfo{action.GetInfo()},
					Schema:       req.GetSchema(),
					LoadMeta:     req.GetLoadMeta(),
					CollectionID: req.GetCollectionID(),
					ReplicaID:    req.GetReplicaID(),
					DstNodeID:    action.GetNodeID(),
					Version:      action.GetVersion(),
					NeedTransfer: false,
					LoadScope:    querypb.LoadScope_Delta,
				})
			})
		case querypb.SyncType_UpdateVersion:
			log.Info("sync action", zap.Int64("TargetVersion", action.GetTargetVersion()))
			pipeline := node.pipelineManager.Get(req.GetChannel())
			if pipeline != nil {
				droppedInfos := lo.Map(action.GetDroppedInTarget(), func(id int64, _ int) *datapb.SegmentInfo {
					return &datapb.SegmentInfo{
						ID: id,
						DmlPosition: &msgpb.MsgPosition{
							Timestamp: typeutil.MaxTimestamp,
						},
					}
				})
				pipeline.ExcludedSegments(droppedInfos...)
			}
			shardDelegator.SyncTargetVersion(action.GetTargetVersion(), action.GetGrowingInTarget(),
				action.GetSealedInTarget(), action.GetDroppedInTarget())
		default:
			return merr.Status(merr.WrapErrServiceInternal("unknown action type", action.GetType().String())), nil
		}
	}

	err := group.Wait()
	if err != nil {
		log.Warn("failed to sync distribution", zap.Error(err))
		return merr.Status(err), nil
	}

	for _, action := range removeActions {
		shardDelegator.ReleaseSegments(ctx, &querypb.ReleaseSegmentsRequest{
			NodeID:       action.GetNodeID(),
			SegmentIDs:   []int64{action.GetSegmentID()},
			Scope:        querypb.DataScope_Historical,
			CollectionID: req.GetCollectionID(),
		}, true)
	}

	return merr.Success(), nil
}

// Delete is used to forward delete message between delegator and workers.
func (node *QueryNode) Delete(ctx context.Context, req *querypb.DeleteRequest) (*commonpb.Status, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", req.GetCollectionId()),
		zap.String("channel", req.GetVchannelName()),
		zap.Int64("segmentID", req.GetSegmentId()),
	)

	// check node healthy
	if err := node.lifetime.Add(merr.IsHealthy); err != nil {
		return merr.Status(err), nil
	}
	defer node.lifetime.Done()

	// check target matches
	if err := merr.CheckTargetID(req.GetBase()); err != nil {
		return merr.Status(err), nil
	}

	log.Info("QueryNode received worker delete request")
	log.Debug("Worker delete detail",
		zap.String("pks", req.GetPrimaryKeys().String()),
		zap.Uint64s("tss", req.GetTimestamps()),
	)

	segments := node.manager.Segment.GetBy(segments.WithID(req.GetSegmentId()))
	if len(segments) == 0 {
		err := merr.WrapErrSegmentNotFound(req.GetSegmentId())
		log.Warn("segment not found for delete")
		return merr.Status(err), nil
	}

	pks := storage.ParseIDs2PrimaryKeys(req.GetPrimaryKeys())
	for _, segment := range segments {
		err := segment.Delete(pks, req.GetTimestamps())
		if err != nil {
			log.Warn("segment delete failed", zap.Error(err))
			return merr.Status(err), nil
		}
	}

	return merr.Success(), nil
}
