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

package querycoordv2

import (
	"context"
	"fmt"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/job"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/errorutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var (
// ErrRemoveNodeFromRGFailed      = errors.New("failed to remove node from resource group")
// ErrTransferNodeFailed          = errors.New("failed to transfer node between resource group")
// ErrTransferReplicaFailed       = errors.New("failed to transfer replica between resource group")
// ErrListResourceGroupsFailed    = errors.New("failed to list resource group")
// ErrDescribeResourceGroupFailed = errors.New("failed to describe resource group")
// ErrLoadUseWrongRG              = errors.New("load operation should use collection's resource group")
// ErrLoadWithDefaultRG           = errors.New("load operation can't use default resource group and other resource group together")
)

func (s *Server) ShowCollections(ctx context.Context, req *querypb.ShowCollectionsRequest) (*querypb.ShowCollectionsResponse, error) {
	log.Ctx(ctx).Info("show collections request received", zap.Int64s("collections", req.GetCollectionIDs()))

	if err := merr.CheckHealthy(s.State()); err != nil {
		msg := "failed to show collections"
		log.Warn(msg, zap.Error(err))
		return &querypb.ShowCollectionsResponse{
			Status: merr.Status(errors.Wrap(err, msg)),
		}, nil
	}
	defer meta.GlobalFailedLoadCache.TryExpire()

	isGetAll := false
	collectionSet := typeutil.NewUniqueSet(req.GetCollectionIDs()...)
	if len(req.GetCollectionIDs()) == 0 {
		for _, collection := range s.meta.GetAllCollections() {
			collectionSet.Insert(collection.GetCollectionID())
		}
		isGetAll = true
	}
	collections := collectionSet.Collect()

	resp := &querypb.ShowCollectionsResponse{
		Status:                &commonpb.Status{},
		CollectionIDs:         make([]int64, 0, len(collectionSet)),
		InMemoryPercentages:   make([]int64, 0, len(collectionSet)),
		QueryServiceAvailable: make([]bool, 0, len(collectionSet)),
	}
	for _, collectionID := range collections {
		log := log.With(zap.Int64("collectionID", collectionID))

		collection := s.meta.CollectionManager.GetCollection(collectionID)
		percentage := s.meta.CollectionManager.CalculateLoadPercentage(collectionID)
		refreshProgress := int64(0)
		if percentage < 0 {
			if isGetAll {
				// The collection is released during this,
				// ignore it
				continue
			}
			err := meta.GlobalFailedLoadCache.Get(collectionID)
			if err != nil {
				msg := "show collection failed"
				log.Warn(msg, zap.Error(err))
				status := merr.Status(errors.Wrap(err, msg))
				status.ErrorCode = commonpb.ErrorCode_InsufficientMemoryToLoad
				return &querypb.ShowCollectionsResponse{
					Status: status,
				}, nil
			}

			err = fmt.Errorf("collection %d has not been loaded to memory or load failed", collectionID)
			log.Warn("show collection failed", zap.Error(err))
			return &querypb.ShowCollectionsResponse{
				Status: merr.Status(err),
			}, nil
		}

		if collection.IsRefreshed() {
			refreshProgress = 100
		}

		resp.CollectionIDs = append(resp.CollectionIDs, collectionID)
		resp.InMemoryPercentages = append(resp.InMemoryPercentages, int64(percentage))
		resp.QueryServiceAvailable = append(resp.QueryServiceAvailable, s.checkAnyReplicaAvailable(collectionID))
		resp.RefreshProgress = append(resp.RefreshProgress, refreshProgress)
	}

	return resp, nil
}

func (s *Server) ShowPartitions(ctx context.Context, req *querypb.ShowPartitionsRequest) (*querypb.ShowPartitionsResponse, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", req.GetCollectionID()),
	)

	log.Info("show partitions request received", zap.Int64s("partitions", req.GetPartitionIDs()))

	if err := merr.CheckHealthy(s.State()); err != nil {
		msg := "failed to show partitions"
		log.Warn(msg, zap.Error(err))
		return &querypb.ShowPartitionsResponse{
			Status: merr.Status(errors.Wrap(err, msg)),
		}, nil
	}
	defer meta.GlobalFailedLoadCache.TryExpire()

	partitions := req.GetPartitionIDs()
	percentages := make([]int64, 0)
	refreshProgress := int64(0)

	if len(partitions) == 0 {
		partitions = lo.Map(s.meta.GetPartitionsByCollection(req.GetCollectionID()), func(partition *meta.Partition, _ int) int64 {
			return partition.GetPartitionID()
		})
	}
	for _, partitionID := range partitions {
		percentage := s.meta.GetPartitionLoadPercentage(partitionID)
		if percentage < 0 {
			err := meta.GlobalFailedLoadCache.Get(req.GetCollectionID())
			if err != nil {
				status := merr.Status(err)
				status.ErrorCode = commonpb.ErrorCode_InsufficientMemoryToLoad
				log.Warn("show partition failed", zap.Error(err))
				return &querypb.ShowPartitionsResponse{
					Status: status,
				}, nil
			}

			err = merr.WrapErrPartitionNotLoaded(partitionID)
			msg := fmt.Sprintf("partition %d has not been loaded to memory or load failed", partitionID)
			log.Warn(msg)
			return &querypb.ShowPartitionsResponse{
				Status: merr.Status(errors.Wrap(err, msg)),
			}, nil
		}
		percentages = append(percentages, int64(percentage))
	}

	collection := s.meta.GetCollection(req.GetCollectionID())
	if collection != nil && collection.IsRefreshed() {
		refreshProgress = 100
	}
	refreshProgresses := make([]int64, len(partitions))
	for i := range partitions {
		refreshProgresses[i] = refreshProgress
	}

	return &querypb.ShowPartitionsResponse{
		Status:              merr.Status(nil),
		PartitionIDs:        partitions,
		InMemoryPercentages: percentages,
		RefreshProgress:     refreshProgresses,
	}, nil
}

func (s *Server) LoadCollection(ctx context.Context, req *querypb.LoadCollectionRequest) (*commonpb.Status, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", req.GetCollectionID()),
		zap.Int32("replicaNumber", req.GetReplicaNumber()),
		zap.Strings("resourceGroups", req.GetResourceGroups()),
		zap.Bool("refreshMode", req.GetRefresh()),
	)

	log.Info("load collection request received",
		zap.Any("schema", req.Schema),
		zap.Int64s("fieldIndexes", lo.Values(req.GetFieldIndexID())),
	)
	metrics.QueryCoordLoadCount.WithLabelValues(metrics.TotalLabel).Inc()

	if err := merr.CheckHealthy(s.State()); err != nil {
		msg := "failed to load collection"
		log.Warn(msg, zap.Error(err))
		metrics.QueryCoordLoadCount.WithLabelValues(metrics.FailLabel).Inc()
		return merr.Status(errors.Wrap(err, msg)), nil
	}

	// If refresh mode is ON.
	if req.GetRefresh() {
		err := s.refreshCollection(req.GetCollectionID())
		if err != nil {
			log.Warn("failed to refresh collection", zap.Error(err))
		}
		return merr.Status(err), nil
	}

	if err := s.checkResourceGroup(req.GetCollectionID(), req.GetResourceGroups()); err != nil {
		msg := "failed to load collection"
		log.Warn(msg, zap.Error(err))
		metrics.QueryCoordLoadCount.WithLabelValues(metrics.FailLabel).Inc()
		return merr.Status(errors.Wrap(err, msg)), nil
	}

	loadJob := job.NewLoadCollectionJob(ctx,
		req,
		s.dist,
		s.meta,
		s.broker,
		s.cluster,
		s.targetMgr,
		s.targetObserver,
		s.nodeMgr,
	)
	s.jobScheduler.Add(loadJob)
	err := loadJob.Wait()
	if err != nil {
		msg := "failed to load collection"
		log.Warn(msg, zap.Error(err))
		metrics.QueryCoordLoadCount.WithLabelValues(metrics.FailLabel).Inc()
		return merr.Status(errors.Wrap(err, msg)), nil
	}

	metrics.QueryCoordLoadCount.WithLabelValues(metrics.SuccessLabel).Inc()
	return merr.Status(nil), nil
}

func (s *Server) ReleaseCollection(ctx context.Context, req *querypb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", req.GetCollectionID()),
	)

	log.Info("release collection request received")
	tr := timerecord.NewTimeRecorder("release-collection")

	if err := merr.CheckHealthy(s.State()); err != nil {
		msg := "failed to release collection"
		log.Warn(msg, zap.Error(err))
		metrics.QueryCoordReleaseCount.WithLabelValues(metrics.FailLabel).Inc()
		return merr.Status(errors.Wrap(err, msg)), nil
	}

	releaseJob := job.NewReleaseCollectionJob(ctx,
		req,
		s.dist,
		s.meta,
		s.broker,
		s.cluster,
		s.targetMgr,
		s.targetObserver,
		s.checkerController,
	)
	s.jobScheduler.Add(releaseJob)
	err := releaseJob.Wait()
	if err != nil {
		msg := "failed to release collection"
		log.Error(msg, zap.Error(err))
		metrics.QueryCoordReleaseCount.WithLabelValues(metrics.FailLabel).Inc()
		return merr.Status(errors.Wrap(err, msg)), nil
	}

	log.Info("collection released")
	metrics.QueryCoordReleaseLatency.WithLabelValues().Observe(float64(tr.ElapseSpan().Milliseconds()))
	meta.GlobalFailedLoadCache.Remove(req.GetCollectionID())

	return merr.Status(nil), nil
}

func (s *Server) LoadPartitions(ctx context.Context, req *querypb.LoadPartitionsRequest) (*commonpb.Status, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", req.GetCollectionID()),
		zap.Int32("replicaNumber", req.GetReplicaNumber()),
		zap.Strings("resourceGroups", req.GetResourceGroups()),
		zap.Bool("refreshMode", req.GetRefresh()),
	)

	log.Info("received load partitions request",
		zap.Any("schema", req.Schema),
		zap.Int64s("partitions", req.GetPartitionIDs()))
	metrics.QueryCoordLoadCount.WithLabelValues(metrics.TotalLabel).Inc()

	if err := merr.CheckHealthy(s.State()); err != nil {
		msg := "failed to load partitions"
		log.Warn(msg, zap.Error(err))
		metrics.QueryCoordLoadCount.WithLabelValues(metrics.FailLabel).Inc()
		return merr.Status(errors.Wrap(err, msg)), nil
	}

	// If refresh mode is ON.
	if req.GetRefresh() {
		err := s.refreshCollection(req.GetCollectionID())
		if err != nil {
			log.Warn("failed to refresh partitions", zap.Error(err))
		}
		return merr.Status(err), nil
	}

	if err := s.checkResourceGroup(req.GetCollectionID(), req.GetResourceGroups()); err != nil {
		msg := "failed to load partitions"
		log.Warn(msg, zap.Error(err))
		metrics.QueryCoordLoadCount.WithLabelValues(metrics.FailLabel).Inc()
		return merr.Status(errors.Wrap(err, msg)), nil
	}

	loadJob := job.NewLoadPartitionJob(ctx,
		req,
		s.dist,
		s.meta,
		s.broker,
		s.cluster,
		s.targetMgr,
		s.targetObserver,
		s.nodeMgr,
	)
	s.jobScheduler.Add(loadJob)
	err := loadJob.Wait()
	if err != nil {
		msg := "failed to load partitions"
		log.Warn(msg, zap.Error(err))
		metrics.QueryCoordLoadCount.WithLabelValues(metrics.FailLabel).Inc()
		return merr.Status(errors.Wrap(err, msg)), nil
	}

	metrics.QueryCoordLoadCount.WithLabelValues(metrics.SuccessLabel).Inc()
	return merr.Status(nil), nil
}

func (s *Server) checkResourceGroup(collectionID int64, resourceGroups []string) error {
	if len(resourceGroups) != 0 {
		collectionUsedRG := s.meta.ReplicaManager.GetResourceGroupByCollection(collectionID)
		for _, rgName := range resourceGroups {
			if len(collectionUsedRG) > 0 && !collectionUsedRG.Contain(rgName) {
				return merr.WrapErrParameterInvalid("created resource group(s)", rgName, "given resource group not found")
			}

			if len(resourceGroups) > 1 && rgName == meta.DefaultResourceGroupName {
				return merr.WrapErrParameterInvalid("no default resource group mixed with the other resource group(s)", rgName)
			}
		}
	}

	return nil
}

func (s *Server) ReleasePartitions(ctx context.Context, req *querypb.ReleasePartitionsRequest) (*commonpb.Status, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", req.GetCollectionID()),
	)

	log.Info("release partitions", zap.Int64s("partitions", req.GetPartitionIDs()))
	metrics.QueryCoordReleaseCount.WithLabelValues(metrics.TotalLabel).Inc()

	if err := merr.CheckHealthy(s.State()); err != nil {
		msg := "failed to release partitions"
		log.Warn(msg, zap.Error(err))
		metrics.QueryCoordReleaseCount.WithLabelValues(metrics.FailLabel).Inc()
		return merr.Status(errors.Wrap(err, msg)), nil
	}

	if len(req.GetPartitionIDs()) == 0 {
		err := merr.WrapErrParameterInvalid("any parttiion", "empty partition list")
		log.Warn("no partition to release", zap.Error(err))
		metrics.QueryCoordReleaseCount.WithLabelValues(metrics.FailLabel).Inc()
		return merr.Status(err), nil
	}

	tr := timerecord.NewTimeRecorder("release-partitions")
	releaseJob := job.NewReleasePartitionJob(ctx,
		req,
		s.dist,
		s.meta,
		s.broker,
		s.cluster,
		s.targetMgr,
		s.targetObserver,
		s.checkerController,
	)
	s.jobScheduler.Add(releaseJob)
	err := releaseJob.Wait()
	if err != nil {
		msg := "failed to release partitions"
		log.Error(msg, zap.Error(err))
		metrics.QueryCoordReleaseCount.WithLabelValues(metrics.FailLabel).Inc()
		return merr.Status(errors.Wrap(err, msg)), nil
	}

	metrics.QueryCoordReleaseCount.WithLabelValues(metrics.SuccessLabel).Inc()
	metrics.QueryCoordReleaseLatency.WithLabelValues().Observe(float64(tr.ElapseSpan().Milliseconds()))

	meta.GlobalFailedLoadCache.Remove(req.GetCollectionID())
	return merr.Status(nil), nil
}

func (s *Server) GetPartitionStates(ctx context.Context, req *querypb.GetPartitionStatesRequest) (*querypb.GetPartitionStatesResponse, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", req.GetCollectionID()),
	)

	log.Info("get partition states", zap.Int64s("partitions", req.GetPartitionIDs()))

	if err := merr.CheckHealthy(s.State()); err != nil {
		msg := "failed to get partition states"
		log.Warn(msg, zap.Error(err))
		return &querypb.GetPartitionStatesResponse{
			Status: merr.Status(errors.Wrap(err, msg)),
		}, nil
	}

	msg := "partition not loaded"
	notLoadResp := &querypb.GetPartitionStatesResponse{
		Status: merr.Status(merr.WrapErrPartitionNotLoaded(req.GetPartitionIDs())),
	}

	states := make([]*querypb.PartitionStates, 0, len(req.GetPartitionIDs()))
	switch s.meta.GetLoadType(req.GetCollectionID()) {
	case querypb.LoadType_LoadCollection:
		collection := s.meta.GetCollection(req.GetCollectionID())
		state := querypb.PartitionState_PartialInMemory
		if collection.LoadPercentage >= 100 {
			state = querypb.PartitionState_InMemory
		}
		releasedPartitions := typeutil.NewUniqueSet(collection.GetReleasedPartitions()...)
		for _, partition := range req.GetPartitionIDs() {
			if releasedPartitions.Contain(partition) {
				log.Warn(msg)
				return notLoadResp, nil
			}
			states = append(states, &querypb.PartitionStates{
				PartitionID: partition,
				State:       state,
			})
		}

	case querypb.LoadType_LoadPartition:
		for _, partitionID := range req.GetPartitionIDs() {
			partition := s.meta.GetPartition(partitionID)
			if partition == nil {
				log.Warn(msg, zap.Int64("partition", partitionID))
				return notLoadResp, nil
			}
			state := querypb.PartitionState_PartialInMemory
			if partition.LoadPercentage >= 100 {
				state = querypb.PartitionState_InMemory
			}
			states = append(states, &querypb.PartitionStates{
				PartitionID: partitionID,
				State:       state,
			})
		}

	default:
		log.Warn(msg)
		return notLoadResp, nil
	}

	return &querypb.GetPartitionStatesResponse{
		Status:                merr.Status(nil),
		PartitionDescriptions: states,
	}, nil
}

func (s *Server) GetSegmentInfo(ctx context.Context, req *querypb.GetSegmentInfoRequest) (*querypb.GetSegmentInfoResponse, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", req.GetCollectionID()),
	)

	log.Info("get segment info", zap.Int64s("segments", req.GetSegmentIDs()))

	if err := merr.CheckHealthy(s.State()); err != nil {
		msg := "failed to get segment info"
		log.Warn(msg, zap.Error(err))
		return &querypb.GetSegmentInfoResponse{
			Status: merr.Status(errors.Wrap(err, msg)),
		}, nil
	}

	infos := make([]*querypb.SegmentInfo, 0, len(req.GetSegmentIDs()))
	if len(req.GetSegmentIDs()) == 0 {
		infos = s.getCollectionSegmentInfo(req.GetCollectionID())
	} else {
		for _, segmentID := range req.GetSegmentIDs() {
			segments := s.dist.SegmentDistManager.Get(segmentID)
			if len(segments) == 0 {
				err := merr.WrapErrSegmentNotLoaded(segmentID)
				msg := fmt.Sprintf("segment %v not found in any node", segmentID)
				log.Warn(msg, zap.Int64("segment", segmentID))
				return &querypb.GetSegmentInfoResponse{
					Status: merr.Status(errors.Wrap(err, msg)),
				}, nil
			}
			info := &querypb.SegmentInfo{}
			utils.MergeMetaSegmentIntoSegmentInfo(info, segments...)
			infos = append(infos, info)
		}
	}

	return &querypb.GetSegmentInfoResponse{
		Status: merr.Status(nil),
		Infos:  infos,
	}, nil
}

func (s *Server) SyncNewCreatedPartition(ctx context.Context, req *querypb.SyncNewCreatedPartitionRequest) (*commonpb.Status, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", req.GetCollectionID()),
		zap.Int64("partitionID", req.GetPartitionID()),
	)

	log.Info("received sync new created partition request")

	failedMsg := "failed to sync new created partition"
	if err := merr.CheckHealthy(s.State()); err != nil {
		log.Warn(failedMsg, zap.Error(err))
		return merr.Status(err), nil
	}

	syncJob := job.NewSyncNewCreatedPartitionJob(ctx, req, s.meta, s.cluster, s.broker)
	s.jobScheduler.Add(syncJob)
	err := syncJob.Wait()
	if err != nil {
		log.Warn(failedMsg, zap.Error(err))
		return merr.Status(err), nil
	}

	return merr.Status(nil), nil
}

// refreshCollection must be called after loading a collection. It looks for new segments that are not loaded yet and
// tries to load them up. It returns when all segments of the given collection are loaded, or when error happens.
// Note that a collection's loading progress always stays at 100% after a successful load and will not get updated
// during refreshCollection.
func (s *Server) refreshCollection(collectionID int64) error {
	collection := s.meta.CollectionManager.GetCollection(collectionID)
	if collection == nil {
		return merr.WrapErrCollectionNotLoaded(collectionID)
	}

	// Check that collection is fully loaded.
	if collection.GetStatus() != querypb.LoadStatus_Loaded {
		return merr.WrapErrCollectionNotLoaded(collectionID, "collection not fully loaded")
	}

	// Pull the latest target.
	readyCh, err := s.targetObserver.UpdateNextTarget(collectionID)
	if err != nil {
		return err
	}

	collection.SetRefreshNotifier(readyCh)
	return nil
}

// This is totally same to refreshCollection, remove it for now
// refreshPartitions must be called after loading a collection. It looks for new segments that are not loaded yet and
// tries to load them up. It returns when all segments of the given collection are loaded, or when error happens.
// Note that a collection's loading progress always stays at 100% after a successful load and will not get updated
// during refreshPartitions.
// func (s *Server) refreshPartitions(ctx context.Context, collID int64, partIDs []int64) (*commonpb.Status, error) {
// 	ctx, cancel := context.WithTimeout(ctx, Params.QueryCoordCfg.LoadTimeoutSeconds.GetAsDuration(time.Second))
// 	defer cancel()

// 	log := log.Ctx(ctx).With(
// 		zap.Int64("collectionID", collID),
// 		zap.Int64s("partitionIDs", partIDs),
// 	)
// 	if s.status.Load() != commonpb.StateCode_Healthy {
// 		msg := "failed to refresh partitions"
// 		log.Warn(msg, zap.Error(ErrNotHealthy))
// 		metrics.QueryCoordReleaseCount.WithLabelValues(metrics.FailLabel).Inc()
// 		return utils.WrapStatus(commonpb.ErrorCode_UnexpectedError, msg, ErrNotHealthy), nil
// 	}

// 	// Check that all partitions are fully loaded.
// 	if s.meta.CollectionManager.GetCurrentLoadPercentage(collID) != 100 {
// 		errMsg := "partitions must be fully loaded before refreshing"
// 		log.Warn(errMsg)
// 		return &commonpb.Status{
// 			ErrorCode: commonpb.ErrorCode_UnexpectedError,
// 			Reason:    errMsg,
// 		}, nil
// 	}

// 	// Pull the latest target.
// 	readyCh, err := s.targetObserver.UpdateNextTarget(collID)
// 	if err != nil {
// 		log.Warn("failed to update next target", zap.Error(err))
// 		return &commonpb.Status{
// 			ErrorCode: commonpb.ErrorCode_UnexpectedError,
// 			Reason:    err.Error(),
// 		}, nil
// 	}

// 	select {
// 	case <-ctx.Done():
// 		log.Warn("refresh partitions failed as context canceled")
// 		return &commonpb.Status{
// 			ErrorCode: commonpb.ErrorCode_UnexpectedError,
// 			Reason:    "context canceled",
// 		}, nil
// 	case <-readyCh:
// 		log.Info("refresh partitions succeeded")
// 		return &commonpb.Status{
// 			ErrorCode: commonpb.ErrorCode_Success,
// 		}, nil
// 	}
// }

func (s *Server) isStoppingNode(nodeID int64) error {
	isStopping, err := s.nodeMgr.IsStoppingNode(nodeID)
	if err != nil {
		log.Warn("fail to check whether the node is stopping", zap.Int64("node_id", nodeID), zap.Error(err))
		return err
	}
	if isStopping {
		msg := fmt.Sprintf("failed to balance due to the source/destination node[%d] is stopping", nodeID)
		log.Warn(msg)
		return errors.New(msg)
	}
	return nil
}

func (s *Server) LoadBalance(ctx context.Context, req *querypb.LoadBalanceRequest) (*commonpb.Status, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", req.GetCollectionID()),
	)

	log.Info("load balance request received",
		zap.Int64s("source", req.GetSourceNodeIDs()),
		zap.Int64s("dest", req.GetDstNodeIDs()),
		zap.Int64s("segments", req.GetSealedSegmentIDs()))

	if err := merr.CheckHealthy(s.State()); err != nil {
		msg := "failed to load balance"
		log.Warn(msg, zap.Error(err))
		return merr.Status(errors.Wrap(err, msg)), nil
	}

	// Verify request
	if len(req.GetSourceNodeIDs()) != 1 {
		err := merr.WrapErrParameterInvalid("only 1 source node", fmt.Sprintf("%d source nodes", len(req.GetSourceNodeIDs())))
		msg := "source nodes can only contain 1 node"
		log.Warn(msg, zap.Int("source-nodes-num", len(req.GetSourceNodeIDs())))
		return merr.Status(err), nil
	}
	if s.meta.CollectionManager.CalculateLoadPercentage(req.GetCollectionID()) < 100 {
		err := merr.WrapErrCollectionNotFullyLoaded(req.GetCollectionID())
		msg := "can't balance segments of not fully loaded collection"
		log.Warn(msg)
		return merr.Status(err), nil
	}
	srcNode := req.GetSourceNodeIDs()[0]
	replica := s.meta.ReplicaManager.GetByCollectionAndNode(req.GetCollectionID(), srcNode)
	if replica == nil {
		err := merr.WrapErrReplicaNotFound(-1, fmt.Sprintf("replica not found for collection %d and node %d", req.GetCollectionID(), srcNode))
		msg := "source node not found in any replica"
		log.Warn(msg)
		return merr.Status(err), nil
	}
	if err := s.isStoppingNode(srcNode); err != nil {
		return merr.Status(errors.Wrap(err,
			fmt.Sprintf("can't balance, because the source node[%d] is invalid", srcNode))), nil
	}
	for _, dstNode := range req.GetDstNodeIDs() {
		if !replica.Contains(dstNode) {
			err := merr.WrapErrParameterInvalid("destination node in the same replica as source node", fmt.Sprintf("destination node %d not in replica %d", dstNode, replica.GetID()))
			msg := "destination nodes have to be in the same replica of source node"
			log.Warn(msg)
			return merr.Status(err), nil
		}
		if err := s.isStoppingNode(dstNode); err != nil {
			return merr.Status(errors.Wrap(err,
				fmt.Sprintf("can't balance, because the destination node[%d] is invalid", dstNode))), nil
		}
	}

	err := s.balanceSegments(ctx, req, replica)
	if err != nil {
		msg := "failed to balance segments"
		log.Warn(msg, zap.Error(err))
		return merr.Status(errors.Wrap(err, msg)), nil
	}
	return merr.Status(nil), nil
}

func (s *Server) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest) (*internalpb.ShowConfigurationsResponse, error) {
	log := log.Ctx(ctx)

	log.Info("show configurations request received", zap.String("pattern", req.GetPattern()))

	if err := merr.CheckHealthy(s.State()); err != nil {
		msg := "failed to show configurations"
		log.Warn(msg, zap.Error(err))
		return &internalpb.ShowConfigurationsResponse{
			Status: merr.Status(errors.Wrap(err, msg)),
		}, nil
	}
	configList := make([]*commonpb.KeyValuePair, 0)
	for key, value := range Params.GetComponentConfigurations("querycoord", req.Pattern) {
		configList = append(configList,
			&commonpb.KeyValuePair{
				Key:   key,
				Value: value,
			})
	}

	return &internalpb.ShowConfigurationsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Configuations: configList,
	}, nil
}

func (s *Server) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	log := log.Ctx(ctx)

	log.RatedDebug(60, "get metrics request received",
		zap.String("metricType", req.GetRequest()))

	if err := merr.CheckHealthy(s.State()); err != nil {
		msg := "failed to get metrics"
		log.Warn(msg, zap.Error(err))
		return &milvuspb.GetMetricsResponse{
			Status: merr.Status(errors.Wrap(err, msg)),
		}, nil
	}

	resp := &milvuspb.GetMetricsResponse{
		Status: merr.Status(nil),
		ComponentName: metricsinfo.ConstructComponentName(typeutil.QueryCoordRole,
			paramtable.GetNodeID()),
	}

	metricType, err := metricsinfo.ParseMetricType(req.GetRequest())
	if err != nil {
		msg := "failed to parse metric type"
		log.Warn(msg, zap.Error(err))
		resp.Status = merr.Status(errors.Wrap(err, msg))
		return resp, nil
	}

	if metricType != metricsinfo.SystemInfoMetrics {
		msg := "invalid metric type"
		err := errors.New(metricsinfo.MsgUnimplementedMetric)
		log.Warn(msg, zap.Error(err))
		resp.Status = merr.Status(errors.Wrap(err, msg))
		return resp, nil
	}

	resp.Response, err = s.getSystemInfoMetrics(ctx, req)
	if err != nil {
		msg := "failed to get system info metrics"
		log.Warn(msg, zap.Error(err))
		resp.Status = merr.Status(errors.Wrap(err, msg))
		return resp, nil
	}

	return resp, nil
}

func (s *Server) GetReplicas(ctx context.Context, req *milvuspb.GetReplicasRequest) (*milvuspb.GetReplicasResponse, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", req.GetCollectionID()),
	)

	log.Info("get replicas request received", zap.Bool("with-shard-nodes", req.GetWithShardNodes()))

	if err := merr.CheckHealthy(s.State()); err != nil {
		msg := "failed to get replicas"
		log.Warn(msg, zap.Error(err))
		return &milvuspb.GetReplicasResponse{
			Status: merr.Status(errors.Wrap(err, msg)),
		}, nil
	}

	resp := &milvuspb.GetReplicasResponse{
		Status:   merr.Status(nil),
		Replicas: make([]*milvuspb.ReplicaInfo, 0),
	}

	replicas := s.meta.ReplicaManager.GetByCollection(req.GetCollectionID())
	if len(replicas) == 0 {
		err := merr.WrapErrReplicaNotFound(req.GetCollectionID(), "failed to get replicas by collection")
		msg := "failed to get replicas, collection not loaded"
		log.Warn(msg)
		resp.Status = merr.Status(err)
		return resp, nil
	}

	for _, replica := range replicas {
		msg := "failed to get replica info"
		if len(replica.GetNodes()) == 0 {
			err := merr.WrapErrReplicaNotAvailable(replica.GetID(), "no available nodes in replica")
			log.Warn(msg,
				zap.Int64("replica", replica.GetID()),
				zap.Error(err))
			resp.Status = merr.Status(err)
			break
		}

		info, err := s.fillReplicaInfo(replica, req.GetWithShardNodes())
		if err != nil {
			log.Warn(msg,
				zap.Int64("replica", replica.GetID()),
				zap.Error(err))
			resp.Status = merr.Status(err)
			break
		}
		resp.Replicas = append(resp.Replicas, info)
	}
	return resp, nil
}

func (s *Server) GetShardLeaders(ctx context.Context, req *querypb.GetShardLeadersRequest) (*querypb.GetShardLeadersResponse, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", req.GetCollectionID()),
	)

	log.Info("get shard leaders request received")
	if err := merr.CheckHealthy(s.State()); err != nil {
		msg := "failed to get shard leaders"
		log.Warn(msg, zap.Error(err))
		return &querypb.GetShardLeadersResponse{
			Status: merr.Status(errors.Wrap(err, msg)),
		}, nil
	}

	resp := &querypb.GetShardLeadersResponse{
		Status: merr.Status(nil),
	}

	percentage := s.meta.CollectionManager.CalculateLoadPercentage(req.GetCollectionID())
	if percentage < 0 {
		err := merr.WrapErrCollectionNotLoaded(req.GetCollectionID())
		log.Warn("failed to GetShardLeaders", zap.Error(err))
		resp.Status = merr.Status(err)
		return resp, nil
	}
	collection := s.meta.CollectionManager.GetCollection(req.GetCollectionID())
	if collection.GetStatus() == querypb.LoadStatus_Loaded {
		// when collection is loaded, regard collection as readable, set percentage == 100
		percentage = 100
	}
	if percentage < 100 {
		err := merr.WrapErrCollectionNotFullyLoaded(req.GetCollectionID())
		msg := fmt.Sprintf("collection %v is not fully loaded", req.GetCollectionID())
		log.Warn(msg)
		resp.Status = merr.Status(err)
		return resp, nil
	}

	channels := s.targetMgr.GetDmChannelsByCollection(req.GetCollectionID(), meta.CurrentTarget)
	if len(channels) == 0 {
		msg := "failed to get channels"
		err := merr.WrapErrCollectionNotLoaded(req.GetCollectionID())
		log.Warn(msg, zap.Error(err))
		resp.Status = merr.Status(err)
		return resp, nil
	}

	currentTargets := s.targetMgr.GetHistoricalSegmentsByCollection(req.GetCollectionID(), meta.CurrentTarget)
	for _, channel := range channels {
		log := log.With(zap.String("channel", channel.GetChannelName()))

		leaders := s.dist.LeaderViewManager.GetLeadersByShard(channel.GetChannelName())
		leaders = filterDupLeaders(s.meta.ReplicaManager, leaders)
		ids := make([]int64, 0, len(leaders))
		addrs := make([]string, 0, len(leaders))

		var channelErr error

		// In a replica, a shard is available, if and only if:
		// 1. The leader is online
		// 2. All QueryNodes in the distribution are online
		// 3. The last heartbeat response time is within HeartbeatAvailableInterval for all QueryNodes(include leader) in the distribution
		// 4. All segments of the shard in target should be in the distribution
		for _, leader := range leaders {
			log := log.With(zap.Int64("leaderID", leader.ID))
			info := s.nodeMgr.Get(leader.ID)

			// Check whether leader is online
			err := checkNodeAvailable(leader.ID, info)
			if err != nil {
				log.Info("leader is not available", zap.Error(err))
				multierr.AppendInto(&channelErr, fmt.Errorf("leader not available: %w", err))
				continue
			}
			// Check whether QueryNodes are online and available
			isAvailable := true
			for _, version := range leader.Segments {
				info := s.nodeMgr.Get(version.GetNodeID())
				err = checkNodeAvailable(version.GetNodeID(), info)
				if err != nil {
					log.Info("leader is not available due to QueryNode unavailable", zap.Error(err))
					isAvailable = false
					multierr.AppendInto(&channelErr, err)
					break
				}
			}

			// Avoid iterating all segments if any QueryNode unavailable
			if !isAvailable {
				continue
			}

			// Check whether segments are fully loaded
			for segmentID, info := range currentTargets {
				if info.GetInsertChannel() != leader.Channel {
					continue
				}

				_, exist := leader.Segments[segmentID]
				if !exist {
					log.Info("leader is not available due to lack of segment", zap.Int64("segmentID", segmentID))
					multierr.AppendInto(&channelErr, merr.WrapErrSegmentLack(segmentID))
					isAvailable = false
					break
				}
			}
			if !isAvailable {
				continue
			}

			ids = append(ids, info.ID())
			addrs = append(addrs, info.Addr())
		}

		if len(ids) == 0 {
			msg := fmt.Sprintf("channel %s is not available in any replica", channel.GetChannelName())
			log.Warn(msg, zap.Error(channelErr))
			resp.Status = merr.Status(
				errors.Wrap(merr.WrapErrChannelNotAvailable(channel.GetChannelName()), channelErr.Error()))
			resp.Shards = nil
			return resp, nil
		}

		resp.Shards = append(resp.Shards, &querypb.ShardLeadersList{
			ChannelName: channel.GetChannelName(),
			NodeIds:     ids,
			NodeAddrs:   addrs,
		})
	}

	return resp, nil
}

func (s *Server) CheckHealth(ctx context.Context, req *milvuspb.CheckHealthRequest) (*milvuspb.CheckHealthResponse, error) {
	if err := merr.CheckHealthy(s.State()); err != nil {
		reason := errorutil.UnHealthReason("querycoord", paramtable.GetNodeID(), "querycoord is unhealthy")
		return &milvuspb.CheckHealthResponse{IsHealthy: false, Reasons: []string{reason}}, nil
	}

	errReasons, err := s.checkNodeHealth(ctx)
	if err != nil || len(errReasons) != 0 {
		return &milvuspb.CheckHealthResponse{IsHealthy: false, Reasons: errReasons}, nil
	}

	return &milvuspb.CheckHealthResponse{IsHealthy: true, Reasons: errReasons}, nil
}

func (s *Server) checkNodeHealth(ctx context.Context) ([]string, error) {
	group, ctx := errgroup.WithContext(ctx)
	errReasons := make([]string, 0)

	mu := &sync.Mutex{}
	for _, node := range s.nodeMgr.GetAll() {
		node := node
		group.Go(func() error {
			resp, err := s.cluster.GetComponentStates(ctx, node.ID())
			isHealthy, reason := errorutil.UnHealthReasonWithComponentStatesOrErr("querynode", node.ID(), resp, err)
			if !isHealthy {
				mu.Lock()
				defer mu.Unlock()
				errReasons = append(errReasons, reason)
			}
			return err
		})
	}

	err := group.Wait()

	return errReasons, err
}

func (s *Server) CreateResourceGroup(ctx context.Context, req *milvuspb.CreateResourceGroupRequest) (*commonpb.Status, error) {
	log := log.Ctx(ctx).With(
		zap.String("rgName", req.GetResourceGroup()),
	)

	log.Info("create resource group request received")
	if err := merr.CheckHealthy(s.State()); err != nil {
		log.Warn("failed to create resource group", zap.Error(err))
		return merr.Status(err), nil
	}

	err := s.meta.ResourceManager.AddResourceGroup(req.GetResourceGroup())
	if err != nil {
		log.Warn("failed to create resource group", zap.Error(err))
		return merr.Status(err), nil
	}
	return merr.Status(nil), nil
}

func (s *Server) DropResourceGroup(ctx context.Context, req *milvuspb.DropResourceGroupRequest) (*commonpb.Status, error) {
	log := log.Ctx(ctx).With(
		zap.String("rgName", req.GetResourceGroup()),
	)

	log.Info("drop resource group request received")
	if err := merr.CheckHealthy(s.State()); err != nil {
		log.Warn("failed to drop resource group", zap.Error(err))
		return merr.Status(err), nil
	}

	replicas := s.meta.ReplicaManager.GetByResourceGroup(req.GetResourceGroup())
	if len(replicas) > 0 {
		err := merr.WrapErrParameterInvalid("empty resource group", fmt.Sprintf("resource group %s has collection %d loaded", req.GetResourceGroup(), replicas[0].GetCollectionID()))
		return merr.Status(errors.Wrap(err,
			fmt.Sprintf("some replicas still loaded in resource group[%s], release it first", req.GetResourceGroup()))), nil
	}

	err := s.meta.ResourceManager.RemoveResourceGroup(req.GetResourceGroup())
	if err != nil {
		log.Warn("failed to drop resource group", zap.Error(err))
		return merr.Status(err), nil
	}
	return merr.Status(nil), nil
}

func (s *Server) TransferNode(ctx context.Context, req *milvuspb.TransferNodeRequest) (*commonpb.Status, error) {
	log := log.Ctx(ctx).With(
		zap.String("source", req.GetSourceResourceGroup()),
		zap.String("target", req.GetTargetResourceGroup()),
		zap.Int32("numNode", req.GetNumNode()),
	)

	log.Info("transfer node between resource group request received")
	if err := merr.CheckHealthy(s.State()); err != nil {
		log.Warn("failed to transfer node between resource group", zap.Error(err))
		return merr.Status(err), nil
	}

	if ok := s.meta.ResourceManager.ContainResourceGroup(req.GetSourceResourceGroup()); !ok {
		err := merr.WrapErrParameterInvalid("valid resource group", req.GetSourceResourceGroup(), "source resource group not found")
		return merr.Status(err), nil
	}

	if ok := s.meta.ResourceManager.ContainResourceGroup(req.GetTargetResourceGroup()); !ok {
		err := merr.WrapErrParameterInvalid("valid resource group", req.GetTargetResourceGroup(), "target resource group not found")
		return merr.Status(err), nil
	}

	if req.GetNumNode() <= 0 {
		err := merr.WrapErrParameterInvalid("NumNode > 0", fmt.Sprintf("invalid NumNode %d", req.GetNumNode()))
		return merr.Status(err), nil
	}

	replicasInSource := s.meta.ReplicaManager.GetByResourceGroup(req.GetSourceResourceGroup())
	replicasInTarget := s.meta.ReplicaManager.GetByResourceGroup(req.GetTargetResourceGroup())
	loadSameCollection := false
	sameCollectionID := int64(0)
	for _, r1 := range replicasInSource {
		for _, r2 := range replicasInTarget {
			if r1.GetCollectionID() == r2.GetCollectionID() {
				loadSameCollection = true
				sameCollectionID = r1.GetCollectionID()
			}
		}
	}
	if loadSameCollection {
		err := merr.WrapErrParameterInvalid("resource groups load not the same collection", fmt.Sprintf("collection %d loaded for both", sameCollectionID))
		return merr.Status(err), nil
	}

	nodes, err := s.meta.ResourceManager.TransferNode(req.GetSourceResourceGroup(), req.GetTargetResourceGroup(), int(req.GetNumNode()))
	if err != nil {
		log.Warn("failed to transfer node", zap.Error(err))
		return merr.Status(err), nil
	}

	utils.AddNodesToCollectionsInRG(s.meta, req.GetTargetResourceGroup(), nodes...)

	return merr.Status(nil), nil
}

func (s *Server) TransferReplica(ctx context.Context, req *querypb.TransferReplicaRequest) (*commonpb.Status, error) {
	log := log.Ctx(ctx).With(
		zap.String("source", req.GetSourceResourceGroup()),
		zap.String("target", req.GetTargetResourceGroup()),
		zap.Int64("collectionID", req.GetCollectionID()),
	)

	log.Info("transfer replica request received")
	if err := merr.CheckHealthy(s.State()); err != nil {
		log.Warn("failed to transfer replica between resource group", zap.Error(err))
		return merr.Status(err), nil
	}

	if ok := s.meta.ResourceManager.ContainResourceGroup(req.GetSourceResourceGroup()); !ok {
		err := merr.WrapErrResourceGroupNotFound(req.GetSourceResourceGroup())
		return merr.Status(errors.Wrap(err,
			fmt.Sprintf("the source resource group[%s] doesn't exist", req.GetSourceResourceGroup()))), nil
	}

	if ok := s.meta.ResourceManager.ContainResourceGroup(req.GetTargetResourceGroup()); !ok {
		err := merr.WrapErrResourceGroupNotFound(req.GetTargetResourceGroup())
		return merr.Status(errors.Wrap(err,
			fmt.Sprintf("the target resource group[%s] doesn't exist", req.GetTargetResourceGroup()))), nil
	}

	if req.GetNumReplica() <= 0 {
		err := merr.WrapErrParameterInvalid("NumReplica > 0", fmt.Sprintf("invalid NumReplica %d", req.GetNumReplica()))
		return merr.Status(err), nil
	}

	replicas := s.meta.ReplicaManager.GetByCollectionAndRG(req.GetCollectionID(), req.GetSourceResourceGroup())
	if len(replicas) < int(req.GetNumReplica()) {
		err := merr.WrapErrParameterInvalid("NumReplica not greater than the number of replica in source resource group", fmt.Sprintf("only found [%d] replicas in source resource group[%s]",
			len(replicas), req.GetSourceResourceGroup()))
		return merr.Status(err), nil
	}

	replicas = s.meta.ReplicaManager.GetByCollectionAndRG(req.GetCollectionID(), req.GetTargetResourceGroup())
	if len(replicas) > 0 {
		err := merr.WrapErrParameterInvalid("no same collection in target resource group", fmt.Sprintf("found [%d] replicas of same collection in target resource group[%s], dynamically increase replica num is unsupported",
			len(replicas), req.GetTargetResourceGroup()))
		return merr.Status(err), nil
	}

	replicas = s.meta.ReplicaManager.GetByCollection(req.GetCollectionID())
	if (req.GetSourceResourceGroup() == meta.DefaultResourceGroupName || req.GetTargetResourceGroup() == meta.DefaultResourceGroupName) &&
		len(replicas) != int(req.GetNumReplica()) {
		err := merr.WrapErrParameterInvalid("tranfer all replicas from/to default resource group",
			fmt.Sprintf("try to transfer %d replicas from/to but %d replicas exist", req.GetNumReplica(), len(replicas)))
		return merr.Status(err), nil
	}

	err := s.transferReplica(req.GetTargetResourceGroup(), replicas[:req.GetNumReplica()])
	if err != nil {
		return merr.Status(err), nil
	}

	return merr.Status(nil), nil
}

func (s *Server) transferReplica(targetRG string, replicas []*meta.Replica) error {
	ret := make([]*meta.Replica, 0)
	for _, replica := range replicas {
		newReplica := replica.Clone()
		newReplica.ResourceGroup = targetRG

		ret = append(ret, newReplica)
	}
	err := utils.AssignNodesToReplicas(s.meta, targetRG, ret...)
	if err != nil {
		return err
	}

	return s.meta.ReplicaManager.Put(ret...)
}

func (s *Server) ListResourceGroups(ctx context.Context, req *milvuspb.ListResourceGroupsRequest) (*milvuspb.ListResourceGroupsResponse, error) {
	log := log.Ctx(ctx)

	log.Info("list resource group request received")
	resp := &milvuspb.ListResourceGroupsResponse{
		Status: merr.Status(nil),
	}
	if err := merr.CheckHealthy(s.State()); err != nil {
		log.Warn("failed to list resource group", zap.Error(err))
		resp.Status = merr.Status(err)
		return resp, nil
	}

	resp.ResourceGroups = s.meta.ResourceManager.ListResourceGroups()
	return resp, nil
}

func (s *Server) DescribeResourceGroup(ctx context.Context, req *querypb.DescribeResourceGroupRequest) (*querypb.DescribeResourceGroupResponse, error) {
	log := log.Ctx(ctx).With(
		zap.String("rgName", req.GetResourceGroup()),
	)

	log.Info("describe resource group request received")
	resp := &querypb.DescribeResourceGroupResponse{
		Status: merr.Status(nil),
	}
	if err := merr.CheckHealthy(s.State()); err != nil {
		log.Warn("failed to describe resource group", zap.Error(err))
		resp.Status = merr.Status(err)
		return resp, nil
	}

	rg, err := s.meta.ResourceManager.GetResourceGroup(req.GetResourceGroup())
	if err != nil {
		resp.Status = merr.Status(err)
		return resp, nil
	}

	loadedReplicas := make(map[int64]int32)
	outgoingNodes := make(map[int64]int32)
	replicasInRG := s.meta.GetByResourceGroup(req.GetResourceGroup())
	for _, replica := range replicasInRG {
		loadedReplicas[replica.GetCollectionID()]++
		for _, node := range replica.GetNodes() {
			if !s.meta.ContainsNode(replica.GetResourceGroup(), node) {
				outgoingNodes[replica.GetCollectionID()]++
			}
		}
	}
	incomingNodes := make(map[int64]int32)
	collections := s.meta.GetAll()
	for _, collection := range collections {
		replicas := s.meta.GetByCollection(collection)

		for _, replica := range replicas {
			if replica.GetResourceGroup() == req.GetResourceGroup() {
				continue
			}
			for _, node := range replica.GetNodes() {
				if s.meta.ContainsNode(req.GetResourceGroup(), node) {
					incomingNodes[collection]++
				}
			}
		}
	}

	resp.ResourceGroup = &querypb.ResourceGroupInfo{
		Name:             req.GetResourceGroup(),
		Capacity:         int32(rg.GetCapacity()),
		NumAvailableNode: int32(len(rg.GetNodes())),
		NumLoadedReplica: loadedReplicas,
		NumOutgoingNode:  outgoingNodes,
		NumIncomingNode:  incomingNodes,
	}
	return resp, nil
}
