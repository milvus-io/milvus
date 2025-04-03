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
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/querycoordv2/job"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/util/componentutil"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
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

func (s *Server) ShowLoadCollections(ctx context.Context, req *querypb.ShowCollectionsRequest) (*querypb.ShowCollectionsResponse, error) {
	log.Ctx(ctx).Debug("show collections request received", zap.Int64s("collections", req.GetCollectionIDs()))
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
		for _, collection := range s.meta.GetAllCollections(ctx) {
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

		collection := s.meta.CollectionManager.GetCollection(ctx, collectionID)
		percentage := s.meta.CollectionManager.CalculateLoadPercentage(ctx, collectionID)
		loadFields := s.meta.CollectionManager.GetLoadFields(ctx, collectionID)
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
				return &querypb.ShowCollectionsResponse{
					Status: status,
				}, nil
			}

			err = merr.WrapErrCollectionNotLoaded(collectionID)
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
		resp.LoadFields = append(resp.LoadFields, &schemapb.LongArray{
			Data: loadFields,
		})
	}

	return resp, nil
}

func (s *Server) ShowLoadPartitions(ctx context.Context, req *querypb.ShowPartitionsRequest) (*querypb.ShowPartitionsResponse, error) {
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
		partitions = lo.Map(s.meta.GetPartitionsByCollection(ctx, req.GetCollectionID()), func(partition *meta.Partition, _ int) int64 {
			return partition.GetPartitionID()
		})
	}

	for _, partitionID := range partitions {
		percentage := s.meta.GetPartitionLoadPercentage(ctx, partitionID)
		if percentage < 0 {
			err := meta.GlobalFailedLoadCache.Get(req.GetCollectionID())
			if err != nil {
				partitionErr := merr.WrapErrPartitionNotLoaded(partitionID, err.Error())
				status := merr.Status(partitionErr)
				log.Warn("show partition failed", zap.Error(partitionErr))
				return &querypb.ShowPartitionsResponse{
					Status: status,
				}, nil
			}

			err = merr.WrapErrPartitionNotLoaded(partitionID)
			log.Warn("show partition failed", zap.Error(err))
			return &querypb.ShowPartitionsResponse{
				Status: merr.Status(err),
			}, nil
		}

		percentages = append(percentages, int64(percentage))
	}

	collection := s.meta.GetCollection(ctx, req.GetCollectionID())
	if collection != nil && collection.IsRefreshed() {
		refreshProgress = 100
	}
	refreshProgresses := make([]int64, len(partitions))
	for i := range partitions {
		refreshProgresses[i] = refreshProgress
	}

	return &querypb.ShowPartitionsResponse{
		Status:              merr.Success(),
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
		err := s.refreshCollection(ctx, req.GetCollectionID())
		if err != nil {
			log.Warn("failed to refresh collection", zap.Error(err))
		}
		return merr.Status(err), nil
	}

	// to be compatible with old sdk, which set replica=1 if replica is not specified
	// so only both replica and resource groups didn't set in request, it will turn to use the configured load info
	if req.GetReplicaNumber() <= 0 && len(req.GetResourceGroups()) == 0 {
		// when replica number or resource groups is not set, use pre-defined load config
		rgs, replicas, err := s.broker.GetCollectionLoadInfo(ctx, req.GetCollectionID())
		if err != nil {
			log.Warn("failed to get pre-defined load info", zap.Error(err))
		} else {
			if req.GetReplicaNumber() <= 0 && replicas > 0 {
				req.ReplicaNumber = int32(replicas)
			}

			if len(req.GetResourceGroups()) == 0 && len(rgs) > 0 {
				req.ResourceGroups = rgs
			}
		}
	}

	if req.GetReplicaNumber() <= 0 {
		log.Info("request doesn't indicate the number of replicas, set it to 1")
		req.ReplicaNumber = 1
	}

	if len(req.GetResourceGroups()) == 0 {
		log.Info(fmt.Sprintf("request doesn't indicate the resource groups, set it to %s", meta.DefaultResourceGroupName))
		req.ResourceGroups = []string{meta.DefaultResourceGroupName}
	}

	var loadJob job.Job
	collection := s.meta.GetCollection(ctx, req.GetCollectionID())
	if collection != nil {
		// if collection is loaded, check if collection is loaded with the same replica number and resource groups
		// if replica number or resource group changes， switch to update load config
		collectionUsedRG := s.meta.ReplicaManager.GetResourceGroupByCollection(ctx, collection.GetCollectionID()).Collect()
		left, right := lo.Difference(collectionUsedRG, req.GetResourceGroups())
		rgChanged := len(left) > 0 || len(right) > 0
		replicaChanged := collection.GetReplicaNumber() != req.GetReplicaNumber()
		if replicaChanged || rgChanged {
			log.Warn("collection is loaded with different replica number or resource group, switch to update load config",
				zap.Int32("oldReplicaNumber", collection.GetReplicaNumber()),
				zap.Strings("oldResourceGroups", collectionUsedRG))
			updateReq := &querypb.UpdateLoadConfigRequest{
				CollectionIDs:  []int64{req.GetCollectionID()},
				ReplicaNumber:  req.GetReplicaNumber(),
				ResourceGroups: req.GetResourceGroups(),
			}
			loadJob = job.NewUpdateLoadConfigJob(
				ctx,
				updateReq,
				s.meta,
				s.targetMgr,
				s.targetObserver,
				s.collectionObserver,
			)
		}
	}

	if loadJob == nil {
		loadJob = job.NewLoadCollectionJob(ctx,
			req,
			s.dist,
			s.meta,
			s.broker,
			s.targetMgr,
			s.targetObserver,
			s.collectionObserver,
			s.nodeMgr,
		)
	}

	s.jobScheduler.Add(loadJob)
	err := loadJob.Wait()
	if err != nil {
		msg := "failed to load collection"
		log.Warn(msg, zap.Error(err))
		metrics.QueryCoordLoadCount.WithLabelValues(metrics.FailLabel).Inc()
		return merr.Status(errors.Wrap(err, msg)), nil
	}

	metrics.QueryCoordLoadCount.WithLabelValues(metrics.SuccessLabel).Inc()
	return merr.Success(), nil
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
		s.targetMgr,
		s.targetObserver,
		s.checkerController,
		s.proxyClientManager,
	)
	s.jobScheduler.Add(releaseJob)
	err := releaseJob.Wait()
	if err != nil {
		msg := "failed to release collection"
		log.Warn(msg, zap.Error(err))
		metrics.QueryCoordReleaseCount.WithLabelValues(metrics.FailLabel).Inc()
		return merr.Status(errors.Wrap(err, msg)), nil
	}

	log.Info("collection released")
	metrics.QueryCoordReleaseLatency.WithLabelValues().Observe(float64(tr.ElapseSpan().Milliseconds()))
	meta.GlobalFailedLoadCache.Remove(req.GetCollectionID())

	return merr.Success(), nil
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
		err := s.refreshCollection(ctx, req.GetCollectionID())
		if err != nil {
			log.Warn("failed to refresh partitions", zap.Error(err))
		}
		return merr.Status(err), nil
	}

	// to be compatible with old sdk, which set replica=1 if replica is not specified
	// so only both replica and resource groups didn't set in request, it will turn to use the configured load info
	if req.GetReplicaNumber() <= 0 && len(req.GetResourceGroups()) == 0 {
		// when replica number or resource groups is not set, use database level config
		rgs, replicas, err := s.broker.GetCollectionLoadInfo(ctx, req.GetCollectionID())
		if err != nil {
			log.Warn("failed to get data base level load info", zap.Error(err))
		}

		if req.GetReplicaNumber() <= 0 {
			log.Info("load collection use database level replica number", zap.Int64("databaseLevelReplicaNum", replicas))
			req.ReplicaNumber = int32(replicas)
		}

		if len(req.GetResourceGroups()) == 0 {
			log.Info("load collection use database level resource groups", zap.Strings("databaseLevelResourceGroups", rgs))
			req.ResourceGroups = rgs
		}
	}

	loadJob := job.NewLoadPartitionJob(ctx,
		req,
		s.dist,
		s.meta,
		s.broker,
		s.targetMgr,
		s.targetObserver,
		s.collectionObserver,
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
	return merr.Success(), nil
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
		err := merr.WrapErrParameterInvalid("any partition", "empty partition list")
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
		s.targetMgr,
		s.targetObserver,
		s.checkerController,
		s.proxyClientManager,
	)
	s.jobScheduler.Add(releaseJob)
	err := releaseJob.Wait()
	if err != nil {
		msg := "failed to release partitions"
		log.Warn(msg, zap.Error(err))
		metrics.QueryCoordReleaseCount.WithLabelValues(metrics.FailLabel).Inc()
		return merr.Status(errors.Wrap(err, msg)), nil
	}

	metrics.QueryCoordReleaseCount.WithLabelValues(metrics.SuccessLabel).Inc()
	metrics.QueryCoordReleaseLatency.WithLabelValues().Observe(float64(tr.ElapseSpan().Milliseconds()))

	meta.GlobalFailedLoadCache.Remove(req.GetCollectionID())
	return merr.Success(), nil
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
	switch s.meta.GetLoadType(ctx, req.GetCollectionID()) {
	case querypb.LoadType_LoadCollection:
		collection := s.meta.GetCollection(ctx, req.GetCollectionID())
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
			partition := s.meta.GetPartition(ctx, partitionID)
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
		Status:                merr.Success(),
		PartitionDescriptions: states,
	}, nil
}

func (s *Server) GetLoadSegmentInfo(ctx context.Context, req *querypb.GetSegmentInfoRequest) (*querypb.GetSegmentInfoResponse, error) {
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
		infos = s.getCollectionSegmentInfo(ctx, req.GetCollectionID())
	} else {
		for _, segmentID := range req.GetSegmentIDs() {
			segments := s.dist.SegmentDistManager.GetByFilter(meta.WithSegmentID(segmentID))
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
		Status: merr.Success(),
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

	syncJob := job.NewSyncNewCreatedPartitionJob(ctx, req, s.meta, s.broker, s.targetObserver, s.targetMgr)
	s.jobScheduler.Add(syncJob)
	err := syncJob.Wait()
	if err != nil {
		log.Warn(failedMsg, zap.Error(err))
		return merr.Status(err), nil
	}

	return merr.Success(), nil
}

// refreshCollection must be called after loading a collection. It looks for new segments that are not loaded yet and
// tries to load them up. It returns when all segments of the given collection are loaded, or when error happens.
// Note that a collection's loading progress always stays at 100% after a successful load and will not get updated
// during refreshCollection.
func (s *Server) refreshCollection(ctx context.Context, collectionID int64) error {
	collection := s.meta.CollectionManager.GetCollection(ctx, collectionID)
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

func (s *Server) isStoppingNode(ctx context.Context, nodeID int64) error {
	isStopping, err := s.nodeMgr.IsStoppingNode(nodeID)
	if err != nil {
		log.Ctx(ctx).Warn("fail to check whether the node is stopping", zap.Int64("node_id", nodeID), zap.Error(err))
		return err
	}
	if isStopping {
		msg := fmt.Sprintf("failed to balance due to the source/destination node[%d] is stopping", nodeID)
		log.Ctx(ctx).Warn(msg)
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
	if s.meta.CollectionManager.CalculateLoadPercentage(ctx, req.GetCollectionID()) < 100 {
		err := merr.WrapErrCollectionNotFullyLoaded(req.GetCollectionID())
		msg := "can't balance segments of not fully loaded collection"
		log.Warn(msg)
		return merr.Status(err), nil
	}
	srcNode := req.GetSourceNodeIDs()[0]
	replica := s.meta.ReplicaManager.GetByCollectionAndNode(ctx, req.GetCollectionID(), srcNode)
	if replica == nil {
		err := merr.WrapErrNodeNotFound(srcNode, fmt.Sprintf("source node not found in any replica of collection %d", req.GetCollectionID()))
		msg := "source node not found in any replica"
		log.Warn(msg)
		return merr.Status(err), nil
	}
	if err := s.isStoppingNode(ctx, srcNode); err != nil {
		return merr.Status(errors.Wrap(err,
			fmt.Sprintf("can't balance, because the source node[%d] is invalid", srcNode))), nil
	}

	// when no dst node specified, default to use all other nodes in same
	dstNodeSet := typeutil.NewUniqueSet()
	if len(req.GetDstNodeIDs()) == 0 {
		dstNodeSet.Insert(replica.GetRWNodes()...)
	} else {
		for _, dstNode := range req.GetDstNodeIDs() {
			if !replica.Contains(dstNode) {
				err := merr.WrapErrNodeNotFound(dstNode, "destination node not found in the same replica")
				log.Warn("failed to balance to the destination node", zap.Error(err))
				return merr.Status(err), nil
			}
			dstNodeSet.Insert(dstNode)
		}
	}

	// check whether dstNode is healthy
	for dstNode := range dstNodeSet {
		if err := s.isStoppingNode(ctx, dstNode); err != nil {
			return merr.Status(errors.Wrap(err,
				fmt.Sprintf("can't balance, because the destination node[%d] is invalid", dstNode))), nil
		}
	}

	// check sealed segment list
	segments := s.dist.SegmentDistManager.GetByFilter(meta.WithCollectionID(req.GetCollectionID()), meta.WithNodeID(srcNode))
	segmentsMap := lo.SliceToMap(segments, func(s *meta.Segment) (int64, *meta.Segment) {
		return s.GetID(), s
	})

	toBalance := typeutil.NewSet[*meta.Segment]()
	if len(req.GetSealedSegmentIDs()) == 0 {
		toBalance.Insert(segments...)
	} else {
		// check whether sealed segment exist
		for _, segmentID := range req.GetSealedSegmentIDs() {
			segment, ok := segmentsMap[segmentID]
			if !ok {
				err := merr.WrapErrSegmentNotFound(segmentID, "segment not found in source node")
				return merr.Status(err), nil
			}

			// Only balance segments in targets
			existInTarget := s.targetMgr.GetSealedSegment(ctx, segment.GetCollectionID(), segment.GetID(), meta.CurrentTarget) != nil
			if !existInTarget {
				log.Info("segment doesn't exist in current target, skip it", zap.Int64("segmentID", segmentID))
				continue
			}
			toBalance.Insert(segment)
		}
	}

	err := s.balanceSegments(ctx, replica.GetCollectionID(), replica, srcNode, dstNodeSet.Collect(), toBalance.Collect(), true, false)
	if err != nil {
		msg := "failed to balance segments"
		log.Warn(msg, zap.Error(err))
		return merr.Status(errors.Wrap(err, msg)), nil
	}

	return merr.Success(), nil
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
		Status:        merr.Success(),
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
		Status: merr.Success(),
		ComponentName: metricsinfo.ConstructComponentName(typeutil.QueryCoordRole,
			paramtable.GetNodeID()),
	}

	ret, err := s.metricsRequest.ExecuteMetricsRequest(ctx, req)
	if err != nil {
		resp.Status = merr.Status(err)
		return resp, nil
	}

	resp.Response = ret
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
		Status:   merr.Success(),
		Replicas: make([]*milvuspb.ReplicaInfo, 0),
	}

	replicas := s.meta.ReplicaManager.GetByCollection(ctx, req.GetCollectionID())
	if len(replicas) == 0 {
		return resp, nil
	}

	for _, replica := range replicas {
		resp.Replicas = append(resp.Replicas, s.fillReplicaInfo(ctx, replica, req.GetWithShardNodes()))
	}
	return resp, nil
}

func (s *Server) GetShardLeaders(ctx context.Context, req *querypb.GetShardLeadersRequest) (*querypb.GetShardLeadersResponse, error) {
	log := log.Ctx(ctx).WithRateGroup("qcv2.GetShardLeaders", 1, 60).With(
		zap.Int64("collectionID", req.GetCollectionID()),
	)

	log.RatedInfo(10, "get shard leaders request received")
	if err := merr.CheckHealthy(s.State()); err != nil {
		msg := "failed to get shard leaders"
		log.Warn(msg, zap.Error(err))
		return &querypb.GetShardLeadersResponse{
			Status: merr.Status(errors.Wrap(err, msg)),
		}, nil
	}

	leaders, err := utils.GetShardLeaders(ctx, s.meta, s.targetMgr, s.dist, s.nodeMgr, req.GetCollectionID(), req.GetWithUnserviceableShards())
	return &querypb.GetShardLeadersResponse{
		Status: merr.Status(err),
		Shards: leaders,
	}, nil
}

func (s *Server) CheckHealth(ctx context.Context, req *milvuspb.CheckHealthRequest) (*milvuspb.CheckHealthResponse, error) {
	if err := merr.CheckHealthy(s.State()); err != nil {
		return &milvuspb.CheckHealthResponse{Status: merr.Status(err), IsHealthy: false, Reasons: []string{err.Error()}}, nil
	}

	errReasons, err := s.checkNodeHealth(ctx)
	if err != nil || len(errReasons) != 0 {
		return componentutil.CheckHealthRespWithErrMsg(errReasons...), nil
	}

	if err := utils.CheckCollectionsQueryable(ctx, s.meta, s.targetMgr, s.dist, s.nodeMgr); err != nil {
		log.Ctx(ctx).Warn("some collection is not queryable during health check", zap.Error(err))
	}

	return componentutil.CheckHealthRespWithErr(nil), nil
}

func (s *Server) checkNodeHealth(ctx context.Context) ([]string, error) {
	group, ctx := errgroup.WithContext(ctx)
	errReasons := make([]string, 0)

	mu := &sync.Mutex{}
	for _, node := range s.nodeMgr.GetAll() {
		node := node
		group.Go(func() error {
			resp, err := s.cluster.GetComponentStates(ctx, node.ID())
			if err != nil {
				return err
			}

			err = merr.AnalyzeState("QueryNode", node.ID(), resp)
			if err != nil {
				mu.Lock()
				defer mu.Unlock()
				errReasons = append(errReasons, err.Error())
			}
			return nil
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

	err := s.meta.ResourceManager.AddResourceGroup(ctx, req.GetResourceGroup(), req.GetConfig())
	if err != nil {
		log.Warn("failed to create resource group", zap.Error(err))
		return merr.Status(err), nil
	}
	return merr.Success(), nil
}

func (s *Server) UpdateResourceGroups(ctx context.Context, req *querypb.UpdateResourceGroupsRequest) (*commonpb.Status, error) {
	log := log.Ctx(ctx).With(
		zap.Any("rgName", req.GetResourceGroups()),
	)

	log.Info("update resource group request received")
	if err := merr.CheckHealthy(s.State()); err != nil {
		log.Warn("failed to update resource group", zap.Error(err))
		return merr.Status(err), nil
	}

	err := s.meta.ResourceManager.UpdateResourceGroups(ctx, req.GetResourceGroups())
	if err != nil {
		log.Warn("failed to update resource group", zap.Error(err))
		return merr.Status(err), nil
	}
	return merr.Success(), nil
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

	replicas := s.meta.ReplicaManager.GetByResourceGroup(ctx, req.GetResourceGroup())
	if len(replicas) > 0 {
		err := merr.WrapErrParameterInvalid("empty resource group", fmt.Sprintf("resource group %s has collection %d loaded", req.GetResourceGroup(), replicas[0].GetCollectionID()))
		return merr.Status(errors.Wrap(err,
			fmt.Sprintf("some replicas still loaded in resource group[%s], release it first", req.GetResourceGroup()))), nil
	}

	err := s.meta.ResourceManager.RemoveResourceGroup(ctx, req.GetResourceGroup())
	if err != nil {
		log.Warn("failed to drop resource group", zap.Error(err))
		return merr.Status(err), nil
	}
	return merr.Success(), nil
}

// go:deprecated TransferNode transfer nodes between resource groups.
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

	// Move node from source resource group to target resource group.
	if err := s.meta.ResourceManager.TransferNode(ctx, req.GetSourceResourceGroup(), req.GetTargetResourceGroup(), int(req.GetNumNode())); err != nil {
		log.Warn("failed to transfer node", zap.Error(err))
		return merr.Status(err), nil
	}

	return merr.Success(), nil
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

	// TODO: !!!WARNING, replica manager and resource manager doesn't protected with each other by lock.
	if ok := s.meta.ResourceManager.ContainResourceGroup(ctx, req.GetSourceResourceGroup()); !ok {
		err := merr.WrapErrResourceGroupNotFound(req.GetSourceResourceGroup())
		return merr.Status(errors.Wrap(err,
			fmt.Sprintf("the source resource group[%s] doesn't exist", req.GetSourceResourceGroup()))), nil
	}

	if ok := s.meta.ResourceManager.ContainResourceGroup(ctx, req.GetTargetResourceGroup()); !ok {
		err := merr.WrapErrResourceGroupNotFound(req.GetTargetResourceGroup())
		return merr.Status(errors.Wrap(err,
			fmt.Sprintf("the target resource group[%s] doesn't exist", req.GetTargetResourceGroup()))), nil
	}

	// Apply change into replica manager.
	err := s.meta.TransferReplica(ctx, req.GetCollectionID(), req.GetSourceResourceGroup(), req.GetTargetResourceGroup(), int(req.GetNumReplica()))
	return merr.Status(err), nil
}

func (s *Server) ListResourceGroups(ctx context.Context, req *milvuspb.ListResourceGroupsRequest) (*milvuspb.ListResourceGroupsResponse, error) {
	log := log.Ctx(ctx)

	log.Info("list resource group request received")
	resp := &milvuspb.ListResourceGroupsResponse{
		Status: merr.Success(),
	}
	if err := merr.CheckHealthy(s.State()); err != nil {
		log.Warn("failed to list resource group", zap.Error(err))
		resp.Status = merr.Status(err)
		return resp, nil
	}

	resp.ResourceGroups = s.meta.ResourceManager.ListResourceGroups(ctx)
	return resp, nil
}

func (s *Server) DescribeResourceGroup(ctx context.Context, req *querypb.DescribeResourceGroupRequest) (*querypb.DescribeResourceGroupResponse, error) {
	log := log.Ctx(ctx).With(
		zap.String("rgName", req.GetResourceGroup()),
	)

	log.Info("describe resource group request received")
	resp := &querypb.DescribeResourceGroupResponse{
		Status: merr.Success(),
	}
	if err := merr.CheckHealthy(s.State()); err != nil {
		log.Warn("failed to describe resource group", zap.Error(err))
		resp.Status = merr.Status(err)
		return resp, nil
	}

	rg := s.meta.ResourceManager.GetResourceGroup(ctx, req.GetResourceGroup())
	if rg == nil {
		err := merr.WrapErrResourceGroupNotFound(req.GetResourceGroup())
		resp.Status = merr.Status(err)
		return resp, nil
	}

	loadedReplicas := make(map[int64]int32)
	outgoingNodes := make(map[int64]int32)
	replicasInRG := s.meta.GetByResourceGroup(ctx, req.GetResourceGroup())
	for _, replica := range replicasInRG {
		loadedReplicas[replica.GetCollectionID()]++
		for _, node := range replica.GetRONodes() {
			if !s.meta.ContainsNode(ctx, replica.GetResourceGroup(), node) {
				outgoingNodes[replica.GetCollectionID()]++
			}
		}
	}
	incomingNodes := make(map[int64]int32)
	collections := s.meta.GetAll(ctx)
	for _, collection := range collections {
		replicas := s.meta.GetByCollection(ctx, collection)

		for _, replica := range replicas {
			if replica.GetResourceGroup() == req.GetResourceGroup() {
				continue
			}
			for _, node := range replica.GetRONodes() {
				if s.meta.ContainsNode(ctx, req.GetResourceGroup(), node) {
					incomingNodes[collection]++
				}
			}
		}
	}

	nodes := make([]*commonpb.NodeInfo, 0, len(rg.GetNodes()))
	for _, nodeID := range rg.GetNodes() {
		nodeSessionInfo := s.nodeMgr.Get(nodeID)
		if nodeSessionInfo != nil {
			nodes = append(nodes, &commonpb.NodeInfo{
				NodeId:   nodeSessionInfo.ID(),
				Address:  nodeSessionInfo.Addr(),
				Hostname: nodeSessionInfo.Hostname(),
			})
		}
	}

	resp.ResourceGroup = &querypb.ResourceGroupInfo{
		Name:             req.GetResourceGroup(),
		Capacity:         int32(rg.GetCapacity()),
		NumAvailableNode: int32(len(nodes)),
		NumLoadedReplica: loadedReplicas,
		NumOutgoingNode:  outgoingNodes,
		NumIncomingNode:  incomingNodes,
		Config:           rg.GetConfig(),
		Nodes:            nodes,
	}
	return resp, nil
}

func (s *Server) UpdateLoadConfig(ctx context.Context, req *querypb.UpdateLoadConfigRequest) (*commonpb.Status, error) {
	log := log.Ctx(ctx).With(
		zap.Int64s("collectionIDs", req.GetCollectionIDs()),
		zap.Int32("replicaNumber", req.GetReplicaNumber()),
		zap.Strings("resourceGroups", req.GetResourceGroups()),
	)

	log.Info("update load config request received")
	if err := merr.CheckHealthy(s.State()); err != nil {
		msg := "failed to update load config"
		log.Warn(msg, zap.Error(err))
		return merr.Status(errors.Wrap(err, msg)), nil
	}

	jobs := make([]job.Job, 0, len(req.GetCollectionIDs()))
	for _, collectionID := range req.GetCollectionIDs() {
		collection := s.meta.GetCollection(ctx, collectionID)
		if collection == nil {
			err := merr.WrapErrCollectionNotLoaded(collectionID)
			log.Warn("failed to update load config", zap.Error(err))
			continue
		}

		collectionUsedRG := s.meta.ReplicaManager.GetResourceGroupByCollection(ctx, collection.GetCollectionID()).Collect()
		left, right := lo.Difference(collectionUsedRG, req.GetResourceGroups())
		rgChanged := len(left) > 0 || len(right) > 0
		replicaChanged := collection.GetReplicaNumber() != req.GetReplicaNumber()

		subReq := proto.Clone(req).(*querypb.UpdateLoadConfigRequest)
		subReq.CollectionIDs = []int64{collectionID}
		if len(req.ResourceGroups) == 0 {
			subReq.ResourceGroups = collectionUsedRG
			rgChanged = false
		}

		if subReq.GetReplicaNumber() == 0 {
			subReq.ReplicaNumber = collection.GetReplicaNumber()
			replicaChanged = false
		}

		if !replicaChanged && !rgChanged {
			log.Info("no need to update load config", zap.Int64("collectionID", collectionID))
			continue
		}

		updateJob := job.NewUpdateLoadConfigJob(
			ctx,
			subReq,
			s.meta,
			s.targetMgr,
			s.targetObserver,
			s.collectionObserver,
		)

		jobs = append(jobs, updateJob)
		s.jobScheduler.Add(updateJob)
	}

	var err error
	for _, job := range jobs {
		subErr := job.Wait()
		if subErr != nil {
			err = merr.Combine(err, subErr)
		}
	}

	if err != nil {
		msg := "failed to update load config"
		log.Warn(msg, zap.Error(err))
		return merr.Status(errors.Wrap(err, msg)), nil
	}
	log.Info("update load config request finished")

	return merr.Success(), nil
}
