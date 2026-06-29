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
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/coordinator/snmanager"
	"github.com/milvus-io/milvus/internal/querycoordv2/job"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/util/componentutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

var (
// ErrRemoveNodeFromRGFailed      = merr.WrapErrServiceInternalMsg("failed to remove node from resource group")
// ErrTransferNodeFailed          = merr.WrapErrServiceInternalMsg("failed to transfer node between resource group")
// ErrTransferReplicaFailed       = merr.WrapErrServiceInternalMsg("failed to transfer replica between resource group")
// ErrListResourceGroupsFailed    = merr.WrapErrServiceInternalMsg("failed to list resource group")
// ErrDescribeResourceGroupFailed = merr.WrapErrServiceInternalMsg("failed to describe resource group")
// ErrLoadUseWrongRG              = merr.WrapErrServiceInternalMsg("load operation should use collection's resource group")
// ErrLoadWithDefaultRG           = merr.WrapErrServiceInternalMsg("load operation can't use default resource group and other resource group together")
)

func (s *Server) ShowLoadCollections(ctx context.Context, req *querypb.ShowCollectionsRequest) (*querypb.ShowCollectionsResponse, error) {
	mlog.Debug(ctx, "show collections request received", mlog.Int64s("collections", req.GetCollectionIDs()))
	if err := merr.CheckHealthy(s.State()); err != nil {
		msg := "failed to show collections"
		mlog.Warn(ctx, msg, mlog.Err(err))
		return &querypb.ShowCollectionsResponse{
			Status: merr.Status(merr.Wrapf(err, "%s", msg)),
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
		collection := s.meta.GetCollection(ctx, collectionID)
		percentage := s.meta.CalculateLoadPercentage(ctx, collectionID)
		loadFields := s.meta.GetLoadFields(ctx, collectionID)
		refreshProgress := int64(0)
		if percentage < 0 {
			if isGetAll {
				// The collection is released during this,
				// ignore it
				continue
			}
			err := meta.GlobalFailedLoadCache.Get(collectionID)
			if err != nil {
				err = merr.WrapErrCollectionNotLoaded(collectionID, err.Error())
				mlog.Warn(context.TODO(), "show collection failed", mlog.Err(err))
				return &querypb.ShowCollectionsResponse{
					Status: merr.Status(err),
				}, nil
			}

			err = merr.WrapErrCollectionNotLoaded(collectionID)
			mlog.Warn(context.TODO(), "show collection failed", mlog.Err(err))
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
	mlog.Info(context.TODO(), "show partitions request received", mlog.Int64s("partitions", req.GetPartitionIDs()))

	if err := merr.CheckHealthy(s.State()); err != nil {
		msg := "failed to show partitions"
		mlog.Warn(ctx, msg, mlog.Err(err))
		return &querypb.ShowPartitionsResponse{
			Status: merr.Status(merr.Wrapf(err, "%s", msg)),
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
				mlog.Warn(context.TODO(), "show partition failed", mlog.Err(partitionErr))
				return &querypb.ShowPartitionsResponse{
					Status: status,
				}, nil
			}

			err = merr.WrapErrPartitionNotLoaded(partitionID)
			mlog.Warn(context.TODO(), "show partition failed", mlog.Err(err))
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
	logger := mlog.With(
		mlog.Int64("dbID", req.GetDbID()),
		mlog.Int64("collectionID", req.GetCollectionID()),
		mlog.Int32("replicaNumber", req.GetReplicaNumber()),
		mlog.Strings("resourceGroups", req.GetResourceGroups()),
		mlog.Bool("refreshMode", req.GetRefresh()),
	)

	logger.Info(ctx, "load collection request received",
		mlog.Any("schema", req.Schema),
		mlog.Int64s("fieldIndexes", lo.Values(req.GetFieldIndexID())),
	)
	metrics.QueryCoordLoadCount.WithLabelValues(metrics.TotalLabel).Inc()

	if err := merr.CheckHealthy(s.State()); err != nil {
		logger.Warn(ctx, "failed to load collection", mlog.Err(err))
		metrics.QueryCoordLoadCount.WithLabelValues(metrics.FailLabel).Inc()
		return merr.Status(err), nil
	}
	// If refresh mode is ON.
	if req.GetRefresh() {
		err := s.refreshCollection(ctx, req.GetCollectionID())
		if err != nil {
			logger.Warn(ctx, "failed to refresh collection", mlog.Err(err))
			metrics.QueryCoordLoadCount.WithLabelValues(metrics.FailLabel).Inc()
			return merr.Status(err), nil
		}
		logger.Info(ctx, "refresh collection done")
		metrics.QueryCoordLoadCount.WithLabelValues(metrics.SuccessLabel).Inc()
		return merr.Success(), nil
	}

	if err := s.broadcastAlterLoadConfigCollectionV2ForLoadCollection(ctx, req); err != nil {
		logger.Warn(ctx, "failed to load collection", mlog.Err(err))
		metrics.QueryCoordLoadCount.WithLabelValues(metrics.FailLabel).Inc()
		return merr.Status(err), nil
	}

	logger.Info(ctx, "load collection done")
	metrics.QueryCoordLoadCount.WithLabelValues(metrics.SuccessLabel).Inc()
	return merr.Success(), nil
}

func (s *Server) ReleaseCollection(ctx context.Context, req *querypb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	logger := mlog.With(mlog.Int64("collectionID", req.GetCollectionID()))

	logger.Info(ctx, "release collection request received")
	metrics.QueryCoordReleaseCount.WithLabelValues(metrics.TotalLabel).Inc()
	tr := timerecord.NewTimeRecorder("release-collection")

	if err := merr.CheckHealthy(s.State()); err != nil {
		logger.Warn(ctx, "failed to release collection", mlog.Err(err))
		metrics.QueryCoordReleaseCount.WithLabelValues(metrics.FailLabel).Inc()
		return merr.Status(err), nil
	}

	if err := s.broadcastDropLoadConfigCollectionV2ForReleaseCollection(ctx, req); err != nil {
		if errors.Is(err, errReleaseCollectionNotLoaded) {
			logger.Info(ctx, "release collection ignored, collection is not loaded")
			metrics.QueryCoordReleaseCount.WithLabelValues(metrics.SuccessLabel).Inc()
			return merr.Success(), nil
		}
		logger.Warn(ctx, "failed to release collection", mlog.Err(err))
		metrics.QueryCoordReleaseCount.WithLabelValues(metrics.FailLabel).Inc()
		return merr.Status(err), nil
	}
	logger.Info(ctx, "release collection done")
	metrics.QueryCoordReleaseCount.WithLabelValues(metrics.SuccessLabel).Inc()
	metrics.QueryCoordReleaseLatency.WithLabelValues().Observe(float64(tr.ElapseSpan().Milliseconds()))
	return merr.Success(), nil
}

func (s *Server) LoadPartitions(ctx context.Context, req *querypb.LoadPartitionsRequest) (*commonpb.Status, error) {
	logger := mlog.With(
		mlog.Int64("dbID", req.GetDbID()),
		mlog.Int64("collectionID", req.GetCollectionID()),
		mlog.Int32("replicaNumber", req.GetReplicaNumber()),
		mlog.Int64s("partitions", req.GetPartitionIDs()),
		mlog.Strings("resourceGroups", req.GetResourceGroups()),
		mlog.Bool("refreshMode", req.GetRefresh()),
	)

	logger.Info(ctx, "received load partitions request",
		mlog.Any("schema", req.Schema))
	metrics.QueryCoordLoadCount.WithLabelValues(metrics.TotalLabel).Inc()

	if err := merr.CheckHealthy(s.State()); err != nil {
		logger.Warn(ctx, "failed to load partitions", mlog.Err(err))
		metrics.QueryCoordLoadCount.WithLabelValues(metrics.FailLabel).Inc()
		return merr.Status(err), nil
	}

	// If refresh mode is ON.
	if req.GetRefresh() {
		err := s.refreshCollection(ctx, req.GetCollectionID())
		if err != nil {
			logger.Warn(ctx, "failed to refresh partitions", mlog.Err(err))
			metrics.QueryCoordLoadCount.WithLabelValues(metrics.FailLabel).Inc()
			return merr.Status(err), nil
		}
		logger.Info(ctx, "refresh partitions done")
		metrics.QueryCoordLoadCount.WithLabelValues(metrics.SuccessLabel).Inc()
		return merr.Success(), nil
	}

	if err := s.broadcastAlterLoadConfigCollectionV2ForLoadPartitions(ctx, req); err != nil {
		logger.Warn(ctx, "failed to load partitions", mlog.Err(err))
		metrics.QueryCoordLoadCount.WithLabelValues(metrics.FailLabel).Inc()
		return merr.Status(err), nil
	}
	logger.Info(ctx, "load partitions done")
	metrics.QueryCoordLoadCount.WithLabelValues(metrics.SuccessLabel).Inc()
	return merr.Success(), nil
}

func (s *Server) Prewarm(ctx context.Context, req *querypb.PrewarmRequest) (*commonpb.Status, error) {
	logger := mlog.With(
		mlog.Int64("dbID", req.GetDbID()),
		mlog.Int64("collectionID", req.GetCollectionID()),
		mlog.Int64s("partitions", req.GetPartitionIDs()),
		mlog.String("namespace", req.GetNamespace()),
	)

	logger.Info(ctx, "received prewarm request")
	if err := merr.CheckHealthy(s.State()); err != nil {
		logger.Warn(ctx, "failed to prewarm", mlog.Err(err))
		return merr.Status(err), nil
	}

	if err := validatePrewarmRequest(req); err != nil {
		logger.Warn(ctx, "invalid prewarm request", mlog.Err(err))
		return merr.Status(err), nil
	}

	if s.jobScheduler == nil {
		err := merr.WrapErrServiceInternalMsg("querycoord job scheduler is not initialized")
		logger.Warn(ctx, "failed to prewarm", mlog.Err(err))
		return merr.Status(err), nil
	}

	job := newPrewarmJob(ctx, s, req)
	s.jobScheduler.Add(job)
	if err := job.Wait(); err != nil {
		logger.Warn(ctx, "failed to prewarm", mlog.Err(err))
		return merr.Status(err), nil
	}
	logger.Info(ctx, "prewarm done")
	return merr.Success(), nil
}

func (s *Server) ReleasePartitions(ctx context.Context, req *querypb.ReleasePartitionsRequest) (*commonpb.Status, error) {
	logger := mlog.With(
		mlog.Int64("collectionID", req.GetCollectionID()),
		mlog.Int64s("partitionIDs", req.GetPartitionIDs()),
	)

	logger.Info(ctx, "release partitions")
	metrics.QueryCoordReleaseCount.WithLabelValues(metrics.TotalLabel).Inc()

	if err := merr.CheckHealthy(s.State()); err != nil {
		logger.Warn(ctx, "failed to release partitions", mlog.Err(err))
		metrics.QueryCoordReleaseCount.WithLabelValues(metrics.FailLabel).Inc()
		return merr.Status(err), nil
	}

	if len(req.GetPartitionIDs()) == 0 {
		err := merr.WrapErrParameterInvalid("any partition", "empty partition list")
		logger.Warn(ctx, "no partition to release", mlog.Err(err))
		metrics.QueryCoordReleaseCount.WithLabelValues(metrics.FailLabel).Inc()
		return merr.Status(err), nil
	}

	collectionReleased, err := s.broadcastAlterLoadConfigCollectionV2ForReleasePartitions(ctx, req)
	if err != nil {
		logger.Warn(ctx, "failed to release partitions", mlog.Err(err))
		metrics.QueryCoordReleaseCount.WithLabelValues(metrics.FailLabel).Inc()
		return merr.Status(err), nil
	}
	logger.Info(ctx, "release partitions done", mlog.Bool("collectionReleased", collectionReleased))
	metrics.QueryCoordReleaseCount.WithLabelValues(metrics.SuccessLabel).Inc()
	meta.GlobalFailedLoadCache.Remove(req.GetCollectionID())
	return merr.Success(), nil
}

func validatePrewarmRequest(req *querypb.PrewarmRequest) error {
	if req.GetSchema() == nil {
		return merr.WrapErrParameterInvalidMsg("schema is required for prewarm")
	}
	if err := common.CheckNamespace(req.GetSchema(), req.Namespace); err != nil {
		return err
	}
	if !common.IsNamespaceModePartition(req.GetSchema().GetProperties()...) {
		return merr.WrapErrParameterInvalidMsg("prewarm only supports namespace.mode=%s", common.NamespaceModePartition)
	}
	if len(req.GetPartitionIDs()) != 1 {
		return merr.WrapErrParameterInvalid("single namespace partition", fmt.Sprintf("%d partitions", len(req.GetPartitionIDs())))
	}
	return nil
}

func (s *Server) ensurePrewarmPartitionLoaded(ctx context.Context, req *querypb.PrewarmRequest) error {
	partitionID := req.GetPartitionIDs()[0]
	partition := s.meta.GetPartition(ctx, partitionID)
	if partition == nil {
		loadReq := &querypb.LoadPartitionsRequest{
			Base:            req.GetBase(),
			DbID:            req.GetDbID(),
			CollectionID:    req.GetCollectionID(),
			PartitionIDs:    req.GetPartitionIDs(),
			Schema:          req.GetSchema(),
			FieldIndexID:    req.GetFieldIndexID(),
			LoadFields:      req.GetLoadFields(),
			Priority:        req.GetPriority(),
			ForceSyncWarmup: true,
			ReplicaNumber:   req.GetReplicaNumber(),
			ResourceGroups:  req.GetResourceGroups(),
		}
		if err := s.broadcastAlterLoadConfigCollectionV2ForLoadPartitions(ctx, loadReq); err != nil {
			return err
		}
	}

	return s.waitPrewarmPartitionLoaded(ctx, req.GetCollectionID(), partitionID)
}

func (s *Server) waitPrewarmPartitionLoaded(ctx context.Context, collectionID int64, partitionID int64) error {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		if err := ctx.Err(); err != nil {
			return merr.Wrapf(err, "context error while waiting for prewarm partition loaded, collection=%d, partition=%d", collectionID, partitionID)
		}
		if err := meta.GlobalFailedLoadCache.Get(collectionID); err != nil {
			return merr.Wrap(err, "failed to load partition before prewarm")
		}
		partition := s.meta.GetPartition(ctx, partitionID)
		if partition != nil && partition.LoadPercentage >= 100 {
			return job.WaitUpdatePartition(ctx, s.targetObserver, collectionID, partitionID)
		}

		select {
		case <-ctx.Done():
			return merr.Wrapf(ctx.Err(), "context error while waiting for prewarm partition loaded, collection=%d, partition=%d", collectionID, partitionID)
		case <-ticker.C:
		}
	}
}

func (s *Server) prewarmLoadedSegments(ctx context.Context, req *querypb.PrewarmRequest) error {
	partitionID := req.GetPartitionIDs()[0]
	requestedSegments := typeutil.NewUniqueSet(req.GetSegmentIDs()...)
	nodeSegments := make(map[int64][]int64)
	for _, segment := range s.dist.SegmentDistManager.GetByFilter(
		meta.WithCollectionID(req.GetCollectionID()),
		meta.SegmentDistFilterFunc(func(segment *meta.Segment) bool {
			if segment.GetPartitionID() != partitionID {
				return false
			}
			return requestedSegments.Len() == 0 || requestedSegments.Contain(segment.GetID())
		}),
	) {
		nodeSegments[segment.Node] = append(nodeSegments[segment.Node], segment.GetID())
	}
	if len(nodeSegments) == 0 {
		return nil
	}

	group, groupCtx := errgroup.WithContext(ctx)
	for nodeID, segmentIDs := range nodeSegments {
		nodeID := nodeID
		segmentIDs := segmentIDs
		group.Go(func() error {
			status, err := s.cluster.Prewarm(groupCtx, nodeID, &querypb.PrewarmRequest{
				Base:            &commonpb.MsgBase{},
				DbID:            req.GetDbID(),
				CollectionID:    req.GetCollectionID(),
				PartitionIDs:    req.GetPartitionIDs(),
				Schema:          req.GetSchema(),
				Namespace:       req.Namespace,
				SegmentIDs:      segmentIDs,
				Priority:        req.GetPriority(),
				ForceSyncWarmup: true,
			})
			return merr.CheckRPCCall(status, err)
		})
	}
	return group.Wait()
}

func (s *Server) clearPrewarmForceSyncWarmup(ctx context.Context, collectionID int64, partitionID int64) error {
	partition := s.meta.GetPartition(ctx, partitionID)
	if partition != nil && partition.GetForceSyncWarmup() {
		partition = partition.Clone()
		partition.ForceSyncWarmup = false
		if err := s.meta.PutPartition(ctx, partition); err != nil {
			return merr.Wrapf(err, "failed to clear prewarm force sync warmup for partition %d", partitionID)
		}
	}

	collection := s.meta.GetCollection(ctx, collectionID)
	if collection == nil || !collection.GetForceSyncWarmup() {
		return nil
	}

	for _, partition := range s.meta.GetPartitionsByCollection(ctx, collectionID) {
		if partition.GetForceSyncWarmup() {
			return nil
		}
	}

	collection = collection.Clone()
	collection.ForceSyncWarmup = false
	if err := s.meta.PutCollection(ctx, collection); err != nil {
		return merr.Wrapf(err, "failed to clear prewarm force sync warmup for collection %d", collectionID)
	}
	return nil
}

func (s *Server) GetPartitionStates(ctx context.Context, req *querypb.GetPartitionStatesRequest) (*querypb.GetPartitionStatesResponse, error) {
	mlog.Info(context.TODO(), "get partition states", mlog.Int64s("partitions", req.GetPartitionIDs()))

	if err := merr.CheckHealthy(s.State()); err != nil {
		msg := "failed to get partition states"
		mlog.Warn(ctx, msg, mlog.Err(err))
		return &querypb.GetPartitionStatesResponse{
			Status: merr.Status(merr.Wrapf(err, "%s", msg)),
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
				mlog.Warn(ctx, msg)
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
				mlog.Warn(ctx, msg, mlog.Int64("partition", partitionID))
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
		mlog.Warn(ctx, msg)
		return notLoadResp, nil
	}

	return &querypb.GetPartitionStatesResponse{
		Status:                merr.Success(),
		PartitionDescriptions: states,
	}, nil
}

func (s *Server) GetLoadSegmentInfo(ctx context.Context, req *querypb.GetSegmentInfoRequest) (*querypb.GetSegmentInfoResponse, error) {
	mlog.Info(context.TODO(), "get segment info", mlog.Int64s("segments", req.GetSegmentIDs()))

	if err := merr.CheckHealthy(s.State()); err != nil {
		msg := "failed to get segment info"
		mlog.Warn(ctx, msg, mlog.Err(err))
		return &querypb.GetSegmentInfoResponse{
			Status: merr.Status(merr.Wrapf(err, "%s", msg)),
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
				mlog.Warn(ctx, msg, mlog.Int64("segment", segmentID))
				return &querypb.GetSegmentInfoResponse{
					Status: merr.Status(merr.Wrapf(err, "%s", msg)),
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
	mlog.Info(context.TODO(), "received sync new created partition request")

	failedMsg := "failed to sync new created partition"
	if err := merr.CheckHealthy(s.State()); err != nil {
		mlog.Warn(ctx, failedMsg, mlog.Err(err))
		return merr.Status(err), nil
	}

	syncJob := job.NewSyncNewCreatedPartitionJob(ctx, req, s.meta, s.broker, s.targetObserver, s.targetMgr)
	go func() {
		defer func() {
			syncJob.PostExecute()
			syncJob.Done()
		}()

		err := syncJob.PreExecute()
		if err != nil {
			mlog.Warn(ctx, failedMsg, mlog.Err(err))
			syncJob.SetError(err)
			return
		}
		err = syncJob.Execute()
		if err != nil {
			mlog.Warn(ctx, failedMsg, mlog.Err(err))
			syncJob.SetError(err)
			return
		}
	}()
	err := syncJob.Wait()
	if err != nil {
		mlog.Warn(ctx, failedMsg, mlog.Err(err))
		return merr.Status(err), nil
	}

	return merr.Success(), nil
}

// refreshCollection must be called after loading a collection. It looks for new segments that are not loaded yet and
// tries to load them up. It returns when all segments of the given collection are loaded, or when error happens.
// Note that a collection's loading progress always stays at 100% after a successful load and will not get updated
// during refreshCollection.
func (s *Server) refreshCollection(ctx context.Context, collectionID int64) error {
	collection := s.meta.GetCollection(ctx, collectionID)
	if collection == nil {
		return merr.WrapErrCollectionNotLoaded(collectionID)
	}

	// Check that collection is fully loaded.
	if collection.GetStatus() != querypb.LoadStatus_Loaded {
		return merr.WrapErrCollectionNotLoaded(collectionID, "collection not fully loaded")
	}

	// Set a placeholder notifier BEFORE updating the target to avoid a race condition.
	// Without this, the segment checker might run between UpdateNextTarget and SetNotifierCollectionOp,
	// see the new segments in next target, but think IsRefreshed() is true (because the notifier
	// hasn't been set yet), and incorrectly assign LOW priority to import segments.
	placeholderCh := make(chan struct{})
	if err := s.meta.UpdateCollection(ctx, collectionID, meta.SetNotifierCollectionOp(placeholderCh)); err != nil {
		if errors.Is(err, merr.ErrCollectionNotFound) {
			return nil
		}
		return err
	}

	// Pull the latest target.
	readyCh, err := s.targetObserver.UpdateNextTarget(collectionID)
	if err != nil {
		// On failure, close the placeholder channel so IsRefreshed() returns true
		close(placeholderCh)
		return err
	}

	// Replace the placeholder with the real readyCh from target observer
	err = s.meta.UpdateCollection(ctx, collectionID, meta.SetNotifierCollectionOp(readyCh))
	// Close the placeholder channel (no one is waiting on it, but good practice)
	close(placeholderCh)

	// if collection already released, treat as success
	if errors.Is(err, merr.ErrCollectionNotFound) {
		return nil
	}
	return err
}

// This is totally same to refreshCollection, remove it for now
// refreshPartitions must be called after loading a collection. It looks for new segments that are not loaded yet and
// tries to load them up. It returns when all segments of the given collection are loaded, or when error happens.
// Note that a collection's loading progress always stays at 100% after a successful load and will not get updated
// during refreshPartitions.
// func (s *Server) refreshPartitions(ctx context.Context, collID int64, partIDs []int64) (*commonpb.Status, error) {
// 	ctx, cancel := context.WithTimeout(ctx, Params.QueryCoordCfg.LoadTimeoutSeconds.GetAsDuration(time.Second))
// 	defer cancel()

// 	log := mlog.With(
// 		mlog.Int64("collectionID", collID),
// 		mlog.Int64s("partitionIDs", partIDs),
// 	)
// 	if s.status.Load() != commonpb.StateCode_Healthy {
// 		msg := "failed to refresh partitions"
// 		mlog.Warn(ctx, msg, mlog.Err(ErrNotHealthy))
// 		metrics.QueryCoordReleaseCount.WithLabelValues(metrics.FailLabel).Inc()
// 		return utils.WrapStatus(commonpb.ErrorCode_UnexpectedError, msg, ErrNotHealthy), nil
// 	}

// 	// Check that all partitions are fully loaded.
// 	if s.meta.CollectionManager.GetCurrentLoadPercentage(collID) != 100 {
// 		errMsg := "partitions must be fully loaded before refreshing"
// 		mlog.Warn(errMsg)
// 		return &commonpb.Status{
// 			ErrorCode: commonpb.ErrorCode_UnexpectedError,
// 			Reason:    errMsg,
// 		}, nil
// 	}

// 	// Pull the latest target.
// 	readyCh, err := s.targetObserver.UpdateNextTarget(collID)
// 	if err != nil {
// 		mlog.Warn(context.TODO(), "failed to update next target", mlog.Err(err))
// 		return &commonpb.Status{
// 			ErrorCode: commonpb.ErrorCode_UnexpectedError,
// 			Reason:    err.Error(),
// 		}, nil
// 	}

// 	select {
// 	case <-ctx.Done():
// 		mlog.Warn(context.TODO(), "refresh partitions failed as context canceled")
// 		return &commonpb.Status{
// 			ErrorCode: commonpb.ErrorCode_UnexpectedError,
// 			Reason:    "context canceled",
// 		}, nil
// 	case <-readyCh:
// 		mlog.Info(context.TODO(), "refresh partitions succeeded")
// 		return &commonpb.Status{
// 			ErrorCode: commonpb.ErrorCode_Success,
// 		}, nil
// 	}
// }

func (s *Server) isStoppingNode(ctx context.Context, nodeID int64) error {
	isStopping, err := s.nodeMgr.IsStoppingNode(nodeID)
	if err != nil {
		mlog.Warn(ctx, "fail to check whether the node is stopping", mlog.Int64("node_id", nodeID), mlog.Err(err))
		return err
	}
	if isStopping {
		msg := fmt.Sprintf("failed to balance due to the source/destination node[%d] is stopping", nodeID)
		mlog.Warn(ctx, msg)
		return merr.WrapErrServiceUnavailable(msg)
	}
	return nil
}

func (s *Server) LoadBalance(ctx context.Context, req *querypb.LoadBalanceRequest) (*commonpb.Status, error) {
	mlog.Info(context.TODO(), "load balance request received",
		mlog.Int64s("source", req.GetSourceNodeIDs()),
		mlog.Int64s("dest", req.GetDstNodeIDs()),
		mlog.Int64s("segments", req.GetSealedSegmentIDs()))

	if err := merr.CheckHealthy(s.State()); err != nil {
		msg := "failed to load balance"
		mlog.Warn(ctx, msg, mlog.Err(err))
		return merr.Status(merr.Wrapf(err, "%s", msg)), nil
	}

	// Verify request
	if len(req.GetSourceNodeIDs()) != 1 {
		err := merr.WrapErrParameterInvalid("only 1 source node", fmt.Sprintf("%d source nodes", len(req.GetSourceNodeIDs())))
		msg := "source nodes can only contain 1 node"
		mlog.Warn(ctx, msg, mlog.Int("source-nodes-num", len(req.GetSourceNodeIDs())))
		return merr.Status(err), nil
	}
	if s.meta.CalculateLoadPercentage(ctx, req.GetCollectionID()) < 100 {
		err := merr.WrapErrCollectionNotFullyLoaded(req.GetCollectionID())
		msg := "can't balance segments of not fully loaded collection"
		mlog.Warn(ctx, msg)
		return merr.Status(err), nil
	}
	srcNode := req.GetSourceNodeIDs()[0]
	replica := s.meta.GetByCollectionAndNode(ctx, req.GetCollectionID(), srcNode)
	if replica == nil || !replica.ContainRWNode(srcNode) {
		err := merr.WrapErrNodeNotFound(srcNode, fmt.Sprintf("source node not found in any replica of collection %d", req.GetCollectionID()))
		msg := "source node not found in any replica"
		mlog.Warn(ctx, msg)
		return merr.Status(err), nil
	}
	if err := s.isStoppingNode(ctx, srcNode); err != nil {
		return merr.Status(merr.Wrapf(err, "can't balance, because the source node[%d] is invalid", srcNode)), nil
	}

	// when no dst node specified, default to use all other nodes in same
	dstNodeSet := typeutil.NewUniqueSet()
	if len(req.GetDstNodeIDs()) == 0 {
		dstNodeSet.Insert(replica.GetRWNodes()...)
	} else {
		for _, dstNode := range req.GetDstNodeIDs() {
			if !replica.Contains(dstNode) {
				err := merr.WrapErrNodeNotFound(dstNode, "destination node not found in the same replica")
				mlog.Warn(context.TODO(), "failed to balance to the destination node", mlog.Err(err))
				return merr.Status(err), nil
			}
			dstNodeSet.Insert(dstNode)
		}
	}

	// check whether dstNode is healthy
	for dstNode := range dstNodeSet {
		if err := s.isStoppingNode(ctx, dstNode); err != nil {
			return merr.Status(merr.Wrapf(err, "can't balance, because the destination node[%d] is invalid", dstNode)), nil
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
				mlog.Info(context.TODO(), "segment doesn't exist in current target, skip it", mlog.Int64("segmentID", segmentID))
				continue
			}
			toBalance.Insert(segment)
		}
	}

	err := s.balanceSegments(ctx, replica.GetCollectionID(), replica, srcNode, dstNodeSet.Collect(), toBalance.Collect(), true, false)
	if err != nil {
		msg := "failed to balance segments"
		mlog.Warn(ctx, msg, mlog.Err(err))
		return merr.Status(merr.Wrapf(err, "%s", msg)), nil
	}

	return merr.Success(), nil
}

func (s *Server) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest) (*internalpb.ShowConfigurationsResponse, error) {
	mlog.Info(context.TODO(), "show configurations request received", mlog.String("pattern", req.GetPattern()))

	if err := merr.CheckHealthy(s.State()); err != nil {
		msg := "failed to show configurations"
		mlog.Warn(ctx, msg, mlog.Err(err))
		return &internalpb.ShowConfigurationsResponse{
			Status: merr.Status(merr.Wrapf(err, "%s", msg)),
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
	mlog.RatedDebug(context.TODO(), rate.Limit(60), "get metrics request received",
		mlog.String("metricType", req.GetRequest()))

	if err := merr.CheckHealthy(s.State()); err != nil {
		msg := "failed to get metrics"
		mlog.Warn(ctx, msg, mlog.Err(err))
		return &milvuspb.GetMetricsResponse{
			Status: merr.Status(merr.Wrapf(err, "%s", msg)),
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
	mlog.Info(context.TODO(), "get replicas request received", mlog.Bool("with-shard-nodes", req.GetWithShardNodes()))

	if err := merr.CheckHealthy(s.State()); err != nil {
		msg := "failed to get replicas"
		mlog.Warn(ctx, msg, mlog.Err(err))
		return &milvuspb.GetReplicasResponse{
			Status: merr.Status(merr.Wrapf(err, "%s", msg)),
		}, nil
	}

	resp := &milvuspb.GetReplicasResponse{
		Status:   merr.Success(),
		Replicas: make([]*milvuspb.ReplicaInfo, 0),
	}

	replicas := s.meta.GetByCollection(ctx, req.GetCollectionID())
	if len(replicas) == 0 {
		return resp, nil
	}

	for _, replica := range replicas {
		resp.Replicas = append(resp.Replicas, s.fillReplicaInfo(ctx, replica, req.GetWithShardNodes()))
	}
	return resp, nil
}

func (s *Server) GetShardLeaders(ctx context.Context, req *querypb.GetShardLeadersRequest) (*querypb.GetShardLeadersResponse, error) {
	mlog.RatedInfo(context.TODO(), rate.Limit(10), "get shard leaders request received")
	if err := merr.CheckHealthy(s.State()); err != nil {
		msg := "failed to get shard leaders"
		mlog.Warn(ctx, msg, mlog.Err(err))
		return &querypb.GetShardLeadersResponse{
			Status: merr.Status(merr.Wrapf(err, "%s", msg)),
		}, nil
	}

	leaders, err := utils.GetShardLeadersWithReplicaFilter(ctx,
		s.meta,
		s.targetMgr,
		s.dist,
		s.nodeMgr,
		req.GetCollectionID(),
		req.GetWithUnserviceableShards(),
		func(replica *meta.Replica) bool {
			return replica.IsQueryVisible()
		})
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
		mlog.Warn(ctx, "some collection is not queryable during health check", mlog.Err(err))
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
	mlog.Info(context.TODO(), "create resource group request received")
	if err := merr.CheckHealthy(s.State()); err != nil {
		mlog.Warn(context.TODO(), "failed to create resource group", mlog.Err(err))
		return merr.Status(err), nil
	}

	ignored, err := s.broadcastCreateResourceGroup(ctx, req)
	if err != nil {
		mlog.Warn(context.TODO(), "failed to create resource group", mlog.Err(err))
		return merr.Status(err), nil
	}
	if ignored {
		mlog.Info(context.TODO(), "create resource group request ignored")
		return merr.Success(), nil
	}
	mlog.Info(context.TODO(), "create resource group done")
	return merr.Success(), nil
}

func (s *Server) UpdateResourceGroups(ctx context.Context, req *querypb.UpdateResourceGroupsRequest) (*commonpb.Status, error) {
	mlog.Info(context.TODO(), "update resource group request received")
	if err := merr.CheckHealthy(s.State()); err != nil {
		mlog.Warn(context.TODO(), "failed to update resource group", mlog.Err(err))
		return merr.Status(err), nil
	}

	if err := s.broadcastUpdateResourceGroups(ctx, req); err != nil {
		mlog.Warn(context.TODO(), "failed to update resource group", mlog.Err(err))
		return merr.Status(err), nil
	}
	mlog.Info(context.TODO(), "update resource group done")
	return merr.Success(), nil
}

func (s *Server) DropResourceGroup(ctx context.Context, req *milvuspb.DropResourceGroupRequest) (*commonpb.Status, error) {
	mlog.Info(context.TODO(), "drop resource group request received")
	if err := merr.CheckHealthy(s.State()); err != nil {
		mlog.Warn(context.TODO(), "failed to drop resource group", mlog.Err(err))
		return merr.Status(err), nil
	}

	ignored, err := s.broadcastDropResourceGroup(ctx, req)
	if err != nil {
		mlog.Warn(context.TODO(), "failed to drop resource group", mlog.Err(err))
		return merr.Status(err), nil
	}
	if ignored {
		mlog.Info(context.TODO(), "drop resource group request ignored")
		return merr.Success(), nil
	}
	mlog.Info(context.TODO(), "drop resource group done")
	return merr.Success(), nil
}

// Deprecated: TransferNode transfer nodes between resource groups.
// Use UpdateResourceGroups instead.
func (s *Server) TransferNode(ctx context.Context, req *milvuspb.TransferNodeRequest) (*commonpb.Status, error) {
	mlog.Info(context.TODO(), "transfer node between resource group request received")
	if err := merr.CheckHealthy(s.State()); err != nil {
		mlog.Warn(context.TODO(), "failed to transfer node between resource group", mlog.Err(err))
		return merr.Status(err), nil
	}

	if err := s.broadcastTransferNode(ctx, req); err != nil {
		mlog.Warn(context.TODO(), "failed to transfer node", mlog.Err(err))
		return merr.Status(err), nil
	}
	mlog.Info(context.TODO(), "transfer node done")
	return merr.Success(), nil
}

func (s *Server) TransferReplica(ctx context.Context, req *querypb.TransferReplicaRequest) (*commonpb.Status, error) {
	mlog.Info(context.TODO(), "transfer replica request received")
	if err := merr.CheckHealthy(s.State()); err != nil {
		mlog.Warn(context.TODO(), "failed to transfer replica between resource group", mlog.Err(err))
		return merr.Status(err), nil
	}

	if err := s.broadcastAlterLoadConfigCollectionV2ForTransferReplica(ctx, req); err != nil {
		mlog.Warn(context.TODO(), "failed to transfer replica between resource group", mlog.Err(err))
		return merr.Status(err), nil
	}
	mlog.Info(context.TODO(), "transfer replica done")
	return merr.Success(), nil
}

func (s *Server) ListResourceGroups(ctx context.Context, req *milvuspb.ListResourceGroupsRequest) (*milvuspb.ListResourceGroupsResponse, error) {
	mlog.Info(context.TODO(), "list resource group request received")
	resp := &milvuspb.ListResourceGroupsResponse{
		Status: merr.Success(),
	}
	if err := merr.CheckHealthy(s.State()); err != nil {
		mlog.Warn(context.TODO(), "failed to list resource group", mlog.Err(err))
		resp.Status = merr.Status(err)
		return resp, nil
	}

	resp.ResourceGroups = s.meta.ListResourceGroups(ctx)
	return resp, nil
}

func (s *Server) DescribeResourceGroup(ctx context.Context, req *querypb.DescribeResourceGroupRequest) (*querypb.DescribeResourceGroupResponse, error) {
	mlog.Info(context.TODO(), "describe resource group request received")
	resp := &querypb.DescribeResourceGroupResponse{
		Status: merr.Success(),
	}
	if err := merr.CheckHealthy(s.State()); err != nil {
		mlog.Warn(context.TODO(), "failed to describe resource group", mlog.Err(err))
		resp.Status = merr.Status(err)
		return resp, nil
	}

	rg := s.meta.GetResourceGroup(ctx, req.GetResourceGroup())
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
	mlog.Info(context.TODO(), "update load config request received")
	if err := merr.CheckHealthy(s.State()); err != nil {
		msg := "failed to update load config"
		mlog.Warn(ctx, msg, mlog.Err(err))
		return merr.Status(merr.Wrapf(err, "%s", msg)), nil
	}

	err := s.updateLoadConfig(ctx, req.GetCollectionIDs(), req.GetReplicaNumber(), req.GetResourceGroups())
	if err != nil {
		msg := "failed to update load config"
		mlog.Warn(ctx, msg, mlog.Err(err))
		return merr.Status(merr.Wrapf(err, "%s", msg)), nil
	}

	mlog.Info(context.TODO(), "update load config request finished")

	return merr.Success(), nil
}

func (s *Server) updateLoadConfig(ctx context.Context, collectionIDs []int64, newReplicaNum int32, newRGs []string, needWaitRGReady ...bool) error {
	jobs := make([]job.Job, 0, len(collectionIDs))
	for _, collectionID := range collectionIDs {
		collection := s.meta.GetCollection(ctx, collectionID)
		if collection == nil {
			err := merr.WrapErrCollectionNotLoaded(collectionID)
			mlog.Warn(context.TODO(), "failed to update load config", mlog.Err(err))
			continue
		}

		collectionUsedRG := s.meta.ReplicaManager.GetResourceGroupByCollection(ctx, collection.GetCollectionID()).Collect()
		left, right := lo.Difference(collectionUsedRG, newRGs)
		rgChanged := len(left) > 0 || len(right) > 0
		replicaChanged := collection.GetReplicaNumber() != newReplicaNum

		subReq := &querypb.UpdateLoadConfigRequest{
			CollectionIDs:  []int64{collectionID},
			ReplicaNumber:  newReplicaNum,
			ResourceGroups: newRGs,
		}
		if len(subReq.GetResourceGroups()) == 0 {
			subReq.ResourceGroups = collectionUsedRG
			rgChanged = false
		}

		if subReq.GetReplicaNumber() == 0 {
			subReq.ReplicaNumber = collection.GetReplicaNumber()
			replicaChanged = false
		}

		if !replicaChanged && !rgChanged {
			mlog.Info(context.TODO(), "no need to update load config", mlog.Int64("collectionID", collectionID))
			continue
		}

		waitRG := len(needWaitRGReady) > 0 && needWaitRGReady[0]
		updateJob := job.NewUpdateLoadConfigJob(
			ctx,
			subReq,
			s.meta,
			s.targetMgr,
			s.targetObserver,
			s.collectionObserver,
			s.proxyClientManager,
			false,
			waitRG,
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

	return err
}

func (s *Server) ListLoadedSegments(ctx context.Context, req *querypb.ListLoadedSegmentsRequest) (*querypb.ListLoadedSegmentsResponse, error) {
	if err := merr.CheckHealthy(s.State()); err != nil {
		return &querypb.ListLoadedSegmentsResponse{
			Status: merr.Status(merr.Wrap(err, "failed to list loaded segments")),
		}, nil
	}
	segmentIDs := typeutil.NewUniqueSet()

	collections := s.meta.GetAllCollections(ctx)
	for _, collection := range collections {
		segments := s.targetMgr.GetSealedSegmentsByCollection(ctx, collection.GetCollectionID(), meta.CurrentTarget)
		for _, segment := range segments {
			segmentIDs.Insert(segment.ID)
		}
		segments = s.targetMgr.GetSealedSegmentsByCollection(ctx, collection.GetCollectionID(), meta.NextTarget)
		for _, segment := range segments {
			segmentIDs.Insert(segment.ID)
		}
	}

	segments := s.dist.SegmentDistManager.GetByFilter()
	for _, segment := range segments {
		segmentIDs.Insert(segment.ID)
	}

	resp := &querypb.ListLoadedSegmentsResponse{
		Status:     merr.Success(),
		SegmentIDs: segmentIDs.Collect(),
	}
	return resp, nil
}

func (s *Server) RunAnalyzer(ctx context.Context, req *querypb.RunAnalyzerRequest) (*milvuspb.RunAnalyzerResponse, error) {
	if err := merr.CheckHealthy(s.State()); err != nil {
		return &milvuspb.RunAnalyzerResponse{
			Status: merr.Status(merr.Wrap(err, "failed to run analyzer")),
		}, nil
	}

	nodeIDs := snmanager.StaticStreamingNodeManager.GetStreamingQueryNodeIDs().Collect()

	if len(nodeIDs) == 0 {
		return &milvuspb.RunAnalyzerResponse{
			Status: merr.Status(merr.WrapErrServiceUnavailable("failed to validate analyzer, no delegator")),
		}, nil
	}

	idx := s.nodeIdx.Inc() % uint32(len(nodeIDs))
	resp, err := s.cluster.RunAnalyzer(ctx, nodeIDs[idx], req)
	if err != nil {
		return &milvuspb.RunAnalyzerResponse{
			Status: merr.Status(err),
		}, nil
	}
	return resp, nil
}

func (s *Server) ValidateAnalyzer(ctx context.Context, req *querypb.ValidateAnalyzerRequest) (*querypb.ValidateAnalyzerResponse, error) {
	if err := merr.CheckHealthy(s.State()); err != nil {
		return &querypb.ValidateAnalyzerResponse{Status: merr.Status(merr.Wrap(err, "failed to validate analyzer"))}, nil
	}

	nodeIDs := snmanager.StaticStreamingNodeManager.GetStreamingQueryNodeIDs().Collect()

	if len(nodeIDs) == 0 {
		return &querypb.ValidateAnalyzerResponse{Status: merr.Status(merr.WrapErrServiceUnavailable("failed to validate analyzer, no delegator"))}, nil
	}

	idx := s.nodeIdx.Inc() % uint32(len(nodeIDs))
	resp, err := s.cluster.ValidateAnalyzer(ctx, nodeIDs[idx], req)
	if err != nil {
		return &querypb.ValidateAnalyzerResponse{Status: merr.Status(err)}, nil
	}
	return resp, nil
}

func (s *Server) ComputePhraseMatchSlop(ctx context.Context, req *querypb.ComputePhraseMatchSlopRequest) (*querypb.ComputePhraseMatchSlopResponse, error) {
	if err := merr.CheckHealthy(s.State()); err != nil {
		return &querypb.ComputePhraseMatchSlopResponse{
			Status: merr.Status(merr.Wrap(err, "failed to compute phrase match slop")),
		}, nil
	}

	nodeIDs := snmanager.StaticStreamingNodeManager.GetStreamingQueryNodeIDs().Collect()

	if len(nodeIDs) == 0 {
		return &querypb.ComputePhraseMatchSlopResponse{
			Status: merr.Status(merr.WrapErrServiceUnavailable("failed to compute phrase match slop, no query node available")),
		}, nil
	}

	idx := s.nodeIdx.Inc() % uint32(len(nodeIDs))
	resp, err := s.cluster.ComputePhraseMatchSlop(ctx, nodeIDs[idx], req)
	if err != nil {
		return &querypb.ComputePhraseMatchSlopResponse{
			Status: merr.Status(err),
		}, nil
	}
	return resp, nil
}

// ManualUpdateCurrentTarget is used to manually update the current target for TruncateCollection
func (s *Server) ManualUpdateCurrentTarget(ctx context.Context, collectionID int64) error {
	mlog.Info(context.TODO(), "manual update current target request received")

	if err := merr.CheckHealthy(s.State()); err != nil {
		mlog.Warn(context.TODO(), "failed to manual update current target", mlog.Err(err))
		return err
	}

	// Check if collection is loaded
	percentage := s.meta.CalculateLoadPercentage(ctx, collectionID)
	if percentage < 0 {
		mlog.Info(context.TODO(), "collection not loaded, skip ManualUpdateCurrentTarget")
		return nil
	}

	err := job.WaitCurrentTargetUpdated(ctx, s.targetObserver, collectionID)
	if err != nil {
		mlog.Warn(context.TODO(), "failed to wait current target updated", mlog.Err(err))
		return err
	}

	mlog.Info(context.TODO(), "manual update current target done")
	return nil
}
