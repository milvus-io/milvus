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

package delegator

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querynodev2/cluster"
	"github.com/milvus-io/milvus/internal/querynodev2/pkoracle"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/retry"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

const (
	ForwardPolicyDefault         = ``
	L0ForwardPolicyBF            = `FilterByBF`
	L0ForwardPolicyRemoteLoad    = `RemoteLoad`
	StreamingForwardPolicyBF     = `FilterByBF`
	StreamingForwardPolicyDirect = `Direct`
)

func (sd *shardDelegator) forwardL0Deletion(ctx context.Context,
	info *querypb.SegmentLoadInfo,
	req *querypb.LoadSegmentsRequest,
	candidate *pkoracle.BloomFilterSet,
	targetNodeID int64,
	worker cluster.Worker,
) error {
	switch sd.l0ForwardPolicy {
	case ForwardPolicyDefault, L0ForwardPolicyBF:
		return sd.forwardL0ByBF(ctx, info, candidate, targetNodeID, worker)
	case L0ForwardPolicyRemoteLoad:
		return sd.forwardL0RemoteLoad(ctx, info, req, targetNodeID, worker)
	default:
		return merr.WrapErrServiceInternal("Unknown l0 forward policy: %s", sd.l0ForwardPolicy)
	}
}

func (sd *shardDelegator) forwardStreamingDeletion(ctx context.Context, deleteData []*DeleteData) {
	// TODO @congqixia add `auto` policy
	// using direct when streaming size is too large
	// need some experimental data to support this policy
	switch policy := paramtable.Get().QueryNodeCfg.StreamingDeltaForwardPolicy.GetValue(); policy {
	case ForwardPolicyDefault, StreamingForwardPolicyBF:
		sd.forwardStreamingByBF(ctx, deleteData)
	case StreamingForwardPolicyDirect:
		// forward streaming deletion without bf filtering
		sd.forwardStreamingDirect(ctx, deleteData)
	default:
		log.Fatal("unsupported streaming forward policy", zap.String("policy", policy))
	}
}

func (sd *shardDelegator) addL0ForGrowing(ctx context.Context, segment segments.Segment) error {
	switch sd.l0ForwardPolicy {
	case ForwardPolicyDefault, L0ForwardPolicyBF:
		return sd.addL0GrowingBF(ctx, segment)
	case L0ForwardPolicyRemoteLoad:
		// forward streaming deletion without bf filtering
		return sd.addL0ForGrowingLoad(ctx, segment)
	default:
		log.Fatal("unsupported l0 forward policy", zap.String("policy", sd.l0ForwardPolicy))
	}
	return nil
}

func (sd *shardDelegator) addL0GrowingBF(ctx context.Context, segment segments.Segment) error {
	deletedPks, deletedTss := sd.GetLevel0Deletions(segment.Partition(), pkoracle.NewCandidateKey(segment.ID(), segment.Partition(), segments.SegmentTypeGrowing))
	if deletedPks == nil || deletedPks.Len() == 0 {
		return nil
	}

	log.Info("forwarding L0 delete records...", zap.Int64("segmentID", segment.ID()), zap.Int("deletionCount", deletedPks.Len()))
	return segment.Delete(ctx, deletedPks, deletedTss)
}

func (sd *shardDelegator) addL0ForGrowingLoad(ctx context.Context, segment segments.Segment) error {
	deltalogs := sd.getLevel0Deltalogs(segment.Partition())
	log.Info("forwarding L0 via loader...", zap.Int64("segmentID", segment.ID()), zap.Int("deltalogsNum", len(deltalogs)))
	return sd.loader.LoadDeltaLogs(ctx, segment, deltalogs)
}

func (sd *shardDelegator) forwardL0ByBF(ctx context.Context,
	info *querypb.SegmentLoadInfo,
	candidate *pkoracle.BloomFilterSet,
	targetNodeID int64,
	worker cluster.Worker,
) error {
	// after L0 segment feature
	// growing segemnts should have load stream delete as well
	deleteScope := querypb.DataScope_All
	switch candidate.Type() {
	case commonpb.SegmentState_Sealed:
		deleteScope = querypb.DataScope_Historical
	case commonpb.SegmentState_Growing:
		deleteScope = querypb.DataScope_Streaming
	}

	deletedPks, deletedTss := sd.GetLevel0Deletions(candidate.Partition(), candidate)
	if deletedPks != nil && deletedPks.Len() > 0 {
		log.Info("forward L0 delete to worker...",
			zap.Int("deleteRowNum", deletedPks.Len()),
		)
		pks, err := storage.ParsePrimaryKeysBatch2IDs(deletedPks)
		if err != nil {
			return err
		}
		err = worker.Delete(ctx, &querypb.DeleteRequest{
			Base:         commonpbutil.NewMsgBase(commonpbutil.WithTargetID(targetNodeID)),
			CollectionId: info.GetCollectionID(),
			PartitionId:  info.GetPartitionID(),
			SegmentId:    info.GetSegmentID(),
			PrimaryKeys:  pks,
			Timestamps:   deletedTss,
			Scope:        deleteScope,
		})
		if err != nil {
			log.Warn("failed to apply delete when LoadSegment", zap.Error(err))
			return err
		}
	}
	return nil
}

func (sd *shardDelegator) forwardL0RemoteLoad(ctx context.Context,
	info *querypb.SegmentLoadInfo,
	req *querypb.LoadSegmentsRequest,
	targetNodeID int64,
	worker cluster.Worker,
) error {
	info = typeutil.Clone(info)
	// load l0 segment deltalogs
	info.Deltalogs = sd.getLevel0Deltalogs(info.GetPartitionID())

	return worker.LoadSegments(ctx, &querypb.LoadSegmentsRequest{
		Base: &commonpb.MsgBase{
			TargetID: targetNodeID,
		},
		DstNodeID: targetNodeID,
		Infos: []*querypb.SegmentLoadInfo{
			info,
		},
		CollectionID:  info.GetCollectionID(),
		LoadScope:     querypb.LoadScope_Delta,
		Schema:        req.GetSchema(),
		IndexInfoList: req.GetIndexInfoList(),
		Version:       req.GetVersion(),
	})
}

func (sd *shardDelegator) getLevel0Deltalogs(partitionID int64) []*datapb.FieldBinlog {
	level0Segments := sd.segmentManager.GetBy(
		segments.WithLevel(datapb.SegmentLevel_L0),
		segments.WithChannel(sd.vchannelName))

	var deltalogs []*datapb.FieldBinlog

	for _, segment := range level0Segments {
		if segment.Partition() != common.AllPartitionsID && segment.Partition() != partitionID {
			continue
		}
		segment := segment.(*segments.L0Segment)
		deltalogs = append(deltalogs, segment.LoadInfo().GetDeltalogs()...)
	}

	return deltalogs
}

func (sd *shardDelegator) forwardStreamingByBF(ctx context.Context, deleteData []*DeleteData) {
	start := time.Now()
	retMap := sd.applyBFInParallel(deleteData, segments.GetBFApplyPool())
	// segment => delete data
	delRecords := make(map[int64]DeleteData)
	retMap.Range(func(key int, value *BatchApplyRet) bool {
		startIdx := value.StartIdx
		pk2SegmentIDs := value.Segment2Hits

		pks := deleteData[value.DeleteDataIdx].PrimaryKeys
		tss := deleteData[value.DeleteDataIdx].Timestamps

		for segmentID, hits := range pk2SegmentIDs {
			for i, hit := range hits {
				if hit {
					delRecord := delRecords[segmentID]
					delRecord.PrimaryKeys = append(delRecord.PrimaryKeys, pks[startIdx+i])
					delRecord.Timestamps = append(delRecord.Timestamps, tss[startIdx+i])
					delRecord.RowCount++
					delRecords[segmentID] = delRecord
				}
			}
		}
		return true
	})
	bfCost := time.Since(start)

	offlineSegments := typeutil.NewConcurrentSet[int64]()

	sealed, growing, version := sd.distribution.PinOnlineSegments()

	start = time.Now()
	eg, ctx := errgroup.WithContext(context.Background())
	for _, entry := range sealed {
		entry := entry
		eg.Go(func() error {
			worker, err := sd.workerManager.GetWorker(ctx, entry.NodeID)
			if err != nil {
				log.Warn("failed to get worker",
					zap.Int64("nodeID", paramtable.GetNodeID()),
					zap.Error(err),
				)
				// skip if node down
				// delete will be processed after loaded again
				return nil
			}
			offlineSegments.Upsert(sd.applyDelete(ctx, entry.NodeID, worker, func(segmentID int64) (DeleteData, bool) {
				data, ok := delRecords[segmentID]
				return data, ok
			}, entry.Segments, querypb.DataScope_Historical)...)
			return nil
		})
	}
	if len(growing) > 0 {
		eg.Go(func() error {
			worker, err := sd.workerManager.GetWorker(ctx, paramtable.GetNodeID())
			if err != nil {
				log.Error("failed to get worker(local)",
					zap.Int64("nodeID", paramtable.GetNodeID()),
					zap.Error(err),
				)
				// panic here, local worker shall not have error
				panic(err)
			}
			offlineSegments.Upsert(sd.applyDelete(ctx, paramtable.GetNodeID(), worker, func(segmentID int64) (DeleteData, bool) {
				data, ok := delRecords[segmentID]
				return data, ok
			}, growing, querypb.DataScope_Streaming)...)
			return nil
		})
	}
	// not error return in apply delete
	_ = eg.Wait()
	forwardDeleteCost := time.Since(start)

	sd.distribution.Unpin(version)
	offlineSegIDs := offlineSegments.Collect()
	if len(offlineSegIDs) > 0 {
		log.Warn("failed to apply delete, mark segment offline", zap.Int64s("offlineSegments", offlineSegIDs))
		sd.markSegmentOffline(offlineSegIDs...)
	}

	metrics.QueryNodeApplyBFCost.WithLabelValues("ProcessDelete", fmt.Sprint(paramtable.GetNodeID())).Observe(float64(bfCost.Milliseconds()))
	metrics.QueryNodeForwardDeleteCost.WithLabelValues("ProcessDelete", fmt.Sprint(paramtable.GetNodeID())).Observe(float64(forwardDeleteCost.Milliseconds()))
}

func (sd *shardDelegator) forwardStreamingDirect(ctx context.Context, deleteData []*DeleteData) {
	start := time.Now()

	// group by partition id
	groups := lo.GroupBy(deleteData, func(delData *DeleteData) int64 {
		return delData.PartitionID
	})

	offlineSegments := typeutil.NewConcurrentSet[int64]()
	eg, ctx := errgroup.WithContext(ctx)

	for partitionID, group := range groups {
		partitionID := partitionID
		group := group
		eg.Go(func() error {
			partitions := []int64{partitionID}
			// check if all partitions
			if partitionID == common.AllPartitionsID {
				partitions = []int64{}
			}
			sealed, growing, version := sd.distribution.PinOnlineSegments(partitions...)
			defer sd.distribution.Unpin(version)

			for _, entry := range sealed {
				entry := entry
				worker, err := sd.workerManager.GetWorker(ctx, entry.NodeID)
				if err != nil {
					log.Warn("failed to get worker",
						zap.Int64("nodeID", entry.NodeID),
						zap.Error(err),
					)
					// skip if node down
					// delete will be processed after loaded again
					continue
				}
				// forward to non level0 segment only
				segments := lo.Filter(entry.Segments, func(segmentEntry SegmentEntry, _ int) bool {
					return segmentEntry.Level != datapb.SegmentLevel_L0
				})

				eg.Go(func() error {
					offlineSegments.Upsert(sd.applyDeleteBatch(ctx, entry.NodeID, worker, group, segments, querypb.DataScope_Historical)...)
					return nil
				})
			}

			if len(growing) > 0 {
				worker, err := sd.workerManager.GetWorker(ctx, paramtable.GetNodeID())
				if err != nil {
					log.Error("failed to get worker(local)",
						zap.Int64("nodeID", paramtable.GetNodeID()),
						zap.Error(err),
					)
					// panic here, local worker shall not have error
					panic(err)
				}
				eg.Go(func() error {
					offlineSegments.Upsert(sd.applyDeleteBatch(ctx, paramtable.GetNodeID(), worker, group, growing, querypb.DataScope_Streaming)...)
					return nil
				})
			}
			return nil
		})
	}
	// not error return in apply delete
	_ = eg.Wait()
	forwardDeleteCost := time.Since(start)

	offlineSegIDs := offlineSegments.Collect()
	if len(offlineSegIDs) > 0 {
		log.Warn("failed to apply delete, mark segment offline", zap.Int64s("offlineSegments", offlineSegIDs))
		sd.markSegmentOffline(offlineSegIDs...)
	}

	metrics.QueryNodeForwardDeleteCost.WithLabelValues("ProcessDelete", fmt.Sprint(paramtable.GetNodeID())).Observe(float64(forwardDeleteCost.Milliseconds()))
}

// applyDeleteBatch handles delete record and apply them to corresponding workers in batch.
func (sd *shardDelegator) applyDeleteBatch(ctx context.Context,
	nodeID int64,
	worker cluster.Worker,
	data []*DeleteData,
	entries []SegmentEntry,
	scope querypb.DataScope,
) []int64 {
	offlineSegments := typeutil.NewConcurrentSet[int64]()
	log := sd.getLogger(ctx)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	pool := conc.NewPool[struct{}](runtime.GOMAXPROCS(0) * 4)
	defer pool.Release()

	var futures []*conc.Future[struct{}]
	for _, delData := range data {
		delData := delData
		segmentIDs := lo.Map(entries, func(entry SegmentEntry, _ int) int64 {
			return entry.SegmentID
		})
		future := pool.Submit(func() (struct{}, error) {
			log.Debug("delegator plan to applyDelete via worker")
			err := retry.Handle(ctx, func() (bool, error) {
				if sd.Stopped() {
					return false, merr.WrapErrChannelNotAvailable(sd.vchannelName, "channel is unsubscribing")
				}

				resp, err := worker.DeleteBatch(ctx, &querypb.DeleteBatchRequest{
					Base:         commonpbutil.NewMsgBase(commonpbutil.WithTargetID(nodeID)),
					CollectionId: sd.collectionID,
					PartitionId:  delData.PartitionID,
					VchannelName: sd.vchannelName,
					SegmentIds:   segmentIDs,
					PrimaryKeys:  storage.ParsePrimaryKeys2IDs(delData.PrimaryKeys),
					Timestamps:   delData.Timestamps,
					Scope:        scope,
				})
				if errors.Is(err, merr.ErrNodeNotFound) {
					log.Warn("try to delete data on non-exist node")
					// cancel other request
					cancel()
					return false, err
				}
				// grpc/network error
				if err != nil {
					return true, err
				}
				if len(resp.GetMissingIds()) > 0 {
					log.Warn("try to delete data of released segment", zap.Int64s("ids", resp.GetMissingIds()))
				}
				if len(resp.GetFailedIds()) > 0 {
					log.Warn("apply delete for segment failed, marking it offline")
					offlineSegments.Upsert(resp.GetFailedIds()...)
				}
				return false, nil
			}, retry.Attempts(10))

			return struct{}{}, err
		})
		futures = append(futures, future)
	}

	conc.AwaitAll(futures...)
	return offlineSegments.Collect()
}
