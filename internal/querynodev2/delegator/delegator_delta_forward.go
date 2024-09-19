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

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querynodev2/cluster"
	"github.com/milvus-io/milvus/internal/querynodev2/pkoracle"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

const (
	L0ForwardPolicyDefault    = ``
	L0ForwardPolicyBF         = `FilterByBF`
	L0ForwardPolicyRemoteLoad = `RemoteLoad`
)

func (sd *shardDelegator) forwardL0Deletion(ctx context.Context,
	info *querypb.SegmentLoadInfo,
	req *querypb.LoadSegmentsRequest,
	candidate *pkoracle.BloomFilterSet,
	targetNodeID int64,
	worker cluster.Worker,
) error {
	switch policy := paramtable.Get().QueryNodeCfg.LevelZeroForwardPolicy.GetValue(); policy {
	case L0ForwardPolicyDefault, L0ForwardPolicyBF:
		return sd.forwardL0ByBF(ctx, info, candidate, targetNodeID, worker)
	case L0ForwardPolicyRemoteLoad:
		return sd.forwardL0RemoteLoad(ctx, info, req, targetNodeID, worker)
	default:
		return merr.WrapErrServiceInternal("Unknown l0 forward policy: %s", policy)
	}
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

	deletedPks, deletedTss := sd.GetLevel0Deletions(candidate.Partition(), candidate, info.GetL0SegmentIds())
	deleteData := &storage.DeleteData{}
	deleteData.AppendBatch(deletedPks, deletedTss)
	if deleteData.RowCount > 0 {
		log.Info("forward L0 delete to worker...",
			zap.Int64("deleteRowNum", deleteData.RowCount),
		)
		err := worker.Delete(ctx, &querypb.DeleteRequest{
			Base:         commonpbutil.NewMsgBase(commonpbutil.WithTargetID(targetNodeID)),
			CollectionId: info.GetCollectionID(),
			PartitionId:  info.GetPartitionID(),
			SegmentId:    info.GetSegmentID(),
			PrimaryKeys:  storage.ParsePrimaryKeys2IDs(deleteData.Pks),
			Timestamps:   deleteData.Tss,
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
	info.Deltalogs = sd.getLevel0Deltalogs(info.GetPartitionID(), info.GetL0SegmentIds())

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
	})
}

func (sd *shardDelegator) getLevel0Deltalogs(partitionID int64, targetLevelZeroIDs []int64) []*datapb.FieldBinlog {
	sd.level0Mut.Lock()
	defer sd.level0Mut.Unlock()

	// NOTE for compatibility concern:
	// system only allow new coord with old querynode
	// New Coord with old nodes, old node will not have target l0 segment id filtering here, OK
	// New Coord with new nodes, new node will always have correct l0 segment ids with target, OK
	// Old Coord with new node, target level zero ids will be nil, but shall not happen
	targetIDs := typeutil.NewSet(targetLevelZeroIDs...)

	level0Segments := sd.segmentManager.GetBy(
		segments.WithLevel(datapb.SegmentLevel_L0),
		segments.WithChannel(sd.vchannelName))

	level0Segments = lo.Filter(level0Segments, func(segment segments.Segment, _ int) bool {
		return targetIDs.Contain(segment.ID())
	})

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
