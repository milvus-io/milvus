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

package rootcoord

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type watchInfo struct {
	ts             Timestamp
	collectionID   UniqueID
	partitionID    UniqueID
	vChannels      []string
	startPositions []*commonpb.KeyDataPair
	schema         *schemapb.CollectionSchema
	dbProperties   []*commonpb.KeyValuePair
}

// Broker communicates with other components.
type Broker interface {
	ReleaseCollection(ctx context.Context, collectionID UniqueID) error
	ReleasePartitions(ctx context.Context, collectionID UniqueID, partitionIDs ...UniqueID) error
	SyncNewCreatedPartition(ctx context.Context, collectionID UniqueID, partitionID UniqueID) error
	GetQuerySegmentInfo(ctx context.Context, collectionID int64, segIDs []int64) (retResp *querypb.GetSegmentInfoResponse, retErr error)

	WatchChannels(ctx context.Context, info *watchInfo) error
	UnwatchChannels(ctx context.Context, info *watchInfo) error
	GetSegmentStates(context.Context, *datapb.GetSegmentStatesRequest) (*datapb.GetSegmentStatesResponse, error)
	GcConfirm(ctx context.Context, collectionID, partitionID UniqueID) bool

	DropCollectionIndex(ctx context.Context, collID UniqueID, partIDs []UniqueID) error
	// notify observer to clean their meta cache
	BroadcastAlteredCollection(ctx context.Context, req *milvuspb.AlterCollectionRequest) error
}

type ServerBroker struct {
	s *Core
}

func newServerBroker(s *Core) *ServerBroker {
	return &ServerBroker{s: s}
}

func (b *ServerBroker) ReleaseCollection(ctx context.Context, collectionID UniqueID) error {
	log.Ctx(ctx).Info("releasing collection", zap.Int64("collection", collectionID))

	resp, err := b.s.queryCoord.ReleaseCollection(ctx, &querypb.ReleaseCollectionRequest{
		Base:         commonpbutil.NewMsgBase(commonpbutil.WithMsgType(commonpb.MsgType_ReleaseCollection)),
		CollectionID: collectionID,
		NodeID:       b.s.session.ServerID,
	})
	if err != nil {
		return err
	}

	if resp.GetErrorCode() != commonpb.ErrorCode_Success {
		return fmt.Errorf("failed to release collection, code: %s, reason: %s", resp.GetErrorCode(), resp.GetReason())
	}

	log.Ctx(ctx).Info("done to release collection", zap.Int64("collection", collectionID))
	return nil
}

func (b *ServerBroker) ReleasePartitions(ctx context.Context, collectionID UniqueID, partitionIDs ...UniqueID) error {
	if len(partitionIDs) == 0 {
		return nil
	}
	log := log.Ctx(ctx).With(zap.Int64("collection", collectionID), zap.Int64s("partitionIDs", partitionIDs))
	log.Info("releasing partitions")
	resp, err := b.s.queryCoord.ReleasePartitions(ctx, &querypb.ReleasePartitionsRequest{
		Base:         commonpbutil.NewMsgBase(commonpbutil.WithMsgType(commonpb.MsgType_ReleasePartitions)),
		CollectionID: collectionID,
		PartitionIDs: partitionIDs,
	})
	if err != nil {
		return err
	}

	if resp.GetErrorCode() != commonpb.ErrorCode_Success {
		return fmt.Errorf("release partition failed, reason: %s", resp.GetReason())
	}

	log.Info("release partitions done")
	return nil
}

func (b *ServerBroker) SyncNewCreatedPartition(ctx context.Context, collectionID UniqueID, partitionID UniqueID) error {
	log := log.Ctx(ctx).With(zap.Int64("collection", collectionID), zap.Int64("partitionID", partitionID))
	log.Info("begin to sync new partition")
	resp, err := b.s.queryCoord.SyncNewCreatedPartition(ctx, &querypb.SyncNewCreatedPartitionRequest{
		Base:         commonpbutil.NewMsgBase(commonpbutil.WithMsgType(commonpb.MsgType_ReleasePartitions)),
		CollectionID: collectionID,
		PartitionID:  partitionID,
	})
	if err != nil {
		return err
	}

	if resp.GetErrorCode() != commonpb.ErrorCode_Success {
		return fmt.Errorf("sync new partition failed, reason: %s", resp.GetReason())
	}

	log.Info("sync new partition done")
	return nil
}

func (b *ServerBroker) GetQuerySegmentInfo(ctx context.Context, collectionID int64, segIDs []int64) (retResp *querypb.GetSegmentInfoResponse, retErr error) {
	resp, err := b.s.queryCoord.GetSegmentInfo(ctx, &querypb.GetSegmentInfoRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_GetSegmentState),
			commonpbutil.WithSourceID(b.s.session.ServerID),
		),
		CollectionID: collectionID,
		SegmentIDs:   segIDs,
	})
	return resp, err
}

func toKeyDataPairs(m map[string][]byte) []*commonpb.KeyDataPair {
	ret := make([]*commonpb.KeyDataPair, 0, len(m))
	for k, data := range m {
		ret = append(ret, &commonpb.KeyDataPair{
			Key:  k,
			Data: data,
		})
	}
	return ret
}

func (b *ServerBroker) WatchChannels(ctx context.Context, info *watchInfo) error {
	log.Ctx(ctx).Info("watching channels", zap.Uint64("ts", info.ts), zap.Int64("collection", info.collectionID), zap.Strings("vChannels", info.vChannels))

	resp, err := b.s.dataCoord.WatchChannels(ctx, &datapb.WatchChannelsRequest{
		CollectionID:    info.collectionID,
		ChannelNames:    info.vChannels,
		StartPositions:  info.startPositions,
		Schema:          info.schema,
		CreateTimestamp: info.ts,
		DbProperties:    info.dbProperties,
	})
	if err != nil {
		return err
	}

	if resp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		return fmt.Errorf("failed to watch channels, code: %s, reason: %s", resp.GetStatus().GetErrorCode(), resp.GetStatus().GetReason())
	}

	log.Ctx(ctx).Info("done to watch channels", zap.Uint64("ts", info.ts), zap.Int64("collection", info.collectionID), zap.Strings("vChannels", info.vChannels))
	return nil
}

func (b *ServerBroker) UnwatchChannels(ctx context.Context, info *watchInfo) error {
	// TODO: release flowgraph on datanodes.
	return nil
}

func (b *ServerBroker) GetSegmentStates(ctx context.Context, req *datapb.GetSegmentStatesRequest) (*datapb.GetSegmentStatesResponse, error) {
	return b.s.dataCoord.GetSegmentStates(ctx, req)
}

func (b *ServerBroker) DropCollectionIndex(ctx context.Context, collID UniqueID, partIDs []UniqueID) error {
	log.Ctx(ctx).Info("dropping collection index", zap.Int64("collection", collID), zap.Int64s("partitions", partIDs))

	rsp, err := b.s.dataCoord.DropIndex(ctx, &indexpb.DropIndexRequest{
		CollectionID: collID,
		PartitionIDs: partIDs,
		IndexName:    "",
		DropAll:      true,
	})
	if err != nil {
		return err
	}
	if rsp.ErrorCode != commonpb.ErrorCode_Success {
		return fmt.Errorf(rsp.Reason)
	}

	log.Ctx(ctx).Info("done to drop collection index", zap.Int64("collection", collID), zap.Int64s("partitions", partIDs))

	return nil
}

func (b *ServerBroker) GetSegmentIndexState(ctx context.Context, collID UniqueID, indexName string, segIDs []UniqueID) ([]*indexpb.SegmentIndexState, error) {
	resp, err := b.s.dataCoord.GetSegmentIndexState(ctx, &indexpb.GetSegmentIndexStateRequest{
		CollectionID: collID,
		IndexName:    indexName,
		SegmentIDs:   segIDs,
	})
	if err != nil {
		return nil, err
	}
	if resp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		return nil, merr.Error(resp.GetStatus())
	}

	return resp.GetStates(), nil
}

func (b *ServerBroker) BroadcastAlteredCollection(ctx context.Context, req *milvuspb.AlterCollectionRequest) error {
	log.Ctx(ctx).Info("broadcasting request to alter collection",
		zap.String("collectionName", req.GetCollectionName()),
		zap.Int64("collectionID", req.GetCollectionID()),
		zap.Any("props", req.GetProperties()),
		zap.Any("deleteKeys", req.GetDeleteKeys()))

	colMeta, err := b.s.meta.GetCollectionByID(ctx, req.GetDbName(), req.GetCollectionID(), typeutil.MaxTimestamp, false)
	if err != nil {
		return err
	}

	db, err := b.s.meta.GetDatabaseByName(ctx, req.GetDbName(), typeutil.MaxTimestamp)
	if err != nil {
		return err
	}

	partitionIDs := make([]int64, len(colMeta.Partitions))
	for _, p := range colMeta.Partitions {
		partitionIDs = append(partitionIDs, p.PartitionID)
	}
	dcReq := &datapb.AlterCollectionRequest{
		CollectionID: req.GetCollectionID(),
		Schema: &schemapb.CollectionSchema{
			Name:        colMeta.Name,
			Description: colMeta.Description,
			AutoID:      colMeta.AutoID,
			Fields:      model.MarshalFieldModels(colMeta.Fields),
			Functions:   model.MarshalFunctionModels(colMeta.Functions),
		},
		PartitionIDs:   partitionIDs,
		StartPositions: colMeta.StartPositions,
		Properties:     colMeta.Properties,
		DbID:           db.ID,
		VChannels:      colMeta.VirtualChannelNames,
	}

	resp, err := b.s.dataCoord.BroadcastAlteredCollection(ctx, dcReq)
	if err != nil {
		return err
	}

	if resp.ErrorCode != commonpb.ErrorCode_Success {
		return errors.New(resp.Reason)
	}
	log.Ctx(ctx).Info("done to broadcast request to alter collection", zap.String("collectionName", req.GetCollectionName()), zap.Int64("collectionID", req.GetCollectionID()), zap.Any("props", req.GetProperties()))
	return nil
}

func (b *ServerBroker) GcConfirm(ctx context.Context, collectionID, partitionID UniqueID) bool {
	log := log.Ctx(ctx).With(zap.Int64("collection", collectionID), zap.Int64("partition", partitionID))

	log.Info("confirming if gc is finished")

	req := &datapb.GcConfirmRequest{CollectionId: collectionID, PartitionId: partitionID}
	resp, err := b.s.dataCoord.GcConfirm(ctx, req)
	if err != nil {
		log.Warn("gc is not finished", zap.Error(err))
		return false
	}

	if resp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("gc is not finished", zap.String("code", resp.GetStatus().GetErrorCode().String()),
			zap.String("reason", resp.GetStatus().GetReason()))
		return false
	}

	log.Info("received gc_confirm response", zap.Bool("finished", resp.GetGcFinished()))
	return resp.GetGcFinished()
}
