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

package proxy

import (
	"context"
	"errors"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/types"

	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/milvus-io/milvus/api/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
)

const (
	CreatePartitionTaskName = "CreatePartitionTask"
	DropPartitionTaskName   = "DropPartitionTask"
	HasPartitionTaskName    = "HasPartitionTask"
	ShowPartitionTaskName   = "ShowPartitionTask"
)

type createPartitionTask struct {
	Condition
	*milvuspb.CreatePartitionRequest
	ctx       context.Context
	rootCoord types.RootCoord
	result    *commonpb.Status
}

func (cpt *createPartitionTask) TraceCtx() context.Context {
	return cpt.ctx
}

func (cpt *createPartitionTask) ID() UniqueID {
	return cpt.Base.MsgID
}

func (cpt *createPartitionTask) SetID(uid UniqueID) {
	cpt.Base.MsgID = uid
}

func (cpt *createPartitionTask) Name() string {
	return CreatePartitionTaskName
}

func (cpt *createPartitionTask) Type() commonpb.MsgType {
	return cpt.Base.MsgType
}

func (cpt *createPartitionTask) BeginTs() Timestamp {
	return cpt.Base.Timestamp
}

func (cpt *createPartitionTask) EndTs() Timestamp {
	return cpt.Base.Timestamp
}

func (cpt *createPartitionTask) SetTs(ts Timestamp) {
	cpt.Base.Timestamp = ts
}

func (cpt *createPartitionTask) OnEnqueue() error {
	cpt.Base = &commonpb.MsgBase{}
	return nil
}

func (cpt *createPartitionTask) PreExecute(ctx context.Context) error {
	cpt.Base.MsgType = commonpb.MsgType_CreatePartition
	cpt.Base.SourceID = Params.ProxyCfg.GetNodeID()

	collName, partitionTag := cpt.CollectionName, cpt.PartitionName

	if err := validateCollectionName(collName); err != nil {
		return err
	}

	if err := validatePartitionTag(partitionTag, true); err != nil {
		return err
	}

	return nil
}

func (cpt *createPartitionTask) Execute(ctx context.Context) (err error) {
	cpt.result, err = cpt.rootCoord.CreatePartition(ctx, cpt.CreatePartitionRequest)
	if err != nil {
		return err
	}
	if cpt.result.ErrorCode != commonpb.ErrorCode_Success {
		return errors.New(cpt.result.Reason)
	}
	return err
}

func (cpt *createPartitionTask) PostExecute(ctx context.Context) error {
	return nil
}

type dropPartitionTask struct {
	Condition
	*milvuspb.DropPartitionRequest
	ctx       context.Context
	rootCoord types.RootCoord
	result    *commonpb.Status
}

func (dpt *dropPartitionTask) TraceCtx() context.Context {
	return dpt.ctx
}

func (dpt *dropPartitionTask) ID() UniqueID {
	return dpt.Base.MsgID
}

func (dpt *dropPartitionTask) SetID(uid UniqueID) {
	dpt.Base.MsgID = uid
}

func (dpt *dropPartitionTask) Name() string {
	return DropPartitionTaskName
}

func (dpt *dropPartitionTask) Type() commonpb.MsgType {
	return dpt.Base.MsgType
}

func (dpt *dropPartitionTask) BeginTs() Timestamp {
	return dpt.Base.Timestamp
}

func (dpt *dropPartitionTask) EndTs() Timestamp {
	return dpt.Base.Timestamp
}

func (dpt *dropPartitionTask) SetTs(ts Timestamp) {
	dpt.Base.Timestamp = ts
}

func (dpt *dropPartitionTask) OnEnqueue() error {
	dpt.Base = &commonpb.MsgBase{}
	return nil
}

func (dpt *dropPartitionTask) PreExecute(ctx context.Context) error {
	dpt.Base.MsgType = commonpb.MsgType_DropPartition
	dpt.Base.SourceID = Params.ProxyCfg.GetNodeID()

	collName, partitionTag := dpt.CollectionName, dpt.PartitionName

	if err := validateCollectionName(collName); err != nil {
		return err
	}

	if err := validatePartitionTag(partitionTag, true); err != nil {
		return err
	}

	return nil
}

func (dpt *dropPartitionTask) Execute(ctx context.Context) (err error) {
	dpt.result, err = dpt.rootCoord.DropPartition(ctx, dpt.DropPartitionRequest)
	if err != nil {
		return err
	}
	if dpt.result.ErrorCode != commonpb.ErrorCode_Success {
		return errors.New(dpt.result.Reason)
	}
	return err
}

func (dpt *dropPartitionTask) PostExecute(ctx context.Context) error {
	return nil
}

type hasPartitionTask struct {
	Condition
	*milvuspb.HasPartitionRequest
	ctx       context.Context
	rootCoord types.RootCoord
	result    *milvuspb.BoolResponse
}

func (hpt *hasPartitionTask) TraceCtx() context.Context {
	return hpt.ctx
}

func (hpt *hasPartitionTask) ID() UniqueID {
	return hpt.Base.MsgID
}

func (hpt *hasPartitionTask) SetID(uid UniqueID) {
	hpt.Base.MsgID = uid
}

func (hpt *hasPartitionTask) Name() string {
	return HasPartitionTaskName
}

func (hpt *hasPartitionTask) Type() commonpb.MsgType {
	return hpt.Base.MsgType
}

func (hpt *hasPartitionTask) BeginTs() Timestamp {
	return hpt.Base.Timestamp
}

func (hpt *hasPartitionTask) EndTs() Timestamp {
	return hpt.Base.Timestamp
}

func (hpt *hasPartitionTask) SetTs(ts Timestamp) {
	hpt.Base.Timestamp = ts
}

func (hpt *hasPartitionTask) OnEnqueue() error {
	hpt.Base = &commonpb.MsgBase{}
	return nil
}

func (hpt *hasPartitionTask) PreExecute(ctx context.Context) error {
	hpt.Base.MsgType = commonpb.MsgType_HasPartition
	hpt.Base.SourceID = Params.ProxyCfg.GetNodeID()

	collName, partitionTag := hpt.CollectionName, hpt.PartitionName

	if err := validateCollectionName(collName); err != nil {
		return err
	}

	if err := validatePartitionTag(partitionTag, true); err != nil {
		return err
	}
	return nil
}

func (hpt *hasPartitionTask) Execute(ctx context.Context) (err error) {
	hpt.result, err = hpt.rootCoord.HasPartition(ctx, hpt.HasPartitionRequest)
	if err != nil {
		return err
	}
	if hpt.result.Status.ErrorCode != commonpb.ErrorCode_Success {
		return errors.New(hpt.result.Status.Reason)
	}
	return err
}

func (hpt *hasPartitionTask) PostExecute(ctx context.Context) error {
	return nil
}

type showPartitionsTask struct {
	Condition
	*milvuspb.ShowPartitionsRequest
	ctx        context.Context
	rootCoord  types.RootCoord
	queryCoord types.QueryCoord
	result     *milvuspb.ShowPartitionsResponse
}

func (spt *showPartitionsTask) TraceCtx() context.Context {
	return spt.ctx
}

func (spt *showPartitionsTask) ID() UniqueID {
	return spt.Base.MsgID
}

func (spt *showPartitionsTask) SetID(uid UniqueID) {
	spt.Base.MsgID = uid
}

func (spt *showPartitionsTask) Name() string {
	return ShowPartitionTaskName
}

func (spt *showPartitionsTask) Type() commonpb.MsgType {
	return spt.Base.MsgType
}

func (spt *showPartitionsTask) BeginTs() Timestamp {
	return spt.Base.Timestamp
}

func (spt *showPartitionsTask) EndTs() Timestamp {
	return spt.Base.Timestamp
}

func (spt *showPartitionsTask) SetTs(ts Timestamp) {
	spt.Base.Timestamp = ts
}

func (spt *showPartitionsTask) OnEnqueue() error {
	spt.Base = &commonpb.MsgBase{}
	return nil
}

func (spt *showPartitionsTask) PreExecute(ctx context.Context) error {
	spt.Base.MsgType = commonpb.MsgType_ShowPartitions
	spt.Base.SourceID = Params.ProxyCfg.GetNodeID()

	if err := validateCollectionName(spt.CollectionName); err != nil {
		return err
	}

	if spt.GetType() == milvuspb.ShowType_InMemory {
		for _, partitionName := range spt.PartitionNames {
			if err := validatePartitionTag(partitionName, true); err != nil {
				return err
			}
		}
	}

	return nil
}

func (spt *showPartitionsTask) Execute(ctx context.Context) error {
	respFromRootCoord, err := spt.rootCoord.ShowPartitions(ctx, spt.ShowPartitionsRequest)
	if err != nil {
		return err
	}

	if respFromRootCoord == nil {
		return errors.New("failed to show partitions")
	}

	if respFromRootCoord.Status.ErrorCode != commonpb.ErrorCode_Success {
		return errors.New(respFromRootCoord.Status.Reason)
	}

	if spt.GetType() == milvuspb.ShowType_InMemory {
		collectionName := spt.CollectionName
		collectionID, err := globalMetaCache.GetCollectionID(ctx, collectionName)
		if err != nil {
			log.Debug("Failed to get collection id.", zap.Any("collectionName", collectionName),
				zap.Any("requestID", spt.Base.MsgID), zap.Any("requestType", "showPartitions"))
			return err
		}
		IDs2Names := make(map[UniqueID]string)
		for offset, partitionName := range respFromRootCoord.PartitionNames {
			partitionID := respFromRootCoord.PartitionIDs[offset]
			IDs2Names[partitionID] = partitionName
		}
		partitionIDs := make([]UniqueID, 0)
		for _, partitionName := range spt.PartitionNames {
			partitionID, err := globalMetaCache.GetPartitionID(ctx, collectionName, partitionName)
			if err != nil {
				log.Debug("Failed to get partition id.", zap.Any("partitionName", partitionName),
					zap.Any("requestID", spt.Base.MsgID), zap.Any("requestType", "showPartitions"))
				return err
			}
			partitionIDs = append(partitionIDs, partitionID)
			IDs2Names[partitionID] = partitionName
		}
		resp, err := spt.queryCoord.ShowPartitions(ctx, &querypb.ShowPartitionsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_ShowCollections,
				MsgID:     spt.Base.MsgID,
				Timestamp: spt.Base.Timestamp,
				SourceID:  spt.Base.SourceID,
			},
			CollectionID: collectionID,
			PartitionIDs: partitionIDs,
		})

		if err != nil {
			return err
		}

		if resp == nil {
			return errors.New("failed to show partitions")
		}

		if resp.Status.ErrorCode != commonpb.ErrorCode_Success {
			return errors.New(resp.Status.Reason)
		}

		spt.result = &milvuspb.ShowPartitionsResponse{
			Status:               resp.Status,
			PartitionNames:       make([]string, 0, len(resp.PartitionIDs)),
			PartitionIDs:         make([]int64, 0, len(resp.PartitionIDs)),
			CreatedTimestamps:    make([]uint64, 0, len(resp.PartitionIDs)),
			CreatedUtcTimestamps: make([]uint64, 0, len(resp.PartitionIDs)),
			InMemoryPercentages:  make([]int64, 0, len(resp.PartitionIDs)),
		}

		for offset, id := range resp.PartitionIDs {
			partitionName, ok := IDs2Names[id]
			if !ok {
				log.Debug("Failed to get partition id.", zap.Any("partitionName", partitionName),
					zap.Any("requestID", spt.Base.MsgID), zap.Any("requestType", "showPartitions"))
				return errors.New("failed to show partitions")
			}
			partitionInfo, err := globalMetaCache.GetPartitionInfo(ctx, collectionName, partitionName)
			if err != nil {
				log.Debug("Failed to get partition id.", zap.Any("partitionName", partitionName),
					zap.Any("requestID", spt.Base.MsgID), zap.Any("requestType", "showPartitions"))
				return err
			}
			spt.result.PartitionIDs = append(spt.result.PartitionIDs, id)
			spt.result.PartitionNames = append(spt.result.PartitionNames, partitionName)
			spt.result.CreatedTimestamps = append(spt.result.CreatedTimestamps, partitionInfo.createdTimestamp)
			spt.result.CreatedUtcTimestamps = append(spt.result.CreatedUtcTimestamps, partitionInfo.createdUtcTimestamp)
			spt.result.InMemoryPercentages = append(spt.result.InMemoryPercentages, resp.InMemoryPercentages[offset])
		}
	} else {
		spt.result = respFromRootCoord
	}

	return nil
}

func (spt *showPartitionsTask) PostExecute(ctx context.Context) error {
	return nil
}
