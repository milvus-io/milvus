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
	"fmt"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/types"

	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/milvus-io/milvus/api/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/querypb"

	"github.com/milvus-io/milvus/internal/util/typeutil"
)

const (
	LoadCollectionTaskName    = "LoadCollectionTask"
	ReleaseCollectionTaskName = "ReleaseCollectionTask"
	LoadPartitionTaskName     = "LoadPartitionsTask"
	ReleasePartitionTaskName  = "ReleasePartitionsTask"
)

type loadCollectionTask struct {
	Condition
	*milvuspb.LoadCollectionRequest
	ctx        context.Context
	queryCoord types.QueryCoord
	result     *commonpb.Status

	collectionID UniqueID
}

func (lct *loadCollectionTask) TraceCtx() context.Context {
	return lct.ctx
}

func (lct *loadCollectionTask) ID() UniqueID {
	return lct.Base.MsgID
}

func (lct *loadCollectionTask) SetID(uid UniqueID) {
	lct.Base.MsgID = uid
}

func (lct *loadCollectionTask) Name() string {
	return LoadCollectionTaskName
}

func (lct *loadCollectionTask) Type() commonpb.MsgType {
	return lct.Base.MsgType
}

func (lct *loadCollectionTask) BeginTs() Timestamp {
	return lct.Base.Timestamp
}

func (lct *loadCollectionTask) EndTs() Timestamp {
	return lct.Base.Timestamp
}

func (lct *loadCollectionTask) SetTs(ts Timestamp) {
	lct.Base.Timestamp = ts
}

func (lct *loadCollectionTask) OnEnqueue() error {
	lct.Base = &commonpb.MsgBase{}
	return nil
}

func (lct *loadCollectionTask) PreExecute(ctx context.Context) error {
	log.Debug("loadCollectionTask PreExecute", zap.String("role", typeutil.ProxyRole), zap.Int64("msgID", lct.Base.MsgID))
	lct.Base.MsgType = commonpb.MsgType_LoadCollection
	lct.Base.SourceID = Params.ProxyCfg.GetNodeID()

	collName := lct.CollectionName

	if err := validateCollectionName(collName); err != nil {
		return err
	}

	// To compat with LoadCollcetion before Milvus@2.1
	if lct.ReplicaNumber == 0 {
		lct.ReplicaNumber = 1
	}

	return nil
}

func (lct *loadCollectionTask) Execute(ctx context.Context) (err error) {
	log.Debug("loadCollectionTask Execute", zap.String("role", typeutil.ProxyRole), zap.Int64("msgID", lct.Base.MsgID))
	collID, err := globalMetaCache.GetCollectionID(ctx, lct.CollectionName)
	if err != nil {
		return err
	}

	lct.collectionID = collID
	collSchema, err := globalMetaCache.GetCollectionSchema(ctx, lct.CollectionName)
	if err != nil {
		return err
	}
	request := &querypb.LoadCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_LoadCollection,
			MsgID:     lct.Base.MsgID,
			Timestamp: lct.Base.Timestamp,
			SourceID:  lct.Base.SourceID,
		},
		DbID:          0,
		CollectionID:  collID,
		Schema:        collSchema,
		ReplicaNumber: lct.ReplicaNumber,
	}
	log.Debug("send LoadCollectionRequest to query coordinator", zap.String("role", typeutil.ProxyRole),
		zap.Int64("msgID", request.Base.MsgID), zap.Int64("collectionID", request.CollectionID),
		zap.Any("schema", request.Schema))
	lct.result, err = lct.queryCoord.LoadCollection(ctx, request)
	if err != nil {
		return fmt.Errorf("call query coordinator LoadCollection: %s", err)
	}
	return nil
}

func (lct *loadCollectionTask) PostExecute(ctx context.Context) error {
	log.Debug("loadCollectionTask PostExecute", zap.String("role", typeutil.ProxyRole),
		zap.Int64("msgID", lct.Base.MsgID))
	return nil
}

type releaseCollectionTask struct {
	Condition
	*milvuspb.ReleaseCollectionRequest
	ctx        context.Context
	queryCoord types.QueryCoord
	result     *commonpb.Status
	chMgr      channelsMgr

	collectionID UniqueID
}

func (rct *releaseCollectionTask) TraceCtx() context.Context {
	return rct.ctx
}

func (rct *releaseCollectionTask) ID() UniqueID {
	return rct.Base.MsgID
}

func (rct *releaseCollectionTask) SetID(uid UniqueID) {
	rct.Base.MsgID = uid
}

func (rct *releaseCollectionTask) Name() string {
	return ReleaseCollectionTaskName
}

func (rct *releaseCollectionTask) Type() commonpb.MsgType {
	return rct.Base.MsgType
}

func (rct *releaseCollectionTask) BeginTs() Timestamp {
	return rct.Base.Timestamp
}

func (rct *releaseCollectionTask) EndTs() Timestamp {
	return rct.Base.Timestamp
}

func (rct *releaseCollectionTask) SetTs(ts Timestamp) {
	rct.Base.Timestamp = ts
}

func (rct *releaseCollectionTask) OnEnqueue() error {
	rct.Base = &commonpb.MsgBase{}
	return nil
}

func (rct *releaseCollectionTask) PreExecute(ctx context.Context) error {
	rct.Base.MsgType = commonpb.MsgType_ReleaseCollection
	rct.Base.SourceID = Params.ProxyCfg.GetNodeID()

	collName := rct.CollectionName

	if err := validateCollectionName(collName); err != nil {
		return err
	}

	return nil
}

func (rct *releaseCollectionTask) Execute(ctx context.Context) (err error) {
	collID, err := globalMetaCache.GetCollectionID(ctx, rct.CollectionName)
	if err != nil {
		return err
	}
	rct.collectionID = collID
	request := &querypb.ReleaseCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_ReleaseCollection,
			MsgID:     rct.Base.MsgID,
			Timestamp: rct.Base.Timestamp,
			SourceID:  rct.Base.SourceID,
		},
		DbID:         0,
		CollectionID: collID,
	}

	rct.result, err = rct.queryCoord.ReleaseCollection(ctx, request)

	globalMetaCache.RemoveCollection(ctx, rct.CollectionName)

	return err
}

func (rct *releaseCollectionTask) PostExecute(ctx context.Context) error {
	globalMetaCache.ClearShards(rct.CollectionName)
	return nil
}

type loadPartitionsTask struct {
	Condition
	*milvuspb.LoadPartitionsRequest
	ctx        context.Context
	queryCoord types.QueryCoord
	result     *commonpb.Status

	collectionID UniqueID
}

func (lpt *loadPartitionsTask) TraceCtx() context.Context {
	return lpt.ctx
}

func (lpt *loadPartitionsTask) ID() UniqueID {
	return lpt.Base.MsgID
}

func (lpt *loadPartitionsTask) SetID(uid UniqueID) {
	lpt.Base.MsgID = uid
}

func (lpt *loadPartitionsTask) Name() string {
	return LoadPartitionTaskName
}

func (lpt *loadPartitionsTask) Type() commonpb.MsgType {
	return lpt.Base.MsgType
}

func (lpt *loadPartitionsTask) BeginTs() Timestamp {
	return lpt.Base.Timestamp
}

func (lpt *loadPartitionsTask) EndTs() Timestamp {
	return lpt.Base.Timestamp
}

func (lpt *loadPartitionsTask) SetTs(ts Timestamp) {
	lpt.Base.Timestamp = ts
}

func (lpt *loadPartitionsTask) OnEnqueue() error {
	lpt.Base = &commonpb.MsgBase{}
	return nil
}

func (lpt *loadPartitionsTask) PreExecute(ctx context.Context) error {
	lpt.Base.MsgType = commonpb.MsgType_LoadPartitions
	lpt.Base.SourceID = Params.ProxyCfg.GetNodeID()

	collName := lpt.CollectionName

	if err := validateCollectionName(collName); err != nil {
		return err
	}

	return nil
}

func (lpt *loadPartitionsTask) Execute(ctx context.Context) error {
	var partitionIDs []int64
	collID, err := globalMetaCache.GetCollectionID(ctx, lpt.CollectionName)
	if err != nil {
		return err
	}
	lpt.collectionID = collID
	collSchema, err := globalMetaCache.GetCollectionSchema(ctx, lpt.CollectionName)
	if err != nil {
		return err
	}
	for _, partitionName := range lpt.PartitionNames {
		partitionID, err := globalMetaCache.GetPartitionID(ctx, lpt.CollectionName, partitionName)
		if err != nil {
			return err
		}
		partitionIDs = append(partitionIDs, partitionID)
	}
	request := &querypb.LoadPartitionsRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_LoadPartitions,
			MsgID:     lpt.Base.MsgID,
			Timestamp: lpt.Base.Timestamp,
			SourceID:  lpt.Base.SourceID,
		},
		DbID:          0,
		CollectionID:  collID,
		PartitionIDs:  partitionIDs,
		Schema:        collSchema,
		ReplicaNumber: lpt.ReplicaNumber,
	}
	lpt.result, err = lpt.queryCoord.LoadPartitions(ctx, request)
	return err
}

func (lpt *loadPartitionsTask) PostExecute(ctx context.Context) error {
	return nil
}

type releasePartitionsTask struct {
	Condition
	*milvuspb.ReleasePartitionsRequest
	ctx        context.Context
	queryCoord types.QueryCoord
	result     *commonpb.Status

	collectionID UniqueID
}

func (rpt *releasePartitionsTask) TraceCtx() context.Context {
	return rpt.ctx
}

func (rpt *releasePartitionsTask) ID() UniqueID {
	return rpt.Base.MsgID
}

func (rpt *releasePartitionsTask) SetID(uid UniqueID) {
	rpt.Base.MsgID = uid
}

func (rpt *releasePartitionsTask) Type() commonpb.MsgType {
	return rpt.Base.MsgType
}

func (rpt *releasePartitionsTask) Name() string {
	return ReleasePartitionTaskName
}

func (rpt *releasePartitionsTask) BeginTs() Timestamp {
	return rpt.Base.Timestamp
}

func (rpt *releasePartitionsTask) EndTs() Timestamp {
	return rpt.Base.Timestamp
}

func (rpt *releasePartitionsTask) SetTs(ts Timestamp) {
	rpt.Base.Timestamp = ts
}

func (rpt *releasePartitionsTask) OnEnqueue() error {
	rpt.Base = &commonpb.MsgBase{}
	return nil
}

func (rpt *releasePartitionsTask) PreExecute(ctx context.Context) error {
	rpt.Base.MsgType = commonpb.MsgType_ReleasePartitions
	rpt.Base.SourceID = Params.ProxyCfg.GetNodeID()

	collName := rpt.CollectionName

	if err := validateCollectionName(collName); err != nil {
		return err
	}

	return nil
}

func (rpt *releasePartitionsTask) Execute(ctx context.Context) (err error) {
	var partitionIDs []int64
	collID, err := globalMetaCache.GetCollectionID(ctx, rpt.CollectionName)
	if err != nil {
		return err
	}
	rpt.collectionID = collID
	for _, partitionName := range rpt.PartitionNames {
		partitionID, err := globalMetaCache.GetPartitionID(ctx, rpt.CollectionName, partitionName)
		if err != nil {
			return err
		}
		partitionIDs = append(partitionIDs, partitionID)
	}
	request := &querypb.ReleasePartitionsRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_ReleasePartitions,
			MsgID:     rpt.Base.MsgID,
			Timestamp: rpt.Base.Timestamp,
			SourceID:  rpt.Base.SourceID,
		},
		DbID:         0,
		CollectionID: collID,
		PartitionIDs: partitionIDs,
	}
	rpt.result, err = rpt.queryCoord.ReleasePartitions(ctx, request)
	return err
}

func (rpt *releasePartitionsTask) PostExecute(ctx context.Context) error {
	globalMetaCache.ClearShards(rpt.CollectionName)
	return nil
}
