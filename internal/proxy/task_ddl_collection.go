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
	"fmt"

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/types"

	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/milvus-io/milvus/api/milvuspb"
	"github.com/milvus-io/milvus/api/schemapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
)

const (
	CreateCollectionTaskName = "CreateCollectionTask"
	DropCollectionTaskName   = "DropCollectionTask"
	ShowCollectionTaskName   = "ShowCollectionTask"

	HasCollectionTaskName      = "HasCollectionTask"
	DescribeCollectionTaskName = "DescribeCollectionTask"
)

type createCollectionTask struct {
	Condition
	*milvuspb.CreateCollectionRequest
	ctx       context.Context
	rootCoord types.RootCoord
	result    *commonpb.Status
	schema    *schemapb.CollectionSchema
}

func (cct *createCollectionTask) TraceCtx() context.Context {
	return cct.ctx
}

func (cct *createCollectionTask) ID() UniqueID {
	return cct.Base.MsgID
}

func (cct *createCollectionTask) SetID(uid UniqueID) {
	cct.Base.MsgID = uid
}

func (cct *createCollectionTask) Name() string {
	return CreateCollectionTaskName
}

func (cct *createCollectionTask) Type() commonpb.MsgType {
	return cct.Base.MsgType
}

func (cct *createCollectionTask) BeginTs() Timestamp {
	return cct.Base.Timestamp
}

func (cct *createCollectionTask) EndTs() Timestamp {
	return cct.Base.Timestamp
}

func (cct *createCollectionTask) SetTs(ts Timestamp) {
	cct.Base.Timestamp = ts
}

func (cct *createCollectionTask) OnEnqueue() error {
	cct.Base = &commonpb.MsgBase{}
	cct.Base.MsgType = commonpb.MsgType_CreateCollection
	cct.Base.SourceID = Params.ProxyCfg.GetNodeID()
	return nil
}

func (cct *createCollectionTask) PreExecute(ctx context.Context) error {
	cct.Base.MsgType = commonpb.MsgType_CreateCollection
	cct.Base.SourceID = Params.ProxyCfg.GetNodeID()

	cct.schema = &schemapb.CollectionSchema{}
	err := proto.Unmarshal(cct.Schema, cct.schema)
	if err != nil {
		return err
	}
	cct.schema.AutoID = false

	if cct.ShardsNum > Params.ProxyCfg.MaxShardNum {
		return fmt.Errorf("maximum shards's number should be limited to %d", Params.ProxyCfg.MaxShardNum)
	}

	if int64(len(cct.schema.Fields)) > Params.ProxyCfg.MaxFieldNum {
		return fmt.Errorf("maximum field's number should be limited to %d", Params.ProxyCfg.MaxFieldNum)
	}

	// validate collection name
	if err := validateCollectionName(cct.schema.Name); err != nil {
		return err
	}

	// validate whether field names duplicates
	if err := validateDuplicatedFieldName(cct.schema.Fields); err != nil {
		return err
	}

	// validate primary key definition
	if err := validatePrimaryKey(cct.schema); err != nil {
		return err
	}

	// validate auto id definition
	if err := ValidateFieldAutoID(cct.schema); err != nil {
		return err
	}

	// validate field type definition
	if err := validateFieldType(cct.schema); err != nil {
		return err
	}

	for _, field := range cct.schema.Fields {
		// validate field name
		if err := validateFieldName(field.Name); err != nil {
			return err
		}
		// validate vector field type parameters
		if field.DataType == schemapb.DataType_FloatVector || field.DataType == schemapb.DataType_BinaryVector {
			err = validateDimension(field)
			if err != nil {
				return err
			}
		}
		// valid max length per row parameters
		// if max_length not specified, return error
		if field.DataType == schemapb.DataType_VarChar {
			err = validateMaxLengthPerRow(cct.schema.Name, field)
			if err != nil {
				return err
			}
		}
	}

	if err := validateMultipleVectorFields(cct.schema); err != nil {
		return err
	}

	cct.CreateCollectionRequest.Schema, err = proto.Marshal(cct.schema)
	if err != nil {
		return err
	}

	return nil
}

func (cct *createCollectionTask) Execute(ctx context.Context) error {
	var err error
	cct.result, err = cct.rootCoord.CreateCollection(ctx, cct.CreateCollectionRequest)
	return err
}

func (cct *createCollectionTask) PostExecute(ctx context.Context) error {
	return nil
}

type dropCollectionTask struct {
	Condition
	*milvuspb.DropCollectionRequest
	ctx       context.Context
	rootCoord types.RootCoord
	result    *commonpb.Status
	chMgr     channelsMgr
	chTicker  channelsTimeTicker
}

func (dct *dropCollectionTask) TraceCtx() context.Context {
	return dct.ctx
}

func (dct *dropCollectionTask) ID() UniqueID {
	return dct.Base.MsgID
}

func (dct *dropCollectionTask) SetID(uid UniqueID) {
	dct.Base.MsgID = uid
}

func (dct *dropCollectionTask) Name() string {
	return DropCollectionTaskName
}

func (dct *dropCollectionTask) Type() commonpb.MsgType {
	return dct.Base.MsgType
}

func (dct *dropCollectionTask) BeginTs() Timestamp {
	return dct.Base.Timestamp
}

func (dct *dropCollectionTask) EndTs() Timestamp {
	return dct.Base.Timestamp
}

func (dct *dropCollectionTask) SetTs(ts Timestamp) {
	dct.Base.Timestamp = ts
}

func (dct *dropCollectionTask) OnEnqueue() error {
	dct.Base = &commonpb.MsgBase{}
	return nil
}

func (dct *dropCollectionTask) PreExecute(ctx context.Context) error {
	dct.Base.MsgType = commonpb.MsgType_DropCollection
	dct.Base.SourceID = Params.ProxyCfg.GetNodeID()

	if err := validateCollectionName(dct.CollectionName); err != nil {
		return err
	}
	return nil
}

func (dct *dropCollectionTask) Execute(ctx context.Context) error {
	collID, err := globalMetaCache.GetCollectionID(ctx, dct.CollectionName)
	if err != nil {
		// make dropping collection idempotent.
		dct.result = &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}
		return nil
	}

	dct.result, err = dct.rootCoord.DropCollection(ctx, dct.DropCollectionRequest)
	if err != nil {
		return err
	}

	_ = dct.chMgr.removeDMLStream(collID)
	globalMetaCache.RemoveCollection(ctx, dct.CollectionName)
	return nil
}

func (dct *dropCollectionTask) PostExecute(ctx context.Context) error {
	globalMetaCache.RemoveCollection(ctx, dct.CollectionName)
	return nil
}

type hasCollectionTask struct {
	Condition
	*milvuspb.HasCollectionRequest
	ctx       context.Context
	rootCoord types.RootCoord
	result    *milvuspb.BoolResponse
}

func (hct *hasCollectionTask) TraceCtx() context.Context {
	return hct.ctx
}

func (hct *hasCollectionTask) ID() UniqueID {
	return hct.Base.MsgID
}

func (hct *hasCollectionTask) SetID(uid UniqueID) {
	hct.Base.MsgID = uid
}

func (hct *hasCollectionTask) Name() string {
	return HasCollectionTaskName
}

func (hct *hasCollectionTask) Type() commonpb.MsgType {
	return hct.Base.MsgType
}

func (hct *hasCollectionTask) BeginTs() Timestamp {
	return hct.Base.Timestamp
}

func (hct *hasCollectionTask) EndTs() Timestamp {
	return hct.Base.Timestamp
}

func (hct *hasCollectionTask) SetTs(ts Timestamp) {
	hct.Base.Timestamp = ts
}

func (hct *hasCollectionTask) OnEnqueue() error {
	hct.Base = &commonpb.MsgBase{}
	return nil
}

func (hct *hasCollectionTask) PreExecute(ctx context.Context) error {
	hct.Base.MsgType = commonpb.MsgType_HasCollection
	hct.Base.SourceID = Params.ProxyCfg.GetNodeID()

	if err := validateCollectionName(hct.CollectionName); err != nil {
		return err
	}
	return nil
}

func (hct *hasCollectionTask) Execute(ctx context.Context) error {
	var err error
	hct.result, err = hct.rootCoord.HasCollection(ctx, hct.HasCollectionRequest)
	if err != nil {
		return err
	}
	if hct.result == nil {
		return errors.New("has collection resp is nil")
	}
	if hct.result.Status.ErrorCode != commonpb.ErrorCode_Success {
		return errors.New(hct.result.Status.Reason)
	}
	return nil
}

func (hct *hasCollectionTask) PostExecute(ctx context.Context) error {
	return nil
}

type describeCollectionTask struct {
	Condition
	*milvuspb.DescribeCollectionRequest
	ctx       context.Context
	rootCoord types.RootCoord
	result    *milvuspb.DescribeCollectionResponse
}

func (dct *describeCollectionTask) TraceCtx() context.Context {
	return dct.ctx
}

func (dct *describeCollectionTask) ID() UniqueID {
	return dct.Base.MsgID
}

func (dct *describeCollectionTask) SetID(uid UniqueID) {
	dct.Base.MsgID = uid
}

func (dct *describeCollectionTask) Name() string {
	return DescribeCollectionTaskName
}

func (dct *describeCollectionTask) Type() commonpb.MsgType {
	return dct.Base.MsgType
}

func (dct *describeCollectionTask) BeginTs() Timestamp {
	return dct.Base.Timestamp
}

func (dct *describeCollectionTask) EndTs() Timestamp {
	return dct.Base.Timestamp
}

func (dct *describeCollectionTask) SetTs(ts Timestamp) {
	dct.Base.Timestamp = ts
}

func (dct *describeCollectionTask) OnEnqueue() error {
	dct.Base = &commonpb.MsgBase{}
	return nil
}

func (dct *describeCollectionTask) PreExecute(ctx context.Context) error {
	dct.Base.MsgType = commonpb.MsgType_DescribeCollection
	dct.Base.SourceID = Params.ProxyCfg.GetNodeID()

	if dct.CollectionID != 0 && len(dct.CollectionName) == 0 {
		return nil
	}

	return validateCollectionName(dct.CollectionName)
}

func (dct *describeCollectionTask) Execute(ctx context.Context) error {
	var err error
	dct.result = &milvuspb.DescribeCollectionResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		Schema: &schemapb.CollectionSchema{
			Name:        "",
			Description: "",
			AutoID:      false,
			Fields:      make([]*schemapb.FieldSchema, 0),
		},
		CollectionID:         0,
		VirtualChannelNames:  nil,
		PhysicalChannelNames: nil,
		CollectionName:       dct.GetCollectionName(),
	}

	result, err := dct.rootCoord.DescribeCollection(ctx, dct.DescribeCollectionRequest)

	if err != nil {
		return err
	}

	if result.Status.ErrorCode != commonpb.ErrorCode_Success {
		dct.result.Status = result.Status
	} else {
		dct.result.Schema.Name = result.Schema.Name
		dct.result.Schema.Description = result.Schema.Description
		dct.result.Schema.AutoID = result.Schema.AutoID
		dct.result.CollectionID = result.CollectionID
		dct.result.VirtualChannelNames = result.VirtualChannelNames
		dct.result.PhysicalChannelNames = result.PhysicalChannelNames
		dct.result.CreatedTimestamp = result.CreatedTimestamp
		dct.result.CreatedUtcTimestamp = result.CreatedUtcTimestamp
		dct.result.ShardsNum = result.ShardsNum
		dct.result.ConsistencyLevel = result.ConsistencyLevel
		dct.result.Aliases = result.Aliases
		for _, field := range result.Schema.Fields {
			if field.FieldID >= common.StartOfUserFieldID {
				dct.result.Schema.Fields = append(dct.result.Schema.Fields, &schemapb.FieldSchema{
					FieldID:      field.FieldID,
					Name:         field.Name,
					IsPrimaryKey: field.IsPrimaryKey,
					AutoID:       field.AutoID,
					Description:  field.Description,
					DataType:     field.DataType,
					TypeParams:   field.TypeParams,
					IndexParams:  field.IndexParams,
				})
			}
		}
	}
	return nil
}

func (dct *describeCollectionTask) PostExecute(ctx context.Context) error {
	return nil
}

type showCollectionsTask struct {
	Condition
	*milvuspb.ShowCollectionsRequest
	ctx        context.Context
	rootCoord  types.RootCoord
	queryCoord types.QueryCoord
	result     *milvuspb.ShowCollectionsResponse
}

func (sct *showCollectionsTask) TraceCtx() context.Context {
	return sct.ctx
}

func (sct *showCollectionsTask) ID() UniqueID {
	return sct.Base.MsgID
}

func (sct *showCollectionsTask) SetID(uid UniqueID) {
	sct.Base.MsgID = uid
}

func (sct *showCollectionsTask) Name() string {
	return ShowCollectionTaskName
}

func (sct *showCollectionsTask) Type() commonpb.MsgType {
	return sct.Base.MsgType
}

func (sct *showCollectionsTask) BeginTs() Timestamp {
	return sct.Base.Timestamp
}

func (sct *showCollectionsTask) EndTs() Timestamp {
	return sct.Base.Timestamp
}

func (sct *showCollectionsTask) SetTs(ts Timestamp) {
	sct.Base.Timestamp = ts
}

func (sct *showCollectionsTask) OnEnqueue() error {
	sct.Base = &commonpb.MsgBase{}
	return nil
}

func (sct *showCollectionsTask) PreExecute(ctx context.Context) error {
	sct.Base.MsgType = commonpb.MsgType_ShowCollections
	sct.Base.SourceID = Params.ProxyCfg.GetNodeID()
	if sct.GetType() == milvuspb.ShowType_InMemory {
		for _, collectionName := range sct.CollectionNames {
			if err := validateCollectionName(collectionName); err != nil {
				return err
			}
		}
	}

	return nil
}

func (sct *showCollectionsTask) Execute(ctx context.Context) error {
	respFromRootCoord, err := sct.rootCoord.ShowCollections(ctx, sct.ShowCollectionsRequest)

	if err != nil {
		return err
	}

	if respFromRootCoord == nil {
		return errors.New("failed to show collections")
	}

	if respFromRootCoord.Status.ErrorCode != commonpb.ErrorCode_Success {
		return errors.New(respFromRootCoord.Status.Reason)
	}

	if sct.GetType() == milvuspb.ShowType_InMemory {
		IDs2Names := make(map[UniqueID]string)
		for offset, collectionName := range respFromRootCoord.CollectionNames {
			collectionID := respFromRootCoord.CollectionIds[offset]
			IDs2Names[collectionID] = collectionName
		}
		collectionIDs := make([]UniqueID, 0)
		for _, collectionName := range sct.CollectionNames {
			collectionID, err := globalMetaCache.GetCollectionID(ctx, collectionName)
			if err != nil {
				log.Debug("Failed to get collection id.", zap.Any("collectionName", collectionName),
					zap.Any("requestID", sct.Base.MsgID), zap.Any("requestType", "showCollections"))
				return err
			}
			collectionIDs = append(collectionIDs, collectionID)
			IDs2Names[collectionID] = collectionName
		}

		resp, err := sct.queryCoord.ShowCollections(ctx, &querypb.ShowCollectionsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_ShowCollections,
				MsgID:     sct.Base.MsgID,
				Timestamp: sct.Base.Timestamp,
				SourceID:  sct.Base.SourceID,
			},
			//DbID: sct.ShowCollectionsRequest.DbName,
			CollectionIDs: collectionIDs,
		})

		if err != nil {
			return err
		}

		if resp == nil {
			return errors.New("failed to show collections")
		}

		if resp.Status.ErrorCode != commonpb.ErrorCode_Success {
			// update collectionID to collection name, and return new error info to sdk
			newErrorReason := resp.Status.Reason
			for _, collectionID := range collectionIDs {
				newErrorReason = ReplaceID2Name(newErrorReason, collectionID, IDs2Names[collectionID])
			}
			return errors.New(newErrorReason)
		}

		sct.result = &milvuspb.ShowCollectionsResponse{
			Status:                resp.Status,
			CollectionNames:       make([]string, 0, len(resp.CollectionIDs)),
			CollectionIds:         make([]int64, 0, len(resp.CollectionIDs)),
			CreatedTimestamps:     make([]uint64, 0, len(resp.CollectionIDs)),
			CreatedUtcTimestamps:  make([]uint64, 0, len(resp.CollectionIDs)),
			InMemoryPercentages:   make([]int64, 0, len(resp.CollectionIDs)),
			QueryServiceAvailable: make([]bool, 0, len(resp.CollectionIDs)),
		}

		for offset, id := range resp.CollectionIDs {
			collectionName, ok := IDs2Names[id]
			if !ok {
				log.Debug("Failed to get collection info.", zap.Any("collectionName", collectionName),
					zap.Any("requestID", sct.Base.MsgID), zap.Any("requestType", "showCollections"))
				return errors.New("failed to show collections")
			}
			collectionInfo, err := globalMetaCache.GetCollectionInfo(ctx, collectionName)
			if err != nil {
				log.Debug("Failed to get collection info.", zap.Any("collectionName", collectionName),
					zap.Any("requestID", sct.Base.MsgID), zap.Any("requestType", "showCollections"))
				return err
			}
			sct.result.CollectionIds = append(sct.result.CollectionIds, id)
			sct.result.CollectionNames = append(sct.result.CollectionNames, collectionName)
			sct.result.CreatedTimestamps = append(sct.result.CreatedTimestamps, collectionInfo.createdTimestamp)
			sct.result.CreatedUtcTimestamps = append(sct.result.CreatedUtcTimestamps, collectionInfo.createdUtcTimestamp)
			sct.result.InMemoryPercentages = append(sct.result.InMemoryPercentages, resp.InMemoryPercentages[offset])
			sct.result.QueryServiceAvailable = append(sct.result.QueryServiceAvailable, resp.QueryServiceAvailable[offset])
		}
	} else {
		sct.result = respFromRootCoord
	}

	return nil
}

func (sct *showCollectionsTask) PostExecute(ctx context.Context) error {
	return nil
}
