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
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/golang/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/milvuserrors"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"github.com/milvus-io/milvus/pkg/util/uniquegenerator"
)

type collectionMeta struct {
	name                 string
	id                   typeutil.UniqueID
	schema               *schemapb.CollectionSchema
	shardsNum            int32
	virtualChannelNames  []string
	physicalChannelNames []string
	createdTimestamp     uint64
	createdUtcTimestamp  uint64
}

type partitionMeta struct {
	createdTimestamp    uint64
	createdUtcTimestamp uint64
}

type partitionMap struct {
	collID typeutil.UniqueID
	// naive inverted index
	partitionName2ID map[string]typeutil.UniqueID
	partitionID2Name map[typeutil.UniqueID]string
	partitionID2Meta map[typeutil.UniqueID]partitionMeta
}

type RootCoordMockOption func(mock *RootCoordMock)

type describeCollectionFuncType func(ctx context.Context, request *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error)

type showPartitionsFuncType func(ctx context.Context, request *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error)

type RootCoordMock struct {
	nodeID  typeutil.UniqueID
	address string

	state atomic.Value // internal.StateCode

	statisticsChannel string
	timeTickChannel   string

	// naive inverted index
	collName2ID  map[string]typeutil.UniqueID
	collID2Meta  map[typeutil.UniqueID]collectionMeta
	collAlias2ID map[string]typeutil.UniqueID
	collMtx      sync.RWMutex

	// TODO(dragondriver): need default partition?
	collID2Partitions map[typeutil.UniqueID]partitionMap
	partitionMtx      sync.RWMutex

	describeCollectionFunc describeCollectionFuncType
	showPartitionsFunc     showPartitionsFuncType
	showConfigurationsFunc showConfigurationsFuncType
	getMetricsFunc         getMetricsFuncType

	// TODO(dragondriver): index-related

	// TODO(dragondriver): segment-related

	// TODO(dragondriver): TimeTick-related

	lastTs          typeutil.Timestamp
	lastTsMtx       sync.Mutex
	checkHealthFunc func(ctx context.Context, req *milvuspb.CheckHealthRequest) (*milvuspb.CheckHealthResponse, error)
}

func (coord *RootCoordMock) CreateAlias(ctx context.Context, req *milvuspb.CreateAliasRequest) (*commonpb.Status, error) {
	code := coord.state.Load().(commonpb.StateCode)
	if code != commonpb.StateCode_Healthy {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    fmt.Sprintf("state code = %s", commonpb.StateCode_name[int32(code)]),
		}, nil
	}
	coord.collMtx.Lock()
	defer coord.collMtx.Unlock()

	_, exist := coord.collAlias2ID[req.Alias]
	if exist {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    fmt.Sprintf("duplicate collection alias, alias = %s", req.Alias),
		}, nil
	}

	collID, exist := coord.collName2ID[req.CollectionName]
	if !exist {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    fmt.Sprintf("aliased collection name does not exist, name = %s", req.CollectionName),
		}, nil
	}

	coord.collAlias2ID[req.Alias] = collID
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}, nil
}

func (coord *RootCoordMock) DropAlias(ctx context.Context, req *milvuspb.DropAliasRequest) (*commonpb.Status, error) {
	code := coord.state.Load().(commonpb.StateCode)
	if code != commonpb.StateCode_Healthy {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    fmt.Sprintf("state code = %s", commonpb.StateCode_name[int32(code)]),
		}, nil
	}
	coord.collMtx.Lock()
	defer coord.collMtx.Unlock()

	_, exist := coord.collAlias2ID[req.Alias]
	if !exist {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    fmt.Sprintf("alias does not exist, alias = %s", req.Alias),
		}, nil
	}

	delete(coord.collAlias2ID, req.Alias)
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}, nil
}

func (coord *RootCoordMock) AlterAlias(ctx context.Context, req *milvuspb.AlterAliasRequest) (*commonpb.Status, error) {
	code := coord.state.Load().(commonpb.StateCode)
	if code != commonpb.StateCode_Healthy {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    fmt.Sprintf("state code = %s", commonpb.StateCode_name[int32(code)]),
		}, nil
	}
	coord.collMtx.Lock()
	defer coord.collMtx.Unlock()

	_, exist := coord.collAlias2ID[req.Alias]
	if !exist {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_CollectionNotExists,
			Reason:    fmt.Sprintf("alias does not exist, alias = %s", req.Alias),
		}, nil
	}
	collID, exist := coord.collName2ID[req.CollectionName]
	if !exist {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_CollectionNotExists,
			Reason:    fmt.Sprintf("aliased collection name does not exist, name = %s", req.CollectionName),
		}, nil
	}
	coord.collAlias2ID[req.Alias] = collID
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}, nil
}

func (coord *RootCoordMock) updateState(state commonpb.StateCode) {
	coord.state.Store(state)
}

func (coord *RootCoordMock) getState() commonpb.StateCode {
	return coord.state.Load().(commonpb.StateCode)
}

func (coord *RootCoordMock) healthy() bool {
	return coord.getState() == commonpb.StateCode_Healthy
}

func (coord *RootCoordMock) Init() error {
	coord.updateState(commonpb.StateCode_Initializing)
	return nil
}

func (coord *RootCoordMock) Start() error {
	defer coord.updateState(commonpb.StateCode_Healthy)

	return nil
}

func (coord *RootCoordMock) Stop() error {
	defer coord.updateState(commonpb.StateCode_Abnormal)

	return nil
}

func (coord *RootCoordMock) GetComponentStates(ctx context.Context) (*milvuspb.ComponentStates, error) {
	return &milvuspb.ComponentStates{
		State: &milvuspb.ComponentInfo{
			NodeID:    coord.nodeID,
			Role:      typeutil.RootCoordRole,
			StateCode: coord.getState(),
			ExtraInfo: nil,
		},
		SubcomponentStates: nil,
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
	}, nil
}

func (coord *RootCoordMock) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	code := coord.state.Load().(commonpb.StateCode)
	if code != commonpb.StateCode_Healthy {
		return &milvuspb.StringResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    fmt.Sprintf("state code = %s", commonpb.StateCode_name[int32(code)]),
			},
		}, nil
	}
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Value: coord.statisticsChannel,
	}, nil
}

func (coord *RootCoordMock) Register() error {
	return nil
}

func (coord *RootCoordMock) GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	code := coord.state.Load().(commonpb.StateCode)
	if code != commonpb.StateCode_Healthy {
		return &milvuspb.StringResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    fmt.Sprintf("state code = %s", commonpb.StateCode_name[int32(code)]),
			},
		}, nil
	}
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Value: coord.timeTickChannel,
	}, nil
}

func (coord *RootCoordMock) CreateCollection(ctx context.Context, req *milvuspb.CreateCollectionRequest) (*commonpb.Status, error) {
	code := coord.state.Load().(commonpb.StateCode)
	if code != commonpb.StateCode_Healthy {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    fmt.Sprintf("state code = %s", commonpb.StateCode_name[int32(code)]),
		}, nil
	}
	coord.collMtx.Lock()
	defer coord.collMtx.Unlock()

	_, exist := coord.collName2ID[req.CollectionName]
	if exist {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    milvuserrors.MsgCollectionAlreadyExist(req.CollectionName),
		}, nil
	}

	var schema schemapb.CollectionSchema
	err := proto.Unmarshal(req.Schema, &schema)
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    fmt.Sprintf("failed to parse schema, error: %v", err),
		}, nil
	}
	for i := range schema.Fields {
		schema.Fields[i].FieldID = int64(common.StartOfUserFieldID + i)
	}
	if schema.EnableDynamicField {
		schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
			FieldID:     int64(common.StartOfUserFieldID + len(schema.Fields)),
			Name:        common.MetaFieldName,
			Description: "$meta",
			DataType:    schemapb.DataType_JSON,
			IsDynamic:   true,
		})
	}

	collID := typeutil.UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	coord.collName2ID[req.CollectionName] = collID

	var shardsNum int32
	if req.ShardsNum <= 0 {
		shardsNum = common.DefaultShardsNum
	} else {
		shardsNum = req.ShardsNum
	}

	virtualChannelNames := make([]string, 0, shardsNum)
	physicalChannelNames := make([]string, 0, shardsNum)
	for i := 0; i < int(shardsNum); i++ {
		virtualChannelNames = append(virtualChannelNames, funcutil.GenRandomStr())
		physicalChannelNames = append(physicalChannelNames, funcutil.GenRandomStr())
	}

	ts := uint64(time.Now().Nanosecond())

	coord.collID2Meta[collID] = collectionMeta{
		name:                 req.CollectionName,
		id:                   collID,
		schema:               &schema,
		shardsNum:            shardsNum,
		virtualChannelNames:  virtualChannelNames,
		physicalChannelNames: physicalChannelNames,
		createdTimestamp:     ts,
		createdUtcTimestamp:  ts,
	}

	coord.partitionMtx.Lock()
	defer coord.partitionMtx.Unlock()

	coord.collID2Partitions[collID] = partitionMap{
		collID:           collID,
		partitionName2ID: make(map[string]typeutil.UniqueID),
		partitionID2Name: make(map[typeutil.UniqueID]string),
		partitionID2Meta: make(map[typeutil.UniqueID]partitionMeta),
	}

	idGenerator := uniquegenerator.GetUniqueIntGeneratorIns()
	defaultPartitionName := Params.CommonCfg.DefaultPartitionName.GetValue()
	_, err = typeutil.GetPartitionKeyFieldSchema(&schema)
	if err == nil {
		partitionNums := req.GetNumPartitions()
		for i := int64(0); i < partitionNums; i++ {
			partitionName := fmt.Sprintf("%s_%d", defaultPartitionName, i)
			id := UniqueID(idGenerator.GetInt())
			coord.collID2Partitions[collID].partitionName2ID[partitionName] = id
			coord.collID2Partitions[collID].partitionID2Name[id] = partitionName
			coord.collID2Partitions[collID].partitionID2Meta[id] = partitionMeta{}
		}
	} else {
		id := UniqueID(idGenerator.GetInt())
		coord.collID2Partitions[collID].partitionName2ID[defaultPartitionName] = id
		coord.collID2Partitions[collID].partitionID2Name[id] = defaultPartitionName
		coord.collID2Partitions[collID].partitionID2Meta[id] = partitionMeta{}
	}

	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}, nil
}

func (coord *RootCoordMock) DropCollection(ctx context.Context, req *milvuspb.DropCollectionRequest) (*commonpb.Status, error) {
	code := coord.state.Load().(commonpb.StateCode)
	if code != commonpb.StateCode_Healthy {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    fmt.Sprintf("state code = %s", commonpb.StateCode_name[int32(code)]),
		}, nil
	}
	coord.collMtx.Lock()
	defer coord.collMtx.Unlock()

	collID, exist := coord.collName2ID[req.CollectionName]
	if !exist {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_CollectionNotExists,
			Reason:    milvuserrors.MsgCollectionNotExist(req.CollectionName),
		}, nil
	}

	delete(coord.collName2ID, req.CollectionName)

	delete(coord.collID2Meta, collID)

	coord.partitionMtx.Lock()
	defer coord.partitionMtx.Unlock()

	delete(coord.collID2Partitions, collID)

	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}, nil
}

func (coord *RootCoordMock) HasCollection(ctx context.Context, req *milvuspb.HasCollectionRequest) (*milvuspb.BoolResponse, error) {
	code := coord.state.Load().(commonpb.StateCode)
	if code != commonpb.StateCode_Healthy {
		return &milvuspb.BoolResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    fmt.Sprintf("state code = %s", commonpb.StateCode_name[int32(code)]),
			},
			Value: false,
		}, nil
	}
	coord.collMtx.RLock()
	defer coord.collMtx.RUnlock()

	_, exist := coord.collName2ID[req.CollectionName]

	return &milvuspb.BoolResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Value: exist,
	}, nil
}

func (coord *RootCoordMock) SetDescribeCollectionFunc(f describeCollectionFuncType) {
	coord.describeCollectionFunc = f
}

func (coord *RootCoordMock) ResetDescribeCollectionFunc() {
	coord.describeCollectionFunc = nil
}

func (coord *RootCoordMock) DescribeCollection(ctx context.Context, req *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	code := coord.state.Load().(commonpb.StateCode)
	if code != commonpb.StateCode_Healthy {
		return &milvuspb.DescribeCollectionResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    fmt.Sprintf("state code = %s", commonpb.StateCode_name[int32(code)]),
			},
			Schema:       nil,
			CollectionID: 0,
		}, nil
	}

	if coord.describeCollectionFunc != nil {
		return coord.describeCollectionFunc(ctx, req)
	}

	coord.collMtx.RLock()
	defer coord.collMtx.RUnlock()

	var collID UniqueID
	usingID := false
	if req.CollectionName == "" {
		usingID = true
	}

	collID, exist := coord.collName2ID[req.CollectionName]
	if !exist && !usingID {
		return &milvuspb.DescribeCollectionResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_CollectionNotExists,
				Reason:    milvuserrors.MsgCollectionNotExist(req.CollectionName),
			},
		}, nil
	}

	if usingID {
		collID = req.CollectionID
	}

	meta := coord.collID2Meta[collID]
	if meta.shardsNum == 0 {
		meta.shardsNum = int32(len(meta.virtualChannelNames))
	}

	return &milvuspb.DescribeCollectionResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Schema:               meta.schema,
		CollectionID:         collID,
		ShardsNum:            meta.shardsNum,
		VirtualChannelNames:  meta.virtualChannelNames,
		PhysicalChannelNames: meta.physicalChannelNames,
		CreatedTimestamp:     meta.createdUtcTimestamp,
		CreatedUtcTimestamp:  meta.createdUtcTimestamp,
	}, nil
}

func (coord *RootCoordMock) DescribeCollectionInternal(ctx context.Context, req *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	return coord.DescribeCollection(ctx, req)
}

func (coord *RootCoordMock) ShowCollections(ctx context.Context, req *milvuspb.ShowCollectionsRequest) (*milvuspb.ShowCollectionsResponse, error) {
	code := coord.state.Load().(commonpb.StateCode)
	if code != commonpb.StateCode_Healthy {
		return &milvuspb.ShowCollectionsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    fmt.Sprintf("state code = %s", commonpb.StateCode_name[int32(code)]),
			},
			CollectionNames: nil,
		}, nil
	}

	coord.collMtx.RLock()
	defer coord.collMtx.RUnlock()

	names := make([]string, 0, len(coord.collName2ID))
	ids := make([]int64, 0, len(coord.collName2ID))
	createdTimestamps := make([]uint64, 0, len(coord.collName2ID))
	createdUtcTimestamps := make([]uint64, 0, len(coord.collName2ID))

	for name, id := range coord.collName2ID {
		names = append(names, name)
		ids = append(ids, id)
		meta := coord.collID2Meta[id]
		createdTimestamps = append(createdTimestamps, meta.createdTimestamp)
		createdUtcTimestamps = append(createdUtcTimestamps, meta.createdUtcTimestamp)
	}

	return &milvuspb.ShowCollectionsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		CollectionNames:      names,
		CollectionIds:        ids,
		CreatedTimestamps:    createdTimestamps,
		CreatedUtcTimestamps: createdUtcTimestamps,
		InMemoryPercentages:  nil,
	}, nil
}

func (coord *RootCoordMock) CreatePartition(ctx context.Context, req *milvuspb.CreatePartitionRequest) (*commonpb.Status, error) {
	code := coord.state.Load().(commonpb.StateCode)
	if code != commonpb.StateCode_Healthy {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    fmt.Sprintf("state code = %s", commonpb.StateCode_name[int32(code)]),
		}, nil
	}
	coord.collMtx.RLock()
	defer coord.collMtx.RUnlock()

	collID, exist := coord.collName2ID[req.CollectionName]
	if !exist {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_CollectionNotExists,
			Reason:    milvuserrors.MsgCollectionNotExist(req.CollectionName),
		}, nil
	}

	coord.partitionMtx.Lock()
	defer coord.partitionMtx.Unlock()

	_, partitionExist := coord.collID2Partitions[collID].partitionName2ID[req.PartitionName]
	if partitionExist {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    milvuserrors.MsgPartitionAlreadyExist(req.PartitionName),
		}, nil
	}

	ts := uint64(time.Now().Nanosecond())

	partitionID := typeutil.UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	coord.collID2Partitions[collID].partitionName2ID[req.PartitionName] = partitionID
	coord.collID2Partitions[collID].partitionID2Name[partitionID] = req.PartitionName
	coord.collID2Partitions[collID].partitionID2Meta[partitionID] = partitionMeta{
		createdTimestamp:    ts,
		createdUtcTimestamp: ts,
	}

	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}, nil
}

func (coord *RootCoordMock) DropPartition(ctx context.Context, req *milvuspb.DropPartitionRequest) (*commonpb.Status, error) {
	code := coord.state.Load().(commonpb.StateCode)
	if code != commonpb.StateCode_Healthy {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    fmt.Sprintf("state code = %s", commonpb.StateCode_name[int32(code)]),
		}, nil
	}
	coord.collMtx.RLock()
	defer coord.collMtx.RUnlock()

	collID, exist := coord.collName2ID[req.CollectionName]
	if !exist {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_CollectionNotExists,
			Reason:    milvuserrors.MsgCollectionNotExist(req.CollectionName),
		}, nil
	}

	coord.partitionMtx.Lock()
	defer coord.partitionMtx.Unlock()

	partitionID, partitionExist := coord.collID2Partitions[collID].partitionName2ID[req.PartitionName]
	if !partitionExist {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    milvuserrors.MsgPartitionNotExist(req.PartitionName),
		}, nil
	}

	delete(coord.collID2Partitions[collID].partitionName2ID, req.PartitionName)
	delete(coord.collID2Partitions[collID].partitionID2Name, partitionID)

	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}, nil
}

func (coord *RootCoordMock) HasPartition(ctx context.Context, req *milvuspb.HasPartitionRequest) (*milvuspb.BoolResponse, error) {
	code := coord.state.Load().(commonpb.StateCode)
	if code != commonpb.StateCode_Healthy {
		return &milvuspb.BoolResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    fmt.Sprintf("state code = %s", commonpb.StateCode_name[int32(code)]),
			},
			Value: false,
		}, nil
	}
	coord.collMtx.RLock()
	defer coord.collMtx.RUnlock()

	collID, exist := coord.collName2ID[req.CollectionName]
	if !exist {
		return &milvuspb.BoolResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_CollectionNotExists,
				Reason:    milvuserrors.MsgCollectionNotExist(req.CollectionName),
			},
			Value: false,
		}, nil
	}

	coord.partitionMtx.RLock()
	defer coord.partitionMtx.RUnlock()

	_, partitionExist := coord.collID2Partitions[collID].partitionName2ID[req.PartitionName]
	return &milvuspb.BoolResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Value: partitionExist,
	}, nil
}

func (coord *RootCoordMock) ShowPartitions(ctx context.Context, req *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error) {
	code := coord.state.Load().(commonpb.StateCode)
	if code != commonpb.StateCode_Healthy {
		return &milvuspb.ShowPartitionsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    fmt.Sprintf("rootcoord is not healthy, state code = %s", commonpb.StateCode_name[int32(code)]),
			},
			PartitionNames: nil,
			PartitionIDs:   nil,
		}, nil
	}

	if coord.showPartitionsFunc != nil {
		return coord.showPartitionsFunc(ctx, req)
	}

	coord.collMtx.RLock()
	defer coord.collMtx.RUnlock()

	collID, exist := coord.collName2ID[req.CollectionName]
	if !exist {
		return &milvuspb.ShowPartitionsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_CollectionNotExists,
				Reason:    milvuserrors.MsgCollectionNotExist(req.CollectionName),
			},
		}, nil
	}

	coord.partitionMtx.RLock()
	defer coord.partitionMtx.RUnlock()

	l := len(coord.collID2Partitions[collID].partitionName2ID)

	names := make([]string, 0, l)
	ids := make([]int64, 0, l)
	createdTimestamps := make([]uint64, 0, l)
	createdUtcTimestamps := make([]uint64, 0, l)

	for name, id := range coord.collID2Partitions[collID].partitionName2ID {
		names = append(names, name)
		ids = append(ids, id)
		meta := coord.collID2Partitions[collID].partitionID2Meta[id]
		createdTimestamps = append(createdTimestamps, meta.createdTimestamp)
		createdUtcTimestamps = append(createdUtcTimestamps, meta.createdUtcTimestamp)
	}

	return &milvuspb.ShowPartitionsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		PartitionNames:       names,
		PartitionIDs:         ids,
		CreatedTimestamps:    createdTimestamps,
		CreatedUtcTimestamps: createdUtcTimestamps,
		InMemoryPercentages:  nil,
	}, nil
}

func (coord *RootCoordMock) ShowPartitionsInternal(ctx context.Context, req *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error) {
	return coord.ShowPartitions(ctx, req)
}

//func (coord *RootCoordMock) CreateIndex(ctx context.Context, req *milvuspb.CreateIndexRequest) (*commonpb.Status, error) {
//	code := coord.state.Load().(commonpb.StateCode)
//	if code != commonpb.StateCode_Healthy {
//		return &commonpb.Status{
//			ErrorCode: commonpb.ErrorCode_UnexpectedError,
//			Reason:    fmt.Sprintf("state code = %s", commonpb.StateCode_name[int32(code)]),
//		}, nil
//	}
//	return &commonpb.Status{
//		ErrorCode: commonpb.ErrorCode_Success,
//		Reason:    "",
//	}, nil
//}

//func (coord *RootCoordMock) DescribeIndex(ctx context.Context, req *milvuspb.DescribeIndexRequest) (*milvuspb.DescribeIndexResponse, error) {
//	code := coord.state.Load().(commonpb.StateCode)
//	if code != commonpb.StateCode_Healthy {
//		return &milvuspb.DescribeIndexResponse{
//			Status: &commonpb.Status{
//				ErrorCode: commonpb.ErrorCode_UnexpectedError,
//				Reason:    fmt.Sprintf("state code = %s", commonpb.StateCode_name[int32(code)]),
//			},
//			IndexDescriptions: nil,
//		}, nil
//	}
//	return &milvuspb.DescribeIndexResponse{
//		Status: &commonpb.Status{
//			ErrorCode: commonpb.ErrorCode_Success,
//			Reason:    "",
//		},
//		IndexDescriptions: nil,
//	}, nil
//}

//func (coord *RootCoordMock) GetIndexState(ctx context.Context, req *milvuspb.GetIndexStateRequest) (*indexpb.GetIndexStatesResponse, error) {
//	code := coord.state.Load().(commonpb.StateCode)
//	if code != commonpb.StateCode_Healthy {
//		return &indexpb.GetIndexStatesResponse{
//			Status: &commonpb.Status{
//				ErrorCode: commonpb.ErrorCode_UnexpectedError,
//				Reason:    fmt.Sprintf("state code = %s", commonpb.StateCode_name[int32(code)]),
//			},
//		}, nil
//	}
//	return &indexpb.GetIndexStatesResponse{
//		Status: &commonpb.Status{
//			ErrorCode: commonpb.ErrorCode_Success,
//			Reason:    "",
//		},
//	}, nil
//}

//func (coord *RootCoordMock) DropIndex(ctx context.Context, req *milvuspb.DropIndexRequest) (*commonpb.Status, error) {
//	code := coord.state.Load().(commonpb.StateCode)
//	if code != commonpb.StateCode_Healthy {
//		return &commonpb.Status{
//			ErrorCode: commonpb.ErrorCode_UnexpectedError,
//			Reason:    fmt.Sprintf("state code = %s", commonpb.StateCode_name[int32(code)]),
//		}, nil
//	}
//	return &commonpb.Status{
//		ErrorCode: commonpb.ErrorCode_Success,
//		Reason:    "",
//	}, nil
//}

func (coord *RootCoordMock) AllocTimestamp(ctx context.Context, req *rootcoordpb.AllocTimestampRequest) (*rootcoordpb.AllocTimestampResponse, error) {
	code := coord.state.Load().(commonpb.StateCode)
	if code != commonpb.StateCode_Healthy {
		return &rootcoordpb.AllocTimestampResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    fmt.Sprintf("state code = %s", commonpb.StateCode_name[int32(code)]),
			},
			Timestamp: 0,
			Count:     0,
		}, nil
	}
	coord.lastTsMtx.Lock()
	defer coord.lastTsMtx.Unlock()

	ts := uint64(time.Now().UnixNano())
	if ts < coord.lastTs+typeutil.Timestamp(req.Count) {
		ts = coord.lastTs + typeutil.Timestamp(req.Count)
	}

	coord.lastTs = ts
	return &rootcoordpb.AllocTimestampResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Timestamp: ts,
		Count:     req.Count,
	}, nil
}

func (coord *RootCoordMock) AllocID(ctx context.Context, req *rootcoordpb.AllocIDRequest) (*rootcoordpb.AllocIDResponse, error) {
	code := coord.state.Load().(commonpb.StateCode)
	if code != commonpb.StateCode_Healthy {
		return &rootcoordpb.AllocIDResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    fmt.Sprintf("state code = %s", commonpb.StateCode_name[int32(code)]),
			},
			ID:    0,
			Count: 0,
		}, nil
	}
	begin, _ := uniquegenerator.GetUniqueIntGeneratorIns().GetInts(int(req.Count))
	return &rootcoordpb.AllocIDResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		ID:    int64(begin),
		Count: req.Count,
	}, nil
}

func (coord *RootCoordMock) UpdateChannelTimeTick(ctx context.Context, req *internalpb.ChannelTimeTickMsg) (*commonpb.Status, error) {
	code := coord.state.Load().(commonpb.StateCode)
	if code != commonpb.StateCode_Healthy {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    fmt.Sprintf("state code = %s", commonpb.StateCode_name[int32(code)]),
		}, nil
	}
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}, nil
}

func (coord *RootCoordMock) DescribeSegment(ctx context.Context, req *milvuspb.DescribeSegmentRequest) (*milvuspb.DescribeSegmentResponse, error) {
	code := coord.state.Load().(commonpb.StateCode)
	if code != commonpb.StateCode_Healthy {
		return &milvuspb.DescribeSegmentResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    fmt.Sprintf("state code = %s", commonpb.StateCode_name[int32(code)]),
			},
			IndexID: 0,
		}, nil
	}
	return &milvuspb.DescribeSegmentResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		IndexID:     0,
		BuildID:     0,
		EnableIndex: false,
	}, nil
}

func (coord *RootCoordMock) ShowSegments(ctx context.Context, req *milvuspb.ShowSegmentsRequest) (*milvuspb.ShowSegmentsResponse, error) {
	code := coord.state.Load().(commonpb.StateCode)
	if code != commonpb.StateCode_Healthy {
		return &milvuspb.ShowSegmentsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    fmt.Sprintf("state code = %s", commonpb.StateCode_name[int32(code)]),
			},
			SegmentIDs: nil,
		}, nil
	}
	return &milvuspb.ShowSegmentsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		SegmentIDs: nil,
	}, nil
}

func (coord *RootCoordMock) DescribeSegments(ctx context.Context, req *rootcoordpb.DescribeSegmentsRequest) (*rootcoordpb.DescribeSegmentsResponse, error) {
	panic("implement me")
}

func (coord *RootCoordMock) InvalidateCollectionMetaCache(ctx context.Context, in *proxypb.InvalidateCollMetaCacheRequest) (*commonpb.Status, error) {
	code := coord.state.Load().(commonpb.StateCode)
	if code != commonpb.StateCode_Healthy {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    fmt.Sprintf("state code = %s", commonpb.StateCode_name[int32(code)]),
		}, nil
	}
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}, nil
}

func (coord *RootCoordMock) SegmentFlushCompleted(ctx context.Context, in *datapb.SegmentFlushCompletedMsg) (*commonpb.Status, error) {
	code := coord.state.Load().(commonpb.StateCode)
	if code != commonpb.StateCode_Healthy {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    fmt.Sprintf("state code = %s", commonpb.StateCode_name[int32(code)]),
		}, nil
	}
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}, nil
}

func (coord *RootCoordMock) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest) (*internalpb.ShowConfigurationsResponse, error) {
	if !coord.healthy() {
		return &internalpb.ShowConfigurationsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "unhealthy",
			},
		}, nil
	}

	if coord.getMetricsFunc != nil {
		return coord.showConfigurationsFunc(ctx, req)
	}

	return &internalpb.ShowConfigurationsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "not implemented",
		},
	}, nil
}

func (coord *RootCoordMock) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	if !coord.healthy() {
		return &milvuspb.GetMetricsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "unhealthy",
			},
		}, nil
	}

	if coord.getMetricsFunc != nil {
		return coord.getMetricsFunc(ctx, req)
	}

	return &milvuspb.GetMetricsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "not implemented",
		},
		Response:      "",
		ComponentName: "",
	}, nil
}

func (coord *RootCoordMock) Import(ctx context.Context, req *milvuspb.ImportRequest) (*milvuspb.ImportResponse, error) {
	code := coord.state.Load().(commonpb.StateCode)
	if code != commonpb.StateCode_Healthy {
		return &milvuspb.ImportResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    fmt.Sprintf("state code = %s", commonpb.StateCode_name[int32(code)]),
			},
			Tasks: make([]int64, 0),
		}, nil
	}
	return &milvuspb.ImportResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Tasks: make([]int64, 3),
	}, nil
}

func (coord *RootCoordMock) GetImportState(ctx context.Context, req *milvuspb.GetImportStateRequest) (*milvuspb.GetImportStateResponse, error) {
	code := coord.state.Load().(commonpb.StateCode)
	if code != commonpb.StateCode_Healthy {
		return &milvuspb.GetImportStateResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    fmt.Sprintf("state code = %s", commonpb.StateCode_name[int32(code)]),
			},
			RowCount: 0,
			IdList:   make([]int64, 0),
		}, nil
	}
	return &milvuspb.GetImportStateResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		RowCount: 10,
		IdList:   make([]int64, 3),
	}, nil
}

func (coord *RootCoordMock) ListImportTasks(ctx context.Context, in *milvuspb.ListImportTasksRequest) (*milvuspb.ListImportTasksResponse, error) {
	code := coord.state.Load().(commonpb.StateCode)
	if code != commonpb.StateCode_Healthy {
		return &milvuspb.ListImportTasksResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    fmt.Sprintf("state code = %s", commonpb.StateCode_name[int32(code)]),
			},
			Tasks: make([]*milvuspb.GetImportStateResponse, 0),
		}, nil
	}
	return &milvuspb.ListImportTasksResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Tasks: make([]*milvuspb.GetImportStateResponse, 3),
	}, nil
}

func (coord *RootCoordMock) ReportImport(ctx context.Context, req *rootcoordpb.ImportResult) (*commonpb.Status, error) {
	code := coord.state.Load().(commonpb.StateCode)
	if code != commonpb.StateCode_Healthy {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    fmt.Sprintf("state code = %s", commonpb.StateCode_name[int32(code)]),
		}, nil
	}
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}, nil
}

func NewRootCoordMock(opts ...RootCoordMockOption) *RootCoordMock {
	rc := &RootCoordMock{
		nodeID:            typeutil.UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt()),
		address:           funcutil.GenRandomStr(), // TODO(dragondriver): random address
		statisticsChannel: funcutil.GenRandomStr(),
		timeTickChannel:   funcutil.GenRandomStr(),
		collName2ID:       make(map[string]typeutil.UniqueID),
		collID2Meta:       make(map[typeutil.UniqueID]collectionMeta),
		collID2Partitions: make(map[typeutil.UniqueID]partitionMap),
		lastTs:            typeutil.Timestamp(time.Now().UnixNano()),
	}

	for _, opt := range opts {
		opt(rc)
	}

	return rc
}

func (coord *RootCoordMock) CreateCredential(ctx context.Context, req *internalpb.CredentialInfo) (*commonpb.Status, error) {
	return &commonpb.Status{}, nil
}

func (coord *RootCoordMock) UpdateCredential(ctx context.Context, req *internalpb.CredentialInfo) (*commonpb.Status, error) {
	return &commonpb.Status{}, nil
}

func (coord *RootCoordMock) DeleteCredential(ctx context.Context, req *milvuspb.DeleteCredentialRequest) (*commonpb.Status, error) {
	return &commonpb.Status{}, nil
}

func (coord *RootCoordMock) ListCredUsers(ctx context.Context, req *milvuspb.ListCredUsersRequest) (*milvuspb.ListCredUsersResponse, error) {
	return &milvuspb.ListCredUsersResponse{}, nil
}

func (coord *RootCoordMock) GetCredential(ctx context.Context, req *rootcoordpb.GetCredentialRequest) (*rootcoordpb.GetCredentialResponse, error) {
	return &rootcoordpb.GetCredentialResponse{}, nil
}

func (coord *RootCoordMock) CreateRole(ctx context.Context, req *milvuspb.CreateRoleRequest) (*commonpb.Status, error) {
	return &commonpb.Status{}, nil
}

func (coord *RootCoordMock) DropRole(ctx context.Context, req *milvuspb.DropRoleRequest) (*commonpb.Status, error) {
	return &commonpb.Status{}, nil
}

func (coord *RootCoordMock) OperateUserRole(ctx context.Context, req *milvuspb.OperateUserRoleRequest) (*commonpb.Status, error) {
	return &commonpb.Status{}, nil
}

func (coord *RootCoordMock) SelectRole(ctx context.Context, req *milvuspb.SelectRoleRequest) (*milvuspb.SelectRoleResponse, error) {
	return &milvuspb.SelectRoleResponse{}, nil
}

func (coord *RootCoordMock) SelectUser(ctx context.Context, req *milvuspb.SelectUserRequest) (*milvuspb.SelectUserResponse, error) {
	return &milvuspb.SelectUserResponse{}, nil
}

func (coord *RootCoordMock) OperatePrivilege(ctx context.Context, req *milvuspb.OperatePrivilegeRequest) (*commonpb.Status, error) {
	return &commonpb.Status{}, nil
}

func (coord *RootCoordMock) SelectGrant(ctx context.Context, req *milvuspb.SelectGrantRequest) (*milvuspb.SelectGrantResponse, error) {
	return &milvuspb.SelectGrantResponse{}, nil
}

func (coord *RootCoordMock) ListPolicy(ctx context.Context, in *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
	return &internalpb.ListPolicyResponse{}, nil
}

func (coord *RootCoordMock) AlterCollection(ctx context.Context, request *milvuspb.AlterCollectionRequest) (*commonpb.Status, error) {
	return &commonpb.Status{}, nil
}

func (coord *RootCoordMock) CreateDatabase(ctx context.Context, in *milvuspb.CreateDatabaseRequest) (*commonpb.Status, error) {
	return &commonpb.Status{}, nil
}

func (coord *RootCoordMock) DropDatabase(ctx context.Context, in *milvuspb.DropDatabaseRequest) (*commonpb.Status, error) {
	return &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil
}

func (coord *RootCoordMock) ListDatabases(ctx context.Context, in *milvuspb.ListDatabasesRequest) (*milvuspb.ListDatabasesResponse, error) {
	return &milvuspb.ListDatabasesResponse{}, nil
}

func (coord *RootCoordMock) CheckHealth(ctx context.Context, req *milvuspb.CheckHealthRequest) (*milvuspb.CheckHealthResponse, error) {
	if coord.checkHealthFunc != nil {
		return coord.checkHealthFunc(ctx, req)
	}
	return &milvuspb.CheckHealthResponse{IsHealthy: true}, nil
}

func (coord *RootCoordMock) RenameCollection(ctx context.Context, req *milvuspb.RenameCollectionRequest) (*commonpb.Status, error) {
	return &commonpb.Status{}, nil
}

type DescribeCollectionFunc func(ctx context.Context, request *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error)

type ShowPartitionsFunc func(ctx context.Context, request *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error)

type ShowSegmentsFunc func(ctx context.Context, request *milvuspb.ShowSegmentsRequest) (*milvuspb.ShowSegmentsResponse, error)

type DescribeSegmentsFunc func(ctx context.Context, request *rootcoordpb.DescribeSegmentsRequest) (*rootcoordpb.DescribeSegmentsResponse, error)

type ImportFunc func(ctx context.Context, req *milvuspb.ImportRequest) (*milvuspb.ImportResponse, error)

type DropCollectionFunc func(ctx context.Context, request *milvuspb.DropCollectionRequest) (*commonpb.Status, error)

type GetGetCredentialFunc func(ctx context.Context, req *rootcoordpb.GetCredentialRequest) (*rootcoordpb.GetCredentialResponse, error)

type mockRootCoord struct {
	types.RootCoord
	DescribeCollectionFunc
	ShowPartitionsFunc
	ShowSegmentsFunc
	DescribeSegmentsFunc
	ImportFunc
	DropCollectionFunc
	GetGetCredentialFunc
}

func (m *mockRootCoord) GetCredential(ctx context.Context, request *rootcoordpb.GetCredentialRequest) (*rootcoordpb.GetCredentialResponse, error) {
	if m.GetGetCredentialFunc != nil {
		return m.GetGetCredentialFunc(ctx, request)
	}
	return nil, errors.New("mock")

}

func (m *mockRootCoord) DescribeCollection(ctx context.Context, request *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	if m.DescribeCollectionFunc != nil {
		return m.DescribeCollectionFunc(ctx, request)
	}
	return nil, errors.New("mock")
}

func (m *mockRootCoord) DescribeCollectionInternal(ctx context.Context, request *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	return m.DescribeCollection(ctx, request)
}

func (m *mockRootCoord) ShowPartitions(ctx context.Context, request *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error) {
	if m.ShowPartitionsFunc != nil {
		return m.ShowPartitionsFunc(ctx, request)
	}
	return nil, errors.New("mock")
}

func (m *mockRootCoord) ShowPartitionsInternal(ctx context.Context, request *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error) {
	return m.ShowPartitions(ctx, request)
}

func (m *mockRootCoord) ShowSegments(ctx context.Context, request *milvuspb.ShowSegmentsRequest) (*milvuspb.ShowSegmentsResponse, error) {
	if m.ShowSegmentsFunc != nil {
		return m.ShowSegmentsFunc(ctx, request)
	}
	return nil, errors.New("mock")
}

func (m *mockRootCoord) Import(ctx context.Context, request *milvuspb.ImportRequest) (*milvuspb.ImportResponse, error) {
	if m.ImportFunc != nil {
		return m.ImportFunc(ctx, request)
	}
	return nil, errors.New("mock")
}

func (m *mockRootCoord) DropCollection(ctx context.Context, request *milvuspb.DropCollectionRequest) (*commonpb.Status, error) {
	if m.DropCollectionFunc != nil {
		return m.DropCollectionFunc(ctx, request)
	}
	return nil, errors.New("mock")
}

func (m *mockRootCoord) ListPolicy(ctx context.Context, in *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
	return &internalpb.ListPolicyResponse{}, nil
}

func (m *mockRootCoord) CheckHealth(ctx context.Context, req *milvuspb.CheckHealthRequest) (*milvuspb.CheckHealthResponse, error) {
	return &milvuspb.CheckHealthResponse{
		IsHealthy: true,
	}, nil
}

func (m *mockRootCoord) CreateDatabase(ctx context.Context, in *milvuspb.CreateDatabaseRequest) (*commonpb.Status, error) {
	return &commonpb.Status{}, nil
}

func (m *mockRootCoord) DropDatabase(ctx context.Context, in *milvuspb.DropDatabaseRequest) (*commonpb.Status, error) {
	return &commonpb.Status{}, nil
}

func (m *mockRootCoord) ListDatabases(ctx context.Context, in *milvuspb.ListDatabasesRequest) (*milvuspb.ListDatabasesResponse, error) {
	return &milvuspb.ListDatabasesResponse{}, nil
}

func newMockRootCoord() *mockRootCoord {
	return &mockRootCoord{}
}
