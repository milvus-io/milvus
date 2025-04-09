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
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
	"github.com/milvus-io/milvus/pkg/v2/util/uniquegenerator"
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
	properties           []*commonpb.KeyValuePair
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

type RootCoordMockOption func(mock *MixCoordMock)

type describeCollectionFuncType func(ctx context.Context, request *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error)

type showPartitionsFuncType func(ctx context.Context, request *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error)

type MixCoordMock struct {
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

	lastTs                  typeutil.Timestamp
	lastTsMtx               sync.Mutex
	checkHealthFunc         func(ctx context.Context, req *milvuspb.CheckHealthRequest, opts ...grpc.CallOption) (*milvuspb.CheckHealthResponse, error)
	GetIndexStateFunc       func(ctx context.Context, request *indexpb.GetIndexStateRequest, opts ...grpc.CallOption) (*indexpb.GetIndexStateResponse, error)
	DescribeIndexFunc       func(ctx context.Context, request *indexpb.DescribeIndexRequest, opts ...grpc.CallOption) (*indexpb.DescribeIndexResponse, error)
	GetShardLeadersFunc     func(ctx context.Context, request *querypb.GetShardLeadersRequest, opts ...grpc.CallOption) (*querypb.GetShardLeadersResponse, error)
	ShowLoadPartitionsFunc  func(ctx context.Context, request *querypb.ShowPartitionsRequest, opts ...grpc.CallOption) (*querypb.ShowPartitionsResponse, error)
	ShowLoadCollectionsFunc func(ctx context.Context, request *querypb.ShowCollectionsRequest, opts ...grpc.CallOption) (*querypb.ShowCollectionsResponse, error)
	GetGetCredentialFunc
	DescribeCollectionFunc
	ShowPartitionsFunc
	ShowSegmentsFunc
	DescribeSegmentsFunc
	ImportFunc
	DropCollectionFunc
}

func (coord *MixCoordMock) CreateAlias(ctx context.Context, req *milvuspb.CreateAliasRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
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
	return merr.Success(), nil
}

func (coord *MixCoordMock) DropAlias(ctx context.Context, req *milvuspb.DropAliasRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
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
	return merr.Success(), nil
}

func (coord *MixCoordMock) AlterAlias(ctx context.Context, req *milvuspb.AlterAliasRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
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
	return merr.Success(), nil
}

func (coord *MixCoordMock) DescribeAlias(ctx context.Context, req *milvuspb.DescribeAliasRequest, opts ...grpc.CallOption) (*milvuspb.DescribeAliasResponse, error) {
	code := coord.state.Load().(commonpb.StateCode)
	if code != commonpb.StateCode_Healthy {
		return &milvuspb.DescribeAliasResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    fmt.Sprintf("state code = %s", commonpb.StateCode_name[int32(code)]),
			},
		}, nil
	}
	coord.collMtx.Lock()
	defer coord.collMtx.Unlock()

	collID, exist := coord.collAlias2ID[req.Alias]
	if !exist {
		return &milvuspb.DescribeAliasResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_CollectionNotExists,
				Reason:    fmt.Sprintf("alias does not exist, alias = %s", req.Alias),
			},
		}, nil
	}
	collMeta, exist := coord.collID2Meta[collID]
	if !exist {
		return &milvuspb.DescribeAliasResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_CollectionNotExists,
				Reason:    fmt.Sprintf("alias exist but not find related collection, alias = %s collID = %d", req.Alias, collID),
			},
		}, nil
	}
	return &milvuspb.DescribeAliasResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		DbName:     req.GetDbName(),
		Alias:      req.GetAlias(),
		Collection: collMeta.name,
	}, nil
}

func (coord *MixCoordMock) ListAliases(ctx context.Context, req *milvuspb.ListAliasesRequest, opts ...grpc.CallOption) (*milvuspb.ListAliasesResponse, error) {
	code := coord.state.Load().(commonpb.StateCode)
	if code != commonpb.StateCode_Healthy {
		return &milvuspb.ListAliasesResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    fmt.Sprintf("state code = %s", commonpb.StateCode_name[int32(code)]),
			},
		}, nil
	}
	coord.collMtx.Lock()
	defer coord.collMtx.Unlock()

	var aliases []string
	for alias := range coord.collAlias2ID {
		aliases = append(aliases, alias)
	}
	return &milvuspb.ListAliasesResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		DbName:  req.GetDbName(),
		Aliases: aliases,
	}, nil
}

func (coord *MixCoordMock) updateState(state commonpb.StateCode) {
	coord.state.Store(state)
}

func (coord *MixCoordMock) getState() commonpb.StateCode {
	return coord.state.Load().(commonpb.StateCode)
}

func (coord *MixCoordMock) healthy() bool {
	return coord.getState() == commonpb.StateCode_Healthy
}

func (coord *MixCoordMock) Close() error {
	defer coord.updateState(commonpb.StateCode_Abnormal)
	return nil
}

func (coord *MixCoordMock) GetComponentStates(ctx context.Context, req *milvuspb.GetComponentStatesRequest, opts ...grpc.CallOption) (*milvuspb.ComponentStates, error) {
	return &milvuspb.ComponentStates{
		State: &milvuspb.ComponentInfo{
			NodeID:    coord.nodeID,
			Role:      typeutil.RootCoordRole,
			StateCode: coord.getState(),
			ExtraInfo: nil,
		},
		SubcomponentStates: nil,
		Status:             merr.Success(),
	}, nil
}

func (coord *MixCoordMock) GetStatisticsChannel(ctx context.Context, req *internalpb.GetStatisticsChannelRequest, opts ...grpc.CallOption) (*milvuspb.StringResponse, error) {
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
		Status: merr.Success(),
		Value:  coord.statisticsChannel,
	}, nil
}

func (coord *MixCoordMock) Register() error {
	return nil
}

func (coord *MixCoordMock) GetTimeTickChannel(ctx context.Context, req *internalpb.GetTimeTickChannelRequest, opts ...grpc.CallOption) (*milvuspb.StringResponse, error) {
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
		Status: merr.Success(),
		Value:  coord.timeTickChannel,
	}, nil
}

func (coord *MixCoordMock) AddCollectionField(ctx context.Context, req *milvuspb.AddCollectionFieldRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
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
			Reason:    "collection not exist",
		}, nil
	}

	collInfo, exist := coord.collID2Meta[collID]

	if !exist {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_CollectionNotExists,
			Reason:    "collection info not exist",
		}, nil
	}
	fieldSchema := &schemapb.FieldSchema{}

	err := proto.Unmarshal(req.Schema, fieldSchema)
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "invalid parameter",
		}, nil
	}

	fieldSchema.FieldID = int64(common.StartOfUserFieldID + len(collInfo.schema.Fields) + 1)
	collInfo.schema.Fields = append(collInfo.schema.Fields, fieldSchema)
	ts := uint64(time.Now().Nanosecond())
	coord.collID2Meta[collID] = collectionMeta{
		name:                 req.CollectionName,
		id:                   collID,
		schema:               collInfo.schema,
		shardsNum:            collInfo.shardsNum,
		virtualChannelNames:  collInfo.virtualChannelNames,
		physicalChannelNames: collInfo.physicalChannelNames,
		createdTimestamp:     ts,
		createdUtcTimestamp:  ts,
		properties:           collInfo.properties,
	}
	return merr.Success(), nil
}

func (coord *MixCoordMock) CreateCollection(ctx context.Context, req *milvuspb.CreateCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	code := coord.state.Load().(commonpb.StateCode)
	if code != commonpb.StateCode_Healthy {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    fmt.Sprintf("state code = %s", commonpb.StateCode_name[int32(code)]),
		}, nil
	}
	coord.collMtx.Lock()
	defer coord.collMtx.Unlock()

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
		properties:           req.GetProperties(),
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

	return merr.Success(), nil
}

func (coord *MixCoordMock) DropCollection(ctx context.Context, req *milvuspb.DropCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
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
		return merr.Status(merr.WrapErrCollectionNotFound(req.CollectionName)), nil
	}

	delete(coord.collName2ID, req.CollectionName)

	delete(coord.collID2Meta, collID)

	coord.partitionMtx.Lock()
	defer coord.partitionMtx.Unlock()

	delete(coord.collID2Partitions, collID)

	return merr.Success(), nil
}

func (coord *MixCoordMock) HasCollection(ctx context.Context, req *milvuspb.HasCollectionRequest, opts ...grpc.CallOption) (*milvuspb.BoolResponse, error) {
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
		Status: merr.Success(),
		Value:  exist,
	}, nil
}

func (coord *MixCoordMock) SetDescribeCollectionFunc(f describeCollectionFuncType) {
	coord.describeCollectionFunc = f
}

func (coord *MixCoordMock) ResetDescribeCollectionFunc() {
	coord.describeCollectionFunc = nil
}

func (coord *MixCoordMock) DescribeCollection(ctx context.Context, req *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
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
			Status: merr.Status(merr.WrapErrCollectionNotFound(req.CollectionName)),
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
		Status:               merr.Success(),
		Schema:               meta.schema,
		CollectionID:         collID,
		ShardsNum:            meta.shardsNum,
		VirtualChannelNames:  meta.virtualChannelNames,
		PhysicalChannelNames: meta.physicalChannelNames,
		CreatedTimestamp:     meta.createdUtcTimestamp,
		CreatedUtcTimestamp:  meta.createdUtcTimestamp,
		Properties:           meta.properties,
	}, nil
}

func (coord *MixCoordMock) DescribeCollectionInternal(ctx context.Context, req *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
	return coord.DescribeCollection(ctx, req)
}

func (coord *MixCoordMock) ShowCollections(ctx context.Context, req *milvuspb.ShowCollectionsRequest, opts ...grpc.CallOption) (*milvuspb.ShowCollectionsResponse, error) {
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
		Status:               merr.Success(),
		CollectionNames:      names,
		CollectionIds:        ids,
		CreatedTimestamps:    createdTimestamps,
		CreatedUtcTimestamps: createdUtcTimestamps,
		InMemoryPercentages:  nil,
	}, nil
}

func (coord *MixCoordMock) ShowCollectionIDs(ctx context.Context, req *rootcoordpb.ShowCollectionIDsRequest, opts ...grpc.CallOption) (*rootcoordpb.ShowCollectionIDsResponse, error) {
	panic("implements me")
}

func (coord *MixCoordMock) CreatePartition(ctx context.Context, req *milvuspb.CreatePartitionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
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
		return merr.Status(merr.WrapErrCollectionNotFound(req.CollectionName)), nil
	}

	coord.partitionMtx.Lock()
	defer coord.partitionMtx.Unlock()

	ts := uint64(time.Now().Nanosecond())

	partitionID := typeutil.UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	coord.collID2Partitions[collID].partitionName2ID[req.PartitionName] = partitionID
	coord.collID2Partitions[collID].partitionID2Name[partitionID] = req.PartitionName
	coord.collID2Partitions[collID].partitionID2Meta[partitionID] = partitionMeta{
		createdTimestamp:    ts,
		createdUtcTimestamp: ts,
	}

	return merr.Success(), nil
}

func (coord *MixCoordMock) DropPartition(ctx context.Context, req *milvuspb.DropPartitionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
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
		return merr.Status(merr.WrapErrCollectionNotFound(req.CollectionName)), nil
	}

	coord.partitionMtx.Lock()
	defer coord.partitionMtx.Unlock()

	partitionID, partitionExist := coord.collID2Partitions[collID].partitionName2ID[req.PartitionName]
	if !partitionExist {
		return merr.Status(merr.WrapErrPartitionNotFound(req.PartitionName)), nil
	}

	delete(coord.collID2Partitions[collID].partitionName2ID, req.PartitionName)
	delete(coord.collID2Partitions[collID].partitionID2Name, partitionID)

	return merr.Success(), nil
}

func (coord *MixCoordMock) HasPartition(ctx context.Context, req *milvuspb.HasPartitionRequest, opts ...grpc.CallOption) (*milvuspb.BoolResponse, error) {
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
			Status: merr.Status(merr.WrapErrCollectionNotFound(req.CollectionName)),
			Value:  false,
		}, nil
	}

	coord.partitionMtx.RLock()
	defer coord.partitionMtx.RUnlock()

	_, partitionExist := coord.collID2Partitions[collID].partitionName2ID[req.PartitionName]
	return &milvuspb.BoolResponse{
		Status: merr.Success(),
		Value:  partitionExist,
	}, nil
}

func (coord *MixCoordMock) ShowPartitions(ctx context.Context, req *milvuspb.ShowPartitionsRequest, opts ...grpc.CallOption) (*milvuspb.ShowPartitionsResponse, error) {
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
			Status: merr.Status(merr.WrapErrCollectionNotFound(req.CollectionName)),
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
		Status:               merr.Success(),
		PartitionNames:       names,
		PartitionIDs:         ids,
		CreatedTimestamps:    createdTimestamps,
		CreatedUtcTimestamps: createdUtcTimestamps,
		InMemoryPercentages:  nil,
	}, nil
}

func (coord *MixCoordMock) ShowPartitionsInternal(ctx context.Context, req *milvuspb.ShowPartitionsRequest, opts ...grpc.CallOption) (*milvuspb.ShowPartitionsResponse, error) {
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

func (coord *MixCoordMock) AllocTimestamp(ctx context.Context, req *rootcoordpb.AllocTimestampRequest, opts ...grpc.CallOption) (*rootcoordpb.AllocTimestampResponse, error) {
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
		Status:    merr.Success(),
		Timestamp: ts,
		Count:     req.Count,
	}, nil
}

func (coord *MixCoordMock) AllocID(ctx context.Context, req *rootcoordpb.AllocIDRequest, opts ...grpc.CallOption) (*rootcoordpb.AllocIDResponse, error) {
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
		Status: merr.Success(),
		ID:     int64(begin),
		Count:  req.Count,
	}, nil
}

func (coord *MixCoordMock) UpdateChannelTimeTick(ctx context.Context, req *internalpb.ChannelTimeTickMsg, opts ...grpc.CallOption) (*commonpb.Status, error) {
	code := coord.state.Load().(commonpb.StateCode)
	if code != commonpb.StateCode_Healthy {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    fmt.Sprintf("state code = %s", commonpb.StateCode_name[int32(code)]),
		}, nil
	}
	return merr.Success(), nil
}

func (coord *MixCoordMock) DescribeSegment(ctx context.Context, req *milvuspb.DescribeSegmentRequest, opts ...grpc.CallOption) (*milvuspb.DescribeSegmentResponse, error) {
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
		Status:      merr.Success(),
		IndexID:     0,
		BuildID:     0,
		EnableIndex: false,
	}, nil
}

func (coord *MixCoordMock) ShowSegments(ctx context.Context, req *milvuspb.ShowSegmentsRequest, opts ...grpc.CallOption) (*milvuspb.ShowSegmentsResponse, error) {
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
		Status:     merr.Success(),
		SegmentIDs: nil,
	}, nil
}

func (coord *MixCoordMock) GetPChannelInfo(ctx context.Context, in *rootcoordpb.GetPChannelInfoRequest, opts ...grpc.CallOption) (*rootcoordpb.GetPChannelInfoResponse, error) {
	panic("implement me")
}

func (coord *MixCoordMock) DescribeSegments(ctx context.Context, req *rootcoordpb.DescribeSegmentsRequest, opts ...grpc.CallOption) (*rootcoordpb.DescribeSegmentsResponse, error) {
	panic("implement me")
}

func (coord *MixCoordMock) InvalidateCollectionMetaCache(ctx context.Context, in *proxypb.InvalidateCollMetaCacheRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	code := coord.state.Load().(commonpb.StateCode)
	if code != commonpb.StateCode_Healthy {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    fmt.Sprintf("state code = %s", commonpb.StateCode_name[int32(code)]),
		}, nil
	}
	return merr.Success(), nil
}

func (coord *MixCoordMock) SegmentFlushCompleted(ctx context.Context, in *datapb.SegmentFlushCompletedMsg) (*commonpb.Status, error) {
	code := coord.state.Load().(commonpb.StateCode)
	if code != commonpb.StateCode_Healthy {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    fmt.Sprintf("state code = %s", commonpb.StateCode_name[int32(code)]),
		}, nil
	}
	return merr.Success(), nil
}

func (coord *MixCoordMock) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest, opts ...grpc.CallOption) (*internalpb.ShowConfigurationsResponse, error) {
	if !coord.healthy() {
		return &internalpb.ShowConfigurationsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "unhealthy",
			},
		}, nil
	}

	if coord.showConfigurationsFunc != nil {
		return coord.showConfigurationsFunc(ctx, req)
	}

	return &internalpb.ShowConfigurationsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "not implemented",
		},
	}, nil
}

func (coord *MixCoordMock) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest, opts ...grpc.CallOption) (*milvuspb.GetMetricsResponse, error) {
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

func (coord *MixCoordMock) Import(ctx context.Context, req *milvuspb.ImportRequest, opts ...grpc.CallOption) (*milvuspb.ImportResponse, error) {
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
		Status: merr.Success(),
		Tasks:  make([]int64, 3),
	}, nil
}

func (coord *MixCoordMock) GetImportState(ctx context.Context, req *milvuspb.GetImportStateRequest, opts ...grpc.CallOption) (*milvuspb.GetImportStateResponse, error) {
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
		Status:   merr.Success(),
		RowCount: 10,
		IdList:   make([]int64, 3),
	}, nil
}

func (coord *MixCoordMock) ListImportTasks(ctx context.Context, in *milvuspb.ListImportTasksRequest, opts ...grpc.CallOption) (*milvuspb.ListImportTasksResponse, error) {
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
		Status: merr.Success(),
		Tasks:  make([]*milvuspb.GetImportStateResponse, 3),
	}, nil
}

func NewMixCoordMock(opts ...RootCoordMockOption) *MixCoordMock {
	coord := &MixCoordMock{
		nodeID:            typeutil.UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt()),
		address:           funcutil.GenRandomStr(), // TODO(dragondriver): random address
		statisticsChannel: funcutil.GenRandomStr(),
		timeTickChannel:   funcutil.GenRandomStr(),
		collName2ID:       make(map[string]typeutil.UniqueID),
		collID2Meta:       make(map[typeutil.UniqueID]collectionMeta),
		collID2Partitions: make(map[typeutil.UniqueID]partitionMap),
		lastTs:            typeutil.Timestamp(time.Now().UnixNano()),
		state:             atomic.Value{},
		getMetricsFunc:    nil,
	}

	for _, opt := range opts {
		opt(coord)
	}

	coord.updateState(commonpb.StateCode_Healthy)
	return coord
}

func (coord *MixCoordMock) CreateCredential(ctx context.Context, req *internalpb.CredentialInfo, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, nil
}

func (coord *MixCoordMock) UpdateCredential(ctx context.Context, req *internalpb.CredentialInfo, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, nil
}

func (coord *MixCoordMock) DeleteCredential(ctx context.Context, req *milvuspb.DeleteCredentialRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, nil
}

func (coord *MixCoordMock) ListCredUsers(ctx context.Context, req *milvuspb.ListCredUsersRequest, opts ...grpc.CallOption) (*milvuspb.ListCredUsersResponse, error) {
	return &milvuspb.ListCredUsersResponse{}, nil
}

func (coord *MixCoordMock) GetCredential(ctx context.Context, req *rootcoordpb.GetCredentialRequest, opts ...grpc.CallOption) (*rootcoordpb.GetCredentialResponse, error) {
	if coord.GetGetCredentialFunc != nil {
		return coord.GetGetCredentialFunc(ctx, req)
	}
	return nil, errors.New("mock")
}

func (coord *MixCoordMock) CreateRole(ctx context.Context, req *milvuspb.CreateRoleRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, nil
}

func (coord *MixCoordMock) DropRole(ctx context.Context, req *milvuspb.DropRoleRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, nil
}

func (coord *MixCoordMock) OperateUserRole(ctx context.Context, req *milvuspb.OperateUserRoleRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, nil
}

func (coord *MixCoordMock) SelectRole(ctx context.Context, req *milvuspb.SelectRoleRequest, opts ...grpc.CallOption) (*milvuspb.SelectRoleResponse, error) {
	return &milvuspb.SelectRoleResponse{}, nil
}

func (coord *MixCoordMock) SelectUser(ctx context.Context, req *milvuspb.SelectUserRequest, opts ...grpc.CallOption) (*milvuspb.SelectUserResponse, error) {
	return &milvuspb.SelectUserResponse{}, nil
}

func (coord *MixCoordMock) OperatePrivilege(ctx context.Context, req *milvuspb.OperatePrivilegeRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, nil
}

func (coord *MixCoordMock) SelectGrant(ctx context.Context, req *milvuspb.SelectGrantRequest, opts ...grpc.CallOption) (*milvuspb.SelectGrantResponse, error) {
	return &milvuspb.SelectGrantResponse{}, nil
}

func (coord *MixCoordMock) ListPolicy(ctx context.Context, in *internalpb.ListPolicyRequest, opts ...grpc.CallOption) (*internalpb.ListPolicyResponse, error) {
	return &internalpb.ListPolicyResponse{}, nil
}

func (coord *MixCoordMock) AlterCollection(ctx context.Context, request *milvuspb.AlterCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, nil
}

func (coord *MixCoordMock) AlterCollectionField(ctx context.Context, request *milvuspb.AlterCollectionFieldRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, nil
}

func (coord *MixCoordMock) CreateDatabase(ctx context.Context, in *milvuspb.CreateDatabaseRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, nil
}

func (coord *MixCoordMock) DropDatabase(ctx context.Context, in *milvuspb.DropDatabaseRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil
}

func (coord *MixCoordMock) ListDatabases(ctx context.Context, in *milvuspb.ListDatabasesRequest, opts ...grpc.CallOption) (*milvuspb.ListDatabasesResponse, error) {
	return &milvuspb.ListDatabasesResponse{}, nil
}

func (coord *MixCoordMock) CheckHealth(ctx context.Context, req *milvuspb.CheckHealthRequest, opts ...grpc.CallOption) (*milvuspb.CheckHealthResponse, error) {
	if coord.checkHealthFunc != nil {
		return coord.checkHealthFunc(ctx, req)
	}
	return &milvuspb.CheckHealthResponse{IsHealthy: true}, nil
}

func (coord *MixCoordMock) RenameCollection(ctx context.Context, req *milvuspb.RenameCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, nil
}

func (coord *MixCoordMock) DescribeDatabase(ctx context.Context, in *rootcoordpb.DescribeDatabaseRequest, opts ...grpc.CallOption) (*rootcoordpb.DescribeDatabaseResponse, error) {
	return &rootcoordpb.DescribeDatabaseResponse{}, nil
}

func (coord *MixCoordMock) AlterDatabase(ctx context.Context, in *rootcoordpb.AlterDatabaseRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, nil
}

func (coord *MixCoordMock) GetChannelRecoveryInfo(ctx context.Context, in *datapb.GetChannelRecoveryInfoRequest, opts ...grpc.CallOption) (*datapb.GetChannelRecoveryInfoResponse, error) {
	return &datapb.GetChannelRecoveryInfoResponse{}, nil
}

func (coord *MixCoordMock) BackupRBAC(ctx context.Context, in *milvuspb.BackupRBACMetaRequest, opts ...grpc.CallOption) (*milvuspb.BackupRBACMetaResponse, error) {
	return &milvuspb.BackupRBACMetaResponse{}, nil
}

func (coord *MixCoordMock) RestoreRBAC(ctx context.Context, in *milvuspb.RestoreRBACMetaRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, nil
}

func (coord *MixCoordMock) CreatePrivilegeGroup(ctx context.Context, req *milvuspb.CreatePrivilegeGroupRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, nil
}

func (coord *MixCoordMock) DropPrivilegeGroup(ctx context.Context, req *milvuspb.DropPrivilegeGroupRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, nil
}

func (coord *MixCoordMock) ListPrivilegeGroups(ctx context.Context, req *milvuspb.ListPrivilegeGroupsRequest, opts ...grpc.CallOption) (*milvuspb.ListPrivilegeGroupsResponse, error) {
	return &milvuspb.ListPrivilegeGroupsResponse{}, nil
}

func (coord *MixCoordMock) OperatePrivilegeGroup(ctx context.Context, req *milvuspb.OperatePrivilegeGroupRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, nil
}

func (coord *MixCoordMock) Flush(ctx context.Context, req *datapb.FlushRequest, opts ...grpc.CallOption) (*datapb.FlushResponse, error) {
	panic("implement me")
}

func (coord *MixCoordMock) MarkSegmentsDropped(ctx context.Context, req *datapb.MarkSegmentsDroppedRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (coord *MixCoordMock) BroadcastAlteredCollection(ctx context.Context, req *datapb.AlterCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (coord *MixCoordMock) AssignSegmentID(ctx context.Context, req *datapb.AssignSegmentIDRequest, opts ...grpc.CallOption) (*datapb.AssignSegmentIDResponse, error) {
	panic("implement me")
}

func (coord *MixCoordMock) GetSegmentStates(ctx context.Context, req *datapb.GetSegmentStatesRequest, opts ...grpc.CallOption) (*datapb.GetSegmentStatesResponse, error) {
	panic("implement me")
}

func (coord *MixCoordMock) GetInsertBinlogPaths(ctx context.Context, req *datapb.GetInsertBinlogPathsRequest, opts ...grpc.CallOption) (*datapb.GetInsertBinlogPathsResponse, error) {
	panic("implement me")
}

func (coord *MixCoordMock) GetSegmentInfoChannel(ctx context.Context, in *datapb.GetSegmentInfoChannelRequest, opts ...grpc.CallOption) (*milvuspb.StringResponse, error) {
	panic("implement me")
}

func (coord *MixCoordMock) GetCollectionStatistics(ctx context.Context, req *datapb.GetCollectionStatisticsRequest, opts ...grpc.CallOption) (*datapb.GetCollectionStatisticsResponse, error) {
	panic("implement me")
}

func (coord *MixCoordMock) GetPartitionStatistics(ctx context.Context, req *datapb.GetPartitionStatisticsRequest, opts ...grpc.CallOption) (*datapb.GetPartitionStatisticsResponse, error) {
	panic("implement me")
}

// AllocSegment alloc a new growing segment, add it into segment meta.
func (coord *MixCoordMock) AllocSegment(ctx context.Context, in *datapb.AllocSegmentRequest, opts ...grpc.CallOption) (*datapb.AllocSegmentResponse, error) {
	panic("implement me")
}

func (coord *MixCoordMock) GetSegmentInfo(ctx context.Context, req *datapb.GetSegmentInfoRequest, opts ...grpc.CallOption) (*datapb.GetSegmentInfoResponse, error) {
	panic("implement me")
}

func (coord *MixCoordMock) GetRecoveryInfo(ctx context.Context, req *datapb.GetRecoveryInfoRequest, opts ...grpc.CallOption) (*datapb.GetRecoveryInfoResponse, error) {
	panic("implement me")
}

func (coord *MixCoordMock) SaveBinlogPaths(ctx context.Context, req *datapb.SaveBinlogPathsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (coord *MixCoordMock) GetFlushedSegments(ctx context.Context, req *datapb.GetFlushedSegmentsRequest, opts ...grpc.CallOption) (*datapb.GetFlushedSegmentsResponse, error) {
	panic("implement me")
}

func (coord *MixCoordMock) GetSegmentsByStates(ctx context.Context, req *datapb.GetSegmentsByStatesRequest, opts ...grpc.CallOption) (*datapb.GetSegmentsByStatesResponse, error) {
	panic("implement me")
}

func (coord *MixCoordMock) CompleteCompaction(ctx context.Context, req *datapb.CompactionPlanResult, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, nil
}

func (coord *MixCoordMock) ManualCompaction(ctx context.Context, req *milvuspb.ManualCompactionRequest, opts ...grpc.CallOption) (*milvuspb.ManualCompactionResponse, error) {
	return &milvuspb.ManualCompactionResponse{}, nil
}

func (coord *MixCoordMock) GetCompactionState(ctx context.Context, req *milvuspb.GetCompactionStateRequest, opts ...grpc.CallOption) (*milvuspb.GetCompactionStateResponse, error) {
	return &milvuspb.GetCompactionStateResponse{}, nil
}

func (coord *MixCoordMock) GetCompactionStateWithPlans(ctx context.Context, req *milvuspb.GetCompactionPlansRequest, opts ...grpc.CallOption) (*milvuspb.GetCompactionPlansResponse, error) {
	return &milvuspb.GetCompactionPlansResponse{}, nil
}

func (coord *MixCoordMock) WatchChannels(ctx context.Context, req *datapb.WatchChannelsRequest, opts ...grpc.CallOption) (*datapb.WatchChannelsResponse, error) {
	return &datapb.WatchChannelsResponse{}, nil
}

func (coord *MixCoordMock) GetFlushState(ctx context.Context, req *datapb.GetFlushStateRequest, opts ...grpc.CallOption) (*milvuspb.GetFlushStateResponse, error) {
	return &milvuspb.GetFlushStateResponse{}, nil
}

func (coord *MixCoordMock) GetFlushAllState(ctx context.Context, req *milvuspb.GetFlushAllStateRequest, opts ...grpc.CallOption) (*milvuspb.GetFlushAllStateResponse, error) {
	return &milvuspb.GetFlushAllStateResponse{}, nil
}

func (coord *MixCoordMock) DropVirtualChannel(ctx context.Context, req *datapb.DropVirtualChannelRequest, opts ...grpc.CallOption) (*datapb.DropVirtualChannelResponse, error) {
	return &datapb.DropVirtualChannelResponse{}, nil
}

func (coord *MixCoordMock) SetSegmentState(ctx context.Context, req *datapb.SetSegmentStateRequest, opts ...grpc.CallOption) (*datapb.SetSegmentStateResponse, error) {
	return &datapb.SetSegmentStateResponse{}, nil
}

func (coord *MixCoordMock) UpdateSegmentStatistics(ctx context.Context, req *datapb.UpdateSegmentStatisticsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return merr.Success(), nil
}

func (coord *MixCoordMock) UpdateChannelCheckpoint(ctx context.Context, req *datapb.UpdateChannelCheckpointRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return merr.Success(), nil
}

func (coord *MixCoordMock) CreateIndex(ctx context.Context, req *indexpb.CreateIndexRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return merr.Success(), nil
}

func (coord *MixCoordMock) AlterIndex(ctx context.Context, req *indexpb.AlterIndexRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return merr.Success(), nil
}

func (coord *MixCoordMock) ListIndexes(ctx context.Context, in *indexpb.ListIndexesRequest, opts ...grpc.CallOption) (*indexpb.ListIndexesResponse, error) {
	return &indexpb.ListIndexesResponse{
		Status: merr.Success(),
	}, nil
}

func (coord *MixCoordMock) GcConfirm(ctx context.Context, in *datapb.GcConfirmRequest, opts ...grpc.CallOption) (*datapb.GcConfirmResponse, error) {
	return &datapb.GcConfirmResponse{
		Status: merr.Success(),
	}, nil
}

func (coord *MixCoordMock) ReportDataNodeTtMsgs(ctx context.Context, in *datapb.ReportDataNodeTtMsgsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return merr.Success(), nil
}

func (coord *MixCoordMock) GcControl(ctx context.Context, in *datapb.GcControlRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return merr.Success(), nil
}

// importV2
func (coord *MixCoordMock) ImportV2(ctx context.Context, in *internalpb.ImportRequestInternal, opts ...grpc.CallOption) (*internalpb.ImportResponse, error) {
	return &internalpb.ImportResponse{
		Status: merr.Success(),
	}, nil
}

func (coord *MixCoordMock) GetImportProgress(ctx context.Context, in *internalpb.GetImportProgressRequest, opts ...grpc.CallOption) (*internalpb.GetImportProgressResponse, error) {
	return &internalpb.GetImportProgressResponse{
		Status: merr.Success(),
	}, nil
}

func (coord *MixCoordMock) ListImports(ctx context.Context, in *internalpb.ListImportsRequestInternal, opts ...grpc.CallOption) (*internalpb.ListImportsResponse, error) {
	return &internalpb.ListImportsResponse{
		Status: merr.Success(),
	}, nil
}

func (coord *MixCoordMock) DropIndex(ctx context.Context, req *indexpb.DropIndexRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return merr.Success(), nil
}

func (coord *MixCoordMock) GetIndexState(ctx context.Context, req *indexpb.GetIndexStateRequest, opts ...grpc.CallOption) (*indexpb.GetIndexStateResponse, error) {
	if coord.GetIndexStateFunc != nil {
		return coord.GetIndexStateFunc(ctx, req, opts...)
	}
	return &indexpb.GetIndexStateResponse{
		Status:     merr.Success(),
		State:      commonpb.IndexState_Finished,
		FailReason: "",
	}, nil
}

// GetSegmentIndexState gets the index state of the segments in the request from RootCoord.
func (coord *MixCoordMock) GetSegmentIndexState(ctx context.Context, req *indexpb.GetSegmentIndexStateRequest, opts ...grpc.CallOption) (*indexpb.GetSegmentIndexStateResponse, error) {
	return &indexpb.GetSegmentIndexStateResponse{
		Status: merr.Success(),
	}, nil
}

// GetIndexInfos gets the index files of the IndexBuildIDs in the request from RootCoordinator.
func (coord *MixCoordMock) GetIndexInfos(ctx context.Context, req *indexpb.GetIndexInfoRequest, opts ...grpc.CallOption) (*indexpb.GetIndexInfoResponse, error) {
	return &indexpb.GetIndexInfoResponse{
		Status: merr.Success(),
	}, nil
}

// DescribeIndex describe the index info of the collection.
func (coord *MixCoordMock) DescribeIndex(ctx context.Context, req *indexpb.DescribeIndexRequest, opts ...grpc.CallOption) (*indexpb.DescribeIndexResponse, error) {
	if coord.DescribeIndexFunc != nil {
		return coord.DescribeIndexFunc(ctx, req, opts...)
	}
	return &indexpb.DescribeIndexResponse{
		Status:     merr.Success(),
		IndexInfos: nil,
	}, nil
}

// GetIndexStatistics get the statistics of the index.
func (coord *MixCoordMock) GetIndexStatistics(ctx context.Context, req *indexpb.GetIndexStatisticsRequest, opts ...grpc.CallOption) (*indexpb.GetIndexStatisticsResponse, error) {
	return &indexpb.GetIndexStatisticsResponse{
		Status:     merr.Success(),
		IndexInfos: nil,
	}, nil
}

// GetIndexBuildProgress get the index building progress by num rows.
func (coord *MixCoordMock) GetIndexBuildProgress(ctx context.Context, req *indexpb.GetIndexBuildProgressRequest, opts ...grpc.CallOption) (*indexpb.GetIndexBuildProgressResponse, error) {
	return &indexpb.GetIndexBuildProgressResponse{
		Status: merr.Success(),
	}, nil
}

func (coord *MixCoordMock) ShowLoadCollections(ctx context.Context, in *querypb.ShowCollectionsRequest, opts ...grpc.CallOption) (*querypb.ShowCollectionsResponse, error) {
	if coord.ShowLoadCollectionsFunc != nil {
		return coord.ShowLoadCollectionsFunc(ctx, in)
	}
	return &querypb.ShowCollectionsResponse{
		Status: merr.Success(),
	}, nil
}

func (coord *MixCoordMock) ShowLoadPartitions(ctx context.Context, in *querypb.ShowPartitionsRequest, opts ...grpc.CallOption) (*querypb.ShowPartitionsResponse, error) {
	if coord.ShowLoadPartitionsFunc != nil {
		return coord.ShowLoadPartitionsFunc(ctx, in)
	}
	return &querypb.ShowPartitionsResponse{
		Status: merr.Success(),
	}, nil
}

func (coord *MixCoordMock) LoadPartitions(ctx context.Context, in *querypb.LoadPartitionsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return merr.Success(), nil
}

func (coord *MixCoordMock) ReleasePartitions(ctx context.Context, in *querypb.ReleasePartitionsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return merr.Success(), nil
}

func (coord *MixCoordMock) LoadCollection(ctx context.Context, in *querypb.LoadCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return merr.Success(), nil
}

func (coord *MixCoordMock) ReleaseCollection(ctx context.Context, in *querypb.ReleaseCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return merr.Success(), nil
}

func (coord *MixCoordMock) SyncNewCreatedPartition(ctx context.Context, in *querypb.SyncNewCreatedPartitionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return merr.Success(), nil
}

func (coord *MixCoordMock) GetPartitionStates(ctx context.Context, in *querypb.GetPartitionStatesRequest, opts ...grpc.CallOption) (*querypb.GetPartitionStatesResponse, error) {
	return &querypb.GetPartitionStatesResponse{
		Status: merr.Success(),
	}, nil
}

func (coord *MixCoordMock) GetLoadSegmentInfo(ctx context.Context, in *querypb.GetSegmentInfoRequest, opts ...grpc.CallOption) (*querypb.GetSegmentInfoResponse, error) {
	return &querypb.GetSegmentInfoResponse{
		Status: merr.Success(),
	}, nil
}

func (coord *MixCoordMock) LoadBalance(ctx context.Context, in *querypb.LoadBalanceRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return merr.Success(), nil
}

func (coord *MixCoordMock) GetReplicas(ctx context.Context, in *milvuspb.GetReplicasRequest, opts ...grpc.CallOption) (*milvuspb.GetReplicasResponse, error) {
	return &milvuspb.GetReplicasResponse{
		Status: merr.Success(),
	}, nil
}

func (coord *MixCoordMock) GetShardLeaders(ctx context.Context, in *querypb.GetShardLeadersRequest, opts ...grpc.CallOption) (*querypb.GetShardLeadersResponse, error) {
	if coord.GetShardLeadersFunc != nil {
		return coord.GetShardLeadersFunc(ctx, in)
	}
	return &querypb.GetShardLeadersResponse{
		Status: merr.Success(),
		Shards: []*querypb.ShardLeadersList{
			{
				ChannelName: "channel-1",
				NodeIds:     []int64{1, 2, 3},
				NodeAddrs:   []string{"localhost:9000", "localhost:9001", "localhost:9002"},
			},
		},
	}, nil
}

func (coord *MixCoordMock) CreateResourceGroup(ctx context.Context, in *milvuspb.CreateResourceGroupRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return merr.Success(), nil
}

func (coord *MixCoordMock) UpdateResourceGroups(ctx context.Context, in *querypb.UpdateResourceGroupsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return merr.Success(), nil
}

func (coord *MixCoordMock) DropResourceGroup(ctx context.Context, in *milvuspb.DropResourceGroupRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return merr.Success(), nil
}

func (coord *MixCoordMock) TransferNode(ctx context.Context, in *milvuspb.TransferNodeRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return merr.Success(), nil
}

func (coord *MixCoordMock) TransferReplica(ctx context.Context, in *querypb.TransferReplicaRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return merr.Success(), nil
}

func (coord *MixCoordMock) ListResourceGroups(ctx context.Context, in *milvuspb.ListResourceGroupsRequest, opts ...grpc.CallOption) (*milvuspb.ListResourceGroupsResponse, error) {
	return &milvuspb.ListResourceGroupsResponse{
		Status: merr.Success(),
	}, nil
}

func (coord *MixCoordMock) DescribeResourceGroup(ctx context.Context, in *querypb.DescribeResourceGroupRequest, opts ...grpc.CallOption) (*querypb.DescribeResourceGroupResponse, error) {
	return &querypb.DescribeResourceGroupResponse{
		Status: merr.Success(),
	}, nil
}

// ops interfaces
func (coord *MixCoordMock) ListCheckers(ctx context.Context, in *querypb.ListCheckersRequest, opts ...grpc.CallOption) (*querypb.ListCheckersResponse, error) {
	return &querypb.ListCheckersResponse{
		Status: merr.Success(),
	}, nil
}

func (coord *MixCoordMock) ActivateChecker(ctx context.Context, in *querypb.ActivateCheckerRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return merr.Success(), nil
}

func (coord *MixCoordMock) DeactivateChecker(ctx context.Context, in *querypb.DeactivateCheckerRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return merr.Success(), nil
}

func (coord *MixCoordMock) ListQueryNode(ctx context.Context, in *querypb.ListQueryNodeRequest, opts ...grpc.CallOption) (*querypb.ListQueryNodeResponse, error) {
	return &querypb.ListQueryNodeResponse{
		Status: merr.Success(),
	}, nil
}

func (coord *MixCoordMock) GetQueryNodeDistribution(ctx context.Context, in *querypb.GetQueryNodeDistributionRequest, opts ...grpc.CallOption) (*querypb.GetQueryNodeDistributionResponse, error) {
	return &querypb.GetQueryNodeDistributionResponse{
		Status: merr.Success(),
	}, nil
}

func (coord *MixCoordMock) SuspendBalance(ctx context.Context, in *querypb.SuspendBalanceRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return merr.Success(), nil
}

func (coord *MixCoordMock) ResumeBalance(ctx context.Context, in *querypb.ResumeBalanceRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return merr.Success(), nil
}

func (coord *MixCoordMock) CheckBalanceStatus(ctx context.Context, in *querypb.CheckBalanceStatusRequest, opts ...grpc.CallOption) (*querypb.CheckBalanceStatusResponse, error) {
	return &querypb.CheckBalanceStatusResponse{
		Status: merr.Success(),
	}, nil
}

func (coord *MixCoordMock) SuspendNode(ctx context.Context, in *querypb.SuspendNodeRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return merr.Success(), nil
}

func (coord *MixCoordMock) ResumeNode(ctx context.Context, in *querypb.ResumeNodeRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return merr.Success(), nil
}

func (coord *MixCoordMock) TransferSegment(ctx context.Context, in *querypb.TransferSegmentRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return merr.Success(), nil
}

func (coord *MixCoordMock) TransferChannel(ctx context.Context, in *querypb.TransferChannelRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return merr.Success(), nil
}

func (coord *MixCoordMock) CheckQueryNodeDistribution(ctx context.Context, in *querypb.CheckQueryNodeDistributionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return merr.Success(), nil
}

func (coord *MixCoordMock) UpdateLoadConfig(ctx context.Context, in *querypb.UpdateLoadConfigRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return merr.Success(), nil
}

func (coord *MixCoordMock) GetRecoveryInfoV2(ctx context.Context, in *datapb.GetRecoveryInfoRequestV2, opts ...grpc.CallOption) (*datapb.GetRecoveryInfoResponseV2, error) {
	return &datapb.GetRecoveryInfoResponseV2{}, nil
}

func (coord *MixCoordMock) Search() {
}

type DescribeCollectionFunc func(ctx context.Context, request *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error)

type ShowPartitionsFunc func(ctx context.Context, request *milvuspb.ShowPartitionsRequest, opts ...grpc.CallOption) (*milvuspb.ShowPartitionsResponse, error)

type ShowSegmentsFunc func(ctx context.Context, request *milvuspb.ShowSegmentsRequest, opts ...grpc.CallOption) (*milvuspb.ShowSegmentsResponse, error)

type DescribeSegmentsFunc func(ctx context.Context, request *rootcoordpb.DescribeSegmentsRequest, opts ...grpc.CallOption) (*rootcoordpb.DescribeSegmentsResponse, error)

type ImportFunc func(ctx context.Context, req *milvuspb.ImportRequest, opts ...grpc.CallOption) (*milvuspb.ImportResponse, error)

type DropCollectionFunc func(ctx context.Context, request *milvuspb.DropCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error)

type GetGetCredentialFunc func(ctx context.Context, req *rootcoordpb.GetCredentialRequest, opts ...grpc.CallOption) (*rootcoordpb.GetCredentialResponse, error)
