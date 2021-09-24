// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package msgstream

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
)

func TestBaseMsg(t *testing.T) {
	ctx := context.Background()
	baseMsg := &BaseMsg{
		Ctx:            ctx,
		BeginTimestamp: Timestamp(0),
		EndTimestamp:   Timestamp(1),
		HashValues:     []uint32{2},
		MsgPosition:    nil,
	}

	position := &MsgPosition{
		ChannelName: "test-channel",
		MsgID:       []byte{},
		MsgGroup:    "test-group",
		Timestamp:   0,
	}

	assert.Equal(t, Timestamp(0), baseMsg.BeginTs())
	assert.Equal(t, Timestamp(1), baseMsg.EndTs())
	assert.Equal(t, []uint32{2}, baseMsg.HashKeys())
	assert.Equal(t, (*MsgPosition)(nil), baseMsg.Position())

	baseMsg.SetPosition(position)
	assert.Equal(t, position, baseMsg.Position())
}

func Test_ConvertToByteArray(t *testing.T) {
	{
		bytes := []byte{1, 2, 3}
		byteArray, err := ConvertToByteArray(bytes)
		assert.Equal(t, bytes, byteArray)
		assert.Nil(t, err)
	}

	{
		bytes := 4
		byteArray, err := ConvertToByteArray(bytes)
		assert.Equal(t, ([]byte)(nil), byteArray)
		assert.NotNil(t, err)
	}
}

func generateBaseMsg() BaseMsg {
	ctx := context.Background()
	return BaseMsg{
		Ctx:            ctx,
		BeginTimestamp: Timestamp(0),
		EndTimestamp:   Timestamp(1),
		HashValues:     []uint32{2},
		MsgPosition:    nil,
	}
}

func TestInsertMsg(t *testing.T) {
	insertMsg := &InsertMsg{
		BaseMsg: generateBaseMsg(),
		InsertRequest: internalpb.InsertRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Insert,
				MsgID:     1,
				Timestamp: 2,
				SourceID:  3,
			},

			DbName:         "test_db",
			CollectionName: "test_collection",
			PartitionName:  "test_partition",
			DbID:           4,
			CollectionID:   5,
			PartitionID:    6,
			SegmentID:      7,
			ChannelID:      "test-channel",
			Timestamps:     []uint64{2, 1, 3},
			RowData:        []*commonpb.Blob{},
		},
	}

	assert.NotNil(t, insertMsg.TraceCtx())

	ctx := context.Background()
	insertMsg.SetTraceCtx(ctx)
	assert.Equal(t, ctx, insertMsg.TraceCtx())

	assert.Equal(t, int64(1), insertMsg.ID())
	assert.Equal(t, commonpb.MsgType_Insert, insertMsg.Type())
	assert.Equal(t, int64(3), insertMsg.SourceID())

	bytes, err := insertMsg.Marshal(insertMsg)
	assert.Nil(t, err)

	tsMsg, err := insertMsg.Unmarshal(bytes)
	assert.Nil(t, err)

	insertMsg2, ok := tsMsg.(*InsertMsg)
	assert.True(t, ok)
	assert.Equal(t, int64(1), insertMsg2.ID())
	assert.Equal(t, commonpb.MsgType_Insert, insertMsg2.Type())
	assert.Equal(t, int64(3), insertMsg2.SourceID())
}

func TestInsertMsg_Unmarshal_IllegalParameter(t *testing.T) {
	insertMsg := &InsertMsg{}
	tsMsg, err := insertMsg.Unmarshal(10)
	assert.NotNil(t, err)
	assert.Nil(t, tsMsg)
}

func TestDeleteMsg(t *testing.T) {
	deleteMsg := &DeleteMsg{
		BaseMsg: generateBaseMsg(),
		DeleteRequest: internalpb.DeleteRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Delete,
				MsgID:     1,
				Timestamp: 2,
				SourceID:  3,
			},

			CollectionName: "test_collection",
			ChannelID:      "test-channel",
			Timestamp:      uint64(1),
		},
	}

	assert.NotNil(t, deleteMsg.TraceCtx())

	ctx := context.Background()
	deleteMsg.SetTraceCtx(ctx)
	assert.Equal(t, ctx, deleteMsg.TraceCtx())

	assert.Equal(t, int64(1), deleteMsg.ID())
	assert.Equal(t, commonpb.MsgType_Delete, deleteMsg.Type())
	assert.Equal(t, int64(3), deleteMsg.SourceID())

	bytes, err := deleteMsg.Marshal(deleteMsg)
	assert.Nil(t, err)

	tsMsg, err := deleteMsg.Unmarshal(bytes)
	assert.Nil(t, err)

	deleteMsg2, ok := tsMsg.(*DeleteMsg)
	assert.True(t, ok)
	assert.Equal(t, int64(1), deleteMsg2.ID())
	assert.Equal(t, commonpb.MsgType_Delete, deleteMsg2.Type())
	assert.Equal(t, int64(3), deleteMsg2.SourceID())
}

func TestDeleteMsg_Unmarshal_IllegalParameter(t *testing.T) {
	deleteMsg := &DeleteMsg{}
	tsMsg, err := deleteMsg.Unmarshal(10)
	assert.NotNil(t, err)
	assert.Nil(t, tsMsg)
}

func TestSearchMsg(t *testing.T) {
	searchMsg := &SearchMsg{
		BaseMsg: generateBaseMsg(),
		SearchRequest: internalpb.SearchRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Search,
				MsgID:     1,
				Timestamp: 2,
				SourceID:  3,
			},
			ResultChannelID:    "test-channel",
			DbID:               4,
			CollectionID:       5,
			PartitionIDs:       []int64{},
			Dsl:                "dsl",
			PlaceholderGroup:   []byte{},
			DslType:            commonpb.DslType_BoolExprV1,
			SerializedExprPlan: []byte{},
			OutputFieldsId:     []int64{},
			TravelTimestamp:    6,
			GuaranteeTimestamp: 7,
		},
	}

	assert.NotNil(t, searchMsg.TraceCtx())

	ctx := context.Background()
	searchMsg.SetTraceCtx(ctx)
	assert.Equal(t, ctx, searchMsg.TraceCtx())

	assert.Equal(t, int64(1), searchMsg.ID())
	assert.Equal(t, commonpb.MsgType_Search, searchMsg.Type())
	assert.Equal(t, int64(3), searchMsg.SourceID())
	assert.Equal(t, uint64(7), searchMsg.GuaranteeTs())
	assert.Equal(t, uint64(6), searchMsg.TravelTs())

	bytes, err := searchMsg.Marshal(searchMsg)
	assert.Nil(t, err)

	tsMsg, err := searchMsg.Unmarshal(bytes)
	assert.Nil(t, err)

	searchMsg2, ok := tsMsg.(*SearchMsg)
	assert.True(t, ok)
	assert.Equal(t, int64(1), searchMsg2.ID())
	assert.Equal(t, commonpb.MsgType_Search, searchMsg2.Type())
	assert.Equal(t, int64(3), searchMsg2.SourceID())
	assert.Equal(t, uint64(7), searchMsg2.GuaranteeTs())
	assert.Equal(t, uint64(6), searchMsg2.TravelTs())
}

func TestSearchMsg_Unmarshal_IllegalParameter(t *testing.T) {
	searchMsg := &SearchMsg{}
	tsMsg, err := searchMsg.Unmarshal(10)
	assert.NotNil(t, err)
	assert.Nil(t, tsMsg)
}

func TestSearchResultMsg(t *testing.T) {
	searchResultMsg := &SearchResultMsg{
		BaseMsg: generateBaseMsg(),
		SearchResults: internalpb.SearchResults{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_SearchResult,
				MsgID:     1,
				Timestamp: 2,
				SourceID:  3,
			},
			ResultChannelID:          "test-channel",
			MetricType:               "l2",
			NumQueries:               5,
			TopK:                     6,
			SealedSegmentIDsSearched: []int64{7},
			ChannelIDsSearched:       []string{"test-searched"},
			GlobalSealedSegmentIDs:   []int64{8},
		},
	}

	assert.NotNil(t, searchResultMsg.TraceCtx())

	ctx := context.Background()
	searchResultMsg.SetTraceCtx(ctx)
	assert.Equal(t, ctx, searchResultMsg.TraceCtx())

	assert.Equal(t, int64(1), searchResultMsg.ID())
	assert.Equal(t, commonpb.MsgType_SearchResult, searchResultMsg.Type())
	assert.Equal(t, int64(3), searchResultMsg.SourceID())

	bytes, err := searchResultMsg.Marshal(searchResultMsg)
	assert.Nil(t, err)

	tsMsg, err := searchResultMsg.Unmarshal(bytes)
	assert.Nil(t, err)

	searchResultMsg2, ok := tsMsg.(*SearchResultMsg)
	assert.True(t, ok)
	assert.Equal(t, int64(1), searchResultMsg2.ID())
	assert.Equal(t, commonpb.MsgType_SearchResult, searchResultMsg2.Type())
	assert.Equal(t, int64(3), searchResultMsg2.SourceID())
}

func TestSearchResultMsg_Unmarshal_IllegalParameter(t *testing.T) {
	searchResultMsg := &SearchResultMsg{}
	tsMsg, err := searchResultMsg.Unmarshal(10)
	assert.NotNil(t, err)
	assert.Nil(t, tsMsg)
}

func TestRetrieveMsg(t *testing.T) {
	retrieveMsg := &RetrieveMsg{
		BaseMsg: generateBaseMsg(),
		RetrieveRequest: internalpb.RetrieveRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Retrieve,
				MsgID:     1,
				Timestamp: 2,
				SourceID:  3,
			},
			ResultChannelID:    "test-channel",
			DbID:               5,
			CollectionID:       6,
			PartitionIDs:       []int64{7, 8},
			SerializedExprPlan: []byte{},
			OutputFieldsId:     []int64{8, 9},
			TravelTimestamp:    10,
			GuaranteeTimestamp: 11,
		},
	}

	assert.NotNil(t, retrieveMsg.TraceCtx())

	ctx := context.Background()
	retrieveMsg.SetTraceCtx(ctx)
	assert.Equal(t, ctx, retrieveMsg.TraceCtx())

	assert.Equal(t, int64(1), retrieveMsg.ID())
	assert.Equal(t, commonpb.MsgType_Retrieve, retrieveMsg.Type())
	assert.Equal(t, int64(3), retrieveMsg.SourceID())
	assert.Equal(t, uint64(11), retrieveMsg.GuaranteeTs())
	assert.Equal(t, uint64(10), retrieveMsg.TravelTs())

	bytes, err := retrieveMsg.Marshal(retrieveMsg)
	assert.Nil(t, err)

	tsMsg, err := retrieveMsg.Unmarshal(bytes)
	assert.Nil(t, err)

	retrieveMsg2, ok := tsMsg.(*RetrieveMsg)
	assert.True(t, ok)
	assert.Equal(t, int64(1), retrieveMsg2.ID())
	assert.Equal(t, commonpb.MsgType_Retrieve, retrieveMsg2.Type())
	assert.Equal(t, int64(3), retrieveMsg2.SourceID())
	assert.Equal(t, uint64(11), retrieveMsg2.GuaranteeTs())
	assert.Equal(t, uint64(10), retrieveMsg2.TravelTs())
}

func TestRetrieveMsg_Unmarshal_IllegalParameter(t *testing.T) {
	retrieveMsg := &RetrieveMsg{}
	tsMsg, err := retrieveMsg.Unmarshal(10)
	assert.NotNil(t, err)
	assert.Nil(t, tsMsg)
}

func TestRetrieveResultMsg(t *testing.T) {
	retrieveResultMsg := &RetrieveResultMsg{
		BaseMsg: generateBaseMsg(),
		RetrieveResults: internalpb.RetrieveResults{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_RetrieveResult,
				MsgID:     1,
				Timestamp: 2,
				SourceID:  3,
			},
			ResultChannelID: "test-channel",
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{
						Data: []int64{},
					},
				},
			},
			FieldsData: []*schemapb.FieldData{
				{
					Type:      schemapb.DataType_FloatVector,
					FieldName: "vector_field",
					Field: &schemapb.FieldData_Vectors{
						Vectors: &schemapb.VectorField{
							Dim: 4,
							Data: &schemapb.VectorField_FloatVector{
								FloatVector: &schemapb.FloatArray{
									Data: []float32{1.1, 2.2, 3.3, 4.4},
								},
							},
						},
					},
					FieldId: 5,
				},
			},
			SealedSegmentIDsRetrieved: []int64{6, 7},
			ChannelIDsRetrieved:       []string{"test-retrieved-channel"},
			GlobalSealedSegmentIDs:    []int64{8, 9},
		},
	}

	assert.NotNil(t, retrieveResultMsg.TraceCtx())

	ctx := context.Background()
	retrieveResultMsg.SetTraceCtx(ctx)
	assert.Equal(t, ctx, retrieveResultMsg.TraceCtx())

	assert.Equal(t, int64(1), retrieveResultMsg.ID())
	assert.Equal(t, commonpb.MsgType_RetrieveResult, retrieveResultMsg.Type())
	assert.Equal(t, int64(3), retrieveResultMsg.SourceID())

	bytes, err := retrieveResultMsg.Marshal(retrieveResultMsg)
	assert.Nil(t, err)

	tsMsg, err := retrieveResultMsg.Unmarshal(bytes)
	assert.Nil(t, err)

	retrieveResultMsg2, ok := tsMsg.(*RetrieveResultMsg)
	assert.True(t, ok)
	assert.Equal(t, int64(1), retrieveResultMsg2.ID())
	assert.Equal(t, commonpb.MsgType_RetrieveResult, retrieveResultMsg2.Type())
	assert.Equal(t, int64(3), retrieveResultMsg2.SourceID())
}

func TestRetrieveResultMsg_Unmarshal_IllegalParameter(t *testing.T) {
	retrieveResultMsg := &RetrieveResultMsg{}
	tsMsg, err := retrieveResultMsg.Unmarshal(10)
	assert.NotNil(t, err)
	assert.Nil(t, tsMsg)
}

func TestTimeTickMsg(t *testing.T) {
	timeTickMsg := &TimeTickMsg{
		BaseMsg: generateBaseMsg(),
		TimeTickMsg: internalpb.TimeTickMsg{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_TimeTick,
				MsgID:     1,
				Timestamp: 2,
				SourceID:  3,
			},
		},
	}

	assert.NotNil(t, timeTickMsg.TraceCtx())

	ctx := context.Background()
	timeTickMsg.SetTraceCtx(ctx)
	assert.Equal(t, ctx, timeTickMsg.TraceCtx())

	assert.Equal(t, int64(1), timeTickMsg.ID())
	assert.Equal(t, commonpb.MsgType_TimeTick, timeTickMsg.Type())
	assert.Equal(t, int64(3), timeTickMsg.SourceID())

	bytes, err := timeTickMsg.Marshal(timeTickMsg)
	assert.Nil(t, err)

	tsMsg, err := timeTickMsg.Unmarshal(bytes)
	assert.Nil(t, err)

	timeTickMsg2, ok := tsMsg.(*TimeTickMsg)
	assert.True(t, ok)
	assert.Equal(t, int64(1), timeTickMsg2.ID())
	assert.Equal(t, commonpb.MsgType_TimeTick, timeTickMsg2.Type())
	assert.Equal(t, int64(3), timeTickMsg2.SourceID())
}

func TestTimeTickMsg_Unmarshal_IllegalParameter(t *testing.T) {
	timeTickMsg := &TimeTickMsg{}
	tsMsg, err := timeTickMsg.Unmarshal(10)
	assert.NotNil(t, err)
	assert.Nil(t, tsMsg)
}

func TestQueryNodeStatsMsg(t *testing.T) {
	queryNodeStatsMsg := &QueryNodeStatsMsg{
		BaseMsg: generateBaseMsg(),
		QueryNodeStats: internalpb.QueryNodeStats{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_TimeTick,
				MsgID:     1,
				Timestamp: 2,
				SourceID:  3,
			},
			SegStats:   []*internalpb.SegmentStats{},
			FieldStats: []*internalpb.FieldStats{},
		},
	}

	assert.NotNil(t, queryNodeStatsMsg.TraceCtx())

	ctx := context.Background()
	queryNodeStatsMsg.SetTraceCtx(ctx)
	assert.Equal(t, ctx, queryNodeStatsMsg.TraceCtx())

	assert.Equal(t, int64(1), queryNodeStatsMsg.ID())
	assert.Equal(t, commonpb.MsgType_TimeTick, queryNodeStatsMsg.Type())
	assert.Equal(t, int64(3), queryNodeStatsMsg.SourceID())

	bytes, err := queryNodeStatsMsg.Marshal(queryNodeStatsMsg)
	assert.Nil(t, err)

	tsMsg, err := queryNodeStatsMsg.Unmarshal(bytes)
	assert.Nil(t, err)

	queryNodeStatsMsg2, ok := tsMsg.(*QueryNodeStatsMsg)
	assert.True(t, ok)
	assert.Equal(t, int64(1), queryNodeStatsMsg2.ID())
	assert.Equal(t, commonpb.MsgType_TimeTick, queryNodeStatsMsg2.Type())
	assert.Equal(t, int64(3), queryNodeStatsMsg2.SourceID())
}

func TestQueryNodeStatsMsg_Unmarshal_IllegalParameter(t *testing.T) {
	queryNodeStatsMsg := &QueryNodeStatsMsg{}
	tsMsg, err := queryNodeStatsMsg.Unmarshal(10)
	assert.NotNil(t, err)
	assert.Nil(t, tsMsg)
}

func TestSegmentStatisticsMsg(t *testing.T) {
	segmentStatisticsMsg := &SegmentStatisticsMsg{
		BaseMsg: generateBaseMsg(),
		SegmentStatistics: internalpb.SegmentStatistics{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_SegmentStatistics,
				MsgID:     1,
				Timestamp: 2,
				SourceID:  3,
			},
			SegStats: []*internalpb.SegmentStatisticsUpdates{},
		},
	}

	assert.NotNil(t, segmentStatisticsMsg.TraceCtx())

	ctx := context.Background()
	segmentStatisticsMsg.SetTraceCtx(ctx)
	assert.Equal(t, ctx, segmentStatisticsMsg.TraceCtx())

	assert.Equal(t, int64(1), segmentStatisticsMsg.ID())
	assert.Equal(t, commonpb.MsgType_SegmentStatistics, segmentStatisticsMsg.Type())
	assert.Equal(t, int64(3), segmentStatisticsMsg.SourceID())

	bytes, err := segmentStatisticsMsg.Marshal(segmentStatisticsMsg)
	assert.Nil(t, err)

	tsMsg, err := segmentStatisticsMsg.Unmarshal(bytes)
	assert.Nil(t, err)

	segmentStatisticsMsg2, ok := tsMsg.(*SegmentStatisticsMsg)
	assert.True(t, ok)
	assert.Equal(t, int64(1), segmentStatisticsMsg2.ID())
	assert.Equal(t, commonpb.MsgType_SegmentStatistics, segmentStatisticsMsg2.Type())
	assert.Equal(t, int64(3), segmentStatisticsMsg2.SourceID())
}

func TestSegmentStatisticsMsg_Unmarshal_IllegalParameter(t *testing.T) {
	segmentStatisticsMsg := &SegmentStatisticsMsg{}
	tsMsg, err := segmentStatisticsMsg.Unmarshal(10)
	assert.NotNil(t, err)
	assert.Nil(t, tsMsg)
}

func TestCreateCollectionMsg(t *testing.T) {
	createCollectionMsg := &CreateCollectionMsg{
		BaseMsg: generateBaseMsg(),
		CreateCollectionRequest: internalpb.CreateCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_CreateCollection,
				MsgID:     1,
				Timestamp: 2,
				SourceID:  3,
			},
			DbName:               "test_db",
			CollectionName:       "test_collection",
			PartitionName:        "test_partition",
			DbID:                 4,
			CollectionID:         5,
			PartitionID:          6,
			Schema:               []byte{},
			VirtualChannelNames:  []string{},
			PhysicalChannelNames: []string{},
		},
	}

	assert.NotNil(t, createCollectionMsg.TraceCtx())

	ctx := context.Background()
	createCollectionMsg.SetTraceCtx(ctx)
	assert.Equal(t, ctx, createCollectionMsg.TraceCtx())

	assert.Equal(t, int64(1), createCollectionMsg.ID())
	assert.Equal(t, commonpb.MsgType_CreateCollection, createCollectionMsg.Type())
	assert.Equal(t, int64(3), createCollectionMsg.SourceID())

	bytes, err := createCollectionMsg.Marshal(createCollectionMsg)
	assert.Nil(t, err)

	tsMsg, err := createCollectionMsg.Unmarshal(bytes)
	assert.Nil(t, err)

	createCollectionMsg2, ok := tsMsg.(*CreateCollectionMsg)
	assert.True(t, ok)
	assert.Equal(t, int64(1), createCollectionMsg2.ID())
	assert.Equal(t, commonpb.MsgType_CreateCollection, createCollectionMsg2.Type())
	assert.Equal(t, int64(3), createCollectionMsg2.SourceID())
}

func TestCreateCollectionMsg_Unmarshal_IllegalParameter(t *testing.T) {
	createCollectionMsg := &CreateCollectionMsg{}
	tsMsg, err := createCollectionMsg.Unmarshal(10)
	assert.NotNil(t, err)
	assert.Nil(t, tsMsg)
}

func TestDropCollectionMsg(t *testing.T) {
	dropCollectionMsg := &DropCollectionMsg{
		BaseMsg: generateBaseMsg(),
		DropCollectionRequest: internalpb.DropCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DropCollection,
				MsgID:     1,
				Timestamp: 2,
				SourceID:  3,
			},
			DbName:         "test_db",
			CollectionName: "test_collection",
			DbID:           4,
			CollectionID:   5,
		},
	}

	assert.NotNil(t, dropCollectionMsg.TraceCtx())

	ctx := context.Background()
	dropCollectionMsg.SetTraceCtx(ctx)
	assert.Equal(t, ctx, dropCollectionMsg.TraceCtx())

	assert.Equal(t, int64(1), dropCollectionMsg.ID())
	assert.Equal(t, commonpb.MsgType_DropCollection, dropCollectionMsg.Type())
	assert.Equal(t, int64(3), dropCollectionMsg.SourceID())

	bytes, err := dropCollectionMsg.Marshal(dropCollectionMsg)
	assert.Nil(t, err)

	tsMsg, err := dropCollectionMsg.Unmarshal(bytes)
	assert.Nil(t, err)

	dropCollectionMsg2, ok := tsMsg.(*DropCollectionMsg)
	assert.True(t, ok)
	assert.Equal(t, int64(1), dropCollectionMsg2.ID())
	assert.Equal(t, commonpb.MsgType_DropCollection, dropCollectionMsg2.Type())
	assert.Equal(t, int64(3), dropCollectionMsg2.SourceID())
}

func TestDropCollectionMsg_Unmarshal_IllegalParameter(t *testing.T) {
	dropCollectionMsg := &DropCollectionMsg{}
	tsMsg, err := dropCollectionMsg.Unmarshal(10)
	assert.NotNil(t, err)
	assert.Nil(t, tsMsg)
}

func TestCreatePartitionMsg(t *testing.T) {
	createPartitionMsg := &CreatePartitionMsg{
		BaseMsg: generateBaseMsg(),
		CreatePartitionRequest: internalpb.CreatePartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_CreatePartition,
				MsgID:     1,
				Timestamp: 2,
				SourceID:  3,
			},
			DbName:         "test_db",
			CollectionName: "test_collection",
			PartitionName:  "test_partition",
			DbID:           4,
			CollectionID:   5,
			PartitionID:    6,
		},
	}

	assert.NotNil(t, createPartitionMsg.TraceCtx())

	ctx := context.Background()
	createPartitionMsg.SetTraceCtx(ctx)
	assert.Equal(t, ctx, createPartitionMsg.TraceCtx())

	assert.Equal(t, int64(1), createPartitionMsg.ID())
	assert.Equal(t, commonpb.MsgType_CreatePartition, createPartitionMsg.Type())
	assert.Equal(t, int64(3), createPartitionMsg.SourceID())

	bytes, err := createPartitionMsg.Marshal(createPartitionMsg)
	assert.Nil(t, err)

	tsMsg, err := createPartitionMsg.Unmarshal(bytes)
	assert.Nil(t, err)

	createPartitionMsg2, ok := tsMsg.(*CreatePartitionMsg)
	assert.True(t, ok)
	assert.Equal(t, int64(1), createPartitionMsg2.ID())
	assert.Equal(t, commonpb.MsgType_CreatePartition, createPartitionMsg2.Type())
	assert.Equal(t, int64(3), createPartitionMsg2.SourceID())
}

func TestCreatePartitionMsg_Unmarshal_IllegalParameter(t *testing.T) {
	createPartitionMsg := &CreatePartitionMsg{}
	tsMsg, err := createPartitionMsg.Unmarshal(10)
	assert.NotNil(t, err)
	assert.Nil(t, tsMsg)
}

func TestDropPartitionMsg(t *testing.T) {
	dropPartitionMsg := &DropPartitionMsg{
		BaseMsg: generateBaseMsg(),
		DropPartitionRequest: internalpb.DropPartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DropPartition,
				MsgID:     1,
				Timestamp: 2,
				SourceID:  3,
			},
			DbName:         "test_db",
			CollectionName: "test_collection",
			PartitionName:  "test_partition",
			DbID:           4,
			CollectionID:   5,
			PartitionID:    6,
		},
	}

	assert.NotNil(t, dropPartitionMsg.TraceCtx())

	ctx := context.Background()
	dropPartitionMsg.SetTraceCtx(ctx)
	assert.Equal(t, ctx, dropPartitionMsg.TraceCtx())

	assert.Equal(t, int64(1), dropPartitionMsg.ID())
	assert.Equal(t, commonpb.MsgType_DropPartition, dropPartitionMsg.Type())
	assert.Equal(t, int64(3), dropPartitionMsg.SourceID())

	bytes, err := dropPartitionMsg.Marshal(dropPartitionMsg)
	assert.Nil(t, err)

	tsMsg, err := dropPartitionMsg.Unmarshal(bytes)
	assert.Nil(t, err)

	dropPartitionMsg2, ok := tsMsg.(*DropPartitionMsg)
	assert.True(t, ok)
	assert.Equal(t, int64(1), dropPartitionMsg2.ID())
	assert.Equal(t, commonpb.MsgType_DropPartition, dropPartitionMsg2.Type())
	assert.Equal(t, int64(3), dropPartitionMsg2.SourceID())
}

func TestDropPartitionMsg_Unmarshal_IllegalParameter(t *testing.T) {
	dropPartitionMsg := &DropPartitionMsg{}
	tsMsg, err := dropPartitionMsg.Unmarshal(10)
	assert.NotNil(t, err)
	assert.Nil(t, tsMsg)
}

func TestLoadBalanceSegmentsMsg(t *testing.T) {
	loadBalanceSegmentsMsg := &LoadBalanceSegmentsMsg{
		BaseMsg: generateBaseMsg(),
		LoadBalanceSegmentsRequest: internalpb.LoadBalanceSegmentsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_LoadBalanceSegments,
				MsgID:     1,
				Timestamp: 2,
				SourceID:  3,
			},
			SegmentIDs: []int64{},
		},
	}

	assert.NotNil(t, loadBalanceSegmentsMsg.TraceCtx())

	ctx := context.Background()
	loadBalanceSegmentsMsg.SetTraceCtx(ctx)
	assert.Equal(t, ctx, loadBalanceSegmentsMsg.TraceCtx())

	assert.Equal(t, int64(1), loadBalanceSegmentsMsg.ID())
	assert.Equal(t, commonpb.MsgType_LoadBalanceSegments, loadBalanceSegmentsMsg.Type())
	assert.Equal(t, int64(3), loadBalanceSegmentsMsg.SourceID())

	bytes, err := loadBalanceSegmentsMsg.Marshal(loadBalanceSegmentsMsg)
	assert.Nil(t, err)

	tsMsg, err := loadBalanceSegmentsMsg.Unmarshal(bytes)
	assert.Nil(t, err)

	loadBalanceSegmentsMsg2, ok := tsMsg.(*LoadBalanceSegmentsMsg)
	assert.True(t, ok)
	assert.Equal(t, int64(1), loadBalanceSegmentsMsg2.ID())
	assert.Equal(t, commonpb.MsgType_LoadBalanceSegments, loadBalanceSegmentsMsg2.Type())
	assert.Equal(t, int64(3), loadBalanceSegmentsMsg2.SourceID())
}

func TestLoadBalanceSegmentsMsg_Unmarshal_IllegalParameter(t *testing.T) {
	loadBalanceSegmentsMsg := &LoadBalanceSegmentsMsg{}
	tsMsg, err := loadBalanceSegmentsMsg.Unmarshal(10)
	assert.NotNil(t, err)
	assert.Nil(t, tsMsg)
}

func TestDataNodeTtMsg(t *testing.T) {
	dataNodeTtMsg := &DataNodeTtMsg{
		BaseMsg: generateBaseMsg(),
		DataNodeTtMsg: datapb.DataNodeTtMsg{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DataNodeTt,
				MsgID:     1,
				Timestamp: 2,
				SourceID:  3,
			},
			ChannelName: "test-channel",
			Timestamp:   4,
		},
	}

	assert.NotNil(t, dataNodeTtMsg.TraceCtx())

	ctx := context.Background()
	dataNodeTtMsg.SetTraceCtx(ctx)
	assert.Equal(t, ctx, dataNodeTtMsg.TraceCtx())

	assert.Equal(t, int64(1), dataNodeTtMsg.ID())
	assert.Equal(t, commonpb.MsgType_DataNodeTt, dataNodeTtMsg.Type())
	assert.Equal(t, int64(3), dataNodeTtMsg.SourceID())

	bytes, err := dataNodeTtMsg.Marshal(dataNodeTtMsg)
	assert.Nil(t, err)

	tsMsg, err := dataNodeTtMsg.Unmarshal(bytes)
	assert.Nil(t, err)

	dataNodeTtMsg2, ok := tsMsg.(*DataNodeTtMsg)
	assert.True(t, ok)
	assert.Equal(t, int64(1), dataNodeTtMsg2.ID())
	assert.Equal(t, commonpb.MsgType_DataNodeTt, dataNodeTtMsg2.Type())
	assert.Equal(t, int64(3), dataNodeTtMsg2.SourceID())
}

func TestDataNodeTtMsg_Unmarshal_IllegalParameter(t *testing.T) {
	dataNodeTtMsg := &DataNodeTtMsg{}
	tsMsg, err := dataNodeTtMsg.Unmarshal(10)
	assert.NotNil(t, err)
	assert.Nil(t, tsMsg)
}
