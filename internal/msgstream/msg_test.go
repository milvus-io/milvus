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

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/stretchr/testify/assert"
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
