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

package pipeline

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
)

func TestEmbeddingNode_BM25_Operator(t *testing.T) {
	collSchema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:  common.TimeStampField,
				Name:     common.TimeStampFieldName,
				DataType: schemapb.DataType_Int64,
			}, {
				Name:         "pk",
				FieldID:      100,
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
			}, {
				Name:     "text",
				FieldID:  101,
				DataType: schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   "enable_analyzer",
						Value: "true",
					},
				},
			}, {
				Name:             "sparse",
				FieldID:          102,
				DataType:         schemapb.DataType_SparseFloatVector,
				IsFunctionOutput: true,
			},
		},
		Functions: []*schemapb.FunctionSchema{{
			Name:           "BM25",
			Type:           schemapb.FunctionType_BM25,
			InputFieldIds:  []int64{101},
			OutputFieldIds: []int64{102},
		}},
	}

	t.Run("normal case", func(t *testing.T) {
		node, err := newEmbeddingNode("test-channel", collSchema)
		assert.NoError(t, err)

		var output []Msg
		assert.NotPanics(t, func() {
			output = node.Operate([]Msg{
				&FlowGraphMsg{
					BaseMsg: flowgraph.NewBaseMsg(false),
					InsertMessages: []*msgstream.InsertMsg{{
						BaseMsg: msgstream.BaseMsg{},
						InsertRequest: &msgpb.InsertRequest{
							SegmentID:  1,
							Version:    msgpb.InsertDataVersion_ColumnBased,
							Timestamps: []uint64{1, 1, 1},
							FieldsData: []*schemapb.FieldData{
								{
									FieldId: 100,
									Field: &schemapb.FieldData_Scalars{
										Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1, 2, 3}}}},
									},
								}, {
									FieldId: 101,
									Field: &schemapb.FieldData_Scalars{
										Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"test1", "test2", "test3"}}}},
									},
								},
							},
						},
					}},
				},
			})
		})

		assert.Equal(t, 1, len(output))

		msg, ok := output[0].(*FlowGraphMsg)
		assert.True(t, ok)
		assert.NotNil(t, msg.InsertData)
	})

	t.Run("with close msg", func(t *testing.T) {
		node, err := newEmbeddingNode("test-channel", collSchema)
		assert.NoError(t, err)

		var output []Msg

		assert.NotPanics(t, func() {
			output = node.Operate([]Msg{
				&FlowGraphMsg{
					BaseMsg: flowgraph.NewBaseMsg(true),
				},
			})
		})

		assert.Equal(t, 1, len(output))
	})

	t.Run("prepare insert failed", func(t *testing.T) {
		node, err := newEmbeddingNode("test-channel", collSchema)
		assert.NoError(t, err)

		assert.Panics(t, func() {
			node.Operate([]Msg{
				&FlowGraphMsg{
					BaseMsg: flowgraph.NewBaseMsg(false),
					InsertMessages: []*msgstream.InsertMsg{{
						BaseMsg: msgstream.BaseMsg{},
						InsertRequest: &msgpb.InsertRequest{
							FieldsData: []*schemapb.FieldData{{
								FieldId: 1100, // invalid fieldID
							}},
						},
					}},
				},
			})
		})
	})

	t.Run("embedding failed", func(t *testing.T) {
		node, err := newEmbeddingNode("test-channel", collSchema)
		assert.NoError(t, err)

		node.functionRunners[0].GetSchema().Type = 0
		assert.Panics(t, func() {
			node.Operate([]Msg{
				&FlowGraphMsg{
					BaseMsg: flowgraph.NewBaseMsg(false),
					InsertMessages: []*msgstream.InsertMsg{{
						BaseMsg: msgstream.BaseMsg{},
						InsertRequest: &msgpb.InsertRequest{
							SegmentID:  1,
							Version:    msgpb.InsertDataVersion_ColumnBased,
							Timestamps: []uint64{1, 1, 1},
							FieldsData: []*schemapb.FieldData{
								{
									FieldId: 100,
									Field: &schemapb.FieldData_Scalars{
										Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1, 2, 3}}}},
									},
								}, {
									FieldId: 101,
									Field: &schemapb.FieldData_Scalars{
										Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"test1", "test2", "test3"}}}},
									},
								},
							},
						},
					}},
				},
			})
		})
	})
}
