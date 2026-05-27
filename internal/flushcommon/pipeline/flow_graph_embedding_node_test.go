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
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
)

// makeBM25Schema returns a schema with one BM25 function field, reused across tests.
func makeBM25Schema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:  common.TimeStampField,
				Name:     common.TimeStampFieldName,
				DataType: schemapb.DataType_Int64,
			},
			{
				Name:         "pk",
				FieldID:      100,
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
			},
			{
				Name:     "text",
				FieldID:  101,
				DataType: schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: "enable_analyzer", Value: "true"},
				},
			},
			{
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
}

// makePlainSchema returns a schema with no function fields.
func makePlainSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: common.TimeStampField, Name: common.TimeStampFieldName, DataType: schemapb.DataType_Int64},
			{FieldID: 100, Name: "pk", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
		},
	}
}

func TestBuildFunctionRunners(t *testing.T) {
	t.Run("schema with no functions returns empty map", func(t *testing.T) {
		runners, err := buildFunctionRunners(makePlainSchema())
		assert.NoError(t, err)
		assert.Empty(t, runners)
	})

	t.Run("schema with BM25 function returns one runner", func(t *testing.T) {
		runners, err := buildFunctionRunners(makeBM25Schema())
		assert.NoError(t, err)
		assert.Len(t, runners, 1)
		closeFunctionRunners(runners)
	})
}

func TestCloseFunctionRunners(t *testing.T) {
	t.Run("nil map does not panic", func(t *testing.T) {
		assert.NotPanics(t, func() { closeFunctionRunners(nil) })
	})

	t.Run("empty map does not panic", func(t *testing.T) {
		emptyRunners, err := buildFunctionRunners(makePlainSchema())
		assert.NoError(t, err)
		assert.NotPanics(t, func() { closeFunctionRunners(emptyRunners) })
	})

	t.Run("map with valid runner closes cleanly", func(t *testing.T) {
		runners, err := buildFunctionRunners(makeBM25Schema())
		assert.NoError(t, err)
		assert.NotPanics(t, func() { closeFunctionRunners(runners) })
	})
}

func TestSyncFunctionRunners(t *testing.T) {
	schema1 := makeBM25Schema()

	metaCache := metacache.NewMockMetaCache(t)
	metaCache.EXPECT().GetSchema(mock.Anything).Return(schema1)
	metaCache.EXPECT().Collection().Return(int64(1)).Maybe()

	node, err := newEmbeddingNode("test-ch", metaCache)
	assert.NoError(t, err)
	defer node.Free()

	t.Run("same schema pointer is a no-op", func(t *testing.T) {
		prevRunners := node.functionRunners
		has, err := node.syncFunctionRunners(schema1)
		assert.NoError(t, err)
		assert.True(t, has)
		// runners must not be replaced when schema pointer is unchanged
		assert.Equal(t, prevRunners, node.functionRunners)
	})

	t.Run("new schema with no functions clears runners", func(t *testing.T) {
		schema2 := makePlainSchema()
		has, err := node.syncFunctionRunners(schema2)
		assert.NoError(t, err)
		assert.False(t, has)
		assert.Equal(t, schema2, node.curSchema)
		assert.Empty(t, node.functionRunners)
	})

	t.Run("new schema with BM25 function rebuilds runners", func(t *testing.T) {
		schema3 := makeBM25Schema()
		has, err := node.syncFunctionRunners(schema3)
		assert.NoError(t, err)
		assert.True(t, has)
		assert.Equal(t, schema3, node.curSchema)
		assert.Len(t, node.functionRunners, 1)
	})
}

func TestEmbeddingNode_OperateWithSchemaChange(t *testing.T) {
	plainSchema := makePlainSchema()
	bm25Schema := makeBM25Schema()

	t.Run("no functions schema returns msg unchanged", func(t *testing.T) {
		metaCache := metacache.NewMockMetaCache(t)
		metaCache.EXPECT().GetSchema(mock.Anything).Return(plainSchema)
		metaCache.EXPECT().Collection().Return(int64(1)).Maybe()

		node, err := newEmbeddingNode("ch", metaCache)
		assert.NoError(t, err)
		defer node.Free()

		fgMsg := &FlowGraphMsg{
			BaseMsg: flowgraph.NewBaseMsg(false),
			InsertMessages: []*msgstream.InsertMsg{{
				BaseMsg:       msgstream.BaseMsg{},
				InsertRequest: &msgpb.InsertRequest{},
			}},
		}
		out := node.Operate([]Msg{fgMsg})
		assert.Len(t, out, 1)
		// InsertData must not be set — embedding was skipped
		assert.Nil(t, out[0].(*FlowGraphMsg).InsertData)
	})

	t.Run("functions present but no insert messages returns empty InsertData", func(t *testing.T) {
		metaCache := metacache.NewMockMetaCache(t)
		metaCache.EXPECT().GetSchema(mock.Anything).Return(bm25Schema)
		metaCache.EXPECT().Collection().Return(int64(1)).Maybe()

		node, err := newEmbeddingNode("ch", metaCache)
		assert.NoError(t, err)
		defer node.Free()

		fgMsg := &FlowGraphMsg{
			BaseMsg:        flowgraph.NewBaseMsg(false),
			InsertMessages: nil,
		}
		out := node.Operate([]Msg{fgMsg})
		assert.Len(t, out, 1)
		msg := out[0].(*FlowGraphMsg)
		assert.NotNil(t, msg.InsertData)
		assert.Empty(t, msg.InsertData)
	})

	t.Run("schema changes from no-function to BM25 mid-stream", func(t *testing.T) {
		// GetSchema is called once during newEmbeddingNode construction, then once per Operate.
		// calls 1+2 → plainSchema; call 3 → bm25Schema.
		callCount := 0
		metaCache := metacache.NewMockMetaCache(t)
		metaCache.EXPECT().GetSchema(mock.Anything).RunAndReturn(func(_ uint64) *schemapb.CollectionSchema {
			callCount++
			if callCount <= 2 {
				return plainSchema
			}
			return bm25Schema
		})
		metaCache.EXPECT().Collection().Return(int64(1)).Maybe()

		node, err := newEmbeddingNode("ch", metaCache)
		assert.NoError(t, err)
		defer node.Free()

		// First call: plain schema — no functions, embedding skipped
		out1 := node.Operate([]Msg{&FlowGraphMsg{
			BaseMsg:        flowgraph.NewBaseMsg(false),
			InsertMessages: []*msgstream.InsertMsg{{}},
		}})
		assert.Nil(t, out1[0].(*FlowGraphMsg).InsertData)

		// Second call: BM25 schema — functions present, no messages → empty InsertData
		out2 := node.Operate([]Msg{&FlowGraphMsg{
			BaseMsg:        flowgraph.NewBaseMsg(false),
			InsertMessages: nil,
		}})
		msg2 := out2[0].(*FlowGraphMsg)
		assert.NotNil(t, msg2.InsertData)
		assert.Empty(t, msg2.InsertData)
		// node must now track the BM25 schema
		assert.Equal(t, bm25Schema, node.curSchema)
	})
}

func TestEmbeddingNode_Operator(t *testing.T) {
	// Define test cases for different function types
	testCases := []struct {
		name        string
		setupSchema func() *schemapb.CollectionSchema
		setupMock   func(*schemapb.CollectionSchema) error
	}{
		{
			name: "BM25",
			setupSchema: func() *schemapb.CollectionSchema {
				return &schemapb.CollectionSchema{
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
			},
			setupMock: func(schema *schemapb.CollectionSchema) error {
				return nil // BM25 doesn't need mock setup
			},
		},
		{
			name: "MinHash",
			setupSchema: func() *schemapb.CollectionSchema {
				return &schemapb.CollectionSchema{
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
							Name:             "binary_vector",
							FieldID:          102,
							DataType:         schemapb.DataType_BinaryVector,
							IsFunctionOutput: true,
							TypeParams: []*commonpb.KeyValuePair{
								{
									Key:   "dim",
									Value: "1024",
								},
							},
						},
					},
					Functions: []*schemapb.FunctionSchema{{
						Name:           "MinHash",
						Type:           schemapb.FunctionType_MinHash,
						InputFieldIds:  []int64{101},
						OutputFieldIds: []int64{102},
					}},
				}
			},
			setupMock: func(schema *schemapb.CollectionSchema) error {
				return nil
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			collSchema := tc.setupSchema()
			err := tc.setupMock(collSchema)
			assert.NoError(t, err)

			metaCache := metacache.NewMockMetaCache(t)
			metaCache.EXPECT().GetSchema(mock.Anything).Return(collSchema)
			metaCache.EXPECT().Collection().Return(int64(0)).Maybe()

			t.Run("normal case", func(t *testing.T) {
				node, err := newEmbeddingNode("test-channel", metaCache)
				assert.NoError(t, err)
				defer node.Free()

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
				node, err := newEmbeddingNode("test-channel", metaCache)
				assert.NoError(t, err)
				defer node.Free()

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
				node, err := newEmbeddingNode("test-channel", metaCache)
				assert.NoError(t, err)
				defer node.Free()

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
				node, err := newEmbeddingNode("test-channel", metaCache)
				assert.NoError(t, err)
				defer node.Free()

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
		})
	}
}
