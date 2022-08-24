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

package indexcoord

//func TestNodeManager_PeekClient(t *testing.T) {
//	t.Run("success", func(t *testing.T) {
//		nm := NewNodeManager(context.Background())
//		meta := &Meta{
//			indexMeta: &indexpb.IndexMeta{
//				Req: &indexpb.BuildIndexRequest{
//					DataPaths: []string{"PeekClient-1", "PeekClient-2"},
//					NumRows:   1000,
//					TypeParams: []*commonpb.KeyValuePair{
//						{
//							Key:   "dim",
//							Value: "128",
//						},
//					},
//					FieldSchema: &schemapb.FieldSchema{
//						DataType: schemapb.DataType_FloatVector,
//					},
//				},
//			},
//		}
//		nodeID, client := nm.PeekClient(meta)
//		assert.Equal(t, int64(-1), nodeID)
//		assert.Nil(t, client)
//		err := nm.AddNode(1, "indexnode-1")
//		assert.Nil(t, err)
//		nm.pq.SetMemory(1, 100)
//		nodeID2, client2 := nm.PeekClient(meta)
//		assert.Equal(t, int64(0), nodeID2)
//		assert.Nil(t, client2)
//	})
//
//	t.Run("multiple unavailable IndexNode", func(t *testing.T) {
//		nm := &NodeManager{
//			ctx: context.TODO(),
//			nodeClients: map[UniqueID]types.IndexNode{
//				1: &indexnode.MockIndexNode{
//					GetTaskSlotsMock: func(ctx context.Context, req *indexpb.GetTaskSlotsRequest) (*indexpb.GetTaskSlotsResponse, error) {
//						return &indexpb.GetTaskSlotsResponse{
//							Status: &commonpb.Status{
//								ErrorCode: commonpb.ErrorCode_UnexpectedError,
//							},
//						}, errors.New("error")
//					},
//				},
//				2: &indexnode.MockIndexNode{
//					GetTaskSlotsMock: func(ctx context.Context, req *indexpb.GetTaskSlotsRequest) (*indexpb.GetTaskSlotsResponse, error) {
//						return &indexpb.GetTaskSlotsResponse{
//							Status: &commonpb.Status{
//								ErrorCode: commonpb.ErrorCode_UnexpectedError,
//							},
//						}, errors.New("error")
//					},
//				},
//				3: &indexnode.MockIndexNode{
//					GetTaskSlotsMock: func(ctx context.Context, req *indexpb.GetTaskSlotsRequest) (*indexpb.GetTaskSlotsResponse, error) {
//						return &indexpb.GetTaskSlotsResponse{
//							Status: &commonpb.Status{
//								ErrorCode: commonpb.ErrorCode_UnexpectedError,
//							},
//						}, errors.New("error")
//					},
//				},
//				4: &indexnode.MockIndexNode{
//					GetTaskSlotsMock: func(ctx context.Context, req *indexpb.GetTaskSlotsRequest) (*indexpb.GetTaskSlotsResponse, error) {
//						return &indexpb.GetTaskSlotsResponse{
//							Status: &commonpb.Status{
//								ErrorCode: commonpb.ErrorCode_UnexpectedError,
//							},
//						}, errors.New("error")
//					},
//				},
//				5: &indexnode.MockIndexNode{
//					GetTaskSlotsMock: func(ctx context.Context, req *indexpb.GetTaskSlotsRequest) (*indexpb.GetTaskSlotsResponse, error) {
//						return &indexpb.GetTaskSlotsResponse{
//							Status: &commonpb.Status{
//								ErrorCode: commonpb.ErrorCode_UnexpectedError,
//								Reason:    "fail reason",
//							},
//						}, nil
//					},
//				},
//				6: &indexnode.MockIndexNode{
//					GetTaskSlotsMock: func(ctx context.Context, req *indexpb.GetTaskSlotsRequest) (*indexpb.GetTaskSlotsResponse, error) {
//						return &indexpb.GetTaskSlotsResponse{
//							Status: &commonpb.Status{
//								ErrorCode: commonpb.ErrorCode_UnexpectedError,
//								Reason:    "fail reason",
//							},
//						}, nil
//					},
//				},
//				7: &indexnode.MockIndexNode{
//					GetTaskSlotsMock: func(ctx context.Context, req *indexpb.GetTaskSlotsRequest) (*indexpb.GetTaskSlotsResponse, error) {
//						return &indexpb.GetTaskSlotsResponse{
//							Status: &commonpb.Status{
//								ErrorCode: commonpb.ErrorCode_UnexpectedError,
//								Reason:    "fail reason",
//							},
//						}, nil
//					},
//				},
//				8: &indexnode.MockIndexNode{
//					GetTaskSlotsMock: func(ctx context.Context, req *indexpb.GetTaskSlotsRequest) (*indexpb.GetTaskSlotsResponse, error) {
//						return &indexpb.GetTaskSlotsResponse{
//							Slots: 1,
//							Status: &commonpb.Status{
//								ErrorCode: commonpb.ErrorCode_Success,
//								Reason:    "",
//							},
//						}, nil
//					},
//				},
//				9: &indexnode.MockIndexNode{
//					GetTaskSlotsMock: func(ctx context.Context, req *indexpb.GetTaskSlotsRequest) (*indexpb.GetTaskSlotsResponse, error) {
//						return &indexpb.GetTaskSlotsResponse{
//							Slots: 10,
//							Status: &commonpb.Status{
//								ErrorCode: commonpb.ErrorCode_Success,
//								Reason:    "",
//							},
//						}, nil
//					},
//				},
//			},
//		}
//
//		nodeID, client := nm.PeekClient(&Meta{})
//		assert.NotNil(t, client)
//		assert.Contains(t, []UniqueID{8, 9}, nodeID)
//	})
//}
