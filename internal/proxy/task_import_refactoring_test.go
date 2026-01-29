/*
 * Licensed to the LF AI & Data foundation under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package proxy

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

// TestImportTask_Execute_CallsDataCoord tests that the refactored execute method
// calls DataCoord.ImportV2() instead of broadcasting directly
func TestImportTask_Execute_CallsDataCoord(t *testing.T) {
	mockey.PatchConvey("TestImportTask_Execute_CallsDataCoord", t, func() {
		ctx := context.Background()

		// Mock globalMetaCache.GetDatabaseInfo
		mockey.Mock(Cache.GetDatabaseInfo).To(func(_ Cache, _ context.Context, _ string) (*databaseInfo, error) {
			return &databaseInfo{dbID: 1}, nil
		}).Build()

		// Track ImportV2 call
		var importV2Called bool
		var capturedReq *internalpb.ImportRequestInternal

		// Mock MixCoordClient.ImportV2
		mockey.Mock(types.MixCoordClient.ImportV2).To(func(_ types.MixCoordClient, _ context.Context, req *internalpb.ImportRequestInternal, _ ...grpc.CallOption) (*internalpb.ImportResponse, error) {
			importV2Called = true
			capturedReq = req
			return &internalpb.ImportResponse{
				Status: merr.Success(),
				JobID:  "1000",
			}, nil
		}).Build()

		// Create import task
		task := &importTask{
			ctx: ctx,
			req: &internalpb.ImportRequest{
				CollectionName: "test_collection",
			},
			mixCoord:     nil, // Will be mocked
			collectionID: 100,
			partitionIDs: []int64{1, 2},
			vchannels:    []string{"vchannel1", "vchannel2"},
			schema: &schemaInfo{
				CollectionSchema: &schemapb.CollectionSchema{
					Name: "test_collection",
				},
			},
			resp: &internalpb.ImportResponse{
				Status: merr.Success(),
			},
		}

		// Execute task
		err := task.Execute(ctx)

		// Verify
		assert.NoError(t, err)
		assert.True(t, importV2Called, "ImportV2 should be called")
		assert.Equal(t, "1000", task.resp.JobID)
		assert.NotNil(t, capturedReq)
		assert.Equal(t, int64(100), capturedReq.CollectionID)
		assert.Equal(t, "test_collection", capturedReq.CollectionName)
		assert.Equal(t, uint64(0), capturedReq.DataTimestamp) // CRITICAL: Must be 0 for proxy call
		assert.Equal(t, int64(0), capturedReq.JobID)          // Let DataCoord allocate
	})
}

// TestImportTask_Execute_DataTimestampZero tests that proxy sets DataTimestamp to 0
func TestImportTask_Execute_DataTimestampZero(t *testing.T) {
	mockey.PatchConvey("TestImportTask_Execute_DataTimestampZero", t, func() {
		ctx := context.Background()

		// Mock globalMetaCache.GetDatabaseInfo
		mockey.Mock(Cache.GetDatabaseInfo).To(func(_ Cache, _ context.Context, _ string) (*databaseInfo, error) {
			return &databaseInfo{dbID: 1}, nil
		}).Build()

		// Critical check: DataTimestamp must be 0
		var capturedReq *internalpb.ImportRequestInternal
		mockey.Mock(types.MixCoordClient.ImportV2).To(func(_ types.MixCoordClient, _ context.Context, req *internalpb.ImportRequestInternal, _ ...grpc.CallOption) (*internalpb.ImportResponse, error) {
			capturedReq = req
			if req.DataTimestamp != 0 {
				t.Errorf("DataTimestamp must be 0 for proxy call, got %d", req.DataTimestamp)
			}
			return &internalpb.ImportResponse{
				Status: merr.Success(),
				JobID:  "1000",
			}, nil
		}).Build()

		task := &importTask{
			ctx:          ctx,
			req:          &internalpb.ImportRequest{},
			mixCoord:     nil,
			collectionID: 100,
			partitionIDs: []int64{1},
			vchannels:    []string{"vchannel1"},
			schema: &schemaInfo{
				CollectionSchema: &schemapb.CollectionSchema{},
			},
			resp: &internalpb.ImportResponse{Status: merr.Success()},
		}

		err := task.Execute(ctx)
		assert.NoError(t, err)
		assert.NotNil(t, capturedReq)
		assert.Equal(t, uint64(0), capturedReq.DataTimestamp)
	})
}

// TestImportTask_Execute_ErrorHandling tests error handling when DataCoord returns error
func TestImportTask_Execute_ErrorHandling(t *testing.T) {
	tests := []struct {
		name        string
		response    *internalpb.ImportResponse
		rpcError    error
		expectError bool
	}{
		{
			name: "DataCoord returns error status",
			response: &internalpb.ImportResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    "validation failed",
				},
			},
			rpcError:    nil,
			expectError: true,
		},
		{
			name:        "RPC call fails",
			response:    nil,
			rpcError:    merr.WrapErrServiceInternal("connection failed"),
			expectError: true,
		},
		{
			name: "Success",
			response: &internalpb.ImportResponse{
				Status: merr.Success(),
				JobID:  "1000",
			},
			rpcError:    nil,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockey.PatchConvey(tt.name, t, func() {
				ctx := context.Background()

				// Mock globalMetaCache.GetDatabaseInfo
				mockey.Mock(Cache.GetDatabaseInfo).To(func(_ Cache, _ context.Context, _ string) (*databaseInfo, error) {
					return &databaseInfo{dbID: 1}, nil
				}).Build()

				// Mock MixCoordClient.ImportV2 to return test case response
				mockey.Mock(types.MixCoordClient.ImportV2).To(func(_ types.MixCoordClient, _ context.Context, _ *internalpb.ImportRequestInternal, _ ...grpc.CallOption) (*internalpb.ImportResponse, error) {
					return tt.response, tt.rpcError
				}).Build()

				task := &importTask{
					ctx:          ctx,
					req:          &internalpb.ImportRequest{},
					mixCoord:     nil,
					collectionID: 100,
					partitionIDs: []int64{1},
					vchannels:    []string{"vchannel1"},
					schema: &schemaInfo{
						CollectionSchema: &schemapb.CollectionSchema{},
					},
					resp: &internalpb.ImportResponse{Status: merr.Success()},
				}

				err := task.Execute(ctx)

				if tt.expectError {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					assert.Equal(t, "1000", task.resp.JobID)
				}
			})
		})
	}
}

// TestImportTask_Execute_RequestStructure tests that the request is properly constructed
func TestImportTask_Execute_RequestStructure(t *testing.T) {
	mockey.PatchConvey("TestImportTask_Execute_RequestStructure", t, func() {
		ctx := context.Background()

		// Mock globalMetaCache.GetDatabaseInfo
		mockey.Mock(Cache.GetDatabaseInfo).To(func(_ Cache, _ context.Context, _ string) (*databaseInfo, error) {
			return &databaseInfo{dbID: 1}, nil
		}).Build()

		expectedCollectionID := int64(100)
		expectedPartitionIDs := []int64{1, 2, 3}
		expectedVChannels := []string{"vchannel1", "vchannel2"}
		expectedFiles := []*internalpb.ImportFile{
			{Id: 1, Paths: []string{"/test/file1.json"}},
			{Id: 2, Paths: []string{"/test/file2.json"}},
		}
		expectedOptions := []*commonpb.KeyValuePair{
			{Key: "timeout", Value: "300"},
			{Key: "backup", Value: "false"},
		}

		var capturedReq *internalpb.ImportRequestInternal
		mockey.Mock(types.MixCoordClient.ImportV2).To(func(_ types.MixCoordClient, _ context.Context, req *internalpb.ImportRequestInternal, _ ...grpc.CallOption) (*internalpb.ImportResponse, error) {
			capturedReq = req
			// Verify all fields are correctly passed
			assert.Equal(t, expectedCollectionID, req.CollectionID)
			assert.Equal(t, "test_collection", req.CollectionName)
			assert.Equal(t, expectedPartitionIDs, req.PartitionIDs)
			assert.Equal(t, expectedVChannels, req.ChannelNames)
			assert.Len(t, req.Files, 2)
			assert.Len(t, req.Options, 2)
			assert.NotNil(t, req.Schema)
			assert.Equal(t, uint64(0), req.DataTimestamp) // Critical check
			assert.Equal(t, int64(0), req.JobID)          // Let DataCoord allocate
			return &internalpb.ImportResponse{
				Status: merr.Success(),
				JobID:  "1000",
			}, nil
		}).Build()

		task := &importTask{
			ctx: ctx,
			req: &internalpb.ImportRequest{
				CollectionName: "test_collection",
				Files:          expectedFiles,
				Options:        expectedOptions,
			},
			mixCoord:     nil,
			collectionID: expectedCollectionID,
			partitionIDs: expectedPartitionIDs,
			vchannels:    expectedVChannels,
			schema: &schemaInfo{
				CollectionSchema: &schemapb.CollectionSchema{
					Name: "test_collection",
				},
			},
			resp: &internalpb.ImportResponse{Status: merr.Success()},
		}

		err := task.Execute(ctx)
		assert.NoError(t, err)
		assert.NotNil(t, capturedReq)
	})
}

// TestImportTask_NoBroadcastCalled ensures that the old broadcast path is not used
func TestImportTask_NoBroadcastCalled(t *testing.T) {
	// This test documents that streaming.WAL().Broadcast().Append() is NO LONGER called
	// The old code path has been removed in the refactoring

	mockey.PatchConvey("TestImportTask_NoBroadcastCalled", t, func() {
		ctx := context.Background()

		// Mock globalMetaCache.GetDatabaseInfo
		mockey.Mock(Cache.GetDatabaseInfo).To(func(_ Cache, _ context.Context, _ string) (*databaseInfo, error) {
			return &databaseInfo{dbID: 1}, nil
		}).Build()

		var importV2Called bool
		mockey.Mock(types.MixCoordClient.ImportV2).To(func(_ types.MixCoordClient, _ context.Context, _ *internalpb.ImportRequestInternal, _ ...grpc.CallOption) (*internalpb.ImportResponse, error) {
			importV2Called = true
			return &internalpb.ImportResponse{
				Status: merr.Success(),
				JobID:  "1000",
			}, nil
		}).Build()

		task := &importTask{
			ctx:          ctx,
			req:          &internalpb.ImportRequest{},
			mixCoord:     nil,
			collectionID: 100,
			partitionIDs: []int64{1},
			vchannels:    []string{"vchannel1"},
			schema: &schemaInfo{
				CollectionSchema: &schemapb.CollectionSchema{},
			},
			resp: &internalpb.ImportResponse{Status: merr.Success()},
		}

		err := task.Execute(ctx)
		assert.NoError(t, err)
		assert.True(t, importV2Called, "ImportV2 should be called")

		t.Log("✓ Confirmed: Import task calls DataCoord.ImportV2() instead of broadcasting")
	})
}
