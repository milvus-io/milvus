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

package snapshot

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metric"
	"github.com/milvus-io/milvus/tests/integration"
)

type SnapshotRestoreSuite struct {
	integration.MiniClusterSuite
}

func TestSnapshotRestore(t *testing.T) {
	suite.Run(t, new(SnapshotRestoreSuite))
}

// TestSnapshotRestoreWithDynamicField verifies that snapshot restore correctly
// handles collections with dynamic fields (JSON) and that the restored collection
// can be loaded and queried successfully.
//
// This is a regression test for https://github.com/milvus-io/milvus/issues/48579
// where LoadCollection hangs at 90% after restoring a StorageV3 collection with
// dynamic fields, because json_key_index buildIDs were incorrectly remapped during
// CopySegment, causing QueryNode 404 errors when loading the LOON manifest.
func (s *SnapshotRestoreSuite) TestSnapshotRestoreWithDynamicField() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	c := s.Cluster

	const (
		dim    = 128
		rowNum = 3000
	)

	collectionName := "TestSnapshotRestore_" + funcutil.GenRandomStr()

	// Step 1: Create collection with JSON field and dynamic fields enabled
	schema := &schemapb.CollectionSchema{
		Name:               collectionName,
		EnableDynamicField: true,
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "id",
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:  101,
				Name:     "metadata",
				DataType: schemapb.DataType_JSON,
			},
			{
				FieldID:  102,
				Name:     "embeddings",
				DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.DimKey, Value: strconv.Itoa(dim)},
				},
			},
		},
	}
	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)

	createResp, err := c.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      common.DefaultShardsNum,
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, createResp.GetErrorCode())
	log.Info("Created collection", zap.String("name", collectionName))

	// Step 2: Create indexes
	// Vector index (HNSW)
	createIdxResp, err := c.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      "embeddings",
		IndexName:      "vec_idx",
		ExtraParams:    integration.ConstructIndexParam(dim, integration.IndexHNSW, metric.L2),
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, createIdxResp.GetErrorCode())

	// JSON path index on metadata["category"]
	createIdxResp, err = c.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      "metadata",
		IndexName:      "idx_category",
		ExtraParams: []*commonpb.KeyValuePair{
			{Key: common.IndexTypeKey, Value: "INVERTED"},
			{Key: "json_path", Value: `metadata["category"]`},
			{Key: "json_cast_type", Value: "varchar"},
		},
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, createIdxResp.GetErrorCode())

	s.WaitForIndexBuiltWithIndexName(ctx, collectionName, "embeddings", "vec_idx")
	s.WaitForIndexBuiltWithIndexName(ctx, collectionName, "metadata", "idx_category")
	log.Info("Indexes built")

	// Step 3: Insert data with JSON content
	idData := make([]int64, rowNum)
	jsonData := make([][]byte, rowNum)
	vecData := make([]float32, rowNum*dim)
	categories := []string{"electronics", "books", "clothing", "food", "toys"}

	for i := 0; i < rowNum; i++ {
		idData[i] = int64(i)
		category := categories[i%len(categories)]
		data := map[string]interface{}{
			"category": category,
			"price":    float64(i) * 1.5,
			"stock":    i * 10,
		}
		jsonBytes, marshalErr := json.Marshal(data)
		s.NoError(marshalErr)
		jsonData[i] = jsonBytes

		for j := 0; j < dim; j++ {
			vecData[i*dim+j] = float32(i*dim+j) * 0.001
		}
	}

	insertResult, err := c.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
		CollectionName: collectionName,
		FieldsData: []*schemapb.FieldData{
			{
				Type:      schemapb.DataType_Int64,
				FieldName: "id",
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{Data: idData},
						},
					},
				},
			},
			{
				Type:      schemapb.DataType_JSON,
				FieldName: "metadata",
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_JsonData{
							JsonData: &schemapb.JSONArray{Data: jsonData},
						},
					},
				},
			},
			{
				Type:      schemapb.DataType_FloatVector,
				FieldName: "embeddings",
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Dim: dim,
						Data: &schemapb.VectorField_FloatVector{
							FloatVector: &schemapb.FloatArray{Data: vecData},
						},
					},
				},
			},
		},
		NumRows: uint32(rowNum),
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, insertResult.GetStatus().GetErrorCode())
	log.Info("Inserted data", zap.Int("rows", rowNum))

	// Step 4: Flush
	flushResp, err := c.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{
		CollectionNames: []string{collectionName},
	})
	s.NoError(err)
	segmentIDs, has := flushResp.GetCollSegIDs()[collectionName]
	s.True(has)
	s.NotEmpty(segmentIDs.GetData())
	flushTs, has := flushResp.GetCollFlushTs()[collectionName]
	s.True(has)
	s.WaitForFlush(ctx, segmentIDs.GetData(), flushTs, "", collectionName)
	log.Info("Flushed")

	// Step 5: Load and verify initial data
	loadResp, err := c.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		CollectionName: collectionName,
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, loadResp.GetErrorCode())
	s.WaitForLoad(ctx, collectionName)

	queryResult, err := c.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		CollectionName: collectionName,
		Expr:           "",
		OutputFields:   []string{"count(*)"},
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, queryResult.GetStatus().GetErrorCode())
	initialCount := queryResult.GetFieldsData()[0].GetScalars().GetLongData().GetData()[0]
	s.Equal(int64(rowNum), initialCount)
	log.Info("Verified initial data", zap.Int64("count", initialCount))

	// Step 6: Create snapshot
	snapshotName := fmt.Sprintf("snap_%s", funcutil.GenRandomStr())
	createSnapResp, err := c.MilvusClient.CreateSnapshot(ctx, &milvuspb.CreateSnapshotRequest{
		Name:           snapshotName,
		CollectionName: collectionName,
		Description:    "snapshot restore load test",
	})
	err = merr.CheckRPCCall(createSnapResp, err)
	s.NoError(err)
	log.Info("Created snapshot", zap.String("name", snapshotName))

	// Step 7: Insert more data after snapshot (to verify point-in-time restore)
	extraIDs := make([]int64, 1000)
	extraJSON := make([][]byte, 1000)
	extraVec := make([]float32, 1000*dim)
	for i := 0; i < 1000; i++ {
		extraIDs[i] = int64(rowNum + i)
		jsonBytes, _ := json.Marshal(map[string]interface{}{"category": "extra", "price": 0.0})
		extraJSON[i] = jsonBytes
		for j := 0; j < dim; j++ {
			extraVec[i*dim+j] = 0.001
		}
	}
	extraInsert, err := c.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
		CollectionName: collectionName,
		FieldsData: []*schemapb.FieldData{
			{
				Type: schemapb.DataType_Int64, FieldName: "id",
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: extraIDs}},
				}},
			},
			{
				Type: schemapb.DataType_JSON, FieldName: "metadata",
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_JsonData{JsonData: &schemapb.JSONArray{Data: extraJSON}},
				}},
			},
			{
				Type: schemapb.DataType_FloatVector, FieldName: "embeddings",
				Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
					Dim:  dim,
					Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: extraVec}},
				}},
			},
		},
		NumRows: 1000,
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, extraInsert.GetStatus().GetErrorCode())

	// Step 8: Restore snapshot to a new collection
	restoredCollName := fmt.Sprintf("restored_%s", funcutil.GenRandomStr())
	restoreResp, err := c.MilvusClient.RestoreSnapshot(ctx, &milvuspb.RestoreSnapshotRequest{
		Name:           snapshotName,
		CollectionName: restoredCollName,
	})
	err = merr.CheckRPCCall(restoreResp, err)
	s.NoError(err)
	jobID := restoreResp.GetJobId()
	log.Info("Restore started", zap.Int64("jobID", jobID), zap.String("target", restoredCollName))

	// Step 9: Wait for restore to complete
	s.waitForRestoreComplete(ctx, jobID)
	log.Info("Restore completed")

	// Step 10: Load restored collection - this is where the bug manifests
	// (LoadCollection would hang at ~90% if json_key_index buildIDs were remapped)
	loadRestored, err := c.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		CollectionName: restoredCollName,
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, loadRestored.GetErrorCode())
	s.WaitForLoad(ctx, restoredCollName)
	log.Info("Loaded restored collection")

	// Step 11: Verify restored data count matches snapshot point-in-time
	queryRestored, err := c.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		CollectionName: restoredCollName,
		Expr:           "",
		OutputFields:   []string{"count(*)"},
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, queryRestored.GetStatus().GetErrorCode())
	restoredCount := queryRestored.GetFieldsData()[0].GetScalars().GetLongData().GetData()[0]
	s.Equal(int64(rowNum), restoredCount, "Restored count should match snapshot point-in-time, not include post-snapshot inserts")
	log.Info("Verified restored data count", zap.Int64("count", restoredCount))

	// Step 12: Verify JSON path index is functional via filter query
	categoryQuery, err := c.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		CollectionName: restoredCollName,
		Expr:           `metadata["category"] == "electronics"`,
		OutputFields:   []string{"count(*)"},
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, categoryQuery.GetStatus().GetErrorCode())
	categoryCount := categoryQuery.GetFieldsData()[0].GetScalars().GetLongData().GetData()[0]
	// "electronics" is categories[0], assigned to i%5==0, so count = rowNum/5
	s.Equal(int64(rowNum/5), categoryCount, "JSON path index filter should return correct count")
	log.Info("Verified JSON path index query", zap.Int64("electronicsCount", categoryCount))

	// Step 13: Verify search works on restored collection
	searchVec := make([]float32, dim)
	for i := range searchVec {
		searchVec[i] = 0.1
	}
	searchResult, err := c.MilvusClient.Search(ctx, integration.ConstructSearchRequest(
		"", restoredCollName, "", "embeddings",
		schemapb.DataType_FloatVector, nil, metric.L2,
		integration.GetSearchParams(integration.IndexHNSW, metric.L2),
		1, dim, 10, -1,
	))
	err = merr.CheckRPCCall(searchResult, err)
	s.NoError(err)
	s.NotEmpty(searchResult.GetResults().GetIds())
	log.Info("Verified search on restored collection")

	// Cleanup
	dropSnap, err := c.MilvusClient.DropSnapshot(ctx, &milvuspb.DropSnapshotRequest{
		Name: snapshotName,
	})
	err = merr.CheckRPCCall(dropSnap, err)
	s.NoError(err)
	log.Info("Test completed: snapshot restore with dynamic field and JSON path index verified")
}

// waitForRestoreComplete polls GetRestoreSnapshotState until the restore job completes or fails.
func (s *SnapshotRestoreSuite) waitForRestoreComplete(ctx context.Context, jobID int64) {
	for {
		select {
		case <-ctx.Done():
			s.FailNow("timeout waiting for restore to complete")
			return
		default:
			time.Sleep(1 * time.Second)
		}

		resp, err := s.Cluster.MilvusClient.GetRestoreSnapshotState(ctx, &milvuspb.GetRestoreSnapshotStateRequest{
			JobId: jobID,
		})
		err = merr.CheckRPCCall(resp, err)
		s.NoError(err)

		info := resp.GetInfo()
		state := info.GetState()
		log.Info("Restore progress", zap.Int32("progress", info.GetProgress()), zap.String("state", state.String()))

		switch state {
		case milvuspb.RestoreSnapshotState_RestoreSnapshotCompleted:
			return
		case milvuspb.RestoreSnapshotState_RestoreSnapshotFailed:
			s.FailNow("restore failed: " + info.GetReason())
			return
		}
	}
}
