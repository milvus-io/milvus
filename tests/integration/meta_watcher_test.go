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

package integration

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/distance"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestShowSessions(t *testing.T) {
	ctx := context.Background()
	c, err := StartMiniCluster(ctx)
	assert.NoError(t, err)
	err = c.Start()
	assert.NoError(t, err)
	defer c.Stop()
	assert.NoError(t, err)

	sessions, err := c.metaWatcher.ShowSessions()
	assert.NoError(t, err)
	assert.NotEmpty(t, sessions)
	for _, session := range sessions {
		log.Info("ShowSessions result", zap.String("session", session.String()))
	}

}

func TestShowSegments(t *testing.T) {
	ctx := context.Background()
	c, err := StartMiniCluster(ctx)
	assert.NoError(t, err)
	err = c.Start()
	assert.NoError(t, err)
	defer c.Stop()
	assert.NoError(t, err)

	prefix := "TestShowSegments"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	int64Field := "int64"
	floatVecField := "fvec"
	dim := 128
	rowNum := 3000

	constructCollectionSchema := func() *schemapb.CollectionSchema {
		pk := &schemapb.FieldSchema{
			FieldID:      0,
			Name:         int64Field,
			IsPrimaryKey: true,
			Description:  "",
			DataType:     schemapb.DataType_Int64,
			TypeParams:   nil,
			IndexParams:  nil,
			AutoID:       true,
		}
		fVec := &schemapb.FieldSchema{
			FieldID:      0,
			Name:         floatVecField,
			IsPrimaryKey: false,
			Description:  "",
			DataType:     schemapb.DataType_FloatVector,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   "dim",
					Value: strconv.Itoa(dim),
				},
			},
			IndexParams: nil,
			AutoID:      false,
		}
		return &schemapb.CollectionSchema{
			Name:        collectionName,
			Description: "",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				pk,
				fVec,
			},
		}
	}
	schema := constructCollectionSchema()
	marshaledSchema, err := proto.Marshal(schema)
	assert.NoError(t, err)

	createCollectionStatus, err := c.proxy.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      2,
	})
	assert.NoError(t, err)
	assert.Equal(t, createCollectionStatus.GetErrorCode(), commonpb.ErrorCode_Success)

	log.Info("CreateCollection result", zap.Any("createCollectionStatus", createCollectionStatus))
	showCollectionsResp, err := c.proxy.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{})
	assert.NoError(t, err)
	assert.Equal(t, showCollectionsResp.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)
	log.Info("ShowCollections result", zap.Any("showCollectionsResp", showCollectionsResp))

	fVecColumn := newFloatVectorFieldData(floatVecField, rowNum, dim)
	hashKeys := generateHashKeys(rowNum)
	insertResult, err := c.proxy.Insert(ctx, &milvuspb.InsertRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldsData:     []*schemapb.FieldData{fVecColumn},
		HashKeys:       hashKeys,
		NumRows:        uint32(rowNum),
	})
	assert.NoError(t, err)
	assert.Equal(t, insertResult.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)

	segments, err := c.metaWatcher.ShowSegments()
	assert.NoError(t, err)
	assert.NotEmpty(t, segments)
	for _, segment := range segments {
		log.Info("ShowSegments result", zap.String("segment", segment.String()))
	}
}

func TestShowReplicas(t *testing.T) {
	ctx := context.Background()
	c, err := StartMiniCluster(ctx)
	assert.NoError(t, err)
	err = c.Start()
	assert.NoError(t, err)
	defer c.Stop()
	assert.NoError(t, err)

	prefix := "TestShowReplicas"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	int64Field := "int64"
	floatVecField := "fvec"
	dim := 128
	rowNum := 3000

	constructCollectionSchema := func() *schemapb.CollectionSchema {
		pk := &schemapb.FieldSchema{
			FieldID:      0,
			Name:         int64Field,
			IsPrimaryKey: true,
			Description:  "",
			DataType:     schemapb.DataType_Int64,
			TypeParams:   nil,
			IndexParams:  nil,
			AutoID:       true,
		}
		fVec := &schemapb.FieldSchema{
			FieldID:      0,
			Name:         floatVecField,
			IsPrimaryKey: false,
			Description:  "",
			DataType:     schemapb.DataType_FloatVector,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   "dim",
					Value: strconv.Itoa(dim),
				},
			},
			IndexParams: nil,
			AutoID:      false,
		}
		return &schemapb.CollectionSchema{
			Name:        collectionName,
			Description: "",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				pk,
				fVec,
			},
		}
	}
	schema := constructCollectionSchema()
	marshaledSchema, err := proto.Marshal(schema)
	assert.NoError(t, err)

	createCollectionStatus, err := c.proxy.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      2,
	})
	assert.NoError(t, err)
	if createCollectionStatus.GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("createCollectionStatus fail reason", zap.String("reason", createCollectionStatus.GetReason()))
	}
	assert.Equal(t, createCollectionStatus.GetErrorCode(), commonpb.ErrorCode_Success)

	log.Info("CreateCollection result", zap.Any("createCollectionStatus", createCollectionStatus))
	showCollectionsResp, err := c.proxy.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{})
	assert.NoError(t, err)
	assert.Equal(t, showCollectionsResp.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)
	log.Info("ShowCollections result", zap.Any("showCollectionsResp", showCollectionsResp))

	fVecColumn := newFloatVectorFieldData(floatVecField, rowNum, dim)
	hashKeys := generateHashKeys(rowNum)
	insertResult, err := c.proxy.Insert(ctx, &milvuspb.InsertRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldsData:     []*schemapb.FieldData{fVecColumn},
		HashKeys:       hashKeys,
		NumRows:        uint32(rowNum),
	})
	assert.NoError(t, err)
	assert.Equal(t, insertResult.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)

	// flush
	flushResp, err := c.proxy.Flush(ctx, &milvuspb.FlushRequest{
		DbName:          dbName,
		CollectionNames: []string{collectionName},
	})
	assert.NoError(t, err)
	segmentIDs, has := flushResp.GetCollSegIDs()[collectionName]
	ids := segmentIDs.GetData()
	assert.NotEmpty(t, segmentIDs)

	segments, err := c.metaWatcher.ShowSegments()
	assert.NoError(t, err)
	assert.NotEmpty(t, segments)
	for _, segment := range segments {
		log.Info("ShowSegments result", zap.String("segment", segment.String()))
	}

	if has && len(ids) > 0 {
		flushed := func() bool {
			resp, err := c.proxy.GetFlushState(ctx, &milvuspb.GetFlushStateRequest{
				SegmentIDs: ids,
			})
			if err != nil {
				//panic(errors.New("GetFlushState failed"))
				return false
			}
			return resp.GetFlushed()
		}
		for !flushed() {
			// respect context deadline/cancel
			select {
			case <-ctx.Done():
				panic(errors.New("deadline exceeded"))
			default:
			}
			time.Sleep(500 * time.Millisecond)
		}
	}

	// create index
	createIndexStatus, err := c.proxy.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      floatVecField,
		IndexName:      "_default",
		ExtraParams: []*commonpb.KeyValuePair{
			{
				Key:   "dim",
				Value: strconv.Itoa(dim),
			},
			{
				Key:   common.MetricTypeKey,
				Value: distance.L2,
			},
			{
				Key:   "index_type",
				Value: "IVF_FLAT",
			},
			{
				Key:   "nlist",
				Value: strconv.Itoa(10),
			},
		},
	})
	assert.NoError(t, err)
	if createIndexStatus.GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("createIndexStatus fail reason", zap.String("reason", createIndexStatus.GetReason()))
	}
	assert.Equal(t, commonpb.ErrorCode_Success, createIndexStatus.GetErrorCode())

	// load
	loadStatus, err := c.proxy.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	assert.NoError(t, err)
	if loadStatus.GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("loadStatus fail reason", zap.String("reason", loadStatus.GetReason()))
	}
	assert.Equal(t, commonpb.ErrorCode_Success, loadStatus.GetErrorCode())
	for {
		loadProgress, err := c.proxy.GetLoadingProgress(ctx, &milvuspb.GetLoadingProgressRequest{
			CollectionName: collectionName,
		})
		if err != nil {
			panic("GetLoadingProgress fail")
		}
		if loadProgress.GetProgress() == 100 {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	replicas, err := c.metaWatcher.ShowReplicas()
	assert.NoError(t, err)
	assert.NotEmpty(t, replicas)
	for _, replica := range replicas {
		log.Info("ShowReplicas result", zap.String("replica", PrettyReplica(replica)))
	}

	log.Info("TestShowReplicas succeed")
}
