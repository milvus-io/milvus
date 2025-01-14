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

package sealpolicies

import (
	"context"
	"time"

	"github.com/samber/lo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration"
)

func (s *SealSuite) TestSealByTotalGrowingSegmentsSize() {
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.GrowingSegmentsMemSizeInMB.Key, "10")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.GrowingSegmentsMemSizeInMB.Key)

	paramtable.Get().Save(paramtable.Get().DataNodeCfg.SyncPeriod.Key, "5")
	defer paramtable.Get().Reset(paramtable.Get().DataNodeCfg.SyncPeriod.Key)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*10)
	defer cancel()
	c := s.Cluster

	const (
		dim    = 128
		dbName = ""
		rowNum = 100000

		vecType = schemapb.DataType_FloatVector
	)

	collectionName := "TestSealByGrowingSegmentsSize_" + funcutil.GenRandomStr()

	schema := integration.ConstructSchemaOfVecDataType(collectionName, dim, true, vecType)
	schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
		FieldID:        102,
		Name:           "pid",
		DataType:       schemapb.DataType_Int64,
		IsPartitionKey: true,
	})
	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)

	// create collection
	createCollectionStatus, err := c.Proxy.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:           dbName,
		CollectionName:   collectionName,
		Schema:           marshaledSchema,
		ShardsNum:        common.DefaultShardsNum,
		ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
	})
	err = merr.CheckRPCCall(createCollectionStatus, err)
	s.NoError(err)
	log.Info("CreateCollection result", zap.Any("createCollectionStatus", createCollectionStatus))

	// show collection
	showCollectionsResp, err := c.Proxy.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{})
	err = merr.CheckRPCCall(showCollectionsResp, err)
	s.NoError(err)
	log.Info("ShowCollections result", zap.Any("showCollectionsResp", showCollectionsResp))

	// insert
	fVecColumn := integration.NewFloatVectorFieldData(integration.FloatVecField, rowNum, dim)
	partitionKeyColumn := integration.NewInt64FieldDataWithStart("pid", rowNum, 0)
	hashKeys := integration.GenerateHashKeys(rowNum)
	insertResult, err := c.Proxy.Insert(ctx, &milvuspb.InsertRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldsData:     []*schemapb.FieldData{fVecColumn, partitionKeyColumn},
		HashKeys:       hashKeys,
		NumRows:        uint32(rowNum),
	})
	err = merr.CheckRPCCall(insertResult, err)
	s.NoError(err)
	s.Equal(int64(rowNum), insertResult.GetInsertCnt())

	// wait for segment seal and flush
	showSegments := func() bool {
		var segments []*datapb.SegmentInfo
		segments, err = c.MetaWatcher.ShowSegments()
		s.NoError(err)
		s.NotEmpty(segments)
		flushedSegments := lo.Filter(segments, func(segment *datapb.SegmentInfo, _ int) bool {
			return segment.GetState() == commonpb.SegmentState_Flushed
		})
		log.Info("ShowSegments result", zap.Int("len(segments)", len(segments)),
			zap.Int("len(flushedSegments)", len(flushedSegments)))
		return len(flushedSegments) >= 1
	}
	for !showSegments() {
		select {
		case <-ctx.Done():
			s.Fail("waiting for segment sealed timeout")
			return
		case <-time.After(1 * time.Second):
		}
	}

	// release collection
	status, err := c.Proxy.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{
		CollectionName: collectionName,
	})
	err = merr.CheckRPCCall(status, err)
	s.NoError(err)

	// drop collection
	status, err = c.Proxy.DropCollection(ctx, &milvuspb.DropCollectionRequest{
		CollectionName: collectionName,
	})
	err = merr.CheckRPCCall(status, err)
	s.NoError(err)
}
