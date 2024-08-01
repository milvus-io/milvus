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

package levelzero

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metric"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration"
)

func (s *LevelZeroSuite) createCollection(collection string) {
	schema := integration.ConstructSchema(collection, s.dim, false)
	marshaledSchema, err := proto.Marshal(schema)
	s.Require().NoError(err)

	status, err := s.Cluster.Proxy.CreateCollection(context.TODO(), &milvuspb.CreateCollectionRequest{
		CollectionName: collection,
		Schema:         marshaledSchema,
		ShardsNum:      1,
	})
	s.Require().NoError(err)
	s.Require().True(merr.Ok(status))
	log.Info("CreateCollection result", zap.Any("status", status))
}

func (s *LevelZeroSuite) generateSegment(collection string, numRows int, startPk int64, seal bool) {
	log.Info("=========================Start generate one segment=========================")
	pkColumn := integration.NewInt64FieldDataWithStart(integration.Int64Field, numRows, startPk)
	fVecColumn := integration.NewFloatVectorFieldData(integration.FloatVecField, numRows, s.dim)
	hashKeys := integration.GenerateHashKeys(numRows)
	insertResult, err := s.Cluster.Proxy.Insert(context.TODO(), &milvuspb.InsertRequest{
		CollectionName: collection,
		FieldsData:     []*schemapb.FieldData{pkColumn, fVecColumn},
		HashKeys:       hashKeys,
		NumRows:        uint32(numRows),
	})
	s.Require().NoError(err)
	s.True(merr.Ok(insertResult.GetStatus()))
	s.Require().EqualValues(numRows, insertResult.GetInsertCnt())
	s.Require().EqualValues(numRows, len(insertResult.GetIDs().GetIntId().GetData()))

	if seal {
		log.Info("=========================Start to flush =========================",
			zap.String("collection", collection),
			zap.Int("numRows", numRows),
			zap.Int64("startPK", startPk),
		)

		flushResp, err := s.Cluster.Proxy.Flush(context.TODO(), &milvuspb.FlushRequest{
			CollectionNames: []string{collection},
		})
		s.NoError(err)
		segmentLongArr, has := flushResp.GetCollSegIDs()[collection]
		s.Require().True(has)
		segmentIDs := segmentLongArr.GetData()
		s.Require().NotEmpty(segmentLongArr)
		s.Require().True(has)

		flushTs, has := flushResp.GetCollFlushTs()[collection]
		s.True(has)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		s.WaitForFlush(ctx, segmentIDs, flushTs, "", collection)
		log.Info("=========================Finish to generate one segment=========================",
			zap.String("collection", collection),
			zap.Int("numRows", numRows),
			zap.Int64("startPK", startPk),
		)
	}
}

func (s *LevelZeroSuite) TestDeleteOnGrowing() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*10)
	defer cancel()
	c := s.Cluster

	// make sure L0 segment are flushed per msgpack
	paramtable.Get().Save(paramtable.Get().DataNodeCfg.FlushDeleteBufferBytes.Key, "1")
	defer paramtable.Get().Reset(paramtable.Get().DataNodeCfg.FlushDeleteBufferBytes.Key)

	const (
		indexType  = integration.IndexFaissIvfFlat
		metricType = metric.L2
		vecType    = schemapb.DataType_FloatVector
	)

	collectionName := "TestLevelZero_" + funcutil.GenRandomStr()
	s.createCollection(collectionName)

	// create index
	createIndexStatus, err := c.Proxy.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      integration.FloatVecField,
		IndexName:      "_default",
		ExtraParams:    integration.ConstructIndexParam(s.dim, indexType, metricType),
	})
	err = merr.CheckRPCCall(createIndexStatus, err)
	s.NoError(err)
	s.WaitForIndexBuilt(ctx, collectionName, integration.FloatVecField)

	// load
	loadStatus, err := c.Proxy.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		CollectionName: collectionName,
	})
	err = merr.CheckRPCCall(loadStatus, err)
	s.Require().NoError(err)
	s.WaitForLoad(ctx, collectionName)

	s.generateSegment(collectionName, 1, 0, true)
	s.generateSegment(collectionName, 2, 1, true)
	s.generateSegment(collectionName, 2, 3, false)

	checkRowCount := func(rowCount int) {
		// query
		queryResult, err := c.Proxy.Query(ctx, &milvuspb.QueryRequest{
			CollectionName: collectionName,
			OutputFields:   []string{"count(*)"},
		})
		err = merr.CheckRPCCall(queryResult, err)
		s.NoError(err)
		s.EqualValues(rowCount, queryResult.GetFieldsData()[0].GetScalars().GetLongData().GetData()[0])
	}
	checkRowCount(5)

	// delete
	deleteResult, err := c.Proxy.Delete(ctx, &milvuspb.DeleteRequest{
		CollectionName: collectionName,
		Expr:           fmt.Sprintf("%s > -1", integration.Int64Field),
	})
	err = merr.CheckRPCCall(deleteResult, err)
	s.NoError(err)

	checkRowCount(0)

	l0Exist := func() ([]*datapb.SegmentInfo, bool) {
		segments, err := s.Cluster.MetaWatcher.ShowSegments()
		s.Require().NoError(err)
		s.Require().Greater(len(segments), 0)
		for _, segment := range segments {
			if segment.GetLevel() == datapb.SegmentLevel_L0 {
				return segments, true
			}
		}
		return nil, false
	}

	checkL0Exist := func() {
		failT := time.NewTimer(10 * time.Second)
		checkT := time.NewTicker(100 * time.Millisecond)
		for {
			select {
			case <-failT.C:
				s.FailNow("L0 segment timeout")
			case <-checkT.C:
				if segments, exist := l0Exist(); exist {
					failT.Stop()
					for _, segment := range segments {
						if segment.GetLevel() == datapb.SegmentLevel_L0 {
							s.EqualValues(5, segment.Deltalogs[0].GetBinlogs()[0].GetEntriesNum())
						}
					}
					return
				}
			}
		}
	}

	checkL0Exist()

	// release collection
	status, err := c.Proxy.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{CollectionName: collectionName})
	err = merr.CheckRPCCall(status, err)
	s.NoError(err)

	// load
	loadStatus, err = c.Proxy.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		CollectionName: collectionName,
	})
	err = merr.CheckRPCCall(loadStatus, err)
	s.Require().NoError(err)
	s.WaitForLoad(ctx, collectionName)

	checkRowCount(0)

	// drop collection
	status, err = c.Proxy.DropCollection(ctx, &milvuspb.DropCollectionRequest{
		CollectionName: collectionName,
	})
	err = merr.CheckRPCCall(status, err)
	s.NoError(err)
}
