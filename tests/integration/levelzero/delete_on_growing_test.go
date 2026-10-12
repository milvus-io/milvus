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

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/tests/integration"
)

func (s *LevelZeroSuite) TestDeleteOnGrowing() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*10)
	defer cancel()
	c := s.Cluster

	const (
		indexType  = integration.IndexFaissIvfFlat
		metricType = metric.L2
		vecType    = schemapb.DataType_FloatVector
	)

	collectionName := "TestLevelZero_" + funcutil.GenRandomStr()
	s.schema = integration.ConstructSchema(collectionName, s.dim, false)
	req := s.buildCreateCollectionRequest(collectionName, s.schema, 0)
	s.createCollection(req)

	// create index
	createIndexStatus, err := c.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      integration.FloatVecField,
		IndexName:      "_default",
		ExtraParams:    integration.ConstructIndexParam(s.dim, indexType, metricType),
	})
	err = merr.CheckRPCCall(createIndexStatus, err)
	s.NoError(err)
	s.WaitForIndexBuilt(ctx, collectionName, integration.FloatVecField)

	// load
	loadStatus, err := c.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		CollectionName: collectionName,
	})
	err = merr.CheckRPCCall(loadStatus, err)
	s.Require().NoError(err)
	s.WaitForLoad(ctx, collectionName)

	s.generateSegment(collectionName, 1, 0, true, -1)
	s.generateSegment(collectionName, 2, 1, true, -1)
	s.generateSegment(collectionName, 2, 3, false, -1)

	checkRowCount := func(rowCount int) {
		// query
		queryResult, err := c.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
			CollectionName: collectionName,
			OutputFields:   []string{"count(*)"},
		})
		err = merr.CheckRPCCall(queryResult, err)
		s.NoError(err)
		s.EqualValues(rowCount, queryResult.GetFieldsData()[0].GetScalars().GetLongData().GetData()[0])
	}
	checkRowCount(5)

	// delete
	deleteResult, err := c.MilvusClient.Delete(ctx, &milvuspb.DeleteRequest{
		CollectionName: collectionName,
		Expr:           fmt.Sprintf("%s > -1", integration.Int64Field),
	})
	err = merr.CheckRPCCall(deleteResult, err)
	s.NoError(err)
	s.EqualValues(5, deleteResult.GetDeleteCnt())

	checkRowCount(0)

	// The materializer may split a Delete batch across multiple L0 files.
	// Wait for all delete entries to be persisted, not just the first L0.
	s.Require().EventuallyWithT(func(t *assert.CollectT) {
		segments, err := s.Cluster.ShowSegments(collectionName)
		if !assert.NoError(t, err) {
			return
		}
		var entries int64
		for _, segment := range segments {
			if segment.GetLevel() != datapb.SegmentLevel_L0 {
				continue
			}
			for _, field := range segment.GetDeltalogs() {
				for _, binlog := range field.GetBinlogs() {
					entries += binlog.GetEntriesNum()
				}
			}
		}
		assert.Equal(t, int64(5), entries, "all deletes must be persisted across L0 files")
	}, 10*time.Second, 100*time.Millisecond)

	// release collection
	status, err := c.MilvusClient.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{CollectionName: collectionName})
	err = merr.CheckRPCCall(status, err)
	s.NoError(err)

	// load
	loadStatus, err = c.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		CollectionName: collectionName,
	})
	err = merr.CheckRPCCall(loadStatus, err)
	s.Require().NoError(err)
	s.WaitForLoad(ctx, collectionName)

	checkRowCount(0)

	// drop collection
	status, err = c.MilvusClient.DropCollection(ctx, &milvuspb.DropCollectionRequest{
		CollectionName: collectionName,
	})
	err = merr.CheckRPCCall(status, err)
	s.NoError(err)
}
