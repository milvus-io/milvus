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

package pkindex

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/tests/integration"
)

// The suite writes the same primary keys twice and checks what a reader sees.
// The streaming node deduplicates them only if its binary carries a primary key
// index engine, which today is compiled under the "test" build tag. The cluster
// spawns bin/milvus, and the usual build of that binary has no engine. The suite
// therefore runs only when the environment variable below is set, and the binary
// must be built with the "test" tag first, for example:
//
//	go build -tags "dynamic,sonic,with_jemalloc,test" -o bin/milvus ./cmd/main.go
//
// TODO: remove the gate once a primary key index engine is part of the normal build.
const (
	dedupBinaryEnv = "MILVUS_PKINDEX_TEST_BINARY"
	dedupDim       = 8
	dedupTagField  = "tag"
	dedupPKField   = integration.Int64Field
)

type WriteDedupSuite struct {
	integration.MiniClusterSuite
}

func (s *WriteDedupSuite) SetupSuite() {
	s.WithMilvusConfig("streaming.pkindex.enabled", "true")
	s.MiniClusterSuite.SetupSuite()
}

func TestWriteDedup(t *testing.T) {
	if os.Getenv(dedupBinaryEnv) == "" {
		t.Skipf("set %s=1 and build bin/milvus with the \"test\" build tag to run this suite", dedupBinaryEnv)
	}
	suite.Run(t, new(WriteDedupSuite))
}

// TestDuplicateInsertDoesNotInflateCount checks that inserting a key a second
// time replaces the first row instead of adding one.
func (s *WriteDedupSuite) TestDuplicateInsertDoesNotInflateCount() {
	ctx, cancel := context.WithCancel(s.Cluster.GetContext())
	defer cancel()

	collectionName := "TestWriteDedup" + funcutil.GenRandomStr()
	s.createAndLoadCollection(ctx, collectionName)

	const firstRows = 100
	const secondRows = 50
	s.insert(ctx, collectionName, 0, firstRows, 1)
	s.insert(ctx, collectionName, 0, secondRows, 2)

	s.Equal(int64(firstRows), s.queryCount(ctx, collectionName), "the second insert of a key must not add a row")
	s.Equal(map[int64]int64{0: 2, 1: 2, 2: 2}, s.queryTags(ctx, collectionName, []int64{0, 1, 2}), "the row written last is the one that survives")

	s.flush(ctx, collectionName)

	s.Equal(int64(firstRows), s.queryCount(ctx, collectionName), "a flush must not change the number of rows")
	s.Equal(map[int64]int64{0: 2, 1: 2, 2: 2}, s.queryTags(ctx, collectionName, []int64{0, 1, 2}), "a flush must not bring the old rows back")
}

// TestDeleteOfAbsentKeyIsHarmless checks that a delete of keys that were never
// written does not hide a later insert of the same keys.
func (s *WriteDedupSuite) TestDeleteOfAbsentKeyIsHarmless() {
	ctx, cancel := context.WithCancel(s.Cluster.GetContext())
	defer cancel()

	collectionName := "TestWriteDedupDelete" + funcutil.GenRandomStr()
	s.createAndLoadCollection(ctx, collectionName)

	deleteResp, err := s.Cluster.MilvusClient.Delete(ctx, &milvuspb.DeleteRequest{
		CollectionName: collectionName,
		Expr:           fmt.Sprintf("%s in [1000, 1001]", dedupPKField),
	})
	s.Require().NoError(err)
	s.Require().True(merr.Ok(deleteResp.GetStatus()), deleteResp.GetStatus().GetReason())

	s.insert(ctx, collectionName, 1000, 1, 1)

	s.Equal(int64(1), s.queryCount(ctx, collectionName))
	s.Equal(map[int64]int64{1000: 1}, s.queryTags(ctx, collectionName, []int64{1000}), "the insert after the delete of the same key is visible")
}

// createAndLoadCollection creates a collection with one user provided int64
// primary key, one float vector and one int64 tag, on a single shard, and loads it.
func (s *WriteDedupSuite) createAndLoadCollection(ctx context.Context, collectionName string) {
	schema := integration.ConstructSchema(collectionName, dedupDim, false,
		&schemapb.FieldSchema{
			FieldID:      100,
			Name:         dedupPKField,
			IsPrimaryKey: true,
			DataType:     schemapb.DataType_Int64,
		},
		&schemapb.FieldSchema{
			FieldID:  101,
			Name:     integration.FloatVecField,
			DataType: schemapb.DataType_FloatVector,
			TypeParams: []*commonpb.KeyValuePair{
				{Key: common.DimKey, Value: strconv.Itoa(dedupDim)},
			},
		},
		&schemapb.FieldSchema{
			FieldID:  102,
			Name:     dedupTagField,
			DataType: schemapb.DataType_Int64,
		},
	)
	marshaledSchema, err := proto.Marshal(schema)
	s.Require().NoError(err)

	createResp, err := s.Cluster.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      1,
	})
	s.Require().NoError(err)
	s.Require().True(merr.Ok(createResp), createResp.GetReason())

	indexResp, err := s.Cluster.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      integration.FloatVecField,
		IndexName:      "_default",
		ExtraParams:    integration.ConstructIndexParam(dedupDim, integration.IndexFaissIvfFlat, metric.L2),
	})
	s.Require().NoError(err)
	s.Require().True(merr.Ok(indexResp), indexResp.GetReason())
	s.WaitForIndexBuilt(ctx, collectionName, integration.FloatVecField)

	loadResp, err := s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		CollectionName: collectionName,
	})
	s.Require().NoError(err)
	s.Require().True(merr.Ok(loadResp), loadResp.GetReason())
	s.WaitForLoad(ctx, collectionName)
}

// insert writes rowNum rows whose primary keys start at startPK, all tagged with tag.
func (s *WriteDedupSuite) insert(ctx context.Context, collectionName string, startPK int64, rowNum int, tag int64) {
	insertResp, err := s.Cluster.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
		CollectionName: collectionName,
		FieldsData: []*schemapb.FieldData{
			integration.NewInt64FieldDataWithStart(dedupPKField, rowNum, startPK),
			integration.NewFloatVectorFieldData(integration.FloatVecField, rowNum, dedupDim),
			integration.NewInt64SameFieldData(dedupTagField, rowNum, tag),
		},
		HashKeys: integration.GenerateHashKeys(rowNum),
		NumRows:  uint32(rowNum),
	})
	s.Require().NoError(err)
	s.Require().True(merr.Ok(insertResp.GetStatus()), insertResp.GetStatus().GetReason())
	s.Require().EqualValues(rowNum, insertResp.GetInsertCnt())
}

func (s *WriteDedupSuite) flush(ctx context.Context, collectionName string) {
	flushResp, err := s.Cluster.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{
		CollectionNames: []string{collectionName},
	})
	s.Require().NoError(err)
	segmentIDs, has := flushResp.GetCollSegIDs()[collectionName]
	s.Require().True(has)
	flushTs, has := flushResp.GetCollFlushTs()[collectionName]
	s.Require().True(has)
	s.WaitForFlush(ctx, segmentIDs.GetData(), flushTs, "", collectionName)
}

// queryCount reads the number of rows of the collection with strong consistency.
func (s *WriteDedupSuite) queryCount(ctx context.Context, collectionName string) int64 {
	queryResp, err := s.Cluster.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		CollectionName:   collectionName,
		OutputFields:     []string{"count(*)"},
		ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
	})
	s.Require().NoError(err)
	s.Require().True(merr.Ok(queryResp.GetStatus()), queryResp.GetStatus().GetReason())
	for _, field := range queryResp.GetFieldsData() {
		if field.GetFieldName() == "count(*)" {
			return field.GetScalars().GetLongData().GetData()[0]
		}
	}
	s.Require().Fail("the query response carries no count(*) field")
	return 0
}

// queryTags reads the tag of every primary key of pks that exists.
func (s *WriteDedupSuite) queryTags(ctx context.Context, collectionName string, pks []int64) map[int64]int64 {
	values := make([]string, 0, len(pks))
	for _, pk := range pks {
		values = append(values, strconv.FormatInt(pk, 10))
	}
	queryResp, err := s.Cluster.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		CollectionName:   collectionName,
		Expr:             fmt.Sprintf("%s in [%s]", dedupPKField, strings.Join(values, ", ")),
		OutputFields:     []string{dedupPKField, dedupTagField},
		ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
	})
	s.Require().NoError(err)
	s.Require().True(merr.Ok(queryResp.GetStatus()), queryResp.GetStatus().GetReason())

	var keys, tags []int64
	for _, field := range queryResp.GetFieldsData() {
		switch field.GetFieldName() {
		case dedupPKField:
			keys = field.GetScalars().GetLongData().GetData()
		case dedupTagField:
			tags = field.GetScalars().GetLongData().GetData()
		}
	}
	s.Require().Len(tags, len(keys))
	tagOf := make(map[int64]int64, len(keys))
	for i, key := range keys {
		tagOf[key] = tags[i]
	}
	return tagOf
}
