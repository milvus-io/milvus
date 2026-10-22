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

package importv2

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/tests/integration"
	"github.com/milvus-io/milvus/tests/integration/cluster"
)

// ImportV3FeatureSuite runs the format/feature matrix through the V3 reshard
// chain. Each case reuses the same body as its V2 counterpart (the bodies live
// on importTestBase), so a V3 regression that changes rows/segments/values shows
// up against the exact same post-conditions the V2 suite asserts.
type ImportV3FeatureSuite struct {
	importTestBase
}

func (s *ImportV3FeatureSuite) SetupSuite() {
	configureImportV3(&s.MiniClusterSuite)
	s.MiniClusterSuite.SetupSuite()
}

// TestMultiFileTypesV3 runs the parquet/csv/numpy/json readers through V3.
func (s *ImportV3FeatureSuite) TestMultiFileTypesV3() {
	s.vecType = schemapb.DataType_FloatVector
	s.indexType = "HNSW"
	s.metricType = metric.L2
	fileTypes := []importutilv2.FileType{
		importutilv2.FileTypeJSON,
		importutilv2.FileTypeNumpy,
		importutilv2.FileTypeParquet,
		importutilv2.FileTypeCsv,
	}
	for _, fileType := range fileTypes {
		fileType := fileType
		s.Run(fileType.String(), func() {
			s.fileType = fileType
			s.run()
		})
	}
}

// TestImportDynamicFieldV3 covers the dynamic field path (FillDynamicData).
func (s *ImportV3FeatureSuite) TestImportDynamicFieldV3() {
	for _, fileType := range []importutilv2.FileType{importutilv2.FileTypeParquet, importutilv2.FileTypeJSON} {
		fileType := fileType
		s.Run(fileType.String(), func() {
			s.fileType = fileType
			s.testImportDynamicField()
		})
	}
}

// TestMultipleVectorFieldsV3 covers a plan that spans two vector column groups.
func (s *ImportV3FeatureSuite) TestMultipleVectorFieldsV3() {
	s.fileType = importutilv2.FileTypeParquet
	s.testMultipleVectorFields()
}

// TestStructArrayVectorV3 covers the struct-array / vector-array reader.
func (s *ImportV3FeatureSuite) TestStructArrayVectorV3() {
	s.fileType = importutilv2.FileTypeParquet
	s.vecType = schemapb.DataType_FloatVector
	s.indexType = integration.IndexHNSW
	s.metricType = metric.MaxSim
	s.runForStructArray()
}

// TestImportWithPartitionKeyV3 covers the partition-key sort/route path.
func (s *ImportV3FeatureSuite) TestImportWithPartitionKeyV3() {
	s.runImportWithPartitionKey()
}

// TestZeroRowAutoCommitV3: an empty ordinary job short-circuits to Completed
// and produces no segments.
func (s *ImportV3FeatureSuite) TestZeroRowAutoCommitV3() {
	s.runZeroRowCountTest(true)
}

// TestZeroRowUncommittedV3: the same empty job without auto_commit parks in
// Uncommitted and still produces no segments.
func (s *ImportV3FeatureSuite) TestZeroRowUncommittedV3() {
	s.runZeroRowCountTest(false)
}

// TestAutoIDAssigningIDRangeV3 exercises the ordinary AutoID path: the job
// parks in AssigningIDRange, the exact per-file range is broadcast, and Reshard
// derives PK = range.begin + row_offset (deterministic). Importing the same
// source file into two collections must assign the same PK to the same source
// row, and every produced segment must be fully queryable.
func (s *ImportV3FeatureSuite) TestAutoIDAssigningIDRangeV3() {
	const rowCount = 100

	c := s.Cluster
	ctx, cancel := context.WithTimeout(c.GetContext(), 300*time.Second)
	defer cancel()

	makeSchema := func(name string) *schemapb.CollectionSchema {
		return integration.ConstructSchema(name, dim, false,
			&schemapb.FieldSchema{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true, AutoID: true},
			&schemapb.FieldSchema{FieldID: 101, Name: "image_path", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "65535"}}},
			&schemapb.FieldSchema{FieldID: 102, Name: "embeddings", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: fmt.Sprintf("%d", dim)}}},
		)
	}

	fileSchema := makeSchema("ImportV3AutoID_src")
	insertData, filePath, err := GenerateParquetFileAndReturnInsertData(c, fileSchema, rowCount)
	s.NoError(err)
	origPaths := insertData.Data[101].(*storage.StringFieldData).Data
	s.Len(origPaths, rowCount)

	importInto := func(collectionName string) {
		schema := makeSchema(collectionName)
		marshaledSchema, err := proto.Marshal(schema)
		s.NoError(err)
		createStatus, err := c.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
			CollectionName: collectionName,
			Schema:         marshaledSchema,
			ShardsNum:      common.DefaultShardsNum,
		})
		s.NoError(merr.CheckRPCCall(createStatus, err))

		importResp, err := c.ProxyClient.ImportV2(ctx, &internalpb.ImportRequest{
			CollectionName: collectionName,
			Files:          []*internalpb.ImportFile{{Paths: []string{filePath}}},
			Options:        []*commonpb.KeyValuePair{{Key: "auto_commit", Value: "true"}},
		})
		s.NoError(err)
		s.Equal(int32(0), importResp.GetStatus().GetCode())
		s.NoError(WaitForImportDone(ctx, c, importResp.GetJobID()))

		// Every produced segment must be queryable, i.e. carry all its rows.
		segments, err := c.ShowSegments(collectionName)
		s.NoError(err)
		s.NotEmpty(segments)
		var segmentRows int64
		for _, segment := range segments {
			s.Equal(commonpb.SegmentState_Flushed, segment.GetState())
			segmentRows += segment.GetNumOfRows()
		}
		s.Equal(int64(rowCount), segmentRows, "every imported row must live in a flushed segment")
	}

	first := "ImportV3AutoID_a_" + funcutil.RandomString(8)
	second := "ImportV3AutoID_b_" + funcutil.RandomString(8)
	importInto(first)
	importInto(second)

	// The PK-to-source-row mapping must be identical in both collections: sort
	// by the generated PK and compare the image_path sequence to the source.
	firstIDs, firstPaths := s.queryIDsAndPaths(ctx, c, first)
	secondIDs, secondPaths := s.queryIDsAndPaths(ctx, c, second)

	s.Len(firstIDs, rowCount)
	s.Len(secondIDs, rowCount)
	// Generated PKs are one contiguous, unique run of the reserved width.
	s.assertContiguousPKs(firstIDs)
	s.assertContiguousPKs(secondIDs)
	s.Equal(firstPaths, origPaths, "generated PK must map to the source row order deterministically")
	s.Equal(secondPaths, origPaths, "the same source file must yield the same PK-to-row mapping")
	s.Equal(firstPaths, secondPaths)
}

// queryIDsAndPaths loads a collection, queries every row and returns the IDs
// (ascending) together with the image_path of each row in that order.
func (s *ImportV3FeatureSuite) queryIDsAndPaths(ctx context.Context, c *cluster.MiniClusterV3, collectionName string) ([]int64, []string) {
	createIndexStatus, err := c.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      "embeddings",
		IndexName:      "_default",
		ExtraParams:    integration.ConstructIndexParam(dim, "HNSW", "L2"),
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, createIndexStatus.GetErrorCode())
	s.WaitForIndexBuilt(ctx, collectionName, "embeddings")

	loadStatus, err := c.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{CollectionName: collectionName})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, loadStatus.GetErrorCode())
	s.WaitForLoad(ctx, collectionName)

	queryResult, err := c.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		CollectionName:   collectionName,
		Expr:             "id >= 0",
		OutputFields:     []string{"id", "image_path"},
		ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
	})
	s.NoError(merr.CheckRPCCall(queryResult, err))

	var ids []int64
	var paths []string
	for _, fieldData := range queryResult.GetFieldsData() {
		switch fieldData.GetFieldName() {
		case "id":
			ids = fieldData.GetScalars().GetLongData().GetData()
		case "image_path":
			paths = fieldData.GetScalars().GetStringData().GetData()
		}
	}
	s.Len(ids, len(paths))
	idToPath := make(map[int64]string, len(ids))
	for i, id := range ids {
		idToPath[id] = paths[i]
	}
	sortedIDs := append([]int64(nil), ids...)
	sort.Slice(sortedIDs, func(i, j int) bool { return sortedIDs[i] < sortedIDs[j] })
	sortedPaths := make([]string, 0, len(sortedIDs))
	for _, id := range sortedIDs {
		sortedPaths = append(sortedPaths, idToPath[id])
	}
	return sortedIDs, sortedPaths
}

// assertContiguousPKs proves the reserved range was consumed exactly once:
// the generated PKs are unique and increase by one without gaps.
func (s *ImportV3FeatureSuite) assertContiguousPKs(ids []int64) {
	seen := make(map[int64]struct{}, len(ids))
	for i := 1; i < len(ids); i++ {
		s.Equalf(ids[i-1]+1, ids[i], "generated PKs must be contiguous")
	}
	for _, id := range ids {
		_, ok := seen[id]
		s.Falsef(ok, "duplicate generated PK %d", id)
		seen[id] = struct{}{}
	}
}

// TestMultiVchannelMultiPartitionFanoutV3 imports the same file into three
// partitions of a 4-shard collection. V3 routes each row to a (vchannel,
// partition) bucket and materializes one segment per packed bucket interval, so
// the row count can be checked both per partition and in total.
func (s *ImportV3FeatureSuite) TestMultiVchannelMultiPartitionFanoutV3() {
	const (
		rowCount = 120
		shards   = 4
	)
	c := s.Cluster
	ctx, cancel := context.WithTimeout(c.GetContext(), 300*time.Second)
	defer cancel()

	collectionName := "ImportV3Fanout_" + funcutil.RandomString(8)
	schema := integration.ConstructSchema(collectionName, dim, false,
		&schemapb.FieldSchema{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true, AutoID: false},
		&schemapb.FieldSchema{FieldID: 101, Name: "image_path", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "65535"}}},
		&schemapb.FieldSchema{FieldID: 102, Name: "embeddings", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: fmt.Sprintf("%d", dim)}}},
	)
	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)
	createStatus, err := c.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      shards,
	})
	s.NoError(merr.CheckRPCCall(createStatus, err))

	partitions := []string{"v3_p0", "v3_p1", "v3_p2"}
	for _, p := range partitions {
		status, err := c.MilvusClient.CreatePartition(ctx, &milvuspb.CreatePartitionRequest{
			CollectionName: collectionName,
			PartitionName:  p,
		})
		s.NoError(merr.CheckRPCCall(status, err))
	}

	_, filePath, err := GenerateParquetFileAndReturnInsertData(c, schema, rowCount)
	s.NoError(err)
	for _, p := range partitions {
		importResp, err := c.ProxyClient.ImportV2(ctx, &internalpb.ImportRequest{
			CollectionName: collectionName,
			PartitionName:  p,
			Files:          []*internalpb.ImportFile{{Paths: []string{filePath}}},
			Options:        []*commonpb.KeyValuePair{{Key: "auto_commit", Value: "true"}},
		})
		s.NoError(err)
		s.Equal(int32(0), importResp.GetStatus().GetCode())
		s.NoError(WaitForImportDone(ctx, c, importResp.GetJobID()))
	}

	segments, err := c.ShowSegments(collectionName)
	s.NoError(err)
	s.NotEmpty(segments)

	perBucket := make(map[string]int64) // (vchannel, partitionID) -> rows
	perPartition := make(map[int64]int64)
	var total int64
	for _, segment := range segments {
		if segment.GetState() != commonpb.SegmentState_Flushed {
			continue
		}
		perBucket[fmt.Sprintf("%s|%d", segment.GetInsertChannel(), segment.GetPartitionID())] += segment.GetNumOfRows()
		perPartition[segment.GetPartitionID()] += segment.GetNumOfRows()
		total += segment.GetNumOfRows()
	}
	mlog.Info(ctx, "v3 fanout distribution", mlog.Any("perBucket", perBucket), mlog.Any("perPartition", perPartition))
	s.Equal(int64(rowCount*len(partitions)), total, "total imported rows must be preserved")
	// One bucket per partition at minimum; a sharded collection may split a
	// partition across several vchannels.
	s.GreaterOrEqual(len(perBucket), len(partitions))
	s.Equal(len(partitions), len(perPartition))
	for _, rows := range perPartition {
		s.Equal(int64(rowCount), rows)
	}

	// Every partition is queryable and returns exactly its imported rows.
	createIndexStatus, err := c.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      "embeddings",
		IndexName:      "_default",
		ExtraParams:    integration.ConstructIndexParam(dim, "HNSW", "L2"),
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, createIndexStatus.GetErrorCode())
	s.WaitForIndexBuilt(ctx, collectionName, "embeddings")
	loadStatus, err := c.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{CollectionName: collectionName})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, loadStatus.GetErrorCode())
	s.WaitForLoad(ctx, collectionName)

	for _, p := range partitions {
		queryResult, err := c.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
			CollectionName:   collectionName,
			PartitionNames:   []string{p},
			Expr:             "id >= 0",
			OutputFields:     []string{"count(*)"},
			ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
		})
		s.NoError(merr.CheckRPCCall(queryResult, err))
		s.Equal(int64(rowCount), queryResult.GetFieldsData()[0].GetScalars().GetLongData().GetData()[0],
			"partition %s must return all its imported rows", p)
	}
}

func TestImportV3FeatureSuite(t *testing.T) {
	suite.Run(t, new(ImportV3FeatureSuite))
}
