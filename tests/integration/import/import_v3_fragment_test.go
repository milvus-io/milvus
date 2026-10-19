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
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration"
)

// ImportV3FragmentSuite forces a plan with more fragments than the direct merge
// fan-in, so the DataNode must run hierarchical merge rounds before writing the
// final segment. A tiny fragment soft target combined with small read batches
// materializes many fragments; fan-in 2 turns every round above two inputs into
// an intermediate merge. The assertions are the end-result contract: the final
// segment holds every source row, unchanged.
type ImportV3FragmentSuite struct {
	importTestBase
}

func (s *ImportV3FragmentSuite) SetupSuite() {
	configureImportV3(&s.MiniClusterSuite)
	// Small read batches (< fragment target) so one bucket requires several
	// batches to fill a fragment, and a 1 MiB fragment target (the smallest
	// positive integer) so the plan owns many fragments.
	s.WithMilvusConfig(paramtable.Get().DataNodeCfg.ImportBaseBufferSize.Key, "0.1")
	s.WithMilvusConfig(paramtable.Get().DataCoordCfg.ImportFragmentSizeInMB.Key, "1")
	// Direct merge fan-in 2: any plan with >2 fragments must merge hierarchically.
	s.WithMilvusConfig(paramtable.Get().DataCoordCfg.FragmentMergeFanIn.Key, "2")
	s.MiniClusterSuite.SetupSuite()
}

func (s *ImportV3FragmentSuite) TestHierarchicalMergeV3() {
	const rowCount = 12000

	c := s.Cluster
	ctx, cancel := context.WithTimeout(c.GetContext(), 300*time.Second)
	defer cancel()

	collectionName := "ImportV3Fragment_" + funcutil.RandomString(8)
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
		ShardsNum:      1,
	})
	s.NoError(merr.CheckRPCCall(createStatus, err))

	insertData, filePath, err := GenerateParquetFileAndReturnInsertData(c, schema, rowCount)
	s.NoError(err)
	origIDs := insertData.Data[100].(*storage.Int64FieldData).Data
	origPaths := insertData.Data[101].(*storage.StringFieldData).Data

	importResp, err := c.ProxyClient.ImportV2(ctx, &internalpb.ImportRequest{
		CollectionName: collectionName,
		Files:          []*internalpb.ImportFile{{Paths: []string{filePath}}},
		Options:        []*commonpb.KeyValuePair{{Key: "auto_commit", Value: "true"}},
	})
	s.NoError(err)
	s.Equal(int32(0), importResp.GetStatus().GetCode())
	s.NoError(WaitForImportDone(ctx, c, importResp.GetJobID()))

	// The final segment(s) must account for exactly the source rows.
	segments, err := c.ShowSegments(collectionName)
	s.NoError(err)
	s.NotEmpty(segments)
	var segmentRows int64
	for _, segment := range segments {
		s.Equal(commonpb.SegmentState_Flushed, segment.GetState())
		segmentRows += segment.GetNumOfRows()
	}
	s.Equal(int64(rowCount), segmentRows)
	mlog.Info(ctx, "v3 hierarchical merge segments", mlog.Int("segments", len(segments)), mlog.Int64("rows", segmentRows))

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
	s.Len(ids, rowCount)
	s.Len(paths, rowCount)

	// Content equality: every source (id, image_path) pair survives the merge.
	idToPath := make(map[int64]string, len(origIDs))
	for i, id := range origIDs {
		idToPath[id] = origPaths[i]
	}
	seen := make(map[int64]struct{}, len(ids))
	for i, id := range ids {
		s.Equal(idToPath[id], paths[i], "row %d image_path diverged after hierarchical merge", id)
		seen[id] = struct{}{}
	}
	s.Len(seen, rowCount)
}

func TestImportV3FragmentSuite(t *testing.T) {
	suite.Run(t, new(ImportV3FragmentSuite))
}
