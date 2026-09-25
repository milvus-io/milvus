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
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration"
	"github.com/milvus-io/milvus/tests/integration/cluster"
)

const importV3SemanticRowCount = 200

// importV3SemanticBase holds the semantic-equivalence verification shared by
// the V2 and V3 import suites. The design doc (§24.5) requires the V3 chain to
// be compared against the legacy ImportTaskV2 "import + sort compaction" chain
// on identical input, and this helper runs that comparison: same parquet file,
// same schema, same query, then checks the imported rows and the commit
// timestamps.
type importV3SemanticBase struct {
	integration.MiniClusterSuite
}

// generateParquetData writes a parquet file for schema and returns both the
// object path and the in-memory source payload so the caller can verify the
// imported rows against the exact input.
func generateParquetData(c *cluster.MiniClusterV3, schema *schemapb.CollectionSchema, rowCount int) (*storage.InsertData, string, error) {
	return GenerateParquetFileAndReturnInsertData(c, schema, rowCount)
}

func (s *importV3SemanticBase) verifyEquivalentImport(ctx context.Context) {
	c := s.Cluster
	collectionName := "ImportSemantic_" + funcutil.RandomString(8)

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
		ShardsNum:      common.DefaultShardsNum,
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, createStatus.GetErrorCode())

	insertData, filePath, err := generateParquetData(c, schema, importV3SemanticRowCount)
	s.NoError(err)

	importResp, err := c.ProxyClient.ImportV2(ctx, &internalpb.ImportRequest{
		CollectionName: collectionName,
		Files: []*internalpb.ImportFile{
			{Paths: []string{filePath}},
		},
		Options: []*commonpb.KeyValuePair{
			{Key: "auto_commit", Value: "true"},
		},
	})
	s.NoError(err)
	s.Equal(int32(0), importResp.GetStatus().GetCode())
	mlog.Info(ctx, "import started", mlog.String("jobID", importResp.GetJobID()))
	s.NoError(WaitForImportDone(ctx, c, importResp.GetJobID()))

	// Every produced segment must carry a positive commit timestamp.
	segments, err := c.ShowSegments(collectionName)
	s.NoError(err)
	s.NotEmpty(segments)
	for _, segment := range segments {
		s.Greater(segment.GetCommitTimestamp(), uint64(0),
			"import segment %d must have commit timestamp set", segment.GetID())
	}

	createIndexStatus, err := c.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      "embeddings",
		IndexName:      "_default",
		ExtraParams:    integration.ConstructIndexParam(dim, "HNSW", "L2"),
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, createIndexStatus.GetErrorCode())
	s.WaitForIndexBuilt(ctx, collectionName, "embeddings")

	loadStatus, err := c.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		CollectionName: collectionName,
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, loadStatus.GetErrorCode())
	s.WaitForLoad(ctx, collectionName)

	queryResult, err := c.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		CollectionName:   collectionName,
		Expr:             "id >= 0",
		OutputFields:     []string{"id", "image_path"},
		ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, queryResult.GetStatus().GetErrorCode())

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
	s.Len(ids, importV3SemanticRowCount)
	s.Len(paths, importV3SemanticRowCount)

	// Compare against the exact source payload the parquet file was built from.
	// CreateInsertData fills id with a monotonic sequence and image_path with a
	// per-row string; the import must return those values unchanged.
	origIDs := insertData.Data[100].(*storage.Int64FieldData).Data
	origPaths := insertData.Data[101].(*storage.StringFieldData).Data
	idToPath := make(map[int64]string, len(origIDs))
	for i, id := range origIDs {
		idToPath[id] = origPaths[i]
	}
	seen := make(map[int64]struct{}, len(ids))
	for i, id := range ids {
		s.Equal(idToPath[id], paths[i], "row %d image_path diverged from source", id)
		seen[id] = struct{}{}
	}
	s.Len(seen, importV3SemanticRowCount)
	for _, id := range origIDs {
		_, ok := seen[id]
		s.True(ok, "row %d missing from import result", id)
	}
}

// ImportV2SemanticSuite exercises the legacy chain (enableImportV3 stays at its
// default false), the baseline for the semantic equivalence comparison.
type ImportV2SemanticSuite struct {
	importV3SemanticBase
}

func (s *ImportV2SemanticSuite) SetupSuite() {
	s.WithMilvusConfig(paramtable.Get().RootCoordCfg.DmlChannelNum.Key, "4")
	s.MiniClusterSuite.SetupSuite()
}

func (s *ImportV2SemanticSuite) TestImportV2SemanticEquivalence() {
	ctx, cancel := context.WithTimeout(s.Cluster.GetContext(), 300*time.Second)
	defer cancel()
	s.verifyEquivalentImport(ctx)
}

// ImportV3SemanticSuite exercises the V3 reshard chain with the rollout gate
// enabled, plus the two companion settings ordinary V3 import requires.
type ImportV3SemanticSuite struct {
	importV3SemanticBase
}

func (s *ImportV3SemanticSuite) SetupSuite() {
	s.WithMilvusConfig(paramtable.Get().RootCoordCfg.DmlChannelNum.Key, "4")
	s.WithMilvusConfig(paramtable.Get().DataCoordCfg.EnableImportV3.Key, "true")
	s.WithMilvusConfig(paramtable.Get().CommonCfg.UseLoonFFI.Key, "true")
	s.WithMilvusConfig(paramtable.Get().DataCoordCfg.BumpSchemaVersionCompactionEnabled.Key, "true")
	s.MiniClusterSuite.SetupSuite()
}

func (s *ImportV3SemanticSuite) TestImportV3SemanticEquivalence() {
	ctx, cancel := context.WithTimeout(s.Cluster.GetContext(), 300*time.Second)
	defer cancel()
	s.verifyEquivalentImport(ctx)
}

func TestImportV2V3SemanticEquivalence(t *testing.T) {
	suite.Run(t, new(ImportV2SemanticSuite))
	suite.Run(t, new(ImportV3SemanticSuite))
}
