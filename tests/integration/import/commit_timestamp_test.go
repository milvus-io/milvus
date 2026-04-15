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
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/tests/integration"
)

type CommitTimestampSuite struct {
	integration.MiniClusterSuite
}

// TestImport_CommitTimestampSetAfterCompletion verifies that all segments produced
// by a bulk-insert import have CommitTimestamp > 0 once the job completes.
func (s *CommitTimestampSuite) TestImport_CommitTimestampSetAfterCompletion() {
	const rowCount = 100

	c := s.Cluster
	ctx, cancel := context.WithTimeout(c.GetContext(), 240*time.Second)
	defer cancel()

	collectionName := "TestCommitTS_" + funcutil.RandomString(8)

	schema := integration.ConstructSchema(collectionName, dim, false)
	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)

	createCollectionStatus, err := c.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      common.DefaultShardsNum,
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, createCollectionStatus.GetErrorCode())

	filePath, err := GenerateParquetFile(s.Cluster, schema, rowCount)
	s.NoError(err)

	importResp, err := c.ProxyClient.ImportV2(ctx, &internalpb.ImportRequest{
		CollectionName: collectionName,
		Files: []*internalpb.ImportFile{
			{Paths: []string{filePath}},
		},
	})
	s.NoError(err)
	s.Equal(int32(0), importResp.GetStatus().GetCode())

	err = WaitForImportDone(ctx, c, importResp.GetJobID())
	s.NoError(err)

	AssertImportSegmentsHaveCommitTimestamp(s.T(), c, collectionName)
}

// TestImport_DataQueryableAfterCommit verifies that a Strong-consistency query
// returns all imported rows once the import job has completed.
func (s *CommitTimestampSuite) TestImport_DataQueryableAfterCommit() {
	const rowCount = 100

	c := s.Cluster
	ctx, cancel := context.WithTimeout(c.GetContext(), 300*time.Second)
	defer cancel()

	collectionName := "TestCommitTSQuery_" + funcutil.RandomString(8)

	schema := integration.ConstructSchema(collectionName, dim, false)
	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)

	createCollectionStatus, err := c.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      common.DefaultShardsNum,
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, createCollectionStatus.GetErrorCode())

	filePath, err := GenerateParquetFile(s.Cluster, schema, rowCount)
	s.NoError(err)

	importResp, err := c.ProxyClient.ImportV2(ctx, &internalpb.ImportRequest{
		CollectionName: collectionName,
		Files: []*internalpb.ImportFile{
			{Paths: []string{filePath}},
		},
	})
	s.NoError(err)
	s.Equal(int32(0), importResp.GetStatus().GetCode())

	err = WaitForImportDone(ctx, c, importResp.GetJobID())
	s.NoError(err)

	// create index on the vector field
	createIndexStatus, err := c.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      integration.FloatVecField,
		IndexName:      "_default",
		ExtraParams:    integration.ConstructIndexParam(dim, "HNSW", "L2"),
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, createIndexStatus.GetErrorCode())

	s.WaitForIndexBuilt(ctx, collectionName, integration.FloatVecField)

	// load collection
	loadStatus, err := c.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		CollectionName: collectionName,
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, loadStatus.GetErrorCode())
	s.WaitForLoad(ctx, collectionName)

	// Strong-consistency count(*) query — must see all imported rows
	queryResult, err := c.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		CollectionName:   collectionName,
		Expr:             integration.Int64Field + " >= 0",
		OutputFields:     []string{"count(*)"},
		ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
	})
	s.NoError(merr.CheckRPCCall(queryResult, err))
	count := int(queryResult.GetFieldsData()[0].GetScalars().GetLongData().GetData()[0])
	s.Equal(rowCount, count,
		"Strong-consistency query should return all %d imported rows, got %d", rowCount, count)
}

func TestCommitTimestampSuite(t *testing.T) {
	suite.Run(t, new(CommitTimestampSuite))
}
