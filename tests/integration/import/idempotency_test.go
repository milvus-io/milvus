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
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/tests/integration"
)

// IdempotencySuite covers the client-visible contract of an idempotent BulkImport
// end to end: the key travels as gRPC metadata from the client, through the proxy
// and DataCoord, into the broadcast message property, and is deduplicated by the
// StreamingCoord broadcaster.
//
// The unit tests prove each hop in isolation. Only these tests prove the hops are
// actually connected: a key dropped anywhere along the way leaves every unit test
// green while silently disabling deduplication.
type IdempotencySuite struct {
	integration.MiniClusterSuite
}

// withIdempotencyKey attaches the key the way a real client does — as outgoing
// gRPC metadata, which the proxy reads back as incoming metadata.
func withIdempotencyKey(ctx context.Context, key string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, util.HeaderIdempotencyKey, key)
}

func (s *IdempotencySuite) createCollection(ctx context.Context, name string) *schemapb.CollectionSchema {
	schema := integration.ConstructSchema(name, dim, false)
	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)

	status, err := s.Cluster.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName: name,
		Schema:         marshaledSchema,
		ShardsNum:      common.DefaultShardsNum,
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, status.GetErrorCode())
	return schema
}

// countRows runs a Strong-consistency count(*), which is what makes a duplicated
// import observable: a second job would double the row count.
func (s *IdempotencySuite) countRows(ctx context.Context, collectionName string) int {
	c := s.Cluster

	createIndexStatus, err := c.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      integration.FloatVecField,
		IndexName:      "_default",
		ExtraParams:    integration.ConstructIndexParam(dim, "HNSW", "L2"),
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, createIndexStatus.GetErrorCode())
	s.WaitForIndexBuilt(ctx, collectionName, integration.FloatVecField)

	loadStatus, err := c.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		CollectionName: collectionName,
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, loadStatus.GetErrorCode())
	s.WaitForLoad(ctx, collectionName)

	queryResult, err := c.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		CollectionName:   collectionName,
		Expr:             integration.Int64Field + " >= 0",
		OutputFields:     []string{"count(*)"},
		ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
	})
	s.NoError(merr.CheckRPCCall(queryResult, err))
	return int(queryResult.GetFieldsData()[0].GetScalars().GetLongData().GetData()[0])
}

// TestImport_SameKeyImportsOnce is the core end-to-end guarantee: an orchestrator
// that retries after losing the response gets the ORIGINAL jobID back and the data
// is imported exactly once.
func (s *IdempotencySuite) TestImport_SameKeyImportsOnce() {
	const rowCount = 100

	c := s.Cluster
	ctx, cancel := context.WithTimeout(c.GetContext(), 300*time.Second)
	defer cancel()

	collectionName := "TestImportIdem_" + funcutil.RandomString(8)
	schema := s.createCollection(ctx, collectionName)

	filePath, err := GenerateParquetFile(c, schema, rowCount)
	s.NoError(err)

	req := &internalpb.ImportRequest{
		CollectionName: collectionName,
		Files:          []*internalpb.ImportFile{{Paths: []string{filePath}}},
	}
	keyedCtx := withIdempotencyKey(ctx, "run-1-batch-1-"+funcutil.RandomString(8))

	first, err := c.ProxyClient.ImportV2(keyedCtx, req)
	s.NoError(err)
	s.Equal(int32(0), first.GetStatus().GetCode())
	s.NotEmpty(first.GetJobID())

	// The retry an orchestrator issues after it crashed before persisting the jobID.
	second, err := c.ProxyClient.ImportV2(keyedCtx, req)
	s.NoError(err)
	s.Equal(int32(0), second.GetStatus().GetCode())
	s.Equal(first.GetJobID(), second.GetJobID(),
		"a retry carrying the same idempotency key must resolve to the original job")

	s.NoError(WaitForImportDone(ctx, c, first.GetJobID()))

	// The assertion that actually rules out a duplicate import: one job's worth of rows.
	s.Equal(rowCount, s.countRows(ctx, collectionName),
		"the retried import must not have written a second copy of the data")
}

// TestImport_DifferentKeysCreateDistinctJobs is the negative control for the test
// above: without it, an implementation that returned the first job for EVERY
// request would pass the same-key assertions.
func (s *IdempotencySuite) TestImport_DifferentKeysCreateDistinctJobs() {
	const rowCount = 10

	c := s.Cluster
	ctx, cancel := context.WithTimeout(c.GetContext(), 300*time.Second)
	defer cancel()

	collectionName := "TestImportIdemDistinct_" + funcutil.RandomString(8)
	schema := s.createCollection(ctx, collectionName)

	filePath, err := GenerateParquetFile(c, schema, rowCount)
	s.NoError(err)

	req := &internalpb.ImportRequest{
		CollectionName: collectionName,
		Files:          []*internalpb.ImportFile{{Paths: []string{filePath}}},
	}
	suffix := funcutil.RandomString(8)

	first, err := c.ProxyClient.ImportV2(withIdempotencyKey(ctx, "key-a-"+suffix), req)
	s.NoError(err)
	s.Equal(int32(0), first.GetStatus().GetCode())

	second, err := c.ProxyClient.ImportV2(withIdempotencyKey(ctx, "key-b-"+suffix), req)
	s.NoError(err)
	s.Equal(int32(0), second.GetStatus().GetCode())

	s.NotEqual(first.GetJobID(), second.GetJobID(),
		"distinct idempotency keys are distinct logical requests and must get distinct jobs")

	s.NoError(WaitForImportDone(ctx, c, first.GetJobID()))
	s.NoError(WaitForImportDone(ctx, c, second.GetJobID()))
}

// TestImport_SameKeyDifferentFilesRejected pins the refusal that keeps a key from
// silently swallowing data: reusing a key with a different file set returns an
// error rather than the original jobID plus a success the caller would believe.
func (s *IdempotencySuite) TestImport_SameKeyDifferentFilesRejected() {
	const rowCount = 10

	c := s.Cluster
	ctx, cancel := context.WithTimeout(c.GetContext(), 300*time.Second)
	defer cancel()

	collectionName := "TestImportIdemMismatch_" + funcutil.RandomString(8)
	schema := s.createCollection(ctx, collectionName)

	firstFile, err := GenerateParquetFile(c, schema, rowCount)
	s.NoError(err)
	secondFile, err := GenerateParquetFile(c, schema, rowCount)
	s.NoError(err)
	s.NotEqual(firstFile, secondFile)

	keyedCtx := withIdempotencyKey(ctx, "run-mismatch-"+funcutil.RandomString(8))

	first, err := c.ProxyClient.ImportV2(keyedCtx, &internalpb.ImportRequest{
		CollectionName: collectionName,
		Files:          []*internalpb.ImportFile{{Paths: []string{firstFile}}},
	})
	s.NoError(err)
	s.Equal(int32(0), first.GetStatus().GetCode())

	mismatch, err := c.ProxyClient.ImportV2(keyedCtx, &internalpb.ImportRequest{
		CollectionName: collectionName,
		Files:          []*internalpb.ImportFile{{Paths: []string{secondFile}}},
	})
	s.NoError(err)
	s.NotEqual(int32(0), mismatch.GetStatus().GetCode(),
		"reusing a key with a different file set must be refused, not silently deduplicated")
	s.Empty(mismatch.GetJobID())
	s.Contains(mismatch.GetStatus().GetReason(), "idempotency key was reused with a different file set")

	// The original job is untouched by the refused request.
	s.NoError(WaitForImportDone(ctx, c, first.GetJobID()))
	s.Equal(rowCount, s.countRows(ctx, collectionName))
}

func TestIdempotencySuite(t *testing.T) {
	suite.Run(t, new(IdempotencySuite))
}
