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
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration"
	"github.com/milvus-io/milvus/tests/integration/cluster"
)

const twoPCRowCount = 100

type TwoPCImportSuite struct {
	integration.MiniClusterSuite
}

func (s *TwoPCImportSuite) SetupSuite() {
	s.WithMilvusConfig(paramtable.Get().RootCoordCfg.DmlChannelNum.Key, "4")
	s.MiniClusterSuite.SetupSuite()
}

// WaitForImportState polls GetImportProgress until the job reaches the target state.
// Returns error on context timeout or if the job reaches Failed when not expected.
func WaitForImportState(ctx context.Context, c *cluster.MiniClusterV3, jobID string, targetState internalpb.ImportJobState) error {
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for import job %s to reach state %s", jobID, targetState.String())
		default:
		}

		resp, err := c.ProxyClient.GetImportProgress(ctx, &internalpb.GetImportProgressRequest{
			JobID: jobID,
		})
		if err != nil {
			return err
		}
		if err = merr.Error(resp.GetStatus()); err != nil {
			return err
		}

		currentState := resp.GetState()
		if currentState == targetState {
			return nil
		}

		// If we're waiting for a non-Failed state and the job has failed, return error immediately.
		if targetState != internalpb.ImportJobState_Failed && currentState == internalpb.ImportJobState_Failed {
			return fmt.Errorf("import job %s failed unexpectedly: %s", jobID, resp.GetReason())
		}

		log.Info("waiting for import state",
			zap.String("jobID", jobID),
			zap.String("current", currentState.String()),
			zap.String("target", targetState.String()),
			zap.Int64("progress", resp.GetProgress()))
		time.Sleep(1 * time.Second)
	}
}

// createCollectionForImport creates a simple collection with Int64 PK + VarChar + FloatVector fields.
func (s *TwoPCImportSuite) createCollectionForImport(ctx context.Context) string {
	collectionName := "TestTwoPCImport_" + funcutil.RandomString(8)

	schema := integration.ConstructSchema(collectionName, dim, false,
		&schemapb.FieldSchema{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true, AutoID: false},
		&schemapb.FieldSchema{FieldID: 101, Name: "image_path", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "65535"}}},
		&schemapb.FieldSchema{FieldID: 102, Name: "embeddings", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "128"}}},
	)
	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)

	createStatus, err := s.Cluster.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      common.DefaultShardsNum,
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, createStatus.GetErrorCode())

	return collectionName
}

// importWithAutoCommit triggers an import with auto_commit=false and returns the job ID.
func (s *TwoPCImportSuite) importWithAutoCommit(ctx context.Context, collectionName string, autoCommit bool) string {
	schema := integration.ConstructSchema(collectionName, dim, false,
		&schemapb.FieldSchema{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true, AutoID: false},
		&schemapb.FieldSchema{FieldID: 101, Name: "image_path", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "65535"}}},
		&schemapb.FieldSchema{FieldID: 102, Name: "embeddings", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "128"}}},
	)

	filePath, err := GenerateParquetFile(s.Cluster, schema, twoPCRowCount)
	s.NoError(err)

	options := []*commonpb.KeyValuePair{}
	if !autoCommit {
		options = append(options, &commonpb.KeyValuePair{Key: "auto_commit", Value: "false"})
	}

	importResp, err := s.Cluster.ProxyClient.ImportV2(ctx, &internalpb.ImportRequest{
		CollectionName: collectionName,
		Files: []*internalpb.ImportFile{
			{Paths: []string{filePath}},
		},
		Options: options,
	})
	s.NoError(err)
	s.Equal(int32(0), importResp.GetStatus().GetCode())
	log.Info("Import started", zap.String("jobID", importResp.GetJobID()))

	return importResp.GetJobID()
}

// loadAndQuery loads the collection, waits for load, and queries row count.
func (s *TwoPCImportSuite) loadAndQuery(ctx context.Context, collectionName string) int {
	c := s.Cluster

	// Create index first
	createIndexStatus, err := c.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      "embeddings",
		IndexName:      "_default",
		ExtraParams:    integration.ConstructIndexParam(dim, "HNSW", "L2"),
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, createIndexStatus.GetErrorCode())
	s.WaitForIndexBuilt(ctx, collectionName, "embeddings")

	// Load
	loadStatus, err := c.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		CollectionName: collectionName,
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, loadStatus.GetErrorCode())
	s.WaitForLoad(ctx, collectionName)

	// Query with strong consistency to see committed data
	queryResult, err := c.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		CollectionName:   collectionName,
		Expr:             "id >= 0",
		OutputFields:     []string{"count(*)"},
		ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
	})
	err = merr.CheckRPCCall(queryResult, err)
	s.NoError(err)

	for _, field := range queryResult.GetFieldsData() {
		if field.GetFieldName() == "count(*)" {
			return int(field.GetScalars().GetLongData().GetData()[0])
		}
	}
	return 0
}

// queryRowCount queries the row count assuming collection is already loaded.
func (s *TwoPCImportSuite) queryRowCount(ctx context.Context, collectionName string) int {
	queryResult, err := s.Cluster.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		CollectionName:   collectionName,
		Expr:             "id >= 0",
		OutputFields:     []string{"count(*)"},
		ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
	})
	err = merr.CheckRPCCall(queryResult, err)
	s.NoError(err)

	for _, field := range queryResult.GetFieldsData() {
		if field.GetFieldName() == "count(*)" {
			return int(field.GetScalars().GetLongData().GetData()[0])
		}
	}
	return 0
}

// commitImport calls CommitImport via MixCoord client.
func (s *TwoPCImportSuite) commitImport(ctx context.Context, jobID int64) (*commonpb.Status, error) {
	mixCoordClient := s.Cluster.DefaultMixCoord().MustGetClient(ctx)
	return mixCoordClient.CommitImport(ctx, &datapb.CommitImportRequest{
		JobId: jobID,
	})
}

// abortImport calls AbortImport via MixCoord client.
func (s *TwoPCImportSuite) abortImport(ctx context.Context, jobID int64) (*commonpb.Status, error) {
	mixCoordClient := s.Cluster.DefaultMixCoord().MustGetClient(ctx)
	return mixCoordClient.AbortImport(ctx, &datapb.AbortImportRequest{
		JobId: jobID,
	})
}

// getJobID parses the job ID string to int64.
func getJobID(jobID string) int64 {
	id, err := strconv.ParseInt(jobID, 10, 64)
	if err != nil {
		panic(fmt.Sprintf("invalid job ID %q: %v", jobID, err))
	}
	return id
}

// TestImportWithManualCommit tests the full 2PC happy path:
// auto_commit=false -> wait Uncommitted -> CommitImport -> wait Completed -> verify data visible
func (s *TwoPCImportSuite) TestImportWithManualCommit() {
	ctx, cancel := context.WithTimeout(s.Cluster.GetContext(), 240*time.Second)
	defer cancel()

	collectionName := s.createCollectionForImport(ctx)
	jobID := s.importWithAutoCommit(ctx, collectionName, false)

	// Wait for Uncommitted state
	err := WaitForImportState(ctx, s.Cluster, jobID, internalpb.ImportJobState_Uncommitted)
	s.NoError(err)
	log.Info("import job reached Uncommitted state", zap.String("jobID", jobID))

	// Commit the import
	status, err := s.commitImport(ctx, getJobID(jobID))
	err = merr.CheckRPCCall(status, err)
	s.NoError(err)
	log.Info("CommitImport succeeded", zap.String("jobID", jobID))

	// Wait for Completed state
	err = WaitForImportState(ctx, s.Cluster, jobID, internalpb.ImportJobState_Completed)
	s.NoError(err)
	log.Info("import job completed", zap.String("jobID", jobID))

	// Verify data is visible
	count := s.loadAndQuery(ctx, collectionName)
	s.Equal(twoPCRowCount, count)
}

// TestImportWithAbort tests abort path:
// auto_commit=false -> wait Uncommitted -> AbortImport -> wait Failed -> verify data NOT visible
func (s *TwoPCImportSuite) TestImportWithAbort() {
	ctx, cancel := context.WithTimeout(s.Cluster.GetContext(), 240*time.Second)
	defer cancel()

	collectionName := s.createCollectionForImport(ctx)
	jobID := s.importWithAutoCommit(ctx, collectionName, false)

	// Wait for Uncommitted state
	err := WaitForImportState(ctx, s.Cluster, jobID, internalpb.ImportJobState_Uncommitted)
	s.NoError(err)

	// Abort the import
	status, err := s.abortImport(ctx, getJobID(jobID))
	err = merr.CheckRPCCall(status, err)
	s.NoError(err)
	log.Info("AbortImport succeeded", zap.String("jobID", jobID))

	// Wait for Failed state
	err = WaitForImportState(ctx, s.Cluster, jobID, internalpb.ImportJobState_Failed)
	s.NoError(err)

	// Verify data is NOT visible
	count := s.loadAndQuery(ctx, collectionName)
	s.Equal(0, count)
}

// TestImportAutoCommitDefault tests backward compatibility:
// default import (no auto_commit option) -> wait Completed -> verify data visible
func (s *TwoPCImportSuite) TestImportAutoCommitDefault() {
	ctx, cancel := context.WithTimeout(s.Cluster.GetContext(), 240*time.Second)
	defer cancel()

	collectionName := s.createCollectionForImport(ctx)
	jobID := s.importWithAutoCommit(ctx, collectionName, true)

	// With default auto_commit=true, job should go directly to Completed
	err := WaitForImportDone(ctx, s.Cluster, jobID)
	s.NoError(err)

	// Verify data is visible
	count := s.loadAndQuery(ctx, collectionName)
	s.Equal(twoPCRowCount, count)
}

// TestCommitImportIdempotent tests that calling CommitImport twice succeeds both times.
func (s *TwoPCImportSuite) TestCommitImportIdempotent() {
	ctx, cancel := context.WithTimeout(s.Cluster.GetContext(), 240*time.Second)
	defer cancel()

	collectionName := s.createCollectionForImport(ctx)
	jobID := s.importWithAutoCommit(ctx, collectionName, false)

	// Wait for Uncommitted state
	err := WaitForImportState(ctx, s.Cluster, jobID, internalpb.ImportJobState_Uncommitted)
	s.NoError(err)

	// First commit
	status, err := s.commitImport(ctx, getJobID(jobID))
	err = merr.CheckRPCCall(status, err)
	s.NoError(err)

	// Second commit (idempotent) - should also succeed
	status, err = s.commitImport(ctx, getJobID(jobID))
	err = merr.CheckRPCCall(status, err)
	s.NoError(err)

	// Wait for Completed
	err = WaitForImportState(ctx, s.Cluster, jobID, internalpb.ImportJobState_Completed)
	s.NoError(err)

	// Verify data
	count := s.loadAndQuery(ctx, collectionName)
	s.Equal(twoPCRowCount, count)
}

// TestAbortAfterCommit tests that aborting after commit returns an error.
func (s *TwoPCImportSuite) TestAbortAfterCommit() {
	ctx, cancel := context.WithTimeout(s.Cluster.GetContext(), 240*time.Second)
	defer cancel()

	collectionName := s.createCollectionForImport(ctx)
	jobID := s.importWithAutoCommit(ctx, collectionName, false)

	err := WaitForImportState(ctx, s.Cluster, jobID, internalpb.ImportJobState_Uncommitted)
	s.NoError(err)

	// Commit first
	status, err := s.commitImport(ctx, getJobID(jobID))
	err = merr.CheckRPCCall(status, err)
	s.NoError(err)

	// Wait for Completed
	err = WaitForImportState(ctx, s.Cluster, jobID, internalpb.ImportJobState_Completed)
	s.NoError(err)

	// Abort should fail
	status, err = s.abortImport(ctx, getJobID(jobID))
	err = merr.CheckRPCCall(status, err)
	s.Error(err)
	log.Info("AbortImport after commit correctly returned error", zap.Error(err))
}

// TestCommitAfterAbort tests that committing after abort returns an error.
func (s *TwoPCImportSuite) TestCommitAfterAbort() {
	ctx, cancel := context.WithTimeout(s.Cluster.GetContext(), 240*time.Second)
	defer cancel()

	collectionName := s.createCollectionForImport(ctx)
	jobID := s.importWithAutoCommit(ctx, collectionName, false)

	err := WaitForImportState(ctx, s.Cluster, jobID, internalpb.ImportJobState_Uncommitted)
	s.NoError(err)

	// Abort first
	status, err := s.abortImport(ctx, getJobID(jobID))
	err = merr.CheckRPCCall(status, err)
	s.NoError(err)

	// Wait for Failed
	err = WaitForImportState(ctx, s.Cluster, jobID, internalpb.ImportJobState_Failed)
	s.NoError(err)

	// Commit should fail
	status, err = s.commitImport(ctx, getJobID(jobID))
	err = merr.CheckRPCCall(status, err)
	s.Error(err)
	log.Info("CommitImport after abort correctly returned error", zap.Error(err))
}

// TestQueryBeforeCommit tests that data is invisible before commit and visible after.
func (s *TwoPCImportSuite) TestQueryBeforeCommit() {
	ctx, cancel := context.WithTimeout(s.Cluster.GetContext(), 240*time.Second)
	defer cancel()

	collectionName := s.createCollectionForImport(ctx)
	jobID := s.importWithAutoCommit(ctx, collectionName, false)

	// Wait for Uncommitted state
	err := WaitForImportState(ctx, s.Cluster, jobID, internalpb.ImportJobState_Uncommitted)
	s.NoError(err)

	// Load and query BEFORE commit - data should be invisible
	countBefore := s.loadAndQuery(ctx, collectionName)
	s.Equal(0, countBefore, "data should be invisible before commit")
	log.Info("verified data invisible before commit", zap.Int("count", countBefore))

	// Now commit
	status, err := s.commitImport(ctx, getJobID(jobID))
	err = merr.CheckRPCCall(status, err)
	s.NoError(err)

	// Wait for Completed
	err = WaitForImportState(ctx, s.Cluster, jobID, internalpb.ImportJobState_Completed)
	s.NoError(err)

	// Query AFTER commit - data should be visible
	countAfter := s.queryRowCount(ctx, collectionName)
	s.Equal(twoPCRowCount, countAfter, "data should be visible after commit")
	log.Info("verified data visible after commit", zap.Int("count", countAfter))
}

func TestTwoPCImport(t *testing.T) {
	suite.Run(t, new(TwoPCImportSuite))
}
