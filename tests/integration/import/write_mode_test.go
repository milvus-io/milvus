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
	"bytes"
	"context"
	"fmt"
	"path"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/testutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration"
	"github.com/milvus-io/milvus/tests/integration/cluster"
)

// WriteModeSuite covers the import write_mode=Delete and write_mode=Upsert end-to-end paths.
//
// Both modes require the collection's data segments to be manifest-bearing (storage v3),
// because a delete produced by these imports only reaches an already-loaded segment through
// the manifest bump that L0 compaction performs and QueryCoord's SegmentChecker turning that
// into a segment Reopen — there is no delegator fast-path. common.storage.useLoonFFI=true makes
// every segment carry a manifest from the moment it is written, so a plain APPEND import already
// produces qualifying segments; no separate storage-version-upgrade compaction is needed.
//
// dataCoord.compaction.levelzero.forceTrigger.deltalogMinNum is lowered to 1 so L0 compaction
// fires promptly instead of waiting for the default deltalog-count threshold (mirrors
// tests/integration/compaction/l0_compaction_test.go).
type WriteModeSuite struct {
	integration.MiniClusterSuite
}

func (s *WriteModeSuite) SetupSuite() {
	s.WithMilvusConfig(paramtable.Get().CommonCfg.UseLoonFFI.Key, "true")
	s.WithMilvusConfig(paramtable.Get().DataCoordCfg.LevelZeroCompactionTriggerDeltalogMinNum.Key, "1")
	s.MiniClusterSuite.SetupSuite()
}

func TestWriteModeSuite(t *testing.T) {
	suite.Run(t, new(WriteModeSuite))
}

// buildWriteModeSchema returns a schema with an int64 (non-autoID) primary key, a float vector
// field, and a varchar field used as an upsert marker.
func buildWriteModeSchema(collectionName string) *schemapb.CollectionSchema {
	return integration.ConstructSchema(collectionName, dim, false,
		&schemapb.FieldSchema{
			FieldID:      100,
			Name:         integration.Int64Field,
			IsPrimaryKey: true,
			DataType:     schemapb.DataType_Int64,
			AutoID:       false,
		},
		&schemapb.FieldSchema{
			FieldID:  101,
			Name:     integration.FloatVecField,
			DataType: schemapb.DataType_FloatVector,
			TypeParams: []*commonpb.KeyValuePair{
				{Key: common.DimKey, Value: fmt.Sprintf("%d", dim)},
			},
		},
		&schemapb.FieldSchema{
			FieldID:  102,
			Name:     integration.VarCharField,
			DataType: schemapb.DataType_VarChar,
			TypeParams: []*commonpb.KeyValuePair{
				{Key: common.MaxLengthKey, Value: "256"},
			},
		},
	)
}

// writeDeleteKeyParquetFile writes a parquet file holding only the given primary keys under
// pkFieldName, plus one extra column (extraFieldName) that is not part of the collection schema.
// The extra column proves the datanode reads delete-key files through a primary-key-projected
// schema that ignores every other column, rather than validating against the full schema.
func writeDeleteKeyParquetFile(c *cluster.MiniClusterV3, pkFieldName string, pks []int64, extraFieldName string) (string, error) {
	mem := memory.NewGoAllocator()

	pkBuilder := array.NewInt64Builder(mem)
	defer pkBuilder.Release()
	pkBuilder.AppendValues(pks, nil)
	pkArr := pkBuilder.NewArray()
	defer pkArr.Release()

	extraBuilder := array.NewStringBuilder(mem)
	defer extraBuilder.Release()
	extraValues := make([]string, len(pks))
	for i := range extraValues {
		extraValues[i] = fmt.Sprintf("not-in-collection-schema-%d", i)
	}
	extraBuilder.AppendValues(extraValues, nil)
	extraArr := extraBuilder.NewArray()
	defer extraArr.Release()

	pqSchema := arrow.NewSchema([]arrow.Field{
		{Name: pkFieldName, Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: extraFieldName, Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)

	buf := bytes.NewBuffer(make([]byte, 0, 10240))
	fw, err := pqarrow.NewFileWriter(pqSchema, buf,
		parquet.NewWriterProperties(parquet.WithMaxRowGroupLength(int64(len(pks)))),
		pqarrow.DefaultWriterProps())
	if err != nil {
		return "", err
	}

	recordBatch := array.NewRecord(pqSchema, []arrow.Array{pkArr, extraArr}, int64(len(pks)))
	defer recordBatch.Release()
	if err := fw.Write(recordBatch); err != nil {
		return "", err
	}
	if err := fw.Close(); err != nil {
		return "", err
	}

	filePath := path.Join(c.RootPath(), "parquet", uuid.New().String()+".parquet")
	if err := c.ChunkManager.Write(context.Background(), filePath, buf.Bytes()); err != nil {
		return "", err
	}
	return filePath, nil
}

// writeUpsertMarkerParquetFile writes a full-row parquet file for the same row count as the
// original import, with the varchar field identified by markerFieldID overridden to a
// distinctive value on every row. Primary keys are generated the same deterministic way
// GenerateParquetFileAndReturnInsertData does (0..rowCount-1), so they line up with the
// original import's primary keys.
func writeUpsertMarkerParquetFile(c *cluster.MiniClusterV3, schema *schemapb.CollectionSchema, rowCount int, markerFieldID int64, markerValue string) (string, error) {
	insertData, err := testutil.CreateInsertData(schema, rowCount)
	if err != nil {
		return "", err
	}
	markerData, ok := insertData.Data[markerFieldID].(*storage.StringFieldData)
	if !ok {
		return "", errors.New("marker field is not a varchar field")
	}
	values := make([]string, rowCount)
	for i := range values {
		values[i] = markerValue
	}
	markerData.Data = values

	buf, err := searilizeParquetFile(schema, insertData, rowCount)
	if err != nil {
		return "", err
	}
	filePath := path.Join(c.RootPath(), "parquet", uuid.New().String()+".parquet")
	if err := c.ChunkManager.Write(context.Background(), filePath, buf.Bytes()); err != nil {
		return "", err
	}
	return filePath, nil
}

func (s *WriteModeSuite) queryStrongCount(ctx context.Context, collectionName string) int64 {
	queryResult, err := s.Cluster.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		CollectionName:   collectionName,
		Expr:             "",
		OutputFields:     []string{"count(*)"},
		ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
	})
	s.Require().NoError(merr.CheckRPCCall(queryResult, err))
	return queryResult.GetFieldsData()[0].GetScalars().GetLongData().GetData()[0]
}

// waitForStrongCount polls a Strong-consistency count(*) until it reaches want or timeout
// elapses, returning the last observed value either way. Visibility of an import-produced
// delete is minutes-scale (L0 compaction, then QueryCoord's SegmentChecker Reopen), not
// seconds, so this must be polled rather than checked once.
func (s *WriteModeSuite) waitForStrongCount(ctx context.Context, collectionName string, want int64, timeout time.Duration) int64 {
	deadline := time.Now().Add(timeout)
	for {
		last := s.queryStrongCount(ctx, collectionName)
		mlog.Info(ctx, "polling row count while waiting for L0 compaction + Reopen",
			mlog.String("collection", collectionName), mlog.Int64("want", want), mlog.Int64("got", last))
		if last == want {
			return last
		}
		if time.Now().After(deadline) {
			return last
		}
		select {
		case <-ctx.Done():
			return last
		case <-time.After(3 * time.Second):
		}
	}
}

// queryMarkers runs a Strong-consistency query for (pk, marker) over the whole collection.
func (s *WriteModeSuite) queryMarkers(ctx context.Context, collectionName string) (pks []int64, markers []string) {
	queryResult, err := s.Cluster.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		CollectionName:   collectionName,
		Expr:             fmt.Sprintf("%s >= 0", integration.Int64Field),
		OutputFields:     []string{integration.Int64Field, integration.VarCharField},
		ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
	})
	s.Require().NoError(merr.CheckRPCCall(queryResult, err))
	for _, fd := range queryResult.GetFieldsData() {
		switch fd.GetFieldName() {
		case integration.Int64Field:
			pks = fd.GetScalars().GetLongData().GetData()
		case integration.VarCharField:
			markers = fd.GetScalars().GetStringData().GetData()
		}
	}
	return pks, markers
}

// waitForUpsertMarkers polls Strong-consistency (pk, marker) query results until the collection
// holds exactly wantTotal rows and every one of them carries wantMarker, or timeout elapses.
// Row count alone cannot gate an upsert wait: an upsert's net row count is N both before and
// after it takes effect (the companion delete removes exactly as many rows as the new write
// adds), so a count-only gate is satisfied by the pre-upsert state and proves nothing. The
// marker value is the only observable that actually changes, so convergence must be defined on
// it. Each poll logs the marker distribution (how many rows carry the new marker vs. something
// else) so a failure shows how far the upsert got, not just that it timed out.
func (s *WriteModeSuite) waitForUpsertMarkers(ctx context.Context, collectionName string, wantMarker string, wantTotal int, timeout time.Duration) ([]int64, []string) {
	deadline := time.Now().Add(timeout)
	var pks []int64
	var markers []string
	for {
		pks, markers = s.queryMarkers(ctx, collectionName)

		newCount := 0
		for _, m := range markers {
			if m == wantMarker {
				newCount++
			}
		}
		mlog.Info(ctx, "polling upsert marker convergence while waiting for L0 compaction + Reopen",
			mlog.String("collection", collectionName),
			mlog.Int("wantTotal", wantTotal),
			mlog.Int("totalRows", len(markers)),
			mlog.Int("rowsWithNewMarker", newCount),
			mlog.Int("rowsWithOldMarker", len(markers)-newCount))

		if len(markers) == wantTotal && newCount == wantTotal {
			return pks, markers
		}
		if time.Now().After(deadline) {
			return pks, markers
		}
		select {
		case <-ctx.Done():
			return pks, markers
		case <-time.After(3 * time.Second):
		}
	}
}

// TestWriteModeDelete imports N rows via a normal APPEND import, then imports a delete-key
// file for half of those primary keys under write_mode=Delete. The delete-key file also
// carries a column not present in the collection schema, proving the datanode reads it
// through a primary-key projection that ignores extra columns rather than failing on them.
// Row count must eventually settle at N/2, and the surviving half must be exactly the PKs
// that were not targeted by the delete.
func (s *WriteModeSuite) TestWriteModeDelete() {
	const rowCount = 200
	half := rowCount / 2

	c := s.Cluster
	ctx, cancel := context.WithTimeout(c.GetContext(), 10*time.Minute)
	defer cancel()

	collectionName := "TestWriteModeDelete_" + funcutil.RandomString(8)
	schema := buildWriteModeSchema(collectionName)
	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)

	createCollectionStatus, err := c.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      common.DefaultShardsNum,
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, createCollectionStatus.GetErrorCode())

	insertData, filePath, err := GenerateParquetFileAndReturnInsertData(c, schema, rowCount)
	s.NoError(err)
	pks := insertData.Data[int64(100)].GetDataRows().([]int64)
	s.Require().Len(pks, rowCount)

	importResp, err := c.ProxyClient.ImportV2(ctx, &internalpb.ImportRequest{
		CollectionName: collectionName,
		Files:          []*internalpb.ImportFile{{Paths: []string{filePath}}},
	})
	s.NoError(err)
	s.Equal(int32(0), importResp.GetStatus().GetCode(), importResp.GetStatus().GetReason())
	s.NoError(WaitForImportDone(ctx, c, importResp.GetJobID()))

	createIndexStatus, err := c.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      integration.FloatVecField,
		IndexName:      "_default",
		ExtraParams:    integration.ConstructIndexParam(dim, integration.IndexFaissIvfFlat, metric.L2),
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

	initialCount := s.queryStrongCount(ctx, collectionName)
	s.Equal(int64(rowCount), initialCount, "all imported rows should be visible before the delete import")

	deletePKs := append([]int64(nil), pks[:half]...)
	remainingPKs := append([]int64(nil), pks[half:]...)

	deleteFilePath, err := writeDeleteKeyParquetFile(c, integration.Int64Field, deletePKs, "not_in_schema_field")
	s.NoError(err)

	deleteImportResp, err := c.ProxyClient.ImportV2(ctx, &internalpb.ImportRequest{
		CollectionName: collectionName,
		Files:          []*internalpb.ImportFile{{Paths: []string{deleteFilePath}}},
		WriteMode:      internalpb.ImportWriteMode_Delete,
	})
	s.NoError(err)
	s.Equal(int32(0), deleteImportResp.GetStatus().GetCode(), deleteImportResp.GetStatus().GetReason())
	s.NoError(WaitForImportDone(ctx, c, deleteImportResp.GetJobID()))

	finalCount := s.waitForStrongCount(ctx, collectionName, int64(rowCount-half), 6*time.Minute)
	s.Equal(int64(rowCount-half), finalCount,
		"row count should settle at N/2 once L0 compaction folds the delete-mode import's deletes "+
			"into the data segment's deltalog and QueryCoord reopens the segment")

	remainingQuery, err := c.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		CollectionName:   collectionName,
		Expr:             fmt.Sprintf("%s >= 0", integration.Int64Field),
		OutputFields:     []string{integration.Int64Field},
		ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
	})
	s.NoError(merr.CheckRPCCall(remainingQuery, err))
	gotPKs := remainingQuery.GetFieldsData()[0].GetScalars().GetLongData().GetData()
	s.ElementsMatch(remainingPKs, gotPKs, "surviving rows should be exactly the half not targeted by the delete import")
}

// TestWriteModeUpsert imports N rows via a normal APPEND import, then imports a full-row file
// for the same N primary keys under write_mode=Upsert, with a distinctive marker value on a
// scalar field. Row count must settle at exactly N (not 2N: the companion delete never took
// effect; not 0: the companion delete ate the job's own new rows), and every surviving row
// must carry the new marker value, proving the old rows were the ones removed.
func (s *WriteModeSuite) TestWriteModeUpsert() {
	const (
		rowCount  = 200
		newMarker = "write-mode-upsert-marker"
	)

	c := s.Cluster
	ctx, cancel := context.WithTimeout(c.GetContext(), 10*time.Minute)
	defer cancel()

	collectionName := "TestWriteModeUpsert_" + funcutil.RandomString(8)
	schema := buildWriteModeSchema(collectionName)
	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)

	createCollectionStatus, err := c.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      common.DefaultShardsNum,
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, createCollectionStatus.GetErrorCode())

	insertData, filePath, err := GenerateParquetFileAndReturnInsertData(c, schema, rowCount)
	s.NoError(err)
	pks := insertData.Data[int64(100)].GetDataRows().([]int64)
	s.Require().Len(pks, rowCount)
	oldMarkers := insertData.Data[int64(102)].GetDataRows().([]string)
	s.NotContains(oldMarkers, newMarker, "sanity: the randomly generated marker values must not already collide with the upsert marker")

	importResp, err := c.ProxyClient.ImportV2(ctx, &internalpb.ImportRequest{
		CollectionName: collectionName,
		Files:          []*internalpb.ImportFile{{Paths: []string{filePath}}},
	})
	s.NoError(err)
	s.Equal(int32(0), importResp.GetStatus().GetCode(), importResp.GetStatus().GetReason())
	s.NoError(WaitForImportDone(ctx, c, importResp.GetJobID()))

	createIndexStatus, err := c.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      integration.FloatVecField,
		IndexName:      "_default",
		ExtraParams:    integration.ConstructIndexParam(dim, integration.IndexFaissIvfFlat, metric.L2),
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

	initialCount := s.queryStrongCount(ctx, collectionName)
	s.Equal(int64(rowCount), initialCount, "all imported rows should be visible before the upsert import")

	upsertFilePath, err := writeUpsertMarkerParquetFile(c, schema, rowCount, 102, newMarker)
	s.NoError(err)

	upsertImportResp, err := c.ProxyClient.ImportV2(ctx, &internalpb.ImportRequest{
		CollectionName: collectionName,
		Files:          []*internalpb.ImportFile{{Paths: []string{upsertFilePath}}},
		WriteMode:      internalpb.ImportWriteMode_Upsert,
	})
	s.NoError(err)
	s.Equal(int32(0), upsertImportResp.GetStatus().GetCode(), upsertImportResp.GetStatus().GetReason())
	s.NoError(WaitForImportDone(ctx, c, upsertImportResp.GetJobID()))

	// The wait must gate on the marker, not on row count: an upsert's net row count is N both
	// before and after it takes effect, so a count-only gate would be satisfied by the
	// pre-upsert state and never actually wait for anything. See waitForUpsertMarkers.
	gotPKs, gotMarkers := s.waitForUpsertMarkers(ctx, collectionName, newMarker, rowCount, 6*time.Minute)

	// Only now, after the marker wait has resolved (converged or timed out), does the row
	// count become a meaningful assertion: 2N means the companion delete never took effect,
	// 0 means the companion delete ate the rows the job itself wrote.
	s.Require().Len(gotMarkers, rowCount,
		"row count must settle at N: 2N would mean the companion delete never took effect, "+
			"0 would mean the companion delete ate the rows the job itself wrote; got %d rows", len(gotMarkers))
	s.ElementsMatch(pks, gotPKs, "upsert must not lose or duplicate any original primary key")
	for i, marker := range gotMarkers {
		s.Equal(newMarker, marker, "row pk=%d should carry the upsert marker value, proving the old row was the one removed", gotPKs[i])
	}
}
