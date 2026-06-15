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

package external

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/function/embedding"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/externalspec"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type RefreshExternalCollectionTaskSuite struct {
	suite.Suite
	collectionID int64
	taskID       int64
}

type fakeRecordReader struct {
	records []storage.Record
	errs    []error
	idx     int
}

func (r *fakeRecordReader) Next() (storage.Record, error) {
	if r.idx < len(r.errs) && r.errs[r.idx] != nil {
		err := r.errs[r.idx]
		r.idx++
		return nil, err
	}
	if r.idx >= len(r.records) {
		return nil, io.EOF
	}
	record := r.records[r.idx]
	r.idx++
	return record, nil
}

func (r *fakeRecordReader) Close() error {
	return nil
}

func (s *RefreshExternalCollectionTaskSuite) SetupSuite() {
	s.collectionID = 1000
	s.taskID = 1
}

func (s *RefreshExternalCollectionTaskSuite) TestNewRefreshExternalCollectionTask() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID:   s.collectionID,
		TaskID:         s.taskID,
		ExternalSource: "test_source",
		ExternalSpec:   "test_spec",
	}

	task := NewRefreshExternalCollectionTask(ctx, req)

	s.NotNil(task)
	s.Equal(s.collectionID, task.req.GetCollectionID())
	s.Equal(s.taskID, task.req.GetTaskID())
	s.Equal(indexpb.JobState_JobStateInit, task.GetState())
	s.Contains(task.Name(), "RefreshExternalCollectionTask")
}

func (s *RefreshExternalCollectionTaskSuite) TestTaskLifecycle() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID:   s.collectionID,
		TaskID:         s.taskID,
		ExternalSource: "test_source",
		ExternalSpec:   `{"format":"parquet"}`,
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{Name: "id", ExternalField: "id"},
			},
		},
		StorageConfig: &indexpb.StorageConfig{
			StorageType: "local",
		},
		CurrentSegments: []*datapb.SegmentInfo{
			{ID: 1, CollectionID: s.collectionID, NumOfRows: 1000},
			{ID: 2, CollectionID: s.collectionID, NumOfRows: 2000},
		},
		PreAllocatedSegmentIds: &datapb.IDRange{
			Begin: 1000,
			End:   2000,
		},
	}

	task := NewRefreshExternalCollectionTask(ctx, req)

	// Test OnEnqueue
	err := task.OnEnqueue(ctx)
	s.NoError(err)

	// Test PreExecute - validates schema, storage config, external source
	err = task.PreExecute(ctx)
	s.NoError(err)
	s.NotNil(task.parsedSpec)
	s.Equal("parquet", task.parsedSpec.Format)
	s.Equal([]string{"id"}, task.columns)

	// Test PostExecute (Execute skipped as it requires CGO FFI calls)
	err = task.PostExecute(ctx)
	s.NoError(err)

	// Test GetSlot
	s.Equal(int64(1), task.GetSlot())
}

func (s *RefreshExternalCollectionTaskSuite) TestPreExecuteClonesSchemaBeforeFillingExternalMetadata() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sourceSchema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{Name: "id", ExternalField: "id"},
		},
	}
	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID:   s.collectionID,
		TaskID:         s.taskID,
		ExternalSource: "s3://bucket/data/",
		ExternalSpec:   `{"format":"parquet"}`,
		Schema:         sourceSchema,
		StorageConfig: &indexpb.StorageConfig{
			StorageType: "local",
		},
	}

	task := NewRefreshExternalCollectionTask(ctx, req)
	err := task.PreExecute(ctx)

	s.NoError(err)
	s.Empty(sourceSchema.GetExternalSource())
	s.Empty(sourceSchema.GetExternalSpec())
	s.NotSame(sourceSchema, task.req.GetSchema())
	s.Equal(req.GetExternalSource(), task.req.GetSchema().GetExternalSource())
	s.Equal(req.GetExternalSpec(), task.req.GetSchema().GetExternalSpec())
	s.Equal([]string{"id"}, task.columns)
}

func (s *RefreshExternalCollectionTaskSuite) TestPreExecuteWithNilRequest() {
	ctx, cancel := context.WithCancel(context.Background()) //nolint:gosec // cancel is deferred below
	defer cancel()
	task := &RefreshExternalCollectionTask{
		ctx: ctx,
		req: nil,
	}

	err := task.PreExecute(ctx)
	s.Error(err)
}

func (s *RefreshExternalCollectionTaskSuite) TestSetAndGetState() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID: s.collectionID,
		TaskID:       s.taskID,
	}

	task := NewRefreshExternalCollectionTask(ctx, req)

	task.SetState(indexpb.JobState_JobStateInProgress, "")
	s.Equal(indexpb.JobState_JobStateInProgress, task.GetState())

	task.SetState(indexpb.JobState_JobStateFailed, "test failure")
	s.Equal(indexpb.JobState_JobStateFailed, task.GetState())
	s.Equal("test failure", task.failReason)
}

func (s *RefreshExternalCollectionTaskSuite) TestReset() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID: s.collectionID,
		TaskID:       s.taskID,
	}

	task := NewRefreshExternalCollectionTask(ctx, req)
	task.Reset()

	s.Nil(task.ctx)
	s.Nil(task.req)
	s.Nil(task.tr)
	s.Nil(task.updatedSegments)
}

func (s *RefreshExternalCollectionTaskSuite) TestBalanceFragmentsToSegments_Empty() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID: s.collectionID,
		TaskID:       s.taskID,
	}

	task := NewRefreshExternalCollectionTask(ctx, req)
	result, err := task.balanceFragmentsToSegments(context.Background(), []packed.Fragment{})
	s.NoError(err)
	s.Nil(result)
}

// Regression for #49225: zero-row parquet produces fragments whose RowCount
// sums to 0 and previously triggered divide-by-zero panic in
// balanceFragmentsToSegments. Must return error, never panic.
func (s *RefreshExternalCollectionTaskSuite) TestBalanceFragmentsToSegments_ZeroTotalRowsWithFragments() {
	paramtable.Init()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID: s.collectionID,
		TaskID:       s.taskID,
	}
	task := NewRefreshExternalCollectionTask(ctx, req)

	fragments := []packed.Fragment{
		{FragmentID: 1, RowCount: 0, FilePath: "s3://bucket/zero.parquet"},
		{FragmentID: 2, RowCount: 0, FilePath: "s3://bucket/zero2.parquet"},
	}
	s.NotPanics(func() {
		result, err := task.balanceFragmentsToSegments(context.Background(), fragments)
		s.Error(err)
		s.Nil(result)
		s.Contains(err.Error(), "zero total rows")
	})
}

func (s *RefreshExternalCollectionTaskSuite) TestBalanceFragmentsToSegments_SingleFragment() {
	paramtable.Init()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID:           s.collectionID,
		TaskID:                 s.taskID,
		PreAllocatedSegmentIds: &datapb.IDRange{Begin: 100, End: 200},
		StorageConfig:          &indexpb.StorageConfig{StorageType: "local"},
		ExternalSource:         "s3://bucket/data/",
		ExternalSpec:           `{"format":"parquet"}`,
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "text", ExternalField: "text_col"},
				{FieldID: 101, Name: "vec", ExternalField: "vec_col"},
			},
		},
	}

	task := NewRefreshExternalCollectionTask(ctx, req)
	task.preallocatedIDRange = req.GetPreAllocatedSegmentIds()
	task.nextAllocID = task.preallocatedIDRange.Begin
	task.parsedSpec = &externalspec.ExternalSpec{Format: "parquet"}

	m1 := mockey.Mock(packed.CreateSegmentManifestWithBasePathAndExtfs).
		To(func(ctx context.Context, basePath, format string, columns []string, fragments []packed.Fragment, storageConfig *indexpb.StorageConfig, extfs packed.ExternalSpecContext) (string, error) {
			return fmt.Sprintf("%s/manifest.json", basePath), nil
		}).Build()
	defer m1.UnPatch()

	m2 := mockey.Mock(packed.SampleExternalFieldSizes).
		Return(map[string]int64{"text_col": 64, "vec_col": 512}, nil).Build()
	defer m2.UnPatch()

	fragments := []packed.Fragment{
		{FragmentID: 1, RowCount: 500},
	}

	result, err := task.balanceFragmentsToSegments(context.Background(), fragments)
	s.NoError(err)
	s.Len(result, 1)
	s.Equal(int64(500), result[0].GetNumOfRows())
	s.Equal(storage.StorageV3, result[0].GetStorageVersion(),
		"external segments should have StorageVersion=V3")

	// Verify fake binlogs
	s.Len(result[0].GetBinlogs(), 1)
	fb := result[0].GetBinlogs()[0]
	s.Equal(int64(0), fb.GetFieldID(), "should use DefaultShortColumnGroupID")
	s.ElementsMatch([]int64{100, 101}, fb.GetChildFields())
	s.Len(fb.GetBinlogs(), 1)
	binlog := fb.GetBinlogs()[0]
	s.Equal(int64(500), binlog.GetEntriesNum())
	// avgBytesPerRow = 64 + 512 = 576, memorySize = 576 * 500 = 288000
	s.Equal(int64(288000), binlog.GetMemorySize())
	s.Equal(int64(288000), binlog.GetLogSize())
}

func (s *RefreshExternalCollectionTaskSuite) TestBalanceFragmentsToSegments_MultipleFragments() {
	paramtable.Init()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID:           s.collectionID,
		TaskID:                 s.taskID,
		PreAllocatedSegmentIds: &datapb.IDRange{Begin: 100, End: 200},
		StorageConfig:          &indexpb.StorageConfig{StorageType: "local"},
		ExternalSource:         "s3://bucket/data/",
		ExternalSpec:           `{"format":"parquet"}`,
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "text", ExternalField: "text_col"},
			},
		},
	}

	task := NewRefreshExternalCollectionTask(ctx, req)
	task.preallocatedIDRange = req.GetPreAllocatedSegmentIds()
	task.nextAllocID = task.preallocatedIDRange.Begin
	task.parsedSpec = &externalspec.ExternalSpec{Format: "parquet"}

	m1 := mockey.Mock(packed.CreateSegmentManifestWithBasePathAndExtfs).
		To(func(ctx context.Context, basePath, format string, columns []string, fragments []packed.Fragment, storageConfig *indexpb.StorageConfig, extfs packed.ExternalSpecContext) (string, error) {
			return fmt.Sprintf("%s/manifest.json", basePath), nil
		}).Build()
	defer m1.UnPatch()

	m2 := mockey.Mock(packed.SampleExternalFieldSizes).
		Return(map[string]int64{"text_col": 100}, nil).Build()
	defer m2.UnPatch()

	fragments := []packed.Fragment{
		{FragmentID: 1, RowCount: 300000},
		{FragmentID: 2, RowCount: 400000},
		{FragmentID: 3, RowCount: 500000},
		{FragmentID: 4, RowCount: 600000},
		{FragmentID: 5, RowCount: 200000},
	}

	result, err := task.balanceFragmentsToSegments(context.Background(), fragments)
	s.NoError(err)

	// Verify all segments have StorageVersion=V3 and fake binlogs
	for i, seg := range result {
		s.Equal(storage.StorageV3, seg.GetStorageVersion(),
			"segment %d should have StorageVersion=V3", i)
		s.NotEmpty(seg.GetBinlogs(), "segment %d should have fake binlogs", i)
		fb := seg.GetBinlogs()[0]
		s.Equal(int64(0), fb.GetFieldID())
		s.Equal([]int64{100}, fb.GetChildFields())
		// MemorySize = avgBytesPerRow(100) * numRows
		s.Equal(int64(100)*seg.GetNumOfRows(), fb.GetBinlogs()[0].GetMemorySize())
	}

	// Verify total rows are preserved
	var totalRows int64
	for _, seg := range result {
		totalRows += seg.GetNumOfRows()
	}
	s.Equal(int64(2000000), totalRows)

	// Verify segments are reasonably balanced
	if len(result) > 1 {
		minRows, maxRows := result[0].GetNumOfRows(), result[0].GetNumOfRows()
		for _, seg := range result {
			if seg.GetNumOfRows() < minRows {
				minRows = seg.GetNumOfRows()
			}
			if seg.GetNumOfRows() > maxRows {
				maxRows = seg.GetNumOfRows()
			}
		}
		// The difference between max and min should be reasonable
		avgFragmentSize := int64(2000000 / 5)
		s.Less(maxRows-minRows, avgFragmentSize*2)
	}
}

func (s *RefreshExternalCollectionTaskSuite) TestPreExecuteContextCanceled() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID: s.collectionID,
		TaskID:       s.taskID,
	}

	task := NewRefreshExternalCollectionTask(ctx, req)
	cancel()

	err := task.PreExecute(ctx)
	s.ErrorIs(err, context.Canceled)
}

func (s *RefreshExternalCollectionTaskSuite) TestExecuteContextCanceled() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID: s.collectionID,
		TaskID:       s.taskID,
	}

	task := NewRefreshExternalCollectionTask(ctx, req)
	cancel()

	err := task.Execute(ctx)
	s.ErrorIs(err, context.Canceled)
}

func (s *RefreshExternalCollectionTaskSuite) TestBalanceFragmentsToSegmentsContextCanceled() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID: s.collectionID,
		TaskID:       s.taskID,
	}

	task := NewRefreshExternalCollectionTask(ctx, req)
	cancel()

	result, err := task.balanceFragmentsToSegments(ctx, []packed.Fragment{{FragmentID: 1, RowCount: 10}})
	s.ErrorIs(err, context.Canceled)
	s.Nil(result)
}

func (s *RefreshExternalCollectionTaskSuite) TestOrganizeSegments_AllFragmentsExist() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID: s.collectionID,
		TaskID:       s.taskID,
		CurrentSegments: []*datapb.SegmentInfo{
			{ID: 1, CollectionID: s.collectionID, NumOfRows: 1000},
			{ID: 2, CollectionID: s.collectionID, NumOfRows: 2000},
		},
	}

	task := NewRefreshExternalCollectionTask(ctx, req)

	// Simulate current segment fragments mapping (use FilePath as identifier)
	currentSegmentFragments := packed.SegmentFragments{
		1: []packed.Fragment{{FragmentID: 101, FilePath: "/data/file1.parquet", RowCount: 1000}},
		2: []packed.Fragment{{FragmentID: 102, FilePath: "/data/file2.parquet", RowCount: 2000}},
	}

	// New fragments contain all existing fragments
	newFragments := []packed.Fragment{
		{FragmentID: 101, FilePath: "/data/file1.parquet", RowCount: 1000},
		{FragmentID: 102, FilePath: "/data/file2.parquet", RowCount: 2000},
	}

	result, err := task.organizeSegments(context.Background(), currentSegmentFragments, newFragments)
	s.NoError(err)

	// Both segments should be kept
	s.Len(result, 2)

	// Verify kept/new tracking
	s.ElementsMatch([]int64{1, 2}, task.GetKeptSegmentIDs())
	s.Empty(task.GetUpdatedSegments())
}

func (s *RefreshExternalCollectionTaskSuite) TestOrganizeSegments_SameFragmentsMissingFieldPatchesSegment() {
	paramtable.Init()

	ctx := context.Background()
	partitionID := int64(2000)
	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID:   s.collectionID,
		PartitionID:    partitionID,
		TaskID:         s.taskID,
		ExternalSpec:   `{"format":"parquet"}`,
		ExternalSource: "s3://bucket/data/",
		StorageConfig:  &indexpb.StorageConfig{StorageType: "local"},
		Schema: &schemapb.CollectionSchema{
			Version: 4,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "id", ExternalField: "id"},
				{FieldID: 101, Name: "vec", ExternalField: "vec"},
				{FieldID: 102, Name: "text", ExternalField: "text"},
				{FieldID: 103, Name: "score", ExternalField: "score"},
			},
		},
		CurrentSegments: []*datapb.SegmentInfo{{
			ID:             10,
			CollectionID:   s.collectionID,
			PartitionID:    partitionID,
			NumOfRows:      1000,
			ManifestPath:   `{"base_path":"seg10","ver":1}`,
			StorageVersion: storage.StorageV3,
			SchemaVersion:  3,
			Binlogs: buildFakeBinlogs(10, 1000, 3000, &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{FieldID: 100, Name: "id", ExternalField: "id"},
					{FieldID: 101, Name: "vec", ExternalField: "vec"},
					{FieldID: 102, Name: "text", ExternalField: "text"},
				},
			}, "parquet"),
		}},
	}
	task := NewRefreshExternalCollectionTask(ctx, req)
	task.parsedSpec = &externalspec.ExternalSpec{Format: "parquet"}
	task.columns = []string{"id", "vec", "text", "score"}

	mockAppend := mockey.Mock(packed.AppendSegmentManifestColumns).
		To(func(ctx context.Context, oldManifestPath string, format string, columns []string, fragments []packed.Fragment, storageConfig *indexpb.StorageConfig) (string, error) {
			s.Equal(`{"base_path":"seg10","ver":1}`, oldManifestPath)
			s.Equal([]string{"score"}, columns)
			return `{"base_path":"seg10","ver":2}`, nil
		}).Build()
	defer mockAppend.UnPatch()

	mockSample := mockey.Mock(packed.SampleExternalFieldSizes).
		To(func(manifestPath string, sampleRows int, collectionID int64, externalSource string, externalSpec string, schema *schemapb.CollectionSchema, storageConfig *indexpb.StorageConfig) (map[string]int64, error) {
			s.Equal(`{"base_path":"seg10","ver":2}`, manifestPath)
			s.Equal(100, sampleRows)
			s.Equal(s.collectionID, collectionID)
			s.Equal("s3://bucket/data/", externalSource)
			s.Equal(`{"format":"parquet"}`, externalSpec)
			s.Equal(req.GetSchema(), schema)
			s.Equal(req.GetStorageConfig(), storageConfig)
			return map[string]int64{"id": 8, "vec": 128, "text": 32, "score": 8}, nil
		}).Build()
	defer mockSample.UnPatch()

	current := packed.SegmentFragments{
		10: []packed.Fragment{{FilePath: "s3://bucket/data/a.parquet", StartRow: 0, EndRow: 1000, RowCount: 1000}},
	}
	next := []packed.Fragment{{FilePath: "s3://bucket/data/a.parquet", StartRow: 0, EndRow: 1000, RowCount: 1000}}

	result, err := task.organizeSegments(ctx, current, next)
	s.NoError(err)
	s.Len(result, 1)
	s.Empty(task.GetKeptSegmentIDs())
	s.Len(task.GetUpdatedSegments(), 1)
	patched := task.GetUpdatedSegments()[0]
	s.Equal(int64(10), patched.GetID())
	s.Equal(`{"base_path":"seg10","ver":2}`, patched.GetManifestPath())
	s.Equal(int32(4), patched.GetSchemaVersion())
	s.ElementsMatch([]int64{100, 101, 102, 103}, patched.GetBinlogs()[0].GetChildFields())

	result, err = task.organizeSegments(ctx, current, next)
	s.NoError(err)
	s.Len(result, 1)
	s.Empty(task.GetKeptSegmentIDs())
	s.Len(task.GetUpdatedSegments(), 1)
	s.Equal(int64(10), task.GetUpdatedSegments()[0].GetID())
}

func (s *RefreshExternalCollectionTaskSuite) TestOrganizeSegments_PatchedSegmentCountsFunctionOutputMemory() {
	paramtable.Init()

	ctx := context.Background()
	partitionID := int64(2000)
	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID:   s.collectionID,
		PartitionID:    partitionID,
		TaskID:         s.taskID,
		ExternalSpec:   `{"format":"parquet"}`,
		ExternalSource: "s3://bucket/data/",
		StorageConfig:  &indexpb.StorageConfig{StorageType: "local"},
		Schema: &schemapb.CollectionSchema{
			Version: 4,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, ExternalField: "id"},
				{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar, ExternalField: "text"},
				{FieldID: 102, Name: "score", DataType: schemapb.DataType_Double, ExternalField: "score"},
				{
					FieldID:          103,
					Name:             "embedding",
					DataType:         schemapb.DataType_FloatVector,
					IsFunctionOutput: true,
					TypeParams:       []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "4"}},
				},
			},
		},
		CurrentSegments: []*datapb.SegmentInfo{{
			ID:             10,
			CollectionID:   s.collectionID,
			PartitionID:    partitionID,
			NumOfRows:      100,
			ManifestPath:   `{"base_path":"seg10","ver":1}`,
			StorageVersion: storage.StorageV3,
			SchemaVersion:  3,
			Binlogs: buildFakeBinlogs(10, 100, 4800, &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, ExternalField: "id"},
					{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar, ExternalField: "text"},
					{
						FieldID:          103,
						Name:             "embedding",
						DataType:         schemapb.DataType_FloatVector,
						IsFunctionOutput: true,
						TypeParams:       []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "4"}},
					},
				},
			}, "parquet"),
		}},
	}
	task := NewRefreshExternalCollectionTask(ctx, req)
	task.parsedSpec = &externalspec.ExternalSpec{Format: "parquet"}
	task.columns = []string{"id", "text", "score"}

	mockAppend := mockey.Mock(packed.AppendSegmentManifestColumns).
		To(func(ctx context.Context, oldManifestPath string, format string, columns []string, fragments []packed.Fragment, storageConfig *indexpb.StorageConfig) (string, error) {
			s.Equal(`{"base_path":"seg10","ver":1}`, oldManifestPath)
			s.Equal([]string{"score"}, columns)
			return `{"base_path":"seg10","ver":2}`, nil
		}).Build()
	defer mockAppend.UnPatch()

	mockSample := mockey.Mock(packed.SampleExternalFieldSizes).
		Return(map[string]int64{"id": 8, "text": 32, "score": 8}, nil).Build()
	defer mockSample.UnPatch()

	current := packed.SegmentFragments{
		10: []packed.Fragment{{FilePath: "s3://bucket/data/a.parquet", StartRow: 0, EndRow: 100, RowCount: 100}},
	}
	next := []packed.Fragment{{FilePath: "s3://bucket/data/a.parquet", StartRow: 0, EndRow: 100, RowCount: 100}}

	result, err := task.organizeSegments(ctx, current, next)
	s.NoError(err)
	s.Len(result, 1)
	patched := task.GetUpdatedSegments()[0]
	s.ElementsMatch([]int64{100, 101, 102, 103}, patched.GetBinlogs()[0].GetChildFields())
	s.Equal(int64((8+32+8+16)*100), patched.GetBinlogs()[0].GetBinlogs()[0].GetMemorySize())
}

func (s *RefreshExternalCollectionTaskSuite) TestOrganizeSegments_SameFragmentsAllFieldsCoveredKeepsSegment() {
	ctx := context.Background()
	partitionID := int64(2000)
	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID:  s.collectionID,
		PartitionID:   partitionID,
		TaskID:        s.taskID,
		ExternalSpec:  `{"format":"parquet"}`,
		StorageConfig: &indexpb.StorageConfig{StorageType: "local"},
		Schema: &schemapb.CollectionSchema{
			Version: 4,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "id", ExternalField: "id"},
				{FieldID: 101, Name: "vec", ExternalField: "vec"},
			},
		},
		CurrentSegments: []*datapb.SegmentInfo{{
			ID:             10,
			CollectionID:   s.collectionID,
			PartitionID:    partitionID,
			NumOfRows:      100,
			ManifestPath:   `{"base_path":"seg10","ver":1}`,
			StorageVersion: storage.StorageV3,
			SchemaVersion:  4,
			Binlogs: buildFakeBinlogs(10, 100, 3000, &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{FieldID: 100, Name: "id", ExternalField: "id"},
					{FieldID: 101, Name: "vec", ExternalField: "vec"},
				},
			}, "parquet"),
		}},
	}
	task := NewRefreshExternalCollectionTask(ctx, req)
	current := packed.SegmentFragments{
		10: []packed.Fragment{{FilePath: "s3://bucket/data/a.parquet", StartRow: 0, EndRow: 100, RowCount: 100}},
	}
	next := []packed.Fragment{{FilePath: "s3://bucket/data/a.parquet", StartRow: 0, EndRow: 100, RowCount: 100}}

	result, err := task.organizeSegments(ctx, current, next)
	s.NoError(err)
	s.Len(result, 1)
	s.ElementsMatch([]int64{10}, task.GetKeptSegmentIDs())
	s.Empty(task.GetUpdatedSegments())
}

func (s *RefreshExternalCollectionTaskSuite) TestOrganizeSegments_FragmentRemoved() {
	s.T().Skip("Skip test that requires CGO FFI calls for manifest creation")
}

func (s *RefreshExternalCollectionTaskSuite) TestOrganizeSegments_PartialFragmentRemoved() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID: s.collectionID,
		TaskID:       s.taskID,
		CurrentSegments: []*datapb.SegmentInfo{
			{ID: 1, CollectionID: s.collectionID, NumOfRows: 1000},
			{ID: 2, CollectionID: s.collectionID, NumOfRows: 2000},
			{ID: 3, CollectionID: s.collectionID, NumOfRows: 1500},
		},
		PreAllocatedSegmentIds: &datapb.IDRange{Begin: 100, End: 200},
	}

	task := NewRefreshExternalCollectionTask(ctx, req)

	// S1 has file1, S2 has file2, S3 has file3
	currentSegmentFragments := packed.SegmentFragments{
		1: []packed.Fragment{{FragmentID: 101, FilePath: "/data/file1.parquet", RowCount: 1000}},
		2: []packed.Fragment{{FragmentID: 102, FilePath: "/data/file2.parquet", RowCount: 2000}},
		3: []packed.Fragment{{FragmentID: 103, FilePath: "/data/file3.parquet", RowCount: 1500}},
	}

	// New fragments: file1 and file3 still exist, file2 removed
	newFragments := []packed.Fragment{
		{FragmentID: 201, FilePath: "/data/file1.parquet", RowCount: 1000},
		{FragmentID: 203, FilePath: "/data/file3.parquet", RowCount: 1500},
	}

	result, err := task.organizeSegments(context.Background(), currentSegmentFragments, newFragments)
	s.NoError(err)

	// S1 and S3 should be kept, S2 should be invalidated
	// No new segments because there are no orphan fragments (all new fragments match kept segments)
	s.Len(result, 2)
	s.ElementsMatch([]int64{1, 3}, task.GetKeptSegmentIDs())
	s.Empty(task.GetUpdatedSegments(), "No orphan fragments to create new segments from")
}

func (s *RefreshExternalCollectionTaskSuite) TestOrganizeSegments_NewFragmentsUseBalance() {
	ctx := context.Background()
	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID: s.collectionID,
		TaskID:       s.taskID,
		CurrentSegments: []*datapb.SegmentInfo{
			{ID: 1, CollectionID: s.collectionID, NumOfRows: 1000},
		},
	}
	task := NewRefreshExternalCollectionTask(ctx, req)

	currentSegmentFragments := packed.SegmentFragments{
		1: []packed.Fragment{{FragmentID: 101, FilePath: "/data/file1.parquet", StartRow: 0, EndRow: 1000, RowCount: 1000}},
	}
	newFragments := []packed.Fragment{
		{FragmentID: 201, FilePath: "/data/file1.parquet", StartRow: 0, EndRow: 1000, RowCount: 1000},
		{FragmentID: 202, FilePath: "/data/file2.parquet", StartRow: 0, EndRow: 500, RowCount: 500},
	}
	created := []*datapb.SegmentInfo{{ID: 2, CollectionID: s.collectionID, NumOfRows: 500}}

	var gotOrphans []packed.Fragment
	mockBalance := mockey.Mock(mockey.GetMethod(task, "balanceFragmentsToSegments")).
		To(func(ctx context.Context, fragments []packed.Fragment) ([]*datapb.SegmentInfo, error) {
			gotOrphans = fragments
			return created, nil
		}).Build()
	defer mockBalance.UnPatch()

	result, err := task.organizeSegments(ctx, currentSegmentFragments, newFragments)
	s.NoError(err)
	s.ElementsMatch([]int64{1}, task.GetKeptSegmentIDs())
	s.Equal(created, task.GetUpdatedSegments())
	s.Equal([]*datapb.SegmentInfo{req.GetCurrentSegments()[0], created[0]}, result)
	s.Require().Len(gotOrphans, 1)
	s.Equal("/data/file2.parquet", gotOrphans[0].FilePath)
}

func (s *RefreshExternalCollectionTaskSuite) TestOrganizeSegments_RewritesSegmentMissingFunctionOutputs() {
	ctx := context.Background()
	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID:  s.collectionID,
		TaskID:        s.taskID,
		StorageConfig: &indexpb.StorageConfig{RootPath: "files", StorageType: "local"},
		CurrentSegments: []*datapb.SegmentInfo{
			{
				ID:           1,
				CollectionID: s.collectionID,
				NumOfRows:    1000,
				ManifestPath: packed.MarshalManifestPath("files/insert_log/1000/2000/1", 1),
				Binlogs: []*datapb.FieldBinlog{
					{ChildFields: []int64{100}},
				},
			},
		},
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "text", DataType: schemapb.DataType_VarChar, ExternalField: "text_col"},
				{FieldID: 101, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
			},
			Functions: []*schemapb.FunctionSchema{
				{Type: schemapb.FunctionType_BM25, InputFieldIds: []int64{100}, OutputFieldIds: []int64{101}},
			},
		},
	}
	task := NewRefreshExternalCollectionTask(ctx, req)

	currentSegmentFragments := packed.SegmentFragments{
		1: []packed.Fragment{{FragmentID: 101, FilePath: "/data/file1.parquet", StartRow: 0, EndRow: 1000, RowCount: 1000}},
	}
	newFragments := []packed.Fragment{
		{FragmentID: 201, FilePath: "/data/file1.parquet", StartRow: 0, EndRow: 1000, RowCount: 1000},
	}
	created := []*datapb.SegmentInfo{{ID: 2, CollectionID: s.collectionID, NumOfRows: 1000}}

	var checkedColumns []string
	mockHasColumns := mockey.Mock(packed.ManifestHasColumns).
		To(func(manifestPath string, storageConfig *indexpb.StorageConfig, columns []string) (bool, error) {
			checkedColumns = columns
			return false, nil
		}).Build()
	defer mockHasColumns.UnPatch()

	var gotOrphans []packed.Fragment
	mockBalance := mockey.Mock(mockey.GetMethod(task, "balanceFragmentsToSegments")).
		To(func(ctx context.Context, fragments []packed.Fragment) ([]*datapb.SegmentInfo, error) {
			gotOrphans = fragments
			return created, nil
		}).Build()
	defer mockBalance.UnPatch()

	result, err := task.organizeSegments(ctx, currentSegmentFragments, newFragments)
	s.NoError(err)
	s.Empty(task.GetKeptSegmentIDs())
	s.Equal(created, task.GetUpdatedSegments())
	s.Equal(created, result)
	s.Equal([]string{"101"}, checkedColumns)
	s.Require().Len(gotOrphans, 1)
	s.Equal("/data/file1.parquet", gotOrphans[0].FilePath)
}

func (s *RefreshExternalCollectionTaskSuite) TestOrganizeSegments_KeepsSegmentWithFunctionOutputs() {
	ctx := context.Background()
	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID:  s.collectionID,
		TaskID:        s.taskID,
		StorageConfig: &indexpb.StorageConfig{RootPath: "files", StorageType: "local"},
		CurrentSegments: []*datapb.SegmentInfo{
			{
				ID:           1,
				CollectionID: s.collectionID,
				NumOfRows:    1000,
				ManifestPath: packed.MarshalManifestPath("files/insert_log/1000/2000/1", 1),
				Binlogs: []*datapb.FieldBinlog{
					{ChildFields: []int64{100}},
				},
			},
		},
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "text", DataType: schemapb.DataType_VarChar, ExternalField: "text_col"},
				{FieldID: 101, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
			},
			Functions: []*schemapb.FunctionSchema{
				{Type: schemapb.FunctionType_BM25, InputFieldIds: []int64{100}, OutputFieldIds: []int64{101}},
			},
		},
	}
	task := NewRefreshExternalCollectionTask(ctx, req)

	currentSegmentFragments := packed.SegmentFragments{
		1: []packed.Fragment{{FragmentID: 101, FilePath: "/data/file1.parquet", StartRow: 0, EndRow: 1000, RowCount: 1000}},
	}
	newFragments := []packed.Fragment{
		{FragmentID: 201, FilePath: "/data/file1.parquet", StartRow: 0, EndRow: 1000, RowCount: 1000},
	}

	var checkedColumns []string
	mockHasColumns := mockey.Mock(packed.ManifestHasColumns).
		To(func(manifestPath string, storageConfig *indexpb.StorageConfig, columns []string) (bool, error) {
			checkedColumns = columns
			return true, nil
		}).Build()
	defer mockHasColumns.UnPatch()

	var gotOrphans []packed.Fragment
	mockBalance := mockey.Mock(mockey.GetMethod(task, "balanceFragmentsToSegments")).
		To(func(ctx context.Context, fragments []packed.Fragment) ([]*datapb.SegmentInfo, error) {
			gotOrphans = fragments
			return nil, nil
		}).Build()
	defer mockBalance.UnPatch()

	result, err := task.organizeSegments(ctx, currentSegmentFragments, newFragments)
	s.NoError(err)
	s.ElementsMatch([]int64{1}, task.GetKeptSegmentIDs())
	s.Empty(task.GetUpdatedSegments())
	s.Equal([]*datapb.SegmentInfo{req.GetCurrentSegments()[0]}, result)
	s.Equal([]string{"101"}, checkedColumns)
	s.Empty(gotOrphans)
}

func (s *RefreshExternalCollectionTaskSuite) TestOrganizeSegments_FunctionOutputColumnCheckError() {
	ctx := context.Background()
	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID:  s.collectionID,
		TaskID:        s.taskID,
		StorageConfig: &indexpb.StorageConfig{RootPath: "files", StorageType: "local"},
		CurrentSegments: []*datapb.SegmentInfo{
			{ID: 1, CollectionID: s.collectionID, NumOfRows: 1000, ManifestPath: packed.MarshalManifestPath("files/insert_log/1000/2000/1", 1)},
		},
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "text", DataType: schemapb.DataType_VarChar, ExternalField: "text_col"},
				{FieldID: 101, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
			},
			Functions: []*schemapb.FunctionSchema{
				{Type: schemapb.FunctionType_BM25, InputFieldIds: []int64{100}, OutputFieldIds: []int64{101}},
			},
		},
	}
	task := NewRefreshExternalCollectionTask(ctx, req)

	currentSegmentFragments := packed.SegmentFragments{
		1: []packed.Fragment{{FragmentID: 101, FilePath: "/data/file1.parquet", StartRow: 0, EndRow: 1000, RowCount: 1000}},
	}
	newFragments := []packed.Fragment{
		{FragmentID: 201, FilePath: "/data/file1.parquet", StartRow: 0, EndRow: 1000, RowCount: 1000},
	}

	mockHasColumns := mockey.Mock(packed.ManifestHasColumns).
		Return(false, fmt.Errorf("manifest read failed")).Build()
	defer mockHasColumns.UnPatch()

	result, err := task.organizeSegments(ctx, currentSegmentFragments, newFragments)
	s.Error(err)
	s.Nil(result)
	s.Contains(err.Error(), "check function output columns for segment 1")
	s.Contains(err.Error(), "manifest read failed")
}

func (s *RefreshExternalCollectionTaskSuite) TestFunctionOutputColumnNamesUseFunctionSchema() {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "text", DataType: schemapb.DataType_VarChar, ExternalField: "text_col"},
			{FieldID: 101, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector},
		},
		Functions: []*schemapb.FunctionSchema{
			{Type: schemapb.FunctionType_BM25, OutputFieldIds: []int64{101}, OutputFieldNames: []string{"sparse"}},
		},
	}

	columns, err := functionOutputColumnNames(schema)
	s.NoError(err)
	s.Equal([]string{"101"}, columns)
}

func (s *RefreshExternalCollectionTaskSuite) TestFunctionOutputFieldsUseFunctionSchema() {
	schema := &schemapb.CollectionSchema{
		Name: "test_schema",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "text", DataType: schemapb.DataType_VarChar, ExternalField: "text_col"},
			{
				FieldID:    101,
				Name:       "embedding",
				DataType:   schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "4"}},
			},
		},
		Functions: []*schemapb.FunctionSchema{
			{
				Type:             schemapb.FunctionType_TextEmbedding,
				InputFieldIds:    []int64{100},
				OutputFieldNames: []string{"embedding"},
			},
		},
	}

	outputFields, outputSchema, err := buildOutputSchema(schema)
	s.NoError(err)
	s.Require().Len(outputFields, 1)
	s.Equal(int64(101), outputFields[0].GetFieldID())
	s.Equal("test_schema", outputSchema.GetName())
	s.Equal(outputFields, outputSchema.GetFields())

	bytes, err := estimateFunctionOutputBytesPerRow(schema)
	s.NoError(err)
	s.Equal(int64(16), bytes)

	_, executionSchema, _, err := buildFunctionExecutionSchema(schema)
	s.NoError(err)
	s.Equal([]int64{100, 101}, []int64{
		executionSchema.GetFields()[0].GetFieldID(),
		executionSchema.GetFields()[1].GetFieldID(),
	})
}

func (s *RefreshExternalCollectionTaskSuite) TestFunctionOutputFieldsRejectMissingFunctionOutputs() {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "text", DataType: schemapb.DataType_VarChar, ExternalField: "text_col"},
		},
		Functions: []*schemapb.FunctionSchema{
			{Type: schemapb.FunctionType_TextEmbedding, OutputFieldIds: []int64{101}},
		},
	}

	_, err := functionOutputColumnNames(schema)
	s.Error(err)
	s.Contains(err.Error(), "function output field id 101 not found in schema")

	schema.GetFunctions()[0].OutputFieldIds = nil
	schema.GetFunctions()[0].OutputFieldNames = []string{"embedding"}
	_, err = functionOutputColumnNames(schema)
	s.Error(err)
	s.Contains(err.Error(), "function output field embedding not found in schema")
}

func (s *RefreshExternalCollectionTaskSuite) TestFunctionOutputFieldsBranches() {
	outputFields, err := functionOutputFields(nil)
	s.NoError(err)
	s.Nil(outputFields)

	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 0, Name: "zero", DataType: schemapb.DataType_VarChar, IsFunctionOutput: true},
			{FieldID: 101, Name: "direct", DataType: schemapb.DataType_VarChar, IsFunctionOutput: true},
			{FieldID: 102, Name: "named", DataType: schemapb.DataType_SparseFloatVector},
		},
		Functions: []*schemapb.FunctionSchema{
			{
				OutputFieldIds:   []int64{0, 102},
				OutputFieldNames: []string{"", "named"},
			},
		},
	}

	outputFields, err = functionOutputFields(schema)
	s.NoError(err)
	s.Equal([]int64{101, 102}, []int64{
		outputFields[0].GetFieldID(),
		outputFields[1].GetFieldID(),
	})
}

func (s *RefreshExternalCollectionTaskSuite) TestBuildFunctionExecutionSchemaBranches() {
	_, _, _, err := buildFunctionExecutionSchema(nil)
	s.Error(err)
	s.Contains(err.Error(), "collection schema is nil")

	_, _, err = buildOutputSchema(&schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "text", DataType: schemapb.DataType_VarChar},
		},
		Functions: []*schemapb.FunctionSchema{
			{Type: schemapb.FunctionType_BM25, OutputFieldIds: []int64{101}},
		},
	})
	s.Error(err)
	s.Contains(err.Error(), "function output field id 101 not found")

	_, _, _, err = buildFunctionExecutionSchema(&schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 101, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
		},
		Functions: []*schemapb.FunctionSchema{
			{Type: schemapb.FunctionType_BM25, InputFieldIds: []int64{999}, OutputFieldIds: []int64{101}},
		},
	})
	s.Error(err)
	s.Contains(err.Error(), "function input field id 999 not found in schema")

	_, _, _, err = buildFunctionExecutionSchema(&schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "text", DataType: schemapb.DataType_VarChar},
		},
		Functions: []*schemapb.FunctionSchema{
			{Type: schemapb.FunctionType_BM25, InputFieldIds: []int64{100}, OutputFieldIds: []int64{101}},
		},
	})
	s.Error(err)
	s.Contains(err.Error(), "function output field id 101 not found")

	_, _, _, err = buildFunctionExecutionSchema(&schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: common.VirtualPKFieldName, DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
		},
		Functions: []*schemapb.FunctionSchema{
			{Type: schemapb.FunctionType_BM25, InputFieldIds: []int64{100}, OutputFieldIds: []int64{101}},
		},
	})
	s.Error(err)
	s.Contains(err.Error(), "no source input columns")

	_, _, _, err = buildFunctionExecutionSchema(&schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "text", DataType: schemapb.DataType_VarChar},
		},
		Functions: []*schemapb.FunctionSchema{
			{Type: schemapb.FunctionType_BM25, InputFieldIds: []int64{100}, OutputFieldIds: []int64{100}},
		},
	})
	s.Error(err)
	s.Contains(err.Error(), "no source input columns")

	inputSchema, executionSchema, requiredFields, err := buildFunctionExecutionSchema(&schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "text", DataType: schemapb.DataType_VarChar},
			{FieldID: 100, Name: "text_dup", DataType: schemapb.DataType_VarChar},
			{FieldID: 101, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
		},
		Functions: []*schemapb.FunctionSchema{
			{Type: schemapb.FunctionType_BM25, InputFieldIds: []int64{100}, OutputFieldIds: []int64{101}},
		},
	})
	s.NoError(err)
	s.Require().NotNil(inputSchema)
	s.Require().NotNil(executionSchema)
	s.True(requiredFields.Contain(int64(100)))
	s.Len(executionSchema.GetFields(), 2)
}

func (s *RefreshExternalCollectionTaskSuite) TestSegmentHasFunctionOutputColumnsBranches() {
	task := NewRefreshExternalCollectionTask(context.Background(), &datapb.RefreshExternalCollectionTaskRequest{})

	hasColumns, err := task.segmentHasFunctionOutputColumns(&datapb.SegmentInfo{}, nil)
	s.NoError(err)
	s.True(hasColumns)

	hasColumns, err = task.segmentHasFunctionOutputColumns(&datapb.SegmentInfo{}, []string{"101"})
	s.NoError(err)
	s.False(hasColumns)

	mock := mockey.Mock(packed.ManifestHasColumns).To(
		func(string, *indexpb.StorageConfig, []string) (bool, error) {
			s.FailNow("ManifestHasColumns should not be called when ChildFields already prove the output field exists")
			return false, nil
		}).Build()
	defer mock.UnPatch()
	hasColumns, err = task.segmentHasFunctionOutputColumns(&datapb.SegmentInfo{
		ManifestPath: "manifest",
		Binlogs: []*datapb.FieldBinlog{
			{ChildFields: []int64{100, 101}},
		},
	}, []string{"101"})
	s.NoError(err)
	s.True(hasColumns)
}

func (s *RefreshExternalCollectionTaskSuite) TestOrganizeSegmentsFunctionOutputColumnError() {
	ctx := context.Background()
	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID: s.collectionID,
		TaskID:       s.taskID,
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "text", DataType: schemapb.DataType_VarChar, ExternalField: "text_col"},
			},
			Functions: []*schemapb.FunctionSchema{
				{Type: schemapb.FunctionType_BM25, OutputFieldIds: []int64{101}},
			},
		},
	}
	task := NewRefreshExternalCollectionTask(ctx, req)

	result, err := task.organizeSegments(ctx, nil, nil)
	s.Error(err)
	s.Nil(result)
	s.Contains(err.Error(), "resolve function output columns")

	_, err = estimateFunctionOutputBytesPerRow(req.GetSchema())
	s.Error(err)
	s.Contains(err.Error(), "function output field id 101 not found")
}

func (s *RefreshExternalCollectionTaskSuite) TestOrganizeSegmentsBalanceError() {
	ctx := context.Background()
	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID: s.collectionID,
		TaskID:       s.taskID,
	}
	task := NewRefreshExternalCollectionTask(ctx, req)
	mockBalance := mockey.Mock(mockey.GetMethod(task, "balanceFragmentsToSegments")).
		Return(nil, fmt.Errorf("balance failed")).Build()
	defer mockBalance.UnPatch()

	result, err := task.organizeSegments(ctx, nil, []packed.Fragment{{FragmentID: 1, FilePath: "/data/file.parquet", RowCount: 100}})
	s.Error(err)
	s.Nil(result)
}

func (s *RefreshExternalCollectionTaskSuite) TestOrganizeSegmentsContextCanceledDuringLoops() {
	testCases := []struct {
		name                    string
		cancelAt                int
		currentSegments         []*datapb.SegmentInfo
		currentSegmentFragments packed.SegmentFragments
		newFragments            []packed.Fragment
	}{
		{
			name:            "current segment loop",
			cancelAt:        2,
			currentSegments: []*datapb.SegmentInfo{{ID: 1, CollectionID: s.collectionID, NumOfRows: 100}},
			currentSegmentFragments: packed.SegmentFragments{
				1: []packed.Fragment{{FragmentID: 1, FilePath: "/data/file1.parquet", RowCount: 100}},
			},
			newFragments: []packed.Fragment{{FragmentID: 1, FilePath: "/data/file1.parquet", RowCount: 100}},
		},
		{
			name:            "kept fragment loop",
			cancelAt:        3,
			currentSegments: []*datapb.SegmentInfo{{ID: 1, CollectionID: s.collectionID, NumOfRows: 100}},
			currentSegmentFragments: packed.SegmentFragments{
				1: []packed.Fragment{{FragmentID: 1, FilePath: "/data/file1.parquet", RowCount: 100}},
			},
			newFragments: []packed.Fragment{{FragmentID: 1, FilePath: "/data/file1.parquet", RowCount: 100}},
		},
		{
			name:         "new fragment loop",
			cancelAt:     2,
			newFragments: []packed.Fragment{{FragmentID: 1, FilePath: "/data/file1.parquet", RowCount: 100}},
		},
	}

	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			req := &datapb.RefreshExternalCollectionTaskRequest{
				CollectionID:    s.collectionID,
				TaskID:          s.taskID,
				CurrentSegments: tc.currentSegments,
			}
			task := NewRefreshExternalCollectionTask(ctx, req)

			var calls int
			mockEnsure := mockey.Mock(ensureContext).To(func(ctx context.Context) error {
				calls++
				if calls == tc.cancelAt {
					return context.Canceled
				}
				return nil
			}).Build()
			defer mockEnsure.UnPatch()

			result, err := task.organizeSegments(ctx, tc.currentSegmentFragments, tc.newFragments)
			s.ErrorIs(err, context.Canceled)
			s.Nil(result)
		})
	}
}

func (s *RefreshExternalCollectionTaskSuite) TestOrganizeSegments_NewFragmentsAdded() {
	s.T().Skip("Skip test that requires CGO FFI calls for manifest creation")
}

func (s *RefreshExternalCollectionTaskSuite) TestSplitFileToFragments_SmallFile() {
	// File smaller than limit - should return single fragment
	fragments := packed.SplitFileToFragments("/data/small.parquet", 500000, packed.DefaultFragmentRowLimit, packed.NewFragmentIDGenerator(0))

	s.Len(fragments, 1)
	s.Equal(int64(0), fragments[0].FragmentID)
	s.Equal("/data/small.parquet", fragments[0].FilePath)
	s.Equal(int64(0), fragments[0].StartRow)
	s.Equal(int64(500000), fragments[0].EndRow)
	s.Equal(int64(500000), fragments[0].RowCount)
}

func (s *RefreshExternalCollectionTaskSuite) TestSplitFileToFragments_ExactLimit() {
	// File exactly at limit - should return single fragment
	fragments := packed.SplitFileToFragments("/data/exact.parquet", packed.DefaultFragmentRowLimit, packed.DefaultFragmentRowLimit, packed.NewFragmentIDGenerator(0))

	s.Len(fragments, 1)
	s.Equal(int64(0), fragments[0].FragmentID)
	s.Equal(int64(0), fragments[0].StartRow)
	s.Equal(int64(packed.DefaultFragmentRowLimit), fragments[0].EndRow)
	s.Equal(int64(packed.DefaultFragmentRowLimit), fragments[0].RowCount)
}

func (s *RefreshExternalCollectionTaskSuite) TestSplitFileToFragments_LargeFile() {
	// File with 2.5 million rows - should split into 3 fragments
	totalRows := int64(2500000)
	fragments := packed.SplitFileToFragments("/data/large.parquet", totalRows, packed.DefaultFragmentRowLimit, packed.NewFragmentIDGenerator(0))

	s.Len(fragments, 3)

	// First fragment: 0-1000000
	s.Equal(int64(0), fragments[0].FragmentID)
	s.Equal("/data/large.parquet", fragments[0].FilePath)
	s.Equal(int64(0), fragments[0].StartRow)
	s.Equal(int64(1000000), fragments[0].EndRow)
	s.Equal(int64(1000000), fragments[0].RowCount)

	// Second fragment: 1000000-2000000
	s.Equal(int64(1), fragments[1].FragmentID)
	s.Equal("/data/large.parquet", fragments[1].FilePath)
	s.Equal(int64(1000000), fragments[1].StartRow)
	s.Equal(int64(2000000), fragments[1].EndRow)
	s.Equal(int64(1000000), fragments[1].RowCount)

	// Third fragment: 2000000-2500000
	s.Equal(int64(2), fragments[2].FragmentID)
	s.Equal("/data/large.parquet", fragments[2].FilePath)
	s.Equal(int64(2000000), fragments[2].StartRow)
	s.Equal(int64(2500000), fragments[2].EndRow)
	s.Equal(int64(500000), fragments[2].RowCount)
}

func (s *RefreshExternalCollectionTaskSuite) TestSplitFileToFragments_BaseFragmentID() {
	// Test with non-zero base fragment ID
	fragments := packed.SplitFileToFragments("/data/test.parquet", 2500000, packed.DefaultFragmentRowLimit, packed.NewFragmentIDGenerator(100))

	s.Len(fragments, 3)
	s.Equal(int64(100), fragments[0].FragmentID)
	s.Equal(int64(101), fragments[1].FragmentID)
	s.Equal(int64(102), fragments[2].FragmentID)
}

func (s *RefreshExternalCollectionTaskSuite) TestSplitFileToFragments_ZeroRows() {
	// Empty file - should return single fragment with zero rows
	fragments := packed.SplitFileToFragments("/data/empty.parquet", 0, packed.DefaultFragmentRowLimit, packed.NewFragmentIDGenerator(0))

	s.Len(fragments, 1)
	s.Equal(int64(0), fragments[0].RowCount)
}

func (s *RefreshExternalCollectionTaskSuite) TestSplitFileToFragments_TenMillionRows() {
	// 10 million rows - should split into 10 fragments
	totalRows := int64(10000000)
	fragments := packed.SplitFileToFragments("/data/huge.parquet", totalRows, packed.DefaultFragmentRowLimit, packed.NewFragmentIDGenerator(0))

	s.Len(fragments, 10)

	// Verify total rows across all fragments
	var totalFragmentRows int64
	for _, f := range fragments {
		totalFragmentRows += f.RowCount
	}
	s.Equal(totalRows, totalFragmentRows)

	// Verify fragment IDs are sequential
	for i, f := range fragments {
		s.Equal(int64(i), f.FragmentID)
	}

	// Verify ranges are continuous
	for i := 1; i < len(fragments); i++ {
		s.Equal(fragments[i-1].EndRow, fragments[i].StartRow)
	}
}

func (s *RefreshExternalCollectionTaskSuite) TestCreateManifestForSegment() {
	ctx := context.Background()
	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID:  s.collectionID,
		PartitionID:   2000,
		StorageConfig: &indexpb.StorageConfig{RootPath: "files", StorageType: "local"},
	}
	task := NewRefreshExternalCollectionTask(ctx, req)
	task.parsedSpec = &externalspec.ExternalSpec{Format: "parquet"}
	task.columns = []string{"col1"}

	var gotBasePath string
	var gotStorageConfig *indexpb.StorageConfig
	var gotExtfs packed.ExternalSpecContext
	mockCreate := mockey.Mock(packed.CreateSegmentManifestWithBasePathAndExtfs).
		To(func(
			ctx context.Context,
			basePath string,
			format string,
			columns []string,
			fragments []packed.Fragment,
			storageConfig *indexpb.StorageConfig,
			extfs packed.ExternalSpecContext,
		) (string, error) {
			gotBasePath = basePath
			gotStorageConfig = storageConfig
			gotExtfs = extfs
			return "manifest-path", nil
		}).Build()
	defer mockCreate.UnPatch()

	manifestPath, err := task.createManifestForSegment(ctx, 3000, []packed.Fragment{{FragmentID: 1}})
	s.NoError(err)
	s.Equal("manifest-path", manifestPath)
	s.Equal("files/insert_log/1000/2000/3000", gotBasePath)
	s.Same(req.GetStorageConfig(), gotStorageConfig)
	s.Equal(s.collectionID, gotExtfs.CollectionID)
	s.Empty(gotExtfs.Source)
	s.Empty(gotExtfs.Spec)
}

func (s *RefreshExternalCollectionTaskSuite) TestCreateManifestWithFunctionsUsesInsertLogBasePath() {
	paramtable.Init()

	ctx := context.Background()
	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID:  s.collectionID,
		PartitionID:   2000,
		StorageConfig: &indexpb.StorageConfig{RootPath: "files", StorageType: "local"},
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "text", DataType: schemapb.DataType_VarChar, ExternalField: "text_col"},
				{FieldID: 101, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
			},
			Functions: []*schemapb.FunctionSchema{
				{
					Name:           "bm25",
					Type:           schemapb.FunctionType_BM25,
					InputFieldIds:  []int64{100},
					OutputFieldIds: []int64{101},
				},
			},
		},
	}
	task := NewRefreshExternalCollectionTask(ctx, req)
	task.parsedSpec = &externalspec.ExternalSpec{Format: "parquet"}

	var gotBasePath string
	var gotStorageConfig *indexpb.StorageConfig
	mockExec := mockey.Mock(ExecuteFunctionsForSegment).
		To(func(
			ctx context.Context,
			schema *schemapb.CollectionSchema,
			fragments []packed.Fragment,
			format string,
			storageConfig *indexpb.StorageConfig,
			collectionID int64,
			segmentID int64,
			basePath string,
			clusterID string,
		) (string, error) {
			gotBasePath = basePath
			gotStorageConfig = storageConfig
			return "manifest-path", nil
		}).Build()
	defer mockExec.UnPatch()

	manifestPath, err := task.createManifestWithFunctions(ctx, 3000, []packed.Fragment{{FragmentID: 1}})
	s.NoError(err)
	s.Equal("manifest-path", manifestPath)
	s.Equal("files/insert_log/1000/2000/3000", gotBasePath)
	s.Same(req.GetStorageConfig(), gotStorageConfig)
}

func (s *RefreshExternalCollectionTaskSuite) TestExecuteFunctionsForSegmentUsesProvidedBasePath() {
	ctx := context.Background()
	storageConfig := &indexpb.StorageConfig{RootPath: "files", StorageType: "local"}
	schema := &schemapb.CollectionSchema{
		ExternalSource: "s3://bucket/path",
		ExternalSpec:   `{"format":"parquet"}`,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "text", DataType: schemapb.DataType_VarChar, ExternalField: "text_col"},
		},
	}
	fragments := []packed.Fragment{{FragmentID: 1, FilePath: "s3://bucket/path/file.parquet"}}

	var gotBasePath string
	var gotColumns []string
	var gotFragments []packed.Fragment
	var gotStorageConfig *indexpb.StorageConfig
	mockCreate := mockey.Mock(packed.CreateSegmentManifestWithBasePathAndExtfs).
		To(func(
			ctx context.Context,
			basePath string,
			format string,
			columns []string,
			fragments []packed.Fragment,
			storageConfig *indexpb.StorageConfig,
			extfs packed.ExternalSpecContext,
		) (string, error) {
			gotBasePath = basePath
			gotColumns = columns
			gotFragments = fragments
			gotStorageConfig = storageConfig
			return "", fmt.Errorf("mock create manifest")
		}).Build()
	defer mockCreate.UnPatch()

	manifestPath, err := ExecuteFunctionsForSegment(
		ctx,
		schema,
		fragments,
		"parquet",
		storageConfig,
		s.collectionID,
		3000,
		"files/insert_log/1000/2000/3000",
		"cluster",
	)
	s.Error(err)
	s.Contains(err.Error(), "create input manifest")
	s.Empty(manifestPath)
	s.Equal("files/insert_log/1000/2000/3000", gotBasePath)
	s.Equal([]string{"text_col"}, gotColumns)
	s.Equal(fragments, gotFragments)
	s.Same(storageConfig, gotStorageConfig)
}

func (s *RefreshExternalCollectionTaskSuite) TestExecuteFunctionsForSegmentRequiresFunctionOutputs() {
	ctx := context.Background()
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "text", DataType: schemapb.DataType_VarChar, ExternalField: "text_col"},
		},
	}

	mockCreate := mockey.Mock(packed.CreateSegmentManifestWithBasePathAndExtfs).
		To(func(
			ctx context.Context,
			basePath string,
			format string,
			columns []string,
			fragments []packed.Fragment,
			storageConfig *indexpb.StorageConfig,
			extfs packed.ExternalSpecContext,
		) (string, error) {
			return packed.MarshalManifestPath(basePath, 42), nil
		}).Build()
	defer mockCreate.UnPatch()

	manifestPath, err := ExecuteFunctionsForSegment(
		ctx,
		schema,
		[]packed.Fragment{{FragmentID: 1}},
		"parquet",
		&indexpb.StorageConfig{RootPath: "files", StorageType: "local"},
		s.collectionID,
		3000,
		"files/insert_log/1000/2000/3000",
		"cluster",
	)
	s.Error(err)
	s.Contains(err.Error(), "no function output fields")
	s.Empty(manifestPath)
}

func (s *RefreshExternalCollectionTaskSuite) TestFunctionExecutorSchemaHelpers() {
	schema := &schemapb.CollectionSchema{
		Name:           "test_schema",
		ExternalSource: "s3://bucket/path",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "text", DataType: schemapb.DataType_VarChar, ExternalField: "text_col"},
			{FieldID: 101, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
			{FieldID: 102, Name: "vec", DataType: schemapb.DataType_FloatVector, ExternalField: "vec_col", TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "2"}}},
			{FieldID: 103, Name: common.VirtualPKFieldName, DataType: schemapb.DataType_Int64},
		},
		Functions: []*schemapb.FunctionSchema{
			{Type: schemapb.FunctionType_BM25, InputFieldIds: []int64{100}, OutputFieldIds: []int64{101}},
		},
	}

	inputSchema, executionSchema, requiredFields, err := buildFunctionExecutionSchema(schema)
	s.NoError(err)
	s.True(requiredFields.Contain(int64(100)))
	s.Require().Len(inputSchema.GetFields(), 1)
	s.Equal("text", inputSchema.GetFields()[0].GetName())
	s.Require().Len(executionSchema.GetFields(), 2)
	s.Equal([]int64{100, 101}, []int64{
		executionSchema.GetFields()[0].GetFieldID(),
		executionSchema.GetFields()[1].GetFieldID(),
	})

	outputFields, outputSchema, err := buildOutputSchema(schema)
	s.NoError(err)
	s.Require().Len(outputFields, 1)
	s.Equal(int64(101), outputFields[0].GetFieldID())
	s.Equal("test_schema", outputSchema.GetName())
	s.Equal(outputFields, outputSchema.GetFields())

	_, _, _, err = buildFunctionExecutionSchema(&schemapb.CollectionSchema{
		ExternalSource: "s3://bucket/path",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "text", DataType: schemapb.DataType_VarChar},
			{FieldID: 101, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
		},
		Functions: []*schemapb.FunctionSchema{
			{Type: schemapb.FunctionType_BM25, InputFieldIds: []int64{100}, OutputFieldIds: []int64{101}},
		},
	})
	s.Error(err)
	s.Contains(err.Error(), "has no external_field")
}

func (s *RefreshExternalCollectionTaskSuite) TestBM25Accumulators() {
	schema := &schemapb.CollectionSchema{
		Functions: []*schemapb.FunctionSchema{
			{Type: schemapb.FunctionType_BM25, OutputFieldIds: []int64{101, 102}},
			{Type: schemapb.FunctionType_TextEmbedding, OutputFieldIds: []int64{103}},
		},
	}

	acc := newBM25Accumulators(schema)
	s.Require().Len(acc, 2)
	s.NotNil(acc[101])
	s.NotNil(acc[102])
	s.Nil(acc[103])

	err := accumulateBM25Stats(&storage.InsertData{Data: map[int64]storage.FieldData{}}, map[int64]*storage.BM25Stats{
		101: storage.NewBM25Stats(),
	})
	s.Error(err)
	s.Contains(err.Error(), "missing from batch")

	err = accumulateBM25Stats(&storage.InsertData{Data: map[int64]storage.FieldData{
		101: &storage.StringFieldData{Data: []string{"wrong"}},
	}}, map[int64]*storage.BM25Stats{101: storage.NewBM25Stats()})
	s.Error(err)
	s.Contains(err.Error(), "wrong type")

	stats := storage.NewBM25Stats()
	err = accumulateBM25Stats(&storage.InsertData{Data: map[int64]storage.FieldData{
		101: &storage.SparseFloatVectorFieldData{
			SparseFloatArray: schemapb.SparseFloatArray{
				Contents: [][]byte{
					typeutil.CreateSparseFloatRow([]uint32{1, 2}, []float32{3, 4}),
					typeutil.CreateSparseFloatRow([]uint32{2}, []float32{5}),
				},
			},
		},
	}}, map[int64]*storage.BM25Stats{101: stats})
	s.NoError(err)
	s.Equal(int64(2), stats.NumRow())
}

func (s *RefreshExternalCollectionTaskSuite) TestFinalizeBM25Stats() {
	ctx := context.Background()
	storageConfig := &indexpb.StorageConfig{RootPath: "files", StorageType: "local"}
	manifestPath := packed.MarshalManifestPath("files/insert_log/1000/2000/3000", 42)

	gotManifest, err := finalizeBM25Stats(ctx, nil, storageConfig, manifestPath)
	s.NoError(err)
	s.Equal(manifestPath, gotManifest)

	gotManifest, err = finalizeBM25Stats(ctx, map[int64]*storage.BM25Stats{
		101: storage.NewBM25Stats(),
	}, storageConfig, "bad manifest")
	s.Error(err)
	s.Empty(gotManifest)

	stats := storage.NewBM25Stats()
	stats.Append(map[uint32]float32{1: 2})
	var gotFilePath string
	var gotEntries []packed.StatEntry
	mockWrite := mockey.Mock(packed.WriteFile).
		To(func(storageConfig *indexpb.StorageConfig, filePath string, data []byte) error {
			gotFilePath = filePath
			s.NotEmpty(data)
			return nil
		}).Build()
	defer mockWrite.UnPatch()
	mockCommit := mockey.Mock(packed.CommitManifestUpdates).
		To(func(basePath string, version int64, storageConfig *indexpb.StorageConfig, updates *packed.ManifestUpdates) (string, error) {
			s.Equal("files/insert_log/1000/2000/3000", basePath)
			s.Equal(int64(42), version)
			gotEntries = updates.Stats
			return "updated-manifest", nil
		}).Build()
	defer mockCommit.UnPatch()

	gotManifest, err = finalizeBM25Stats(ctx, map[int64]*storage.BM25Stats{101: stats}, storageConfig, manifestPath)
	s.NoError(err)
	s.Equal("updated-manifest", gotManifest)
	s.Equal("files/insert_log/1000/2000/3000/_stats/bm25.101/0", gotFilePath)
	s.Require().Len(gotEntries, 1)
	s.Equal("bm25.101", gotEntries[0].Key)
}

func (s *RefreshExternalCollectionTaskSuite) TestFinalizeBM25StatsErrors() {
	ctx := context.Background()
	storageConfig := &indexpb.StorageConfig{RootPath: "files", StorageType: "local"}
	manifestPath := packed.MarshalManifestPath("files/insert_log/1000/2000/3000", 42)
	stats := storage.NewBM25Stats()
	stats.Append(map[uint32]float32{1: 2})

	mockSerialize := mockey.Mock(mockey.GetMethod(stats, "Serialize")).Return(nil, fmt.Errorf("serialize failed")).Build()
	gotManifest, err := finalizeBM25Stats(ctx, map[int64]*storage.BM25Stats{101: stats}, storageConfig, manifestPath)
	s.Error(err)
	s.Empty(gotManifest)
	mockSerialize.UnPatch()

	mockWrite := mockey.Mock(packed.WriteFile).Return(fmt.Errorf("write failed")).Build()
	gotManifest, err = finalizeBM25Stats(ctx, map[int64]*storage.BM25Stats{101: stats}, storageConfig, manifestPath)
	s.Error(err)
	s.Empty(gotManifest)
	mockWrite.UnPatch()

	mockWrite = mockey.Mock(packed.WriteFile).Return(nil).Build()
	defer mockWrite.UnPatch()
	mockCommit := mockey.Mock(packed.CommitManifestUpdates).Return("", fmt.Errorf("commit failed")).Build()
	defer mockCommit.UnPatch()
	gotManifest, err = finalizeBM25Stats(ctx, map[int64]*storage.BM25Stats{101: stats}, storageConfig, manifestPath)
	s.Error(err)
	s.Empty(gotManifest)
}

func (s *RefreshExternalCollectionTaskSuite) TestWriteOutputBatch() {
	outputSchema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 101, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector},
		},
	}
	outputArrow, err := storage.ConvertToArrowSchema(outputSchema, true)
	s.NoError(err)

	writer := &packed.FFIPackedWriter{}
	var wroteRows int64
	mockWrite := mockey.Mock(mockey.GetMethod(writer, "WriteRecordBatch")).
		To(func(recordBatch arrow.Record) error {
			wroteRows = recordBatch.NumRows()
			return nil
		}).Build()
	defer mockWrite.UnPatch()

	err = writeOutputBatch(&storage.InsertData{Data: map[int64]storage.FieldData{
		101: &storage.SparseFloatVectorFieldData{
			SparseFloatArray: schemapb.SparseFloatArray{
				Contents: [][]byte{typeutil.CreateSparseFloatRow([]uint32{1}, []float32{2})},
			},
		},
	}}, outputSchema, outputArrow, writer)
	s.NoError(err)
	s.Equal(int64(1), wroteRows)

	err = writeOutputBatch(&storage.InsertData{Data: map[int64]storage.FieldData{}}, outputSchema, outputArrow, writer)
	s.Error(err)
}

func (s *RefreshExternalCollectionTaskSuite) TestOpenInputReader() {
	schema := &schemapb.CollectionSchema{
		ExternalSource: "s3://bucket/path",
		ExternalSpec:   `{"format":"parquet"}`,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "text", DataType: schemapb.DataType_VarChar, ExternalField: "text_col"},
			{FieldID: 101, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
			{FieldID: 102, Name: "vec", DataType: schemapb.DataType_FloatVector, ExternalField: "vec_col", TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "2"}}},
		},
	}
	storageConfig := &indexpb.StorageConfig{RootPath: "files", StorageType: "local"}
	mockEncryption := mockey.Mock(hookutil.IsClusterEncryptionEnabled).Return(false).Build()
	defer mockEncryption.UnPatch()

	var gotColumns []string
	var gotExt packed.ExternalReaderContext
	var gotSchema *arrow.Schema
	mockReader := mockey.Mock(packed.NewFFIPackedReader).
		To(func(
			manifestPath string,
			schema *arrow.Schema,
			neededColumns []string,
			bufferSize int64,
			storageConfig *indexpb.StorageConfig,
			storagePluginContext *indexcgopb.StoragePluginContext,
			ext packed.ExternalReaderContext,
		) (*packed.FFIPackedReader, error) {
			gotColumns = neededColumns
			gotSchema = schema
			gotExt = ext
			return &packed.FFIPackedReader{}, nil
		}).Build()

	inputSchema, _, _, err := buildFunctionExecutionSchema(&schemapb.CollectionSchema{
		ExternalSource: schema.GetExternalSource(),
		ExternalSpec:   schema.GetExternalSpec(),
		Fields:         schema.GetFields(),
		Functions: []*schemapb.FunctionSchema{
			{Type: schemapb.FunctionType_BM25, InputFieldIds: []int64{100}, OutputFieldIds: []int64{101}},
		},
	})
	s.NoError(err)
	reader, err := openInputReader(context.Background(), schema, "manifest", inputSchema, storageConfig, s.collectionID)
	s.NoError(err)
	s.NotNil(reader)
	s.Equal([]string{"text_col"}, gotColumns)
	s.Require().NotNil(gotSchema)
	s.Equal(1, gotSchema.NumFields())
	s.Equal("text_col", gotSchema.Field(0).Name)
	s.Equal(arrow.BinaryTypes.String, gotSchema.Field(0).Type)
	s.Equal(s.collectionID, gotExt.CollectionID)
	s.Equal("s3://bucket/path", gotExt.Source)
	s.Equal(`{"format":"parquet"}`, gotExt.Spec)
	mockReader.UnPatch()

	mockReader = mockey.Mock(packed.NewFFIPackedReader).Return(nil, fmt.Errorf("open failed")).Build()
	defer mockReader.UnPatch()
	reader, err = openInputReader(context.Background(), schema, "manifest", inputSchema, storageConfig, s.collectionID)
	s.Error(err)
	s.Nil(reader)
	s.Contains(err.Error(), "open input manifest")

	reader, err = openInputReader(context.Background(), &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{{FieldID: 100, Name: "bad", DataType: schemapb.DataType_None}},
	}, "manifest", &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{{FieldID: 100, Name: "bad", DataType: schemapb.DataType_None}},
	}, storageConfig, s.collectionID)
	s.Error(err)
	s.Nil(reader)
	s.Contains(err.Error(), "open input manifest")
}

func (s *RefreshExternalCollectionTaskSuite) TestStreamBatchesSuccess() {
	schema := &schemapb.CollectionSchema{DbName: "db"}
	writer := &packed.FFIPackedWriter{}
	executionSchema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "text", DataType: schemapb.DataType_VarChar},
		},
	}
	reader := &fakeRecordReader{
		records: []storage.Record{s.makeStringStorageRecord(100, "text_col", []string{"hello"})},
	}
	mockRun := mockey.Mock(embedding.RunAll).
		To(func(ctx context.Context, schema *schemapb.CollectionSchema, data *storage.InsertData, opts embedding.RunOptions) error {
			s.Equal("cluster", opts.ClusterID)
			s.Equal("db", opts.DBName)
			s.Equal(1, data.GetRowNum())
			return nil
		}).Build()
	defer mockRun.UnPatch()
	mockAcc := mockey.Mock(accumulateBM25Stats).Return(nil).Build()
	defer mockAcc.UnPatch()
	mockWrite := mockey.Mock(writeOutputBatch).Return(nil).Build()
	defer mockWrite.UnPatch()

	rows, err := streamBatches(context.Background(), schema, executionSchema, nil, nil,
		typeutil.NewSet[int64](100), reader, writer, nil, "cluster")
	s.NoError(err)
	s.Equal(int64(1), rows)
}

func (s *RefreshExternalCollectionTaskSuite) TestStreamBatchesErrors() {
	schema := &schemapb.CollectionSchema{}
	executionSchema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "text", DataType: schemapb.DataType_VarChar},
		},
	}
	writer := &packed.FFIPackedWriter{}
	requiredFields := typeutil.NewSet[int64](100)

	reader := &fakeRecordReader{errs: []error{fmt.Errorf("read failed")}}
	rows, err := streamBatches(context.Background(), schema, executionSchema, nil, nil,
		requiredFields, reader, writer, nil, "cluster")
	s.Error(err)
	s.Equal(int64(0), rows)

	reader = &fakeRecordReader{
		records: []storage.Record{s.makeStringStorageRecord(999, "wrong_col", []string{"hello"})},
	}
	rows, err = streamBatches(context.Background(), schema, executionSchema, nil, nil,
		requiredFields, reader, writer, nil, "cluster")
	s.Error(err)
	s.Equal(int64(0), rows)

	reader = &fakeRecordReader{}
	rows, err = streamBatches(context.Background(), schema, executionSchema, nil, nil,
		requiredFields, reader, writer, nil, "cluster")
	s.NoError(err)
	s.Equal(int64(0), rows)

	reader = &fakeRecordReader{records: []storage.Record{nil}}
	rows, err = streamBatches(context.Background(), schema, executionSchema, nil, nil,
		requiredFields, reader, writer, nil, "cluster")
	s.NoError(err)
	s.Equal(int64(0), rows)
}

func (s *RefreshExternalCollectionTaskSuite) TestStreamBatchesProcessingErrors() {
	schema := &schemapb.CollectionSchema{DbName: "db"}
	writer := &packed.FFIPackedWriter{}
	executionSchema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "text", DataType: schemapb.DataType_VarChar},
		},
	}
	requiredFields := typeutil.NewSet[int64](100)

	s.T().Run("empty batch", func(t *testing.T) {
		reader := &fakeRecordReader{
			records: []storage.Record{s.makeStringStorageRecord(100, "text_col", nil)},
		}
		rows, err := streamBatches(context.Background(), schema, executionSchema, nil, nil,
			requiredFields, reader, writer, nil, "cluster")
		s.NoError(err)
		s.Equal(int64(0), rows)
	})

	s.T().Run("function error", func(t *testing.T) {
		reader := &fakeRecordReader{
			records: []storage.Record{s.makeStringStorageRecord(100, "text_col", []string{"hello"})},
		}
		mockRun := mockey.Mock(embedding.RunAll).Return(fmt.Errorf("run failed")).Build()
		defer mockRun.UnPatch()
		rows, err := streamBatches(context.Background(), schema, executionSchema, nil, nil,
			requiredFields, reader, writer, nil, "cluster")
		s.Error(err)
		s.Equal(int64(0), rows)
	})

	s.T().Run("bm25 stats error", func(t *testing.T) {
		reader := &fakeRecordReader{
			records: []storage.Record{s.makeStringStorageRecord(100, "text_col", []string{"hello"})},
		}
		mockRun := mockey.Mock(embedding.RunAll).Return(nil).Build()
		defer mockRun.UnPatch()
		mockAcc := mockey.Mock(accumulateBM25Stats).Return(fmt.Errorf("stats failed")).Build()
		defer mockAcc.UnPatch()
		rows, err := streamBatches(context.Background(), schema, executionSchema, nil, nil,
			requiredFields, reader, writer, nil, "cluster")
		s.Error(err)
		s.Equal(int64(0), rows)
	})

	s.T().Run("write error", func(t *testing.T) {
		reader := &fakeRecordReader{
			records: []storage.Record{s.makeStringStorageRecord(100, "text_col", []string{"hello"})},
		}
		mockRun := mockey.Mock(embedding.RunAll).Return(nil).Build()
		defer mockRun.UnPatch()
		mockAcc := mockey.Mock(accumulateBM25Stats).Return(nil).Build()
		defer mockAcc.UnPatch()
		mockWrite := mockey.Mock(writeOutputBatch).Return(fmt.Errorf("write failed")).Build()
		defer mockWrite.UnPatch()
		rows, err := streamBatches(context.Background(), schema, executionSchema, nil, nil,
			requiredFields, reader, writer, nil, "cluster")
		s.Error(err)
		s.Equal(int64(0), rows)
	})
}

func (s *RefreshExternalCollectionTaskSuite) TestExecuteFunctionsForSegmentSuccess() {
	ctx := context.Background()
	storageConfig := &indexpb.StorageConfig{RootPath: "files", StorageType: "local"}
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "text", DataType: schemapb.DataType_VarChar, ExternalField: "text_col"},
			{FieldID: 101, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
			{FieldID: 102, Name: "vec", DataType: schemapb.DataType_FloatVector, ExternalField: "vec_col", TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "2"}}},
		},
		Functions: []*schemapb.FunctionSchema{
			{Type: schemapb.FunctionType_BM25, InputFieldIds: []int64{100}, OutputFieldIds: []int64{101}},
		},
	}
	reader := &fakeRecordReader{}
	writer := &packed.FFIPackedWriter{}
	basePath := "files/insert_log/1000/2000/3000"

	var sourceColumns []string
	mockCreate := mockey.Mock(packed.CreateSegmentManifestWithBasePathAndExtfs).
		To(func(
			ctx context.Context,
			basePath string,
			format string,
			columns []string,
			fragments []packed.Fragment,
			storageConfig *indexpb.StorageConfig,
			extfs packed.ExternalSpecContext,
		) (string, error) {
			sourceColumns = columns
			return packed.MarshalManifestPath(basePath, 42), nil
		}).Build()
	defer mockCreate.UnPatch()
	var gotInputSchema *schemapb.CollectionSchema
	mockOpen := mockey.Mock(openInputReader).
		To(func(
			ctx context.Context,
			schema *schemapb.CollectionSchema,
			manifestPath string,
			inputSchema *schemapb.CollectionSchema,
			storageConfig *indexpb.StorageConfig,
			collectionID int64,
		) (storage.RecordReader, error) {
			gotInputSchema = inputSchema
			return reader, nil
		}).Build()
	defer mockOpen.UnPatch()
	mockNewWriter := mockey.Mock(packed.NewFFIPackedWriter).
		To(func(
			basePath string,
			schema *arrow.Schema,
			columnGroups []storagecommon.ColumnGroup,
			storageConfig *indexpb.StorageConfig,
			storagePluginContext *indexcgopb.StoragePluginContext,
			extraProperties ...map[string]string,
		) (*packed.FFIPackedWriter, error) {
			s.Require().NotNil(schema)
			s.Equal("101", schema.Field(0).Name)
			s.Require().Len(columnGroups, 1)
			s.Equal([]int{0}, columnGroups[0].Columns)
			return writer, nil
		}).Build()
	defer mockNewWriter.UnPatch()
	mockStream := mockey.Mock(streamBatches).Return(int64(7), nil).Build()
	defer mockStream.UnPatch()
	output := new(packed.ColumnGroups)
	mockClose := mockey.Mock(mockey.GetMethod(writer, "Close")).Return(output, nil).Build()
	defer mockClose.UnPatch()
	mockAppend := mockey.Mock(appendBM25Stats).Return(nil).Build()
	defer mockAppend.UnPatch()
	mockCommit := mockey.Mock(packed.CommitManifestUpdates).
		To(func(basePath string, version int64, storageConfig *indexpb.StorageConfig, updates *packed.ManifestUpdates) (string, error) {
			s.Equal(int64(42), version)
			s.Same(output, updates.NewFiles)
			return "final-manifest", nil
		}).Build()
	defer mockCommit.UnPatch()

	manifestPath, err := ExecuteFunctionsForSegment(
		ctx,
		schema,
		[]packed.Fragment{{FragmentID: 1}},
		"parquet",
		storageConfig,
		s.collectionID,
		3000,
		basePath,
		"cluster",
	)
	s.NoError(err)
	s.Equal("final-manifest", manifestPath)
	s.Equal([]string{"text_col", "vec_col"}, sourceColumns)
	s.Require().NotNil(gotInputSchema)
	s.Require().Len(gotInputSchema.GetFields(), 1)
	s.Equal(int64(100), gotInputSchema.GetFields()[0].GetFieldID())
}

func (s *RefreshExternalCollectionTaskSuite) TestExecuteFunctionsForSegmentErrorPaths() {
	ctx := context.Background()
	storageConfig := &indexpb.StorageConfig{RootPath: "files", StorageType: "local"}
	basePath := "files/insert_log/1000/2000/3000"
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "text", DataType: schemapb.DataType_VarChar, ExternalField: "text_col"},
			{FieldID: 101, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
		},
		Functions: []*schemapb.FunctionSchema{
			{Type: schemapb.FunctionType_BM25, InputFieldIds: []int64{100}, OutputFieldIds: []int64{101}},
		},
	}

	s.T().Run("parse input manifest", func(t *testing.T) {
		mockCreate := mockey.Mock(packed.CreateSegmentManifestWithBasePathAndExtfs).Return("bad manifest", nil).Build()
		defer mockCreate.UnPatch()
		manifestPath, err := ExecuteFunctionsForSegment(ctx, schema, nil, "parquet", storageConfig, s.collectionID, 3000, basePath, "cluster")
		s.Error(err)
		s.Empty(manifestPath)
	})

	s.T().Run("output schema conversion", func(t *testing.T) {
		badSchema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 101, Name: "bad", DataType: schemapb.DataType_None, IsFunctionOutput: true},
			},
		}
		mockCreate := mockey.Mock(packed.CreateSegmentManifestWithBasePathAndExtfs).Return(packed.MarshalManifestPath(basePath, 42), nil).Build()
		defer mockCreate.UnPatch()
		manifestPath, err := ExecuteFunctionsForSegment(ctx, badSchema, nil, "parquet", storageConfig, s.collectionID, 3000, basePath, "cluster")
		s.Error(err)
		s.Empty(manifestPath)
	})

	s.T().Run("execution schema", func(t *testing.T) {
		badSchema := &schemapb.CollectionSchema{
			ExternalSource: "s3://bucket/path",
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "text", DataType: schemapb.DataType_VarChar},
				{FieldID: 101, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
			},
			Functions: []*schemapb.FunctionSchema{
				{Type: schemapb.FunctionType_BM25, InputFieldIds: []int64{100}, OutputFieldIds: []int64{101}},
			},
		}
		mockCreate := mockey.Mock(packed.CreateSegmentManifestWithBasePathAndExtfs).Return(packed.MarshalManifestPath(basePath, 42), nil).Build()
		defer mockCreate.UnPatch()
		manifestPath, err := ExecuteFunctionsForSegment(ctx, badSchema, nil, "parquet", storageConfig, s.collectionID, 3000, basePath, "cluster")
		s.Error(err)
		s.Empty(manifestPath)
		s.Contains(err.Error(), "has no external_field")
	})

	s.T().Run("open reader", func(t *testing.T) {
		mockCreate := mockey.Mock(packed.CreateSegmentManifestWithBasePathAndExtfs).Return(packed.MarshalManifestPath(basePath, 42), nil).Build()
		defer mockCreate.UnPatch()
		mockOpen := mockey.Mock(openInputReader).Return(nil, fmt.Errorf("open failed")).Build()
		defer mockOpen.UnPatch()
		manifestPath, err := ExecuteFunctionsForSegment(ctx, schema, nil, "parquet", storageConfig, s.collectionID, 3000, basePath, "cluster")
		s.Error(err)
		s.Empty(manifestPath)
	})

	s.T().Run("new writer", func(t *testing.T) {
		reader := &fakeRecordReader{}
		mockCreate := mockey.Mock(packed.CreateSegmentManifestWithBasePathAndExtfs).Return(packed.MarshalManifestPath(basePath, 42), nil).Build()
		defer mockCreate.UnPatch()
		mockOpen := mockey.Mock(openInputReader).Return(reader, nil).Build()
		defer mockOpen.UnPatch()
		mockWriter := mockey.Mock(packed.NewFFIPackedWriter).Return(nil, fmt.Errorf("writer failed")).Build()
		defer mockWriter.UnPatch()
		manifestPath, err := ExecuteFunctionsForSegment(ctx, schema, nil, "parquet", storageConfig, s.collectionID, 3000, basePath, "cluster")
		s.Error(err)
		s.Empty(manifestPath)
	})

	s.T().Run("stream", func(t *testing.T) {
		reader := &fakeRecordReader{}
		writer := &packed.FFIPackedWriter{}
		mockCreate := mockey.Mock(packed.CreateSegmentManifestWithBasePathAndExtfs).Return(packed.MarshalManifestPath(basePath, 42), nil).Build()
		defer mockCreate.UnPatch()
		mockOpen := mockey.Mock(openInputReader).Return(reader, nil).Build()
		defer mockOpen.UnPatch()
		mockWriter := mockey.Mock(packed.NewFFIPackedWriter).Return(writer, nil).Build()
		defer mockWriter.UnPatch()
		mockStream := mockey.Mock(streamBatches).Return(int64(0), fmt.Errorf("stream failed")).Build()
		defer mockStream.UnPatch()
		manifestPath, err := ExecuteFunctionsForSegment(ctx, schema, nil, "parquet", storageConfig, s.collectionID, 3000, basePath, "cluster")
		s.Error(err)
		s.Empty(manifestPath)
	})

	s.T().Run("close writer", func(t *testing.T) {
		reader := &fakeRecordReader{}
		writer := &packed.FFIPackedWriter{}
		mockCreate := mockey.Mock(packed.CreateSegmentManifestWithBasePathAndExtfs).Return(packed.MarshalManifestPath(basePath, 42), nil).Build()
		defer mockCreate.UnPatch()
		mockOpen := mockey.Mock(openInputReader).Return(reader, nil).Build()
		defer mockOpen.UnPatch()
		mockWriter := mockey.Mock(packed.NewFFIPackedWriter).Return(writer, nil).Build()
		defer mockWriter.UnPatch()
		mockStream := mockey.Mock(streamBatches).Return(int64(1), nil).Build()
		defer mockStream.UnPatch()
		mockClose := mockey.Mock(mockey.GetMethod(writer, "Close")).Return(nil, fmt.Errorf("close failed")).Build()
		defer mockClose.UnPatch()
		manifestPath, err := ExecuteFunctionsForSegment(ctx, schema, nil, "parquet", storageConfig, s.collectionID, 3000, basePath, "cluster")
		s.Error(err)
		s.Empty(manifestPath)
	})

	s.T().Run("finalize stats", func(t *testing.T) {
		reader := &fakeRecordReader{}
		writer := &packed.FFIPackedWriter{}
		mockCreate := mockey.Mock(packed.CreateSegmentManifestWithBasePathAndExtfs).Return(packed.MarshalManifestPath(basePath, 42), nil).Build()
		defer mockCreate.UnPatch()
		mockOpen := mockey.Mock(openInputReader).Return(reader, nil).Build()
		defer mockOpen.UnPatch()
		mockWriter := mockey.Mock(packed.NewFFIPackedWriter).Return(writer, nil).Build()
		defer mockWriter.UnPatch()
		mockStream := mockey.Mock(streamBatches).Return(int64(1), nil).Build()
		defer mockStream.UnPatch()
		mockClose := mockey.Mock(mockey.GetMethod(writer, "Close")).Return(new(packed.ColumnGroups), nil).Build()
		defer mockClose.UnPatch()
		mockAppend := mockey.Mock(appendBM25Stats).Return(fmt.Errorf("finalize failed")).Build()
		defer mockAppend.UnPatch()
		manifestPath, err := ExecuteFunctionsForSegment(ctx, schema, nil, "parquet", storageConfig, s.collectionID, 3000, basePath, "cluster")
		s.Error(err)
		s.Empty(manifestPath)
	})
}

func (s *RefreshExternalCollectionTaskSuite) TestTaskAccessorsAndCloneHelpers() {
	var nilCtx context.Context
	s.NoError(ensureContext(nilCtx))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s.Error(ensureContext(ctx))

	taskCtx := context.Background()
	task := NewRefreshExternalCollectionTask(taskCtx, &datapb.RefreshExternalCollectionTaskRequest{TaskID: 10})
	task.updatedSegments = []*datapb.SegmentInfo{{ID: 100}}
	s.Equal(taskCtx, task.Ctx())
	s.Equal(task.updatedSegments, task.GetUpdatedSegments())

	segments := []*datapb.SegmentInfo{{ID: 1}, nil, {ID: 2}}
	cloned := cloneSegments(segments)
	s.Require().Len(cloned, 3)
	s.Equal(int64(1), cloned[0].GetID())
	s.Nil(cloned[1])
	s.NotSame(segments[0], cloned[0])
	cloned[0].ID = 10
	s.Equal(int64(1), segments[0].GetID())

	info := &TaskInfo{
		State:           indexpb.JobState_JobStateInProgress,
		CollID:          1000,
		KeptSegments:    []int64{1, 2},
		UpdatedSegments: []*datapb.SegmentInfo{{ID: 3}},
	}
	infoClone := info.Clone()
	s.Equal(info.KeptSegments, infoClone.KeptSegments)
	s.Equal(info.UpdatedSegments[0].GetID(), infoClone.UpdatedSegments[0].GetID())
	infoClone.KeptSegments[0] = 100
	infoClone.UpdatedSegments[0].ID = 300
	s.Equal(int64(1), info.KeptSegments[0])
	s.Equal(int64(3), info.UpdatedSegments[0].GetID())
}

func (s *RefreshExternalCollectionTaskSuite) TestExecuteWithMockedSteps() {
	ctx := context.Background()
	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID:           s.collectionID,
		TaskID:                 s.taskID,
		PreAllocatedSegmentIds: &datapb.IDRange{Begin: 3000, End: 4000},
		CurrentSegments:        []*datapb.SegmentInfo{{ID: 1}},
		ExploreManifestPath:    "manifest",
		StorageConfig:          &indexpb.StorageConfig{RootPath: "files", StorageType: "local"},
		Schema:                 &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 100, Name: "text", DataType: schemapb.DataType_VarChar}}},
		ExternalSource:         "s3://bucket/path",
		ExternalSpec:           `{"format":"parquet"}`,
		NumSegmentsExpected:    1,
		FileIndexBegin:         0,
		FileIndexEnd:           1,
	}
	task := NewRefreshExternalCollectionTask(ctx, req)
	task.parsedSpec = &externalspec.ExternalSpec{Format: "parquet"}
	task.columns = []string{"text"}
	fragments := []packed.Fragment{{FragmentID: 1, FilePath: "file", StartRow: 0, EndRow: 10, RowCount: 10}}
	segmentFragments := packed.SegmentFragments{1: fragments}
	updated := []*datapb.SegmentInfo{{ID: 1}}

	mockFetch := mockey.Mock(mockey.GetMethod(task, "fetchFragmentsFromExternalSource")).Return(fragments, nil).Build()
	defer mockFetch.UnPatch()
	mockBuild := mockey.Mock(mockey.GetMethod(task, "buildCurrentSegmentFragments")).Return(segmentFragments, nil).Build()
	defer mockBuild.UnPatch()
	mockOrganize := mockey.Mock(mockey.GetMethod(task, "organizeSegments")).
		To(func(ctx context.Context, currentSegmentFragments packed.SegmentFragments, newFragments []packed.Fragment) ([]*datapb.SegmentInfo, error) {
			task.updatedSegments = updated
			return updated, nil
		}).Build()
	defer mockOrganize.UnPatch()

	err := task.Execute(ctx)
	s.NoError(err)
	s.Equal(req.GetPreAllocatedSegmentIds(), task.preallocatedIDRange)
	s.Equal(int64(3000), task.nextAllocID)
	s.Equal(updated, task.GetUpdatedSegments())
}

func (s *RefreshExternalCollectionTaskSuite) TestExecuteErrorPathsWithMockedSteps() {
	ctx := context.Background()
	newTask := func() *RefreshExternalCollectionTask {
		req := &datapb.RefreshExternalCollectionTaskRequest{
			CollectionID:           s.collectionID,
			TaskID:                 s.taskID,
			PreAllocatedSegmentIds: &datapb.IDRange{Begin: 3000, End: 4000},
		}
		return NewRefreshExternalCollectionTask(ctx, req)
	}

	task := NewRefreshExternalCollectionTask(ctx, &datapb.RefreshExternalCollectionTaskRequest{TaskID: s.taskID})
	s.Error(task.Execute(ctx))

	task = newTask()
	mockFetch := mockey.Mock(mockey.GetMethod(task, "fetchFragmentsFromExternalSource")).Return(nil, fmt.Errorf("fetch failed")).Build()
	err := task.Execute(ctx)
	s.Error(err)
	s.Contains(err.Error(), "failed to fetch fragments")
	mockFetch.UnPatch()

	task = newTask()
	fragments := []packed.Fragment{{FragmentID: 1}}
	mockFetch = mockey.Mock(mockey.GetMethod(task, "fetchFragmentsFromExternalSource")).Return(fragments, nil).Build()
	mockBuild := mockey.Mock(mockey.GetMethod(task, "buildCurrentSegmentFragments")).Return(nil, fmt.Errorf("build failed")).Build()
	err = task.Execute(ctx)
	s.Error(err)
	s.Contains(err.Error(), "failed to build current segment fragments")
	mockBuild.UnPatch()
	mockFetch.UnPatch()

	task = newTask()
	mockFetch = mockey.Mock(mockey.GetMethod(task, "fetchFragmentsFromExternalSource")).Return(fragments, nil).Build()
	mockBuild = mockey.Mock(mockey.GetMethod(task, "buildCurrentSegmentFragments")).Return(packed.SegmentFragments{}, nil).Build()
	mockOrganize := mockey.Mock(mockey.GetMethod(task, "organizeSegments")).Return(nil, fmt.Errorf("organize failed")).Build()
	err = task.Execute(ctx)
	s.Error(err)
	s.Contains(err.Error(), "organize failed")
	mockOrganize.UnPatch()
	mockBuild.UnPatch()
	mockFetch.UnPatch()
}

func (s *RefreshExternalCollectionTaskSuite) TestPostExecuteAndOrganizeCanceled() {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	task := NewRefreshExternalCollectionTask(ctx, &datapb.RefreshExternalCollectionTaskRequest{
		TaskID:       s.taskID,
		CollectionID: s.collectionID,
	})
	s.Error(task.PostExecute(ctx))

	segments, err := task.organizeSegments(ctx, nil, nil)
	s.Error(err)
	s.Nil(segments)

	segments, err = task.balanceFragmentsToSegments(ctx, []packed.Fragment{{FragmentID: 1, RowCount: 1}})
	s.Error(err)
	s.Nil(segments)
}

func (s *RefreshExternalCollectionTaskSuite) TestBuildCurrentSegmentFragments() {
	ctx := context.Background()
	req := &datapb.RefreshExternalCollectionTaskRequest{
		StorageConfig:   &indexpb.StorageConfig{RootPath: "files", StorageType: "local"},
		CurrentSegments: []*datapb.SegmentInfo{{ID: 1}},
	}
	task := NewRefreshExternalCollectionTask(ctx, req)
	task.columns = []string{"text_col"}
	expected := packed.SegmentFragments{1: []packed.Fragment{{FragmentID: 1}}}
	var gotColumns []string
	mockBuild := mockey.Mock(packed.BuildCurrentSegmentFragments).
		To(func(
			segments []*datapb.SegmentInfo,
			storageConfig *indexpb.StorageConfig,
			columns []string,
		) (packed.SegmentFragments, error) {
			gotColumns = columns
			return expected, nil
		}).Build()
	defer mockBuild.UnPatch()

	got, err := task.buildCurrentSegmentFragments()
	s.NoError(err)
	s.Equal(expected, got)
	s.Equal([]string{"text_col"}, gotColumns)
}

func (s *RefreshExternalCollectionTaskSuite) TestBalanceFragmentsToSegmentsWithFunctions() {
	paramtable.Init()

	ctx := context.Background()
	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID:           s.collectionID,
		PartitionID:            2000,
		TaskID:                 s.taskID,
		PreAllocatedSegmentIds: &datapb.IDRange{Begin: 3000, End: 4000},
		StorageConfig:          &indexpb.StorageConfig{RootPath: "files", StorageType: "local"},
		ExternalSource:         "s3://bucket/data/",
		ExternalSpec:           `{"format":"parquet"}`,
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "text", DataType: schemapb.DataType_VarChar, ExternalField: "text_col"},
				{FieldID: 101, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
			},
			Functions: []*schemapb.FunctionSchema{
				{Type: schemapb.FunctionType_BM25, InputFieldIds: []int64{100}, OutputFieldIds: []int64{101}},
			},
		},
	}
	task := NewRefreshExternalCollectionTask(ctx, req)
	task.preallocatedIDRange = req.GetPreAllocatedSegmentIds()
	task.nextAllocID = task.preallocatedIDRange.Begin
	task.parsedSpec = &externalspec.ExternalSpec{Format: "parquet"}

	var gotBasePath string
	mockExec := mockey.Mock(ExecuteFunctionsForSegment).
		To(func(
			ctx context.Context,
			schema *schemapb.CollectionSchema,
			fragments []packed.Fragment,
			format string,
			storageConfig *indexpb.StorageConfig,
			collectionID int64,
			segmentID int64,
			basePath string,
			clusterID string,
		) (string, error) {
			gotBasePath = basePath
			return packed.MarshalManifestPath(basePath, 42), nil
		}).Build()
	defer mockExec.UnPatch()
	mockSample := mockey.Mock(packed.SampleExternalFieldSizes).
		Return(map[string]int64{"text_col": 64}, nil).Build()
	defer mockSample.UnPatch()

	result, err := task.balanceFragmentsToSegments(ctx, []packed.Fragment{{FragmentID: 1, RowCount: 10}})
	s.NoError(err)
	s.Require().Len(result, 1)
	s.Equal("files/insert_log/1000/2000/3000", gotBasePath)
	s.Equal(int64(3000), result[0].GetID())
	s.NotZero(result[0].GetBinlogs()[0].GetBinlogs()[0].GetMemorySize())
}

func (s *RefreshExternalCollectionTaskSuite) makeStringRecord(name string, values []string) arrow.Record {
	arrowSchema := arrow.NewSchema([]arrow.Field{{Name: name, Type: arrow.BinaryTypes.String}}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer builder.Release()
	stringBuilder := builder.Field(0).(*array.StringBuilder)
	stringBuilder.AppendValues(values, nil)
	return builder.NewRecord()
}

func (s *RefreshExternalCollectionTaskSuite) makeStringStorageRecord(fieldID int64, name string, values []string) storage.Record {
	return storage.NewSimpleArrowRecord(s.makeStringRecord(name, values), map[storage.FieldID]int{
		fieldID: 0,
	})
}

func (s *RefreshExternalCollectionTaskSuite) TestPreAllocatedSegmentIDs() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create request with pre-allocated IDs
	idRange := &datapb.IDRange{
		Begin: 1000,
		End:   2000,
	}

	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID:           s.collectionID,
		TaskID:                 s.taskID,
		PreAllocatedSegmentIds: idRange,
		NumSegmentsExpected:    1000,
		CurrentSegments:        []*datapb.SegmentInfo{},
	}

	task := NewRefreshExternalCollectionTask(ctx, req)

	// Before Execute(), pre-allocated fields should not be initialized
	s.Nil(task.preallocatedIDRange)
	s.Equal(int64(0), task.nextAllocID)

	// After testing with pre-allocated IDs manually set (simulating Execute)
	task.preallocatedIDRange = idRange
	task.nextAllocID = idRange.Begin

	// Verify pre-allocated IDs are properly initialized
	s.NotNil(task.preallocatedIDRange)
	s.Equal(int64(1000), task.preallocatedIDRange.Begin)
	s.Equal(int64(2000), task.preallocatedIDRange.End)
	s.Equal(int64(1000), task.nextAllocID)
}

func (s *RefreshExternalCollectionTaskSuite) TestPreAllocatedIDAllocation() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create request with pre-allocated IDs
	idRange := &datapb.IDRange{
		Begin: 5000,
		End:   5010, // Only 10 IDs available
	}

	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID:           s.collectionID,
		TaskID:                 s.taskID,
		PreAllocatedSegmentIds: idRange,
		NumSegmentsExpected:    10,
		CurrentSegments:        []*datapb.SegmentInfo{},
	}

	task := NewRefreshExternalCollectionTask(ctx, req)

	// Manually initialize (simulating Execute)
	task.preallocatedIDRange = idRange
	task.nextAllocID = idRange.Begin

	// Simulate creating multiple segments by advancing nextAllocID
	// This should work fine up to the limit
	for i := 0; i < 10; i++ {
		s.True(task.nextAllocID < task.preallocatedIDRange.End, "Should have IDs available")
		task.nextAllocID++
	}

	// Now we should be out of IDs
	s.Equal(task.preallocatedIDRange.End, task.nextAllocID)
}

func (s *RefreshExternalCollectionTaskSuite) TestMissingPreAllocatedIDs() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create request WITHOUT pre-allocated IDs
	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID:    s.collectionID,
		TaskID:          s.taskID,
		CurrentSegments: []*datapb.SegmentInfo{},
		// PreAllocatedSegmentIds is nil
	}

	task := NewRefreshExternalCollectionTask(ctx, req)

	// Execute should fail because pre-allocated IDs are missing
	err := task.Execute(ctx)
	s.NotNil(err)
	s.Contains(err.Error(), "pre-allocated segment IDs not provided")
}

func (s *RefreshExternalCollectionTaskSuite) TestPreExecute_NilSchema() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID:   s.collectionID,
		TaskID:         s.taskID,
		ExternalSource: "test_source",
		StorageConfig:  &indexpb.StorageConfig{StorageType: "local"},
		// Schema is nil
	}
	task := NewRefreshExternalCollectionTask(ctx, req)
	err := task.PreExecute(ctx)
	s.Error(err)
	s.Contains(err.Error(), "schema is nil")
}

func (s *RefreshExternalCollectionTaskSuite) TestPreExecute_NilStorageConfig() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID:   s.collectionID,
		TaskID:         s.taskID,
		ExternalSource: "test_source",
		Schema:         &schemapb.CollectionSchema{},
		// StorageConfig is nil
	}
	task := NewRefreshExternalCollectionTask(ctx, req)
	err := task.PreExecute(ctx)
	s.Error(err)
	s.Contains(err.Error(), "storage config is nil")
}

func (s *RefreshExternalCollectionTaskSuite) TestPreExecute_EmptyExternalSource() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID:  s.collectionID,
		TaskID:        s.taskID,
		Schema:        &schemapb.CollectionSchema{},
		StorageConfig: &indexpb.StorageConfig{StorageType: "local"},
		// ExternalSource is empty
	}
	task := NewRefreshExternalCollectionTask(ctx, req)
	err := task.PreExecute(ctx)
	s.Error(err)
	s.Contains(err.Error(), "external source is empty")
}

func (s *RefreshExternalCollectionTaskSuite) TestPreExecute_InvalidExternalSpec() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID:   s.collectionID,
		TaskID:         s.taskID,
		ExternalSource: "test_source",
		ExternalSpec:   "not-valid-json",
		Schema:         &schemapb.CollectionSchema{},
		StorageConfig:  &indexpb.StorageConfig{StorageType: "local"},
	}
	task := NewRefreshExternalCollectionTask(ctx, req)
	err := task.PreExecute(ctx)
	s.Error(err)
	s.Contains(err.Error(), "failed to parse external spec")
}

func (s *RefreshExternalCollectionTaskSuite) TestPreExecute_UnsupportedFormat() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID:   s.collectionID,
		TaskID:         s.taskID,
		ExternalSource: "test_source",
		ExternalSpec:   `{"format":"avro"}`,
		Schema:         &schemapb.CollectionSchema{},
		StorageConfig:  &indexpb.StorageConfig{StorageType: "local"},
	}
	task := NewRefreshExternalCollectionTask(ctx, req)
	err := task.PreExecute(ctx)
	s.Error(err)
	s.Contains(err.Error(), "unsupported format")
}

func (s *RefreshExternalCollectionTaskSuite) TestParseExternalSpec() {
	// Empty spec defaults to parquet
	spec, err := externalspec.ParseExternalSpec("")
	s.NoError(err)
	s.Equal("parquet", spec.Format)

	// Lance-table format
	spec, err = externalspec.ParseExternalSpec(`{"format":"lance-table"}`)
	s.NoError(err)
	s.Equal("lance-table", spec.Format)

	// Vortex format
	spec, err = externalspec.ParseExternalSpec(`{"format":"vortex"}`)
	s.NoError(err)
	s.Equal("vortex", spec.Format)

	// Missing format defaults to parquet
	spec, err = externalspec.ParseExternalSpec(`{}`)
	s.NoError(err)
	s.Equal("parquet", spec.Format)

	// Invalid JSON
	_, err = externalspec.ParseExternalSpec("not-json")
	s.Error(err)

	// Unsupported format
	_, err = externalspec.ParseExternalSpec(`{"format":"avro"}`)
	s.Error(err)
	s.Contains(err.Error(), "unsupported format")
}

func (s *RefreshExternalCollectionTaskSuite) TestParseExternalSpec_ExtfsWhitelist() {
	// Valid extfs keys
	spec, err := externalspec.ParseExternalSpec(`{"format":"parquet","extfs":{"use_iam":"true","region":"us-west-2"}}`)
	s.NoError(err)
	s.Equal("true", spec.Extfs["use_iam"])
	s.Equal("us-west-2", spec.Extfs["region"])

	// All allowed keys should be accepted
	spec, err = externalspec.ParseExternalSpec(`{"format":"parquet","extfs":{"use_ssl":"false","use_virtual_host":"false","cloud_provider":"aws","iam_endpoint":"https://sts.amazonaws.com","storage_type":"remote","ssl_ca_cert":"cert"}}`)
	s.NoError(err)
	s.Len(spec.Extfs, 6)

	// Credential keys are now allowed for cross-bucket scenarios
	spec, err = externalspec.ParseExternalSpec(`{"format":"parquet","extfs":{"access_key_id":"AKIA..."}}`)
	s.NoError(err)
	s.Equal("AKIA...", spec.Extfs["access_key_id"])

	spec, err = externalspec.ParseExternalSpec(`{"format":"parquet","extfs":{"access_key_value":"secret"}}`)
	s.NoError(err)
	s.Equal("secret", spec.Extfs["access_key_value"])

	// Unknown key
	_, err = externalspec.ParseExternalSpec(`{"format":"parquet","extfs":{"unknown_key":"val"}}`)
	s.Error(err)
	s.Contains(err.Error(), "not allowed")

	// Empty extfs is fine
	spec, err = externalspec.ParseExternalSpec(`{"format":"parquet","extfs":{}}`)
	s.NoError(err)
	s.Empty(spec.Extfs)

	// No extfs field is fine
	spec, err = externalspec.ParseExternalSpec(`{"format":"parquet"}`)
	s.NoError(err)
	s.Nil(spec.Extfs)
}

func (s *RefreshExternalCollectionTaskSuite) TestParseExternalSpec_ExtfsBooleanValidation() {
	// Valid boolean values
	_, err := externalspec.ParseExternalSpec(`{"format":"parquet","extfs":{"use_iam":"true"}}`)
	s.NoError(err)
	_, err = externalspec.ParseExternalSpec(`{"format":"parquet","extfs":{"use_ssl":"false"}}`)
	s.NoError(err)

	// Invalid boolean values
	_, err = externalspec.ParseExternalSpec(`{"format":"parquet","extfs":{"use_iam":"maybe"}}`)
	s.Error(err)
	s.Contains(err.Error(), "must be \"true\" or \"false\"")

	_, err = externalspec.ParseExternalSpec(`{"format":"parquet","extfs":{"use_ssl":"1"}}`)
	s.Error(err)

	_, err = externalspec.ParseExternalSpec(`{"format":"parquet","extfs":{"use_virtual_host":"yes"}}`)
	s.Error(err)

	// Non-boolean keys accept any string value
	_, err = externalspec.ParseExternalSpec(`{"format":"parquet","extfs":{"region":"any-value-is-ok"}}`)
	s.NoError(err)
}

func (s *RefreshExternalCollectionTaskSuite) TestFragmentKey() {
	f1 := packed.Fragment{FilePath: "/data/file1.parquet", StartRow: 0, EndRow: 1000}
	f2 := packed.Fragment{FilePath: "/data/file1.parquet", StartRow: 1000, EndRow: 2000}
	f3 := packed.Fragment{FilePath: "/data/file2.parquet", StartRow: 0, EndRow: 1000}

	// Same file, different ranges should produce different keys
	s.NotEqual(fragmentKey(f1), fragmentKey(f2))

	// Different files, same ranges should produce different keys
	s.NotEqual(fragmentKey(f1), fragmentKey(f3))

	// Identical fragments should produce the same key
	f4 := packed.Fragment{FilePath: "/data/file1.parquet", StartRow: 0, EndRow: 1000}
	s.Equal(fragmentKey(f1), fragmentKey(f4))

	// L0 deltalogs are not part of the L1 data identity. They are handled by a
	// manifest-only refresh so the target segment ID can be reused.
	f5 := packed.Fragment{
		FilePath: "/data/file1.parquet",
		StartRow: 0,
		EndRow:   1000,
		Deltalogs: []*datapb.FieldBinlog{{
			FieldID: 100,
			Binlogs: []*datapb.Binlog{{
				LogID:      10,
				LogPath:    "s3://bucket/files/insert_log/1/_delta/10",
				EntriesNum: 2,
			}},
		}},
	}
	f6 := packed.Fragment{
		FilePath: "/data/file1.parquet",
		StartRow: 0,
		EndRow:   1000,
		Deltalogs: []*datapb.FieldBinlog{{
			FieldID: 100,
			Binlogs: []*datapb.Binlog{{
				LogID:      11,
				LogPath:    "s3://bucket/files/insert_log/1/_delta/11",
				EntriesNum: 2,
			}},
		}},
	}
	s.Equal(fragmentKey(f1), fragmentKey(f5))
	s.Equal(fragmentKey(f5), fragmentKey(f6))
}

func (s *RefreshExternalCollectionTaskSuite) TestGetColumnNamesFromSchema() {
	// Nil schema
	columns := packed.GetColumnNamesFromSchema(nil)
	s.Nil(columns)

	// Schema with ExternalField set
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{Name: "id", ExternalField: "external_id"},
			{Name: "vector", ExternalField: "embedding"},
		},
	}
	columns = packed.GetColumnNamesFromSchema(schema)
	s.Equal([]string{"external_id", "embedding"}, columns)

	// Schema without ExternalField (fallback to field name)
	schema2 := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{Name: "id"},
			{Name: "vector"},
		},
	}
	columns = packed.GetColumnNamesFromSchema(schema2)
	s.Equal([]string{"id", "vector"}, columns)

	// Mixed: some with ExternalField, some without
	schema3 := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{Name: "id", ExternalField: "external_id"},
			{Name: "vector"},
		},
	}
	columns = packed.GetColumnNamesFromSchema(schema3)
	s.Equal([]string{"external_id", "vector"}, columns)
}

func (s *RefreshExternalCollectionTaskSuite) TestFetchFragmentsFromExternalSource_EmptyManifest() {
	ctx := context.Background()
	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID:        s.collectionID,
		TaskID:              s.taskID,
		ExternalSource:      "s3:///bucket/path",
		ExternalSpec:        `{"format":"parquet"}`,
		ExploreManifestPath: "", // empty
	}
	task := NewRefreshExternalCollectionTask(ctx, req)
	task.parsedSpec = &externalspec.ExternalSpec{Format: "parquet"}

	_, err := task.fetchFragmentsFromExternalSource(ctx)
	s.Error(err)
	s.Contains(err.Error(), "manifest path is required")
}

func (s *RefreshExternalCollectionTaskSuite) TestFetchFragmentsFromExternalSource_Success() {
	paramtable.Init()
	ctx := context.Background()
	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID:        s.collectionID,
		TaskID:              s.taskID,
		ExternalSource:      "s3:///bucket/path",
		ExternalSpec:        `{"format":"parquet"}`,
		ExploreManifestPath: "/manifests/explore.json",
		FileIndexBegin:      0,
		FileIndexEnd:        5,
		StorageConfig:       &indexpb.StorageConfig{StorageType: "local", BucketName: "/tmp"},
	}
	task := NewRefreshExternalCollectionTask(ctx, req)
	task.parsedSpec = &externalspec.ExternalSpec{Format: "parquet"}
	task.columns = []string{"col1"}

	mockFetch := mockey.Mock(packed.FetchFragmentsFromExternalSourceWithRange).
		Return([]packed.Fragment{
			{FragmentID: 0, FilePath: "f1.parquet", StartRow: 0, EndRow: 1000, RowCount: 1000},
		}, nil).Build()
	defer mockFetch.UnPatch()

	frags, err := task.fetchFragmentsFromExternalSource(ctx)
	s.NoError(err)
	s.Len(frags, 1)
}

func (s *RefreshExternalCollectionTaskSuite) TestBalanceFragmentsToSegments_CreateManifestError() {
	paramtable.Init()
	ctx := context.Background()
	tmpDir := s.T().TempDir()
	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID:   s.collectionID,
		ExternalSource: "s3:///bucket/path",
		ExternalSpec:   `{"format":"parquet"}`,
		StorageConfig:  &indexpb.StorageConfig{StorageType: "local", BucketName: tmpDir, RootPath: tmpDir},
	}
	task := NewRefreshExternalCollectionTask(ctx, req)
	task.parsedSpec = &externalspec.ExternalSpec{Format: "parquet"}
	task.columns = []string{"col1"}
	task.preallocatedIDRange = &datapb.IDRange{Begin: 1, End: 100}
	task.nextAllocID = 1

	// Mock createManifestForSegment to return error
	mockCM := mockey.Mock((*RefreshExternalCollectionTask).createManifestForSegment).
		Return("", fmt.Errorf("create manifest failed")).Build()
	defer mockCM.UnPatch()

	fragments := []packed.Fragment{
		{FragmentID: 0, FilePath: "f.parquet", RowCount: 100, StartRow: 0, EndRow: 100},
	}
	_, err := task.balanceFragmentsToSegments(ctx, fragments)
	s.Error(err)
	s.Contains(err.Error(), "create manifest failed")
}

func (s *RefreshExternalCollectionTaskSuite) TestBalanceFragmentsToSegments_ContextCanceled() {
	paramtable.Init()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cancel()
	tmpDir := s.T().TempDir()
	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID:   s.collectionID,
		ExternalSource: "s3:///bucket/path",
		ExternalSpec:   `{"format":"parquet"}`,
		StorageConfig:  &indexpb.StorageConfig{StorageType: "local", BucketName: tmpDir, RootPath: tmpDir},
	}
	task := NewRefreshExternalCollectionTask(ctx, req)
	task.parsedSpec = &externalspec.ExternalSpec{Format: "parquet"}
	task.columns = []string{"col1"}
	task.preallocatedIDRange = &datapb.IDRange{Begin: 1, End: 100}
	task.nextAllocID = 1

	// createManifestForSegment should not be called because ctx is canceled
	mockCM := mockey.Mock((*RefreshExternalCollectionTask).createManifestForSegment).
		Return("/manifest", nil).Build()
	defer mockCM.UnPatch()

	fragments := []packed.Fragment{
		{FragmentID: 0, FilePath: "f.parquet", RowCount: 100, StartRow: 0, EndRow: 100},
	}
	_, err := task.balanceFragmentsToSegments(ctx, fragments)
	s.Error(err)
}

// TestBalanceFragmentsToSegments_CtxCancelledDuringManifest verifies the
// post-AwaitAll ctx.Err() guard: if workers finish without error (e.g. a mock
// that ignores ctx) but the context was canceled during the pool run, the
// function must still return ctx.Err() rather than build a result from the
// half-done work.
func (s *RefreshExternalCollectionTaskSuite) TestBalanceFragmentsToSegments_CtxCancelledDuringManifest() {
	paramtable.Init()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tmpDir := s.T().TempDir()
	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID:   s.collectionID,
		ExternalSource: "s3:///bucket/path",
		ExternalSpec:   `{"format":"parquet"}`,
		StorageConfig:  &indexpb.StorageConfig{StorageType: "local", BucketName: tmpDir, RootPath: tmpDir},
	}
	task := NewRefreshExternalCollectionTask(ctx, req)
	task.parsedSpec = &externalspec.ExternalSpec{Format: "parquet"}
	task.columns = []string{"col1"}
	task.preallocatedIDRange = &datapb.IDRange{Begin: 1, End: 100}
	task.nextAllocID = 1

	// Mock createManifestForSegment to cancel the ctx mid-run and return nil.
	// Workers do not propagate an error, so AwaitAll returns nil; the
	// post-AwaitAll ctx.Err() check is the only thing that surfaces the cancel.
	mockCM := mockey.Mock((*RefreshExternalCollectionTask).createManifestForSegment).
		To(func(_ *RefreshExternalCollectionTask, _ context.Context, _ int64, _ []packed.Fragment) (string, error) {
			cancel()
			return "/manifest", nil
		}).Build()
	defer mockCM.UnPatch()

	fragments := []packed.Fragment{
		{FragmentID: 0, FilePath: "f.parquet", RowCount: 100, StartRow: 0, EndRow: 100},
	}
	_, err := task.balanceFragmentsToSegments(ctx, fragments)
	s.Require().Error(err)
	s.ErrorIs(err, context.Canceled)
}

func (s *RefreshExternalCollectionTaskSuite) TestBuildFakeBinlogs() {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 1, Name: "pk"},
			{FieldID: 100, Name: "text"},
			{FieldID: 101, Name: "vec"},
		},
	}

	binlogs := buildFakeBinlogs(999, 1000, 512000, schema, "parquet")
	s.Len(binlogs, 1)
	fb := binlogs[0]
	s.Equal(int64(0), fb.GetFieldID(), "should use DefaultShortColumnGroupID=0")
	s.ElementsMatch([]int64{1, 100, 101}, fb.GetChildFields())
	s.Equal("parquet", fb.GetFormat())
	s.Len(fb.GetBinlogs(), 1)
	s.Equal(int64(999), fb.GetBinlogs()[0].GetLogID(), "logID should be segmentID")
	s.Equal(int64(1000), fb.GetBinlogs()[0].GetEntriesNum())
	s.Equal(int64(512000), fb.GetBinlogs()[0].GetMemorySize())
	s.Equal(int64(512000), fb.GetBinlogs()[0].GetLogSize())
}

func (s *RefreshExternalCollectionTaskSuite) TestBuildFakeBinlogs_NilSchema() {
	binlogs := buildFakeBinlogs(888, 500, 100000, nil, "vortex")
	s.Len(binlogs, 1)
	s.Empty(binlogs[0].GetChildFields())
	s.Equal("vortex", binlogs[0].GetFormat())
	s.Equal(int64(500), binlogs[0].GetBinlogs()[0].GetEntriesNum())
}

func (s *RefreshExternalCollectionTaskSuite) TestSumFieldSizes() {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 1, Name: "pk"},                                    // no ExternalField
			{FieldID: 100, Name: "text", ExternalField: "text_col"},     // external
			{FieldID: 101, Name: "vec", ExternalField: "embedding_col"}, // external
		},
	}

	fieldSizes := map[string]int64{
		"text_col":      64,
		"embedding_col": 512,
		"extra_col":     999, // not in schema, should be ignored
	}

	total := sumFieldSizes(fieldSizes, schema)
	s.Equal(int64(576), total, "should sum only external fields: 64+512")
}

func (s *RefreshExternalCollectionTaskSuite) TestSumFieldSizes_MilvusTableUsesSourceFieldIDs() {
	schema := &schemapb.CollectionSchema{
		ExternalSpec: `{"format":"milvus-table"}`,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 0, Name: common.VirtualPKFieldName},
			{FieldID: 100, Name: "target_pk", ExternalField: "pk"},
			{FieldID: 101, Name: "target_vec", ExternalField: "vec"},
		},
	}
	fieldSizes := map[string]int64{
		"100": 64,
		"101": 512,
	}

	total := sumFieldSizes(fieldSizes, schema)
	s.Equal(int64(576), total)
}

func (s *RefreshExternalCollectionTaskSuite) TestSumFieldSizes_MilvusTableSkipsFunctionOutputField() {
	schema := &schemapb.CollectionSchema{
		ExternalSpec: `{"format":"milvus-table"}`,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "text", ExternalField: "text"},
			{FieldID: 101, Name: "vec", ExternalField: "vec"},
			{FieldID: 102, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
		},
	}
	fieldSizes := map[string]int64{
		"100": 64,
		"101": 512,
		"102": 4096,
	}

	total := sumFieldSizes(fieldSizes, schema)
	s.Equal(int64(576), total)
}

func (s *RefreshExternalCollectionTaskSuite) TestSumFieldSizes_NilSchema() {
	fieldSizes := map[string]int64{"a": 10, "b": 20}
	total := sumFieldSizes(fieldSizes, nil)
	s.Equal(int64(30), total, "nil schema should sum all fields")
}

func (s *RefreshExternalCollectionTaskSuite) TestBalanceFragmentsToSegments_SamplingFails() {
	paramtable.Init()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID:           s.collectionID,
		TaskID:                 s.taskID,
		PreAllocatedSegmentIds: &datapb.IDRange{Begin: 100, End: 200},
		StorageConfig:          &indexpb.StorageConfig{StorageType: "local"},
		ExternalSource:         "s3://bucket/data/",
		ExternalSpec:           `{"format":"parquet"}`,
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "text", ExternalField: "text_col"},
			},
		},
	}

	task := NewRefreshExternalCollectionTask(ctx, req)
	task.preallocatedIDRange = req.GetPreAllocatedSegmentIds()
	task.nextAllocID = task.preallocatedIDRange.Begin
	task.parsedSpec = &externalspec.ExternalSpec{Format: "parquet"}

	m1 := mockey.Mock(packed.CreateSegmentManifestWithBasePathAndExtfs).
		To(func(ctx context.Context, basePath, format string, columns []string, fragments []packed.Fragment, storageConfig *indexpb.StorageConfig, extfs packed.ExternalSpecContext) (string, error) {
			return fmt.Sprintf("%s/manifest.json", basePath), nil
		}).Build()
	defer m1.UnPatch()

	// Sampling fails on the only manifest. The previous behavior silently
	// emitted MemorySize=0 segments, which collapsed QueryNode's resource
	// estimator and risked OOM on load. Current behavior is to fail the
	// task so the task-level retry path can try again with a clean state.
	m2 := mockey.Mock(packed.SampleExternalFieldSizes).
		Return(nil, fmt.Errorf("sampling error")).Build()
	defer m2.UnPatch()

	fragments := []packed.Fragment{
		{FragmentID: 1, RowCount: 500},
	}

	result, err := task.balanceFragmentsToSegments(context.Background(), fragments)
	s.Error(err, "sampling failure must surface as a task error, not a silent MemorySize=0")
	s.Contains(err.Error(), "sampling failed")
	s.Nil(result)
}

// Regression for #48637: when external_field mappings point to columns
// absent in the parquet file, SampleExternalFieldSizes returns a real
// error like "Column 'wrong_col_a' not found in schema". That error
// must propagate to the task error (and thus to the RefreshFailed
// reason surfaced to the client) rather than being swallowed as a
// generic "sampling failed for all N segment(s)" message.
func (s *RefreshExternalCollectionTaskSuite) TestBalanceFragmentsToSegments_SamplingError_RootCausePropagated() {
	paramtable.Init()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID:           s.collectionID,
		TaskID:                 s.taskID,
		PreAllocatedSegmentIds: &datapb.IDRange{Begin: 100, End: 200},
		StorageConfig:          &indexpb.StorageConfig{StorageType: "local"},
		ExternalSource:         "s3://bucket/data/",
		ExternalSpec:           `{"format":"parquet"}`,
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "text", ExternalField: "wrong_col_a"},
			},
		},
	}

	task := NewRefreshExternalCollectionTask(ctx, req)
	task.preallocatedIDRange = req.GetPreAllocatedSegmentIds()
	task.nextAllocID = task.preallocatedIDRange.Begin
	task.parsedSpec = &externalspec.ExternalSpec{Format: "parquet"}

	m1 := mockey.Mock(packed.CreateSegmentManifestWithBasePathAndExtfs).
		To(func(ctx context.Context, basePath, format string, columns []string, fragments []packed.Fragment, storageConfig *indexpb.StorageConfig, extfs packed.ExternalSpecContext) (string, error) {
			return fmt.Sprintf("%s/manifest.json", basePath), nil
		}).Build()
	defer m1.UnPatch()

	cppErr := fmt.Errorf("Invalid: Column 'wrong_col_a' not found in schema. [path=data.parquet]")
	m2 := mockey.Mock(packed.SampleExternalFieldSizes).Return(nil, cppErr).Build()
	defer m2.UnPatch()

	fragments := []packed.Fragment{{FragmentID: 1, RowCount: 500}}

	result, err := task.balanceFragmentsToSegments(context.Background(), fragments)
	s.Error(err)
	s.Nil(result)
	// Root cause string must reach the client-facing task error.
	s.Contains(err.Error(), "Column 'wrong_col_a' not found in schema")
	// And emit actionable hint guiding the user to the real fix.
	s.Contains(err.Error(), "external_field mappings")
}

func (s *RefreshExternalCollectionTaskSuite) TestBalanceFragmentsToSegments_SamplingTypeMismatchFails() {
	paramtable.Init()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID:           s.collectionID,
		TaskID:                 s.taskID,
		PreAllocatedSegmentIds: &datapb.IDRange{Begin: 100, End: 200},
		StorageConfig:          &indexpb.StorageConfig{StorageType: "local"},
		ExternalSource:         "s3://bucket/data/",
		ExternalSpec:           `{"format":"parquet"}`,
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:       100,
					Name:          "age",
					DataType:      schemapb.DataType_Int8,
					ExternalField: "age_col",
				},
			},
		},
	}

	task := NewRefreshExternalCollectionTask(ctx, req)
	task.preallocatedIDRange = req.GetPreAllocatedSegmentIds()
	task.nextAllocID = task.preallocatedIDRange.Begin
	task.parsedSpec = &externalspec.ExternalSpec{Format: "parquet"}

	m1 := mockey.Mock(packed.CreateSegmentManifestWithBasePathAndExtfs).
		To(func(ctx context.Context, basePath, format string, columns []string, fragments []packed.Fragment, storageConfig *indexpb.StorageConfig, extfs packed.ExternalSpecContext) (string, error) {
			return fmt.Sprintf("%s/manifest.json", basePath), nil
		}).Build()
	defer m1.UnPatch()

	typeMismatchErr := fmt.Errorf("field type mismatch, expected Arrow int8, actual Arrow int64")
	m2 := mockey.Mock(packed.SampleExternalFieldSizes).Return(nil, typeMismatchErr).Build()
	defer m2.UnPatch()

	fragments := []packed.Fragment{{FragmentID: 1, RowCount: 500}}

	result, err := task.balanceFragmentsToSegments(context.Background(), fragments)
	s.Error(err)
	s.Nil(result)
	s.Contains(err.Error(), "sampling failed")
	s.Contains(err.Error(), "field type mismatch")
	s.Contains(err.Error(), "expected Arrow int8")
	s.Contains(err.Error(), "actual Arrow int64")
}

// Regression: the samplePerSegment=true branch of Phase 3 was previously
// uncovered by tests. This exercise is three parts:
//  1. All per-segment samples succeed → each segment gets its own
//     distinct MemorySize (proving per-segment semantics actually take
//     effect rather than collapsing to the first value).
//  2. First segment sample fails, later segments succeed → the zero-fill
//     pass retroactively backfills the first slot with the first
//     successful average (fallbackAvg).
//  3. All per-segment samples fail → task fails (no silent MemorySize=0).
func (s *RefreshExternalCollectionTaskSuite) TestBalanceFragmentsToSegments_PerSegmentSampling_AllSucceed() {
	paramtable.Init()
	key := paramtable.Get().QueryNodeCfg.ExternalCollectionSamplePerSegment.Key
	paramtable.Get().Save(key, "true")
	defer paramtable.Get().Reset(key)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID:           s.collectionID,
		TaskID:                 s.taskID,
		PreAllocatedSegmentIds: &datapb.IDRange{Begin: 100, End: 200},
		StorageConfig:          &indexpb.StorageConfig{StorageType: "local"},
		ExternalSource:         "s3://bucket/data/",
		ExternalSpec:           `{"format":"parquet"}`,
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "text", ExternalField: "text_col"},
			},
		},
	}

	task := NewRefreshExternalCollectionTask(ctx, req)
	task.preallocatedIDRange = req.GetPreAllocatedSegmentIds()
	task.nextAllocID = task.preallocatedIDRange.Begin
	task.parsedSpec = &externalspec.ExternalSpec{Format: "parquet"}

	m1 := mockey.Mock(packed.CreateSegmentManifestWithBasePathAndExtfs).
		To(func(ctx context.Context, basePath, format string, columns []string, fragments []packed.Fragment, storageConfig *indexpb.StorageConfig, extfs packed.ExternalSpecContext) (string, error) {
			return fmt.Sprintf("%s/manifest.json", basePath), nil
		}).Build()
	defer m1.UnPatch()

	// Return distinct sizes per call so we can prove per-segment samples
	// actually land in distinct slots.
	var callCount int32
	perCallSizes := []int64{50, 100, 200}
	m2 := mockey.Mock(packed.SampleExternalFieldSizes).
		To(func(manifestPath string, rows int, collectionID int64, externalSource, externalSpec string, schema *schemapb.CollectionSchema, storageConfig *indexpb.StorageConfig) (map[string]int64, error) {
			idx := int(atomic.AddInt32(&callCount, 1)) - 1
			if idx >= len(perCallSizes) {
				idx = len(perCallSizes) - 1
			}
			return map[string]int64{"text_col": perCallSizes[idx]}, nil
		}).Build()
	defer m2.UnPatch()

	// 3 fragments each above targetRowsPerSegment → 3 segments.
	targetRows := paramtable.Get().DataNodeCfg.ExternalCollectionTargetRowsPerSegment.GetAsInt64()
	rowsPerFragment := targetRows * 2 // force one segment per fragment
	fragments := []packed.Fragment{
		{FragmentID: 1, RowCount: rowsPerFragment},
		{FragmentID: 2, RowCount: rowsPerFragment},
		{FragmentID: 3, RowCount: rowsPerFragment},
	}

	result, err := task.balanceFragmentsToSegments(context.Background(), fragments)
	s.NoError(err)
	s.Len(result, 3, "expected 3 segments for 3 oversized fragments")

	// Each segment's MemorySize reflects its own sample (size * rowCount).
	// SampleExternalFieldSizes is called per-segment in manifest order.
	for i, seg := range result {
		expectedSize := perCallSizes[i] * rowsPerFragment
		actual := seg.GetBinlogs()[0].GetBinlogs()[0].GetMemorySize()
		s.Equal(expectedSize, actual, "segment %d MemorySize should match its own sample", i)
	}
}

func (s *RefreshExternalCollectionTaskSuite) TestBalanceFragmentsToSegments_PerSegmentSampling_FirstFailBackfills() {
	paramtable.Init()
	key := paramtable.Get().QueryNodeCfg.ExternalCollectionSamplePerSegment.Key
	paramtable.Get().Save(key, "true")
	defer paramtable.Get().Reset(key)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID:           s.collectionID,
		TaskID:                 s.taskID,
		PreAllocatedSegmentIds: &datapb.IDRange{Begin: 100, End: 200},
		StorageConfig:          &indexpb.StorageConfig{StorageType: "local"},
		ExternalSource:         "s3://bucket/data/",
		ExternalSpec:           `{"format":"parquet"}`,
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "text", ExternalField: "text_col"},
			},
		},
	}

	task := NewRefreshExternalCollectionTask(ctx, req)
	task.preallocatedIDRange = req.GetPreAllocatedSegmentIds()
	task.nextAllocID = task.preallocatedIDRange.Begin
	task.parsedSpec = &externalspec.ExternalSpec{Format: "parquet"}

	m1 := mockey.Mock(packed.CreateSegmentManifestWithBasePathAndExtfs).
		To(func(ctx context.Context, basePath, format string, columns []string, fragments []packed.Fragment, storageConfig *indexpb.StorageConfig, extfs packed.ExternalSpecContext) (string, error) {
			return fmt.Sprintf("%s/manifest.json", basePath), nil
		}).Build()
	defer m1.UnPatch()

	// First call fails; second and third succeed. Verify the first slot
	// is retroactively backfilled with the first successful average (150),
	// not the third (250), since fallbackAvg latches on the first success.
	var callCount int32
	m2 := mockey.Mock(packed.SampleExternalFieldSizes).
		To(func(manifestPath string, rows int, collectionID int64, externalSource, externalSpec string, schema *schemapb.CollectionSchema, storageConfig *indexpb.StorageConfig) (map[string]int64, error) {
			idx := int(atomic.AddInt32(&callCount, 1)) - 1
			switch idx {
			case 0:
				return nil, fmt.Errorf("transient sample failure")
			case 1:
				return map[string]int64{"text_col": 150}, nil
			default:
				return map[string]int64{"text_col": 250}, nil
			}
		}).Build()
	defer m2.UnPatch()

	targetRows := paramtable.Get().DataNodeCfg.ExternalCollectionTargetRowsPerSegment.GetAsInt64()
	rowsPerFragment := targetRows * 2
	fragments := []packed.Fragment{
		{FragmentID: 1, RowCount: rowsPerFragment},
		{FragmentID: 2, RowCount: rowsPerFragment},
		{FragmentID: 3, RowCount: rowsPerFragment},
	}

	result, err := task.balanceFragmentsToSegments(context.Background(), fragments)
	s.NoError(err)
	s.Len(result, 3)

	// Expected: [150*rows, 150*rows, 250*rows]
	// - segment 0: backfilled from fallbackAvg=150
	// - segment 1: native sample 150
	// - segment 2: native sample 250
	s.Equal(int64(150)*rowsPerFragment, result[0].GetBinlogs()[0].GetBinlogs()[0].GetMemorySize(), "segment 0 should be backfilled from fallbackAvg")
	s.Equal(int64(150)*rowsPerFragment, result[1].GetBinlogs()[0].GetBinlogs()[0].GetMemorySize())
	s.Equal(int64(250)*rowsPerFragment, result[2].GetBinlogs()[0].GetBinlogs()[0].GetMemorySize())
}

func (s *RefreshExternalCollectionTaskSuite) TestBalanceFragmentsToSegments_PerSegmentSampling_AllFail() {
	paramtable.Init()
	key := paramtable.Get().QueryNodeCfg.ExternalCollectionSamplePerSegment.Key
	paramtable.Get().Save(key, "true")
	defer paramtable.Get().Reset(key)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID:           s.collectionID,
		TaskID:                 s.taskID,
		PreAllocatedSegmentIds: &datapb.IDRange{Begin: 100, End: 200},
		StorageConfig:          &indexpb.StorageConfig{StorageType: "local"},
		ExternalSource:         "s3://bucket/data/",
		ExternalSpec:           `{"format":"parquet"}`,
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "text", ExternalField: "text_col"},
			},
		},
	}

	task := NewRefreshExternalCollectionTask(ctx, req)
	task.preallocatedIDRange = req.GetPreAllocatedSegmentIds()
	task.nextAllocID = task.preallocatedIDRange.Begin
	task.parsedSpec = &externalspec.ExternalSpec{Format: "parquet"}

	m1 := mockey.Mock(packed.CreateSegmentManifestWithBasePathAndExtfs).
		To(func(ctx context.Context, basePath, format string, columns []string, fragments []packed.Fragment, storageConfig *indexpb.StorageConfig, extfs packed.ExternalSpecContext) (string, error) {
			return fmt.Sprintf("%s/manifest.json", basePath), nil
		}).Build()
	defer m1.UnPatch()

	m2 := mockey.Mock(packed.SampleExternalFieldSizes).
		Return(nil, fmt.Errorf("all samples failed")).Build()
	defer m2.UnPatch()

	targetRows := paramtable.Get().DataNodeCfg.ExternalCollectionTargetRowsPerSegment.GetAsInt64()
	rowsPerFragment := targetRows * 2
	fragments := []packed.Fragment{
		{FragmentID: 1, RowCount: rowsPerFragment},
		{FragmentID: 2, RowCount: rowsPerFragment},
	}

	result, err := task.balanceFragmentsToSegments(context.Background(), fragments)
	s.Error(err, "all per-segment samples failing must surface as a task error")
	s.Contains(err.Error(), "sampling failed")
	s.Nil(result)
}

// Regression: if the schema has no mapped external data fields, sumFieldSizes
// returns 0 even when SampleExternalFieldSizes itself "succeeds". The old
// code treated this as a successful zero-sized sample and wrote MemorySize=0
// fake binlogs into every segment, which feeds QueryNode a degenerate
// EstimatedBytesPerRow=0 and risks OOM on load. Current behavior: fail the
// task.
func (s *RefreshExternalCollectionTaskSuite) TestBalanceFragmentsToSegments_ZeroSumTreatedAsFailure() {
	paramtable.Init()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID:           s.collectionID,
		TaskID:                 s.taskID,
		PreAllocatedSegmentIds: &datapb.IDRange{Begin: 100, End: 200},
		StorageConfig:          &indexpb.StorageConfig{StorageType: "local"},
		ExternalSource:         "s3://bucket/data/",
		ExternalSpec:           `{"format":"parquet"}`,
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				// Schema with NO ExternalField set — sumFieldSizes returns 0.
				{FieldID: 100, Name: "native_only"},
			},
		},
	}

	task := NewRefreshExternalCollectionTask(ctx, req)
	task.preallocatedIDRange = req.GetPreAllocatedSegmentIds()
	task.nextAllocID = task.preallocatedIDRange.Begin
	task.parsedSpec = &externalspec.ExternalSpec{Format: "parquet"}

	m1 := mockey.Mock(packed.CreateSegmentManifestWithBasePathAndExtfs).
		To(func(ctx context.Context, basePath, format string, columns []string, fragments []packed.Fragment, storageConfig *indexpb.StorageConfig, extfs packed.ExternalSpecContext) (string, error) {
			return fmt.Sprintf("%s/manifest.json", basePath), nil
		}).Build()
	defer m1.UnPatch()

	// Sampling "succeeds" but returns sizes for a column the schema
	// doesn't reference — sumFieldSizes totals to 0.
	m2 := mockey.Mock(packed.SampleExternalFieldSizes).
		Return(map[string]int64{"orphan_col": 128}, nil).Build()
	defer m2.UnPatch()

	fragments := []packed.Fragment{
		{FragmentID: 1, RowCount: 500},
	}

	result, err := task.balanceFragmentsToSegments(context.Background(), fragments)
	s.Error(err, "zero-sum sample must surface as a task error")
	s.Contains(err.Error(), "sampling failed")
	s.Nil(result)
}

func (s *RefreshExternalCollectionTaskSuite) TestSumFieldSizes_MilvusTableRealPKIncludesSourceTimestamp() {
	schema := &schemapb.CollectionSchema{
		ExternalSpec: `{"format":"milvus-table"}`,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector},
		},
	}

	total := sumFieldSizes(map[string]int64{
		"100": 16,
		"101": 64,
		"1":   8,
	}, schema)

	s.Equal(int64(88), total)
}

func (s *RefreshExternalCollectionTaskSuite) TestBalanceFragmentsToSegments_InsufficientIDs() {
	paramtable.Init()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID:           s.collectionID,
		TaskID:                 s.taskID,
		PreAllocatedSegmentIds: &datapb.IDRange{Begin: 100, End: 101}, // only 1 ID, need 2
		StorageConfig:          &indexpb.StorageConfig{StorageType: "local"},
		ExternalSource:         "s3://bucket/data/",
		ExternalSpec:           `{"format":"parquet"}`,
		Schema:                 &schemapb.CollectionSchema{},
	}

	task := NewRefreshExternalCollectionTask(ctx, req)
	task.preallocatedIDRange = req.GetPreAllocatedSegmentIds()
	task.nextAllocID = task.preallocatedIDRange.Begin
	task.parsedSpec = &externalspec.ExternalSpec{Format: "parquet"}

	fragments := []packed.Fragment{
		{FragmentID: 1, RowCount: 500},
	}

	_, err := task.balanceFragmentsToSegments(context.Background(), fragments)
	s.Error(err)
	s.Contains(err.Error(), "insufficient pre-allocated IDs")
}

func (s *RefreshExternalCollectionTaskSuite) TestBalanceFragmentsToSegments_ManifestCreateFails() {
	paramtable.Init()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID:           s.collectionID,
		TaskID:                 s.taskID,
		PreAllocatedSegmentIds: &datapb.IDRange{Begin: 100, End: 200},
		StorageConfig:          &indexpb.StorageConfig{StorageType: "local"},
		ExternalSource:         "s3://bucket/data/",
		ExternalSpec:           `{"format":"parquet"}`,
		Schema:                 &schemapb.CollectionSchema{},
	}

	task := NewRefreshExternalCollectionTask(ctx, req)
	task.preallocatedIDRange = req.GetPreAllocatedSegmentIds()
	task.nextAllocID = task.preallocatedIDRange.Begin
	task.parsedSpec = &externalspec.ExternalSpec{Format: "parquet"}

	m1 := mockey.Mock(packed.CreateSegmentManifestWithBasePathAndExtfs).
		Return("", fmt.Errorf("storage error")).Build()
	defer m1.UnPatch()

	fragments := []packed.Fragment{
		{FragmentID: 1, RowCount: 500},
	}

	_, err := task.balanceFragmentsToSegments(context.Background(), fragments)
	s.Error(err)
	s.Contains(err.Error(), "failed to create manifest")
}

func (s *RefreshExternalCollectionTaskSuite) TestBalanceFragmentsToSegments_ContextCancelDuringManifest() {
	paramtable.Init()

	// Use a real cancelable context that we pass to balanceFragmentsToSegments
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID:           s.collectionID,
		TaskID:                 s.taskID,
		PreAllocatedSegmentIds: &datapb.IDRange{Begin: 100, End: 200},
		StorageConfig:          &indexpb.StorageConfig{StorageType: "local"},
		ExternalSource:         "s3://bucket/data/",
		ExternalSpec:           `{"format":"parquet"}`,
		Schema:                 &schemapb.CollectionSchema{},
	}

	task := NewRefreshExternalCollectionTask(ctx, req)
	task.preallocatedIDRange = req.GetPreAllocatedSegmentIds()
	task.nextAllocID = task.preallocatedIDRange.Begin
	task.parsedSpec = &externalspec.ExternalSpec{Format: "parquet"}

	// Mock manifest creation: first call cancels ctx and returns error
	m1 := mockey.Mock(packed.CreateSegmentManifestWithBasePathAndExtfs).
		To(func(_ context.Context, basePath, format string, columns []string, fragments []packed.Fragment, storageConfig *indexpb.StorageConfig, extfs packed.ExternalSpecContext) (string, error) {
			cancel() // cancel the ctx we pass to balanceFragmentsToSegments
			return "", fmt.Errorf("canceled")
		}).Build()
	defer m1.UnPatch()

	// Use multiple fragments so there are multiple works — ctx cancel is checked
	// in the task dispatch loop (L643) and after wg.Wait (L657)
	fragments := []packed.Fragment{
		{FragmentID: 1, RowCount: 300},
		{FragmentID: 2, RowCount: 400},
		{FragmentID: 3, RowCount: 500},
	}

	// Pass the cancelable ctx so L643 `ctx.Err()` and L657 `ctx.Err()` can trigger
	_, err := task.balanceFragmentsToSegments(ctx, fragments)
	s.Error(err)
}

func (s *RefreshExternalCollectionTaskSuite) TestBalanceFragmentsToSegments_EnsureContextInLoop() {
	paramtable.Init()

	// Pre-cancel the context to trigger ensureContext inside for loops (L517, L558)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID:           s.collectionID,
		TaskID:                 s.taskID,
		PreAllocatedSegmentIds: &datapb.IDRange{Begin: 100, End: 200},
		StorageConfig:          &indexpb.StorageConfig{StorageType: "local"},
		ExternalSource:         "s3://bucket/data/",
		ExternalSpec:           `{"format":"parquet"}`,
		Schema:                 &schemapb.CollectionSchema{},
	}

	task := NewRefreshExternalCollectionTask(ctx, req)
	task.preallocatedIDRange = req.GetPreAllocatedSegmentIds()
	task.nextAllocID = task.preallocatedIDRange.Begin
	task.parsedSpec = &externalspec.ExternalSpec{Format: "parquet"}

	fragments := []packed.Fragment{
		{FragmentID: 1, RowCount: 100},
		{FragmentID: 2, RowCount: 200},
	}

	// Cancel immediately — the first ensureContext (L508) passes because it checks
	// before cancel. We need to cancel between L508 and L517.
	// Use a goroutine trick: mock ensureContext behavior isn't possible directly,
	// so just cancel and pass the canceled ctx.
	cancel()
	_, err := task.balanceFragmentsToSegments(ctx, fragments)
	s.ErrorIs(err, context.Canceled)
}

func (s *RefreshExternalCollectionTaskSuite) TestBalanceFragmentsToSegments_EmptyBinSkipped() {
	paramtable.Init()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID:           s.collectionID,
		TaskID:                 s.taskID,
		PreAllocatedSegmentIds: &datapb.IDRange{Begin: 100, End: 200},
		StorageConfig:          &indexpb.StorageConfig{StorageType: "local"},
		ExternalSource:         "s3://bucket/data/",
		ExternalSpec:           `{"format":"parquet"}`,
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				// Non-zero sample is required now that a zero sum is
				// treated as a sampling failure. The intent of this test
				// is the bin-skipping behavior, not the sampling path.
				{FieldID: 100, Name: "text", ExternalField: "text_col"},
			},
		},
	}

	task := NewRefreshExternalCollectionTask(ctx, req)
	task.preallocatedIDRange = req.GetPreAllocatedSegmentIds()
	task.nextAllocID = task.preallocatedIDRange.Begin
	task.parsedSpec = &externalspec.ExternalSpec{Format: "parquet"}

	m1 := mockey.Mock(packed.CreateSegmentManifestWithBasePathAndExtfs).
		To(func(ctx context.Context, basePath, format string, columns []string, fragments []packed.Fragment, storageConfig *indexpb.StorageConfig, extfs packed.ExternalSpecContext) (string, error) {
			return fmt.Sprintf("%s/manifest.json", basePath), nil
		}).Build()
	defer m1.UnPatch()

	m2 := mockey.Mock(packed.SampleExternalFieldSizes).
		Return(map[string]int64{"text_col": 64}, nil).Build()
	defer m2.UnPatch()

	// One tiny fragment but targetRowsPerSegment is large — only 1 bin used, others empty
	fragments := []packed.Fragment{
		{FragmentID: 1, RowCount: 100},
	}

	result, err := task.balanceFragmentsToSegments(context.Background(), fragments)
	s.NoError(err)
	s.Len(result, 1)
	s.Equal(int64(100), result[0].GetNumOfRows())
}

func (s *RefreshExternalCollectionTaskSuite) TestBalanceFragmentsToSegments_ContextCanceledInLoops() {
	paramtable.Init()

	testCases := []struct {
		name      string
		cancelAt  int
		fragments []packed.Fragment
	}{
		{
			name:      "total row loop",
			cancelAt:  2,
			fragments: []packed.Fragment{{FragmentID: 1, RowCount: 100}},
		},
		{
			name:      "bin packing loop",
			cancelAt:  4,
			fragments: []packed.Fragment{{FragmentID: 1, RowCount: 100}, {FragmentID: 2, RowCount: 200}},
		},
	}

	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			req := &datapb.RefreshExternalCollectionTaskRequest{
				CollectionID:           s.collectionID,
				TaskID:                 s.taskID,
				PreAllocatedSegmentIds: &datapb.IDRange{Begin: 100, End: 200},
				StorageConfig:          &indexpb.StorageConfig{StorageType: "local"},
				ExternalSpec:           `{"format":"parquet"}`,
				Schema:                 &schemapb.CollectionSchema{},
			}
			task := NewRefreshExternalCollectionTask(ctx, req)
			task.preallocatedIDRange = req.GetPreAllocatedSegmentIds()
			task.nextAllocID = task.preallocatedIDRange.Begin
			task.parsedSpec = &externalspec.ExternalSpec{Format: "parquet"}

			var calls int
			mockEnsure := mockey.Mock(ensureContext).To(func(ctx context.Context) error {
				calls++
				if calls == tc.cancelAt {
					return context.Canceled
				}
				return nil
			}).Build()
			defer mockEnsure.UnPatch()

			result, err := task.balanceFragmentsToSegments(ctx, tc.fragments)
			s.ErrorIs(err, context.Canceled)
			s.Nil(result)
		})
	}
}

func (s *RefreshExternalCollectionTaskSuite) TestBalanceFragmentsToSegments_ContextCanceledInWorker() {
	paramtable.Init()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID:           s.collectionID,
		TaskID:                 s.taskID,
		PreAllocatedSegmentIds: &datapb.IDRange{Begin: 100, End: 200},
		StorageConfig:          &indexpb.StorageConfig{StorageType: "local"},
		ExternalSpec:           `{"format":"parquet"}`,
		Schema:                 &schemapb.CollectionSchema{},
	}
	task := NewRefreshExternalCollectionTask(ctx, req)
	task.preallocatedIDRange = req.GetPreAllocatedSegmentIds()
	task.nextAllocID = task.preallocatedIDRange.Begin
	task.parsedSpec = &externalspec.ExternalSpec{Format: "parquet"}

	mockEnsure := mockey.Mock(ensureContext).Return(nil).Build()
	defer mockEnsure.UnPatch()

	result, err := task.balanceFragmentsToSegments(ctx, []packed.Fragment{{FragmentID: 1, RowCount: 100}})
	s.ErrorIs(err, context.Canceled)
	s.Nil(result)
}

func (s *RefreshExternalCollectionTaskSuite) TestBalanceFragmentsToSegments_FunctionOutputEstimateError() {
	paramtable.Init()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID:           s.collectionID,
		TaskID:                 s.taskID,
		PreAllocatedSegmentIds: &datapb.IDRange{Begin: 100, End: 200},
		StorageConfig:          &indexpb.StorageConfig{StorageType: "local"},
		ExternalSource:         "s3://bucket/data/",
		ExternalSpec:           `{"format":"parquet"}`,
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "text", DataType: schemapb.DataType_VarChar, ExternalField: "text_col"},
				{FieldID: 101, Name: "bad_output", DataType: schemapb.DataType_VarChar, IsFunctionOutput: true},
			},
		},
	}
	task := NewRefreshExternalCollectionTask(ctx, req)
	task.preallocatedIDRange = req.GetPreAllocatedSegmentIds()
	task.nextAllocID = task.preallocatedIDRange.Begin
	task.parsedSpec = &externalspec.ExternalSpec{Format: "parquet"}

	mockManifest := mockey.Mock(packed.CreateSegmentManifestWithBasePathAndExtfs).
		Return("manifest.json", nil).Build()
	defer mockManifest.UnPatch()
	mockSample := mockey.Mock(packed.SampleExternalFieldSizes).
		Return(map[string]int64{"text_col": 64}, nil).Build()
	defer mockSample.UnPatch()

	result, err := task.balanceFragmentsToSegments(ctx, []packed.Fragment{{FragmentID: 1, RowCount: 100}})
	s.Error(err)
	s.Contains(err.Error(), "estimate function output field bad_output")
	s.Nil(result)
}

// TestBalanceFragmentsToSegments_PassesStorageConfigAndSpecExtfs verifies that
// balanceFragmentsToSegments correctly passes storageConfig and specExtfs
// (built from parsedSpec) to SampleExternalFieldSizes. This is the core change
// that eliminates the dependency on C++ LoonFFIPropertiesSingleton.
func (s *RefreshExternalCollectionTaskSuite) TestEstimateFunctionOutputBytesPerRow_NoOutputs() {
	bytes, err := estimateFunctionOutputBytesPerRow(&schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "text", DataType: schemapb.DataType_VarChar},
		},
	})

	s.NoError(err)
	s.Equal(int64(0), bytes)
}

func (s *RefreshExternalCollectionTaskSuite) TestEstimateFunctionOutputBytesPerRow_VectorOutputs() {
	bytes, err := estimateFunctionOutputBytesPerRow(&schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:          101,
				Name:             "float_vec",
				DataType:         schemapb.DataType_FloatVector,
				IsFunctionOutput: true,
				TypeParams:       []*commonpb.KeyValuePair{{Key: "dim", Value: "4"}},
			},
			{
				FieldID:          102,
				Name:             "int8_vec",
				DataType:         schemapb.DataType_Int8Vector,
				IsFunctionOutput: true,
				TypeParams:       []*commonpb.KeyValuePair{{Key: "dim", Value: "8"}},
			},
			{
				FieldID:          103,
				Name:             "binary_vec",
				DataType:         schemapb.DataType_BinaryVector,
				IsFunctionOutput: true,
				TypeParams:       []*commonpb.KeyValuePair{{Key: "dim", Value: "64"}},
			},
		},
	})

	s.NoError(err)
	s.Equal(int64(16+8+8), bytes)
}

func (s *RefreshExternalCollectionTaskSuite) TestEstimateFunctionOutputBytesPerRow_ScalarOutputs() {
	bytes, err := estimateFunctionOutputBytesPerRow(&schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 101, Name: "score", DataType: schemapb.DataType_Int64, IsFunctionOutput: true},
			{
				FieldID:          102,
				Name:             "label",
				DataType:         schemapb.DataType_VarChar,
				IsFunctionOutput: true,
				TypeParams:       []*commonpb.KeyValuePair{{Key: "max_length", Value: "20"}},
			},
		},
	})

	s.NoError(err)
	s.Equal(int64(8+20), bytes)
}

func (s *RefreshExternalCollectionTaskSuite) TestEstimateFunctionOutputBytesPerRow_InvalidOutputField() {
	_, err := estimateFunctionOutputBytesPerRow(&schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 101, Name: "bad_text", DataType: schemapb.DataType_VarChar, IsFunctionOutput: true},
		},
	})

	s.Error(err)
	s.Contains(err.Error(), "estimate function output field bad_text")
}

func (s *RefreshExternalCollectionTaskSuite) TestBalanceFragmentsToSegments_AddsFunctionOutputMemorySize() {
	paramtable.Init()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID:           s.collectionID,
		TaskID:                 s.taskID,
		PreAllocatedSegmentIds: &datapb.IDRange{Begin: 100, End: 200},
		StorageConfig:          &indexpb.StorageConfig{StorageType: "local"},
		ExternalSource:         "s3://bucket/data/",
		ExternalSpec:           `{"format":"parquet"}`,
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "text", DataType: schemapb.DataType_VarChar, ExternalField: "text_col"},
				{
					FieldID:          101,
					Name:             "embedding",
					DataType:         schemapb.DataType_FloatVector,
					IsFunctionOutput: true,
					TypeParams:       []*commonpb.KeyValuePair{{Key: "dim", Value: "4"}},
				},
				{
					FieldID:          102,
					Name:             "scalar_output",
					DataType:         schemapb.DataType_VarChar,
					IsFunctionOutput: true,
					TypeParams:       []*commonpb.KeyValuePair{{Key: "max_length", Value: "20"}},
				},
			},
		},
	}

	task := NewRefreshExternalCollectionTask(ctx, req)
	task.preallocatedIDRange = req.GetPreAllocatedSegmentIds()
	task.nextAllocID = task.preallocatedIDRange.Begin
	task.parsedSpec = &externalspec.ExternalSpec{Format: "parquet"}

	m1 := mockey.Mock(packed.CreateSegmentManifestWithBasePathAndExtfs).
		To(func(ctx context.Context, basePath, format string, columns []string, fragments []packed.Fragment, storageConfig *indexpb.StorageConfig, extfs packed.ExternalSpecContext) (string, error) {
			return fmt.Sprintf("%s/manifest.json", basePath), nil
		}).Build()
	defer m1.UnPatch()

	m2 := mockey.Mock(packed.SampleExternalFieldSizes).
		Return(map[string]int64{"text_col": 64}, nil).Build()
	defer m2.UnPatch()

	fragments := []packed.Fragment{{FragmentID: 1, RowCount: 10}}

	result, err := task.balanceFragmentsToSegments(context.Background(), fragments)
	s.NoError(err)
	s.Len(result, 1)
	s.Len(result[0].GetBinlogs(), 1)
	s.Len(result[0].GetBinlogs()[0].GetBinlogs(), 1)
	s.Equal(int64((64+16+20)*10), result[0].GetBinlogs()[0].GetBinlogs()[0].GetMemorySize())
}

func (s *RefreshExternalCollectionTaskSuite) TestBalanceFragmentsToSegments_PassesStorageConfigAndSpecExtfs() {
	paramtable.Init()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	expectedStorageConfig := &indexpb.StorageConfig{
		StorageType:     "minio",
		Address:         "localhost:9000",
		BucketName:      "test-bucket",
		AccessKeyID:     "minioadmin",
		SecretAccessKey: "minioadmin",
	}

	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID:           s.collectionID,
		TaskID:                 s.taskID,
		PreAllocatedSegmentIds: &datapb.IDRange{Begin: 100, End: 200},
		StorageConfig:          expectedStorageConfig,
		ExternalSource:         "s3://s3.us-west-2.amazonaws.com/ext-bucket/data/",
		ExternalSpec:           `{"format":"parquet","extfs":{"region":"us-west-2","use_ssl":"true","cloud_provider":"aws"}}`,
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "vec", ExternalField: "vec_col"},
			},
		},
	}

	task := NewRefreshExternalCollectionTask(ctx, req)
	task.preallocatedIDRange = req.GetPreAllocatedSegmentIds()
	task.nextAllocID = task.preallocatedIDRange.Begin
	// Parse the spec so the balance path has an in-memory struct to work with.
	parsed, err := externalspec.ParseExternalSpec(req.GetExternalSpec())
	s.Require().NoError(err)
	task.parsedSpec = parsed

	m1 := mockey.Mock(packed.CreateSegmentManifestWithBasePathAndExtfs).
		To(func(ctx context.Context, basePath, format string, columns []string, fragments []packed.Fragment, sc *indexpb.StorageConfig, extfs packed.ExternalSpecContext) (string, error) {
			return fmt.Sprintf("%s/manifest.json", basePath), nil
		}).Build()
	defer m1.UnPatch()

	var capturedStorageConfig *indexpb.StorageConfig
	var capturedExternalSpec string
	var capturedSchema *schemapb.CollectionSchema
	m2 := mockey.Mock(packed.SampleExternalFieldSizes).
		To(func(manifestPath string, rows int, collectionID int64, externalSource, externalSpec string, schema *schemapb.CollectionSchema, storageConfig *indexpb.StorageConfig) (map[string]int64, error) {
			capturedSchema = schema
			capturedStorageConfig = storageConfig
			capturedExternalSpec = externalSpec
			return map[string]int64{"vec_col": 3072}, nil
		}).Build()
	defer m2.UnPatch()

	fragments := []packed.Fragment{
		{FragmentID: 1, RowCount: 500},
	}

	result, err := task.balanceFragmentsToSegments(context.Background(), fragments)
	s.NoError(err)
	s.Len(result, 1)

	// Verify storageConfig was passed through (not nil)
	s.NotNil(capturedStorageConfig, "storageConfig must be passed to SampleExternalFieldSizes")
	s.Equal(expectedStorageConfig.GetAddress(), capturedStorageConfig.GetAddress())
	s.Equal(expectedStorageConfig.GetBucketName(), capturedStorageConfig.GetBucketName())
	s.Equal(expectedStorageConfig.GetStorageType(), capturedStorageConfig.GetStorageType())
	s.Equal(req.GetSchema(), capturedSchema, "schema must be passed to SampleExternalFieldSizes")

	// Verify raw externalSpec JSON is forwarded so C++ InjectExternalSpecProperties
	// can derive extfs.* overrides.
	s.Equal(req.GetExternalSpec(), capturedExternalSpec)
}

// TestBalanceFragmentsToSegments_NilSpecExtfsWhenNoExtfsInSpec verifies that
// when ExternalSpec has no extfs overrides (same-bucket scenario), specExtfs
// is empty but storageConfig is still passed.
func (s *RefreshExternalCollectionTaskSuite) TestBalanceFragmentsToSegments_NilSpecExtfsWhenNoExtfsInSpec() {
	paramtable.Init()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID:           s.collectionID,
		TaskID:                 s.taskID,
		PreAllocatedSegmentIds: &datapb.IDRange{Begin: 100, End: 200},
		StorageConfig:          &indexpb.StorageConfig{StorageType: "local"},
		ExternalSource:         "/local/data/",
		ExternalSpec:           `{"format":"parquet"}`,
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "text", ExternalField: "text_col"},
			},
		},
	}

	task := NewRefreshExternalCollectionTask(ctx, req)
	task.preallocatedIDRange = req.GetPreAllocatedSegmentIds()
	task.nextAllocID = task.preallocatedIDRange.Begin
	task.parsedSpec = &externalspec.ExternalSpec{Format: "parquet"}

	m1 := mockey.Mock(packed.CreateSegmentManifestWithBasePathAndExtfs).
		To(func(ctx context.Context, basePath, format string, columns []string, fragments []packed.Fragment, sc *indexpb.StorageConfig, extfs packed.ExternalSpecContext) (string, error) {
			return fmt.Sprintf("%s/manifest.json", basePath), nil
		}).Build()
	defer m1.UnPatch()

	var capturedStorageConfig *indexpb.StorageConfig
	var capturedExternalSpec string
	m2 := mockey.Mock(packed.SampleExternalFieldSizes).
		To(func(manifestPath string, rows int, collectionID int64, externalSource, externalSpec string, schema *schemapb.CollectionSchema, storageConfig *indexpb.StorageConfig) (map[string]int64, error) {
			capturedStorageConfig = storageConfig
			capturedExternalSpec = externalSpec
			return map[string]int64{"text_col": 64}, nil
		}).Build()
	defer m2.UnPatch()

	fragments := []packed.Fragment{
		{FragmentID: 1, RowCount: 100},
	}

	result, err := task.balanceFragmentsToSegments(context.Background(), fragments)
	s.NoError(err)
	s.Len(result, 1)

	// storageConfig is always passed
	s.NotNil(capturedStorageConfig)
	s.Equal("local", capturedStorageConfig.GetStorageType())

	// Same-bucket: externalSpec has no extfs → C++ inject is a no-op.
	s.Equal(req.GetExternalSpec(), capturedExternalSpec)
}

func TestRefreshExternalCollectionTaskSuite(t *testing.T) {
	suite.Run(t, new(RefreshExternalCollectionTaskSuite))
}
