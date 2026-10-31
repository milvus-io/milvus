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

package compactor

import (
	"context"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/bytedance/mockey"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

type missingFieldRecord struct {
	storage.Record
	missing int64
}

func (r *missingFieldRecord) Column(id int64) arrow.Array {
	col, _ := r.TryColumn(id)
	return col
}

func (r *missingFieldRecord) TryColumn(id int64) (arrow.Array, bool) {
	if id == r.missing {
		return nil, false
	}
	return storage.TryRecordColumn(r.Record, id)
}

type missingFieldReader struct {
	storage.RecordReader
	missing int64
}

func (r *missingFieldReader) Next() (storage.Record, error) {
	rec, err := r.RecordReader.Next()
	if err != nil {
		return nil, err
	}
	return &missingFieldRecord{Record: rec, missing: r.missing}, nil
}

// Exercise real Sort, Compact and executor code, corrupting only the decoded
// input. A mock Compact error would miss Compact's former failed,nil branch.
func (s *SortCompactionTaskSuite) TestRecordIntegrityFailureSurvivesExecutor() {
	s.prepareSortCompactionTask()
	// Existing Close cleanup may upload stats even after validation fails.
	// This test verifies failure propagation, not the writer cleanup policy.
	for _, call := range s.mockBinlogIO.ExpectedCalls {
		if call.Method == "Upload" {
			call.Maybe()
		}
	}
	var original func(context.Context, *datapb.CompactionSegmentBinlogs, *schemapb.CollectionSchema, *indexpb.StorageConfig, []packed.TextColumnConfig, ...storage.RwOption) (storage.RecordReader, map[int64]struct{}, error)
	patch := mockey.Mock(newTextDecodedCompactionSegmentRecordReader).To(func(ctx context.Context, segment *datapb.CompactionSegmentBinlogs,
		schema *schemapb.CollectionSchema, config *indexpb.StorageConfig, text []packed.TextColumnConfig, opts ...storage.RwOption,
	) (storage.RecordReader, map[int64]struct{}, error) {
		reader, fields, err := original(ctx, segment, schema, config, text, opts...)
		if err != nil {
			return nil, nil, err
		}
		return &missingFieldReader{RecordReader: reader, missing: s.task.sortByFieldIDs[0]}, fields, nil
	}).Origin(&original).Build()
	defer patch.UnPatch()

	ex := NewExecutor()
	ok, err := ex.Enqueue(s.task)
	s.Require().NoError(err)
	s.Require().True(ok)
	s.NotPanics(func() { ex.executeTask(s.task) })
	result := ex.tasks[s.task.GetPlanID()].result
	s.Require().NotNil(result)
	s.Equal(datapb.CompactionTaskState_failed, result.GetState())
	s.Require().NotNil(result.GetFailStatus())
	s.EqualValues(1009, result.GetFailStatus().GetCode())
	s.False(result.GetFailStatus().GetRetriable())
	s.Empty(result.GetSegments(), "failed output cannot be adopted by DataCoord")
	s.Zero(ex.Slots())

	patch.UnPatch()
	s.setupTest()
	s.task.plan.PlanID++
	s.prepareSortCompactionTask()
	ok, err = ex.Enqueue(s.task)
	s.Require().NoError(err)
	s.Require().True(ok)
	ex.executeTask(s.task)
	s.Equal(datapb.CompactionTaskState_completed, ex.tasks[s.task.GetPlanID()].result.GetState(), "the next healthy task must still execute")
	s.Zero(ex.Slots())
}
