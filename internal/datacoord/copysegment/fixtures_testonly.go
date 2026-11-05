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

//go:build test

package copysegment

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	dctask "github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/internal/metacache"
	kvdatacoord "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
)

type CleanupEnqueueRecorder struct {
	dctask.GlobalScheduler
	Enqueued chan int64
}

func (s *CleanupEnqueueRecorder) Enqueue(task dctask.Task) {
	select {
	case s.Enqueued <- task.GetTaskID():
	default:
	}
}

// createTestCopyTask creates a minimal CopySegmentTask for testing syncVectorScalarIndexes.
func CreateTestCopyTask(collectionID int64, segmentID int64) CopySegmentTask {
	task := &Task{
		Ctx:   context.Background(),
		Tr:    timerecord.NewTimeRecorder("test"),
		Times: taskcommon.NewTimes(),
	}
	task.Task.Store(&datapb.CopySegmentTask{
		TaskId:       1001,
		JobId:        100,
		CollectionId: collectionID,
		State:        datapb.CopySegmentTaskState_CopySegmentTaskInProgress,
		IdMappings: []*datapb.CopySegmentIDMapping{
			{SourceSegmentId: 1, TargetSegmentId: segmentID, PartitionId: 10},
		},
	})
	return task
}

func NewTestCopyJob(jobID int64, state datapb.CopySegmentJobState) CopySegmentJob {
	return &Job{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        jobID,
			CollectionId: 100,
			State:        state,
		},
		Tr: timerecord.NewTimeRecorder("test job"),
	}
}

func NewCopySegmentTaskTestMeta(t *testing.T, task *Task) (CopySegmentMeta, *meta.Meta) {
	ctx := context.Background()
	catalog := kvdatacoord.NewCatalog(meta.NewMetaMemoryKV(), "", "")
	store := metacache.NewMetaStore(catalog)
	m := &meta.Meta{
		Catalog:   catalog,
		Segments:  meta.NewSegmentsInfo(store),
		MetaStore: store,
	}
	copyMeta, err := NewCopySegmentMeta(ctx, catalog, m, nil, nil, nil)
	assert.NoError(t, err)
	assert.NoError(t, copyMeta.AddTask(ctx, task))
	return copyMeta, m
}

func NewTestCopySegment(segmentID int64) *meta.SegmentInfo {
	return meta.NewSegmentInfo(&datapb.SegmentInfo{
		ID:            segmentID,
		CollectionID:  100,
		PartitionID:   10,
		State:         commonpb.SegmentState_Importing,
		NumOfRows:     100,
		InsertChannel: "ch1",
	})
}

func MakeTestCopySegmentBinlogs() []*datapb.FieldBinlog {
	return []*datapb.FieldBinlog{
		{
			FieldID: 100,
			Binlogs: []*datapb.Binlog{
				{LogID: 1, EntriesNum: 100},
			},
		},
	}
}

// embeddedAllocator: named type for mockey interface-method patching; avoids a
// go1.26 `go vet` printf-pass panic on method expressions of anonymous structs.
type EmbeddedAllocator struct{ allocator.Allocator }
