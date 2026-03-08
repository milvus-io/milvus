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
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/util/conc"
)

func TestNewCopySegmentTask(t *testing.T) {
	mockCM := mocks.NewChunkManager(t)
	mockManager := NewTaskManager()

	req := &datapb.CopySegmentRequest{
		JobID:    100,
		TaskID:   200,
		TaskSlot: 1,
		Sources: []*datapb.CopySegmentSource{
			{
				CollectionId: 111,
				PartitionId:  222,
				SegmentId:    333,
			},
		},
		Targets: []*datapb.CopySegmentTarget{
			{
				CollectionId: 444,
				PartitionId:  555,
				SegmentId:    666,
			},
			{
				CollectionId: 444,
				PartitionId:  777,
				SegmentId:    888,
			},
		},
	}

	t.Run("create task", func(t *testing.T) {
		task := NewCopySegmentTask(req, mockManager, mockCM)
		assert.NotNil(t, task)

		copyTask := task.(*CopySegmentTask)
		assert.Equal(t, int64(100), copyTask.GetJobID())
		assert.Equal(t, int64(200), copyTask.GetTaskID())
		assert.Equal(t, int64(444), copyTask.GetCollectionID())
		assert.Equal(t, int64(1), copyTask.GetSlots())
		assert.Equal(t, datapb.ImportTaskStateV2_Pending, copyTask.GetState())
		assert.Equal(t, CopySegmentTaskType, copyTask.GetType())

		// Verify partition IDs contain both unique partitions
		partitionIDs := copyTask.GetPartitionIDs()
		assert.Contains(t, partitionIDs, int64(555))
		assert.Contains(t, partitionIDs, int64(777))

		// Verify segment results map is initialized
		assert.Equal(t, 2, len(copyTask.segmentResults))
		assert.NotNil(t, copyTask.segmentResults[666])
		assert.NotNil(t, copyTask.segmentResults[888])
	})

	t.Run("task methods", func(t *testing.T) {
		task := NewCopySegmentTask(req, mockManager, mockCM)
		copyTask := task.(*CopySegmentTask)

		// Test GetVchannels (should return nil for CopySegmentTask)
		assert.Nil(t, copyTask.GetVchannels())

		// Test GetSchema (should return nil for CopySegmentTask)
		assert.Nil(t, copyTask.GetSchema())

		// Test GetBufferSize (should return 0)
		assert.Equal(t, int64(0), copyTask.GetBufferSize())

		// Test Cancel
		copyTask.Cancel()
		// Verify context is cancelled
		select {
		case <-copyTask.ctx.Done():
			// Expected behavior
		default:
			t.Fatal("context should be cancelled")
		}
	})

	t.Run("clone task", func(t *testing.T) {
		task := NewCopySegmentTask(req, mockManager, mockCM)
		cloned := task.Clone()
		assert.NotNil(t, cloned)

		copyTask := task.(*CopySegmentTask)
		clonedTask := cloned.(*CopySegmentTask)

		assert.Equal(t, copyTask.GetJobID(), clonedTask.GetJobID())
		assert.Equal(t, copyTask.GetTaskID(), clonedTask.GetTaskID())
		assert.Equal(t, copyTask.GetCollectionID(), clonedTask.GetCollectionID())
		assert.Equal(t, copyTask.GetState(), clonedTask.GetState())
	})
}

func TestCopySegmentTaskExecute(t *testing.T) {
	mockCM := mocks.NewChunkManager(t)
	mockManager := NewTaskManager()

	t.Run("validation - no sources", func(t *testing.T) {
		req := &datapb.CopySegmentRequest{
			JobID:   100,
			TaskID:  200,
			Sources: []*datapb.CopySegmentSource{},
			Targets: []*datapb.CopySegmentTarget{},
		}

		task := NewCopySegmentTask(req, mockManager, mockCM)
		mockManager.Add(task)

		futures := task.Execute()
		assert.Nil(t, futures)

		// Verify task state is Failed
		updatedTask := mockManager.Get(task.GetTaskID())
		assert.Equal(t, datapb.ImportTaskStateV2_Failed, updatedTask.GetState())
		assert.Contains(t, updatedTask.GetReason(), "no source segments")
	})

	t.Run("validation - mismatched source and target count", func(t *testing.T) {
		req := &datapb.CopySegmentRequest{
			JobID:  100,
			TaskID: 201,
			Sources: []*datapb.CopySegmentSource{
				{CollectionId: 111, PartitionId: 222, SegmentId: 333},
			},
			Targets: []*datapb.CopySegmentTarget{
				{CollectionId: 444, PartitionId: 555, SegmentId: 666},
				{CollectionId: 444, PartitionId: 555, SegmentId: 777},
			},
		}

		task := NewCopySegmentTask(req, mockManager, mockCM)
		mockManager.Add(task)

		futures := task.Execute()
		assert.Nil(t, futures)

		// Verify task state is Failed
		updatedTask := mockManager.Get(task.GetTaskID())
		assert.Equal(t, datapb.ImportTaskStateV2_Failed, updatedTask.GetState())
		assert.Contains(t, updatedTask.GetReason(), "does not match")
	})

	t.Run("validation - no insert or delta binlogs", func(t *testing.T) {
		req := &datapb.CopySegmentRequest{
			JobID:  100,
			TaskID: 202,
			Sources: []*datapb.CopySegmentSource{
				{
					CollectionId:  111,
					PartitionId:   222,
					SegmentId:     333,
					InsertBinlogs: []*datapb.FieldBinlog{},
					DeltaBinlogs:  []*datapb.FieldBinlog{},
				},
			},
			Targets: []*datapb.CopySegmentTarget{
				{CollectionId: 444, PartitionId: 555, SegmentId: 666},
			},
		}

		task := NewCopySegmentTask(req, mockManager, mockCM)
		mockManager.Add(task)

		futures := task.Execute()
		assert.NotNil(t, futures)
		assert.Equal(t, 1, len(futures))

		// Wait for future to complete
		_, err := futures[0].Await()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no insert/delete binlogs")
	})

	t.Run("successful copy", func(t *testing.T) {
		mockCM := mocks.NewChunkManager(t)
		mockCM.EXPECT().Copy(mock.Anything, mock.Anything, mock.Anything).Return(nil).Times(1)

		req := &datapb.CopySegmentRequest{
			JobID:  100,
			TaskID: 203,
			Sources: []*datapb.CopySegmentSource{
				{
					CollectionId: 111,
					PartitionId:  222,
					SegmentId:    333,
					InsertBinlogs: []*datapb.FieldBinlog{
						{
							FieldID: 100,
							Binlogs: []*datapb.Binlog{
								{
									EntriesNum: 1000,
									LogPath:    "files/insert_log/111/222/333/100/100001",
									LogSize:    1024,
								},
							},
						},
					},
				},
			},
			Targets: []*datapb.CopySegmentTarget{
				{CollectionId: 444, PartitionId: 555, SegmentId: 666},
			},
		}

		task := NewCopySegmentTask(req, mockManager, mockCM)
		mockManager.Add(task)

		futures := task.Execute()
		assert.NotNil(t, futures)
		assert.Equal(t, 1, len(futures))

		// Wait for future to complete
		_, err := futures[0].Await()
		assert.NoError(t, err)

		// Verify segment results are updated (get updated task from manager)
		copyTask := mockManager.Get(task.GetTaskID()).(*CopySegmentTask)
		segmentResults := copyTask.GetSegmentResults()
		assert.Equal(t, 1, len(segmentResults))
		assert.NotNil(t, segmentResults[666])
		assert.Equal(t, int64(666), segmentResults[666].SegmentId)
		assert.Equal(t, int64(1000), segmentResults[666].ImportedRows)
	})

	t.Run("successful copy with multiple segments", func(t *testing.T) {
		mockCM := mocks.NewChunkManager(t)
		// Expect 4 copy operations (2 segments * 2 files each)
		mockCM.EXPECT().Copy(mock.Anything, mock.Anything, mock.Anything).Return(nil).Times(4)

		req := &datapb.CopySegmentRequest{
			JobID:  100,
			TaskID: 204,
			Sources: []*datapb.CopySegmentSource{
				{
					CollectionId: 111,
					PartitionId:  222,
					SegmentId:    333,
					InsertBinlogs: []*datapb.FieldBinlog{
						{
							FieldID: 100,
							Binlogs: []*datapb.Binlog{
								{
									EntriesNum: 1000,
									LogPath:    "files/insert_log/111/222/333/100/100001",
									LogSize:    1024,
								},
							},
						},
					},
					StatsBinlogs: []*datapb.FieldBinlog{
						{
							FieldID: 100,
							Binlogs: []*datapb.Binlog{
								{
									LogPath: "files/stats_log/111/222/333/100/200001",
								},
							},
						},
					},
				},
				{
					CollectionId: 111,
					PartitionId:  222,
					SegmentId:    444,
					InsertBinlogs: []*datapb.FieldBinlog{
						{
							FieldID: 100,
							Binlogs: []*datapb.Binlog{
								{
									EntriesNum: 2000,
									LogPath:    "files/insert_log/111/222/444/100/100002",
									LogSize:    2048,
								},
							},
						},
					},
					DeltaBinlogs: []*datapb.FieldBinlog{
						{
							FieldID: 100,
							Binlogs: []*datapb.Binlog{
								{
									LogPath: "files/delta_log/111/222/444/100/300001",
								},
							},
						},
					},
				},
			},
			Targets: []*datapb.CopySegmentTarget{
				{CollectionId: 555, PartitionId: 666, SegmentId: 777},
				{CollectionId: 555, PartitionId: 666, SegmentId: 888},
			},
		}

		task := NewCopySegmentTask(req, mockManager, mockCM)
		mockManager.Add(task)

		futures := task.Execute()
		assert.NotNil(t, futures)
		assert.Equal(t, 2, len(futures))

		// Wait for all futures to complete
		for _, future := range futures {
			_, err := future.Await()
			assert.NoError(t, err)
		}

		// Verify both segment results are updated (get updated task from manager)
		copyTask := mockManager.Get(task.GetTaskID()).(*CopySegmentTask)
		segmentResults := copyTask.GetSegmentResults()
		assert.Equal(t, 2, len(segmentResults))

		assert.NotNil(t, segmentResults[777])
		assert.Equal(t, int64(777), segmentResults[777].SegmentId)
		assert.Equal(t, int64(1000), segmentResults[777].ImportedRows)

		assert.NotNil(t, segmentResults[888])
		assert.Equal(t, int64(888), segmentResults[888].SegmentId)
		assert.Equal(t, int64(2000), segmentResults[888].ImportedRows)
	})

	t.Run("copy failure in middle of multiple segments", func(t *testing.T) {
		mockCM := mocks.NewChunkManager(t)
		// First segment succeeds, second segment fails
		mockCM.EXPECT().Copy(mock.Anything,
			"files/insert_log/111/222/333/100/100001",
			"files/insert_log/555/666/777/100/100001").Return(nil).Once()
		mockCM.EXPECT().Copy(mock.Anything,
			"files/insert_log/111/222/444/100/100002",
			"files/insert_log/555/666/888/100/100002").Return(errors.New("copy failed")).Once()

		req := &datapb.CopySegmentRequest{
			JobID:  100,
			TaskID: 205,
			Sources: []*datapb.CopySegmentSource{
				{
					CollectionId: 111,
					PartitionId:  222,
					SegmentId:    333,
					InsertBinlogs: []*datapb.FieldBinlog{
						{
							FieldID: 100,
							Binlogs: []*datapb.Binlog{
								{
									EntriesNum: 1000,
									LogPath:    "files/insert_log/111/222/333/100/100001",
									LogSize:    1024,
								},
							},
						},
					},
				},
				{
					CollectionId: 111,
					PartitionId:  222,
					SegmentId:    444,
					InsertBinlogs: []*datapb.FieldBinlog{
						{
							FieldID: 100,
							Binlogs: []*datapb.Binlog{
								{
									EntriesNum: 2000,
									LogPath:    "files/insert_log/111/222/444/100/100002",
									LogSize:    2048,
								},
							},
						},
					},
				},
			},
			Targets: []*datapb.CopySegmentTarget{
				{CollectionId: 555, PartitionId: 666, SegmentId: 777},
				{CollectionId: 555, PartitionId: 666, SegmentId: 888},
			},
		}

		task := NewCopySegmentTask(req, mockManager, mockCM)
		mockManager.Add(task)

		futures := task.Execute()
		assert.NotNil(t, futures)
		assert.Equal(t, 2, len(futures))

		// Wait for all futures - at least one should fail
		successCount := 0
		errorCount := 0
		for _, future := range futures {
			_, err := future.Await()
			if err != nil {
				errorCount++
			} else {
				successCount++
			}
		}

		assert.Equal(t, 1, successCount)
		assert.Equal(t, 1, errorCount)
	})

	t.Run("copy with only delta binlogs (no insert binlogs)", func(t *testing.T) {
		mockCM := mocks.NewChunkManager(t)
		mockCM.EXPECT().Copy(mock.Anything, mock.Anything, mock.Anything).Return(nil).Times(1)

		req := &datapb.CopySegmentRequest{
			JobID:  100,
			TaskID: 206,
			Sources: []*datapb.CopySegmentSource{
				{
					CollectionId:  111,
					PartitionId:   222,
					SegmentId:     333,
					InsertBinlogs: []*datapb.FieldBinlog{}, // Empty insert binlogs
					DeltaBinlogs: []*datapb.FieldBinlog{
						{
							FieldID: 100,
							Binlogs: []*datapb.Binlog{
								{
									LogPath: "files/delta_log/111/222/333/100/300001",
								},
							},
						},
					},
				},
			},
			Targets: []*datapb.CopySegmentTarget{
				{CollectionId: 444, PartitionId: 555, SegmentId: 666},
			},
		}

		task := NewCopySegmentTask(req, mockManager, mockCM)
		mockManager.Add(task)

		futures := task.Execute()
		assert.NotNil(t, futures)
		assert.Equal(t, 1, len(futures))

		// Wait for future to complete - should succeed with only delta binlogs
		_, err := futures[0].Await()
		assert.NoError(t, err)

		// Verify segment results are updated
		copyTask := task.(*CopySegmentTask)
		segmentResults := copyTask.GetSegmentResults()
		assert.Equal(t, 1, len(segmentResults))
		assert.NotNil(t, segmentResults[666])
	})

	t.Run("empty targets", func(t *testing.T) {
		req := &datapb.CopySegmentRequest{
			JobID:   100,
			TaskID:  207,
			Sources: []*datapb.CopySegmentSource{},
			Targets: []*datapb.CopySegmentTarget{},
		}

		task := NewCopySegmentTask(req, mockManager, mockCM)

		// Verify task is created with empty collection and partition IDs
		copyTask := task.(*CopySegmentTask)
		assert.Equal(t, int64(0), copyTask.GetCollectionID())
		assert.Equal(t, 0, len(copyTask.GetPartitionIDs()))
		assert.Equal(t, 0, len(copyTask.GetSegmentResults()))
	})
}

func TestCopySegmentTaskGetSegmentResults(t *testing.T) {
	mockCM := mocks.NewChunkManager(t)
	mockManager := NewTaskManager()

	req := &datapb.CopySegmentRequest{
		JobID:    100,
		TaskID:   300,
		TaskSlot: 1,
		Targets: []*datapb.CopySegmentTarget{
			{CollectionId: 444, PartitionId: 555, SegmentId: 666},
			{CollectionId: 444, PartitionId: 555, SegmentId: 777},
		},
	}

	task := NewCopySegmentTask(req, mockManager, mockCM)
	copyTask := task.(*CopySegmentTask)

	t.Run("initial segment results", func(t *testing.T) {
		results := copyTask.GetSegmentResults()
		assert.Equal(t, 2, len(results))

		// Verify initial state
		assert.Equal(t, int64(666), results[666].SegmentId)
		assert.Equal(t, int64(0), results[666].ImportedRows)
		assert.Equal(t, 0, len(results[666].Binlogs))

		assert.Equal(t, int64(777), results[777].SegmentId)
		assert.Equal(t, int64(0), results[777].ImportedRows)
		assert.Equal(t, 0, len(results[777].Binlogs))
	})

	t.Run("update segment results", func(t *testing.T) {
		// Manually update segment result
		mockManager.Add(task)

		newResult := &datapb.CopySegmentResult{
			SegmentId:    666,
			ImportedRows: 5000,
			Binlogs: []*datapb.FieldBinlog{
				{
					FieldID: 100,
					Binlogs: []*datapb.Binlog{
						{
							EntriesNum: 5000,
							LogPath:    "files/insert_log/444/555/666/100/100001",
						},
					},
				},
			},
		}

		mockManager.Update(task.GetTaskID(), UpdateSegmentResult(newResult))

		// Verify update
		updatedTask := mockManager.Get(task.GetTaskID()).(*CopySegmentTask)
		results := updatedTask.GetSegmentResults()
		assert.Equal(t, int64(5000), results[666].ImportedRows)
		assert.Equal(t, 1, len(results[666].Binlogs))
	})
}

func TestCopySegmentTaskStateManagement(t *testing.T) {
	mockCM := mocks.NewChunkManager(t)
	mockManager := NewTaskManager()

	req := &datapb.CopySegmentRequest{
		JobID:    100,
		TaskID:   400,
		TaskSlot: 1,
		Sources: []*datapb.CopySegmentSource{
			{CollectionId: 111, PartitionId: 222, SegmentId: 333},
		},
		Targets: []*datapb.CopySegmentTarget{
			{CollectionId: 444, PartitionId: 555, SegmentId: 666},
		},
	}

	task := NewCopySegmentTask(req, mockManager, mockCM)
	mockManager.Add(task)

	t.Run("initial state", func(t *testing.T) {
		assert.Equal(t, datapb.ImportTaskStateV2_Pending, task.GetState())
		assert.Equal(t, "", task.GetReason())
	})

	t.Run("update state to InProgress", func(t *testing.T) {
		mockManager.Update(task.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_InProgress))
		updatedTask := mockManager.Get(task.GetTaskID())
		assert.Equal(t, datapb.ImportTaskStateV2_InProgress, updatedTask.GetState())
	})

	t.Run("update state to Failed with reason", func(t *testing.T) {
		reason := "test failure reason"
		mockManager.Update(task.GetTaskID(),
			UpdateState(datapb.ImportTaskStateV2_Failed),
			UpdateReason(reason))

		updatedTask := mockManager.Get(task.GetTaskID())
		assert.Equal(t, datapb.ImportTaskStateV2_Failed, updatedTask.GetState())
		assert.Equal(t, reason, updatedTask.GetReason())
	})

	t.Run("update state to Completed", func(t *testing.T) {
		mockManager.Update(task.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_Completed))
		updatedTask := mockManager.Get(task.GetTaskID())
		assert.Equal(t, datapb.ImportTaskStateV2_Completed, updatedTask.GetState())
	})
}

func TestCopySegmentTaskWithIndexFiles(t *testing.T) {
	mockCM := mocks.NewChunkManager(t)
	mockManager := NewTaskManager()

	t.Run("copy with vector/scalar index files", func(t *testing.T) {
		mockCM.EXPECT().Copy(mock.Anything, mock.Anything, mock.Anything).Return(nil).Times(2)

		req := &datapb.CopySegmentRequest{
			JobID:  100,
			TaskID: 500,
			Sources: []*datapb.CopySegmentSource{
				{
					CollectionId: 111,
					PartitionId:  222,
					SegmentId:    333,
					InsertBinlogs: []*datapb.FieldBinlog{
						{
							FieldID: 100,
							Binlogs: []*datapb.Binlog{
								{
									EntriesNum: 1000,
									LogPath:    "files/insert_log/111/222/333/100/100001",
									LogSize:    1024,
								},
							},
						},
					},
					IndexFiles: []*indexpb.IndexFilePathInfo{
						{
							FieldID:        100,
							IndexID:        1001,
							BuildID:        1002,
							IndexFilePaths: []string{"files/index_files/111/222/333/100/1001/1002/index1"},
							SerializedSize: 5000,
						},
					},
				},
			},
			Targets: []*datapb.CopySegmentTarget{
				{CollectionId: 444, PartitionId: 555, SegmentId: 666},
			},
		}

		task := NewCopySegmentTask(req, mockManager, mockCM)
		mockManager.Add(task)

		futures := task.Execute()
		assert.NotNil(t, futures)
		assert.Equal(t, 1, len(futures))

		_, err := futures[0].Await()
		assert.NoError(t, err)

		// Verify segment results include index info
		copyTask := task.(*CopySegmentTask)
		segmentResults := copyTask.GetSegmentResults()
		assert.Equal(t, 1, len(segmentResults))
		assert.NotNil(t, segmentResults[666].IndexInfos)
	})

	t.Run("copy with text index files", func(t *testing.T) {
		mockCM := mocks.NewChunkManager(t)
		mockCM.EXPECT().Copy(mock.Anything, mock.Anything, mock.Anything).Return(nil).Times(2)

		req := &datapb.CopySegmentRequest{
			JobID:  100,
			TaskID: 501,
			Sources: []*datapb.CopySegmentSource{
				{
					CollectionId: 111,
					PartitionId:  222,
					SegmentId:    333,
					InsertBinlogs: []*datapb.FieldBinlog{
						{
							FieldID: 100,
							Binlogs: []*datapb.Binlog{
								{
									EntriesNum: 1000,
									LogPath:    "files/insert_log/111/222/333/100/100001",
									LogSize:    1024,
								},
							},
						},
					},
					TextIndexFiles: map[int64]*datapb.TextIndexStats{
						100: {
							FieldID:    100,
							Version:    1,
							BuildID:    2001,
							Files:      []string{"files/text_log/123/1/111/222/333/100/text1"},
							LogSize:    2048,
							MemorySize: 4096,
						},
					},
				},
			},
			Targets: []*datapb.CopySegmentTarget{
				{CollectionId: 444, PartitionId: 555, SegmentId: 666},
			},
		}

		task := NewCopySegmentTask(req, mockManager, mockCM)
		mockManager.Add(task)

		futures := task.Execute()
		assert.NotNil(t, futures)

		_, err := futures[0].Await()
		assert.NoError(t, err)

		// Verify segment results include text index info
		copyTask := task.(*CopySegmentTask)
		segmentResults := copyTask.GetSegmentResults()
		assert.NotNil(t, segmentResults[666].TextIndexInfos)
	})

	t.Run("copy with json key index files", func(t *testing.T) {
		mockCM := mocks.NewChunkManager(t)
		mockCM.EXPECT().Copy(mock.Anything, mock.Anything, mock.Anything).Return(nil).Times(2)

		req := &datapb.CopySegmentRequest{
			JobID:  100,
			TaskID: 502,
			Sources: []*datapb.CopySegmentSource{
				{
					CollectionId: 111,
					PartitionId:  222,
					SegmentId:    333,
					InsertBinlogs: []*datapb.FieldBinlog{
						{
							FieldID: 100,
							Binlogs: []*datapb.Binlog{
								{
									EntriesNum: 1000,
									LogPath:    "files/insert_log/111/222/333/100/100001",
									LogSize:    1024,
								},
							},
						},
					},
					JsonKeyIndexFiles: map[int64]*datapb.JsonKeyStats{
						101: {
							FieldID:    101,
							Version:    1,
							BuildID:    3001,
							Files:      []string{"files/json_key_index_log/123/1/111/222/333/101/json1"},
							MemorySize: 3072,
						},
					},
				},
			},
			Targets: []*datapb.CopySegmentTarget{
				{CollectionId: 444, PartitionId: 555, SegmentId: 666},
			},
		}

		task := NewCopySegmentTask(req, mockManager, mockCM)
		mockManager.Add(task)

		futures := task.Execute()
		assert.NotNil(t, futures)

		_, err := futures[0].Await()
		assert.NoError(t, err)

		// Verify segment results include json key index info
		copyTask := task.(*CopySegmentTask)
		segmentResults := copyTask.GetSegmentResults()
		assert.NotNil(t, segmentResults[666].JsonKeyIndexInfos)
	})

	t.Run("copy with all types of binlogs and indexes", func(t *testing.T) {
		mockCM := mocks.NewChunkManager(t)
		// Insert + Stats + Delta + BM25 + Index + Text + JsonKey = 7 files
		mockCM.EXPECT().Copy(mock.Anything, mock.Anything, mock.Anything).Return(nil).Times(7)

		req := &datapb.CopySegmentRequest{
			JobID:  100,
			TaskID: 503,
			Sources: []*datapb.CopySegmentSource{
				{
					CollectionId: 111,
					PartitionId:  222,
					SegmentId:    333,
					InsertBinlogs: []*datapb.FieldBinlog{
						{
							FieldID: 100,
							Binlogs: []*datapb.Binlog{
								{
									EntriesNum: 1000,
									LogPath:    "files/insert_log/111/222/333/100/100001",
									LogSize:    1024,
								},
							},
						},
					},
					StatsBinlogs: []*datapb.FieldBinlog{
						{
							FieldID: 100,
							Binlogs: []*datapb.Binlog{
								{
									LogPath: "files/stats_log/111/222/333/100/200001",
								},
							},
						},
					},
					DeltaBinlogs: []*datapb.FieldBinlog{
						{
							FieldID: 100,
							Binlogs: []*datapb.Binlog{
								{
									LogPath: "files/delta_log/111/222/333/100/300001",
								},
							},
						},
					},
					Bm25Binlogs: []*datapb.FieldBinlog{
						{
							FieldID: 100,
							Binlogs: []*datapb.Binlog{
								{
									LogPath: "files/bm25_stats/111/222/333/100/400001",
								},
							},
						},
					},
					IndexFiles: []*indexpb.IndexFilePathInfo{
						{
							FieldID:        100,
							IndexID:        1001,
							BuildID:        1002,
							IndexFilePaths: []string{"files/index_files/111/222/333/100/1001/1002/index1"},
							SerializedSize: 5000,
						},
					},
					TextIndexFiles: map[int64]*datapb.TextIndexStats{
						100: {
							FieldID: 100,
							Files:   []string{"files/text_log/123/1/111/222/333/100/text1"},
						},
					},
					JsonKeyIndexFiles: map[int64]*datapb.JsonKeyStats{
						101: {
							FieldID: 101,
							Files:   []string{"files/json_key_index_log/123/1/111/222/333/101/json1"},
						},
					},
				},
			},
			Targets: []*datapb.CopySegmentTarget{
				{CollectionId: 444, PartitionId: 555, SegmentId: 666},
			},
		}

		task := NewCopySegmentTask(req, mockManager, mockCM)
		mockManager.Add(task)

		futures := task.Execute()
		assert.NotNil(t, futures)

		_, err := futures[0].Await()
		assert.NoError(t, err)

		// Verify all types of data are present
		copyTask := task.(*CopySegmentTask)
		segmentResults := copyTask.GetSegmentResults()
		result := segmentResults[666]

		assert.NotNil(t, result.Binlogs)           // Insert binlogs
		assert.NotNil(t, result.Statslogs)         // Stats binlogs
		assert.NotNil(t, result.Deltalogs)         // Delta binlogs
		assert.NotNil(t, result.Bm25Logs)          // BM25 binlogs
		assert.NotNil(t, result.IndexInfos)        // Vector/Scalar indexes
		assert.NotNil(t, result.TextIndexInfos)    // Text indexes
		assert.NotNil(t, result.JsonKeyIndexInfos) // JSON key indexes
	})
}

func TestCopySegmentTaskConcurrency(t *testing.T) {
	mockManager := NewTaskManager()

	t.Run("concurrent execution of multiple tasks", func(t *testing.T) {
		// Create multiple tasks
		tasks := make([]Task, 0, 5)
		for i := 0; i < 5; i++ {
			taskID := int64(600 + i)
			mockCM := mocks.NewChunkManager(t)
			mockCM.EXPECT().Copy(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

			req := &datapb.CopySegmentRequest{
				JobID:  100,
				TaskID: taskID,
				Sources: []*datapb.CopySegmentSource{
					{
						CollectionId: 111,
						PartitionId:  222,
						SegmentId:    333 + int64(i),
						InsertBinlogs: []*datapb.FieldBinlog{
							{
								FieldID: 100,
								Binlogs: []*datapb.Binlog{
									{
										EntriesNum: 1000,
										LogPath:    fmt.Sprintf("files/insert_log/111/222/%d/100/100001", 333+i),
										LogSize:    1024,
									},
								},
							},
						},
					},
				},
				Targets: []*datapb.CopySegmentTarget{
					{CollectionId: 444, PartitionId: 555, SegmentId: 666 + int64(i)},
				},
			}

			task := NewCopySegmentTask(req, mockManager, mockCM)
			mockManager.Add(task)
			tasks = append(tasks, task)
		}

		// Execute all tasks concurrently
		allFutures := make([]*conc.Future[any], 0)
		for _, task := range tasks {
			futures := task.Execute()
			if futures != nil {
				allFutures = append(allFutures, futures...)
			}
		}

		// Wait for all futures to complete
		successCount := 0
		for _, future := range allFutures {
			_, err := future.Await()
			if err == nil {
				successCount++
			}
		}

		// All tasks should succeed
		assert.Equal(t, len(allFutures), successCount)
	})
}

func TestCopySegmentTaskEdgeCases(t *testing.T) {
	mockCM := mocks.NewChunkManager(t)
	mockManager := NewTaskManager()

	t.Run("large number of binlog files", func(t *testing.T) {
		// Create 100 binlog files
		binlogs := make([]*datapb.Binlog, 100)
		for i := 0; i < 100; i++ {
			binlogs[i] = &datapb.Binlog{
				EntriesNum: 100,
				LogPath:    fmt.Sprintf("files/insert_log/111/222/333/100/%d", 500000+i),
				LogSize:    1024,
			}
		}

		mockCM.EXPECT().Copy(mock.Anything, mock.Anything, mock.Anything).Return(nil).Times(100)

		req := &datapb.CopySegmentRequest{
			JobID:  100,
			TaskID: 700,
			Sources: []*datapb.CopySegmentSource{
				{
					CollectionId: 111,
					PartitionId:  222,
					SegmentId:    333,
					InsertBinlogs: []*datapb.FieldBinlog{
						{
							FieldID: 100,
							Binlogs: binlogs,
						},
					},
				},
			},
			Targets: []*datapb.CopySegmentTarget{
				{CollectionId: 444, PartitionId: 555, SegmentId: 666},
			},
		}

		task := NewCopySegmentTask(req, mockManager, mockCM)
		mockManager.Add(task)

		futures := task.Execute()
		assert.NotNil(t, futures)

		_, err := futures[0].Await()
		assert.NoError(t, err)

		// Verify total rows (get updated task from manager)
		copyTask := mockManager.Get(task.GetTaskID()).(*CopySegmentTask)
		segmentResults := copyTask.GetSegmentResults()
		assert.Equal(t, int64(10000), segmentResults[666].ImportedRows) // 100 files * 100 rows each
	})

	t.Run("task with zero slot", func(t *testing.T) {
		req := &datapb.CopySegmentRequest{
			JobID:    100,
			TaskID:   701,
			TaskSlot: 0, // Zero slot
			Sources: []*datapb.CopySegmentSource{
				{CollectionId: 111, PartitionId: 222, SegmentId: 333},
			},
			Targets: []*datapb.CopySegmentTarget{
				{CollectionId: 444, PartitionId: 555, SegmentId: 666},
			},
		}

		task := NewCopySegmentTask(req, mockManager, mockCM)
		copyTask := task.(*CopySegmentTask)

		assert.Equal(t, int64(0), copyTask.GetSlots())
	})

	t.Run("same partition in multiple targets", func(t *testing.T) {
		req := &datapb.CopySegmentRequest{
			JobID:    100,
			TaskID:   702,
			TaskSlot: 1,
			Sources: []*datapb.CopySegmentSource{
				{CollectionId: 111, PartitionId: 222, SegmentId: 333},
			},
			Targets: []*datapb.CopySegmentTarget{
				{CollectionId: 444, PartitionId: 555, SegmentId: 666},
				{CollectionId: 444, PartitionId: 555, SegmentId: 777}, // Same partition
				{CollectionId: 444, PartitionId: 555, SegmentId: 888}, // Same partition
			},
		}

		task := NewCopySegmentTask(req, mockManager, mockCM)
		copyTask := task.(*CopySegmentTask)

		// Should only have one unique partition ID
		partitionIDs := copyTask.GetPartitionIDs()
		assert.Equal(t, 1, len(partitionIDs))
		assert.Equal(t, int64(555), partitionIDs[0])

		// But should have 3 segment results
		segmentResults := copyTask.GetSegmentResults()
		assert.Equal(t, 3, len(segmentResults))
	})
}

func TestCopySegmentTask_RecordCopiedFiles(t *testing.T) {
	cm := mocks.NewChunkManager(t)
	req := &datapb.CopySegmentRequest{
		JobID:  100,
		TaskID: 1000,
		Sources: []*datapb.CopySegmentSource{
			{
				CollectionId: 111,
				PartitionId:  222,
				SegmentId:    333,
			},
		},
		Targets: []*datapb.CopySegmentTarget{
			{
				CollectionId: 444,
				PartitionId:  555,
				SegmentId:    666,
			},
		},
	}

	mockManager := NewMockTaskManager(t)
	task := NewCopySegmentTask(req, mockManager, cm).(*CopySegmentTask)

	t.Run("record files sequentially", func(t *testing.T) {
		files1 := []string{"10001", "10002"}
		files2 := []string{"10003", "10004"}

		task.recordCopiedFiles(files1)
		task.recordCopiedFiles(files2)

		task.copiedFilesMu.Lock()
		defer task.copiedFilesMu.Unlock()

		assert.Len(t, task.copiedFiles, 4)
		assert.Contains(t, task.copiedFiles, "10001")
		assert.Contains(t, task.copiedFiles, "10002")
		assert.Contains(t, task.copiedFiles, "10003")
		assert.Contains(t, task.copiedFiles, "10004")
	})

	t.Run("record empty files", func(t *testing.T) {
		newTask := NewCopySegmentTask(req, mockManager, cm).(*CopySegmentTask)
		newTask.recordCopiedFiles([]string{})

		newTask.copiedFilesMu.Lock()
		defer newTask.copiedFilesMu.Unlock()

		assert.Empty(t, newTask.copiedFiles)
	})

	t.Run("concurrent recording", func(t *testing.T) {
		newTask := NewCopySegmentTask(req, mockManager, cm).(*CopySegmentTask)
		var wg sync.WaitGroup

		// Simulate 10 concurrent goroutines recording files
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				files := []string{fmt.Sprintf("file%d.log", id)}
				newTask.recordCopiedFiles(files)
			}(i)
		}

		wg.Wait()

		newTask.copiedFilesMu.Lock()
		defer newTask.copiedFilesMu.Unlock()

		assert.Len(t, newTask.copiedFiles, 10)
	})
}

func TestCopySegmentTask_CleanupCopiedFiles(t *testing.T) {
	t.Run("cleanup with files", func(t *testing.T) {
		cm := mocks.NewChunkManager(t)
		req := &datapb.CopySegmentRequest{
			JobID:  100,
			TaskID: 1000,
			Sources: []*datapb.CopySegmentSource{{
				CollectionId: 111,
				PartitionId:  222,
				SegmentId:    333,
			}},
			Targets: []*datapb.CopySegmentTarget{{
				CollectionId: 444,
				PartitionId:  555,
				SegmentId:    666,
			}},
		}

		mockManager := NewMockTaskManager(t)
		task := NewCopySegmentTask(req, mockManager, cm).(*CopySegmentTask)

		// Record some files
		files := []string{
			"files/insert_log/444/555/666/1/10001",
			"files/insert_log/444/555/666/1/10002",
			"files/insert_log/444/555/666/1/10003",
		}
		task.recordCopiedFiles(files)

		// Expect MultiRemove to be called with the files
		cm.EXPECT().MultiRemove(mock.Anything, files).Return(nil).Once()

		// Call cleanup
		task.CleanupCopiedFiles()

		// Verify MultiRemove was called
		cm.AssertExpectations(t)
	})

	t.Run("cleanup with no files", func(t *testing.T) {
		cm := mocks.NewChunkManager(t)
		req := &datapb.CopySegmentRequest{
			JobID:  100,
			TaskID: 1000,
			Sources: []*datapb.CopySegmentSource{{
				CollectionId: 111,
				PartitionId:  222,
				SegmentId:    333,
			}},
			Targets: []*datapb.CopySegmentTarget{{
				CollectionId: 444,
				PartitionId:  555,
				SegmentId:    666,
			}},
		}

		mockManager := NewMockTaskManager(t)
		task := NewCopySegmentTask(req, mockManager, cm).(*CopySegmentTask)

		// Don't record any files
		// MultiRemove should NOT be called

		// Call cleanup - should return early
		task.CleanupCopiedFiles()

		// Verify no calls were made
		cm.AssertNotCalled(t, "MultiRemove", mock.Anything, mock.Anything)
	})

	t.Run("cleanup failure is logged but doesn't panic", func(t *testing.T) {
		cm := mocks.NewChunkManager(t)
		req := &datapb.CopySegmentRequest{
			JobID:  100,
			TaskID: 1000,
			Sources: []*datapb.CopySegmentSource{{
				CollectionId: 111,
				PartitionId:  222,
				SegmentId:    333,
			}},
			Targets: []*datapb.CopySegmentTarget{{
				CollectionId: 444,
				PartitionId:  555,
				SegmentId:    666,
			}},
		}

		mockManager := NewMockTaskManager(t)
		task := NewCopySegmentTask(req, mockManager, cm).(*CopySegmentTask)

		// Record some files
		files := []string{"10001", "10002"}
		task.recordCopiedFiles(files)

		// Expect MultiRemove to fail
		cm.EXPECT().MultiRemove(mock.Anything, files).Return(errors.New("cleanup failed")).Once()

		// Call cleanup - should not panic
		assert.NotPanics(t, func() {
			task.CleanupCopiedFiles()
		})

		// Verify MultiRemove was called
		cm.AssertExpectations(t)
	})

	t.Run("cleanup is idempotent", func(t *testing.T) {
		cm := mocks.NewChunkManager(t)
		req := &datapb.CopySegmentRequest{
			JobID:  100,
			TaskID: 1000,
			Sources: []*datapb.CopySegmentSource{{
				CollectionId: 111,
				PartitionId:  222,
				SegmentId:    333,
			}},
			Targets: []*datapb.CopySegmentTarget{{
				CollectionId: 444,
				PartitionId:  555,
				SegmentId:    666,
			}},
		}

		mockManager := NewMockTaskManager(t)
		task := NewCopySegmentTask(req, mockManager, cm).(*CopySegmentTask)

		// Record some files
		files := []string{"10001", "10002"}
		task.recordCopiedFiles(files)

		// Expect MultiRemove to be called twice (idempotent)
		cm.EXPECT().MultiRemove(mock.Anything, files).Return(nil).Times(2)

		// Call cleanup twice
		task.CleanupCopiedFiles()
		task.CleanupCopiedFiles()

		// Verify MultiRemove was called twice
		cm.AssertExpectations(t)
	})
}

func TestCopySegmentTask_CopySingleSegment_WithCleanup(t *testing.T) {
	t.Run("records files on success", func(t *testing.T) {
		cm := mocks.NewChunkManager(t)
		req := &datapb.CopySegmentRequest{
			JobID:  100,
			TaskID: 1000,
			Sources: []*datapb.CopySegmentSource{{
				CollectionId: 111,
				PartitionId:  222,
				SegmentId:    333,
				InsertBinlogs: []*datapb.FieldBinlog{{
					FieldID: 1,
					Binlogs: []*datapb.Binlog{
						{LogPath: "files/insert_log/111/222/333/1/10001", LogSize: 100},
					},
				}},
			}},
			Targets: []*datapb.CopySegmentTarget{{
				CollectionId: 444,
				PartitionId:  555,
				SegmentId:    666,
			}},
		}

		mockManager := NewMockTaskManager(t)
		mockManager.EXPECT().Update(mock.Anything, mock.Anything).Return()
		task := NewCopySegmentTask(req, mockManager, cm).(*CopySegmentTask)

		// Mock successful copy
		cm.EXPECT().Copy(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

		// Execute copy
		_, err := task.copySingleSegment(req.Sources[0], req.Targets[0])

		assert.NoError(t, err)

		// Verify files were recorded
		task.copiedFilesMu.Lock()
		defer task.copiedFilesMu.Unlock()
		assert.Len(t, task.copiedFiles, 1)
		assert.Contains(t, task.copiedFiles, "files/insert_log/444/555/666/1/10001")
	})

	t.Run("records partial files on failure", func(t *testing.T) {
		cm := mocks.NewChunkManager(t)
		req := &datapb.CopySegmentRequest{
			JobID:  100,
			TaskID: 1000,
			Sources: []*datapb.CopySegmentSource{{
				CollectionId: 111,
				PartitionId:  222,
				SegmentId:    333,
				InsertBinlogs: []*datapb.FieldBinlog{{
					FieldID: 1,
					Binlogs: []*datapb.Binlog{
						{LogPath: "files/insert_log/111/222/333/1/10001", LogSize: 100},
						{LogPath: "files/insert_log/111/222/333/1/10002", LogSize: 200},
					},
				}},
			}},
			Targets: []*datapb.CopySegmentTarget{{
				CollectionId: 444,
				PartitionId:  555,
				SegmentId:    666,
			}},
		}

		mockManager := NewMockTaskManager(t)
		// Update is called twice on failure: UpdateState and UpdateReason
		mockManager.EXPECT().Update(mock.Anything, mock.Anything, mock.Anything).Return()
		task := NewCopySegmentTask(req, mockManager, cm).(*CopySegmentTask)

		// First copy succeeds, second fails
		cm.EXPECT().Copy(mock.Anything, "files/insert_log/111/222/333/1/10001", "files/insert_log/444/555/666/1/10001").Return(nil).Maybe()
		cm.EXPECT().Copy(mock.Anything, "files/insert_log/111/222/333/1/10002", "files/insert_log/444/555/666/1/10002").Return(errors.New("copy failed")).Maybe()

		// Execute copy
		_, err := task.copySingleSegment(req.Sources[0], req.Targets[0])

		assert.Error(t, err)

		// Verify partial files were still recorded
		task.copiedFilesMu.Lock()
		defer task.copiedFilesMu.Unlock()
		assert.True(t, len(task.copiedFiles) <= 1, "should record file copied before failure")
	})
}
