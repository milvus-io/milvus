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

//go:build cmektest || test

package syncmgr

import (
	"os"
	"sync"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration/cmek/testobserver"
)

var cmekObserver struct {
	once     sync.Once
	mu       sync.Mutex
	recorder *testobserver.Recorder
	tasks    map[*SyncTask]uint64
	nextTask uint64
}

func cmekRecorder() *testobserver.Recorder {
	directory, token := os.Getenv(testobserver.DirectoryEnv), os.Getenv(testobserver.TokenEnv)
	if directory == "" && token == "" {
		return nil
	}
	cmekObserver.once.Do(func() {
		var err error
		cmekObserver.recorder, err = testobserver.NewRecorder(directory, token, paramtable.GetNodeID(), os.Getpid())
		if err != nil {
			panic(err)
		}
		cmekObserver.tasks = make(map[*SyncTask]uint64)
	})
	return cmekObserver.recorder
}

func appendCMEKRecord(task *SyncTask, record testobserver.Record) {
	recorder := cmekRecorder()
	if recorder == nil {
		return
	}
	cmekObserver.mu.Lock()
	defer cmekObserver.mu.Unlock()
	if task != nil {
		id, ok := cmekObserver.tasks[task]
		if !ok {
			cmekObserver.nextTask++
			id = cmekObserver.nextTask
			cmekObserver.tasks[task] = id
		}
		record.TaskID = id
		if record.Event == "task" {
			delete(cmekObserver.tasks, task)
		}
	}
	if err := recorder.Append(record); err != nil {
		panic(err)
	}
}

func cmekTaskRecord(task *SyncTask) testobserver.Record {
	rows := make(map[int64]int64, len(task.insertBinlogs))
	for fieldID, field := range task.insertBinlogs {
		for _, binlog := range field.GetBinlogs() {
			rows[fieldID] += binlog.GetEntriesNum()
		}
	}
	var storageVersion int64
	if segment, ok := task.metacache.GetSegmentByID(task.segmentID); ok {
		storageVersion = segment.GetStorageVersion()
	}
	return testobserver.Record{
		TaskType: "SyncTask", CollectionID: task.collectionID, PartitionID: task.partitionID,
		SegmentID: task.segmentID, Channel: task.channelName, StorageVersion: storageVersion,
		Rows: task.batchRows, FieldRows: rows, Manifest: task.manifestPath,
	}
}

// Observe the actual broker result before UpdateSync's stale-segment handling
// can turn a rejected commit into a nil return. Never record keys or payload.
func observeCMEKCommit(task *SyncTask, request *datapb.SaveBinlogPathsRequest, err error) {
	if cmekRecorder() == nil {
		return
	}
	record := cmekTaskRecord(task)
	record.Event, record.Success = "commit", err == nil
	record.CollectionID, record.PartitionID, record.SegmentID = request.GetCollectionID(), request.GetPartitionID(), request.GetSegmentID()
	record.Channel = request.GetChannel()
	record.StorageVersion, record.Manifest = request.GetStorageVersion(), request.GetManifestPath()
	appendCMEKRecord(task, record)
}

// WithCMEKTaskObserver composes with the real production callback; it neither
// replaces the writer nor changes task completion or source selection.
func WithCMEKTaskObserver(channel string, collectionID int64, callback func(Task, error)) func(Task, error) {
	if cmekRecorder() == nil {
		return callback
	}
	appendCMEKRecord(nil, testobserver.Record{Event: "registered", CollectionID: collectionID, Channel: channel, Success: true})
	return func(task Task, err error) {
		if callback != nil {
			callback(task, err)
		}
		if syncTask, ok := task.(*SyncTask); ok {
			record := cmekTaskRecord(syncTask)
			record.Event, record.Success = "task", err == nil
			appendCMEKRecord(syncTask, record)
			return
		}
		appendCMEKRecord(nil, testobserver.Record{Event: "task", TaskType: "non-canonical", CollectionID: collectionID, Channel: channel, Success: err == nil})
	}
}
