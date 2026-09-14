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

package syncmgr

import (
	"os"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration/cmek/testobserver"
)

func TestCMEKTaskObserverPreservesCallbackAndCommitResult(t *testing.T) {
	paramtable.Init()
	oldNodeID := paramtable.GetNodeID()
	paramtable.SetNodeID(7)
	t.Cleanup(func() { paramtable.SetNodeID(oldNodeID) })
	directory := t.TempDir()
	t.Setenv(testobserver.DirectoryEnv, directory)
	t.Setenv(testobserver.TokenEnv, "run")
	cache := metacache.NewMockMetaCache(t)
	cache.EXPECT().GetSegmentByID(int64(29)).Return(metacache.NewSegmentInfo(&datapb.SegmentInfo{ID: 29, StorageVersion: 3}, nil, nil, nil), true)
	task := &SyncTask{
		collectionID: 19, partitionID: 23, segmentID: 29, channelName: "channel", metacache: cache, batchRows: 2,
		manifestPath: `{"base_path":"root/29","ver":1}`, insertBinlogs: map[int64]*datapb.FieldBinlog{0: {Binlogs: []*datapb.Binlog{{EntriesNum: 2}}}},
	}
	request := &datapb.SaveBinlogPathsRequest{CollectionID: 19, PartitionID: 23, SegmentID: 29, Channel: "channel", StorageVersion: 3, ManifestPath: task.manifestPath}
	var calls int
	var observedTask Task
	var observedError error
	callback := WithCMEKTaskObserver("channel", 19, func(task Task, err error) { calls++; observedTask, observedError = task, err })
	observeCMEKCommit(task, request, nil)
	callback(task, nil)
	require.Equal(t, 1, calls)
	require.Same(t, task, observedTask)
	require.NoError(t, observedError)
	records, err := testobserver.ReadRecords(directory, "run", map[int64]int{7: os.Getpid()})
	require.NoError(t, err)
	_, complete, err := testobserver.CanonicalFlushes(records, 19, map[string]bool{"channel": true}, 2)
	require.NoError(t, err)
	require.True(t, complete)

	// A swallowed broker error must not become successful commit evidence, even
	// if the real task callback subsequently receives nil.
	brokerFailure := errors.New("broker rejected commit")
	observeCMEKCommit(task, request, brokerFailure)
	callback(task, nil)
	require.Equal(t, 2, calls)
	require.NoError(t, observedError)
	records, err = testobserver.ReadRecords(directory, "run", map[int64]int{7: os.Getpid()})
	require.NoError(t, err)
	_, complete, err = testobserver.CanonicalFlushes(records, 19, map[string]bool{"channel": true}, 2)
	require.False(t, complete)
	require.ErrorContains(t, err, "without an acknowledged DataCoord commit")

	callback(task, brokerFailure)
	require.Equal(t, 3, calls)
	require.Same(t, task, observedTask)
	require.ErrorIs(t, observedError, brokerFailure)
}
