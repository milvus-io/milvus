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

package datanode

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type dataNodeLogBuffer struct {
	bytes.Buffer
}

func (*dataNodeLogBuffer) Sync() error {
	return nil
}

func captureDataNodeLogs(t *testing.T) *dataNodeLogBuffer {
	t.Helper()

	oldLogger := mlog.L()
	oldLevel := mlog.GetAtomicLevel()
	logs := &dataNodeLogBuffer{}
	logger, props, err := mlog.InitLoggerWithWriteSyncer(&mlog.Config{
		Level:             "debug",
		Format:            "text",
		DisableCaller:     true,
		DisableTimestamp:  true,
		DisableStacktrace: true,
	}, logs)
	require.NoError(t, err)
	mlog.ReplaceGlobals(logger, props)
	t.Cleanup(func() {
		mlog.ReplaceGlobals(oldLogger, &mlog.ZapProperties{Level: oldLevel})
	})
	return logs
}

type failingStorageFactory struct{}

func (failingStorageFactory) NewChunkManager(context.Context, *indexpb.StorageConfig) (storage.ChunkManager, error) {
	return nil, merr.WrapErrIoFailedReason("storage factory unavailable")
}

func TestChunkManagerFailureDoesNotLogStorageAccessKey(t *testing.T) {
	logs := captureDataNodeLogs(t)
	ctx := context.Background()
	node := NewDataNode(ctx)
	node.UpdateStateCode(commonpb.StateCode_Healthy)
	node.storageFactory = failingStorageFactory{}

	accessKey := "DATANODE_ACCESS_KEY_SENTINEL"
	storageConfig := &indexpb.StorageConfig{
		BucketName:  "audit-bucket",
		AccessKeyID: accessKey,
	}

	testCases := []struct {
		name string
		call func() (*commonpb.Status, error)
	}{
		{
			name: "legacy index job",
			call: func() (*commonpb.Status, error) {
				return node.CreateJob(ctx, &workerpb.CreateJobRequest{
					ClusterID:     "cluster",
					BuildID:       1,
					StorageConfig: storageConfig,
				})
			},
		},
		{
			name: "v2 index job",
			call: func() (*commonpb.Status, error) {
				return node.CreateJobV2(ctx, &workerpb.CreateJobV2Request{
					ClusterID: "cluster",
					TaskID:    2,
					JobType:   indexpb.JobType_JobTypeIndexJob,
					Request: &workerpb.CreateJobV2Request_IndexRequest{
						IndexRequest: &workerpb.CreateJobRequest{
							ClusterID:     "cluster",
							BuildID:       2,
							StorageConfig: storageConfig,
						},
					},
				})
			},
		},
		{
			name: "stats job",
			call: func() (*commonpb.Status, error) {
				return node.CreateJobV2(ctx, &workerpb.CreateJobV2Request{
					ClusterID: "cluster",
					TaskID:    3,
					JobType:   indexpb.JobType_JobTypeStatsJob,
					Request: &workerpb.CreateJobV2Request_StatsRequest{
						StatsRequest: &workerpb.CreateStatsRequest{
							ClusterID:     "cluster",
							TaskID:        3,
							StorageConfig: storageConfig,
						},
					},
				})
			},
		},
		{
			name: "pre-import",
			call: func() (*commonpb.Status, error) {
				return node.PreImport(ctx, &datapb.PreImportRequest{TaskID: 4, StorageConfig: storageConfig})
			},
		},
		{
			name: "import",
			call: func() (*commonpb.Status, error) {
				return node.ImportV2(ctx, &datapb.ImportRequest{TaskID: 5, StorageConfig: storageConfig})
			},
		},
		{
			name: "copy segment",
			call: func() (*commonpb.Status, error) {
				return node.CopySegment(ctx, &datapb.CopySegmentRequest{TaskID: 6, StorageConfig: storageConfig})
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			status, err := testCase.call()
			require.NoError(t, err)
			assert.Error(t, merr.Error(status))
		})
	}

	assert.NotContains(t, logs.String(), accessKey)
	assert.Contains(t, logs.String(), "audit-bucket")
}
