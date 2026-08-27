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

package index

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/fileresource"
	"github.com/milvus-io/milvus/internal/util/indexcgowrapper"
	"github.com/milvus-io/milvus/pkg/v3/proto/cgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type statsFileResourceManager struct {
	mode      fileresource.Mode
	downloads int
	releases  int
}

func (m *statsFileResourceManager) GetVersion() uint64 { return 0 }
func (m *statsFileResourceManager) Sync(context.Context, uint64, []*internalpb.FileResourceInfo) error {
	return nil
}

func (m *statsFileResourceManager) Download(context.Context, storage.ChunkManager, ...*internalpb.FileResourceInfo) error {
	m.downloads++
	return nil
}
func (m *statsFileResourceManager) Release(...*internalpb.FileResourceInfo) { m.releases++ }
func (m *statsFileResourceManager) Close()                                  {}
func (m *statsFileResourceManager) Mode() fileresource.Mode                 { return m.mode }

type statsFakeTextIndex struct{}

func (statsFakeTextIndex) Build(*indexcgowrapper.Dataset) error        { return nil }
func (statsFakeTextIndex) Serialize() ([]*indexcgowrapper.Blob, error) { return nil, nil }
func (statsFakeTextIndex) GetIndexFileInfo() ([]*indexcgowrapper.IndexFileInfo, error) {
	return nil, nil
}
func (statsFakeTextIndex) Load([]*indexcgowrapper.Blob) error { return nil }
func (statsFakeTextIndex) Delete() error                      { return nil }
func (statsFakeTextIndex) CleanLocalData() error              { return nil }
func (statsFakeTextIndex) UpLoad() (*cgopb.IndexStats, error) {
	return &cgopb.IndexStats{}, nil
}

func TestStatsCreateTextIndexUsesActualFileResourceMode(t *testing.T) {
	paramtable.Get().Init(paramtable.NewBaseTable())

	for _, testCase := range []struct {
		name            string
		mode            fileresource.Mode
		expectDownloads int
		expectExtraInfo bool
	}{
		{name: "ref", mode: fileresource.RefMode, expectDownloads: 1, expectExtraInfo: true},
		{name: "sync", mode: fileresource.SyncMode, expectDownloads: 0, expectExtraInfo: false},
		{name: "close", mode: fileresource.CloseMode, expectDownloads: 0, expectExtraInfo: false},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			manager := &statsFileResourceManager{mode: testCase.mode}
			oldManager := fileresource.GlobalFileManager
			fileresource.GlobalFileManager = manager
			defer func() { fileresource.GlobalFileManager = oldManager }()

			var captured *indexcgopb.BuildIndexInfo
			buildMock := mockey.Mock(indexcgowrapper.CreateIndex).To(
				func(_ context.Context, info *indexcgopb.BuildIndexInfo) (indexcgowrapper.CodecIndex, error) {
					captured = info
					return statsFakeTextIndex{}, nil
				}).Build()
			defer buildMock.UnPatch()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			req := &workerpb.CreateStatsRequest{
				ClusterID:       "cluster",
				TaskID:          1,
				CollectionID:    1,
				PartitionID:     2,
				TargetSegmentID: 3,
				TaskVersion:     1,
				StorageVersion:  storage.StorageV2,
				StorageConfig:   &indexpb.StorageConfig{RootPath: "storage-root", StorageType: "local"},
				Schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
					{
						FieldID:  101,
						Name:     "text",
						DataType: schemapb.DataType_VarChar,
						TypeParams: []*commonpb.KeyValuePair{
							{Key: "enable_match", Value: "true"},
						},
					},
				}},
				InsertLogs: []*datapb.FieldBinlog{{FieldID: 101}},
				FileResources: []*internalpb.FileResourceInfo{
					{Id: 7, Name: "dict", Path: "dict.jieba"},
				},
			}
			taskManager := NewTaskManager(ctx)
			taskManager.LoadOrStoreStatsTask(req.GetClusterID(), req.GetTaskID(), &StatsTaskInfo{})
			task := NewStatsTask(ctx, cancel, req, taskManager, nil, nil, taskcommon.Resource{})

			err := task.createTextIndex(ctx, req.GetStorageConfig(), 1, 2, 3, 1, 1, req.GetInsertLogs())
			require.NoError(t, err)
			require.NotNil(t, captured)
			require.Equal(t, testCase.expectDownloads, manager.downloads)
			require.Equal(t, testCase.expectDownloads, manager.releases)
			if testCase.expectExtraInfo {
				require.JSONEq(t, `{"resource_map":{"dict":7},"storage_name":"storage-root"}`, captured.GetAnalyzerExtraInfo())
			} else {
				require.Empty(t, captured.GetAnalyzerExtraInfo())
			}
		})
	}
}
