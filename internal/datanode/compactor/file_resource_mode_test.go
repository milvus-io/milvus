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
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/fileresource"
	"github.com/milvus-io/milvus/internal/util/indexcgowrapper"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type compactorFileResourceManager struct {
	mode      fileresource.Mode
	downloads int
	releases  int
}

func (m *compactorFileResourceManager) GetVersion() uint64 { return 0 }
func (m *compactorFileResourceManager) Sync(context.Context, uint64, []*internalpb.FileResourceInfo) error {
	return nil
}

func (m *compactorFileResourceManager) Download(context.Context, storage.ChunkManager, ...*internalpb.FileResourceInfo) error {
	m.downloads++
	return nil
}
func (m *compactorFileResourceManager) Release(...*internalpb.FileResourceInfo) { m.releases++ }
func (m *compactorFileResourceManager) Close()                                  {}
func (m *compactorFileResourceManager) Mode() fileresource.Mode                 { return m.mode }

func TestCreateTextIndexUsesActualFileResourceMode(t *testing.T) {
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
			manager := &compactorFileResourceManager{mode: testCase.mode}
			oldManager := fileresource.GlobalFileManager
			fileresource.GlobalFileManager = manager
			defer func() { fileresource.GlobalFileManager = oldManager }()

			var captured *indexcgopb.BuildIndexInfo
			buildMock := mockey.Mock(indexcgowrapper.CreateIndex).To(
				func(_ context.Context, info *indexcgopb.BuildIndexInfo) (indexcgowrapper.CodecIndex, error) {
					captured = info
					return fakeTextIndex{}, nil
				}).Build()
			defer buildMock.UnPatch()

			params := compaction.GenParams()
			params.StorageConfig = &indexpb.StorageConfig{RootPath: "storage-root", StorageType: "local"}
			_, err := createTextIndex(
				context.Background(),
				nil,
				&datapb.CompactionPlan{
					PlanID: 1,
					Schema: textMatchSchema(101),
					FileResources: []*internalpb.FileResourceInfo{
						{Id: 7, Name: "dict", Path: "dict.jieba"},
					},
				},
				params,
				storage.StorageV2,
				1,
				2,
				3,
				1,
				&datapb.CompactionSegment{SegmentID: 3, StorageVersion: storage.StorageV2},
			)
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
