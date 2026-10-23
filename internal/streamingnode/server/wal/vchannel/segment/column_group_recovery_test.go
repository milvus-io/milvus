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

package segment

import (
	"context"
	"math"
	"path/filepath"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
)

func TestV3GrowingPackResumesFromStatsAndManifest(t *testing.T) {
	config, cm := setupV3TestEnv(t)
	params := paramtable.Get()
	oldFormat := params.DataNodeCfg.StorageFormat.GetValue()
	oldSplit := params.CommonCfg.Stv2SplitSystemColumn.GetValue()
	require.NoError(t, params.Save(params.DataNodeCfg.StorageFormat.Key, "vortex"))
	t.Cleanup(func() {
		_ = params.Save(params.DataNodeCfg.StorageFormat.Key, oldFormat)
		_ = params.Save(params.CommonCfg.Stv2SplitSystemColumn.Key, oldSplit)
	})
	schema := crashTestSchema()
	writer := NewBulkPackWriter(cm, allocator.NewLocalAllocator(1, math.MaxInt64), config)
	first := crashTestPack(1, 2, 3, "v1", 10, schema, buildCrashTestInsertMessage(t, "v1", 1, 2, 3, 10, 10))
	first.Meta.PersistedStorage.ManifestPath = packed.MarshalManifestPath(filepath.Join(config.GetRootPath(), "segment"), packed.ManifestEarliest)
	result, err := writer.FlushInsertBuffer(context.Background(), first)
	require.NoError(t, err)
	baseline := result.PersistedStorage.GetStatistics()
	require.NotEmpty(t, baseline.GetLoadResource().GetColumnGroups())

	// This is the V3 legacy-migration shape after DataCoord catalog reload:
	// Statistics and ManifestPath are durable; per-field KVs were not saved.
	meta := proto.Clone(first.Meta).(*streamingpb.SegmentAssignmentMeta)
	meta.Stat = &streamingpb.SegmentAssignmentStat{ModifiedRows: 1}
	meta.PersistedStorage = &streamingpb.L1SegmentPersistedStorage{
		ManifestPath: result.PersistedStorage.GetManifestPath(), Statistics: baseline,
	}
	encoded, err := proto.Marshal(meta)
	require.NoError(t, err)
	recovered := &streamingpb.SegmentAssignmentMeta{}
	require.NoError(t, proto.Unmarshal(encoded, recovered))
	require.Empty(t, recovered.GetPersistedStorage().GetBinlogs())

	require.NoError(t, params.Save(params.DataNodeCfg.StorageFormat.Key, "parquet"))
	newSplit := "true"
	if oldSplit == "true" {
		newSplit = "false"
	}
	require.NoError(t, params.Save(params.CommonCfg.Stv2SplitSystemColumn.Key, newSplit))
	second := crashTestPack(1, 2, 3, "v1", 20, schema, buildCrashTestInsertMessage(t, "v1", 1, 2, 3, 20, 20))
	second.Meta = recovered
	continued, err := writer.FlushInsertBuffer(context.Background(), second)
	require.NoError(t, err, "append must preserve the old grouping and vortex format")
	stats := continued.PersistedStorage.GetStatistics()
	require.Equal(t, baseline.GetInsertBinlogCount()*2, stats.GetInsertBinlogCount())
	require.Equal(t, []string{"vortex"}, stats.GetFormats())
	oldGroups := baseline.GetLoadResource().GetColumnGroups()
	newGroups := stats.GetLoadResource().GetColumnGroups()
	require.Len(t, newGroups, len(oldGroups))
	for i, group := range oldGroups {
		require.Equal(t, group.GetGroupId(), newGroups[i].GetGroupId())
		require.Equal(t, group.GetFieldIds(), newGroups[i].GetFieldIds())
		require.Greater(t, newGroups[i].GetMemorySize(), group.GetMemorySize())
	}
}

func TestV3ColumnGroupRecoveryReadFailureClassification(t *testing.T) {
	for _, tc := range []struct {
		failure error
		retry   bool
	}{
		{packed.ErrLoonTransient, true},
		{packed.ErrLoonPermanent, false},
	} {
		mockey.PatchConvey("manifest failure classification", t, func() {
			mockey.Mock(packed.RestoreColumnGroupsFromManifest).Return(nil, tc.failure).Build()
			schema := crashTestSchema()
			pack := crashTestPack(1, 2, 3, "v1", 10, schema, buildCrashTestInsertMessage(t, "v1", 1, 2, 3, 10, 10))
			pack.Meta.PersistedStorage = &streamingpb.L1SegmentPersistedStorage{
				ManifestPath: packed.MarshalManifestPath("segment", 1), Statistics: &datapb.Statistics{},
			}
			_, err := NewBulkPackWriter(nil, nil, nil).FlushInsertBuffer(context.Background(), pack)
			require.ErrorIs(t, err, tc.failure)
			require.Equal(t, tc.retry, retry.IsRecoverable(err))
		})
	}
}
