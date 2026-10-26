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

package packed

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// Exercise real writer output, a committed manifest and a metadata reload
// without Binlogs. Physical file prefixes (0, 1) differ from logical IDs (7,
// 101); current writer defaults must not replace either persisted format.
func TestRestoreColumnGroupsFromManifest(t *testing.T) {
	paramtable.Init()
	root := t.TempDir()
	config := &indexpb.StorageConfig{StorageType: "local", RootPath: root}
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "100", Type: arrow.PrimitiveTypes.Int64},
		{Name: "101", Type: arrow.PrimitiveTypes.Int64},
		{Name: "102", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	writer, err := NewFFIPackedWriter(filepath.Join(root, "segment"), schema, []storagecommon.ColumnGroup{
		{GroupID: 7, Fields: []int64{100, 102}, Columns: []int{0, 2}},
		{GroupID: 101, Fields: []int64{101}, Columns: []int{1}},
	}, config, nil, map[string]string{PropertyWriterSchemaBasedFormats: "parquet,vortex"})
	require.NoError(t, err)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	for i := range schema.Fields() {
		builder.Field(i).(*array.Int64Builder).Append(10)
	}
	record := builder.NewRecord()
	defer record.Release()
	require.NoError(t, writer.WriteRecordBatch(record))
	output, err := writer.Close()
	require.NoError(t, err)
	defer output.Destroy()
	manifest, err := CommitManifestUpdates(filepath.Join(root, "segment"), ManifestEarliest, config, &ManifestUpdates{NewFiles: output})
	require.NoError(t, err)

	info := &datapb.SegmentInfo{ManifestPath: manifest, Stats: &datapb.Statistics{
		LoadResource: &datapb.LoadResourceStatistics{ColumnGroups: []*datapb.ColumnGroupStatistics{
			{GroupId: 101, FieldIds: []int64{101}},
			{GroupId: 7, FieldIds: []int64{102, 100}},
		}},
	}}
	encoded, err := proto.Marshal(info)
	require.NoError(t, err)
	reloaded := &datapb.SegmentInfo{}
	require.NoError(t, proto.Unmarshal(encoded, reloaded))
	require.Empty(t, reloaded.GetBinlogs())

	params := paramtable.Get()
	oldFormat := params.DataNodeCfg.StorageFormat.GetValue()
	require.NoError(t, params.Save(params.DataNodeCfg.StorageFormat.Key, "parquet"))
	t.Cleanup(func() { _ = params.Save(params.DataNodeCfg.StorageFormat.Key, oldFormat) })
	groups, err := RestoreColumnGroupsFromManifest(context.Background(), reloaded.GetManifestPath(), reloaded.GetStats(), config)
	require.NoError(t, err)
	require.Equal(t, []storagecommon.ColumnGroup{
		{GroupID: 7, Fields: []int64{100, 102}, Format: "parquet"},
		{GroupID: 101, Fields: []int64{101}, Format: "vortex"},
	}, groups)
}

func TestRestoreColumnGroupsPreservesReadFailures(t *testing.T) {
	for _, failure := range []error{ErrLoonTransient, ErrLoonPermanent} {
		mockey.PatchConvey("read failure", t, func() {
			mockey.Mock(readColumnGroupsFromManifest).Return(nil, failure).Build()
			_, err := RestoreColumnGroupsFromManifest(context.Background(), MarshalManifestPath("segment", 1), nil, &indexpb.StorageConfig{})
			require.ErrorIs(t, err, failure)
			require.ErrorIs(t, err, merr.ErrStorage)
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := RestoreColumnGroupsFromManifest(ctx, "", nil, nil)
	require.ErrorIs(t, err, context.Canceled)
}

func TestRestoreColumnGroupsRejectsUnreadableManifest(t *testing.T) {
	paramtable.Init()
	for _, corrupt := range []bool{false, true} {
		t.Run(map[bool]string{false: "missing", true: "corrupt"}[corrupt], func(t *testing.T) {
			root := t.TempDir()
			if corrupt {
				require.NoError(t, os.MkdirAll(filepath.Join(root, "_metadata"), 0o755))
				require.NoError(t, os.WriteFile(filepath.Join(root, "_metadata", "manifest-1.avro"), []byte("invalid manifest"), 0o600))
			}
			_, err := RestoreColumnGroupsFromManifest(context.Background(), MarshalManifestPath(root, 1), nil,
				&indexpb.StorageConfig{StorageType: "local", RootPath: root})
			require.ErrorIs(t, err, ErrLoonPermanent)
			require.NotErrorIs(t, err, ErrLoonTransient)
		})
	}
}
