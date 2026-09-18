// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datacoord

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"math"
	"path"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	idallocator "github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	snapshotstorage "github.com/milvus-io/milvus/internal/snapshotio/storage"
	milvusstorage "github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	streamingutil "github.com/milvus-io/milvus/internal/util/streamingutil/util"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestSnapshotImportL0URIRead(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	localCM := milvusstorage.NewLocalChunkManager()
	cfg := &indexpb.StorageConfig{StorageType: "local", RootPath: root}
	schema := typeutil.AppendSystemFields(&schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
	}})
	writer, err := milvusstorage.NewBinlogRecordWriter(ctx, 1, 10, 20, schema, idallocator.NewLocalAllocator(1, 1000), 1024*1024, 100,
		milvusstorage.WithVersion(milvusstorage.StorageV3), milvusstorage.WithStorageConfig(cfg),
		milvusstorage.WithColumnGroups([]storagecommon.ColumnGroup{{GroupID: 0, Columns: []int{0, 1, 2}, Fields: []int64{100, 0, 1}}}),
		milvusstorage.WithUploader(localCM.MultiWrite))
	require.NoError(t, err)
	record, err := milvusstorage.ValueSerializer([]*milvusstorage.Value{
		{Value: map[int64]any{100: int64(1), 0: int64(1), 1: int64(100)}},
		{Value: map[int64]any{100: int64(2), 0: int64(2), 1: int64(100)}},
	}, schema)
	require.NoError(t, err)
	require.NoError(t, writer.Write(record))
	record.Release()
	require.NoError(t, writer.Close())
	_, _, _, manifest, _ := writer.GetLogs()
	deltaPath := path.Join(root, "legacy.delta")
	deltaWriter, err := milvusstorage.NewDeltalogWriter(ctx, 1, 10, 30, 1, schemapb.DataType_Int64, deltaPath,
		milvusstorage.WithVersion(milvusstorage.StorageV1), milvusstorage.WithUploader(localCM.MultiWrite))
	require.NoError(t, err)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, arrow.NewSchema([]arrow.Field{
		{Name: "pk", Type: arrow.PrimitiveTypes.Int64}, {Name: "ts", Type: arrow.PrimitiveTypes.Int64},
	}, nil))
	builder.Field(0).(*array.Int64Builder).Append(1)
	builder.Field(1).(*array.Int64Builder).Append(200)
	deltaRecord := milvusstorage.NewSimpleArrowRecord(builder.NewRecord(), map[milvusstorage.FieldID]int{0: 0, 1: 1})
	builder.Release()
	require.NoError(t, deltaWriter.Write(deltaRecord))
	deltaRecord.Release()
	require.NoError(t, deltaWriter.Close())

	key := strings.TrimPrefix(deltaPath, "/")
	cm := milvusstorage.NewRemoteChunkManagerForTesting(nil, "source", "")
	// Only replace object transport. Exercise real expansion, serialization,
	// Import reader construction, V1 decoding and row filtering below.
	reads := 0
	transport := mockey.Mock((*milvusstorage.RemoteChunkManager).MultiRead).To(func(_ *milvusstorage.RemoteChunkManager, ctx context.Context, keys []string) ([][]byte, error) {
		require.Equal(t, []string{key}, keys, "the downloader must receive an object key, not a URI")
		reads++
		return localCM.MultiRead(ctx, []string{deltaPath})
	}).Build()
	defer transport.UnPatch()
	snapshot := snapshotImportTestData(datapb.SnapshotLayout_SnapshotLayoutReferenced)
	data := []*datapb.SegmentDescription{{PartitionId: 10, ChannelName: "source", ManifestPath: manifest}}
	deltas := []*datapb.SegmentDescription{{
		PartitionId: 10, ChannelName: "source", StorageVersion: milvusstorage.StorageV1,
		Deltalogs: []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{LogPath: "s3://source/" + key}, {LogPath: key}}}},
	}}
	files := []*internalpb.ImportFile{{}}
	require.NoError(t, attachSnapshotImportSources(ctx, cm, "s3://source/"+strings.TrimPrefix(root, "/")+"/snapshots/1/metadata/2.json",
		snapshot, data, deltas, files, cfg))
	require.Equal(t, []string{key}, files[0].SnapshotSource.LegacyL0Deltalogs, "URI/key aliases must be deduplicated")
	encoded, err := proto.Marshal(files[0])
	require.NoError(t, err)
	for phase := 0; phase < 2; phase++ {
		file := &internalpb.ImportFile{}
		require.NoError(t, proto.Unmarshal(encoded, file))
		reader, err := importutilv2.NewReader(ctx, cm, schema, file, snapshotImportTestOptions(), 1024, cfg, 1024)
		require.NoError(t, err)
		batch, err := reader.Read()
		require.NoError(t, err)
		require.Equal(t, 1, batch.GetRowNum())
		require.EqualValues(t, 2, batch.Data[100].GetRow(0))
		_, err = reader.Read()
		require.ErrorIs(t, err, io.EOF)
		reader.Close()
	}
	require.Equal(t, 2, reads)
}

func TestSnapshotImportL0URIValidation(t *testing.T) {
	for _, tc := range []struct {
		name, uri, metadataURI string
	}{
		{"bucket", "s3://other/root/delta", "s3://source/root/snapshots/1/metadata/2.json"},
		{"endpoint", "minio://other:9000/source/root/delta", "minio://localhost:9000/source/root/snapshots/1/metadata/2.json"},
		{"root", "s3://source/outside/delta", "s3://source/root/snapshots/1/metadata/2.json"},
		{"signed_uri", "s3://source/root/delta?signature=secret", "s3://source/root/snapshots/1/metadata/2.json"},
		{"decoder_alias", "s3://source/root/delta", "s3://source/root/snapshots/1/metadata/2.json"},
		{"packed_alias", "s3://source/root/delta", "s3://source/root/snapshots/1/metadata/2.json"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := snapshotImportTestData(datapb.SnapshotLayout_SnapshotLayoutReferenced)
			data := snapshot.Segments[:1]
			data[0].ChannelName = "source"
			deltas := []*datapb.SegmentDescription{{
				PartitionId: 10, ChannelName: "source", StorageVersion: milvusstorage.StorageV1,
				Deltalogs: []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{LogPath: "root/delta"}, {LogPath: tc.uri}}}},
			}}
			if tc.name == "decoder_alias" {
				deltas = append(deltas, &datapb.SegmentDescription{
					PartitionId: 10, ChannelName: "source", StorageVersion: milvusstorage.StorageV3,
					ManifestPath: packed.MarshalManifestPath("root/l0", 1),
				})
			}
			packedPaths := []string{"root/delta"}
			if tc.name == "packed_alias" {
				deltas = []*datapb.SegmentDescription{{
					PartitionId: 10, ChannelName: "source", StorageVersion: milvusstorage.StorageV3,
					ManifestPath: packed.MarshalManifestPath("root/l0", 1),
				}}
				packedPaths = append(packedPaths, tc.uri)
			}
			patch := mockey.Mock(packed.GetDeltaLogPathsFromManifest).Return(packedPaths, nil).Build()
			defer patch.UnPatch()
			cm := milvusstorage.NewRemoteChunkManagerForTesting(nil, "source", "root")
			files := []*internalpb.ImportFile{{}}
			err := attachSnapshotImportSources(context.Background(), cm, tc.metadataURI, snapshot, data, deltas, files, nil)
			if tc.name == "packed_alias" {
				require.NoError(t, err)
				require.Equal(t, []string{"root/delta"}, files[0].SnapshotSource.ManifestL0Deltalogs)
				return
			}
			require.Error(t, err, "normalization must not erase an invalid storage identity or decoder conflict")
			if tc.name == "decoder_alias" {
				require.ErrorIs(t, err, merr.ErrDataIntegrity)
			}
		})
	}
}

func TestExpandSnapshotImportExternalStorage(t *testing.T) {
	for _, layout := range []datapb.SnapshotLayout{datapb.SnapshotLayout_SnapshotLayoutReferenced, datapb.SnapshotLayout_SnapshotLayoutSelfContained} {
		t.Run(layout.String(), func(t *testing.T) {
			snapshot := snapshotImportTestData(layout)
			snapshot.SnapshotInfo.SegmentCommitTimestampsPreserved = true
			for _, segment := range snapshot.Segments {
				segment.ChannelName = "source"
			}
			snapshot.Segments = append(snapshot.Segments, &datapb.SegmentDescription{
				SegmentId: 99, PartitionId: common.AllPartitionsID, ChannelName: "source",
				SegmentLevel: datapb.SegmentLevel_L0, StorageVersion: milvusstorage.StorageV3,
				ManifestPath: packed.MarshalManifestPath("root/l0", 1),
			})
			foreignCM := milvusstorage.NewRemoteChunkManagerForTesting(nil, "source", "root")
			foreignCfg := &indexpb.StorageConfig{BucketName: "source"}
			resolve := mockey.Mock(snapshotstorage.ResolveSnapshotReadStorage).Return(&snapshotstorage.ResolvedForeignStorage{
				ForeignCM: foreignCM, ForeignStorageConfig: foreignCfg,
			}, nil).Build()
			defer resolve.UnPatch()
			read := mockey.Mock(snapshotstorage.NewSnapshotReader).When(func(cm milvusstorage.ChunkManager) bool {
				require.Same(t, foreignCM, cm)
				return true
			}).Return(&snapshotstorage.SnapshotReader{}).Build()
			defer read.UnPatch()
			readSnapshot := mockey.Mock((*snapshotstorage.SnapshotReader).ReadSnapshot).Return(snapshot, nil).Build()
			defer readSnapshot.UnPatch()
			validate := mockey.Mock(snapshotstorage.ValidateExternalSnapshotDataFiles).To(func(ctx context.Context,
				cm milvusstorage.ChunkManager, uri string, data *snapshotstorage.SnapshotData, cfg *indexpb.StorageConfig,
			) error {
				require.Same(t, foreignCM, cm)
				require.Same(t, foreignCfg, cfg)
				return nil
			}).Build()
			defer validate.UnPatch()
			delta := mockey.Mock(packed.GetDeltaLogPathsFromManifest).To(func(path string, cfg *indexpb.StorageConfig) ([]string, error) {
				require.Same(t, foreignCfg, cfg)
				return []string{"root/files/l0/delete"}, nil
			}).Build()
			defer delta.UnPatch()
			options := append(snapshotImportTestOptions(), &commonpb.KeyValuePair{Key: importutilv2.ExternalSpec, Value: `{"extfs":{"region":"us-east-1"}}`})
			files, _, err := expandSnapshotImportFiles(context.Background(), nil, milvusstorage.NewLocalChunkManager(), snapshotImportTestSchema(),
				[]*internalpb.ImportFile{{Paths: []string{"s3://source/root/snapshots/1/metadata/2.json"}}}, options)
			require.NoError(t, err)
			for _, file := range files {
				require.Empty(t, file.Paths)
				require.EqualValues(t, 2, file.SnapshotSource.Version)
				require.Equal(t, []string{"root/files/l0/delete"}, file.SnapshotSource.ManifestL0Deltalogs)
			}
			// External jobs without L0 still need a typed descriptor so older
			// workers cannot ignore the source credentials and read target keys.
			snapshot.Segments = snapshot.Segments[:len(snapshot.Segments)-1]
			files, _, err = expandSnapshotImportFiles(context.Background(), nil, milvusstorage.NewLocalChunkManager(), snapshotImportTestSchema(),
				[]*internalpb.ImportFile{{Paths: []string{"s3://source/root/snapshots/1/metadata/2.json"}}}, options)
			require.NoError(t, err)
			for _, file := range files {
				require.Empty(t, file.Paths)
				require.EqualValues(t, 2, file.SnapshotSource.Version)
				require.Empty(t, file.SnapshotSource.ManifestL0Deltalogs)
			}
			mappedOptions := append(append(importutilv2.Options(nil), options...), &commonpb.KeyValuePair{Key: importutilv2.PartitionMapping, Value: `{"source_partition":"X"}`})
			mapped, _, err := expandSnapshotImportFiles(context.Background(), []int64{200}, milvusstorage.NewLocalChunkManager(), snapshotImportTestSchema(),
				[]*internalpb.ImportFile{{Paths: []string{"s3://source/root/snapshots/1/metadata/2.json"}}}, mappedOptions)
			require.NoError(t, err)
			mappedOptions = append(mappedOptions, &commonpb.KeyValuePair{Key: importutilv2.SnapshotSourceURI, Value: "s3://source/root/snapshots/1/metadata/2.json"})
			require.NoError(t, importutilv2.ValidateSnapshotImportFiles(mapped, mappedOptions))
			for _, file := range mapped {
				require.EqualValues(t, 4, file.GetSnapshotSource().GetVersion())
				require.EqualValues(t, 200, file.GetSnapshotSource().GetTargetPartitionId())
			}
			resolve.UnPatch()
			failure := mockey.Mock(snapshotstorage.ResolveSnapshotReadStorage).Return(nil, merr.ErrIoPermissionDenied).Build()
			defer failure.UnPatch()
			files, _, err = expandSnapshotImportFiles(context.Background(), nil, milvusstorage.NewLocalChunkManager(), snapshotImportTestSchema(),
				[]*internalpb.ImportFile{{Paths: []string{"s3://source/root/snapshots/1/metadata/2.json"}}}, options)
			require.ErrorIs(t, err, merr.ErrIoPermissionDenied)
			require.Nil(t, files)
		})
	}
}

func TestSnapshotImportExternalOptionsLifecycle(t *testing.T) {
	options := append(snapshotImportTestOptions(),
		&commonpb.KeyValuePair{Key: importutilv2.ExternalSpec, Value: `{"extfs":{"access_key_id":"source-key","access_key_value":"source-secret"}}`},
		&commonpb.KeyValuePair{Key: importutilv2.SnapshotSourceURI, Value: "s3://source/root/snapshots/1/metadata/2.json"})
	for _, terminal := range []internalpb.ImportJobState{internalpb.ImportJobState_Completed, internalpb.ImportJobState_Failed} {
		job := &importJob{ImportJob: &datapb.ImportJob{Options: options}}
		for _, state := range []internalpb.ImportJobState{internalpb.ImportJobState_PreImporting, internalpb.ImportJobState_Importing, internalpb.ImportJobState_Uncommitted} {
			UpdateJobState(state)(job)
			encoded, err := proto.Marshal(job.ImportJob)
			require.NoError(t, err)
			restored := &datapb.ImportJob{}
			require.NoError(t, proto.Unmarshal(encoded, restored))
			require.True(t, proto.Equal(job.ImportJob, restored))
			require.Len(t, restored.Options, len(options))
			for i, option := range options {
				require.True(t, proto.Equal(option, restored.Options[i]), "source options must survive restarts through both phases")
			}
		}
		UpdateJobState(terminal)(job)
		require.False(t, importutilv2.HasExternalSource(job.Options))
		require.NotContains(t, job.String(), "source-secret")
		require.NotContains(t, job.String(), importutilv2.SnapshotSourceURI)
		require.True(t, importutilv2.HasExternalSource(options), "cleanup must not mutate another request's options")
	}
}

func patchSnapshotImportInstance(t *testing.T) {
	t.Helper()
	patch := mockey.Mock(snapshotstorage.InstanceConfigFromParamtable).To(func(*paramtable.ComponentParam) *objectstorage.Config {
		return &objectstorage.Config{Address: "localhost:9000", BucketName: "source", CloudProvider: "aws"}
	}).Build()
	t.Cleanup(func() { patch.UnPatch() })
}

func TestExpandSnapshotImportMetadataAdmission(t *testing.T) {
	patchSnapshotImportInstance(t)
	readCalls := 0
	patch := mockey.Mock((*snapshotstorage.SnapshotReader).ReadSnapshot).To(func(*snapshotstorage.SnapshotReader, context.Context, string, bool) (*snapshotstorage.SnapshotData, error) {
		readCalls++
		return nil, merr.ErrIoKeyNotFound
	}).Build()
	defer patch.UnPatch()
	for _, uri := range []string{
		"root/snapshots/1/metadata/2.json",
		"s3:///root/snapshots/1/metadata/2.json",
		"s3://other/root/snapshots/1/metadata/2.json",
		"minio://other:9000/source/root/snapshots/1/metadata/2.json",
		"gs://source/root/snapshots/1/metadata/2.json",
	} {
		t.Run(uri, func(t *testing.T) {
			files, _, err := expandSnapshotImportFiles(context.Background(), nil, milvusstorage.NewLocalChunkManager(), snapshotImportTestSchema(),
				[]*internalpb.ImportFile{{Paths: []string{uri}}}, snapshotImportTestOptions())
			require.ErrorIs(t, err, merr.ErrParameterInvalid)
			require.Nil(t, files)
			require.Zero(t, readCalls, "invalid source identity must fail before metadata IO")
		})
	}
	options := append(snapshotImportTestOptions(), &commonpb.KeyValuePair{Key: importutilv2.ExternalSpec, Value: `{"extfs":{"region":"us-east-1"}}`})
	_, _, err := expandSnapshotImportFiles(context.Background(), nil, milvusstorage.NewLocalChunkManager(), snapshotImportTestSchema(),
		[]*internalpb.ImportFile{{Paths: []string{"root/snapshots/1/metadata/2.json"}}}, options)
	require.ErrorIs(t, err, merr.ErrParameterInvalid, "extfs must not permit bare metadata keys either")
	require.Zero(t, readCalls)

	_, _, err = expandSnapshotImportFiles(context.Background(), nil, milvusstorage.NewLocalChunkManager(), snapshotImportTestSchema(),
		[]*internalpb.ImportFile{{Paths: []string{"minio://localhost:9000/source/root/snapshots/1/metadata/2.json"}}}, snapshotImportTestOptions())
	require.ErrorIs(t, err, merr.ErrIoKeyNotFound, "valid admission must preserve the actual IO failure")
	require.Equal(t, 1, readCalls)
}

func TestSnapshotPartitionMappingResolution(t *testing.T) {
	source := map[string]int64{"A": 10, "B": 20, "C": 30}
	for _, tc := range []struct {
		name, mapping string
		ids           []int64
		key           bool
		want          map[int64]int64
		wantErr       error
	}{
		{"absent", "", nil, false, nil, nil},
		{"mapped", `{"C":"Z","A":"X","B":"Y"}`, []int64{300, 100, 200}, false, map[int64]int64{10: 300, 20: 100, 30: 200}, nil},
		{"merge", `{"A":"X","B":"X","C":"X"}`, []int64{300}, false, map[int64]int64{10: 300, 20: 300, 30: 300}, nil},
		{"malformed", `{`, nil, false, nil, merr.ErrImportFailed},
		{"missing_source", `{"A":"X"}`, []int64{300}, false, nil, merr.ErrImportFailed},
		{"unknown_source", `{"A":"X","B":"X","D":"X"}`, []int64{300}, false, nil, merr.ErrImportFailed},
		{"missing_ids", `{"A":"X","B":"X","C":"X"}`, nil, false, nil, merr.ErrServiceInternal},
		{"fewer_ids", `{"A":"X","B":"Y","C":"Z"}`, []int64{300, 100}, false, nil, merr.ErrServiceInternal},
		{"extra_ids", `{"A":"X","B":"X","C":"X"}`, []int64{300, 100}, false, nil, merr.ErrServiceInternal},
		{"invalid_id", `{"A":"X","B":"X","C":"X"}`, []int64{0}, false, nil, merr.ErrServiceInternal},
		{"invalid_last_id", `{"A":"X","B":"Y","C":"Z"}`, []int64{300, 100, -1}, false, nil, merr.ErrServiceInternal},
		{"partition_key", `{"A":"X","B":"X","C":"X"}`, []int64{300}, true, nil, merr.ErrImportFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := snapshotImportTestOptions()
			if tc.mapping != "" {
				opts = append(opts, &commonpb.KeyValuePair{Key: importutilv2.PartitionMapping, Value: tc.mapping})
			}
			schema := snapshotImportTestSchema()
			schema.Fields[0].IsPartitionKey = tc.key
			got, err := resolveSnapshotPartitionMapping(source, tc.ids, schema, opts)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				require.Nil(t, got, "invalid metadata must not produce a partial partition mapping")
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
	for _, source := range []map[string]int64{{"A": 0}, {"A": 10, "B": 10}} {
		opts := snapshotImportTestOptions()
		mapping := `{"A":"X"}`
		if len(source) == 2 {
			mapping = `{"A":"X","B":"X"}`
		}
		opts = append(opts, &commonpb.KeyValuePair{Key: importutilv2.PartitionMapping, Value: mapping})
		_, err := resolveSnapshotPartitionMapping(source, []int64{100}, snapshotImportTestSchema(), opts)
		require.ErrorIs(t, err, merr.ErrImportFailed)
	}
}

func TestExpandSnapshotImportPartitionMapping(t *testing.T) {
	patchSnapshotImportInstance(t)
	for _, layout := range []datapb.SnapshotLayout{datapb.SnapshotLayout_SnapshotLayoutReferenced, datapb.SnapshotLayout_SnapshotLayoutSelfContained} {
		for _, l0 := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/l0=%v", layout, l0), func(t *testing.T) {
				snapshot := snapshotImportTestData(layout)
				snapshot.Collection.Partitions = map[string]int64{"A": 10, "B": 20, "empty": 30}
				snapshot.Segments[0].PartitionId = 20
				for _, segment := range snapshot.Segments {
					segment.ChannelName = "source"
					segment.CommitTimestamp = 100
				}
				if l0 {
					for _, id := range []int64{10, 20, common.AllPartitionsID} {
						snapshot.Segments = append(snapshot.Segments, &datapb.SegmentDescription{
							PartitionId: id, ChannelName: "source", SegmentLevel: datapb.SegmentLevel_L0, StorageVersion: milvusstorage.StorageV1,
							Deltalogs: []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{LogPath: fmt.Sprintf("root/files/delete-%d", id)}}}},
						})
					}
				}
				read := mockey.Mock((*snapshotstorage.SnapshotReader).ReadSnapshot).Return(snapshot, nil).Build()
				defer read.UnPatch()
				validate := mockey.Mock(snapshotstorage.ValidateExternalSnapshotDataFiles).Return(nil).Build()
				defer validate.UnPatch()
				opts := append(snapshotImportTestOptions(), &commonpb.KeyValuePair{Key: importutilv2.PartitionMapping, Value: `{"A":"X","B":"Y","empty":"Z"}`})
				files, normalized, err := expandSnapshotImportFiles(context.Background(), []int64{200, 100, 300}, milvusstorage.NewLocalChunkManager(), snapshotImportTestSchema(),
					[]*internalpb.ImportFile{{Paths: []string{"s3://source/root/snapshots/1/metadata/2.json"}}}, opts)
				require.NoError(t, err)
				require.NoError(t, importutilv2.ValidateSnapshotImportFiles(files, normalized))
				require.Len(t, files, 2)
				for i, target := range []int64{200, 100} {
					source := files[i].GetSnapshotSource()
					require.EqualValues(t, 3, source.GetVersion())
					require.Equal(t, target, source.GetTargetPartitionId())
					require.EqualValues(t, 100, source.GetSourceCommitTimestamp())
					require.Empty(t, files[i].GetPaths())
					if l0 {
						require.ElementsMatch(t, []string{fmt.Sprintf("root/files/delete-%d", (i+1)*10), "root/files/delete--1"}, source.GetLegacyL0Deltalogs())
					}
				}
				// A data segment outside the declared source partition inventory
				// must fail instead of falling back to a default destination.
				snapshot.Segments[0].PartitionId = 999
				_, _, err = expandSnapshotImportFiles(context.Background(), []int64{200, 100, 300}, milvusstorage.NewLocalChunkManager(), snapshotImportTestSchema(),
					[]*internalpb.ImportFile{{Paths: []string{"s3://source/root/snapshots/1/metadata/2.json"}}}, opts)
				require.ErrorContains(t, err, "unknown source partition")
			})
		}
	}
}

func TestExpandSnapshotImportMultiplePartitionsL0(t *testing.T) {
	patchSnapshotImportInstance(t)
	for _, layout := range []datapb.SnapshotLayout{datapb.SnapshotLayout_SnapshotLayoutReferenced, datapb.SnapshotLayout_SnapshotLayoutSelfContained} {
		t.Run(layout.String(), func(t *testing.T) {
			snapshot := snapshotImportTestData(layout)
			snapshot.SnapshotInfo.SegmentCommitTimestampsPreserved = true
			snapshot.Collection.Partitions["second"] = 20
			snapshot.Segments[0].ChannelName = "source"
			snapshot.Segments[1].ChannelName = "source"
			snapshot.Segments[0].PartitionId = 10
			snapshot.Segments[1].PartitionId = 20
			for _, delta := range []struct {
				partition     int64
				channel, path string
			}{
				{10, "source", "root/files/local-10"},
				{20, "source", "root/files/local-20"},
				{common.AllPartitionsID, "source", "root/files/global"},
				{common.AllPartitionsID, "other", "root/files/unrelated"},
			} {
				snapshot.Segments = append(snapshot.Segments, &datapb.SegmentDescription{
					PartitionId: delta.partition, ChannelName: delta.channel, SegmentLevel: datapb.SegmentLevel_L0,
					StorageVersion: milvusstorage.StorageV1,
					Deltalogs:      []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{LogPath: delta.path}}}},
				})
			}
			read := mockey.Mock((*snapshotstorage.SnapshotReader).ReadSnapshot).Return(snapshot, nil).Build()
			defer read.UnPatch()
			validate := mockey.Mock(snapshotstorage.ValidateExternalSnapshotDataFiles).Return(nil).Build()
			defer validate.UnPatch()
			files, _, err := expandSnapshotImportFiles(context.Background(), nil, milvusstorage.NewLocalChunkManager(), snapshotImportTestSchema(),
				[]*internalpb.ImportFile{{Paths: []string{"s3://source/root/snapshots/1/metadata/2.json"}}}, snapshotImportTestOptions())
			require.NoError(t, err)
			require.Len(t, files, 2)
			byManifest := make(map[string][]string)
			for _, file := range files {
				byManifest[file.GetSnapshotSource().GetManifestPath()] = file.GetSnapshotSource().GetLegacyL0Deltalogs()
			}
			require.ElementsMatch(t, []string{"root/files/local-10", "root/files/global"}, byManifest[snapshot.Segments[0].GetManifestPath()])
			require.ElementsMatch(t, []string{"root/files/local-20", "root/files/global"}, byManifest[snapshot.Segments[1].GetManifestPath()])
		})
	}
}

func TestExpandSnapshotImportL0(t *testing.T) {
	patchSnapshotImportInstance(t)
	for _, layout := range []datapb.SnapshotLayout{datapb.SnapshotLayout_SnapshotLayoutReferenced, datapb.SnapshotLayout_SnapshotLayoutSelfContained} {
		for _, mode := range []string{"legacy_v1", "legacy_v2", "packed_v3", "empty_marker", "unrelated", "unrelated_channel", "missing_provenance", "missing_channel", "bad_version", "bad_manifest", "read_failure", "outside_root", "oversized"} {
			t.Run(layout.String()+"/"+mode, func(t *testing.T) {
				snapshot := snapshotImportTestData(layout)
				snapshot.SnapshotInfo.SegmentCommitTimestampsPreserved = mode != "missing_provenance"
				snapshot.Segments[0].ChannelName = "other"
				snapshot.Segments[1].ChannelName = "source"
				snapshot.Segments[1].CommitTimestamp = 300
				deltaPath := "root/files/l0/delete"
				delta := &datapb.SegmentDescription{
					SegmentId: 30, PartitionId: common.AllPartitionsID, ChannelName: "source",
					SegmentLevel: datapb.SegmentLevel_L0, StorageVersion: milvusstorage.StorageV2,
					Deltalogs: []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{LogPath: deltaPath, EntriesNum: 0}, {LogPath: deltaPath}}}},
				}
				switch mode {
				case "legacy_v1":
					delta.StorageVersion = milvusstorage.StorageV1
				case "unrelated":
					delta.PartitionId = 999
					snapshot.SnapshotInfo.SegmentCommitTimestampsPreserved = false
					snapshot.Segments[1].CommitTimestamp = 0
				case "unrelated_channel":
					delta.ChannelName = "unselected_channel"
					snapshot.SnapshotInfo.SegmentCommitTimestampsPreserved = false
					snapshot.Segments[1].CommitTimestamp = 0
				case "missing_channel":
					delta.ChannelName = ""
				case "bad_version":
					delta.StorageVersion = 99
				case "packed_v3", "empty_marker", "bad_manifest", "read_failure", "outside_root", "oversized":
					delta.StorageVersion = milvusstorage.StorageV3
					delta.ManifestPath = packed.MarshalManifestPath("root/files/l0", 1)
					if mode == "bad_manifest" {
						delta.ManifestPath = packed.MarshalManifestPath("root/files/l0", packed.ManifestLatest)
					}
				}
				snapshot.Segments = append(snapshot.Segments, delta)
				readPatch := mockey.Mock((*snapshotstorage.SnapshotReader).ReadSnapshot).Return(snapshot, nil).Build()
				defer readPatch.UnPatch()
				validationPatch := mockey.Mock(snapshotstorage.ValidateExternalSnapshotDataFiles).Return(nil).Build()
				defer validationPatch.UnPatch()
				paths := []string{deltaPath, deltaPath}
				var readErr error
				switch mode {
				case "empty_marker", "unrelated", "unrelated_channel":
					paths = nil
				case "read_failure":
					readErr = merr.ErrIoKeyNotFound
				case "outside_root":
					paths = []string{"escape/delete"}
				case "oversized":
					paths = []string{"root/files/" + strings.Repeat("x", importutilv2.SnapshotSourcePlanMaxBytes)}
				}
				deltaPatch := mockey.Mock(packed.GetDeltaLogPathsFromManifest).Return(paths, readErr).Build()
				defer deltaPatch.UnPatch()
				files, _, err := expandSnapshotImportFiles(context.Background(), nil, milvusstorage.NewLocalChunkManager(), snapshotImportTestSchema(),
					[]*internalpb.ImportFile{{Paths: []string{"s3://source/root/snapshots/1/metadata/2.json"}}}, snapshotImportTestOptions())
				switch mode {
				case "missing_provenance", "missing_channel", "bad_version", "bad_manifest", "read_failure", "outside_root", "oversized":
					require.Error(t, err)
					require.Nil(t, files)
					if readErr != nil {
						require.ErrorIs(t, err, readErr)
					}
				case "unrelated", "unrelated_channel":
					require.NoError(t, err)
					require.Nil(t, files[0].SnapshotSource)
					require.Len(t, files[0].Paths, 1)
				default:
					require.NoError(t, err)
					require.Len(t, files, 2)
					require.Empty(t, files[0].Paths)
					require.EqualValues(t, 300, files[0].SnapshotSource.SourceCommitTimestamp)
					if mode == "packed_v3" {
						require.Equal(t, []string{deltaPath}, files[0].SnapshotSource.ManifestL0Deltalogs)
					} else if mode != "empty_marker" {
						require.Equal(t, []string{deltaPath}, files[0].SnapshotSource.LegacyL0Deltalogs)
					}
					require.NotNil(t, files[1].SnapshotSource, "job-wide activation survives an empty per-file L0 list")
					require.Empty(t, files[1].SnapshotSource.LegacyL0Deltalogs)
					require.Empty(t, files[1].SnapshotSource.ManifestL0Deltalogs)
				}
			})
		}
	}
}

func TestSnapshotImportL0PlanErrors(t *testing.T) {
	for _, mode := range []string{"large_manifest", "canceled", "empty_delete", "conflicting_decoder", "invalid_path"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			snapshot := snapshotImportTestData(datapb.SnapshotLayout_SnapshotLayoutReferenced)
			data := snapshot.Segments[:1]
			data[0].ChannelName = "source"
			paths := []string{"root/delta"}
			deltas := []*datapb.SegmentDescription{{
				ChannelName: "source", PartitionId: 10, StorageVersion: milvusstorage.StorageV1,
				Deltalogs: []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{LogPath: paths[0]}}}},
			}}
			switch mode {
			case "large_manifest":
				data[0].ManifestPath = packed.MarshalManifestPath(strings.Repeat("x", importutilv2.SnapshotSourcePlanMaxBytes), 1)
			case "canceled":
				cancel()
			case "empty_delete":
				deltas[0].Deltalogs[0].Binlogs[0].LogPath = ""
			case "invalid_path":
				deltas[0].Deltalogs[0].Binlogs[0].LogPath = "../escape"
			case "conflicting_decoder":
				deltas = append(deltas, &datapb.SegmentDescription{
					ChannelName: "source", PartitionId: 10, StorageVersion: milvusstorage.StorageV3,
					ManifestPath: packed.MarshalManifestPath("root/l0", 1),
				})
			}
			patch := mockey.Mock(packed.GetDeltaLogPathsFromManifest).Return(paths, nil).Build()
			defer patch.UnPatch()
			err := attachSnapshotImportSources(ctx, milvusstorage.NewLocalChunkManager(), "root/snapshots/1/metadata/2.json",
				snapshot, data, deltas, []*internalpb.ImportFile{{}}, nil)
			require.Error(t, err)
			if mode == "canceled" {
				require.ErrorIs(t, err, context.Canceled)
			}
		})
	}
}

func TestSnapshotPartitionMappingFileGroups(t *testing.T) {
	for _, tc := range []struct {
		name   string
		groups [][]int64
		want   [][]int64
	}{
		{"ordinary", [][]int64{{0, 0}, {0}}, [][]int64{{0, 0}, {0}}},
		{"empty_group", [][]int64{nil, {}}, [][]int64{nil, {}}},
		{"many_to_one", [][]int64{{10, 20, 10, 30, 20}}, [][]int64{{10, 10}, {20, 20}, {30}}},
		{"preserve_limits", [][]int64{{10, 20}, {20, 10}}, [][]int64{{10}, {20}, {20}, {10}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, splitImportFileGroupsByPartition(tc.groups, func(id int64) int64 { return id }))
		})
	}
}

func TestSnapshotPartitionMappingTasksAndWAL(t *testing.T) {
	paramtable.Init()
	opts := append(snapshotImportTestOptions(), &commonpb.KeyValuePair{Key: importutilv2.PartitionMapping, Value: `{"A":"X","B":"Y","C":"Z"}`})
	sources := make([]*internalpb.SnapshotImportSource, 0, 3)
	msgFiles := make([]*msgpb.ImportFile, 0, 3)
	for i, partition := range []int64{300, 100, 200} {
		sources = append(sources, &internalpb.SnapshotImportSource{
			Version: 3, TargetPartitionId: partition,
			ManifestPath: packed.MarshalManifestPath(fmt.Sprintf("root/data/%d", i), 7), SourceCommitTimestamp: 100,
		})
		msgFiles = append(msgFiles, &msgpb.ImportFile{Id: int64(i + 1)})
	}
	wal := message.NewImportMessageBuilderV1().WithHeader(&message.ImportMessageHeader{SnapshotSources: sources}).
		WithBody(&msgpb.ImportMsg{PartitionIDs: []int64{300, 100, 200}, Files: msgFiles}).WithBroadcast([]string{"target_v1"}).MustBuildBroadcast()
	decoded, err := message.AsBroadcastImportMessageV1(message.NewBroadcastMutableMessageBeforeAppend(wal.Payload(), wal.Properties().ToRawMap()))
	require.NoError(t, err)
	files, err := bindSnapshotImportSources(decoded.MustBody().GetFiles(), decoded.Header().GetSnapshotSources(), opts)
	require.NoError(t, err)
	msgID := walimplstest.NewTestMessageID(1)
	immutable := wal.WithBroadcastID(1).SplitIntoMutableMessage()[0].WithTimeTick(100).WithLastConfirmed(msgID).IntoImmutableMessage(msgID)
	replicated := message.MustNewReplicateMessage("source-cluster", immutable.IntoImmutableMessageProto())
	replicated.OverwriteReplicateVChannel("replica_v1", []string{"replica_v1"})
	replica := message.MustAsMutableImportMessageV1(replicated)
	for i, source := range replica.Header().GetSnapshotSources() {
		require.True(t, proto.Equal(sources[i], source))
	}
	encoded, err := proto.Marshal(&datapb.ImportJob{
		JobID: 1, CollectionID: 2, Files: files, Options: opts,
		PartitionIDs: []int64{300, 100, 200}, Vchannels: []string{"target_v1"}, Schema: snapshotImportTestSchema(), DataTs: 100, AutoCommit: false,
	})
	require.NoError(t, err)
	reloaded := &datapb.ImportJob{}
	require.NoError(t, proto.Unmarshal(encoded, reloaded))
	job := &importJob{ImportJob: reloaded}
	type mappingAllocator struct{ allocator.Allocator }
	alloc := &mappingAllocator{}
	next := int64(1000)
	allocate := mockey.Mock((*mappingAllocator).AllocN).To(func(_ *mappingAllocator, n int64) (int64, int64, error) {
		start := next
		next += n
		return start, next, nil
	}).Build()
	defer allocate.UnPatch()
	type mappingMeta struct{ ImportMeta }
	tm := &mappingMeta{}
	getJob := mockey.Mock((*mappingMeta).GetJob).Return(job).Build()
	defer getJob.UnPatch()
	preTasks, err := NewPreImportTasks([][]*internalpb.ImportFile{reloaded.Files}, job, alloc, tm)
	require.NoError(t, err)
	require.Len(t, preTasks, 3)
	stats := make([]*datapb.ImportFileStats, 0, 3)
	for _, task := range preTasks {
		req := AssemblePreImportRequest(task, job)
		require.Len(t, req.GetPartitionIDs(), 1)
		require.NoError(t, importutilv2.ValidateSnapshotTaskPartitions(req.GetImportFiles(), req.GetPartitionIDs()))
		for _, file := range req.GetImportFiles() {
			stats = append(stats, &datapb.ImportFileStats{
				ImportFile: file, TotalRows: 8, TotalMemorySize: 8,
				HashedStats: map[string]*datapb.PartitionImportStats{"target_v1": {
					PartitionRows: map[int64]int64{req.PartitionIDs[0]: 8}, PartitionDataSize: map[int64]int64{req.PartitionIDs[0]: 8},
				}},
			})
		}
	}
	segments := make(map[int64]*SegmentInfo)
	allocateSegment := mockey.Mock(AllocImportSegment).To(func(_ context.Context, _ allocator.Allocator, _ *meta,
		_, _, collection, partition int64, channel string, _ uint64, _ datapb.SegmentLevel, _ int64,
	) (*SegmentInfo, error) {
		next++
		segment := NewSegmentInfo(&datapb.SegmentInfo{ID: next, CollectionID: collection, PartitionID: partition, InsertChannel: channel})
		segments[next] = segment
		return segment, nil
	}).Build()
	defer allocateSegment.UnPatch()
	getSegment := mockey.Mock((*meta).GetSegment).To(func(_ *meta, _ context.Context, id int64) *SegmentInfo { return segments[id] }).Build()
	defer getSegment.UnPatch()
	// Size regrouping can pack files together; task construction must still
	// split them by destination in both phases, including after catalog reload.
	groups := RegroupImportFiles(job, stats, 1024)
	importTasks, err := NewImportTasks(groups, job, alloc, &meta{}, tm, 1024)
	require.NoError(t, err)
	require.Len(t, importTasks, 3)
	for _, task := range importTasks {
		req, err := AssembleImportRequest(task, job, &meta{}, alloc)
		require.NoError(t, err)
		require.Len(t, req.GetPartitionIDs(), 1)
		require.NoError(t, importutilv2.ValidateSnapshotTaskPartitions(req.GetFiles(), req.GetPartitionIDs()))
		for _, segment := range req.GetRequestSegments() {
			require.Equal(t, req.PartitionIDs[0], segment.GetPartitionID())
		}
		require.EqualValues(t, job.GetJobID(), req.GetJobID())
	}
	require.False(t, reloaded.GetAutoCommit(), "partition mapping must not change manual commit mode")
	// Ordinary/partition-key jobs still pass the whole job partition list.
	legacy := &preImportTask{}
	legacy.task.Store(&datapb.PreImportTask{})
	require.Equal(t, job.PartitionIDs, importTaskPartitionIDs(legacy, job))
}

func TestSnapshotImportWALBinding(t *testing.T) {
	sources := []*internalpb.SnapshotImportSource{
		{Version: 1, ManifestPath: packed.MarshalManifestPath("root/data/1", 7), SourceCommitTimestamp: 300, LegacyL0Deltalogs: []string{"root/delta"}},
		{Version: 1, ManifestPath: packed.MarshalManifestPath("root/data/2", 8)},
	}
	wal := message.NewImportMessageBuilderV1().WithHeader(&message.ImportMessageHeader{SnapshotSources: sources}).
		WithBody(&msgpb.ImportMsg{PartitionIDs: []int64{20, 10}, Files: []*msgpb.ImportFile{{Id: 1}, {Id: 2}}}).WithBroadcast([]string{"target_v1"}).MustBuildBroadcast()
	decoded, err := message.AsBroadcastImportMessageV1(message.NewBroadcastMutableMessageBeforeAppend(wal.Payload(), wal.Properties().ToRawMap()))
	require.NoError(t, err)
	files, err := bindSnapshotImportSources(decoded.MustBody().GetFiles(), decoded.Header().GetSnapshotSources(), snapshotImportTestOptions())
	require.NoError(t, err)
	encoded, err := proto.Marshal(&datapb.ImportJob{Files: files, PartitionIDs: decoded.MustBody().GetPartitionIDs()})
	require.NoError(t, err)
	job := &datapb.ImportJob{}
	require.NoError(t, proto.Unmarshal(encoded, job))
	require.Equal(t, []int64{20, 10}, job.GetPartitionIDs())
	for i, file := range job.Files {
		require.True(t, proto.Equal(sources[i], file.SnapshotSource))
		require.Empty(t, file.Paths)
	}
	// CDC rewrites the transport channel, not the source channel association
	// captured in the descriptors. Exercise the actual message transformation.
	msgID := walimplstest.NewTestMessageID(1)
	immutable := wal.WithBroadcastID(1).SplitIntoMutableMessage()[0].WithTimeTick(100).
		WithLastConfirmed(msgID).IntoImmutableMessage(msgID)
	replicated := message.MustNewReplicateMessage("source-cluster", immutable.IntoImmutableMessageProto())
	replicated.OverwriteReplicateVChannel("replica_v1", []string{"replica_v1"})
	replicatedImport := message.MustAsMutableImportMessageV1(replicated)
	for i, source := range replicatedImport.Header().GetSnapshotSources() {
		require.True(t, proto.Equal(sources[i], source))
	}
	require.Len(t, replicatedImport.Header().GetSnapshotSources(), len(sources))
	job.Vchannels = []string{"target_v1"}
	groups := RegroupImportFiles(&importJob{ImportJob: job}, []*datapb.ImportFileStats{
		{ImportFile: job.Files[0], TotalMemorySize: 20}, {ImportFile: job.Files[1], TotalMemorySize: 10},
	}, 1)
	require.Len(t, groups, 2)
	for _, group := range groups {
		file := group[0].GetImportFile()
		require.True(t, proto.Equal(sources[file.Id-1], file.GetSnapshotSource()))
	}
	for _, headers := range [][]*internalpb.SnapshotImportSource{nil, sources[:1], {sources[0], nil}, {{Version: 99}, sources[1]}} {
		_, err := bindSnapshotImportSources(decoded.MustBody().GetFiles(), headers, snapshotImportTestOptions())
		require.Error(t, err, "lost or malformed descriptors must fail closed")
	}
	_, err = bindSnapshotImportSources([]*msgpb.ImportFile{{Paths: []string{"legacy"}}, {}}, sources, snapshotImportTestOptions())
	require.Error(t, err)
}

func TestSnapshotImportMessageAdmission(t *testing.T) {
	for _, backend := range []message.WALName{message.WALNamePulsar, message.WALNameKafka, message.WALNameWoodpecker, message.WALNameRocksmq} {
		t.Run(backend.String(), func(t *testing.T) {
			selection := mockey.Mock(streamingutil.MustSelectWALName).Return(backend).Build()
			defer selection.UnPatch()
			for _, size := range []int{10, 512 * 1024} {
				msg := message.NewImportMessageBuilderV1().WithHeader(&message.ImportMessageHeader{
					SnapshotSources: []*internalpb.SnapshotImportSource{{Version: 1, ManifestPath: strings.Repeat("x", size)}},
				}).WithBody(&msgpb.ImportMsg{}).WithBroadcast([]string{"v1"}).MustBuildBroadcast()
				err := validateSnapshotImportMessageSize(msg)
				if size == 10 {
					require.NoError(t, err)
				} else {
					require.ErrorIs(t, err, merr.ErrImportFailed)
				}
			}
		})
	}
}

func TestSnapshotImportAckFailure(t *testing.T) {
	type ackHandler struct{ Handler }
	type ackAllocator struct{ allocator.Allocator }
	type ackMeta struct{ ImportMeta }
	handlerPatch := mockey.Mock((*ackHandler).GetCollection).Return(&collectionInfo{ID: 1, VChannelNames: []string{"target_v1"}}, nil).Build()
	defer handlerPatch.UnPatch()
	allocatorPatch := mockey.Mock((*ackAllocator).AllocN).Return(int64(100), int64(110), nil).Build()
	defer allocatorPatch.UnPatch()
	var saved ImportJob
	metaPatch := mockey.Mock((*ackMeta).AddJob).To(func(_ *ackMeta, _ context.Context, job ImportJob) error {
		saved = job
		return nil
	}).Build()
	defer metaPatch.UnPatch()
	server := &Server{handler: &ackHandler{}, allocator: &ackAllocator{}, importMeta: &ackMeta{}}
	server.stateCode.Store(commonpb.StateCode_Healthy)
	for _, mode := range []string{"valid", "lost", "unknown", "nil_file", "binding_failure", "bad_timeout"} {
		t.Run(mode, func(t *testing.T) {
			req := &internalpb.ImportRequestInternal{
				JobID: 12, CollectionID: 1, PartitionIDs: []int64{10},
				Options: snapshotImportTestOptions(), Files: []*internalpb.ImportFile{{SnapshotSource: &internalpb.SnapshotImportSource{
					Version: 1, ManifestPath: packed.MarshalManifestPath("root/segment", 1), SourceCommitTimestamp: 300,
				}}},
			}
			var bindingErr error
			switch mode {
			case "lost":
				req.Files[0].SnapshotSource = nil
			case "unknown":
				req.Files[0].SnapshotSource.Version = 99
			case "nil_file":
				req.Files[0] = nil
			case "binding_failure":
				bindingErr = merr.WrapErrServiceInternalMsg("descriptor cardinality mismatch")
			case "bad_timeout":
				req.Options = append(req.Options, &commonpb.KeyValuePair{Key: "timeout", Value: "invalid"})
			}
			resp, err := server.createImportJobFromAck(context.Background(), req, bindingErr)
			require.NoError(t, merr.CheckRPCCall(resp, err), "a malformed durable source must not retry the ACK forever")
			require.Equal(t, "12", resp.JobID)
			require.NotNil(t, saved)
			if mode == "valid" {
				require.Equal(t, internalpb.ImportJobState_Pending, saved.GetState())
				require.EqualValues(t, 300, saved.GetFiles()[0].GetSnapshotSource().GetSourceCommitTimestamp())
			} else {
				require.Equal(t, internalpb.ImportJobState_Failed, saved.GetState())
				require.NotEmpty(t, saved.GetReason())
				require.NotEqual(t, uint64(math.MaxUint64), saved.GetCleanupTs())
			}
		})
	}
}

func snapshotImportTestSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name: "target",
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "pk",
				DataType:     schemapb.DataType_Int64,
				IsPrimaryKey: true,
			},
			{
				FieldID:  101,
				Name:     "text",
				DataType: schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.MaxLengthKey, Value: "1024"},
				},
			},
		},
	}
}

func snapshotImportTestData(layout datapb.SnapshotLayout) *snapshotstorage.SnapshotData {
	sourceSchema := proto.Clone(snapshotImportTestSchema()).(*schemapb.CollectionSchema)
	sourceSchema.Name = "source"
	return &snapshotstorage.SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{Id: 2, CollectionId: 1, SegmentCommitTimestampsPreserved: true},
		Collection: &datapb.CollectionDescription{
			Schema:     sourceSchema,
			Partitions: map[string]int64{"source_partition": 10},
		},
		Segments: []*datapb.SegmentDescription{
			{
				SegmentId:      20,
				PartitionId:    10,
				SegmentLevel:   datapb.SegmentLevel_L1,
				StorageVersion: milvusstorage.StorageV3,
				ManifestPath:   packed.MarshalManifestPath("source/segment/20", 8),
			},
			{
				SegmentId:      10,
				PartitionId:    10,
				SegmentLevel:   datapb.SegmentLevel_L1,
				StorageVersion: milvusstorage.StorageV3,
				ManifestPath:   packed.MarshalManifestPath("source/segment/10", 7),
			},
		},
		Layout: layout,
	}
}

func snapshotImportTestOptions() importutilv2.Options {
	return importutilv2.Options{
		{Key: importutilv2.BackupFlag, Value: "true"},
		{Key: importutilv2.SourceType, Value: importutilv2.SourceTypeSnapshot},
	}
}

func snapshotImportTestEZK(ezID int64) string {
	return base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf(`{"ez_id":%d}`, ezID)))
}

func TestNormalizeSnapshotImportEncryption(t *testing.T) {
	for _, tc := range []struct {
		name       string
		ezID       string
		ezk        string
		text       bool
		errMessage string
	}{
		{name: "plaintext without key"},
		{name: "plaintext ignores valid key", ezk: snapshotImportTestEZK(10)},
		{name: "plaintext ignores malformed key", ezk: "not-base64"},
		{name: "plaintext text ignores malformed key", ezk: "not-base64", text: true},
		{name: "encrypted keeps matching key", ezID: "10", ezk: snapshotImportTestEZK(10)},
		{name: "invalid source encryption zone", ezID: "invalid", errMessage: "invalid"},
		{name: "encrypted requires key", ezID: "10", errMessage: "requires ezk"},
		{name: "encrypted rejects malformed key", ezID: "10", ezk: "not-base64", errMessage: "invalid ezk"},
		{name: "encrypted rejects mismatched key", ezID: "10", ezk: snapshotImportTestEZK(11), errMessage: "source requires zone 10"},
		{name: "encrypted text remains unsupported", ezID: "10", ezk: snapshotImportTestEZK(10), text: true, errMessage: "TEXT/LOB"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			schema := snapshotImportTestSchema()
			schema.Properties = []*commonpb.KeyValuePair{{Key: "unrelated", Value: "retained"}}
			if tc.ezID != "" {
				schema.Properties = append(schema.Properties, &commonpb.KeyValuePair{Key: common.EncryptionEzIDKey, Value: tc.ezID})
			}
			if tc.text {
				schema.Fields = append(schema.Fields, &schemapb.FieldSchema{FieldID: 102, Name: "body", DataType: schemapb.DataType_Text})
			}
			options := append(snapshotImportTestOptions(), &commonpb.KeyValuePair{Key: importutilv2.AutoCommitKey, Value: "false"})
			if tc.ezk != "" {
				options = append(options, &commonpb.KeyValuePair{Key: importutilv2.EZK, Value: tc.ezk})
			}
			original := make(importutilv2.Options, len(options))
			for i, option := range options {
				original[i] = proto.Clone(option).(*commonpb.KeyValuePair)
			}
			normalized, err := normalizeSnapshotImportEncryption(schema, options)
			assert.Equal(t, original, options, "request options must not be mutated")
			if tc.errMessage != "" {
				require.ErrorContains(t, err, tc.errMessage)
				require.Nil(t, normalized)
				return
			}
			require.NoError(t, err)
			if tc.ezID != "" {
				assert.Equal(t, original, normalized)
				return
			}
			assert.Equal(t, original[:3], normalized)
			// Replacing an entry in the normalized slice must not overwrite the
			// caller's backing array, even when there was no EZK to remove.
			normalized[0] = &commonpb.KeyValuePair{Key: "new-key", Value: "new-value"}
			assert.Equal(t, original, options)
		})
	}
}

func TestExpandSnapshotImportFiles(t *testing.T) {
	patchSnapshotImportInstance(t)
	ctx := context.Background()
	cm := milvusstorage.NewLocalChunkManager()
	metadataPath := "s3://source/root/snapshots/1/metadata/2.json"
	targetSchema := snapshotImportTestSchema()

	t.Run("legacy source passes through", func(t *testing.T) {
		files := []*internalpb.ImportFile{{Paths: []string{"legacy/path"}}}
		options := importutilv2.Options{{Key: importutilv2.BackupFlag, Value: "true"}, {Key: importutilv2.EZK, Value: "legacy-key"}}
		result, normalized, err := expandSnapshotImportFiles(ctx, nil, cm, targetSchema, files, options)
		assert.NoError(t, err)
		assert.Same(t, files[0], result[0])
		assert.Equal(t, options, normalized, "legacy backup options must remain unchanged")
	})

	for _, layout := range []datapb.SnapshotLayout{
		datapb.SnapshotLayout_SnapshotLayoutReferenced,
		datapb.SnapshotLayout_SnapshotLayoutSelfContained,
	} {
		t.Run(layout.String(), func(t *testing.T) {
			snapshot := snapshotImportTestData(layout)
			readPatch := mockey.Mock((*snapshotstorage.SnapshotReader).ReadSnapshot).
				Return(snapshot, nil).Build()
			defer readPatch.UnPatch()
			validatePatch := mockey.Mock(snapshotstorage.ValidateExternalSnapshotDataFiles).
				Return(nil).Build()
			defer validatePatch.UnPatch()

			result, _, err := expandSnapshotImportFiles(
				ctx,
				nil,
				cm,
				targetSchema,
				[]*internalpb.ImportFile{{Paths: []string{metadataPath}}},
				snapshotImportTestOptions(),
			)
			require.NoError(t, err)
			require.Len(t, result, 2)
			assert.Equal(t, snapshot.Segments[1].GetManifestPath(), result[0].GetPaths()[0])
			assert.Equal(t, snapshot.Segments[0].GetManifestPath(), result[1].GetPaths()[0])
		})
	}

	t.Run("implicit unique partition", func(t *testing.T) {
		snapshot := snapshotImportTestData(datapb.SnapshotLayout_SnapshotLayoutReferenced)
		readPatch := mockey.Mock((*snapshotstorage.SnapshotReader).ReadSnapshot).
			Return(snapshot, nil).Build()
		defer readPatch.UnPatch()
		validatePatch := mockey.Mock(snapshotstorage.ValidateExternalSnapshotDataFiles).
			Return(nil).Build()
		defer validatePatch.UnPatch()

		result, _, err := expandSnapshotImportFiles(
			ctx,
			nil,
			cm,
			targetSchema,
			[]*internalpb.ImportFile{{Paths: []string{metadataPath}}},
			snapshotImportTestOptions(),
		)
		assert.NoError(t, err)
		assert.Len(t, result, 2)
	})

	t.Run("all source partitions are expanded", func(t *testing.T) {
		snapshot := snapshotImportTestData(datapb.SnapshotLayout_SnapshotLayoutReferenced)
		snapshot.Collection.Partitions["second"] = 20
		snapshot.Segments = append(snapshot.Segments, &datapb.SegmentDescription{
			SegmentId:      30,
			PartitionId:    20,
			SegmentLevel:   datapb.SegmentLevel_L1,
			StorageVersion: milvusstorage.StorageV3,
			ManifestPath:   packed.MarshalManifestPath("source/segment/30", 9),
		})
		readPatch := mockey.Mock((*snapshotstorage.SnapshotReader).ReadSnapshot).
			Return(snapshot, nil).Build()
		defer readPatch.UnPatch()

		validatePatch := mockey.Mock(snapshotstorage.ValidateExternalSnapshotDataFiles).Return(nil).Build()
		defer validatePatch.UnPatch()
		files, _, err := expandSnapshotImportFiles(
			ctx,
			nil,
			cm,
			targetSchema,
			[]*internalpb.ImportFile{{Paths: []string{metadataPath}}},
			snapshotImportTestOptions(),
		)
		require.NoError(t, err)
		require.Len(t, files, 3)
		require.Equal(t, snapshot.Segments[2].GetManifestPath(), files[2].GetPaths()[0])
	})

	t.Run("encrypted source with matching ezk", func(t *testing.T) {
		snapshot := snapshotImportTestData(datapb.SnapshotLayout_SnapshotLayoutReferenced)
		snapshot.Collection.Schema.Properties = []*commonpb.KeyValuePair{
			{Key: common.EncryptionEzIDKey, Value: "10"},
		}
		readPatch := mockey.Mock((*snapshotstorage.SnapshotReader).ReadSnapshot).
			Return(snapshot, nil).Build()
		defer readPatch.UnPatch()
		validatePatch := mockey.Mock(snapshotstorage.ValidateExternalSnapshotDataFiles).
			Return(nil).Build()
		defer validatePatch.UnPatch()

		options := append(snapshotImportTestOptions(), &commonpb.KeyValuePair{
			Key: importutilv2.EZK, Value: snapshotImportTestEZK(10),
		})
		result, normalized, err := expandSnapshotImportFiles(
			ctx,
			nil,
			cm,
			targetSchema,
			[]*internalpb.ImportFile{{Paths: []string{metadataPath}}},
			options,
		)
		require.NoError(t, err)
		assert.Len(t, result, 2)
		assert.Equal(t, options, normalized, "encrypted sources must retain their source EZK")
	})

	t.Run("encrypted source requires matching ezk", func(t *testing.T) {
		snapshot := snapshotImportTestData(datapb.SnapshotLayout_SnapshotLayoutReferenced)
		snapshot.Collection.Schema.Properties = []*commonpb.KeyValuePair{
			{Key: common.EncryptionEzIDKey, Value: "10"},
		}
		readPatch := mockey.Mock((*snapshotstorage.SnapshotReader).ReadSnapshot).
			Return(snapshot, nil).Build()
		defer readPatch.UnPatch()

		_, _, err := expandSnapshotImportFiles(
			ctx,
			nil,
			cm,
			targetSchema,
			[]*internalpb.ImportFile{{Paths: []string{metadataPath}}},
			snapshotImportTestOptions(),
		)
		assert.ErrorIs(t, err, merr.ErrImportFailed)
		assert.ErrorContains(t, err, "requires ezk")

		options := append(snapshotImportTestOptions(), &commonpb.KeyValuePair{
			Key: importutilv2.EZK, Value: snapshotImportTestEZK(11),
		})
		_, _, err = expandSnapshotImportFiles(
			ctx,
			nil,
			cm,
			targetSchema,
			[]*internalpb.ImportFile{{Paths: []string{metadataPath}}},
			options,
		)
		assert.ErrorIs(t, err, merr.ErrImportFailed)
		assert.ErrorContains(t, err, "source requires zone 10")
	})

	t.Run("plaintext text source ignores malformed ezk", func(t *testing.T) {
		snapshot := snapshotImportTestData(datapb.SnapshotLayout_SnapshotLayoutReferenced)
		snapshot.Collection.Schema.Fields = append(snapshot.Collection.Schema.Fields, &schemapb.FieldSchema{
			FieldID: 102, Name: "body", DataType: schemapb.DataType_Text,
		})
		readPatch := mockey.Mock((*snapshotstorage.SnapshotReader).ReadSnapshot).
			Return(snapshot, nil).Build()
		defer readPatch.UnPatch()
		validatePatch := mockey.Mock(snapshotstorage.ValidateExternalSnapshotDataFiles).Return(nil).Build()
		defer validatePatch.UnPatch()

		options := append(snapshotImportTestOptions(), &commonpb.KeyValuePair{
			Key: importutilv2.EZK, Value: "not-base64",
		})
		files, normalized, err := expandSnapshotImportFiles(
			ctx,
			nil,
			cm,
			snapshot.Collection.Schema,
			[]*internalpb.ImportFile{{Paths: []string{metadataPath}}},
			options,
		)
		require.NoError(t, err)
		require.Len(t, files, 2)
		assert.Equal(t, snapshotImportTestOptions(), normalized)
		ezk, err := importutilv2.GetEZK(options)
		require.NoError(t, err)
		assert.Equal(t, "not-base64", ezk, "normalization must not mutate caller-owned options")
	})

	t.Run("encrypted text source is unsupported", func(t *testing.T) {
		snapshot := snapshotImportTestData(datapb.SnapshotLayout_SnapshotLayoutReferenced)
		textField := &schemapb.FieldSchema{
			FieldID: 102, Name: "body", DataType: schemapb.DataType_Text,
		}
		snapshot.Collection.Schema.Fields = append(snapshot.Collection.Schema.Fields, textField)
		snapshot.Collection.Schema.Properties = []*commonpb.KeyValuePair{
			{Key: common.EncryptionEzIDKey, Value: "10"},
		}
		readPatch := mockey.Mock((*snapshotstorage.SnapshotReader).ReadSnapshot).
			Return(snapshot, nil).Build()
		defer readPatch.UnPatch()

		targetWithText := proto.Clone(targetSchema).(*schemapb.CollectionSchema)
		targetWithText.Fields = append(targetWithText.Fields, proto.Clone(textField).(*schemapb.FieldSchema))
		options := append(snapshotImportTestOptions(), &commonpb.KeyValuePair{
			Key: importutilv2.EZK, Value: snapshotImportTestEZK(10),
		})
		_, _, err := expandSnapshotImportFiles(
			ctx,
			nil,
			cm,
			targetWithText,
			[]*internalpb.ImportFile{{Paths: []string{metadataPath}}},
			options,
		)
		assert.ErrorIs(t, err, merr.ErrOperationNotSupported)
		assert.ErrorContains(t, err, "TEXT/LOB")
	})

	tests := []struct {
		name     string
		mutate   func(*snapshotstorage.SnapshotData)
		expected error
		message  string
	}{
		{
			name: "empty snapshot",
			mutate: func(snapshot *snapshotstorage.SnapshotData) {
				snapshot.Segments = nil
			},
			expected: merr.ErrImportFailed,
			message:  "snapshot contains no data segments",
		},
		{
			name: "invalid data in another source partition is not skipped",
			mutate: func(snapshot *snapshotstorage.SnapshotData) {
				snapshot.Collection.Partitions["second"] = 20
				snapshot.Segments = append(snapshot.Segments, &datapb.SegmentDescription{
					SegmentId: 30, PartitionId: 20, SegmentLevel: datapb.SegmentLevel_L1, StorageVersion: milvusstorage.StorageV2,
				})
			},
			expected: merr.ErrOperationNotSupported,
			message:  "only supports StorageV3",
		},
		{
			name: "reject l0 without channel identity",
			mutate: func(snapshot *snapshotstorage.SnapshotData) {
				snapshot.Segments[0].SegmentLevel = datapb.SegmentLevel_L0
			},
			expected: merr.ErrImportFailed,
			message:  "requires source channel identity",
		},
		{
			name: "reject non v3",
			mutate: func(snapshot *snapshotstorage.SnapshotData) {
				snapshot.Segments[0].StorageVersion = milvusstorage.StorageV2
			},
			expected: merr.ErrOperationNotSupported,
			message:  "only supports StorageV3",
		},
		{
			name: "reject latest manifest",
			mutate: func(snapshot *snapshotstorage.SnapshotData) {
				snapshot.Segments[0].ManifestPath = packed.MarshalManifestPath("source/segment/20", packed.ManifestLatest)
			},
			expected: merr.ErrImportFailed,
			message:  "exact manifest version",
		},
		{
			name: "reject external collection",
			mutate: func(snapshot *snapshotstorage.SnapshotData) {
				snapshot.Collection.Schema.Fields[0].ExternalField = "pk"
			},
			expected: merr.ErrOperationNotSupported,
			message:  "external collection snapshots",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			snapshot := snapshotImportTestData(datapb.SnapshotLayout_SnapshotLayoutReferenced)
			test.mutate(snapshot)
			readPatch := mockey.Mock((*snapshotstorage.SnapshotReader).ReadSnapshot).
				Return(snapshot, nil).Build()
			defer readPatch.UnPatch()

			_, _, err := expandSnapshotImportFiles(
				ctx,
				nil,
				cm,
				targetSchema,
				[]*internalpb.ImportFile{{Paths: []string{metadataPath}}},
				snapshotImportTestOptions(),
			)
			assert.ErrorIs(t, err, test.expected)
			assert.ErrorContains(t, err, test.message)
		})
	}
}

func TestValidateSnapshotImportSchema(t *testing.T) {
	target := snapshotImportTestSchema()
	source := proto.Clone(target).(*schemapb.CollectionSchema)
	source.Name = "different_collection"
	source.Description = "source-only description"
	// Snapshot metadata stores RootCoord's complete schema, including system
	// fields that Proxy intentionally omits from the target Import schema.
	source.Fields = append(source.Fields,
		&schemapb.FieldSchema{
			FieldID:  common.RowIDField,
			Name:     common.RowIDFieldName,
			DataType: schemapb.DataType_Int64,
		},
		&schemapb.FieldSchema{
			FieldID:  common.TimeStampField,
			Name:     common.TimeStampFieldName,
			DataType: schemapb.DataType_Int64,
		},
	)
	source.Fields[1].Description = "source field"
	source.Fields[1].IndexParams = []*commonpb.KeyValuePair{{Key: "index_type", Value: "TRIE"}}
	source.Fields[1].TypeParams = []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "1024"}}
	assert.NoError(t, validateSnapshotImportSchema(target, source))

	source.Fields[1].Nullable = true
	err := validateSnapshotImportSchema(target, source)
	assert.NoError(t, err, "backup import applies the target schema rather than requiring equality")
	source.Fields[0].AutoID = true
	source.Fields[1].IsPartitionKey = true
	source.Fields[1].IsClusteringKey = true
	source.Fields[1].DefaultValue = &schemapb.ValueField{Data: &schemapb.ValueField_StringData{StringData: "source"}}
	source.EnableDynamicField = true
	source.Fields = append(source.Fields, &schemapb.FieldSchema{FieldID: 102, Name: "source_only", DataType: schemapb.DataType_Int64})
	target.Fields = append(target.Fields, &schemapb.FieldSchema{FieldID: 103, Name: "target_only", DataType: schemapb.DataType_Int64, Nullable: true})
	assert.NoError(t, validateSnapshotImportSchema(target, source))
	assert.ErrorIs(t, validateSnapshotImportSchema(nil, source), merr.ErrImportSysFailed)
	assert.ErrorIs(t, validateSnapshotImportSchema(target, nil), merr.ErrImportFailed)

	externalTarget := proto.Clone(target).(*schemapb.CollectionSchema)
	externalTarget.Fields[0].ExternalField = "pk"
	err = validateSnapshotImportSchema(externalTarget, target)
	assert.ErrorIs(t, err, merr.ErrOperationNotSupported)
}

func TestValidateSnapshotImportSchema_FieldIdentity(t *testing.T) {
	target := snapshotImportTestSchema()
	target.Fields = append(target.Fields, &schemapb.FieldSchema{
		FieldID: 102, Name: "other_text", DataType: schemapb.DataType_VarChar,
		TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "1024"}},
	})
	target.StructArrayFields = []*schemapb.StructArrayFieldSchema{{
		FieldID: 103, Name: "items", Fields: []*schemapb.FieldSchema{
			{FieldID: 104, Name: "a", DataType: schemapb.DataType_Int64},
			{FieldID: 105, Name: "b", DataType: schemapb.DataType_Int64},
		},
	}}
	for _, mode := range []string{"reordered_schema", "renamed_field", "swapped_fields", "renamed_struct", "swapped_children"} {
		t.Run(mode, func(t *testing.T) {
			source := proto.Clone(target).(*schemapb.CollectionSchema)
			switch mode {
			case "reordered_schema":
				source.Fields[1], source.Fields[2] = source.Fields[2], source.Fields[1]
				source.StructArrayFields[0].Fields[0], source.StructArrayFields[0].Fields[1] = source.StructArrayFields[0].Fields[1], source.StructArrayFields[0].Fields[0]
			case "renamed_field":
				source.Fields[1].Name = "renamed"
			case "swapped_fields":
				source.Fields[1].Name, source.Fields[2].Name = source.Fields[2].Name, source.Fields[1].Name
			case "renamed_struct":
				source.StructArrayFields[0].Name = "other_items"
			case "swapped_children":
				children := source.StructArrayFields[0].Fields
				children[0].Name, children[1].Name = children[1].Name, children[0].Name
			}
			before := proto.Clone(source)
			err := validateSnapshotImportSchema(target, source)
			require.NoError(t, err, "backup import uses physical IDs, not source field names")
			require.True(t, proto.Equal(before, source), "validation must not mutate snapshot metadata")
		})
	}
}

func TestExpandSnapshotImportFiles_AdmissionFailures(t *testing.T) {
	patchSnapshotImportInstance(t)
	for _, mode := range []string{
		"invalid_options", "nil_cm", "empty_input", "bad_object_path", "nil_snapshot",
		"metadata_identity", "layout", "nil_schema", "nil_segment", "too_many_segments",
		"missing_manifest", "invalid_manifest", "duplicate_manifest", "invalid_files",
	} {
		t.Run(mode, func(t *testing.T) {
			snapshot := snapshotImportTestData(datapb.SnapshotLayout_SnapshotLayoutReferenced)
			var cm milvusstorage.ChunkManager = milvusstorage.NewLocalChunkManager()
			files := []*internalpb.ImportFile{{Paths: []string{"s3://source/root/snapshots/1/metadata/2.json"}}}
			options := snapshotImportTestOptions()
			var validationErr error
			wantErr := merr.ErrImportFailed
			switch mode {
			case "invalid_options":
				options = append(options, &commonpb.KeyValuePair{Key: "storage_version", Value: "3"})
			case "nil_cm":
				cm, wantErr = nil, merr.ErrServiceInternal
			case "empty_input":
				files = nil
			case "bad_object_path":
				patch := mockey.Mock(snapshotstorage.ValidateSnapshotObjectPathForBucket).Return(merr.ErrDataIntegrity).Build()
				defer patch.UnPatch()
				wantErr = merr.ErrDataIntegrity
			case "nil_snapshot":
				snapshot, wantErr = nil, merr.ErrImportSysFailed
			case "metadata_identity":
				snapshot.SnapshotInfo.Id = 999
				wantErr = merr.ErrDataIntegrity
			case "layout":
				snapshot.Layout = datapb.SnapshotLayout(999)
			case "nil_schema":
				snapshot.Collection.Schema = nil
			case "nil_segment":
				snapshot.Segments = append(snapshot.Segments, nil)
			case "too_many_segments":
				item := &paramtable.Get().DataCoordCfg.MaxFilesPerImportReq
				previous := item.SwapTempValue("1")
				defer item.SwapTempValue(previous)
			case "missing_manifest":
				snapshot.Segments[0].ManifestPath = ""
			case "invalid_manifest":
				snapshot.Segments[0].ManifestPath = "not-a-manifest"
			case "duplicate_manifest":
				snapshot.Segments[0].ManifestPath = snapshot.Segments[1].ManifestPath
			case "invalid_files":
				validationErr, wantErr = merr.ErrIoPermissionDenied, merr.ErrIoPermissionDenied
			}
			read := mockey.Mock((*snapshotstorage.SnapshotReader).ReadSnapshot).Return(snapshot, nil).Build()
			defer read.UnPatch()
			validate := mockey.Mock(snapshotstorage.ValidateExternalSnapshotDataFiles).Return(validationErr).Build()
			defer validate.UnPatch()
			result, _, err := expandSnapshotImportFiles(context.Background(), nil, cm, snapshotImportTestSchema(), files, options)
			require.ErrorIs(t, err, wantErr)
			require.Nil(t, result)
		})
	}
}

func TestExpandSnapshotImportWithoutL0_TimestampProvenance(t *testing.T) {
	patchSnapshotImportInstance(t)
	for _, tc := range []struct {
		name      string
		preserved bool
		commit    uint64
		deletes   bool
		readErr   error
		canceled  bool
		wantErr   error
	}{
		{name: "known_zero", preserved: true, deletes: true},
		{name: "known_commit", preserved: true, commit: 300, deletes: true},
		{name: "old_without_deletes"},
		{name: "old_known_commit_without_deletes", commit: 300},
		{name: "old_unknown_commit_with_deletes", deletes: true, wantErr: merr.ErrOperationNotSupported},
		{name: "old_nonzero_commit_with_deletes", commit: 300, deletes: true, wantErr: merr.ErrOperationNotSupported},
		{name: "manifest_io_error", readErr: merr.ErrIoPermissionDenied, wantErr: merr.ErrIoPermissionDenied},
		{name: "canceled", canceled: true, wantErr: context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := snapshotImportTestData(datapb.SnapshotLayout_SnapshotLayoutReferenced)
			snapshot.SnapshotInfo.SegmentCommitTimestampsPreserved = tc.preserved
			snapshot.Segments[1].CommitTimestamp = tc.commit
			read := mockey.Mock((*snapshotstorage.SnapshotReader).ReadSnapshot).Return(snapshot, nil).Build()
			defer read.UnPatch()
			validate := mockey.Mock(snapshotstorage.ValidateExternalSnapshotDataFiles).Return(nil).Build()
			defer validate.UnPatch()
			var paths []string
			if tc.deletes {
				paths = []string{"source/delta"}
			}
			delta := mockey.Mock(packed.GetDeltaLogPathsFromManifest).Return(paths, tc.readErr).Build()
			defer delta.UnPatch()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.canceled {
				cancel()
			}
			files, _, err := expandSnapshotImportFiles(ctx, nil, milvusstorage.NewLocalChunkManager(), snapshotImportTestSchema(),
				[]*internalpb.ImportFile{{Paths: []string{"s3://source/root/snapshots/1/metadata/2.json"}}}, snapshotImportTestOptions())
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				require.Nil(t, files)
				return
			}
			require.NoError(t, err)
			require.Len(t, files, 2)
			if tc.commit == 0 {
				require.Nil(t, files[0].SnapshotSource)
				require.Len(t, files[0].Paths, 1)
			} else {
				require.Empty(t, files[0].Paths)
				require.EqualValues(t, tc.commit, files[0].GetSnapshotSource().GetSourceCommitTimestamp())
				require.NotNil(t, files[1].SnapshotSource, "timestamp context uses uniform job-wide descriptors")
			}
		})
	}
}

func TestSnapshotImportManifestDeleteTimestamps(t *testing.T) {
	patchSnapshotImportInstance(t)
	ctx := context.Background()
	root := t.TempDir()
	cm := milvusstorage.NewLocalChunkManager()
	cfg := &indexpb.StorageConfig{StorageType: "local", RootPath: root}
	schema := typeutil.AppendSystemFields(&schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
	}})
	writer, err := milvusstorage.NewBinlogRecordWriter(ctx, 1, 10, 20, schema, idallocator.NewLocalAllocator(1, 1000), 1024*1024, 100,
		milvusstorage.WithVersion(milvusstorage.StorageV3), milvusstorage.WithStorageConfig(cfg),
		milvusstorage.WithColumnGroups([]storagecommon.ColumnGroup{{GroupID: 0, Columns: []int{0, 1, 2}, Fields: []int64{100, 0, 1}}}),
		milvusstorage.WithUploader(cm.MultiWrite))
	require.NoError(t, err)
	values := make([]*milvusstorage.Value, 0, 3)
	for pk := int64(1); pk <= 3; pk++ {
		values = append(values, &milvusstorage.Value{Value: map[int64]any{100: pk, 0: pk, 1: int64(100)}})
	}
	record, err := milvusstorage.ValueSerializer(values, schema)
	require.NoError(t, err)
	require.NoError(t, writer.Write(record))
	record.Release()
	require.NoError(t, writer.Close())
	_, _, _, manifest, _ := writer.GetLogs()
	base, _, err := packed.UnmarshalManifestPath(manifest)
	require.NoError(t, err)
	deltaPath := path.Join(base, "_delta", "1")
	deltaWriter, err := milvusstorage.NewDeltalogWriter(ctx, 1, 10, 20, 1, schemapb.DataType_Int64, deltaPath,
		milvusstorage.WithVersion(milvusstorage.StorageV2), milvusstorage.WithStorageConfig(cfg), milvusstorage.WithUploader(cm.MultiWrite))
	require.NoError(t, err)
	deltaRecord, _, _, err := milvusstorage.BuildDeleteRecord([]milvusstorage.PrimaryKey{
		milvusstorage.NewInt64PrimaryKey(1), milvusstorage.NewInt64PrimaryKey(2), milvusstorage.NewInt64PrimaryKey(3),
	}, []uint64{200, 300, 400})
	require.NoError(t, err)
	require.NoError(t, deltaWriter.Write(deltaRecord))
	deltaRecord.Release()
	require.NoError(t, deltaWriter.Close())
	manifest, err = packed.AddDeltaLogsToManifest(manifest, cfg, []packed.DeltaLogEntry{{Path: deltaPath, NumEntries: 3}})
	require.NoError(t, err)

	// Only the metadata transport/location is replaced. Expansion, descriptor
	// persistence, source selection, manifest/delta decoding and filtering are
	// real. The snapshot has no L0: all deletes already belong to the manifest.
	resolve := mockey.Mock(importutilv2.ResolveSnapshotImportStorage).Return(cm, cfg, nil).Build()
	defer resolve.UnPatch()
	validate := mockey.Mock(snapshotstorage.ValidateExternalSnapshotDataFiles).Return(nil).Build()
	defer validate.UnPatch()
	for _, layout := range []datapb.SnapshotLayout{datapb.SnapshotLayout_SnapshotLayoutReferenced, datapb.SnapshotLayout_SnapshotLayoutSelfContained} {
		for _, external := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/external=%t", layout, external), func(t *testing.T) {
				snapshot := snapshotImportTestData(layout)
				snapshot.Collection.Schema = schema
				snapshot.Segments = []*datapb.SegmentDescription{{
					SegmentId: 20, PartitionId: 10, SegmentLevel: datapb.SegmentLevel_L1,
					StorageVersion: milvusstorage.StorageV3, ManifestPath: manifest, CommitTimestamp: 300,
				}}
				read := mockey.Mock((*snapshotstorage.SnapshotReader).ReadSnapshot).Return(snapshot, nil).Build()
				defer read.UnPatch()
				uri := "s3://source/root/snapshots/1/metadata/2.json"
				options := snapshotImportTestOptions()
				if external {
					options = append(options, &commonpb.KeyValuePair{Key: importutilv2.ExternalSpec, Value: `{"extfs":{"region":"us-east-1"}}`})
				}
				files, _, err := expandSnapshotImportFiles(ctx, nil, cm, schema, []*internalpb.ImportFile{{Paths: []string{uri}}}, options)
				require.NoError(t, err)
				require.Len(t, files, 1)
				require.EqualValues(t, 300, files[0].GetSnapshotSource().GetSourceCommitTimestamp())
				require.Empty(t, files[0].GetSnapshotSource().GetLegacyL0Deltalogs())
				require.Empty(t, files[0].GetSnapshotSource().GetManifestL0Deltalogs())
				if external {
					require.EqualValues(t, 2, files[0].GetSnapshotSource().GetVersion())
					options = append(options, &commonpb.KeyValuePair{Key: importutilv2.SnapshotSourceURI, Value: uri})
				}
				bound, err := bindSnapshotImportSources([]*msgpb.ImportFile{{}}, []*internalpb.SnapshotImportSource{files[0].GetSnapshotSource()}, options)
				require.NoError(t, err)
				encoded, err := proto.Marshal(bound[0])
				require.NoError(t, err)
				for phase := 0; phase < 2; phase++ {
					file := &internalpb.ImportFile{}
					require.NoError(t, proto.Unmarshal(encoded, file))
					reader, err := importutilv2.NewReader(ctx, cm, schema, file, options, 1024, cfg, 1024)
					require.NoError(t, err)
					batch, err := reader.Read()
					require.NoError(t, err)
					require.Equal(t, []int64{1, 2}, batch.Data[100].(*milvusstorage.Int64FieldData).Data,
						"keep pre-commit and equal-commit deletes; apply post-commit deletes in both phases")
					_, err = reader.Read()
					require.ErrorIs(t, err, io.EOF)
					reader.Close()
				}
			})
		}
	}
}
