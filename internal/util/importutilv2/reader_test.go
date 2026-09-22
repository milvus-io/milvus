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

package importutilv2

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	mock "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	snapshotstorage "github.com/milvus-io/milvus/internal/snapshotio/storage"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/internal/util/importutilv2/binlog"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestSnapshotDeferredPathBoundary(t *testing.T) {
	cm := storage.NewRemoteChunkManagerForTesting(nil, "source", "root")
	for _, layout := range []string{"referenced", "self-contained"} {
		t.Run(layout, func(t *testing.T) {
			options := Options{
				{Key: BackupFlag, Value: "true"},
				{Key: SourceType, Value: SourceTypeSnapshot},
				{Key: SnapshotSourceURI, Value: "s3://source/root/snapshots/1/metadata/2.json"},
				{Key: SnapshotLayout, Value: layout},
			}
			validate, err := SnapshotPathValidator(options, cm)
			require.NoError(t, err)
			for _, path := range []string{"root/files/data", "s3://source/root/files/data"} {
				require.NoError(t, validate(path), path)
			}
			for _, path := range []string{"s3://other/root/files/data", "other/files/data", "root/../outside", ""} {
				require.Error(t, validate(path), path)
			}
			require.Error(t, ValidateSnapshotSourceRequest(options), "clients cannot forge deferred validation context")
		})
	}
	validate, err := SnapshotPathValidator(nil, cm)
	require.NoError(t, err)
	require.Nil(t, validate)
	_, err = SnapshotPathValidator(Options{{Key: SnapshotLayout, Value: "unknown"}}, cm)
	require.Error(t, err)
}

func TestSnapshotReaderFactoryEncryption(t *testing.T) {
	paramtable.Init()
	for _, mode := range []string{"plaintext", "encrypted", "disabled_plugin", "parse_error", "key_error", "ordinary_backup"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			options := Options{{Key: BackupFlag, Value: "true"}, {Key: SourceType, Value: SourceTypeSnapshot}}
			if mode != "plaintext" {
				options = append(options, &commonpb.KeyValuePair{Key: "ezk", Value: "source-key"})
			}
			file := &internalpb.ImportFile{Paths: []string{packed.MarshalManifestPath("root/segment", 1)}}
			var parseErr, keyErr error
			if mode == "parse_error" {
				parseErr = merr.ErrParameterInvalid
			}
			if mode == "key_error" {
				keyErr = merr.ErrIoPermissionDenied
			}
			plugin := &indexcgopb.StoragePluginContext{EncryptionZoneId: 10}
			if mode == "disabled_plugin" || mode == "plaintext" {
				plugin = nil
			}
			parse := mockey.Mock(hookutil.GetEzIDByImportEzk).Return(int64(10), parseErr).Build()
			defer parse.UnPatch()
			key := mockey.Mock(hookutil.GetCPluginContextByEzID).Return(plugin, keyErr).Build()
			defer key.UnPatch()
			open := mockey.Mock(binlog.NewStorageV3ManifestReader).Return(nil, io.EOF).Build()
			defer open.UnPatch()
			legacy := mockey.Mock(binlog.NewReader).When(func(_ context.Context, _ storage.ChunkManager,
				_ *schemapb.CollectionSchema, _ *indexpb.StorageConfig, _ int64, _ []string, _, _ uint64, _ int, ezk string,
			) bool {
				require.Equal(t, "source-key", ezk, "ordinary backup retains its reader's key resolution")
				return true
			}).Return(nil, io.EOF).Build()
			defer legacy.UnPatch()
			if mode == "ordinary_backup" {
				options = Options{options[0], options[2]}
			}
			factory := NewReaderFactory(ctx, nil, nil, options)
			for i := 0; i < 2; i++ {
				_, err := factory.NewReader(ctx, nil, file, 1024)
				switch mode {
				case "parse_error":
					require.ErrorIs(t, err, parseErr)
				case "key_error":
					require.ErrorIs(t, err, keyErr)
				default:
					require.ErrorIs(t, err, io.EOF)
				}
			}
			if mode == "plaintext" || mode == "encrypted" || mode == "disabled_plugin" {
				resolved, err := factory.storage()
				require.NoError(t, err)
				require.Equal(t, mode != "plaintext", resolved.encryption.Encrypted)
				require.Equal(t, plugin, resolved.encryption.PluginContext)
			}
			switch mode {
			case "plaintext", "ordinary_backup":
				require.Zero(t, parse.Times())
				require.Zero(t, key.Times())
			case "parse_error":
				require.EqualValues(t, 1, parse.Times())
				require.Zero(t, key.Times())
				require.Zero(t, open.Times())
			default:
				require.EqualValues(t, 1, parse.Times())
				require.EqualValues(t, 1, key.Times())
				if mode == "key_error" {
					require.Zero(t, open.Times())
				}
			}
		})
	}
}

func TestSnapshotReaderFactoryPathValidationError(t *testing.T) {
	ctx := context.Background()
	options := Options{{Key: BackupFlag, Value: "true"}, {Key: SourceType, Value: SourceTypeSnapshot}}
	factory := NewReaderFactory(ctx, nil, nil, options)
	validate := mockey.Mock(SnapshotPathValidator).Return(nil, merr.ErrParameterInvalid).Build()
	defer validate.UnPatch()
	file := &internalpb.ImportFile{Paths: []string{packed.MarshalManifestPath("root/segment", 1)}}
	_, err := factory.NewReader(ctx, nil, file, 1024)
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
}

func TestSnapshotReaderFactoryFailureAndRetry(t *testing.T) {
	paramtable.Init()
	options := Options{
		{Key: BackupFlag, Value: "true"},
		{Key: SourceType, Value: SourceTypeSnapshot},
		{Key: ExternalSpec, Value: `{"extfs":{"region":"us-east-1"}}`},
		{Key: SnapshotSourceURI, Value: "s3://source/root/snapshots/1/metadata/2.json"},
	}
	file := &internalpb.ImportFile{SnapshotSource: &internalpb.SnapshotImportSource{Version: 2, ManifestPath: packed.MarshalManifestPath("root/data/1", 7)}}
	ctx := context.Background()
	var resolutions int
	resolve := mockey.Mock(snapshotstorage.ResolveSnapshotReadStorage).To(func(context.Context, *objectstorage.Config, string, string) (*snapshotstorage.ResolvedForeignStorage, error) {
		resolutions++
		return nil, merr.ErrIoPermissionDenied
	}).Build()
	defer resolve.UnPatch()
	newFactory := func(taskCtx context.Context) *ReaderFactory { return NewReaderFactory(taskCtx, nil, nil, options) }
	factory := newFactory(ctx)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, err := factory.NewReader(canceled, nil, file, 1024)
	require.ErrorIs(t, err, context.Canceled)
	_, err = newFactory(canceled).NewReader(ctx, nil, file, 1024)
	require.ErrorIs(t, err, context.Canceled)
	broken := proto.Clone(file).(*internalpb.ImportFile)
	broken.SnapshotSource.Version = 1 // A foreign task must retain the external descriptor contract.
	_, err = factory.NewReader(ctx, nil, broken, 1024)
	require.Error(t, err)
	require.Zero(t, resolutions)
	for i := 0; i < 2; i++ {
		_, err = factory.NewReader(ctx, nil, file, 1024)
		require.ErrorIs(t, err, merr.ErrIoPermissionDenied)
	}
	require.Equal(t, 1, resolutions, "a failed attempt must not create a client per file")
	resolve.To(func(context.Context, *objectstorage.Config, string, string) (*snapshotstorage.ResolvedForeignStorage, error) {
		resolutions++
		return &snapshotstorage.ResolvedForeignStorage{ForeignCM: storage.NewLocalChunkManager()}, nil
	})
	reader := mockey.Mock(binlog.NewStorageV3ManifestReader).Return(nil, io.EOF).Build()
	defer reader.UnPatch()
	_, err = newFactory(ctx).NewReader(ctx, nil, file, 1024)
	require.ErrorIs(t, err, io.EOF, "a fresh attempt can recover after source access is restored")
	require.Equal(t, 2, resolutions, "redispatch must not retain the previous attempt's resolution error")
	_, err = factory.NewReader(ctx, nil, file, 1024)
	require.ErrorIs(t, err, merr.ErrIoPermissionDenied, "the original attempt keeps its own terminal failure")

	// Task cancellation reaches an in-flight initializer and every waiting file.
	taskCtx, stopTask := context.WithCancel(ctx)
	defer stopTask()
	entered := make(chan struct{})
	resolve.To(func(ctx context.Context, _ *objectstorage.Config, _, _ string) (*snapshotstorage.ResolvedForeignStorage, error) {
		close(entered)
		<-ctx.Done()
		return nil, ctx.Err()
	})
	factory = newFactory(taskCtx)
	results := make(chan error, 4)
	for i := 0; i < cap(results); i++ {
		go func() {
			_, err := factory.NewReader(ctx, nil, file, 1024)
			results <- err
		}()
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Error("source resolution did not start")
	}
	stopTask()
	for i := 0; i < cap(results); i++ {
		select {
		case err := <-results:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(5 * time.Second):
			t.Fatal("waiting reader did not observe task cancellation")
		}
	}
}

func TestSnapshotReaderFactoryReaderIsolation(t *testing.T) {
	type ownedRecordReader struct {
		storage.RecordReader
		ctx    context.Context
		record storage.Record
		read   bool
		closed bool
	}
	paramtable.Init()
	ctx := context.Background()
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true}}}
	options := Options{
		{Key: BackupFlag, Value: "true"},
		{Key: SourceType, Value: SourceTypeSnapshot},
		{Key: ExternalSpec, Value: `{"extfs":{"region":"us-east-1"}}`},
		{Key: SnapshotSourceURI, Value: "s3://source/root/snapshots/1/metadata/2.json"},
	}
	resolve := mockey.Mock(snapshotstorage.ResolveSnapshotReadStorage).Return(&snapshotstorage.ResolvedForeignStorage{ForeignCM: storage.NewLocalChunkManager(), ForeignStorageConfig: &indexpb.StorageConfig{StorageType: "local", RootPath: t.TempDir()}}, nil).Build()
	defer resolve.UnPatch()
	var owners []*ownedRecordReader
	manifest := mockey.Mock(storage.NewManifestRecordReader).To(func(ctx context.Context, _ string, _ *schemapb.CollectionSchema, _ ...storage.RwOption) (storage.RecordReader, error) {
		record, err := storage.ValueSerializer([]*storage.Value{{Value: map[int64]any{0: int64(1), 1: int64(100), 100: int64(42)}}}, typeutil.AppendSystemFields(schema))
		owner := &ownedRecordReader{ctx: ctx, record: record}
		owners = append(owners, owner)
		return owner, err
	}).Build()
	defer manifest.UnPatch()
	next := mockey.Mock((*ownedRecordReader).Next).To(func(r *ownedRecordReader) (storage.Record, error) {
		if err := r.ctx.Err(); err != nil {
			return nil, err
		}
		if r.read {
			return nil, io.EOF
		}
		r.read = true
		return r.record, nil
	}).Build()
	defer next.UnPatch()
	closeReader := mockey.Mock((*ownedRecordReader).Close).To(func(r *ownedRecordReader) error {
		r.closed = true
		r.record.Release()
		return nil
	}).Build()
	defer closeReader.UnPatch()
	fields := mockey.Mock(packed.GetManifestFieldIDs).Return(map[int64]struct{}{0: {}, 1: {}, 100: {}}, nil).Build()
	defer fields.UnPatch()
	fragments := mockey.Mock(packed.ReadFragmentsFromManifest).Return([]packed.Fragment(nil), nil).Build()
	defer fragments.UnPatch()
	lobs := mockey.Mock(packed.GetManifestLobFiles).Return([]packed.LobFileInfo(nil), nil).Build()
	defer lobs.UnPatch()
	deltas := mockey.Mock(packed.GetDeltaLogPathsFromManifest).Return([]string(nil), nil).Build()
	defer deltas.UnPatch()
	file := &internalpb.ImportFile{SnapshotSource: &internalpb.SnapshotImportSource{Version: 2, ManifestPath: packed.MarshalManifestPath("root/data/1", 7)}}
	factory := NewReaderFactory(ctx, nil, nil, options)
	firstCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	first, err := factory.NewReader(firstCtx, schema, file, 1024)
	require.NoError(t, err)
	second, err := factory.NewReader(ctx, schema, file, 1024)
	require.NoError(t, err)
	defer second.Close()
	cancel()
	_, err = first.Read()
	require.ErrorIs(t, err, context.Canceled)
	first.Close()
	require.True(t, owners[0].closed)
	require.False(t, owners[1].closed)
	data, err := second.Read()
	require.NoError(t, err)
	require.EqualValues(t, 42, data.Data[100].GetRow(0))
	_, err = second.Read()
	require.ErrorIs(t, err, io.EOF)
}

func TestImportNewReader_RejectsMixedSnapshotSource(t *testing.T) {
	paramtable.Init()
	snapshotOptions := Options{{Key: BackupFlag, Value: "true"}, {Key: SourceType, Value: SourceTypeSnapshot}}
	for _, tc := range []struct {
		name    string
		paths   []string
		options Options
	}{
		{"legacy_paths", []string{"manifest"}, snapshotOptions},
		{"ordinary_import", nil, nil},
		{"legacy_backup", nil, Options{{Key: BackupFlag, Value: "true"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, err := NewReader(context.Background(), nil, &schemapb.CollectionSchema{}, &internalpb.ImportFile{
				Paths: tc.paths, SnapshotSource: &internalpb.SnapshotImportSource{Version: 1},
			}, tc.options, 1024, nil)
			require.ErrorIs(t, err, merr.ErrServiceInternal)
			require.Nil(t, r)
		})
	}
}

func TestImportNewReader_RejectsInvalidSnapshotOptionsAndPaths(t *testing.T) {
	snapshotOptions := Options{{Key: BackupFlag, Value: "true"}, {Key: SourceType, Value: SourceTypeSnapshot}}
	for _, tc := range []struct {
		name    string
		paths   []string
		options Options
	}{
		{"unsupported_source", nil, Options{{Key: SourceType, Value: "unsupported"}}},
		{"missing_manifest", nil, snapshotOptions},
		{"multiple_manifests", []string{"first", "second"}, snapshotOptions},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, err := NewReader(context.Background(), nil, &schemapb.CollectionSchema{},
				&internalpb.ImportFile{Paths: tc.paths}, tc.options, 1024, nil)
			require.ErrorIs(t, err, merr.ErrImportFailed)
			require.Nil(t, r)
		})
	}
}

func TestImportNewReader_DependencyErrors(t *testing.T) {
	t.Run("storage_version", func(t *testing.T) {
		// Isolate the dispatch-time error forwarding from the earlier option
		// validation, which currently also parses the storage version.
		validationPatch := mockey.Mock(ValidateSnapshotSourceOptions).Return(nil).Build()
		defer validationPatch.UnPatch()
		r, err := NewReader(context.Background(), nil, &schemapb.CollectionSchema{}, &internalpb.ImportFile{},
			Options{{Key: BackupFlag, Value: "true"}, {Key: StorageVersion, Value: "invalid"}}, 1024, nil)
		require.ErrorIs(t, err, merr.ErrImportFailed)
		require.ErrorContains(t, err, "parse storage_version failed")
		require.Nil(t, r)
	})
	t.Run("csv_null_key", func(t *testing.T) {
		errNullKey := merr.WrapErrImportFailed("invalid null key")
		patch := mockey.Mock(GetCSVNullKey).Return("", errNullKey).Build()
		defer patch.UnPatch()
		r, err := NewReader(context.Background(), nil, &schemapb.CollectionSchema{},
			&internalpb.ImportFile{Paths: []string{"data.csv"}}, nil, 1024, nil)
		require.ErrorIs(t, err, errNullKey)
		require.Nil(t, r)
	})
	t.Run("unhandled_file_type", func(t *testing.T) {
		patch := mockey.Mock(GetFileType).Return(FileType(99), nil).Build()
		defer patch.UnPatch()
		r, err := NewReader(context.Background(), nil, &schemapb.CollectionSchema{}, &internalpb.ImportFile{}, nil, 1024, nil)
		require.ErrorIs(t, err, merr.ErrImportFailed)
		require.ErrorContains(t, err, "unexpected import file")
		require.Nil(t, r)
	})
}

func TestSnapshotPartitionMappingDescriptors(t *testing.T) {
	base := Options{{Key: BackupFlag, Value: "true"}, {Key: SourceType, Value: SourceTypeSnapshot}}
	mapping := &commonpb.KeyValuePair{Key: PartitionMapping, Value: `{"A":"X"}`}
	for _, version := range []uint32{3, 4} {
		opts := append(append(Options(nil), base...), mapping)
		if version == 4 {
			opts = append(opts, &commonpb.KeyValuePair{Key: ExternalSpec, Value: `{}`}, &commonpb.KeyValuePair{Key: SnapshotSourceURI, Value: "s3://source/root/metadata"})
		}
		file := &internalpb.ImportFile{SnapshotSource: &internalpb.SnapshotImportSource{Version: version, TargetPartitionId: 20, ManifestPath: packed.MarshalManifestPath("root/data", 7)}}
		files := []*internalpb.ImportFile{file}
		require.NoError(t, ValidateSnapshotImportFiles(files, opts))
		require.NoError(t, ValidateSnapshotTaskPartitions(files, []int64{20}))
		for _, partitions := range [][]int64{nil, {10}, {10, 20}} {
			require.ErrorIs(t, ValidateSnapshotTaskPartitions(files, partitions), merr.ErrServiceInternal)
		}
		require.Error(t, ValidateSnapshotImportFiles(files, base), "losing mapping must fail closed")
		file.SnapshotSource.TargetPartitionId = 0
		require.Error(t, ValidateSnapshotImportFiles(files, opts))
		file.SnapshotSource.TargetPartitionId = 20
		file.SnapshotSource.Version = version - 2
		require.Error(t, ValidateSnapshotImportFiles(files, opts), "old source version must not carry a mapping")
		file.SnapshotSource.Version = 99
		require.ErrorIs(t, ValidateSnapshotImportFiles(files, opts), merr.ErrServiceUnimplemented)
		require.Error(t, ValidateSnapshotImportFiles([]*internalpb.ImportFile{{Paths: []string{"root/data"}}}, opts))
	}
	require.NoError(t, ValidateSnapshotTaskPartitions([]*internalpb.ImportFile{{Paths: []string{"data.json"}}}, []int64{10, 20}))
}

func TestSnapshotImportPlanLimitWithoutL0(t *testing.T) {
	options := Options{{Key: BackupFlag, Value: "true"}, {Key: SourceType, Value: SourceTypeSnapshot}}
	for _, typed := range []bool{false, true} {
		t.Run(fmt.Sprint(typed), func(t *testing.T) {
			makeFile := func(length int) *internalpb.ImportFile {
				manifest := packed.MarshalManifestPath(strings.Repeat("x", length), 1)
				if typed {
					return &internalpb.ImportFile{SnapshotSource: &internalpb.SnapshotImportSource{Version: 1, ManifestPath: manifest}}
				}
				return &internalpb.ImportFile{Paths: []string{manifest}}
			}
			// Choose a serialized plan exactly at the limit, then cross it only
			// by allocating the file ID. The durable representation is the bound.
			file := makeFile(SnapshotSourcePlanMaxBytes)
			file = makeFile(2*SnapshotSourcePlanMaxBytes - proto.Size(file))
			require.Equal(t, SnapshotSourcePlanMaxBytes, proto.Size(file))
			require.NoError(t, ValidateSnapshotImportPlan([]*internalpb.ImportFile{file}, options))
			file.Id = 1 << 60
			require.ErrorContains(t, ValidateSnapshotImportPlan([]*internalpb.ImportFile{file}, options), "snapshot source plan exceeds 256 KiB")
			// The limit covers the whole plan, not just individual files.
			file = makeFile(SnapshotSourcePlanMaxBytes / 2)
			require.NoError(t, ValidateSnapshotImportPlan([]*internalpb.ImportFile{file}, options))
			require.ErrorContains(t, ValidateSnapshotImportPlan([]*internalpb.ImportFile{file, file}, options), "snapshot source plan exceeds 256 KiB")
		})
	}
}

func TestImportNewReader(t *testing.T) {
	ctx := context.Background()
	cm := mocks.NewChunkManager(t)
	cm.EXPECT().Reader(mock.Anything, mock.Anything).Return(nil, merr.WrapErrImportFailed("io error"))

	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "pk",
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
				AutoID:       false,
			},
		},
	}

	checkFunc := func(name string, req *internalpb.ImportFile, options []*commonpb.KeyValuePair) {
		_, err := NewReader(ctx, cm, schema, req, options, 1024, &indexpb.StorageConfig{})
		assert.Error(t, err)
		assert.True(t, strings.Contains(err.Error(), name))
	}

	// binlog import
	req := &internalpb.ImportFile{
		Paths: []string{},
	}
	options := []*commonpb.KeyValuePair{
		{
			Key:   BackupFlag,
			Value: "true",
		},
	}
	checkFunc("no insert binlogs to import", req, options)

	// illegal timestamp
	options = append(options, &commonpb.KeyValuePair{
		Key:   StartTs,
		Value: "abc",
	})
	req.Paths = append(req.Paths, "dummy")
	checkFunc(fmt.Sprintf("parse %s failed", StartTs), req, options)

	// no file to import
	options = []*commonpb.KeyValuePair{}
	req.Paths = []string{}
	checkFunc("no file to import", req, options)

	// inconsistent file type
	req = &internalpb.ImportFile{
		Paths: []string{"1.npy", "2.csv"},
	}
	checkFunc("inconsistency in file types", req, options)

	// accepts only one json file
	req = &internalpb.ImportFile{
		Paths: []string{"1.json", "2.json"},
	}
	checkFunc("accepts only one file", req, options)

	req = &internalpb.ImportFile{
		Paths: []string{"1.jsonl", "2.ndjson"},
	}
	checkFunc("accepts only one file", req, options)

	// json file
	req = &internalpb.ImportFile{
		Paths: []string{"1.json"},
	}
	checkFunc("io error", req, options)

	req = &internalpb.ImportFile{
		Paths: []string{"1.jsonl"},
	}
	checkFunc("io error", req, options)

	req = &internalpb.ImportFile{
		Paths: []string{"1.ndjson"},
	}
	checkFunc("io error", req, options)

	// accepts multiple numpy files
	req = &internalpb.ImportFile{
		Paths: []string{"1.npy", "2.npy"},
	}
	checkFunc("no file for field", req, options)

	// numpy file
	req = &internalpb.ImportFile{
		Paths: []string{"pk.npy"},
	}
	checkFunc("io error", req, options)

	// accepts only one parquet file
	req = &internalpb.ImportFile{
		Paths: []string{"1.parquet", "2.parquet"},
	}
	checkFunc("accepts only one file", req, options)

	// parquet file
	req = &internalpb.ImportFile{
		Paths: []string{"1.parquet"},
	}
	checkFunc("io error", req, options)

	// accepts only one csv file
	req = &internalpb.ImportFile{
		Paths: []string{"1.csv", "2.csv"},
	}
	checkFunc("accepts only one file", req, options)

	// csv file
	req = &internalpb.ImportFile{
		Paths: []string{"1.csv"},
	}
	checkFunc("io error", req, options)

	// illegal sep
	options = []*commonpb.KeyValuePair{
		{
			Key:   CSVSep,
			Value: "\n",
		},
	}
	checkFunc("unsupported csv separator", req, options)

	// invalid file type
	req = &internalpb.ImportFile{
		Paths: []string{"1.txt"},
	}
	checkFunc("unexpected file type", req, options)
}
