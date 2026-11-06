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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	mock "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/snapshotio"
	snapshotstorage "github.com/milvus-io/milvus/internal/snapshotio/storage"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/internal/util/importutilv2/binlog"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestSnapshotPrepareDeleteMasks(t *testing.T) {
	for _, mode := range []string{"success", "contract", "time_range", "storage", "validator", "prepare"} {
		t.Run(mode, func(t *testing.T) {
			options := Options{{Key: BackupFlag, Value: "true"}, {Key: SourceType, Value: SourceTypeSnapshot}}
			file := &internalpb.ImportFile{SnapshotSource: &internalpb.SnapshotImportSource{
				Version: 5, ManifestPath: packed.MarshalManifestPath("root/source", 1), SourceChannel: "source", SourcePartitionId: 1,
			}}
			source := &internalpb.SnapshotImportL0Source{SourceChannel: "source", SourcePartitionId: 1}
			if mode == "contract" {
				source.SourcePartitionId = 2
			}
			if mode == "time_range" {
				options = append(options, &commonpb.KeyValuePair{Key: "start_ts", Value: "invalid"})
			}
			cfg := &indexpb.StorageConfig{StorageType: "local"}
			cm := storage.NewLocalChunkManager()
			factory := &ReaderFactory{options: options, storage: func() (readerStorage, error) {
				if mode == "storage" {
					return readerStorage{}, merr.ErrIoPermissionDenied
				}
				return readerStorage{cm: cm, cfg: cfg}, nil
			}}
			if mode == "validator" {
				p := mockey.Mock(SnapshotPathValidator).Return(nil, merr.ErrServiceInternal).Build()
				defer p.UnPatch()
			}
			var prepareErr error
			if mode == "prepare" {
				prepareErr = merr.ErrIoKeyNotFound
			}
			want := &binlog.SnapshotL0Deletes{}
			p := mockey.Mock(binlog.BuildSnapshotDeleteMasks).To(func(_ context.Context, gotCM storage.ChunkManager,
				_ *schemapb.CollectionSchema, gotCfg *indexpb.StorageConfig, sources []*internalpb.SnapshotImportSource,
				gotSource *internalpb.SnapshotImportL0Source, _, _ uint64, deletes, bitmaps int64, _ binlog.SourceEncryption, _ func(string) error,
			) (*binlog.SnapshotL0Deletes, error) {
				require.Same(t, cm, gotCM)
				require.Same(t, cfg, gotCfg)
				require.Equal(t, []*internalpb.SnapshotImportSource{file.SnapshotSource}, sources)
				require.Same(t, source, gotSource)
				require.EqualValues(t, 128, deletes)
				require.EqualValues(t, 256, bitmaps)
				if prepareErr != nil {
					return nil, prepareErr
				}
				return want, nil
			}).Build()
			defer p.UnPatch()
			result, err := factory.PrepareSnapshotDeletes(context.Background(), nil, []*internalpb.ImportFile{file}, source, 128, 256)
			if mode == "success" {
				require.NoError(t, err)
				require.Same(t, want, result)
			} else {
				require.Error(t, err)
				require.Nil(t, result)
			}
		})
	}
}

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

func TestSnapshotPreparationDescriptor(t *testing.T) {
	options := Options{
		{Key: BackupFlag, Value: "true"},
		{Key: SourceType, Value: SourceTypeSnapshot},
		{Key: SnapshotSourceURI, Value: "s3://source/root/snapshots/1/metadata/2.json"},
		{Key: SnapshotLayout, Value: "referenced"},
	}
	for _, mode := range []string{"valid", "missing_uri", "missing_layout", "paths", "manifest", "timestamp", "partition", "channel", "source_partition", "inline_l0", "pending_l0"} {
		t.Run(mode, func(t *testing.T) {
			source := &internalpb.SnapshotImportSource{Version: SnapshotPreparationVersion}
			files := []*internalpb.ImportFile{{SnapshotSource: source}}
			input := append(Options(nil), options...)
			var l0 []*internalpb.SnapshotImportL0Source
			switch mode {
			case "missing_uri":
				input = append(input[:2:2], input[3:]...)
			case "missing_layout":
				input = input[:3]
			case "paths":
				files[0].Paths = []string{"manifest"}
			case "manifest":
				source.ManifestPath = "manifest"
			case "timestamp":
				source.SourceCommitTimestamp = 1
			case "partition":
				source.TargetPartitionId = 1
			case "channel":
				source.SourceChannel = "channel"
			case "source_partition":
				source.SourcePartitionId = 1
			case "inline_l0":
				source.LegacyL0Deltalogs = []string{"delete"}
			case "pending_l0":
				l0 = []*internalpb.SnapshotImportL0Source{{SourceChannel: "channel", SourcePartitionId: 1}}
			}
			err := ValidateSnapshotImportPlan(files, input, l0)
			if mode == "valid" {
				require.NoError(t, err)
				require.ErrorIs(t, ValidateSnapshotImportTask(files, input, nil), merr.ErrServiceUnimplemented)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestSnapshotReaderFactoryConcurrentResolution(t *testing.T) {
	paramtable.Init()
	for _, useShared := range []bool{false, true} {
		t.Run(fmt.Sprintf("shared_%v", useShared), func(t *testing.T) {
			taskCtx, cancel := context.WithCancel(context.Background())
			defer cancel()
			targetCM, sourceCM := storage.NewLocalChunkManager(), storage.NewLocalChunkManager()
			target := &indexpb.StorageConfig{BucketName: "target"}
			foreign := &indexpb.StorageConfig{BucketName: "source"}
			options := Options{
				{Key: BackupFlag, Value: "true"},
				{Key: SourceType, Value: SourceTypeSnapshot},
				{Key: ExternalSpec, Value: `{"extfs":{"region":"us-east-1"}}`},
				{Key: SnapshotSourceURI, Value: "s3://source/root/snapshots/1/metadata/2.json"},
				{Key: "ezk", Value: "source-key"},
			}
			plugin := &indexcgopb.StoragePluginContext{EncryptionZoneId: 10}
			parse := mockey.Mock(hookutil.GetEzIDByImportEzk).Return(int64(10), nil).Build()
			defer parse.UnPatch()
			key := mockey.Mock(hookutil.GetCPluginContextByEzID).Return(plugin, nil).Build()
			defer key.UnPatch()
			file := &internalpb.ImportFile{SnapshotSource: &internalpb.SnapshotImportSource{
				Version: 2, ManifestPath: packed.MarshalManifestPath("root/data/1", 7),
			}}
			var shared *binlog.SnapshotL0Deletes
			if useShared {
				shared = &binlog.SnapshotL0Deletes{}
				file.SnapshotSource.Version = 6
				file.SnapshotSource.SourceChannel = "source"
				file.SnapshotSource.SourcePartitionId = 10
			}
			original := proto.Clone(file)
			entered, release := make(chan struct{}), make(chan struct{})
			var resolutions, opens atomic.Int64
			resolve := mockey.Mock(snapshotstorage.ResolveSnapshotReadStorage).To(func(ctx context.Context, _ *objectstorage.Config, uri, spec string) (*snapshotstorage.ResolvedForeignStorage, error) {
				assert.Same(t, taskCtx, ctx, "the initializing file must not own the shared client context")
				assert.Equal(t, options[3].Value, uri)
				assert.Equal(t, options[2].Value, spec)
				if resolutions.Add(1) == 1 {
					close(entered)
				}
				<-release
				return &snapshotstorage.ResolvedForeignStorage{ForeignCM: sourceCM, ForeignStorageConfig: foreign}, nil
			}).Build()
			defer resolve.UnPatch()
			checkReader := func(ctx context.Context, cm storage.ChunkManager, cfg *indexpb.StorageConfig, source *internalpb.SnapshotImportSource) {
				assert.NotSame(t, taskCtx, ctx)
				assert.Same(t, sourceCM, cm)
				assert.Same(t, foreign, cfg)
				assert.EqualValues(t, 2, source.Version)
				opens.Add(1)
			}
			ordinary := mockey.Mock(binlog.NewStorageV3ManifestReader).When(func(ctx context.Context, cm storage.ChunkManager, _ *schemapb.CollectionSchema, cfg *indexpb.StorageConfig, _ string, _, _ uint64, _ int, encryption binlog.SourceEncryption, source *internalpb.SnapshotImportSource, _ int64, _ func(string) error) bool {
				checkReader(ctx, cm, cfg, source)
				assert.True(t, encryption.Encrypted)
				assert.Same(t, plugin, encryption.PluginContext)
				return true
			}).Return(nil, io.EOF).Build()
			defer ordinary.UnPatch()
			withShared := mockey.Mock(binlog.NewStorageV3ManifestReaderWithSharedL0).When(func(ctx context.Context, cm storage.ChunkManager, _ *schemapb.CollectionSchema, cfg *indexpb.StorageConfig, _ string, _, _ uint64, _ int, encryption binlog.SourceEncryption, source *internalpb.SnapshotImportSource, _ int64, index *binlog.SnapshotL0Deletes, _ func(string) error) bool {
				checkReader(ctx, cm, cfg, source)
				assert.True(t, encryption.Encrypted)
				assert.Same(t, plugin, encryption.PluginContext)
				assert.Same(t, shared, index)
				return true
			}).Return(nil, io.EOF).Build()
			defer withShared.UnPatch()
			load := mockey.Mock(binlog.BuildSnapshotDeleteMasks).When(func(_ context.Context, cm storage.ChunkManager, _ *schemapb.CollectionSchema, cfg *indexpb.StorageConfig,
				_ []*internalpb.SnapshotImportSource, _ *internalpb.SnapshotImportL0Source, _, _ uint64, _, _ int64, encryption binlog.SourceEncryption, _ func(string) error,
			) bool {
				assert.Same(t, sourceCM, cm)
				assert.Same(t, foreign, cfg)
				assert.True(t, encryption.Encrypted)
				assert.Same(t, plugin, encryption.PluginContext)
				return true
			}).Return(shared, nil).Build()
			defer load.UnPatch()
			factory := NewReaderFactory(taskCtx, targetCM, target, options)
			var workers sync.WaitGroup
			for i := 0; i < 8; i++ {
				workers.Add(1)
				go func() {
					defer workers.Done()
					ctx, stop := context.WithCancel(taskCtx)
					defer stop()
					_, err := factory.NewReader(ctx, &schemapb.CollectionSchema{}, file, 1024, 1024, shared)
					assert.ErrorIs(t, err, io.EOF)
				}()
			}
			if useShared {
				workers.Add(1)
				go func() {
					defer workers.Done()
					got, err := factory.PrepareSnapshotDeletes(taskCtx, &schemapb.CollectionSchema{}, []*internalpb.ImportFile{file},
						&internalpb.SnapshotImportL0Source{SourceChannel: "source", SourcePartitionId: 10}, 1024, 1024)
					assert.NoError(t, err)
					assert.Same(t, shared, got)
				}()
			}
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Error("source resolution did not start")
			}
			close(release)
			workers.Wait()
			require.EqualValues(t, 1, resolutions.Load())
			require.EqualValues(t, 8, opens.Load())
			require.EqualValues(t, 1, parse.Times())
			require.EqualValues(t, 1, key.Times())
			require.True(t, proto.Equal(original, file), "validation/adaptation must not mutate persisted descriptors")
			require.Equal(t, "target", target.BucketName)
			require.NotEmpty(t, options[2].Value, "external_spec must remain available for descriptor validation")
		})
	}
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
				_ *indexcgopb.StoragePluginContext,
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
				_, err := factory.NewReader(ctx, nil, file, 1024, 0, nil)
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
	_, err := factory.NewReader(ctx, nil, file, 1024, 0, nil)
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
	_, err := factory.NewReader(canceled, nil, file, 1024, 1024, nil)
	require.ErrorIs(t, err, context.Canceled)
	_, err = newFactory(canceled).NewReader(ctx, nil, file, 1024, 1024, nil)
	require.ErrorIs(t, err, context.Canceled)
	broken := proto.Clone(file).(*internalpb.ImportFile)
	broken.SnapshotSource.Version = 1 // A foreign task must retain the external descriptor contract.
	_, err = factory.NewReader(ctx, nil, broken, 1024, 1024, nil)
	require.Error(t, err)
	require.Zero(t, resolutions)
	for i := 0; i < 2; i++ {
		_, err = factory.NewReader(ctx, nil, file, 1024, 1024, nil)
		require.ErrorIs(t, err, merr.ErrIoPermissionDenied)
	}
	require.Equal(t, 1, resolutions, "a failed attempt must not create a client per file")
	resolve.To(func(context.Context, *objectstorage.Config, string, string) (*snapshotstorage.ResolvedForeignStorage, error) {
		resolutions++
		return &snapshotstorage.ResolvedForeignStorage{ForeignCM: storage.NewLocalChunkManager()}, nil
	})
	reader := mockey.Mock(binlog.NewStorageV3ManifestReader).Return(nil, io.EOF).Build()
	defer reader.UnPatch()
	_, err = newFactory(ctx).NewReader(ctx, nil, file, 1024, 1024, nil)
	require.ErrorIs(t, err, io.EOF, "a fresh attempt can recover after source access is restored")
	require.Equal(t, 2, resolutions, "redispatch must not retain the previous attempt's resolution error")
	_, err = factory.NewReader(ctx, nil, file, 1024, 1024, nil)
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
			_, err := factory.NewReader(ctx, nil, file, 1024, 1024, nil)
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

func TestSnapshotReaderFactoryInstanceStorage(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	cm := storage.NewLocalChunkManager()
	cfg := &indexpb.StorageConfig{StorageType: "local", RootPath: t.TempDir()}
	options := Options{{Key: BackupFlag, Value: "true"}, {Key: SourceType, Value: SourceTypeSnapshot}}
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true}}}
	factory := NewReaderFactory(ctx, cm, cfg, options)
	file := &internalpb.ImportFile{SnapshotSource: &internalpb.SnapshotImportSource{
		Version: 5, ManifestPath: packed.MarshalManifestPath("root/data/1", 7), SourceChannel: "source", SourcePartitionId: 10,
	}}
	shared := &binlog.SnapshotL0Deletes{}
	prepare := mockey.Mock(binlog.BuildSnapshotDeleteMasks).When(func(_ context.Context, gotCM storage.ChunkManager, _ *schemapb.CollectionSchema,
		gotCfg *indexpb.StorageConfig, _ []*internalpb.SnapshotImportSource, _ *internalpb.SnapshotImportL0Source,
		_, _ uint64, _, _ int64, _ binlog.SourceEncryption, _ func(string) error,
	) bool {
		assert.Same(t, cm, gotCM)
		assert.Same(t, cfg, gotCfg)
		return true
	}).Return(shared, nil).Build()
	defer prepare.UnPatch()
	got, err := factory.PrepareSnapshotDeletes(ctx, schema, []*internalpb.ImportFile{file},
		&internalpb.SnapshotImportL0Source{SourceChannel: "source", SourcePartitionId: 10}, 1024, 1024)
	require.NoError(t, err)
	require.Same(t, shared, got)
	reader := mockey.Mock(binlog.NewStorageV3ManifestReaderWithSharedL0).When(func(_ context.Context, gotCM storage.ChunkManager, _ *schemapb.CollectionSchema, gotCfg *indexpb.StorageConfig, _ string, _, _ uint64, _ int, _ binlog.SourceEncryption, source *internalpb.SnapshotImportSource, _ int64, got *binlog.SnapshotL0Deletes, _ func(string) error) bool {
		assert.Same(t, cm, gotCM)
		assert.Same(t, cfg, gotCfg)
		assert.Same(t, shared, got)
		assert.EqualValues(t, 1, source.Version)
		return true
	}).Return(nil, io.EOF).Build()
	defer reader.UnPatch()
	_, err = factory.NewReader(ctx, schema, file, 1024, 1024, shared)
	require.ErrorIs(t, err, io.EOF)
	// Ordinary imports keep the existing entry point and its validation errors.
	_, err = NewReaderFactory(ctx, cm, cfg, nil).NewReader(ctx, schema, &internalpb.ImportFile{Paths: []string{"unsupported.txt"}}, 1024, 0, nil)
	require.ErrorIs(t, err, merr.ErrImportFailed)
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
	first, err := factory.NewReader(firstCtx, schema, file, 1024, 1024, nil)
	require.NoError(t, err)
	second, err := factory.NewReader(ctx, schema, file, 1024, 1024, nil)
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

func TestSnapshotSharedL0StorageAndReader(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	cm := storage.NewLocalChunkManager()
	target := &indexpb.StorageConfig{BucketName: "target"}
	foreign := &indexpb.StorageConfig{BucketName: "source"}
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true}}}
	options := Options{
		{Key: BackupFlag, Value: "true"},
		{Key: SourceType, Value: SourceTypeSnapshot},
		{Key: ExternalSpec, Value: `{"extfs":{"region":"us-east-1"}}`},
		{Key: SnapshotSourceURI, Value: "s3://source/root/snapshots/1/metadata/2.json"},
	}
	resolve := mockey.Mock(snapshotstorage.ResolveSnapshotReadStorage).Return(&snapshotstorage.ResolvedForeignStorage{ForeignCM: cm, ForeignStorageConfig: foreign}, nil).Build()
	defer resolve.UnPatch()
	input := &internalpb.SnapshotImportL0Source{SourceChannel: "source", SourcePartitionId: 10}
	factory := NewReaderFactory(ctx, cm, target, options)
	file := &internalpb.ImportFile{SnapshotSource: &internalpb.SnapshotImportSource{
		Version: 6, ManifestPath: packed.MarshalManifestPath("root/data/1", 7), SourceChannel: "source", SourcePartitionId: 10,
	}}
	shared := &binlog.SnapshotL0Deletes{}
	loadPatch := mockey.Mock(binlog.BuildSnapshotDeleteMasks).When(func(_ context.Context, gotCM storage.ChunkManager, _ *schemapb.CollectionSchema,
		cfg *indexpb.StorageConfig, sources []*internalpb.SnapshotImportSource, gotSource *internalpb.SnapshotImportL0Source, _, _ uint64, _, _ int64,
		_ binlog.SourceEncryption, _ func(string) error,
	) bool {
		require.Same(t, cm, gotCM)
		require.Same(t, foreign, cfg)
		require.Equal(t, []*internalpb.SnapshotImportSource{file.SnapshotSource}, sources)
		require.Same(t, input, gotSource)
		return true
	}).Return(shared, nil).Build()
	defer loadPatch.UnPatch()
	got, err := factory.PrepareSnapshotDeletes(ctx, schema, []*internalpb.ImportFile{file}, input, 1024, 1024)
	require.NoError(t, err)
	require.Same(t, shared, got)
	reads := 0
	readerPatch := mockey.Mock(binlog.NewStorageV3ManifestReaderWithSharedL0).When(func(_ context.Context, gotCM storage.ChunkManager, gotSchema *schemapb.CollectionSchema,
		cfg *indexpb.StorageConfig, _ string, _, _ uint64, _ int, _ binlog.SourceEncryption, source *internalpb.SnapshotImportSource, budget int64, got *binlog.SnapshotL0Deletes,
		_ func(string) error,
	) bool {
		reads++
		require.Same(t, cm, gotCM)
		require.Same(t, foreign, cfg)
		require.Same(t, schema, gotSchema)
		require.Same(t, shared, got)
		require.EqualValues(t, 2, source.Version)
		require.Empty(t, source.LegacyL0Deltalogs)
		require.Empty(t, source.ManifestL0Deltalogs)
		require.EqualValues(t, 1024, budget)
		return true
	}).Return(nil, merr.ErrIoKeyNotFound).Build()
	defer readerPatch.UnPatch()
	_, err = factory.NewReader(ctx, schema, file, 1024, 1024, shared)
	require.ErrorIs(t, err, merr.ErrIoKeyNotFound)
	require.Equal(t, 1, reads)
	require.EqualValues(t, 6, file.SnapshotSource.Version, "the persisted descriptor must remain compact")
	_, err = factory.NewReader(ctx, schema, file, 1024, 1024, nil)
	require.ErrorIs(t, err, merr.ErrServiceUnimplemented, "a shared descriptor cannot be read without its prepared bitmaps")
	v2 := &internalpb.ImportFile{SnapshotSource: &internalpb.SnapshotImportSource{
		Version: 14, V2Reference: &internalpb.SnapshotImportV2Reference{SegmentId: 20, SnapshotManifestPath: "root/source.avro"},
	}}
	_, err = factory.NewReader(ctx, schema, v2, 1024, 1024, nil)
	require.ErrorIs(t, err, merr.ErrServiceUnimplemented)
	_, err = factory.NewReader(ctx, schema, &internalpb.ImportFile{}, 1024, 1024, shared)
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	broken := proto.Clone(file).(*internalpb.ImportFile)
	broken.SnapshotSource.LegacyL0Deltalogs = []string{"unexpected-inline-delta"}
	_, err = factory.NewReader(ctx, schema, broken, 1024, 1024, shared)
	require.Error(t, err)
	require.Equal(t, 1, reads)
	// Verify that bitmap preparation receives the same foreign storage as
	// segment readers, and never falls back to target storage on failure.
	loadPatch.Return(nil, merr.ErrIoKeyNotFound)
	_, err = factory.PrepareSnapshotDeletes(ctx, schema, []*internalpb.ImportFile{file}, input, 1024, 1024)
	require.ErrorIs(t, err, merr.ErrIoKeyNotFound)
	resolve.Return(nil, merr.ErrIoPermissionDenied)
	_, err = NewReaderFactory(ctx, cm, target, options).PrepareSnapshotDeletes(ctx, schema, []*internalpb.ImportFile{file}, input, 1024, 1024)
	require.ErrorIs(t, err, merr.ErrIoPermissionDenied)
	_, err = NewReaderFactory(ctx, cm, target, append(options, &commonpb.KeyValuePair{Key: "start_ts", Value: "bad"})).PrepareSnapshotDeletes(ctx, schema, []*internalpb.ImportFile{file}, input, 1024, 1024)
	require.Error(t, err)
	require.Equal(t, "target", target.BucketName)
}

func TestImportNewReader_ExternalSnapshotStorage(t *testing.T) {
	paramtable.Init()
	targetCM := storage.NewLocalChunkManager()
	sourceCM := storage.NewLocalChunkManager()
	targetConfig := &indexpb.StorageConfig{BucketName: "target", SecretAccessKey: "target-secret"}
	defaultCM, defaultConfig, err := ResolveSnapshotImportStorage(context.Background(), targetCM, targetConfig, "", nil)
	require.NoError(t, err)
	require.Same(t, targetCM, defaultCM)
	require.Same(t, targetConfig, defaultConfig)
	sourceConfig := &indexpb.StorageConfig{BucketName: "source", SecretAccessKey: "source-secret"}
	options := Options{
		{Key: BackupFlag, Value: "true"},
		{Key: SourceType, Value: SourceTypeSnapshot},
		{Key: ExternalSpec, Value: `{"extfs":{"region":"us-east-1"}}`},
		{Key: SnapshotSourceURI, Value: "s3://source/root/snapshots/1/metadata/2.json"},
	}
	file := &internalpb.ImportFile{SnapshotSource: &internalpb.SnapshotImportSource{
		Version: 2, ManifestPath: packed.MarshalManifestPath("root/data/1", 7),
		SourceCommitTimestamp: 300,
	}}
	var resolveErr error
	resolve := mockey.Mock(snapshotstorage.ResolveSnapshotReadStorage).To(func(ctx context.Context, _ *objectstorage.Config,
		uri, spec string,
	) (*snapshotstorage.ResolvedForeignStorage, error) {
		require.Equal(t, options[3].Value, uri)
		require.Equal(t, options[2].Value, spec)
		return &snapshotstorage.ResolvedForeignStorage{ForeignCM: sourceCM, ForeignStorageConfig: sourceConfig}, resolveErr
	}).Build()
	defer resolve.UnPatch()
	reads := 0
	readerPatch := mockey.Mock(binlog.NewStorageV3ManifestReader).When(func(ctx context.Context, cm storage.ChunkManager,
		schema *schemapb.CollectionSchema, cfg *indexpb.StorageConfig, manifest string, start, end uint64,
		buffer int, encryption binlog.SourceEncryption, source *internalpb.SnapshotImportSource, budget int64,
		_ func(string) error,
	) bool {
		reads++
		require.Same(t, sourceCM, cm)
		require.Same(t, sourceConfig, cfg)
		require.Equal(t, file.SnapshotSource, source)
		require.EqualValues(t, 1024, budget)
		return true
	}).Return(nil, merr.ErrIoKeyNotFound).Build()
	defer readerPatch.UnPatch()
	// Exercise the common reader boundary used by PreImport and Import after
	// serializing the task, as on redispatch/restart. Never patch NewReader.
	for i := 0; i < 2; i++ {
		encoded, err := proto.Marshal(file)
		require.NoError(t, err)
		decoded := &internalpb.ImportFile{}
		require.NoError(t, proto.Unmarshal(encoded, decoded))
		_, err = NewReader(context.Background(), targetCM, &schemapb.CollectionSchema{}, decoded, options, 1024, targetConfig, 1024)
		require.ErrorIs(t, err, merr.ErrIoKeyNotFound)
	}
	require.Equal(t, 2, reads)
	resolveErr = merr.ErrIoPermissionDenied
	_, err = NewReader(context.Background(), targetCM, &schemapb.CollectionSchema{}, file, options, 1024, targetConfig, 1024)
	require.ErrorIs(t, err, resolveErr)
	require.Equal(t, 2, reads, "source access failure must not fall back to the target")
	require.Equal(t, "target", targetConfig.BucketName)
	require.Equal(t, "target-secret", targetConfig.SecretAccessKey)
	for _, broken := range []*internalpb.ImportFile{
		{Paths: []string{file.SnapshotSource.ManifestPath}},
		{SnapshotSource: &internalpb.SnapshotImportSource{Version: 1, ManifestPath: file.SnapshotSource.ManifestPath}},
	} {
		require.Error(t, ValidateSnapshotImportFiles([]*internalpb.ImportFile{broken}, options))
		_, err := NewReader(context.Background(), targetCM, &schemapb.CollectionSchema{}, broken, options, 1024, targetConfig, 1024)
		require.ErrorIs(t, err, merr.ErrServiceInternal)
	}
	require.Error(t, ValidateSnapshotImportFiles([]*internalpb.ImportFile{file}, options[:2]))
}

func TestImportNewReader_SnapshotSourceTimestamp(t *testing.T) {
	type timestampRecordReader struct{ storage.RecordReader }
	paramtable.Init()
	fieldIDsPatch := mockey.Mock(packed.GetManifestFieldIDs).
		Return(map[int64]struct{}{common.RowIDField: {}, common.TimeStampField: {}, 100: {}}, nil).Build()
	defer fieldIDsPatch.UnPatch()
	manifest := packed.MarshalManifestPath("snapshot/files/segment/10", 7)
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
	}}
	options := Options{{Key: BackupFlag, Value: "true"}, {Key: SourceType, Value: SourceTypeSnapshot}}
	for _, bufferSize := range []int{1, 1024} {
		t.Run(fmt.Sprintf("buffer_%d", bufferSize), func(t *testing.T) {
			// Exercise the factory shared by both Import phases, including the
			// internal serialization boundary. Do not mock the factory or reader.
			file := &internalpb.ImportFile{SnapshotSource: &internalpb.SnapshotImportSource{
				Version: 1, ManifestPath: manifest, SourceCommitTimestamp: 300,
			}}
			encoded, err := proto.Marshal(file)
			require.NoError(t, err)
			decoded := &internalpb.ImportFile{}
			require.NoError(t, proto.Unmarshal(encoded, decoded))
			builder := array.NewRecordBuilder(memory.DefaultAllocator, arrow.NewSchema([]arrow.Field{
				{Name: "row_id", Type: arrow.PrimitiveTypes.Int64},
				{Name: "ts", Type: arrow.PrimitiveTypes.Int64},
				{Name: "pk", Type: arrow.PrimitiveTypes.Int64},
			}, nil))
			builder.Field(0).(*array.Int64Builder).Append(1)
			builder.Field(1).(*array.Int64Builder).Append(400)
			builder.Field(2).(*array.Int64Builder).Append(1)
			record := storage.NewSimpleArrowRecord(builder.NewRecord(), map[storage.FieldID]int{
				common.RowIDField: 0, common.TimeStampField: 1, 100: 2,
			})
			builder.Release()
			defer record.Release()
			owner := &timestampRecordReader{}
			read := false
			nextPatch := mockey.Mock((*timestampRecordReader).Next).To(func(_ *timestampRecordReader) (storage.Record, error) {
				if read {
					return nil, io.EOF
				}
				read = true
				return record, nil
			}).Build()
			defer nextPatch.UnPatch()
			closed := false
			closePatch := mockey.Mock((*timestampRecordReader).Close).To(func(_ *timestampRecordReader) error {
				closed = true
				return nil
			}).Build()
			defer closePatch.UnPatch()
			manifestPatch := mockey.Mock(storage.NewManifestRecordReader).Return(owner, nil).Build()
			defer manifestPatch.UnPatch()
			fragmentsPatch := mockey.Mock(packed.ReadFragmentsFromManifest).Return([]packed.Fragment(nil), nil).Build()
			defer fragmentsPatch.UnPatch()
			lobPatch := mockey.Mock(packed.GetManifestLobFiles).Return([]packed.LobFileInfo(nil), nil).Build()
			defer lobPatch.UnPatch()
			deltaPatch := mockey.Mock(packed.GetDeltaLogPathsFromManifest).Return([]string(nil), nil).Build()
			defer deltaPatch.UnPatch()

			r, err := NewReader(context.Background(), nil, schema, decoded, options, bufferSize, &indexpb.StorageConfig{}, 1024)
			require.NoError(t, err)
			defer r.Close()
			data, err := r.Read()
			require.ErrorIs(t, err, merr.ErrDataIntegrity)
			require.Nil(t, data)
			require.True(t, closed)

			// The same manifest through the baseline no-L0 representation must
			// not acquire a source commit time or the new validation behavior.
			read = false
			legacy, err := NewReader(context.Background(), nil, schema, &internalpb.ImportFile{Paths: []string{manifest}},
				options, bufferSize, &indexpb.StorageConfig{}, 0)
			require.NoError(t, err)
			defer legacy.Close()
			data, err = legacy.Read()
			require.NoError(t, err)
			require.Equal(t, 1, data.GetRowNum())
		})
	}
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
			}, tc.options, 1024, nil, 0)
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
				&internalpb.ImportFile{Paths: tc.paths}, tc.options, 1024, nil, 0)
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
			Options{{Key: BackupFlag, Value: "true"}, {Key: StorageVersion, Value: "invalid"}}, 1024, nil, 0)
		require.ErrorIs(t, err, merr.ErrImportFailed)
		require.ErrorContains(t, err, "parse storage_version failed")
		require.Nil(t, r)
	})
	t.Run("csv_null_key", func(t *testing.T) {
		errNullKey := merr.WrapErrImportFailed("invalid null key")
		patch := mockey.Mock(GetCSVNullKey).Return("", errNullKey).Build()
		defer patch.UnPatch()
		r, err := NewReader(context.Background(), nil, &schemapb.CollectionSchema{},
			&internalpb.ImportFile{Paths: []string{"data.csv"}}, nil, 1024, nil, 0)
		require.ErrorIs(t, err, errNullKey)
		require.Nil(t, r)
	})
	t.Run("unhandled_file_type", func(t *testing.T) {
		patch := mockey.Mock(GetFileType).Return(FileType(99), nil).Build()
		defer patch.UnPatch()
		r, err := NewReader(context.Background(), nil, &schemapb.CollectionSchema{}, &internalpb.ImportFile{}, nil, 1024, nil, 0)
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

func TestValidateSnapshotImportFiles(t *testing.T) {
	manifest := packed.MarshalManifestPath("snapshot/data", 7)
	options := Options{{Key: BackupFlag, Value: "true"}, {Key: SourceType, Value: SourceTypeSnapshot}}
	for _, name := range []string{"ordinary", "empty_job", "lost_descriptor", "empty_manifest", "baseline", "typed", "bad_options", "wrong_mode", "mixed", "legacy_paths", "unknown_version", "invalid_manifest", "latest_manifest", "empty_delete", "conflicting_decoder", "duplicate_delete", "oversized"} {
		t.Run(name, func(t *testing.T) {
			source := &internalpb.SnapshotImportSource{Version: 1, ManifestPath: manifest}
			files := []*internalpb.ImportFile{{SnapshotSource: source}}
			opts := options
			valid := false
			switch name {
			case "ordinary":
				files = nil
				opts = nil
				valid = true
			case "empty_job":
				files = nil
			case "lost_descriptor":
				files = []*internalpb.ImportFile{{}}
			case "empty_manifest":
				files = []*internalpb.ImportFile{{Paths: []string{" "}}}
			case "baseline":
				files = []*internalpb.ImportFile{{Paths: []string{manifest}}}
				valid = true
			case "typed":
				valid = true
			case "bad_options":
				opts = Options{{Key: SourceType, Value: "invalid"}}
			case "wrong_mode":
				opts = nil
			case "mixed":
				files = append(files, &internalpb.ImportFile{Paths: []string{manifest}})
			case "legacy_paths":
				files[0].Paths = []string{manifest}
			case "unknown_version":
				source.Version = 99
			case "invalid_manifest":
				source.ManifestPath = "invalid"
			case "latest_manifest":
				source.ManifestPath = packed.MarshalManifestPath("snapshot/data", packed.ManifestLatest)
			case "empty_delete":
				source.LegacyL0Deltalogs = []string{" "}
			case "conflicting_decoder":
				source.LegacyL0Deltalogs = []string{"delta"}
				source.ManifestL0Deltalogs = []string{"delta"}
			case "duplicate_delete":
				source.LegacyL0Deltalogs = []string{"delta", "delta"}
			case "oversized":
				source.ManifestPath = packed.MarshalManifestPath(strings.Repeat("x", SnapshotSourcePlanMaxBytes), 7)
			}
			err := ValidateSnapshotImportFiles(files, opts)
			if valid {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestSnapshotImportRejectsInlineL0(t *testing.T) {
	paramtable.Init()
	for version := uint32(1); version <= 8; version++ {
		for _, kind := range []string{"legacy", "manifest", "empty_path"} {
			t.Run(fmt.Sprintf("v%d/%s", version, kind), func(t *testing.T) {
				options := Options{{Key: BackupFlag, Value: "true"}, {Key: SourceType, Value: SourceTypeSnapshot}}
				source := &internalpb.SnapshotImportSource{Version: version, ManifestPath: packed.MarshalManifestPath("snapshot/data", 7)}
				contract := (version-1)%4 + 1
				if contract == 2 || contract == 4 {
					options = append(options, &commonpb.KeyValuePair{Key: ExternalSpec, Value: `{}`},
						&commonpb.KeyValuePair{Key: SnapshotSourceURI, Value: "s3://source/root/metadata"})
				}
				if contract == 3 || contract == 4 {
					source.TargetPartitionId = 20
					options = append(options, &commonpb.KeyValuePair{Key: PartitionMapping, Value: `{"a":"b"}`})
				}
				if version >= 5 {
					source.SourceChannel, source.SourcePartitionId = "source", 10
				}
				file := &internalpb.ImportFile{SnapshotSource: source}
				require.NoError(t, ValidateSnapshotImportFiles([]*internalpb.ImportFile{file}, options), "no-inline descriptors remain supported")
				switch kind {
				case "legacy":
					source.LegacyL0Deltalogs = []string{"delta"}
				case "manifest":
					source.ManifestL0Deltalogs = []string{"delta"}
				case "empty_path":
					source.LegacyL0Deltalogs = []string{""}
				}
				encoded, err := proto.Marshal(file)
				require.NoError(t, err)
				recovered := &internalpb.ImportFile{}
				require.NoError(t, proto.Unmarshal(encoded, recovered))
				files := []*internalpb.ImportFile{recovered}
				for _, err := range []error{
					ValidateSnapshotImportFiles(files, options),
					ValidateSnapshotImportPlan(files, options, nil),
					ValidateSnapshotImportTask(files, options, nil),
				} {
					require.ErrorIs(t, err, merr.ErrServiceUnimplemented)
					require.ErrorIs(t, merr.Error(merr.Status(err)), merr.ErrServiceUnimplemented)
				}
				// No source client or schema is available. Rejection must precede
				// source resolution, manifest IO and row/delete decoding.
				r, err := NewReaderFactory(context.Background(), nil, nil, options).NewReader(context.Background(), nil, recovered, 1024, 1024, nil)
				require.ErrorIs(t, err, merr.ErrServiceUnimplemented)
				require.Nil(t, r)
			})
		}
	}
}

func TestSnapshotSharedL0Contracts(t *testing.T) {
	base := Options{{Key: BackupFlag, Value: "true"}, {Key: SourceType, Value: SourceTypeSnapshot}}
	for version := uint32(5); version <= 8; version++ {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			opts := append(Options(nil), base...)
			source := &internalpb.SnapshotImportSource{
				Version: version, SourceChannel: "a", SourcePartitionId: 10,
				ManifestPath: packed.MarshalManifestPath("snapshot/data", 7), SourceCommitTimestamp: 100,
			}
			if version == 6 || version == 8 {
				opts = append(opts, &commonpb.KeyValuePair{Key: ExternalSpec, Value: `{}`},
					&commonpb.KeyValuePair{Key: SnapshotSourceURI, Value: "s3://source/root/snapshots/1/metadata/2.json"})
			}
			if version == 7 || version == 8 {
				source.TargetPartitionId = 20
				opts = append(opts, &commonpb.KeyValuePair{Key: PartitionMapping, Value: `{"a":"b"}`})
			}
			file := &internalpb.ImportFile{Id: 1, SnapshotSource: source}
			files := []*internalpb.ImportFile{file, proto.Clone(file).(*internalpb.ImportFile)}
			inventories := []*internalpb.SnapshotImportL0Source{
				{SourceChannel: "a", SourcePartitionId: 10, LegacyL0Deltalogs: []string{"local", "duplicate"}},
				{SourceChannel: "a", SourcePartitionId: common.AllPartitionsID, LegacyL0Deltalogs: []string{"all", "duplicate"}, ManifestL0Paths: []string{packed.MarshalManifestPath("packed", 1)}},
			}
			require.NoError(t, ValidateSnapshotImportPlan(files, opts, inventories))
			shared, err := SnapshotTaskL0Source(files, inventories)
			require.NoError(t, err)
			require.Equal(t, []string{"all", "duplicate", "local"}, shared.LegacyL0Deltalogs)
			require.Equal(t, []string{packed.MarshalManifestPath("packed", 1)}, shared.ManifestL0Paths)
			require.NoError(t, ValidateSnapshotImportTask(files, opts, shared))
			require.Empty(t, file.SnapshotSource.LegacyL0Deltalogs, "task assembly must not expand persisted file stats")
			require.Equal(t, version, file.SnapshotSource.Version)
		})
	}
	for _, name := range []string{"missing", "empty", "wrong_channel", "wrong_partition", "duplicate_scope", "nil_scope", "blank_path", "latest_manifest", "decoder_conflict", "mixed_scope", "mixed_version", "legacy_scope", "inline_paths", "missing_channel", "missing_partition", "oversized", "unknown_version"} {
		t.Run(name, func(t *testing.T) {
			file := &internalpb.ImportFile{SnapshotSource: &internalpb.SnapshotImportSource{
				Version: 5, SourceChannel: "a", SourcePartitionId: 10, ManifestPath: packed.MarshalManifestPath("snapshot/data", 7),
			}}
			files := []*internalpb.ImportFile{file}
			shared := &internalpb.SnapshotImportL0Source{SourceChannel: "a", SourcePartitionId: 10}
			inventories := []*internalpb.SnapshotImportL0Source{shared}
			switch name {
			case "missing":
				inventories = nil
			case "empty": // An explicitly empty inventory is valid.
			case "wrong_channel":
				shared.SourceChannel = "b"
			case "wrong_partition":
				shared.SourcePartitionId = 20
			case "duplicate_scope":
				inventories = append(inventories, shared)
			case "nil_scope":
				inventories = append(inventories, nil)
			case "blank_path":
				shared.LegacyL0Deltalogs = []string{" "}
			case "latest_manifest":
				shared.ManifestL0Paths = []string{packed.MarshalManifestPath("snapshot/l0", packed.ManifestLatest)}
			case "decoder_conflict":
				manifest := packed.MarshalManifestPath("same", 1)
				shared.LegacyL0Deltalogs = []string{manifest}
				shared.ManifestL0Paths = []string{manifest}
			case "mixed_scope":
				other := proto.Clone(file).(*internalpb.ImportFile)
				other.SnapshotSource.SourcePartitionId = 20
				files = append(files, other)
			case "mixed_version":
				files = append(files, &internalpb.ImportFile{SnapshotSource: &internalpb.SnapshotImportSource{Version: 1, ManifestPath: file.SnapshotSource.ManifestPath}})
			case "legacy_scope":
				file.SnapshotSource.Version = 1
			case "inline_paths":
				file.SnapshotSource.LegacyL0Deltalogs = []string{"inline"}
			case "missing_channel":
				file.SnapshotSource.SourceChannel = ""
			case "missing_partition":
				file.SnapshotSource.SourcePartitionId = 0
			case "oversized":
				shared.LegacyL0Deltalogs = []string{strings.Repeat("x", SnapshotSourcePlanMaxBytes)}
			case "unknown_version":
				file.SnapshotSource.Version = 99
			}
			if name == "empty" {
				require.NoError(t, ValidateSnapshotImportPlan(files, base, inventories))
				require.NoError(t, ValidateSnapshotImportTask(files, base, shared))
			} else {
				require.Error(t, ValidateSnapshotImportPlan(files, base, inventories))
			}
			if name == "missing" || name == "wrong_channel" || name == "wrong_partition" || name == "mixed_scope" || name == "mixed_version" || name == "decoder_conflict" {
				_, err := SnapshotTaskL0Source(files, inventories)
				require.Error(t, err)
				if name == "mixed_version" {
					_, err = SnapshotTaskL0Source([]*internalpb.ImportFile{files[1], files[0]}, inventories)
					require.Error(t, err)
				}
			}
		})
	}
	shared, err := SnapshotTaskL0Source(nil, nil)
	require.NoError(t, err)
	require.Nil(t, shared)
	require.NoError(t, ValidateSnapshotImportPlan([]*internalpb.ImportFile{{Paths: []string{strings.Repeat("x", SnapshotSourcePlanMaxBytes+1)}}}, nil, nil), "ordinary imports do not acquire a snapshot plan limit")
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
			require.NoError(t, ValidateSnapshotImportPlan([]*internalpb.ImportFile{file}, options, nil))
			file.Id = 1 << 60
			require.ErrorContains(t, ValidateSnapshotImportPlan([]*internalpb.ImportFile{file}, options, nil), "snapshot source plan exceeds 256 KiB")
			// The limit covers the whole plan, not just individual files.
			file = makeFile(SnapshotSourcePlanMaxBytes / 2)
			require.NoError(t, ValidateSnapshotImportPlan([]*internalpb.ImportFile{file}, options, nil))
			require.ErrorContains(t, ValidateSnapshotImportPlan([]*internalpb.ImportFile{file, file}, options, nil), "snapshot source plan exceeds 256 KiB")
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
		_, err := NewReader(ctx, cm, schema, req, options, 1024, &indexpb.StorageConfig{}, 0)
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

func TestSnapshotV2DescriptorVersions(t *testing.T) {
	for version := uint32(1); version <= 18; version++ {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			source := &internalpb.SnapshotImportSource{Version: version, V2Reference: &internalpb.SnapshotImportV2Reference{SegmentId: 20, SnapshotManifestPath: "root/snapshots/1/manifests/data.avro", SnapshotFormatVersion: 6}}
			opts := Options{{Key: BackupFlag, Value: "true"}, {Key: SourceType, Value: SourceTypeSnapshot}, {Key: SnapshotSourceURI, Value: "s3://bucket/root/snapshots/1/metadata/2.json"}, {Key: SnapshotLayout, Value: "referenced"}}
			contract := SnapshotSourceContractVersion(source)
			if contract == 2 || contract == 4 {
				opts = append(opts, &commonpb.KeyValuePair{Key: ExternalSpec, Value: `{"provider":"aws","region":"us-east-1","access_key_id":"key","secret_access_key":"secret"}`})
			}
			if contract == 3 || contract == 4 {
				opts = append(opts, &commonpb.KeyValuePair{Key: PartitionMapping, Value: `{"source":"target"}`})
				source.TargetPartitionId = 100
			}
			if SnapshotSourceUsesSharedL0(source) {
				source.SourceChannel, source.SourcePartitionId = "source", 10
			}
			file := &internalpb.ImportFile{SnapshotSource: source}
			err := ValidateSnapshotImportFiles([]*internalpb.ImportFile{file}, opts)
			if version < 10 || version > 17 {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			for _, mutate := range []func(*internalpb.SnapshotImportSource){
				func(s *internalpb.SnapshotImportSource) { s.ManifestPath = packed.MarshalManifestPath("root/data", 1) },
				func(s *internalpb.SnapshotImportSource) { s.V2Reference.SegmentId = 0 },
				func(s *internalpb.SnapshotImportSource) { s.V2Reference.SnapshotManifestPath = " " },
				func(s *internalpb.SnapshotImportSource) { s.V2Reference.SnapshotFormatVersion = -1 },
				func(s *internalpb.SnapshotImportSource) { s.V2Reference.OriginalKeyPrefix = proto.String("") },
			} {
				bad := proto.Clone(source).(*internalpb.SnapshotImportSource)
				mutate(bad)
				require.Error(t, ValidateSnapshotImportFiles([]*internalpb.ImportFile{{SnapshotSource: bad}}, opts))
			}
		})
	}
}

func TestResolveSnapshotV2Avro(t *testing.T) {
	ctx := context.Background()
	cm := storage.NewLocalChunkManager()
	root := t.TempDir()
	uri := root + "/snapshots/1/metadata/2.json"
	avroPath := root + "/snapshots/1/manifests/arbitrary.avro"
	for _, mode := range []string{"referenced", "moved", "bucket_root", "absent_prefix", "wrong_id", "wrong_version", "wrong_commit", "wrong_scope", "l0", "missing", "corrupt", "invalid_avro_path", "outside_manifest", "outside_data", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			dataPath := root + "/files/insert"
			source := &internalpb.SnapshotImportSource{
				Version: 14, SourceCommitTimestamp: 300, SourceChannel: "source", SourcePartitionId: 10,
				V2Reference: &internalpb.SnapshotImportV2Reference{SegmentId: 20, SnapshotManifestPath: avroPath, SnapshotFormatVersion: 6},
			}
			opts := Options{{Key: BackupFlag, Value: "true"}, {Key: SourceType, Value: SourceTypeSnapshot}, {Key: SnapshotSourceURI, Value: uri}, {Key: SnapshotLayout, Value: "self-contained"}}
			segment := &datapb.SegmentDescription{SegmentId: 20, PartitionId: 10, ChannelName: "source", StorageVersion: storage.StorageV2, CommitTimestamp: 300, SegmentLevel: datapb.SegmentLevel_L1}
			switch mode {
			case "referenced":
				opts[len(opts)-1].Value = "referenced"
			case "moved":
				source.V2Reference.OriginalKeyPrefix = proto.String("old")
				dataPath = "old/files/insert"
			case "bucket_root":
				source.V2Reference.OriginalKeyPrefix = proto.String("")
				dataPath = "files/insert"
			case "wrong_id":
				segment.SegmentId++
			case "wrong_version":
				segment.StorageVersion = storage.StorageV3
			case "wrong_commit":
				segment.CommitTimestamp++
			case "wrong_scope":
				segment.ChannelName = "other"
			case "l0":
				segment.SegmentLevel = datapb.SegmentLevel_L0
			case "missing":
				source.V2Reference.SnapshotManifestPath += ".missing"
			case "outside_manifest":
				source.V2Reference.SnapshotManifestPath = "/outside/manifest"
			case "invalid_avro_path":
				source.V2Reference.SnapshotManifestPath = avroPath + "/../data"
			case "outside_data":
				dataPath = "/outside/insert"
			}
			segment.Binlogs = []*datapb.FieldBinlog{{FieldID: 7, Binlogs: []*datapb.Binlog{{LogPath: dataPath}}}}
			data, err := snapshotio.MarshalSegmentManifest(segment)
			require.NoError(t, err)
			if mode == "corrupt" {
				data = []byte("not avro")
			}
			require.NoError(t, cm.Write(ctx, avroPath, data))
			readCtx := ctx
			if mode == "cancel" {
				c, cancel := context.WithCancel(ctx)
				cancel()
				readCtx = c
			}
			resolved, err := resolveSnapshotV2Source(readCtx, cm, source, opts)
			success := mode == "referenced" || mode == "moved" || mode == "bucket_root" || mode == "absent_prefix" || mode == "outside_data"
			if success {
				require.NoError(t, err)
				validate, err := SnapshotPathValidator(opts, cm)
				require.NoError(t, err)
				if mode == "outside_data" {
					require.Error(t, validate(resolved.Binlogs[0].Binlogs[0].LogPath))
				} else {
					require.Equal(t, root+"/files/insert", resolved.Binlogs[0].Binlogs[0].LogPath)
				}
			} else {
				require.Error(t, err)
				require.Nil(t, resolved)
			}
		})
	}
}

func TestSnapshotV2CrossBucketRebase(t *testing.T) {
	type bucketIdentity interface{ BucketName() string }
	type sourceCM struct {
		storage.ChunkManager
		bucketIdentity
	}
	cm := &sourceCM{}
	bucket := mockey.Mock((*sourceCM).BucketName).Return("new-bucket").Build()
	defer bucket.UnPatch()
	for _, mode := range []string{"moved", "same_prefix", "bucket_root", "unmatched_prefix", "credentials", "referenced"} {
		t.Run(mode, func(t *testing.T) {
			source := &internalpb.SnapshotImportSource{Version: 10, V2Reference: &internalpb.SnapshotImportV2Reference{SegmentId: 20, SnapshotManifestPath: "new/snapshots/1/manifests/20.avro", SnapshotFormatVersion: 6, OriginalKeyPrefix: proto.String("old")}}
			opts := Options{{Key: BackupFlag, Value: "true"}, {Key: SourceType, Value: SourceTypeSnapshot}, {Key: SnapshotSourceURI, Value: "s3://new-bucket/new/snapshots/1/metadata/2.json"}, {Key: SnapshotLayout, Value: "self-contained"}}
			path := "s3://old-bucket/old/files/insert"
			wantPath := "new/files/insert"
			if mode == "same_prefix" {
				source.V2Reference.OriginalKeyPrefix = proto.String("new")
				path = "s3://old-bucket/new/files/insert"
			}
			if mode == "bucket_root" {
				source.V2Reference.OriginalKeyPrefix = proto.String("")
				source.V2Reference.SnapshotManifestPath = "snapshots/1/manifests/20.avro"
				opts[2].Value = "s3://new-bucket/snapshots/1/metadata/2.json"
				path, wantPath = "s3://old-bucket/files/insert", "files/insert"
			}
			if mode == "unmatched_prefix" {
				path = "s3://old-bucket/other/files/insert"
			}
			if mode == "credentials" {
				path = "s3://user:secret@old-bucket/old/files/insert"
			}
			if mode == "referenced" {
				source.V2Reference.OriginalKeyPrefix = nil
				opts[len(opts)-1].Value = "referenced"
			}
			data, err := snapshotio.MarshalSegmentManifest(&datapb.SegmentDescription{
				SegmentId: 20, StorageVersion: storage.StorageV2, SegmentLevel: datapb.SegmentLevel_L1,
				Binlogs: []*datapb.FieldBinlog{{FieldID: 7, Binlogs: []*datapb.Binlog{{LogPath: path}}}},
			})
			require.NoError(t, err)
			read := mockey.Mock((*sourceCM).Read).Return(data, nil).Build()
			defer read.UnPatch()
			segment, err := resolveSnapshotV2Source(context.Background(), cm, source, opts)
			if mode == "credentials" || mode == "referenced" {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			validate, err := SnapshotPathValidator(opts, cm)
			require.NoError(t, err)
			err = validate(segment.Binlogs[0].Binlogs[0].LogPath)
			if mode == "unmatched_prefix" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, wantPath, segment.Binlogs[0].Binlogs[0].LogPath)
			}
		})
	}
}

func TestSnapshotV2ReaderFactoryDispatch(t *testing.T) {
	paramtable.Init()
	for _, mode := range []string{"private_v2", "shared_v2", "avro_failure", "mixed_v3"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			cm := storage.NewLocalChunkManager()
			cfg := &indexpb.StorageConfig{StorageType: "local"}
			opts := Options{
				{Key: BackupFlag, Value: "true"},
				{Key: SourceType, Value: SourceTypeSnapshot},
				{Key: SnapshotSourceURI, Value: "s3://bucket/root/snapshots/1/metadata/2.json"},
				{Key: SnapshotLayout, Value: "referenced"},
			}
			source := &internalpb.SnapshotImportSource{
				Version: 10, SourceCommitTimestamp: 300,
				V2Reference: &internalpb.SnapshotImportV2Reference{SegmentId: 20, SnapshotManifestPath: "root/snapshots/1/manifests/20.avro", SnapshotFormatVersion: 6},
			}
			segment := &datapb.SegmentDescription{
				SegmentId: 20, StorageVersion: storage.StorageV2, SegmentLevel: datapb.SegmentLevel_L1, CommitTimestamp: 300,
				Binlogs: []*datapb.FieldBinlog{{FieldID: 7, Binlogs: []*datapb.Binlog{{LogPath: "root/data"}}}},
			}
			var shared *binlog.SnapshotL0Deletes
			if mode == "shared_v2" {
				source.Version, source.SourceChannel, source.SourcePartitionId = 14, "source", 10
				shared = &binlog.SnapshotL0Deletes{}
			}
			if mode == "mixed_v3" {
				source.V2Reference = nil
				source.ManifestPath = packed.MarshalManifestPath("root/data", 3)
			}
			before := proto.Clone(source)
			var readErr error
			if mode == "avro_failure" {
				readErr = merr.ErrIoPermissionDenied
			}
			avro := mockey.Mock((*snapshotstorage.SnapshotReader).ReadSegmentManifest).Return(segment, readErr).Build()
			defer avro.UnPatch()
			v2 := mockey.Mock(binlog.NewStorageV2SnapshotReader).When(func(_ context.Context, gotCM storage.ChunkManager, _ *schemapb.CollectionSchema,
				gotCfg *indexpb.StorageConfig, gotSegment *datapb.SegmentDescription, _, _ uint64, _ int, _ binlog.SourceEncryption,
				gotSource *internalpb.SnapshotImportSource, budget int64, gotShared *binlog.SnapshotL0Deletes, _ func(string) error,
			) bool {
				require.Same(t, cm, gotCM)
				require.Same(t, cfg, gotCfg)
				require.EqualValues(t, 1, gotSource.Version)
				require.EqualValues(t, 300, gotSource.SourceCommitTimestamp)
				require.Empty(t, gotSource.SourceChannel)
				require.EqualValues(t, 1024, budget)
				require.Same(t, shared, gotShared)
				if shared == nil {
					require.Equal(t, segment.Binlogs, gotSegment.Binlogs)
				} else {
					require.Nil(t, gotSegment, "shared reads must borrow prepared inputs without reopening Avro")
				}
				return true
			}).Return(nil, merr.ErrIoKeyNotFound).Build()
			defer v2.UnPatch()
			v3 := mockey.Mock(binlog.NewStorageV3ManifestReader).When(func(_ context.Context, _ storage.ChunkManager, _ *schemapb.CollectionSchema,
				_ *indexpb.StorageConfig, path string, _, _ uint64, _ int, _ binlog.SourceEncryption,
				gotSource *internalpb.SnapshotImportSource, _ int64, _ func(string) error,
			) bool {
				require.Equal(t, source.ManifestPath, path)
				require.EqualValues(t, 1, gotSource.Version)
				return true
			}).Return(nil, merr.ErrIoKeyNotFound).Build()
			defer v3.UnPatch()
			_, err := NewReaderFactory(ctx, cm, cfg, opts).NewReader(ctx, nil, &internalpb.ImportFile{SnapshotSource: source}, 1024, 1024, shared)
			if mode == "avro_failure" {
				require.ErrorIs(t, err, readErr)
				require.Zero(t, v2.Times())
			} else {
				require.ErrorIs(t, err, merr.ErrIoKeyNotFound)
				if mode == "mixed_v3" {
					require.Equal(t, 1, v3.Times())
					require.Zero(t, v2.Times())
				} else {
					require.Equal(t, 1, v2.Times())
					require.Zero(t, v3.Times())
				}
			}
			if mode == "shared_v2" || mode == "mixed_v3" {
				require.Zero(t, avro.Times())
			} else {
				require.Equal(t, 1, avro.Times())
			}
			require.True(t, proto.Equal(before, source), "reader adaptation must preserve the durable descriptor")
		})
	}
}

func TestSnapshotV2FactoryPreparesMixedDeletes(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	cm := storage.NewLocalChunkManager()
	cfg := &indexpb.StorageConfig{StorageType: "local"}
	opts := Options{
		{Key: BackupFlag, Value: "true"},
		{Key: SourceType, Value: SourceTypeSnapshot},
		{Key: SnapshotSourceURI, Value: "s3://bucket/root/snapshots/1/metadata/2.json"},
		{Key: SnapshotLayout, Value: "referenced"},
	}
	v2 := &internalpb.SnapshotImportSource{
		Version: 14, SourceChannel: "source", SourcePartitionId: 10,
		V2Reference: &internalpb.SnapshotImportV2Reference{SegmentId: 20, SnapshotManifestPath: "root/snapshots/1/manifests/20.avro", SnapshotFormatVersion: 6},
	}
	v3 := &internalpb.SnapshotImportSource{Version: 14, SourceChannel: "source", SourcePartitionId: 10, ManifestPath: packed.MarshalManifestPath("root/data", 1)}
	files := []*internalpb.ImportFile{{SnapshotSource: v2}, {SnapshotSource: v3}}
	l0 := &internalpb.SnapshotImportL0Source{SourceChannel: "source", SourcePartitionId: 10}
	avro := mockey.Mock((*snapshotstorage.SnapshotReader).ReadSegmentManifest).Return(&datapb.SegmentDescription{
		SegmentId: 20, StorageVersion: storage.StorageV2, SegmentLevel: datapb.SegmentLevel_L1, ChannelName: "source", PartitionId: 10,
	}, nil).Build()
	defer avro.UnPatch()
	want := &binlog.SnapshotL0Deletes{}
	build := mockey.Mock(binlog.BuildSnapshotDeleteMasksWithV2).To(func(gotCtx context.Context, gotCM storage.ChunkManager,
		_ *schemapb.CollectionSchema, gotCfg *indexpb.StorageConfig, sources []*internalpb.SnapshotImportSource,
		gotL0 *internalpb.SnapshotImportL0Source, _, _ uint64, deletes, bitmaps int64, _ binlog.SourceEncryption,
		_ func(string) error, resolve binlog.SnapshotV2Resolver,
	) (*binlog.SnapshotL0Deletes, error) {
		require.Same(t, cm, gotCM)
		require.Same(t, cfg, gotCfg)
		require.Equal(t, []*internalpb.SnapshotImportSource{v2, v3}, sources)
		require.Same(t, l0, gotL0)
		require.EqualValues(t, 1024, deletes)
		require.EqualValues(t, 2048, bitmaps)
		_, err := resolve(gotCtx, v2)
		require.NoError(t, err)
		return want, nil
	}).Build()
	defer build.UnPatch()
	got, err := NewReaderFactory(ctx, cm, cfg, opts).PrepareSnapshotDeletes(ctx, nil, files, l0, 1024, 2048)
	require.NoError(t, err)
	require.Same(t, want, got)
	require.Equal(t, 1, avro.Times())
}

func TestSnapshotSourceVersionCapabilities(t *testing.T) {
	// Explicit wire fixtures prevent an encoder/decoder pair from agreeing on
	// a new numbering scheme that would break persisted jobs.
	cases := []struct {
		legacy, v2   uint32
		capabilities SnapshotSourceCapabilities
	}{
		{1, 10, SnapshotSourceCapabilities{}},
		{2, 11, SnapshotSourceCapabilities{ExternalStorage: true}},
		{3, 12, SnapshotSourceCapabilities{PartitionMapping: true}},
		{4, 13, SnapshotSourceCapabilities{ExternalStorage: true, PartitionMapping: true}},
		{5, 14, SnapshotSourceCapabilities{SharedL0: true}},
		{6, 15, SnapshotSourceCapabilities{ExternalStorage: true, SharedL0: true}},
		{7, 16, SnapshotSourceCapabilities{PartitionMapping: true, SharedL0: true}},
		{8, 17, SnapshotSourceCapabilities{ExternalStorage: true, PartitionMapping: true, SharedL0: true}},
	}
	for _, tc := range cases {
		for _, version := range []uint32{tc.legacy, tc.v2} {
			t.Run(fmt.Sprint(version), func(t *testing.T) {
				want := tc.capabilities
				want.V2Reference = version == tc.v2
				got, valid := DecodeSnapshotSourceVersion(version)
				require.True(t, valid)
				require.Equal(t, want, got)
				require.Equal(t, version, EncodeSnapshotSourceVersion(want))
				source := &internalpb.SnapshotImportSource{Version: version}
				require.Equal(t, want.SharedL0, SnapshotSourceUsesSharedL0(source))
				readerCapabilities, valid := DecodeSnapshotSourceVersion(SnapshotSourceContractVersion(source))
				require.True(t, valid)
				want.SharedL0, want.V2Reference = false, false
				require.Equal(t, want, readerCapabilities)
				require.Equal(t, version, source.GetVersion())
			})
		}
	}
	for _, version := range []uint32{0, SnapshotPreparationVersion, 18, ^uint32(0)} {
		got, valid := DecodeSnapshotSourceVersion(version)
		require.False(t, valid)
		require.Equal(t, SnapshotSourceCapabilities{}, got)
		source := &internalpb.SnapshotImportSource{Version: version}
		require.False(t, SnapshotSourceUsesSharedL0(source))
		require.Equal(t, version, SnapshotSourceContractVersion(source))
	}
	require.False(t, SnapshotSourceUsesSharedL0(nil))
	require.Zero(t, SnapshotSourceContractVersion(nil))
}
