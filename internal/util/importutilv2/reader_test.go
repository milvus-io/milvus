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
	snapshotstorage "github.com/milvus-io/milvus/internal/snapshotio/storage"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/importutilv2/binlog"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

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
	options := Options{{Key: BackupFlag, Value: "true"}, {Key: SourceType, Value: SourceTypeSnapshot},
		{Key: ExternalSpec, Value: `{"extfs":{"region":"us-east-1"}}`},
		{Key: SnapshotSourceURI, Value: "s3://source/root/snapshots/1/metadata/2.json"}}
	file := &internalpb.ImportFile{SnapshotSource: &internalpb.SnapshotImportSource{
		Version: 2, ManifestPath: packed.MarshalManifestPath("root/data/1", 7),
		LegacyL0Deltalogs: []string{"root/delta"}, SourceCommitTimestamp: 300,
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
		buffer int, ezk string, source *internalpb.SnapshotImportSource, budget int64,
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
				valid = true
			case "oversized":
				source.LegacyL0Deltalogs = []string{strings.Repeat("x", SnapshotSourcePlanMaxBytes)}
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
