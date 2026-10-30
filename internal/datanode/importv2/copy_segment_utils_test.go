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

package importv2

import (
	"context"
	"fmt"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	objectstore "github.com/milvus-io/milvus/pkg/v3/objectstorage/store"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	snapshotstorage "github.com/milvus-io/milvus/pkg/v3/snapshotio"
	snapshotformat "github.com/milvus-io/milvus/pkg/v3/snapshotio/format"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type copySegmentExportStoreTarget struct{ objectstore.ObjectStore }

type copySegmentStoreTarget struct {
	objectstore.ObjectStore
}

func newCopySegmentStoreMock(
	t *testing.T,
	copyFn func(context.Context, string, string, string, string) error,
) objectstore.ObjectStore {
	t.Helper()
	target := &copySegmentStoreTarget{}
	mockCopy := mockey.Mock((*copySegmentStoreTarget).Copy).To(
		func(ctx context.Context, req objectstore.CopyRequest) (int64, error) {
			srcBucket := req.Source.Bucket
			srcObject := req.Source.Key
			dstBucket := req.Destination.Bucket
			dstObject := req.Destination.Key
			if copyFn == nil {
				return 0, nil
			}
			return 0, copyFn(ctx, srcBucket, srcObject, dstBucket, dstObject)
		},
	).Build()
	t.Cleanup(func() { mockCopy.UnPatch() })
	return target
}

// manifestInventoryForTest adapts opaque physical-file fixtures to the native
// reader boundary. Tests still execute the real shared inventory and copy path.
func manifestInventoryForTest(paths []string, lobs []packed.LobFileInfo) *snapshotformat.NativeManifestFiles {
	files := &snapshotformat.NativeManifestFiles{LobFiles: lobs}
	for _, file := range paths {
		switch {
		case strings.Contains(file, "/_metadata/"), strings.Contains(file, "/_manifest/"):
			files.ManifestFile = file
		case strings.Contains(file, "/_stats/"):
			files.StatsFiles = append(files.StatsFiles, file)
		case strings.Contains(file, "/_delta/"):
			files.DeltaFiles = append(files.DeltaFiles, file)
		default:
			files.ColumnFiles = append(files.ColumnFiles, file)
		}
	}
	return files
}

// mockCopiedManifestIndexEntries stubs the conservative enumeration performed
// when the source snapshot does not prove its manifest index section empty.
func mockCopiedManifestIndexEntries(t *testing.T, entries []packed.ManifestIndexInfo) {
	t.Helper()
	mock := mockey.Mock(packed.GetManifestIndexInfos).Return(entries, nil).Build()
	t.Cleanup(func() { mock.UnPatch() })
}

func TestCopyObjectWithTimeout(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		t.Run(fmt.Sprintf("size failure retains destination canceled=%t", canceled), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			target := &copySegmentStoreTarget{}
			defer mockey.Mock((*copySegmentStoreTarget).Copy).To(func(context.Context, objectstore.CopyRequest) (int64, error) {
				if canceled {
					cancel()
				}
				return 0, merr.Wrap(&objectstore.CopySizeError{Err: merr.ErrIoPermissionDenied}, "target stat")
			}).Build().UnPatch()
			files, err := copySegmentFiles(ctx, make(chan struct{}, 1),
				[]snapshotstorage.FileCopy{{SourcePath: "src", DestinationPath: "dst"}},
				target, "source", "target", nil)
			if canceled {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.ErrorIs(t, err, merr.ErrIoPermissionDenied)
			}
			require.Equal(t, []string{"dst"}, files)
		})
	}
	for _, canceled := range []bool{false, true} {
		t.Run(fmt.Sprintf("confirmed size canceled=%t", canceled), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			target := &copySegmentStoreTarget{}
			defer mockey.Mock((*copySegmentStoreTarget).Copy).To(func(context.Context, objectstore.CopyRequest) (int64, error) {
				if canceled {
					cancel()
				}
				return 17, nil
			}).Build().UnPatch()
			var bytes int64
			err := copyObjectWithTimeout(ctx, target, "source", "object", "target", "object", func(size int64) { bytes = size })
			require.EqualValues(t, 17, bytes)
			if canceled {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.NoError(t, err)
			}
		})
	}
	t.Run("calls provider once", func(t *testing.T) {
		attempts := 0
		targetStore := newCopySegmentStoreMock(t, func(context.Context, string, string, string, string) error {
			attempts++
			return errors.New("copy failed")
		})

		err := copyObjectWithTimeout(context.Background(), targetStore, "source", "object", "target", "object", nil)

		require.Error(t, err)
		assert.Equal(t, 1, attempts)
	})

	t.Run("stops at object timeout", func(t *testing.T) {
		params := paramtable.Get()
		key := params.DataNodeCfg.ImportCopyObjectTimeout.Key
		require.NoError(t, params.Save(key, "1"))
		t.Cleanup(func() { require.NoError(t, params.Reset(key)) })

		attempts := 0
		targetStore := newCopySegmentStoreMock(t, func(ctx context.Context, _, _, _, _ string) error {
			attempts++
			<-ctx.Done()
			return errors.New("provider hid context error")
		})
		start := time.Now()

		err := copyObjectWithTimeout(context.Background(), targetStore, "source", "object", "target", "object", nil)

		require.Error(t, err)
		assert.ErrorIs(t, err, context.DeadlineExceeded)
		assert.Equal(t, 1, attempts)
		assert.Less(t, time.Since(start), 2*time.Second)
	})
}

func TestGenerateTargetPath(t *testing.T) {
	source := &datapb.CopySegmentSource{
		CollectionId: 111,
		PartitionId:  222,
		SegmentId:    333,
	}
	target := &datapb.CopySegmentTarget{
		TargetRootPath: "files",
		CollectionId:   444,
		PartitionId:    555,
		SegmentId:      666,
	}

	tests := []struct {
		targetRoot string
		name       string
		sourcePath string
		wantPath   string
		wantErr    bool
	}{
		{
			name:       "insert binlog path",
			sourcePath: "files/insert_log/111/222/333/100/log1.log",
			wantPath:   "files/insert_log/444/555/666/100/log1.log",
			wantErr:    false,
		},
		{
			name:       "delta binlog path",
			sourcePath: "files/delta_log/111/222/333/100/log1.log",
			wantPath:   "files/delta_log/444/555/666/100/log1.log",
			wantErr:    false,
		},
		{
			name:       "stats binlog path",
			sourcePath: "files/stats_log/111/222/333/100/log1.log",
			wantPath:   "files/stats_log/444/555/666/100/log1.log",
			wantErr:    false,
		},
		{
			name:       "bm25 binlog path",
			sourcePath: "files/bm25_stats/111/222/333/100/log1.log",
			wantPath:   "files/bm25_stats/444/555/666/100/log1.log",
			wantErr:    false,
		},
		{
			name:       "v3 bm25 stats numeric log id path",
			sourcePath: "files/insert_log/111/222/333/_stats/bm25.100/12345",
			wantPath:   "files/insert_log/444/555/666/_stats/bm25.100/12345",
			wantErr:    false,
		},
		{
			name:       "local complete filesystem path",
			targetRoot: "/var/lib/milvus/data",
			sourcePath: "/var/lib/milvus/data/insert_log/111/222/333/_data/0",
			wantPath:   "/var/lib/milvus/data/insert_log/444/555/666/_data/0",
			wantErr:    false,
		},
		{
			name:       "external component in regular binlog root is not external table",
			sourcePath: "files/external/insert_log/111/222/333/100/log1.log",
			wantPath:   "files/insert_log/444/555/666/100/log1.log",
			wantErr:    false,
		},
		{
			name:       "invalid path - no log type",
			sourcePath: "files/111/222/333/100/log1.log",
			wantPath:   "",
			wantErr:    true,
		},
		{
			name:       "invalid path - too short",
			sourcePath: "files/insert_log/111",
			wantPath:   "",
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			target.TargetRootPath = tt.targetRoot
			if target.TargetRootPath == "" {
				target.TargetRootPath = "files"
			}
			gotPath, err := generateTargetPath(tt.sourcePath, source, target)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.wantPath, gotPath)
			}
		})
	}
}

func TestGenerateMappingsFromFiles_ExternalTable(t *testing.T) {
	source := &datapb.CopySegmentSource{
		CollectionId:         111,
		PartitionId:          222,
		SegmentId:            333,
		StorageVersion:       storage.StorageV3,
		ManifestPath:         packed.MarshalManifestPath("files/insert_log/111/222/333", 1),
		IsExternalCollection: true,
	}
	target := &datapb.CopySegmentTarget{
		TargetRootPath: "files",
		CollectionId:   444,
		PartitionId:    555,
		SegmentId:      666,
	}
	files := &SegmentFiles{
		InsertBinlogs: []string{"files/insert_log/111/222/333/_metadata/manifest.json"},
	}

	mappings, err := generateMappingsFromFiles(files, source, target)
	assert.NoError(t, err)
	assert.Equal(t,
		"files/insert_log/444/555/666/_metadata/manifest.json",
		mappings["files/insert_log/111/222/333/_metadata/manifest.json"])
}

func TestCollectSegmentFiles_UsesSourceStorageConfigForV3(t *testing.T) {
	for _, prefix := range []string{"", "s3://foreign-source/"} {
		t.Run(prefix, func(t *testing.T) {
			cfg := &indexpb.StorageConfig{BucketName: "foreign-source", RootPath: "foreign-root"}
			manifest := packed.MarshalManifestPath(prefix+"foreign-root/files/insert_log/100/1/10", 2)
			calls := 0
			defer mockey.Mock(snapshotstorage.ReadNativeManifestFiles).To(func(got string, gotCfg *indexpb.StorageConfig) (*snapshotformat.NativeManifestFiles, error) {
				calls++
				require.Equal(t, manifest, got)
				require.Same(t, cfg, gotCfg)
				return manifestInventoryForTest([]string{"foreign-root/files/insert_log/100/1/10/_data/0"}, []packed.LobFileInfo{{Path: "foreign-root/files/insert_log/100/1/lobs/101/_data/0", FieldID: 101}}), nil
			}).Build().UnPatch()
			files, err := collectSegmentFiles(context.Background(), nil, cfg, &datapb.CopySegmentSource{SegmentId: 10, StorageVersion: storage.StorageV3, ManifestPath: manifest})
			require.NoError(t, err)
			require.Equal(t, 1, calls)
			require.Equal(t, []string{"foreign-root/files/insert_log/100/1/10/_data/0"}, files.InsertBinlogs)
			require.Equal(t, []string{"foreign-root/files/insert_log/100/1/lobs/101/_data/0"}, files.LobFiles)
		})
	}
}

func TestCollectSegmentFiles_LocalV3ManifestKeysAreCompletePaths(t *testing.T) {
	root := t.TempDir()
	cfg := &indexpb.StorageConfig{StorageType: "local", RootPath: root}
	base := path.Join(root, "insert_log/100/1/10")
	manifest := packed.MarshalManifestPath(base, 2)
	data, delta, lob := path.Join(base, "_data/0"), path.Join(base, "_delta/7"), path.Join(root, "insert_log/100/1/lobs/101/_data/0")
	defer mockey.Mock(snapshotstorage.ReadNativeManifestFiles).To(func(got string, gotCfg *indexpb.StorageConfig) (*snapshotformat.NativeManifestFiles, error) {
		require.Equal(t, manifest, got)
		require.Same(t, cfg, gotCfg)
		return manifestInventoryForTest([]string{data}, []packed.LobFileInfo{{Path: lob, FieldID: 101}}), nil
	}).Build().UnPatch()
	files, err := collectSegmentFiles(context.Background(), nil, cfg, &datapb.CopySegmentSource{
		SegmentId: 10, StorageVersion: storage.StorageV3, ManifestPath: manifest,
		DeltaBinlogs: []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{LogPath: delta}}}},
	})
	require.NoError(t, err)
	require.Equal(t, []string{data}, files.InsertBinlogs)
	require.Equal(t, []string{delta}, files.DeltaBinlogs)
	require.Equal(t, []string{lob}, files.LobFiles)
}

func TestGenerateTargetPath_RemapExternalRoot(t *testing.T) {
	source := &datapb.CopySegmentSource{
		CollectionId:   111,
		PartitionId:    222,
		SegmentId:      333,
		SourceRootPath: "source-cluster-root",
	}
	target := &datapb.CopySegmentTarget{
		CollectionId:   444,
		PartitionId:    555,
		SegmentId:      666,
		TargetRootPath: "target-cluster-root",
	}

	gotPath, err := generateTargetPath(
		"source-cluster-root/files/insert_log/111/222/333/100/log1.log",
		source,
		target,
	)

	assert.NoError(t, err)
	assert.Equal(t, "target-cluster-root/insert_log/444/555/666/100/log1.log", gotPath)
}

func TestGenerateTargetPath_RemapExternalS3Root(t *testing.T) {
	source := &datapb.CopySegmentSource{
		CollectionId:   111,
		PartitionId:    222,
		SegmentId:      333,
		SourceRootPath: "s3://bucket/source-cluster-root",
	}
	target := &datapb.CopySegmentTarget{
		CollectionId:   444,
		PartitionId:    555,
		SegmentId:      666,
		TargetRootPath: "target-cluster-root",
	}

	gotPath, err := generateTargetPath(
		"source-cluster-root/files/insert_log/111/222/333/100/log1.log",
		source,
		target,
	)

	assert.NoError(t, err)
	assert.Equal(t, "target-cluster-root/insert_log/444/555/666/100/log1.log", gotPath)
}

func TestGenerateTargetPath_RemapExternalRootToLocalCompletePath(t *testing.T) {
	source := &datapb.CopySegmentSource{
		CollectionId:   111,
		PartitionId:    222,
		SegmentId:      333,
		SourceRootPath: "s3://bucket/source-cluster-root",
	}
	target := &datapb.CopySegmentTarget{
		CollectionId:   444,
		PartitionId:    555,
		SegmentId:      666,
		TargetRootPath: "/var/lib/milvus/data",
	}

	gotPath, err := generateTargetPath(
		"source-cluster-root/files/insert_log/111/222/333/100/log1.log",
		source,
		target,
	)

	require.NoError(t, err)
	assert.Equal(t, "/var/lib/milvus/data/insert_log/444/555/666/100/log1.log", gotPath)
}

func TestGenerateTargetPath_RemapEndpointStyleExternalRoot(t *testing.T) {
	tests := []struct {
		name           string
		sourceRootPath string
		sourcePath     string
	}{
		{
			name:           "minio endpoint",
			sourceRootPath: "minio://minio.example.com/source-bucket/source-cluster-root",
			sourcePath:     "source-cluster-root/files/insert_log/111/222/333/100/log1.log",
		},
		{
			name:           "https endpoint",
			sourceRootPath: "https://storage.example.com/source-bucket/source-cluster-root",
			sourcePath:     "source-cluster-root/files/insert_log/111/222/333/100/log1.log",
		},
		{
			name:           "s3 cloud endpoint",
			sourceRootPath: "s3://s3.amazonaws.com/source-bucket/source-cluster-root",
			sourcePath:     "source-cluster-root/files/insert_log/111/222/333/100/log1.log",
		},
		{
			name:           "endpoint source path",
			sourceRootPath: "s3://s3.amazonaws.com/source-bucket/source-cluster-root",
			sourcePath:     "s3://s3.amazonaws.com/source-bucket/source-cluster-root/files/insert_log/111/222/333/100/log1.log",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source := &datapb.CopySegmentSource{
				CollectionId:   111,
				PartitionId:    222,
				SegmentId:      333,
				SourceRootPath: tt.sourceRootPath,
			}
			target := &datapb.CopySegmentTarget{
				CollectionId:   444,
				PartitionId:    555,
				SegmentId:      666,
				TargetRootPath: "target-cluster-root",
			}

			gotPath, err := generateTargetPath(tt.sourcePath, source, target)

			assert.NoError(t, err)
			assert.Equal(t, "target-cluster-root/insert_log/444/555/666/100/log1.log", gotPath)
		})
	}
}

func TestGenerateTargetPath_RemapExternalS3SourcePath(t *testing.T) {
	source := &datapb.CopySegmentSource{
		CollectionId:   111,
		PartitionId:    222,
		SegmentId:      333,
		SourceRootPath: "source-cluster-root",
	}
	target := &datapb.CopySegmentTarget{
		CollectionId:   444,
		PartitionId:    555,
		SegmentId:      666,
		TargetRootPath: "target-cluster-root",
	}

	gotPath, err := generateTargetPath(
		"s3://bucket/source-cluster-root/files/insert_log/111/222/333/100/log1.log",
		source,
		target,
	)

	assert.NoError(t, err)
	assert.Equal(t, "target-cluster-root/insert_log/444/555/666/100/log1.log", gotPath)
}

func TestGenerateTargetPath_RejectsPathOutsideExternalRoot(t *testing.T) {
	source := &datapb.CopySegmentSource{
		CollectionId:   111,
		PartitionId:    222,
		SegmentId:      333,
		SourceRootPath: "s3://source-bucket/source-cluster-root",
	}
	target := &datapb.CopySegmentTarget{
		CollectionId:   444,
		PartitionId:    555,
		SegmentId:      666,
		TargetRootPath: "target-cluster-root",
	}

	gotPath, err := generateTargetPath(
		"s3://source-bucket/source-cluster-root-v2/files/insert_log/111/222/333/100/log1.log",
		source,
		target,
	)

	assert.Error(t, err)
	assert.Empty(t, gotPath)
	assert.Contains(t, err.Error(), "outside source root")
}

func TestGenerateTargetPath_RemapExternalBucketRoot(t *testing.T) {
	source := &datapb.CopySegmentSource{
		CollectionId:   111,
		PartitionId:    222,
		SegmentId:      333,
		SourceRootPath: "s3://source-bucket",
	}
	tests := []struct {
		name       string
		targetRoot string
		want       string
	}{
		{
			name:       "configured target root",
			targetRoot: "target-cluster-root",
			want:       "target-cluster-root/insert_log/444/555/666/100/log1.log",
		},
		{
			name: "target bucket root",
			want: "insert_log/444/555/666/100/log1.log",
		},
		{
			name:       "dot target bucket root",
			targetRoot: ".",
			want:       "insert_log/444/555/666/100/log1.log",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			target := &datapb.CopySegmentTarget{
				CollectionId:   444,
				PartitionId:    555,
				SegmentId:      666,
				TargetRootPath: tt.targetRoot,
			}

			gotPath, err := generateTargetPath(
				"s3://source-bucket/files/insert_log/111/222/333/100/log1.log",
				source,
				target,
			)

			assert.NoError(t, err)
			assert.Equal(t, tt.want, gotPath)
		})
	}
}

func TestGenerateTargetPath_InternalCopyBucketRoot(t *testing.T) {
	source := &datapb.CopySegmentSource{CollectionId: 111, PartitionId: 222, SegmentId: 333}
	for _, root := range []string{"", "."} {
		t.Run(fmt.Sprintf("root_%q", root), func(t *testing.T) {
			target := &datapb.CopySegmentTarget{
				CollectionId: 444, PartitionId: 555, SegmentId: 666, TargetRootPath: root,
			}
			got, err := generateTargetPath("insert_log/111/222/333/_data/0", source, target)
			require.NoError(t, err)
			assert.Equal(t, "insert_log/444/555/666/_data/0", got)
		})
	}
}

func TestGenerateTargetPath_AllowsEquivalentExternalSchemes(t *testing.T) {
	tests := []struct {
		name       string
		sourceRoot string
		sourcePath string
	}{
		{
			name:       "GCS aliases",
			sourceRoot: "gs://source-bucket/root",
			sourcePath: "gcs://source-bucket/root/files/insert_log/111/222/333/100/log1.log",
		},
		{
			name:       "Azure aliases",
			sourceRoot: "az://blob.core.windows.net/source-container/root",
			sourcePath: "azure://blob.core.windows.net/source-container/root/files/insert_log/111/222/333/100/log1.log",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source := &datapb.CopySegmentSource{
				CollectionId:   111,
				PartitionId:    222,
				SegmentId:      333,
				SourceRootPath: tt.sourceRoot,
			}
			target := &datapb.CopySegmentTarget{
				CollectionId:   444,
				PartitionId:    555,
				SegmentId:      666,
				TargetRootPath: "target-cluster-root",
			}

			gotPath, err := generateTargetPath(tt.sourcePath, source, target)

			require.NoError(t, err)
			assert.Equal(t, "target-cluster-root/insert_log/444/555/666/100/log1.log", gotPath)
		})
	}
}

func TestGenerateTargetPath_RejectsDifferentExternalLocation(t *testing.T) {
	tests := []struct {
		name       string
		sourceRoot string
		sourcePath string
	}{
		{
			name:       "bucket",
			sourceRoot: "s3://source-bucket",
			sourcePath: "s3://other-bucket/files/insert_log/111/222/333/100/log1.log",
		},
		{
			name:       "endpoint",
			sourceRoot: "minio://source.example.com/source-bucket",
			sourcePath: "minio://other.example.com/source-bucket/files/insert_log/111/222/333/100/log1.log",
		},
		{
			name:       "scheme",
			sourceRoot: "s3://source-bucket",
			sourcePath: "gs://source-bucket/files/insert_log/111/222/333/100/log1.log",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source := &datapb.CopySegmentSource{
				CollectionId:   111,
				PartitionId:    222,
				SegmentId:      333,
				SourceRootPath: tt.sourceRoot,
			}
			target := &datapb.CopySegmentTarget{
				CollectionId:   444,
				PartitionId:    555,
				SegmentId:      666,
				TargetRootPath: "target-cluster-root",
			}

			gotPath, err := generateTargetPath(tt.sourcePath, source, target)

			assert.Error(t, err)
			assert.Empty(t, gotPath)
			assert.Contains(t, err.Error(), "does not match source root")
		})
	}
}

func TestGenerateTargetLOBPath(t *testing.T) {
	source := &datapb.CopySegmentSource{
		CollectionId: 111,
		PartitionId:  222,
		SegmentId:    333,
	}
	target := &datapb.CopySegmentTarget{
		TargetRootPath: "files",
		CollectionId:   444,
		PartitionId:    555,
		SegmentId:      666,
	}

	tests := []struct {
		targetRoot string
		name       string
		sourcePath string
		wantPath   string
		wantErr    bool
	}{
		{
			name:       "standard LOB file path",
			sourcePath: "files/insert_log/111/222/lobs/100/_data/abc123.vx",
			wantPath:   "files/insert_log/444/555/lobs/100/_data/abc123.vx",
			wantErr:    false,
		},
		{
			name:       "LOB file with nested field path",
			targetRoot: "root/data",
			sourcePath: "root/data/insert_log/111/222/lobs/200/_data/xyz.vx",
			wantPath:   "root/data/insert_log/444/555/lobs/200/_data/xyz.vx",
			wantErr:    false,
		},
		{
			name:       "local complete filesystem path",
			targetRoot: "/var/lib/milvus/data",
			sourcePath: "/var/lib/milvus/data/insert_log/111/222/lobs/200/_data/xyz.vx",
			wantPath:   "/var/lib/milvus/data/insert_log/444/555/lobs/200/_data/xyz.vx",
			wantErr:    false,
		},
		{
			name:       "invalid path - no insert_log",
			sourcePath: "files/other/111/222/lobs/100/_data/abc.vx",
			wantPath:   "",
			wantErr:    true,
		},
		{
			name:       "invalid path - too short after insert_log",
			sourcePath: "files/insert_log/111",
			wantPath:   "",
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			target.TargetRootPath = tt.targetRoot
			if target.TargetRootPath == "" {
				target.TargetRootPath = "files"
			}
			gotPath, err := generateTargetLOBPath(tt.sourcePath, source, target)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.wantPath, gotPath)
			}
		})
	}
}

func TestGenerateTargetLOBPath_RemapExternalRoot(t *testing.T) {
	source := &datapb.CopySegmentSource{
		CollectionId:   111,
		PartitionId:    222,
		SegmentId:      333,
		SourceRootPath: "source-cluster-root/",
	}
	target := &datapb.CopySegmentTarget{
		CollectionId:   444,
		PartitionId:    555,
		SegmentId:      666,
		TargetRootPath: "target-cluster-root/",
	}

	gotPath, err := generateTargetLOBPath(
		"source-cluster-root/files/insert_log/111/222/lobs/100/_data/abc123.vx",
		source,
		target,
	)

	assert.NoError(t, err)
	assert.Equal(t, "target-cluster-root/insert_log/444/555/lobs/100/_data/abc123.vx", gotPath)
}

func TestGenerateTargetIndexPath(t *testing.T) {
	source := &datapb.CopySegmentSource{
		CollectionId: 111,
		PartitionId:  222,
		SegmentId:    333,
	}
	target := &datapb.CopySegmentTarget{
		TargetRootPath: "files",
		CollectionId:   444,
		PartitionId:    555,
		SegmentId:      666,
	}

	tests := []struct {
		targetRoot  string
		name        string
		sourcePath  string
		indexType   string
		pathVersion indexpb.IndexStorePathVersion
		wantPath    string
		wantErr     bool
	}{
		{
			name:        "vector scalar index path v0 format",
			sourcePath:  "files/index_files/1001/1/222/333/scalar_index",
			indexType:   IndexTypeVectorScalarV0,
			pathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_BUILD_ROOTED,
			wantPath:    "files/index_files/1001/1/555/666/scalar_index",
			wantErr:     false,
		},
		{
			name:       "text index path",
			sourcePath: "files/text_log/123/1/111/222/333/100/index_file",
			indexType:  IndexTypeText,
			wantPath:   "files/text_log/123/1/444/555/666/100/index_file",
			wantErr:    false,
		},
		{
			name:       "local complete filesystem path",
			targetRoot: "/var/lib/milvus/data",
			sourcePath: "/var/lib/milvus/data/text_log/123/1/111/222/333/100/index_file",
			indexType:  IndexTypeText,
			wantPath:   "/var/lib/milvus/data/text_log/123/1/444/555/666/100/index_file",
			wantErr:    false,
		},
		{
			name:       "json key index path",
			sourcePath: "files/json_key_index_log/123/1/111/222/333/100/index_file",
			indexType:  IndexTypeJSONKey,
			wantPath:   "files/json_key_index_log/123/1/444/555/666/100/index_file",
			wantErr:    false,
		},
		{
			name:       "json stats path - shared_key_index",
			sourcePath: "files/json_stats/2/123/1/111/222/333/100/shared_key_index/index_file",
			indexType:  IndexTypeJSONStats,
			wantPath:   "files/json_stats/2/123/1/444/555/666/100/shared_key_index/index_file",
			wantErr:    false,
		},
		{
			name:       "json stats path - shredding_data",
			sourcePath: "files/json_stats/2/123/1/111/222/333/100/shredding_data/data_file",
			indexType:  IndexTypeJSONStats,
			wantPath:   "files/json_stats/2/123/1/444/555/666/100/shredding_data/data_file",
			wantErr:    false,
		},
		{
			name:       "invalid - keyword not found",
			sourcePath: "files/other_index/111/222/333/100/index",
			indexType:  IndexTypeVectorScalarV0,
			wantPath:   "",
			wantErr:    true,
		},
		{
			name:       "invalid - path too short",
			sourcePath: "files/index_files/111",
			indexType:  IndexTypeVectorScalarV0,
			wantPath:   "",
			wantErr:    true,
		},
		{
			name:       "invalid - unsupported index type",
			sourcePath: "files/unknown_index/111/222/333/100/index",
			indexType:  "unknown_type",
			wantPath:   "",
			wantErr:    true,
		},
		{
			name:        "vector scalar index path v1 format",
			sourcePath:  "files/index_v1/111/222/333/1001/1/scalar_index",
			indexType:   IndexTypeVectorScalarV0,
			pathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
			wantPath:    "files/index_v1/444/555/666/1001/1/scalar_index",
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			target.TargetRootPath = tt.targetRoot
			if target.TargetRootPath == "" {
				target.TargetRootPath = "files"
			}
			gotPath, err := generateTargetIndexPath(tt.sourcePath, source, target, tt.indexType, tt.pathVersion)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.wantPath, gotPath)
			}
		})
	}
}

// TestGenerateTargetIndexPath_BuildIDMapping verifies that NewBuildIds mapping
// is applied at the correct offset for each index path format.
// Regression test: v1 VectorScalar places buildID at offset 4 (not 1).
func TestGenerateTargetIndexPath_BuildIDMapping(t *testing.T) {
	source := &datapb.CopySegmentSource{
		CollectionId: 111,
		PartitionId:  222,
		SegmentId:    333,
	}
	target := &datapb.CopySegmentTarget{
		TargetRootPath: "files",
		CollectionId:   444,
		PartitionId:    555,
		SegmentId:      666,
		NewBuildIds:    map[int64]int64{1001: 2001},
	}

	tests := []struct {
		name        string
		sourcePath  string
		indexType   string
		pathVersion indexpb.IndexStorePathVersion
		wantPath    string
	}{
		{
			name:        "v0 vector scalar maps buildID at offset 1",
			sourcePath:  "files/index_files/1001/1/222/333/scalar_index",
			indexType:   IndexTypeVectorScalarV0,
			pathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_BUILD_ROOTED,
			wantPath:    "files/index_files/2001/1/555/666/scalar_index",
		},
		{
			name:        "v1 vector scalar maps buildID at offset 4",
			sourcePath:  "files/index_v1/111/222/333/1001/1/scalar_index",
			indexType:   IndexTypeVectorScalarV0,
			pathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
			wantPath:    "files/index_v1/444/555/666/2001/1/scalar_index",
		},
		{
			name:       "text index maps buildID at offset 1",
			sourcePath: "files/text_log/1001/1/111/222/333/100/idx",
			indexType:  IndexTypeText,
			wantPath:   "files/text_log/2001/1/444/555/666/100/idx",
		},
		{
			name:       "json stats maps buildID at offset 2",
			sourcePath: "files/json_stats/2/1001/1/111/222/333/100/shared_key_index/idx",
			indexType:  IndexTypeJSONStats,
			wantPath:   "files/json_stats/2/2001/1/444/555/666/100/shared_key_index/idx",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := generateTargetIndexPath(tt.sourcePath, source, target, tt.indexType, tt.pathVersion)
			assert.NoError(t, err)
			assert.Equal(t, tt.wantPath, got)
		})
	}
}

func TestGenerateMappingsFromFiles_VectorScalarUsesSourcePathVersion(t *testing.T) {
	source := &datapb.CopySegmentSource{
		CollectionId: 111,
		PartitionId:  222,
		SegmentId:    333,
		IndexFiles: []*indexpb.IndexFilePathInfo{
			{
				BuildID:               1001,
				IndexVersion:          1,
				IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_BUILD_ROOTED,
				IndexFilePaths:        []string{"files/index_files/1001/1/222/333/v0_file"},
			},
			{
				BuildID:               1002,
				IndexVersion:          1,
				IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
				IndexFilePaths:        []string{"files/index_v1/111/222/333/1002/1/v1_file"},
			},
		},
	}
	target := &datapb.CopySegmentTarget{
		TargetRootPath: "files",
		CollectionId:   444,
		PartitionId:    555,
		SegmentId:      666,
		NewBuildIds:    map[int64]int64{1001: 2001, 1002: 2002},
	}
	files := &SegmentFiles{
		VectorScalarIndex: []string{
			"files/index_files/1001/1/222/333/v0_file",
			"files/index_v1/111/222/333/1002/1/v1_file",
		},
	}

	mappings, err := generateMappingsFromFiles(files, source, target)
	assert.NoError(t, err)
	assert.Equal(t, "files/index_files/2001/1/555/666/v0_file", mappings["files/index_files/1001/1/222/333/v0_file"])
	assert.Equal(t, "files/index_v1/444/555/666/2002/1/v1_file", mappings["files/index_v1/111/222/333/1002/1/v1_file"])
}

func TestGenerateTargetIndexPath_RemapExternalRoot(t *testing.T) {
	source := &datapb.CopySegmentSource{
		CollectionId:   111,
		PartitionId:    222,
		SegmentId:      333,
		SourceRootPath: "source-cluster-root",
	}
	target := &datapb.CopySegmentTarget{
		CollectionId:   444,
		PartitionId:    555,
		SegmentId:      666,
		TargetRootPath: "target-cluster-root",
	}

	gotPath, err := generateTargetIndexPath(
		"source-cluster-root/files/text_log/123/1/111/222/333/100/index_file",
		source,
		target,
		IndexTypeText,
		indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_BUILD_ROOTED,
	)

	assert.NoError(t, err)
	assert.Equal(t, "target-cluster-root/text_log/123/1/444/555/666/100/index_file", gotPath)
}

func TestTransformFieldBinlogs(t *testing.T) {
	mappings := map[string]string{
		"files/insert_log/111/222/333/100/log1.log": "files/insert_log/444/555/666/100/log1.log",
		"files/insert_log/111/222/333/101/log2.log": "files/insert_log/444/555/666/101/log2.log",
	}

	srcFieldBinlogs := []*datapb.FieldBinlog{
		{
			FieldID: 100,
			Binlogs: []*datapb.Binlog{
				{
					EntriesNum:    1000,
					TimestampFrom: 100,
					TimestampTo:   200,
					LogPath:       "files/insert_log/111/222/333/100/log1.log",
					LogSize:       1024,
					MemorySize:    1024,
				},
			},
		},
		{
			FieldID: 101,
			Binlogs: []*datapb.Binlog{
				{
					EntriesNum:    2000,
					TimestampFrom: 150,
					TimestampTo:   250,
					LogPath:       "files/insert_log/111/222/333/101/log2.log",
					LogSize:       2048,
					MemorySize:    2048,
				},
			},
		},
	}

	t.Run("count rows for insert logs", func(t *testing.T) {
		result, totalRows, err := transformFieldBinlogs(srcFieldBinlogs, mappings, transformFieldBinlogOptions{countRows: true})
		assert.NoError(t, err)
		assert.Equal(t, int64(3000), totalRows)
		assert.Equal(t, 2, len(result))

		// Verify first field binlog
		assert.Equal(t, int64(100), result[0].FieldID)
		assert.Equal(t, 1, len(result[0].Binlogs))
		assert.Equal(t, int64(1000), result[0].Binlogs[0].EntriesNum)
		assert.Equal(t, "files/insert_log/444/555/666/100/log1.log", result[0].Binlogs[0].LogPath)
		assert.Equal(t, int64(1024), result[0].Binlogs[0].LogSize)
		assert.Equal(t, int64(1024), result[0].Binlogs[0].MemorySize)

		// Verify second field binlog
		assert.Equal(t, int64(101), result[1].FieldID)
		assert.Equal(t, 1, len(result[1].Binlogs))
		assert.Equal(t, int64(2000), result[1].Binlogs[0].EntriesNum)
	})

	t.Run("no row counting for stats logs", func(t *testing.T) {
		result, totalRows, err := transformFieldBinlogs(srcFieldBinlogs, mappings, transformFieldBinlogOptions{})
		assert.NoError(t, err)
		assert.Equal(t, int64(0), totalRows)
		assert.Equal(t, 2, len(result))
	})

	t.Run("skip binlogs with empty path", func(t *testing.T) {
		srcWithEmpty := []*datapb.FieldBinlog{
			{
				FieldID: 100,
				Binlogs: []*datapb.Binlog{
					{
						EntriesNum: 1000,
						LogPath:    "",
					},
				},
			},
		}
		result, _, err := transformFieldBinlogs(srcWithEmpty, mappings, transformFieldBinlogOptions{})
		assert.NoError(t, err)
		assert.Equal(t, 0, len(result))
	})

	t.Run("error on missing mapping", func(t *testing.T) {
		srcWithUnmapped := []*datapb.FieldBinlog{
			{
				FieldID: 100,
				Binlogs: []*datapb.Binlog{
					{
						EntriesNum: 1000,
						LogPath:    "files/insert_log/999/888/777/100/unmapped.log",
					},
				},
			},
		}
		result, totalRows, err := transformFieldBinlogs(srcWithUnmapped, mappings, transformFieldBinlogOptions{countRows: true})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no mapping found")
		assert.Nil(t, result)
		assert.Equal(t, int64(0), totalRows)
	})
}

func TestGenerateSegmentInfoFromSource(t *testing.T) {
	source := &datapb.CopySegmentSource{
		CollectionId: 111,
		PartitionId:  222,
		SegmentId:    333,
		InsertBinlogs: []*datapb.FieldBinlog{
			{
				FieldID: 100,
				Binlogs: []*datapb.Binlog{
					{
						EntriesNum:    1000,
						TimestampFrom: 100,
						TimestampTo:   200,
						LogPath:       "files/insert_log/111/222/333/100/log1.log",
						LogSize:       1024,
					},
				},
			},
		},
		StatsBinlogs: []*datapb.FieldBinlog{
			{
				FieldID: 100,
				Binlogs: []*datapb.Binlog{
					{
						LogPath: "files/stats_log/111/222/333/100/stats1.log",
					},
				},
			},
		},
	}

	target := &datapb.CopySegmentTarget{
		CollectionId: 444,
		PartitionId:  555,
		SegmentId:    666,
	}

	mappings := map[string]string{
		"files/insert_log/111/222/333/100/log1.log":  "files/insert_log/444/555/666/100/log1.log",
		"files/stats_log/111/222/333/100/stats1.log": "files/stats_log/444/555/666/100/stats1.log",
	}

	t.Run("generate segment info", func(t *testing.T) {
		segmentInfo, err := generateSegmentInfoFromSource(source, target, mappings)

		assert.NoError(t, err)
		assert.NotNil(t, segmentInfo)
		assert.Equal(t, int64(666), segmentInfo.SegmentID)
		assert.Equal(t, int64(1000), segmentInfo.ImportedRows)
		assert.Equal(t, 1, len(segmentInfo.Binlogs))
		assert.Equal(t, 1, len(segmentInfo.Statslogs))
		assert.Equal(t, "files/insert_log/444/555/666/100/log1.log", segmentInfo.Binlogs[0].Binlogs[0].LogPath)
	})
}

func TestBuildIndexInfoFromSource(t *testing.T) {
	source := &datapb.CopySegmentSource{
		CollectionId: 111,
		PartitionId:  222,
		SegmentId:    333,
		IndexFiles: []*indexpb.IndexFilePathInfo{
			{
				FieldID:        100,
				IndexID:        1001,
				BuildID:        1002,
				IndexFilePaths: []string{"files/index_files/1002/1/222/333/index1"},
				SerializedSize: 5000,
			},
		},
		TextIndexFiles: map[int64]*datapb.TextIndexStats{
			100: {
				FieldID:    100,
				Version:    1,
				BuildID:    2001,
				Files:      []string{"files/text_log/123/1/111/222/333/100/text1"},
				LogSize:    2048,
				MemorySize: 4096,
			},
		},
		JsonKeyIndexFiles: map[int64]*datapb.JsonKeyStats{
			101: {
				FieldID:                101,
				Version:                1,
				BuildID:                3001,
				JsonKeyStatsDataFormat: 1, // Legacy format
				Files:                  []string{"files/json_key_index_log/123/1/111/222/333/101/json1"},
				MemorySize:             3072,
			},
			102: {
				FieldID:                102,
				Version:                1,
				BuildID:                3002,
				JsonKeyStatsDataFormat: 2, // New format
				Files: []string{
					"files/json_stats/2/3002/1/111/222/333/102/shared_key_index/index1",
					"files/json_stats/2/3002/1/111/222/333/102/shredding_data/data1",
				},
				MemorySize: 4096,
			},
		},
	}

	target := &datapb.CopySegmentTarget{
		CollectionId: 444,
		PartitionId:  555,
		SegmentId:    666,
	}

	mappings := map[string]string{
		"files/index_files/1002/1/222/333/index1":                           "files/index_files/1002/1/555/666/index1",
		"files/text_log/123/1/111/222/333/100/text1":                        "files/text_log/123/1/444/555/666/100/text1",
		"files/json_key_index_log/123/1/111/222/333/101/json1":              "files/json_key_index_log/123/1/444/555/666/101/json1",
		"files/json_stats/2/3002/1/111/222/333/102/shared_key_index/index1": "files/json_stats/2/3002/1/444/555/666/102/shared_key_index/index1",
		"files/json_stats/2/3002/1/111/222/333/102/shredding_data/data1":    "files/json_stats/2/3002/1/444/555/666/102/shredding_data/data1",
	}

	t.Run("error on missing vector index mapping", func(t *testing.T) {
		emptyMappings := map[string]string{}
		_, _, _, err := buildIndexInfoFromSource(source, target, emptyMappings)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no mapping found for index file")
	})

	t.Run("error on missing text index mapping", func(t *testing.T) {
		partialMappings := map[string]string{
			"files/index_files/1002/1/222/333/index1": "files/index_files/1002/1/555/666/index1",
		}
		_, _, _, err := buildIndexInfoFromSource(source, target, partialMappings)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no mapping found for text index file")
	})

	t.Run("error on missing json index mapping", func(t *testing.T) {
		partialMappings := map[string]string{
			"files/index_files/1002/1/222/333/index1":    "files/index_files/1002/1/555/666/index1",
			"files/text_log/123/1/111/222/333/100/text1": "files/text_log/123/1/444/555/666/100/text1",
		}
		_, _, _, err := buildIndexInfoFromSource(source, target, partialMappings)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no mapping found for JSON index file")
	})

	t.Run("build all index info", func(t *testing.T) {
		indexInfos, textIndexInfos, jsonKeyIndexInfos, err := buildIndexInfoFromSource(source, target, mappings)
		assert.NoError(t, err)

		// Verify vector/scalar index info (keyed by buildID, not fieldID)
		assert.Equal(t, 1, len(indexInfos))
		assert.NotNil(t, indexInfos[1002])
		assert.Equal(t, int64(100), indexInfos[1002].FieldId)
		assert.Equal(t, int64(1001), indexInfos[1002].IndexId)
		assert.Equal(t, int64(1002), indexInfos[1002].BuildId)
		assert.Equal(t, int64(5000), indexInfos[1002].IndexSize)
		assert.Equal(t, "files/index_files/1002/1/555/666/index1", indexInfos[1002].IndexFilePaths[0])

		// Verify text index info
		assert.Equal(t, 1, len(textIndexInfos))
		assert.NotNil(t, textIndexInfos[100])
		assert.Equal(t, int64(100), textIndexInfos[100].FieldID)
		assert.Equal(t, int64(2001), textIndexInfos[100].BuildID)
		assert.Equal(t, "files/text_log/123/1/444/555/666/100/text1", textIndexInfos[100].Files[0])

		// Verify JSON key index info (legacy and new formats)
		assert.Equal(t, 2, len(jsonKeyIndexInfos))

		// Legacy format (data_format = 1)
		assert.NotNil(t, jsonKeyIndexInfos[101])
		assert.Equal(t, int64(101), jsonKeyIndexInfos[101].FieldID)
		assert.Equal(t, int64(3001), jsonKeyIndexInfos[101].BuildID)
		assert.Equal(t, int64(1), jsonKeyIndexInfos[101].JsonKeyStatsDataFormat)
		assert.Equal(t, "files/json_key_index_log/123/1/444/555/666/101/json1", jsonKeyIndexInfos[101].Files[0])

		// New format (data_format = 2)
		assert.NotNil(t, jsonKeyIndexInfos[102])
		assert.Equal(t, int64(102), jsonKeyIndexInfos[102].FieldID)
		assert.Equal(t, int64(3002), jsonKeyIndexInfos[102].BuildID)
		assert.Equal(t, int64(2), jsonKeyIndexInfos[102].JsonKeyStatsDataFormat)
		assert.Equal(t, 2, len(jsonKeyIndexInfos[102].Files))
		assert.Equal(t, "files/json_stats/2/3002/1/444/555/666/102/shared_key_index/index1", jsonKeyIndexInfos[102].Files[0])
		assert.Equal(t, "files/json_stats/2/3002/1/444/555/666/102/shredding_data/data1", jsonKeyIndexInfos[102].Files[1])
	})
}

func TestBuildIndexInfoFromSource_PreservesIndexStorePathVersion(t *testing.T) {
	source := &datapb.CopySegmentSource{
		CollectionId: 111,
		PartitionId:  222,
		SegmentId:    333,
		IndexFiles: []*indexpb.IndexFilePathInfo{
			{
				FieldID:               100,
				IndexID:               10,
				BuildID:               1001,
				IndexName:             "idx_v0",
				IndexVersion:          1,
				SerializedSize:        1024,
				IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_BUILD_ROOTED,
				IndexFilePaths:        []string{"files/index_files/1001/1/222/333/v0_file"},
			},
			{
				FieldID:               101,
				IndexID:               11,
				BuildID:               1002,
				IndexName:             "idx_v1",
				IndexVersion:          1,
				SerializedSize:        2048,
				IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
				IndexFilePaths:        []string{"files/index_v1/111/222/333/1002/1/v1_file"},
			},
		},
	}
	target := &datapb.CopySegmentTarget{
		CollectionId: 444,
		PartitionId:  555,
		SegmentId:    666,
		NewBuildIds:  map[int64]int64{1001: 2001, 1002: 2002},
	}
	mappings := map[string]string{
		"files/index_files/1001/1/222/333/v0_file":  "files/index_files/2001/1/555/666/v0_file",
		"files/index_v1/111/222/333/1002/1/v1_file": "files/index_v1/444/555/666/2002/1/v1_file",
	}

	indexInfos, _, _, err := buildIndexInfoFromSource(source, target, mappings)
	assert.NoError(t, err)
	assert.Equal(t, indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_BUILD_ROOTED, indexInfos[2001].GetIndexStorePathVersion())
	assert.Equal(t, indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED, indexInfos[2002].GetIndexStorePathVersion())
}

func TestCopySegmentAndIndexFiles_NormalizesProviderSourceURI(t *testing.T) {
	const sourceURI = "s3://source-bucket/source-root/files/insert_log/111/222/333/1/10001"
	var copiedSource string
	targetStore := newCopySegmentStoreMock(t, func(_ context.Context, srcBucket, srcObject, dstBucket, dstObject string) error {
		assert.Equal(t, "source-bucket", srcBucket)
		assert.Equal(t, "target-bucket", dstBucket)
		assert.Equal(t, "target-root/insert_log/444/555/666/1/10001", dstObject)
		copiedSource = srcObject
		return nil
	})
	source := &datapb.CopySegmentSource{
		CollectionId:   111,
		PartitionId:    222,
		SegmentId:      333,
		SourceRootPath: "s3://source-bucket/source-root",
		InsertBinlogs: []*datapb.FieldBinlog{{
			FieldID: 1,
			Binlogs: []*datapb.Binlog{{
				LogPath:    sourceURI,
				EntriesNum: 10,
			}},
		}},
	}
	target := &datapb.CopySegmentTarget{
		CollectionId:   444,
		PartitionId:    555,
		SegmentId:      666,
		TargetRootPath: "target-root",
	}

	result, _, err := CopySegmentAndIndexFiles(
		context.Background(),
		make(chan struct{}, 2),
		&struct{ storage.ChunkManager }{},
		&indexpb.StorageConfig{BucketName: "source-bucket"},
		&indexpb.StorageConfig{BucketName: "target-bucket"},
		targetStore,
		"source-bucket",
		"target-bucket",
		source,
		target,
		nil,
	)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, "source-root/files/insert_log/111/222/333/1/10001", copiedSource)
}

func TestGenerateTargetIndexPath_PathTooShort(t *testing.T) {
	source := &datapb.CopySegmentSource{
		CollectionId: 111,
		PartitionId:  222,
		SegmentId:    333,
	}
	target := &datapb.CopySegmentTarget{
		CollectionId: 444,
		PartitionId:  555,
		SegmentId:    666,
	}

	tests := []struct {
		name      string
		path      string
		indexType string
	}{
		{
			name:      "text index path too short",
			path:      "files/text_log/123/1/111",
			indexType: IndexTypeText,
		},
		{
			name:      "json key index path too short",
			path:      "files/json_key_index_log/123",
			indexType: IndexTypeJSONKey,
		},
		{
			name:      "json stats path too short",
			path:      "files/json_stats/2/123/1/111",
			indexType: IndexTypeJSONStats,
		},
		{
			name:      "vector scalar path exactly at keyword",
			path:      "files/index_files",
			indexType: IndexTypeVectorScalarV0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := generateTargetIndexPath(tt.path, source, target, tt.indexType, indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_BUILD_ROOTED)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "invalid")
		})
	}
}

func TestGenerateTargetIndexPath_VectorScalarPreservesIDs(t *testing.T) {
	source := &datapb.CopySegmentSource{
		CollectionId: 111,
		PartitionId:  222,
		SegmentId:    333,
	}
	target := &datapb.CopySegmentTarget{
		TargetRootPath: "files",
		CollectionId:   444,
		PartitionId:    555,
		SegmentId:      666,
	}

	// Verify buildID (1001) and indexVersion (1) are preserved, NOT replaced
	// partitionID (222->555) and segmentID (333->666) ARE replaced
	// collectionID is NOT in path at all
	result, err := generateTargetIndexPath("files/index_files/1001/1/222/333/HNSW_SQ_3", source, target, IndexTypeVectorScalarV0, indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_BUILD_ROOTED)
	assert.NoError(t, err)
	assert.Equal(t, "files/index_files/1001/1/555/666/HNSW_SQ_3", result)

	// Verify collectionID (111) does NOT appear in target path
	assert.NotContains(t, result, "111")
	assert.NotContains(t, result, "444") // collectionID should NOT be injected
}

func TestGenerateTargetPath_AllConstants(t *testing.T) {
	source := &datapb.CopySegmentSource{
		CollectionId: 111,
		PartitionId:  222,
		SegmentId:    333,
	}
	target := &datapb.CopySegmentTarget{
		CollectionId: 444,
		PartitionId:  555,
		SegmentId:    666,
	}

	// Verify all constants match the correct storage path names
	tests := []struct {
		name     string
		logType  string
		expected string
	}{
		{"insert uses insert_log", BinlogTypeInsert, "insert_log"},
		{"delta uses delta_log", BinlogTypeDelta, "delta_log"},
		{"stats uses stats_log", BinlogTypeStats, "stats_log"},
		{"bm25 uses bm25_stats", BinlogTypeBM25, "bm25_stats"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := fmt.Sprintf("files/%s/111/222/333/100/log1", tt.expected)
			result, err := generateTargetPath(path, source, target)
			assert.NoError(t, err)
			assert.Contains(t, result, tt.expected)
			assert.Contains(t, result, "444")
			assert.Contains(t, result, "555")
			assert.Contains(t, result, "666")
		})
	}
}

func TestGenerateSegmentInfoFromSource_AllBinlogTypes(t *testing.T) {
	source := &datapb.CopySegmentSource{
		CollectionId: 111,
		PartitionId:  222,
		SegmentId:    333,
		InsertBinlogs: []*datapb.FieldBinlog{
			{
				FieldID: 100,
				Binlogs: []*datapb.Binlog{
					{EntriesNum: 500, LogPath: "files/insert_log/111/222/333/100/log1"},
				},
			},
		},
		StatsBinlogs: []*datapb.FieldBinlog{
			{
				FieldID: 100,
				Binlogs: []*datapb.Binlog{
					{LogPath: "files/stats_log/111/222/333/100/stats1"},
				},
			},
		},
		DeltaBinlogs: []*datapb.FieldBinlog{
			{
				FieldID: 100,
				Binlogs: []*datapb.Binlog{
					{EntriesNum: 10, LogPath: "files/delta_log/111/222/333/100/delta1"},
				},
			},
		},
		Bm25Binlogs: []*datapb.FieldBinlog{
			{
				FieldID: 100,
				Binlogs: []*datapb.Binlog{
					{LogPath: "files/bm25_stats/111/222/333/100/bm25_1"},
				},
			},
		},
	}

	target := &datapb.CopySegmentTarget{
		CollectionId: 444,
		PartitionId:  555,
		SegmentId:    666,
	}

	mappings := map[string]string{
		"files/insert_log/111/222/333/100/log1":   "files/insert_log/444/555/666/100/log1",
		"files/stats_log/111/222/333/100/stats1":  "files/stats_log/444/555/666/100/stats1",
		"files/delta_log/111/222/333/100/delta1":  "files/delta_log/444/555/666/100/delta1",
		"files/bm25_stats/111/222/333/100/bm25_1": "files/bm25_stats/444/555/666/100/bm25_1",
	}

	segmentInfo, err := generateSegmentInfoFromSource(source, target, mappings)
	assert.NoError(t, err)
	assert.NotNil(t, segmentInfo)
	assert.Equal(t, int64(666), segmentInfo.SegmentID)
	assert.Equal(t, int64(500), segmentInfo.ImportedRows) // Only insert rows counted
	assert.Equal(t, 1, len(segmentInfo.Binlogs))
	assert.Equal(t, 1, len(segmentInfo.Statslogs))
	assert.Equal(t, 1, len(segmentInfo.Deltalogs))
	assert.Equal(t, 1, len(segmentInfo.Bm25Logs))
	assert.Equal(t, "files/delta_log/444/555/666/100/delta1", segmentInfo.Deltalogs[0].Binlogs[0].LogPath)
	assert.Equal(t, "files/bm25_stats/444/555/666/100/bm25_1", segmentInfo.Bm25Logs[0].Binlogs[0].LogPath)
}

func TestGenerateSegmentInfoFromSource_EmptySource(t *testing.T) {
	source := &datapb.CopySegmentSource{}
	target := &datapb.CopySegmentTarget{
		CollectionId: 444,
		PartitionId:  555,
		SegmentId:    666,
	}

	segmentInfo, err := generateSegmentInfoFromSource(source, target, map[string]string{})
	assert.NoError(t, err)
	assert.NotNil(t, segmentInfo)
	assert.Equal(t, int64(666), segmentInfo.SegmentID)
	assert.Equal(t, int64(0), segmentInfo.ImportedRows)
	assert.Equal(t, 0, len(segmentInfo.Binlogs))
	assert.Equal(t, 0, len(segmentInfo.Statslogs))
	assert.Equal(t, 0, len(segmentInfo.Deltalogs))
	assert.Equal(t, 0, len(segmentInfo.Bm25Logs))
}

// TestBuildIndexInfoFromSource_MultipleIndexesPerField verifies that multiple indexes
// on the same field (e.g., JSON path indexes) are all preserved in the result map.
// This is the core scenario for the fix: using buildID as map key instead of fieldID.
func TestBuildIndexInfoFromSource_MultipleIndexesPerField(t *testing.T) {
	source := &datapb.CopySegmentSource{
		CollectionId: 111,
		PartitionId:  222,
		SegmentId:    333,
		IndexFiles: []*indexpb.IndexFilePathInfo{
			{
				FieldID:        100, // Same field
				IndexID:        1001,
				BuildID:        2001,
				IndexName:      "idx_category",
				IndexFilePaths: []string{"files/index_files/2001/1/222/333/index1"},
				SerializedSize: 3000,
			},
			{
				FieldID:        100, // Same field, different path index
				IndexID:        1002,
				BuildID:        2002,
				IndexName:      "idx_price",
				IndexFilePaths: []string{"files/index_files/2002/1/222/333/index2"},
				SerializedSize: 4000,
			},
		},
	}

	target := &datapb.CopySegmentTarget{
		CollectionId: 444,
		PartitionId:  555,
		SegmentId:    666,
		NewBuildIds: map[int64]int64{
			2001: 3001, // old buildID -> new buildID
			2002: 3002,
		},
	}

	mappings := map[string]string{
		"files/index_files/2001/1/222/333/index1": "files/index_files/3001/1/555/666/index1",
		"files/index_files/2002/1/222/333/index2": "files/index_files/3002/1/555/666/index2",
	}

	indexInfos, _, _, err := buildIndexInfoFromSource(source, target, mappings)
	assert.NoError(t, err)

	// Both indexes should be present (keyed by new buildID)
	assert.Equal(t, 2, len(indexInfos), "Both JSON path indexes should be in the result")

	// Verify first index (idx_category)
	assert.NotNil(t, indexInfos[3001])
	assert.Equal(t, int64(100), indexInfos[3001].FieldId)
	assert.Equal(t, int64(1001), indexInfos[3001].IndexId)
	assert.Equal(t, int64(3001), indexInfos[3001].BuildId)
	assert.Equal(t, "idx_category", indexInfos[3001].IndexName)

	// Verify second index (idx_price)
	assert.NotNil(t, indexInfos[3002])
	assert.Equal(t, int64(100), indexInfos[3002].FieldId)
	assert.Equal(t, int64(1002), indexInfos[3002].IndexId)
	assert.Equal(t, int64(3002), indexInfos[3002].BuildId)
	assert.Equal(t, "idx_price", indexInfos[3002].IndexName)
}

func TestBuildIndexInfoFromSource_EmptySource(t *testing.T) {
	source := &datapb.CopySegmentSource{}
	target := &datapb.CopySegmentTarget{
		CollectionId: 444,
		PartitionId:  555,
		SegmentId:    666,
	}

	indexInfos, textIndexInfos, jsonKeyIndexInfos, err := buildIndexInfoFromSource(source, target, map[string]string{})
	assert.NoError(t, err)
	assert.Equal(t, 0, len(indexInfos))
	assert.Equal(t, 0, len(textIndexInfos))
	assert.Equal(t, 0, len(jsonKeyIndexInfos))
}

func TestBuildIndexInfoFromSource_UnmappedPaths(t *testing.T) {
	target := &datapb.CopySegmentTarget{
		CollectionId: 444,
		PartitionId:  555,
		SegmentId:    666,
	}

	t.Run("unmapped vector index path returns error", func(t *testing.T) {
		source := &datapb.CopySegmentSource{
			IndexFiles: []*indexpb.IndexFilePathInfo{
				{
					FieldID:        100,
					IndexID:        1001,
					BuildID:        1002,
					IndexFilePaths: []string{"unmapped/path"},
					SerializedSize: 5000,
				},
			},
		}
		_, _, _, err := buildIndexInfoFromSource(source, target, map[string]string{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no mapping found for index file")
	})

	t.Run("unmapped text index path returns error", func(t *testing.T) {
		source := &datapb.CopySegmentSource{
			TextIndexFiles: map[int64]*datapb.TextIndexStats{
				200: {
					FieldID: 200,
					Files:   []string{"unmapped/text/path"},
				},
			},
		}
		_, _, _, err := buildIndexInfoFromSource(source, target, map[string]string{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no mapping found for text index file")
	})

	t.Run("unmapped json index path returns error", func(t *testing.T) {
		source := &datapb.CopySegmentSource{
			JsonKeyIndexFiles: map[int64]*datapb.JsonKeyStats{
				300: {
					FieldID: 300,
					Files:   []string{"unmapped/json/path"},
				},
			},
		}
		_, _, _, err := buildIndexInfoFromSource(source, target, map[string]string{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no mapping found for JSON index file")
	})
}

func TestTransformFieldBinlogs_NilInput(t *testing.T) {
	result, totalRows, err := transformFieldBinlogs(nil, map[string]string{}, transformFieldBinlogOptions{countRows: true})
	assert.NoError(t, err)
	assert.Equal(t, int64(0), totalRows)
	assert.Equal(t, 0, len(result))
}

func TestTransformFieldBinlogs_MultipleBinlogsPerField(t *testing.T) {
	mappings := map[string]string{
		"files/insert_log/111/222/333/100/log1": "files/insert_log/444/555/666/100/log1",
		"files/insert_log/111/222/333/100/log2": "files/insert_log/444/555/666/100/log2",
		"files/insert_log/111/222/333/100/log3": "files/insert_log/444/555/666/100/log3",
	}

	srcFieldBinlogs := []*datapb.FieldBinlog{
		{
			FieldID: 100,
			Binlogs: []*datapb.Binlog{
				{EntriesNum: 100, LogPath: "files/insert_log/111/222/333/100/log1", LogSize: 512},
				{EntriesNum: 200, LogPath: "files/insert_log/111/222/333/100/log2", LogSize: 1024},
				{EntriesNum: 300, LogPath: "files/insert_log/111/222/333/100/log3", LogSize: 2048},
			},
		},
	}

	result, totalRows, err := transformFieldBinlogs(srcFieldBinlogs, mappings, transformFieldBinlogOptions{countRows: true})
	assert.NoError(t, err)
	assert.Equal(t, int64(600), totalRows)
	assert.Equal(t, 1, len(result))
	assert.Equal(t, 3, len(result[0].Binlogs))
	assert.Equal(t, "files/insert_log/444/555/666/100/log1", result[0].Binlogs[0].LogPath)
	assert.Equal(t, "files/insert_log/444/555/666/100/log2", result[0].Binlogs[1].LogPath)
	assert.Equal(t, "files/insert_log/444/555/666/100/log3", result[0].Binlogs[2].LogPath)
}

func TestTransformFieldBinlogs_UnmappedPath(t *testing.T) {
	// Path not in mappings -> returns error (fail-fast)
	mappings := map[string]string{}

	srcFieldBinlogs := []*datapb.FieldBinlog{
		{
			FieldID: 100,
			Binlogs: []*datapb.Binlog{
				{EntriesNum: 100, LogPath: "files/insert_log/111/222/333/100/log1"},
			},
		},
	}

	_, _, err := transformFieldBinlogs(srcFieldBinlogs, mappings, transformFieldBinlogOptions{countRows: true})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no mapping found for source path")
}

func TestShortenIndexFilePaths(t *testing.T) {
	tests := []struct {
		name      string
		fullPaths []string
		expected  []string
	}{
		{
			name: "vector/scalar index paths",
			fullPaths: []string{
				"files/index_files/1001/1/555/666/scalar_index",
				"files/index_files/1001/1/555/666/vector_index",
			},
			expected: []string{"scalar_index", "vector_index"},
		},
		{
			name:      "empty path list",
			fullPaths: []string{},
			expected:  []string{},
		},
		{
			name: "single file name",
			fullPaths: []string{
				"index_file",
			},
			expected: []string{"index_file"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := shortenIndexFilePaths(tt.fullPaths)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestShortenJsonStatsPath_Files(t *testing.T) {
	tests := []struct {
		name         string
		inputPath    string
		expectedPath string
	}{
		{
			name:         "shared_key_index path",
			inputPath:    "files/json_stats/2/123/1/444/555/666/100/shared_key_index/inverted_index_0",
			expectedPath: "shared_key_index/inverted_index_0",
		},
		{
			name:         "shredding_data path",
			inputPath:    "files/json_stats/2/123/1/444/555/666/100/shredding_data/parquet_data_0",
			expectedPath: "shredding_data/parquet_data_0",
		},
		{
			name:         "already shortened - shared_key_index",
			inputPath:    "shared_key_index/inverted_index_0",
			expectedPath: "shared_key_index/inverted_index_0",
		},
		{
			name:         "already shortened - shredding_data",
			inputPath:    "shredding_data/parquet_data_0",
			expectedPath: "shredding_data/parquet_data_0",
		},
		{
			name:         "no keyword - keep as-is",
			inputPath:    "files/other/path/file.json",
			expectedPath: "files/other/path/file.json",
		},
		{
			name:         "already shortened - meta.json",
			inputPath:    "meta.json",
			expectedPath: "meta.json",
		},
		{
			name:         "full path - meta.json",
			inputPath:    "files/json_stats/2/123/1/444/555/666/100/meta.json",
			expectedPath: "meta.json",
		},
		{
			name:         "full path - nested file",
			inputPath:    "files/json_stats/2/123/1/444/555/666/100/subdir/file.dat",
			expectedPath: "subdir/file.dat",
		},
		{
			name:         "local absolute path",
			inputPath:    "/var/lib/milvus/data/json_stats/2/123/1/444/555/666/100/meta.json",
			expectedPath: "meta.json",
		},
		{
			name:         "reserved names in root",
			inputPath:    "archive/json_stats/shared_key_index/files/json_stats/2/123/1/444/555/666/100/shared_key_index/index",
			expectedPath: "shared_key_index/index",
		},
		{
			name:         "shared key component in root",
			inputPath:    "archive/shared_key_index/files/json_stats/2/123/1/444/555/666/100/meta.json",
			expectedPath: "meta.json",
		},
		{
			name:         "shared key substring is not an anchor",
			inputPath:    "archive/not_shared_key_index/file",
			expectedPath: "archive/not_shared_key_index/file",
		},
		{
			name:         "data format mismatch",
			inputPath:    "files/json_stats/3/123/1/444/555/666/100/meta.json",
			expectedPath: "files/json_stats/3/123/1/444/555/666/100/meta.json",
		},
		{
			name:         "build ID mismatch",
			inputPath:    "files/json_stats/2/456/1/444/555/666/100/meta.json",
			expectedPath: "files/json_stats/2/456/1/444/555/666/100/meta.json",
		},
		{
			name:         "version mismatch",
			inputPath:    "files/json_stats/2/123/2/444/555/666/100/meta.json",
			expectedPath: "files/json_stats/2/123/2/444/555/666/100/meta.json",
		},
		{
			name:         "field ID mismatch",
			inputPath:    "files/json_stats/2/123/1/444/555/666/200/meta.json",
			expectedPath: "files/json_stats/2/123/1/444/555/666/200/meta.json",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stats := &datapb.JsonKeyStats{
				FieldID:                100,
				Version:                1,
				BuildID:                123,
				JsonKeyStatsDataFormat: 2,
				Files:                  []string{tt.inputPath},
			}
			result := shortenJSONStatsPath(map[int64]*datapb.JsonKeyStats{100: stats})
			assert.Equal(t, []string{tt.expectedPath}, result[100].GetFiles())
		})
	}
}

func TestShortenJsonStatsPath(t *testing.T) {
	jsonStats := map[int64]*datapb.JsonKeyStats{
		100: {
			FieldID: 100,
			Version: 1,
			BuildID: 123,
			Files: []string{
				"files/json_stats/2/123/1/444/555/666/100/shared_key_index/inverted_index_0",
				"files/json_stats/2/123/1/444/555/666/100/shared_key_index/inverted_index_1",
			},
			JsonKeyStatsDataFormat: 2,
			MemorySize:             1024,
			LogSize:                2048,
		},
		200: {
			FieldID: 200,
			Version: 1,
			BuildID: 456,
			Files: []string{
				"files/json_stats/2/456/1/444/555/666/200/shredding_data/parquet_data_0",
			},
			JsonKeyStatsDataFormat: 2,
			MemorySize:             512,
		},
	}

	result := shortenJSONStatsPath(jsonStats)

	assert.Equal(t, 2, len(result))

	// Check field 100
	assert.NotNil(t, result[100])
	assert.Equal(t, int64(100), result[100].FieldID)
	assert.Equal(t, int64(1), result[100].Version)
	assert.Equal(t, int64(123), result[100].BuildID)
	assert.Equal(t, int64(2), result[100].JsonKeyStatsDataFormat)
	assert.Equal(t, int64(1024), result[100].MemorySize)
	assert.Equal(t, int64(2048), result[100].LogSize)
	assert.Equal(t, 2, len(result[100].Files))
	assert.Equal(t, "shared_key_index/inverted_index_0", result[100].Files[0])
	assert.Equal(t, "shared_key_index/inverted_index_1", result[100].Files[1])

	// Check field 200
	assert.NotNil(t, result[200])
	assert.Equal(t, int64(200), result[200].FieldID)
	assert.Equal(t, int64(1), result[200].Version)
	assert.Equal(t, int64(456), result[200].BuildID)
	assert.Equal(t, int64(2), result[200].JsonKeyStatsDataFormat)
	assert.Equal(t, int64(512), result[200].MemorySize)
	assert.Equal(t, 1, len(result[200].Files))
	assert.Equal(t, "shredding_data/parquet_data_0", result[200].Files[0])
}

func TestShortenJsonStatsPath_MetaJson(t *testing.T) {
	jsonStats := map[int64]*datapb.JsonKeyStats{
		102: {
			FieldID: 102,
			Version: 1,
			BuildID: 462930163709949539,
			Files: []string{
				"files/json_stats/2/462930163709949539/1/462930163710600038/462930163710600039/462930163710600046/102/meta.json",
			},
			JsonKeyStatsDataFormat: 2,
			MemorySize:             256,
		},
	}

	result := shortenJSONStatsPath(jsonStats)

	assert.Equal(t, 1, len(result))
	assert.NotNil(t, result[102])
	assert.Equal(t, int64(102), result[102].FieldID)
	assert.Equal(t, 1, len(result[102].Files))
	assert.Equal(t, "meta.json", result[102].Files[0])
}

func TestShortenJsonStatsPath_NilEntries(t *testing.T) {
	assert.Nil(t, shortenJSONStatsPath(nil))
	assert.Empty(t, shortenJSONStatsPath(map[int64]*datapb.JsonKeyStats{}))

	jsonStats := map[int64]*datapb.JsonKeyStats{
		100: nil,
		200: {},
	}
	result := shortenJSONStatsPath(jsonStats)
	require.Len(t, result, 2)
	assert.Nil(t, result[100])
	assert.Same(t, jsonStats[200], result[200])
	assert.Nil(t, result[200].GetFiles())
}

func TestTransformManifestPath(t *testing.T) {
	target := &datapb.CopySegmentTarget{
		SegmentId:    2001,
		CollectionId: 111,
		PartitionId:  222,
	}

	sourceManifest := packed.MarshalManifestPath(
		"files/insert_log/449104612037410004/449104621518610066/449104621518610065",
		2,
	)

	source := &datapb.CopySegmentSource{
		CollectionId: 449104612037410004,
		PartitionId:  449104621518610066,
		SegmentId:    449104621518610065,
	}
	targetManifest, err := transformManifestPath(sourceManifest, source, target)
	assert.NoError(t, err)

	basePath, version, err := packed.UnmarshalManifestPath(targetManifest)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), version)
	assert.Contains(t, basePath, "111")
	assert.Contains(t, basePath, "222")
	assert.Contains(t, basePath, "2001")
}

// Restoring from an external snapshot into local storage produces a manifest
// base in the target's canonical layout. The source's legacy files prefix
// must not survive below TargetRootPath.
func TestTransformManifestPath_ExternalSourceToLocalProducesCompletePath(t *testing.T) {
	target := &datapb.CopySegmentTarget{
		SegmentId:      666,
		CollectionId:   444,
		PartitionId:    555,
		TargetRootPath: "/var/lib/milvus/data",
	}
	source := &datapb.CopySegmentSource{
		CollectionId:   111,
		PartitionId:    222,
		SegmentId:      333,
		SourceRootPath: "s3://source-bucket/source-root",
	}
	sourceManifest := packed.MarshalManifestPath(
		"source-root/files/insert_log/111/222/333",
		2,
	)

	targetManifest, err := transformManifestPath(sourceManifest, source, target)
	require.NoError(t, err)
	basePath, version, err := packed.UnmarshalManifestPath(targetManifest)
	require.NoError(t, err)
	assert.Equal(t, int64(2), version)
	assert.Equal(t, "/var/lib/milvus/data/insert_log/444/555/666", basePath)

	physicalPath, err := generateTargetPath(
		"source-root/files/insert_log/111/222/333/_data/0",
		source,
		target,
	)
	require.NoError(t, err)
	assert.Equal(t, "/var/lib/milvus/data/insert_log/444/555/666/_data/0", physicalPath)
}

func TestTransformManifestPath_ExternalTable(t *testing.T) {
	target := &datapb.CopySegmentTarget{
		TargetRootPath: "files",
		SegmentId:      666,
		CollectionId:   444,
		PartitionId:    555,
	}

	sourceManifest := packed.MarshalManifestPath("files/insert_log/111/222/333", 2)
	source := &datapb.CopySegmentSource{
		CollectionId:         111,
		PartitionId:          222,
		SegmentId:            333,
		IsExternalCollection: true,
	}
	targetManifest, err := transformManifestPath(sourceManifest, source, target)
	assert.NoError(t, err)

	basePath, version, err := packed.UnmarshalManifestPath(targetManifest)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), version)
	assert.Equal(t, "files/insert_log/444/555/666", basePath)
}

func TestTransformManifestPath_LegacyExternalTableLayoutUnsupported(t *testing.T) {
	target := &datapb.CopySegmentTarget{
		SegmentId:    666,
		CollectionId: 444,
		PartitionId:  555,
	}

	sourceManifest := packed.MarshalManifestPath("external/111/segments/333", 2)
	source := &datapb.CopySegmentSource{
		CollectionId:         111,
		PartitionId:          222,
		SegmentId:            333,
		IsExternalCollection: true,
	}
	targetManifest, err := transformManifestPath(sourceManifest, source, target)
	assert.Error(t, err)
	assert.Empty(t, targetManifest)
	assert.Contains(t, err.Error(), "invalid binlog path structure")
}

func TestTransformFieldBinlogs_SkipsPathMappingForExternalTable(t *testing.T) {
	src := []*datapb.FieldBinlog{
		{
			FieldID: 100,
			Binlogs: []*datapb.Binlog{
				{
					LogID:      10,
					EntriesNum: 123,
					MemorySize: 456,
					LogSize:    456,
				},
			},
		},
	}
	got, totalRows, err := transformFieldBinlogs(src, nil, transformFieldBinlogOptions{countRows: true, isExternalTable: true})
	assert.NoError(t, err)
	assert.Equal(t, int64(123), totalRows)
	assert.Len(t, got, 1)
	assert.Equal(t, int64(100), got[0].GetFieldID())
	assert.Len(t, got[0].GetBinlogs(), 1)
	assert.Empty(t, got[0].GetBinlogs()[0].GetLogPath())
	assert.Equal(t, int64(10), got[0].GetBinlogs()[0].GetLogID())
	assert.Equal(t, int64(123), got[0].GetBinlogs()[0].GetEntriesNum())
	assert.Equal(t, int64(456), got[0].GetBinlogs()[0].GetMemorySize())
}

func TestTransformFieldBinlogs_DropsPathlessSummaryWhenNotPreserved(t *testing.T) {
	src := []*datapb.FieldBinlog{
		{
			FieldID: 100,
			Binlogs: []*datapb.Binlog{
				{LogID: 10, EntriesNum: 123},
			},
		},
	}

	got, totalRows, err := transformFieldBinlogs(src, nil, transformFieldBinlogOptions{})
	assert.NoError(t, err)
	assert.Zero(t, totalRows)
	assert.Empty(t, got)
}

func TestTransformFieldBinlogs_PreservesPathlessSummary(t *testing.T) {
	src := []*datapb.FieldBinlog{
		{
			FieldID: 100,
			Binlogs: []*datapb.Binlog{
				{
					LogID:         10,
					EntriesNum:    123,
					MemorySize:    456,
					TimestampFrom: 1000,
					TimestampTo:   2000,
				},
			},
		},
	}

	got, totalRows, err := transformFieldBinlogs(src, nil, transformFieldBinlogOptions{countRows: true, preservePathless: true})
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Len(t, got[0].GetBinlogs(), 1)
	assert.Equal(t, int64(123), totalRows)
	assert.Empty(t, got[0].GetBinlogs()[0].GetLogPath())
	assert.Equal(t, src[0].GetBinlogs()[0], got[0].GetBinlogs()[0])
	assert.NotSame(t, src[0].GetBinlogs()[0], got[0].GetBinlogs()[0])
}

func TestGenerateSegmentInfoFromSource_PreservesExternalTableBinlogMetadata(t *testing.T) {
	source := &datapb.CopySegmentSource{
		CollectionId:         111,
		PartitionId:          222,
		SegmentId:            333,
		StorageVersion:       storage.StorageV3,
		ManifestPath:         packed.MarshalManifestPath("files/insert_log/111/222/333", 2),
		IsExternalCollection: true,
		InsertBinlogs: []*datapb.FieldBinlog{
			{
				FieldID:     0,
				ChildFields: []int64{100, 101, 102},
				Binlogs: []*datapb.Binlog{
					{LogID: 10, EntriesNum: 123, MemorySize: 456},
				},
			},
		},
	}
	target := &datapb.CopySegmentTarget{
		CollectionId: 444,
		PartitionId:  555,
		SegmentId:    666,
	}

	segmentInfo, err := generateSegmentInfoFromSource(source, target, nil)
	assert.NoError(t, err)
	assert.Equal(t, int64(123), segmentInfo.GetImportedRows())
	assert.Len(t, segmentInfo.GetBinlogs(), 1)
	assert.Empty(t, segmentInfo.GetBinlogs()[0].GetBinlogs()[0].GetLogPath())
}

func TestGenerateSegmentInfoFromSource_V3UsesDeclaredRowCount(t *testing.T) {
	segmentInfo, err := generateSegmentInfoFromSource(
		&datapb.CopySegmentSource{
			StorageVersion: storage.StorageV3,
			NumOfRows:      4321,
		},
		&datapb.CopySegmentTarget{SegmentId: 666},
		nil,
	)

	require.NoError(t, err)
	assert.Equal(t, int64(4321), segmentInfo.GetImportedRows())
}

func TestGenerateSegmentInfoFromSource_V3PreservesPathlessSummaries(t *testing.T) {
	source := &datapb.CopySegmentSource{
		StorageVersion: storage.StorageV3,
		NumOfRows:      4321,
		InsertBinlogs: []*datapb.FieldBinlog{
			{
				FieldID:     10,
				ChildFields: []int64{10},
				Format:      "parquet",
				Binlogs: []*datapb.Binlog{
					{
						LogID:         1,
						EntriesNum:    123,
						MemorySize:    456,
						TimestampFrom: 100,
						TimestampTo:   200,
					},
				},
			},
		},
		DeltaBinlogs: []*datapb.FieldBinlog{
			{
				FieldID: 10,
				Binlogs: []*datapb.Binlog{
					{
						LogID:         2,
						EntriesNum:    3,
						MemorySize:    128,
						TimestampFrom: 300,
						TimestampTo:   400,
					},
				},
			},
		},
	}

	segmentInfo, err := generateSegmentInfoFromSource(
		source,
		&datapb.CopySegmentTarget{SegmentId: 666},
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, int64(4321), segmentInfo.GetImportedRows())
	require.Len(t, segmentInfo.GetBinlogs(), 1)
	require.Len(t, segmentInfo.GetDeltalogs(), 1)
	assert.Equal(t, source.GetInsertBinlogs()[0], segmentInfo.GetBinlogs()[0])
	assert.Equal(t, source.GetDeltaBinlogs()[0], segmentInfo.GetDeltalogs()[0])

	stats := storage.BuildStatsFromFieldBinlogs(segmentInfo.GetBinlogs(), nil, nil, segmentInfo.GetDeltalogs())
	assert.EqualValues(t, 456, stats.GetInsertBinlogSize())
	assert.EqualValues(t, 1, stats.GetInsertBinlogCount())
	assert.EqualValues(t, 128, stats.GetDeltaBinlogSize())
	assert.EqualValues(t, 3, stats.GetDeleteNumRows())
	assert.EqualValues(t, 1, stats.GetDeltaBinlogCount())
	assert.EqualValues(t, 300, stats.GetDeltaTimestampFrom())
	assert.EqualValues(t, 400, stats.GetDeltaTimestampTo())
}

func TestCopySegmentAndIndexFiles_StorageV2CopiesManifest(t *testing.T) {
	sourceManifest := "files/insert_log/111/222/333/_metadata/manifest.json"
	sourceBinlog := "files/insert_log/111/222/333/100/1"
	source := &datapb.CopySegmentSource{
		CollectionId:   111,
		PartitionId:    222,
		SegmentId:      333,
		StorageVersion: storage.StorageV2,
		ManifestPath:   sourceManifest,
		InsertBinlogs: []*datapb.FieldBinlog{{
			FieldID: 100,
			Binlogs: []*datapb.Binlog{{
				LogPath:    sourceBinlog,
				EntriesNum: 12,
			}},
		}},
	}
	target := &datapb.CopySegmentTarget{
		TargetRootPath: "files",
		CollectionId:   444,
		PartitionId:    555,
		SegmentId:      666,
	}
	copied := make(map[string]string)
	var copyMu sync.Mutex
	targetStore := newCopySegmentStoreMock(t, func(_ context.Context, _, src, _, dst string) error {
		copyMu.Lock()
		defer copyMu.Unlock()
		copied[src] = dst
		return nil
	})

	result, copiedFiles, err := CopySegmentAndIndexFiles(
		context.Background(),
		make(chan struct{}, 2),
		&struct{ storage.ChunkManager }{},
		&indexpb.StorageConfig{BucketName: "test-bucket"},
		&indexpb.StorageConfig{BucketName: "test-bucket"},
		targetStore,
		"test-bucket",
		"test-bucket",
		source,
		target,
		nil)

	require.NoError(t, err)
	expectedManifest := "files/insert_log/444/555/666/_metadata/manifest.json"
	assert.Equal(t, expectedManifest, result.GetManifestPath())
	assert.Equal(t, int64(12), result.GetImportedRows())
	assert.Equal(t, expectedManifest, copied[sourceManifest])
	assert.Equal(t, "files/insert_log/444/555/666/100/1", copied[sourceBinlog])
	assert.Len(t, copiedFiles, 2)
}

func TestCopySegmentAndIndexFiles_ExternalTable(t *testing.T) {
	mockCopiedManifestIndexEntries(t, nil)

	source := &datapb.CopySegmentSource{
		CollectionId:         111,
		PartitionId:          222,
		SegmentId:            333,
		StorageVersion:       storage.StorageV3,
		ManifestPath:         packed.MarshalManifestPath("files/insert_log/111/222/333", 2),
		IsExternalCollection: true,
		InsertBinlogs: []*datapb.FieldBinlog{
			{
				FieldID:     0,
				ChildFields: []int64{100, 101, 102},
				Binlogs: []*datapb.Binlog{
					{LogID: 10, EntriesNum: 123, MemorySize: 456},
				},
			},
		},
	}
	target := &datapb.CopySegmentTarget{
		TargetRootPath: "files",
		CollectionId:   444,
		PartitionId:    555,
		SegmentId:      666,
	}

	defer mockey.Mock(snapshotstorage.ReadNativeManifestFiles).Return(manifestInventoryForTest([]string{
		"files/insert_log/111/222/333/_data/0",
		"files/insert_log/111/222/333/_metadata/manifest.json",
	}, nil), nil).Build().UnPatch()

	copied := make(map[string]string)
	var copyMu sync.Mutex
	targetStore := newCopySegmentStoreMock(t, func(_ context.Context, _, src, _, dst string) error {
		copyMu.Lock()
		defer copyMu.Unlock()
		copied[src] = dst
		return nil
	})

	cm := &struct{ storage.ChunkManager }{}
	result, copiedFiles, err := CopySegmentAndIndexFiles(
		context.Background(),
		make(chan struct{}, 2),
		cm,
		&indexpb.StorageConfig{BucketName: "test-bucket"},
		&indexpb.StorageConfig{BucketName: "test-bucket"},
		targetStore,
		"test-bucket",
		"test-bucket",
		source,
		target,
		nil,
	)
	assert.NoError(t, err)
	assert.Equal(t, int64(123), result.GetImportedRows())
	assert.Equal(t, packed.MarshalManifestPath("files/insert_log/444/555/666", 2), result.GetManifestPath())
	assert.Len(t, result.GetBinlogs(), 1)
	assert.Empty(t, result.GetBinlogs()[0].GetBinlogs()[0].GetLogPath())
	assert.Equal(t, "files/insert_log/444/555/666/_data/0", copied["files/insert_log/111/222/333/_data/0"])
	assert.Equal(t, "files/insert_log/444/555/666/_metadata/manifest.json", copied["files/insert_log/111/222/333/_metadata/manifest.json"])
	assert.ElementsMatch(t, []string{
		"files/insert_log/444/555/666/_data/0",
		"files/insert_log/444/555/666/_metadata/manifest.json",
	}, copiedFiles)
}

func TestCollectSegmentFiles_WithManifest(t *testing.T) {
	paths := []string{
		"files/insert_log/111/222/333/_data/100/file1.log",
		"files/insert_log/111/222/333/_data/101/file2.log",
		"files/insert_log/111/222/333/_metadata/manifest.json",
	}
	for _, empty := range []bool{false, true} {
		t.Run(fmt.Sprintf("empty-%t", empty), func(t *testing.T) {
			physical := paths
			if empty {
				physical = nil
			}
			defer mockey.Mock(snapshotstorage.ReadNativeManifestFiles).Return(manifestInventoryForTest(physical, nil), nil).Build().UnPatch()
			files, err := collectSegmentFiles(context.Background(), nil, &indexpb.StorageConfig{BucketName: "test-bucket"},
				&datapb.CopySegmentSource{
					SegmentId: 333, StorageVersion: storage.StorageV3,
					ManifestPath: packed.MarshalManifestPath("files/insert_log/111/222/333", 2),
				})
			require.NoError(t, err)
			require.NotNil(t, files)
			require.ElementsMatch(t, physical, files.InsertBinlogs)
		})
	}
}

func TestCollectSegmentFiles_NoManifest(t *testing.T) {
	ctx := context.Background()

	cm := &struct{ storage.ChunkManager }{}

	source := &datapb.CopySegmentSource{
		CollectionId:   111,
		PartitionId:    222,
		SegmentId:      333,
		StorageVersion: storage.StorageV1, // V1: use pb paths
		InsertBinlogs: []*datapb.FieldBinlog{
			{
				FieldID: 100,
				Binlogs: []*datapb.Binlog{
					{LogPath: "files/insert_log/111/222/333/100/file1.log"},
					{LogPath: "files/insert_log/111/222/333/100/file2.log"},
				},
			},
		},
	}

	source.JsonKeyIndexFiles = map[int64]*datapb.JsonKeyStats{100: {Files: []string{"files/json_stats/test"}, JsonKeyStatsDataFormat: 2}}
	files, err := collectSegmentFiles(ctx, cm, &indexpb.StorageConfig{BucketName: "test-bucket"}, source)

	assert.NoError(t, err)
	assert.Equal(t, []string{"files/json_stats/test"}, files.JSONStats)
	assert.Len(t, files.InsertBinlogs, 2)
	assert.Contains(t, files.InsertBinlogs, "files/insert_log/111/222/333/100/file1.log")
	assert.Contains(t, files.InsertBinlogs, "files/insert_log/111/222/333/100/file2.log")
}

func TestExtractFromPb(t *testing.T) {
	t.Run("extract paths from field binlogs", func(t *testing.T) {
		fieldBinlogs := []*datapb.FieldBinlog{
			{
				FieldID: 100,
				Binlogs: []*datapb.Binlog{
					{LogPath: "path/a"},
					{LogPath: "path/b"},
				},
			},
			{
				FieldID: 101,
				Binlogs: []*datapb.Binlog{
					{LogPath: "path/c"},
				},
			},
		}
		paths := extractFromPb(fieldBinlogs)
		assert.Equal(t, []string{"path/a", "path/b", "path/c"}, paths)
	})

	t.Run("skip empty paths", func(t *testing.T) {
		fieldBinlogs := []*datapb.FieldBinlog{
			{
				FieldID: 100,
				Binlogs: []*datapb.Binlog{
					{LogPath: "path/a"},
					{LogPath: ""},
				},
			},
		}
		paths := extractFromPb(fieldBinlogs)
		assert.Equal(t, []string{"path/a"}, paths)
	})

	t.Run("empty input", func(t *testing.T) {
		paths := extractFromPb(nil)
		assert.Empty(t, paths)
	})
}

func TestGenerateMappingsFromFiles(t *testing.T) {
	source := &datapb.CopySegmentSource{
		CollectionId: 111,
		PartitionId:  222,
		SegmentId:    333,
	}
	target := &datapb.CopySegmentTarget{
		TargetRootPath: "files",
		CollectionId:   444,
		PartitionId:    555,
		SegmentId:      666,
	}

	t.Run("generate mappings for all file types", func(t *testing.T) {
		files := &SegmentFiles{
			InsertBinlogs:     []string{"files/insert_log/111/222/333/100/log1"},
			DeltaBinlogs:      []string{"files/delta_log/111/222/333/100/delta1"},
			StatsBinlogs:      []string{"files/stats_log/111/222/333/100/stats1"},
			Bm25Binlogs:       []string{"files/bm25_stats/111/222/333/100/bm25_1"},
			VectorScalarIndex: []string{"files/index_files/1002/1/222/333/idx1"},
			TextIndex:         []string{"files/text_log/123/1/111/222/333/100/text1"},
			JSONKeyIndex:      []string{"files/json_key_index_log/123/1/111/222/333/101/json1"},
			JSONStats:         []string{"files/json_stats/2/3002/1/111/222/333/102/shared_key_index/idx1"},
		}

		mappings, err := generateMappingsFromFiles(files, source, target)
		assert.NoError(t, err)
		assert.Len(t, mappings, 8)

		assert.Equal(t, "files/insert_log/444/555/666/100/log1",
			mappings["files/insert_log/111/222/333/100/log1"])
		assert.Equal(t, "files/delta_log/444/555/666/100/delta1",
			mappings["files/delta_log/111/222/333/100/delta1"])
		assert.Equal(t, "files/bm25_stats/444/555/666/100/bm25_1",
			mappings["files/bm25_stats/111/222/333/100/bm25_1"])
		assert.Equal(t, "files/index_files/1002/1/555/666/idx1",
			mappings["files/index_files/1002/1/222/333/idx1"])
	})

	t.Run("empty files", func(t *testing.T) {
		files := &SegmentFiles{}
		mappings, err := generateMappingsFromFiles(files, source, target)
		assert.NoError(t, err)
		assert.Empty(t, mappings)
	})

	t.Run("invalid path returns error", func(t *testing.T) {
		files := &SegmentFiles{
			InsertBinlogs: []string{"invalid/path/no/log_type"},
		}
		_, err := generateMappingsFromFiles(files, source, target)
		assert.Error(t, err)
	})
}

// TestCopySegmentAndIndexFiles_WithManifest tests the complete copy flow for StorageV3 segments.
// When storage_version >= StorageV3, insert binlogs are stored in packed format.
// Physical files (under _data/_metadata) differ from logical paths in protobuf.
// Step 3.5 must add logical pb path mappings AFTER file copying for metadata generation.
func TestCopySegmentAndIndexFiles_WithManifest(t *testing.T) {
	for _, mode := range []string{"success", "no PB binlogs", "new revision", "copy failure", "invalid PB path"} {
		t.Run(mode, func(t *testing.T) {
			base := "files/insert_log/111/222/333"
			binlog := func(field, rows int64, key string) *datapb.FieldBinlog {
				return &datapb.FieldBinlog{FieldID: field, Binlogs: []*datapb.Binlog{{EntriesNum: rows, LogPath: key}}}
			}
			source := &datapb.CopySegmentSource{
				CollectionId: 111, PartitionId: 222, SegmentId: 333,
				StorageVersion: storage.StorageV3, ManifestPath: packed.MarshalManifestPath(base, 2),
				InsertBinlogs: []*datapb.FieldBinlog{binlog(100, 500, base+"/100/10001"), binlog(101, 500, base+"/101/10002")},
				StatsBinlogs:  []*datapb.FieldBinlog{binlog(100, 0, "files/stats_log/111/222/333/100/20001")},
				DeltaBinlogs:  []*datapb.FieldBinlog{binlog(0, 0, "files/delta_log/111/222/333/0/30001")},
			}
			inventory := manifestInventoryForTest([]string{base + "/_data/0", base + "/_metadata/manifest.json"}, nil)
			if mode == "no PB binlogs" {
				source.InsertBinlogs, source.StatsBinlogs, source.DeltaBinlogs = nil, nil, nil
				source.ManifestHasIndex = proto.Bool(false)
				inventory = manifestInventoryForTest([]string{base + "/_data/0"}, nil)
			}
			if mode == "invalid PB path" {
				source.InsertBinlogs[0].Binlogs[0].LogPath = "invalid/path/no/log_type/10001"
			}
			if mode == "new revision" {
				inventory.Indexes = []packed.ManifestIndexInfo{{IndexID: 1}}
			}
			defer mockey.Mock(snapshotstorage.ReadNativeManifestFiles).Return(inventory, nil).Build().UnPatch()
			defer mockey.Mock(packed.GetManifestIndexInfos).To(func(string, *indexpb.StorageConfig) ([]packed.ManifestIndexInfo, error) {
				t.Error("prepared inventory must avoid target enumeration")
				return nil, nil
			}).Build().UnPatch()
			republished := packed.MarshalManifestPath("files/insert_log/444/555/666", 3)
			defer mockey.Mock(packed.ReplaceCopiedManifestIndexes).Return(republished, nil).Build().UnPatch()
			var mu sync.Mutex
			var copiedSources []string
			store := newCopySegmentStoreMock(t, func(_ context.Context, _, src, _, _ string) error {
				if mode == "copy failure" {
					return errors.New("storage unavailable")
				}
				mu.Lock()
				defer mu.Unlock()
				copiedSources = append(copiedSources, src)
				return nil
			})
			cfg := &indexpb.StorageConfig{BucketName: "test-bucket"}
			target := &datapb.CopySegmentTarget{TargetRootPath: "files", CollectionId: 444, PartitionId: 555, SegmentId: 666}
			result, copied, err := CopySegmentAndIndexFiles(context.Background(), make(chan struct{}, 2), &struct{ storage.ChunkManager }{},
				cfg, cfg, store, cfg.BucketName, cfg.BucketName, source, target, nil)
			if mode == "copy failure" || mode == "invalid PB path" {
				require.Error(t, err)
				require.Nil(t, result)
				if mode == "copy failure" {
					require.ErrorContains(t, err, "failed to copy file")
					require.Empty(t, copied)
				} else {
					require.ErrorContains(t, err, "failed to generate target path for protobuf binlog")
				}
				return
			}
			require.NoError(t, err)
			require.EqualValues(t, 666, result.SegmentId)
			require.True(t, result.GetManifestIndexRewritten())
			require.Empty(t, result.GetManifestIndexBuildIds())
			if mode == "no PB binlogs" {
				require.Zero(t, result.ImportedRows)
				require.Len(t, copied, 1)
			} else {
				require.EqualValues(t, 1000, result.ImportedRows)
				require.Len(t, result.Binlogs, 2)
				require.Len(t, result.Statslogs, 1)
				require.Len(t, result.Deltalogs, 1)
				require.ElementsMatch(t, []string{base + "/_data/0", base + "/_metadata/manifest.json", "files/stats_log/111/222/333/100/20001", "files/delta_log/111/222/333/0/30001"}, copiedSources)
				expectedCopies := 4
				if mode == "new revision" {
					expectedCopies++
				}
				require.Len(t, copied, expectedCopies)
			}
			if mode == "new revision" {
				require.Equal(t, republished, result.ManifestPath)
				revision, err := packed.ManifestFilePath(republished)
				require.NoError(t, err)
				require.Contains(t, copied, revision)
			} else {
				require.Equal(t, packed.MarshalManifestPath("files/insert_log/444/555/666", 2), result.ManifestPath)
			}
		})
	}
}

func TestCopySegmentAndIndexFiles_LocalStorageV3UsesCompletePaths(t *testing.T) {
	ctx := context.Background()
	root := filepath.Join(t.TempDir(), "local", "root")
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(root))
	sourceKey := "insert_log/111/222/333/100/10001"
	sourcePath := path.Join(root, sourceKey)
	targetPath := path.Join(root, "insert_log/444/555/666/100/10001")
	sourceDeltaKey := "insert_log/111/222/333/_delta/10002"
	sourceDeltaPath := path.Join(root, sourceDeltaKey)
	targetDeltaPath := path.Join(root, "insert_log/444/555/666/_delta/10002")
	sourcePathlessDeltaKey := "insert_log/111/222/333/_delta/10003"
	sourcePathlessDeltaPath := path.Join(root, sourcePathlessDeltaKey)
	targetPathlessDeltaPath := path.Join(root, "insert_log/444/555/666/_delta/10003")
	require.NoError(t, cm.Write(ctx, sourcePath, []byte("segment-data")))
	require.NoError(t, cm.Write(ctx, sourceDeltaPath, []byte("delta-data")))
	require.NoError(t, cm.Write(ctx, sourcePathlessDeltaPath, []byte("pathless-delta-data")))

	defer mockey.Mock(snapshotstorage.ReadNativeManifestFiles).Return(manifestInventoryForTest([]string{sourcePath, sourceDeltaPath, sourcePathlessDeltaPath}, nil), nil).Build().UnPatch()

	source := &datapb.CopySegmentSource{
		CollectionId:     111,
		PartitionId:      222,
		SegmentId:        333,
		StorageVersion:   storage.StorageV3,
		ManifestHasIndex: proto.Bool(false),
		ManifestPath:     packed.MarshalManifestPath(path.Join(root, "insert_log/111/222/333"), 1),
		InsertBinlogs: []*datapb.FieldBinlog{{
			FieldID: 100,
			Binlogs: []*datapb.Binlog{{
				LogPath:    sourcePath,
				EntriesNum: 10,
			}},
		}},
		DeltaBinlogs: []*datapb.FieldBinlog{{
			Binlogs: []*datapb.Binlog{
				{
					// Protobuf paths are the same complete keys as the physical
					// files; step 3.5 maps them to the complete target path.
					LogPath:    sourceDeltaPath,
					EntriesNum: 1,
				},
				{
					// V3 persists the physical delta in the manifest and keeps only
					// this pathless summary in the protobuf metadata.
					LogID:         10003,
					EntriesNum:    2,
					MemorySize:    128,
					TimestampFrom: 300,
					TimestampTo:   400,
				},
			},
		}},
	}
	target := &datapb.CopySegmentTarget{
		CollectionId:   444,
		PartitionId:    555,
		SegmentId:      666,
		TargetRootPath: root,
	}
	storageConfig := &indexpb.StorageConfig{StorageType: "local", RootPath: root}
	copySources := make(map[string]string)
	var copyMu sync.Mutex
	targetStore := newCopySegmentStoreMock(t, func(ctx context.Context, _, src, _, dst string) error {
		copyMu.Lock()
		defer copyMu.Unlock()
		copySources[src] = dst
		assert.True(t, path.IsAbs(src))
		assert.True(t, path.IsAbs(dst))
		return cm.Copy(ctx, src, dst)
	})

	result, copiedFiles, err := CopySegmentAndIndexFiles(
		ctx, make(chan struct{}, 2),
		cm,
		storageConfig,
		storageConfig,
		targetStore,
		"",
		"",
		source,
		target,
		nil,
	)

	require.NoError(t, err)
	assert.Equal(t, map[string]string{
		sourcePath:              targetPath,
		sourceDeltaPath:         targetDeltaPath,
		sourcePathlessDeltaPath: targetPathlessDeltaPath,
	}, copySources)
	assert.ElementsMatch(t, []string{targetPath, targetDeltaPath, targetPathlessDeltaPath}, copiedFiles)
	assert.Equal(t, packed.MarshalManifestPath(path.Join(root, "insert_log/444/555/666"), 1), result.GetManifestPath())
	require.Len(t, result.GetDeltalogs(), 1)
	require.Len(t, result.GetDeltalogs()[0].GetBinlogs(), 2)
	assert.Equal(t, int64(10002), result.GetDeltalogs()[0].GetBinlogs()[0].GetLogID())
	assert.Equal(t, int64(10003), result.GetDeltalogs()[0].GetBinlogs()[1].GetLogID())
	assert.Empty(t, result.GetDeltalogs()[0].GetBinlogs()[1].GetLogPath())
	assert.Equal(t, int64(2), result.GetDeltalogs()[0].GetBinlogs()[1].GetEntriesNum())
	assert.Equal(t, int64(128), result.GetDeltalogs()[0].GetBinlogs()[1].GetMemorySize())
	content, err := cm.Read(ctx, targetPath)
	require.NoError(t, err)
	assert.Equal(t, []byte("segment-data"), content)
	deltaContent, err := cm.Read(ctx, targetDeltaPath)
	require.NoError(t, err)
	assert.Equal(t, []byte("delta-data"), deltaContent)
	pathlessDeltaContent, err := cm.Read(ctx, targetPathlessDeltaPath)
	require.NoError(t, err)
	assert.Equal(t, []byte("pathless-delta-data"), pathlessDeltaContent)
	assert.NoDirExists(t, path.Join(root, root), "local root must not be prepended twice")
}

func TestCopySegmentAndIndexFiles_ExternalSourceToLocalCanonicalLayout(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(root))
	const sourcePrefix = "source-root/files/"
	const sourceBase = sourcePrefix + "insert_log/111/222/333"
	const sourceLob = sourcePrefix + "insert_log/111/222/lobs/100/_data/lob.vx"
	const sourceStats = sourcePrefix + "stats_log/111/222/333/100/10002"
	const sourceBM25 = sourcePrefix + "bm25_stats/111/222/333/100/10003"
	const sourceDelta = sourcePrefix + "delta_log/111/222/333/10004"
	const sourceIndex = sourcePrefix + "index_v1/111/222/333/7000/1/index.v3"
	mappings := map[string]string{
		sourceBase + "/_data/10001":                     "insert_log/444/555/666/_data/10001",
		sourceBase + "/_manifest/17.avro":               "insert_log/444/555/666/_manifest/17.avro",
		sourceBase + "/_stats/text_index.100/idx":       "insert_log/444/555/666/_stats/text_index.100/idx",
		sourceBase + "/_stats/json_stats.101/meta.json": "insert_log/444/555/666/_stats/json_stats.101/meta.json",
		sourceLob:   "insert_log/444/555/lobs/100/_data/lob.vx",
		sourceStats: "stats_log/444/555/666/100/10002",
		sourceBM25:  "bm25_stats/444/555/666/100/10003",
		sourceDelta: "delta_log/444/555/666/10004",
		sourceIndex: "index_v1/444/555/666/7777/1/index.v3",
	}
	// Mock the foreign provider's enumeration, but perform every destination
	// write through a real LocalChunkManager. Manifest contents are opaque here;
	// this test checks copy destinations, not native manifest deserialization.
	mInventory := mockey.Mock(snapshotstorage.ReadNativeManifestFiles).To(func(manifest string, _ *indexpb.StorageConfig) (*snapshotformat.NativeManifestFiles, error) {
		base, _, err := packed.UnmarshalManifestPath(manifest)
		require.NoError(t, err)
		_ = base

		require.Equal(t, sourceBase, base)
		return manifestInventoryForTest([]string{
			sourceBase + "/_data/10001", sourceBase + "/_manifest/17.avro",
			sourceBase + "/_stats/text_index.100/idx", sourceBase + "/_stats/json_stats.101/meta.json",
		}, []packed.LobFileInfo{{Path: sourceLob, FieldID: 100}}), nil
	}).Build()
	defer mInventory.UnPatch()
	binlog := func(file string) []*datapb.FieldBinlog {
		return []*datapb.FieldBinlog{{FieldID: 100, Binlogs: []*datapb.Binlog{{LogPath: file, EntriesNum: 1}}}}
	}
	source := &datapb.CopySegmentSource{
		CollectionId: 111, PartitionId: 222, SegmentId: 333,
		SourceRootPath: "s3://source-bucket/source-root",
		StorageVersion: storage.StorageV3, ManifestHasIndex: proto.Bool(false), ManifestPath: packed.MarshalManifestPath(sourceBase, 17), NumOfRows: 1,
		StatsBinlogs: binlog(sourceStats), Bm25Binlogs: binlog(sourceBM25), DeltaBinlogs: binlog(sourceDelta),
		IndexFiles: []*indexpb.IndexFilePathInfo{{
			BuildID: 7000, IndexFilePaths: []string{sourceIndex},
			IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
		}},
	}
	target := &datapb.CopySegmentTarget{
		CollectionId: 444, PartitionId: 555, SegmentId: 666,
		TargetRootPath: root, NewBuildIds: map[int64]int64{7000: 7777},
	}
	copied := make(map[string]string)
	var copyMu sync.Mutex
	targetStore := newCopySegmentStoreMock(t, func(ctx context.Context, srcBucket, src, dstBucket, dst string) error {
		copyMu.Lock()
		defer copyMu.Unlock()
		assert.Equal(t, "source-bucket", srcBucket)
		assert.Empty(t, dstBucket)
		suffix, ok := mappings[src]
		require.True(t, ok, "unexpected source file %q", src)
		assert.Equal(t, path.Join(root, suffix), dst)
		copied[src] = dst
		return cm.Write(ctx, dst, []byte(src))
	})
	result, copiedFiles, err := CopySegmentAndIndexFiles(ctx, make(chan struct{}, 2), &struct{ storage.ChunkManager }{},
		&indexpb.StorageConfig{StorageType: "remote", RootPath: "source-root", BucketName: "source-bucket"},
		&indexpb.StorageConfig{StorageType: "local", RootPath: root},
		targetStore, "source-bucket", "", source, target, nil)
	require.NoError(t, err)
	require.Len(t, copiedFiles, len(mappings))
	require.Len(t, copied, len(mappings))
	assert.Equal(t, packed.MarshalManifestPath(path.Join(root, "insert_log/444/555/666"), 17), result.GetManifestPath())
	for src, suffix := range mappings {
		contents, err := cm.Read(ctx, path.Join(root, suffix))
		require.NoError(t, err)
		assert.Equal(t, src, string(contents))
	}
	assert.NoDirExists(t, path.Join(root, "files"), "a restore must not create the source's legacy namespace")
	assert.NoDirExists(t, path.Join(root, "source-root"))
}

func TestGenerateMappingsFromFiles_RemoteCanonicalTargetRoot(t *testing.T) {
	const prefix = "source-root/files/"
	const insert = prefix + "insert_log/111/222/333/_data/10001"
	const delta = prefix + "delta_log/111/222/333/10002"
	const stats = prefix + "stats_log/111/222/333/100/10003"
	const bm25 = prefix + "bm25_stats/111/222/333/100/10004"
	const lob = prefix + "insert_log/111/222/lobs/100/_data/lob.vx"
	const v0 = prefix + "index_files/7000/1/222/333/index.v3"
	const v1 = prefix + "index_v1/111/222/333/7000/1/index.v3"
	const text = prefix + "text_log/7000/1/111/222/333/100/index"
	const jsonKey = prefix + "json_key_index_log/7000/1/111/222/333/100/index"
	const jsonStats = prefix + "json_stats/2/7000/1/111/222/333/100/meta.json"
	source := &datapb.CopySegmentSource{
		CollectionId: 111, PartitionId: 222, SegmentId: 333,
		SourceRootPath: "s3://source-bucket/source-root",
		IndexFiles: []*indexpb.IndexFilePathInfo{{
			IndexFilePaths:        []string{v1},
			IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
		}},
	}
	files := &SegmentFiles{
		InsertBinlogs: []string{insert}, DeltaBinlogs: []string{delta}, StatsBinlogs: []string{stats}, Bm25Binlogs: []string{bm25},
		LobFiles: []string{lob}, VectorScalarIndex: []string{v0, v1}, TextIndex: []string{text}, JSONKeyIndex: []string{jsonKey}, JSONStats: []string{jsonStats},
	}
	for _, root := range []string{"files", "tenant/files", "", "."} {
		t.Run(fmt.Sprintf("root_%q", root), func(t *testing.T) {
			target := &datapb.CopySegmentTarget{
				CollectionId: 444, PartitionId: 555, SegmentId: 666,
				TargetRootPath: root, NewBuildIds: map[int64]int64{7000: 7777},
			}
			got, err := generateMappingsFromFiles(files, source, target)
			require.NoError(t, err)
			assert.Equal(t, map[string]string{
				insert:    path.Join(root, "insert_log/444/555/666/_data/10001"),
				delta:     path.Join(root, "delta_log/444/555/666/10002"),
				stats:     path.Join(root, "stats_log/444/555/666/100/10003"),
				bm25:      path.Join(root, "bm25_stats/444/555/666/100/10004"),
				lob:       path.Join(root, "insert_log/444/555/lobs/100/_data/lob.vx"),
				v0:        path.Join(root, "index_files/7777/1/555/666/index.v3"),
				v1:        path.Join(root, "index_v1/444/555/666/7777/1/index.v3"),
				text:      path.Join(root, "text_log/7777/1/444/555/666/100/index"),
				jsonKey:   path.Join(root, "json_key_index_log/7777/1/444/555/666/100/index"),
				jsonStats: path.Join(root, "json_stats/2/7777/1/444/555/666/100/meta.json"),
			}, got)
		})
	}
}

// TestCopySegmentAndIndexFiles_V3WithTextAndJsonStats verifies that V3 segments
// with text index and JSON key stats succeed during copy. Before the fix, V3
// segments had wrong-format paths in TextIndexFiles/JsonKeyIndexFiles (etcd
// metadata), causing "no mapping found" errors during buildIndexInfoFromSource.
// The fix skips pb path extraction for V3 (files already copied via manifest
// basePath/_stats/) and passes metadata as placeholders.
func TestCopySegmentAndIndexFiles_V3WithTextAndJsonStats(t *testing.T) {
	mockCopiedManifestIndexEntries(t, nil)

	manifestPath := packed.MarshalManifestPath("files/insert_log/111/222/333", 2)

	source := &datapb.CopySegmentSource{
		CollectionId:   111,
		PartitionId:    222,
		SegmentId:      333,
		StorageVersion: storage.StorageV3,
		ManifestPath:   manifestPath,
		InsertBinlogs: []*datapb.FieldBinlog{
			{
				FieldID: 100,
				Binlogs: []*datapb.Binlog{
					{EntriesNum: 500, LogPath: "files/insert_log/111/222/333/100/10001", LogSize: 1024},
				},
			},
		},
		// Text index with relative paths (as stored in etcd after dual-write fix)
		TextIndexFiles: map[int64]*datapb.TextIndexStats{
			101: {
				FieldID: 101,
				BuildID: 7000,
				Version: 1,
				Files:   []string{"tokenizer.json", "index.data"},
			},
		},
		// JSON key stats with relative paths (as stored in etcd after dual-write fix)
		JsonKeyIndexFiles: map[int64]*datapb.JsonKeyStats{
			102: {
				FieldID:                102,
				BuildID:                8000,
				Version:                1,
				Files:                  []string{"shared_key_index/.managed.json_0", "shared_key_index/.managed.json_1"},
				JsonKeyStatsDataFormat: 3,
			},
		},
	}

	target := &datapb.CopySegmentTarget{
		TargetRootPath: "files",
		CollectionId:   444,
		PartitionId:    555,
		SegmentId:      666,
		NewBuildIds:    map[int64]int64{7000: 7777, 8000: 9000},
	}

	mInventory := mockey.Mock(snapshotstorage.ReadNativeManifestFiles).To(func(_ string, _ *indexpb.StorageConfig) (*snapshotformat.NativeManifestFiles, error) {
		// The native manifest lists the physical TEXT/JSON files under _stats/.
		return manifestInventoryForTest([]string{
			"files/insert_log/111/222/333/_data/0",
			"files/insert_log/111/222/333/_metadata/manifest.json",
			"files/insert_log/111/222/333/_stats/text_index.101/tokenizer.json",
			"files/insert_log/111/222/333/_stats/text_index.101/index.data",
			"files/insert_log/111/222/333/_stats/json_stats.102/shared_key_index/.managed.json_0",
			"files/insert_log/111/222/333/_stats/json_stats.102/shared_key_index/.managed.json_1",
		}, nil), nil
	}).Build()
	defer mInventory.UnPatch()

	cm := &struct{ storage.ChunkManager }{}
	result, copiedFiles, err := CopySegmentAndIndexFiles(
		context.Background(),
		make(chan struct{}, 2),
		cm,
		&indexpb.StorageConfig{BucketName: "test-bucket"},
		&indexpb.StorageConfig{BucketName: "test-bucket"},
		newCopySegmentStoreMock(t, nil),
		"test-bucket",
		"test-bucket",
		source,
		target,
		nil,
	)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, int64(666), result.SegmentId)

	// Verify text index metadata is passed through as placeholder
	assert.Len(t, result.TextIndexInfos, 1)
	assert.Contains(t, result.TextIndexInfos, int64(101))
	// Files are passed through as-is (placeholder, not mapped)
	assert.Equal(t, []string{"tokenizer.json", "index.data"}, result.TextIndexInfos[101].GetFiles())
	assert.Equal(t, int64(7777), result.TextIndexInfos[101].GetBuildID(), "text index buildID should be remapped")

	// Verify JSON key stats metadata is passed through as placeholder with new buildID
	assert.Len(t, result.JsonKeyIndexInfos, 1)
	assert.Contains(t, result.JsonKeyIndexInfos, int64(102))
	assert.Equal(t, int64(9000), result.JsonKeyIndexInfos[102].GetBuildID(), "buildID should be remapped")
	// Files are passed through as-is (placeholder, not mapped)
	assert.Equal(t, []string{"shared_key_index/.managed.json_0", "shared_key_index/.managed.json_1"},
		result.JsonKeyIndexInfos[102].GetFiles())

	// Verify manifest path is transformed
	expectedManifestPath := packed.MarshalManifestPath("files/insert_log/444/555/666", 2)
	assert.Equal(t, expectedManifestPath, result.ManifestPath)

	// Verify _stats files are included in the copy (from the pinned inventory, part of InsertBinlogs)
	assert.True(t, len(copiedFiles) >= 6, "should copy all files including _stats/")
}

func TestBuildTargetManifestIndexes(t *testing.T) {
	manifestPath := packed.MarshalManifestPath("files/insert_log/100/200/300", 3)
	target := &datapb.CopySegmentTarget{
		CollectionId: 100,
		PartitionId:  200,
		SegmentId:    300,
		TargetIndexes: []*datapb.CopySegmentTargetIndex{
			{
				IndexName:  "vec_idx",
				IndexId:    777,
				FieldId:    101,
				ColumnName: "vector",
				IndexType:  "HNSW",
				Properties: map[string]string{"M": "16"},
			},
		},
	}
	indexInfos := map[int64]*datapb.VectorScalarIndexInfo{
		888: {
			IndexName:             "vec_idx",
			IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED,
			BuildId:               888,
			Version:               2,
			IndexSize:             1024,
			IndexFilePaths:        []string{"files/insert_log/100/200/300/_index/888/2/a", "files/insert_log/100/200/300/_index/888/2/b"},
		},
	}

	entries, err := buildTargetManifestIndexes(context.TODO(), manifestPath, target, 4096, indexInfos)
	assert.NoError(t, err)
	require.Len(t, entries, 1)

	entry := entries[0]
	// Identity comes from the target definition DataCoord shipped, never from
	// the source snapshot: RestoreIndexes() reallocates index IDs.
	assert.Equal(t, int64(777), entry.IndexID)
	assert.Equal(t, int64(101), entry.FieldID)
	assert.Equal(t, "vector", entry.ColumnName)
	assert.Equal(t, "HNSW", entry.IndexType)
	// Physical facts come from the artifacts this copy wrote.
	assert.Equal(t, int64(888), entry.BuildID)
	assert.Equal(t, int64(2), entry.IndexVersion)
	assert.Equal(t, int64(4096), entry.NumRows)
	assert.Equal(t, []string{"a", "b"}, entry.IndexFileKeys)
	assert.Equal(t, "16", entry.Properties["M"])
	assert.Equal(t, "HNSW", entry.Properties[common.IndexTypeKey])

	expectedPath, err := packed.ManifestIndexRelativePath("files/insert_log/100/200/300", "files/insert_log/100/200/300/_index/888/2")
	assert.NoError(t, err)
	assert.Equal(t, expectedPath, entry.Path)
	assert.Equal(t, "888/2", entry.Path)
}

func TestBuildTargetManifestIndexes_NoTargetDefinition(t *testing.T) {
	manifestPath := packed.MarshalManifestPath("files/insert_log/100/200/300", 3)
	target := &datapb.CopySegmentTarget{SegmentId: 300}
	indexInfos := map[int64]*datapb.VectorScalarIndexInfo{
		888: {
			IndexName:      "dropped_idx",
			IndexFilePaths: []string{"files/index/100/200/300/888/2/a"},
		},
	}

	entries, err := buildTargetManifestIndexes(context.TODO(), manifestPath, target, 4096, indexInfos)
	assert.NoError(t, err)
	assert.Empty(t, entries)
}

// A finished SegmentIndex whose record names no file keys - a build too small
// to produce an artifact, or an index a snapshot captured without its files -
// still reaches the worker as an IndexFilePathInfo, because uncompressIndexFiles
// emits one per finished record regardless. It carries nothing a manifest can
// point at, and the rest of the copy path has always tolerated it, so it must be
// skipped here rather than fail the whole restore.
func TestBuildTargetManifestIndexes_NoArtifactPathIsSkippedNotFatal(t *testing.T) {
	manifestPath := packed.MarshalManifestPath("files/insert_log/100/200/300", 3)
	target := &datapb.CopySegmentTarget{
		SegmentId: 300,
		TargetIndexes: []*datapb.CopySegmentTargetIndex{
			{IndexName: "empty_idx", IndexId: 10, FieldId: 101, ColumnName: "vec", IndexType: "HNSW"},
			{IndexName: "real_idx", IndexId: 11, FieldId: 102, ColumnName: "vec2", IndexType: "HNSW"},
		},
	}
	indexInfos := map[int64]*datapb.VectorScalarIndexInfo{
		888: {IndexName: "empty_idx", BuildId: 888},
		889: {
			IndexName:      "real_idx",
			BuildId:        889,
			IndexFilePaths: []string{"files/insert_log/100/200/300/_index/889/2/a"},
		},
	}

	entries, err := buildTargetManifestIndexes(context.TODO(), manifestPath, target, 4096, indexInfos)
	require.NoError(t, err)
	// The empty one is dropped; the one with a real artifact still lands, so the
	// skip cannot silently swallow a sibling index in the same segment.
	require.Len(t, entries, 1)
	assert.Equal(t, "real_idx", entries[0].IndexName)
}

func TestSplitIndexArtifactPaths(t *testing.T) {
	dir, keys, err := splitIndexArtifactPaths([]string{"a/b/c/1", "a/b/c/2"})
	assert.NoError(t, err)
	assert.Equal(t, "a/b/c", dir)
	assert.Equal(t, []string{"1", "2"}, keys)

	_, _, err = splitIndexArtifactPaths(nil)
	assert.Error(t, err)

	_, _, err = splitIndexArtifactPaths([]string{"bare"})
	assert.Error(t, err)

	// A manifest entry stores one directory plus file names, so artifacts split
	// across directories cannot be represented and must be rejected.
	_, _, err = splitIndexArtifactPaths([]string{"a/b/1", "a/c/2"})
	assert.Error(t, err)

	// The DataCoord-facing result shortens index paths to their base names,
	// which no longer identify a directory. The manifest entry must therefore be
	// derived before that shortening; this guards the ordering.
	_, _, err = splitIndexArtifactPaths(shortenIndexFilePaths([]string{"a/b/c/1", "a/b/c/2"}))
	assert.Error(t, err)
}

// CopySegmentTask is the DataNode worker used by snapshot restore. Exercise its
// final manifest state against the real local FFI rather than only inspecting
// the ManifestUpdates passed to a mock: off must remove inherited source
// entries and leave the target index-free; on must replace them with the
// target collection's identity and copied build.
func TestRepublishCopiedManifestIndexes_WritePlacementMatrix(t *testing.T) {
	for _, tc := range []struct {
		name    string
		enabled bool
	}{
		{name: "manifest writes disabled", enabled: false},
		{name: "manifest writes enabled", enabled: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &indexpb.StorageConfig{StorageType: "local", RootPath: t.TempDir()}
			// Local storage consumes complete paths; RootPath is not prepended by the filesystem.
			basePath := filepath.Join(cfg.GetRootPath(), "copy_manifest_write_matrix", strings.ReplaceAll(tc.name, " ", "_"))
			sourceIndexPath := filepath.Join(basePath, "_index/6001/1")
			relativePath, err := packed.ManifestIndexRelativePath(basePath, sourceIndexPath)
			require.NoError(t, err)
			copiedManifest, err := packed.CommitManifestUpdates(packed.MarshalManifestPath(basePath, packed.ManifestEarliest), cfg,
				&packed.ManifestUpdates{Indexes: []packed.ManifestIndexInfo{{
					ColumnName:            "vector",
					IndexName:             "source_idx",
					IndexType:             "HNSW",
					Path:                  relativePath,
					FieldID:               101,
					IndexID:               5001,
					BuildID:               6001,
					IndexVersion:          1,
					NumRows:               4096,
					IndexFileKeys:         []string{"source.bin"},
					IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED,
				}}})
			require.NoError(t, err)

			target := &datapb.CopySegmentTarget{
				CollectionId: 100,
				PartitionId:  200,
				SegmentId:    300,
			}
			if tc.enabled {
				target.TargetIndexes = []*datapb.CopySegmentTargetIndex{
					{IndexName: "vec_idx", IndexId: 777, FieldId: 101, ColumnName: "vector", IndexType: "HNSW"},
				}
			}
			indexInfos := map[int64]*datapb.VectorScalarIndexInfo{
				888: {
					IndexName:             "vec_idx",
					IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED,
					BuildId:               888,
					Version:               2,
					IndexFilePaths:        []string{filepath.Join(basePath, "_index/888/2/target.bin")},
				},
			}

			republished, publishedBuildIDs, err := republishCopiedManifestIndexes(
				context.TODO(), copiedManifest, target, 4096, cfg, indexInfos, false, nil)
			require.NoError(t, err)
			entries, err := packed.GetManifestIndexInfos(republished, cfg)
			require.NoError(t, err)
			if tc.enabled {
				assert.Equal(t, []int64{888}, publishedBuildIDs)
				require.Len(t, entries, 1)
				assert.Equal(t, int64(777), entries[0].IndexID)
				assert.Equal(t, int64(888), entries[0].BuildID)
				assert.Equal(t, "vec_idx", entries[0].IndexName)
			} else {
				assert.Empty(t, publishedBuildIDs)
				assert.Empty(t, entries)
			}
		})
	}
}

func TestRepublishCopiedManifestIndexes_LegacySourceStillRecordsIndexes(t *testing.T) {
	manifestPath := packed.MarshalManifestPath("files/insert_log/100/200/300", 3)
	target := &datapb.CopySegmentTarget{
		CollectionId: 100,
		PartitionId:  200,
		SegmentId:    300,
		TargetIndexes: []*datapb.CopySegmentTargetIndex{
			{IndexName: "vec_idx", IndexId: 777, FieldId: 101, ColumnName: "vector", IndexType: "HNSW"},
		},
	}
	indexInfos := map[int64]*datapb.VectorScalarIndexInfo{
		888: {
			IndexName:      "vec_idx",
			BuildId:        888,
			Version:        2,
			IndexFilePaths: []string{"files/insert_log/100/200/300/_index/888/2/a"},
		},
	}

	// The sticky marker proves this legacy source manifest has no index section,
	// so the copied manifest does not need to be read before adding the target.
	manifestReads := 0
	mock := mockey.Mock(packed.GetManifestIndexInfos).To(
		func(string, *indexpb.StorageConfig) ([]packed.ManifestIndexInfo, error) {
			manifestReads++
			return nil, nil
		}).Build()
	defer mock.UnPatch()

	var gotDrops []int64
	var gotIndexes []packed.ManifestIndexInfo
	var captured bool
	republished := packed.MarshalManifestPath("files/insert_log/100/200/300", 4)
	defer mockey.Mock(packed.ReplaceCopiedManifestIndexes).To(
		func(basePath string, baseVersion int64, storageConfig *indexpb.StorageConfig,
			drops []int64, indexes []packed.ManifestIndexInfo,
		) (string, error) {
			gotDrops, gotIndexes, captured = drops, indexes, true
			return republished, nil
		}).Build().UnPatch()

	got, publishedBuildIDs, err := republishCopiedManifestIndexes(
		context.TODO(), manifestPath, target, 4096, &indexpb.StorageConfig{}, indexInfos, true, nil)
	assert.NoError(t, err)
	assert.Equal(t, republished, got, "a new revision must be published")

	require.True(t, captured, "the commit must have been reached")
	assert.Empty(t, gotDrops, "nothing was inherited, so nothing is retracted")
	require.Len(t, gotIndexes, 1, "the etcd-recorded index must still be written to the new manifest")
	assert.Equal(t, int64(777), gotIndexes[0].IndexID)
	assert.Equal(t, "vec_idx", gotIndexes[0].IndexName)
	assert.Equal(t, int64(888), gotIndexes[0].BuildID)
	assert.Equal(t, []int64{888}, publishedBuildIDs)
	assert.Zero(t, manifestReads)
}

// External restore: the source manifest sits in a foreign bucket DataCoord
// cannot address, so it deliberately ships NO inherited index IDs - yet the
// worker byte-copied that manifest into the target bucket, entries and all.
// The retraction must therefore be driven by what the copied manifest actually
// holds, not by the shipped list: every enumerated entry is dropped and only
// the re-derived target entries are installed. The shipped list being out of
// step is a logged inconsistency, never a failure.
func TestRepublishCopiedManifestIndexes_DropsEntriesAbsentFromShippedList(t *testing.T) {
	manifestPath := packed.MarshalManifestPath("files/insert_log/100/200/300", 3)
	target := &datapb.CopySegmentTarget{
		CollectionId: 100,
		PartitionId:  200,
		SegmentId:    300,
		TargetIndexes: []*datapb.CopySegmentTargetIndex{
			{IndexName: "vec_idx", IndexId: 777, FieldId: 101, ColumnName: "vector", IndexType: "HNSW"},
		},
	}
	indexInfos := map[int64]*datapb.VectorScalarIndexInfo{
		888: {
			IndexName:      "vec_idx",
			BuildId:        888,
			Version:        2,
			IndexFilePaths: []string{"files/insert_log/100/200/300/_index/888/2/a"},
		},
	}

	// The copied manifest still carries the SOURCE collection's entries,
	// including one for an index the snapshot no longer defines, and a
	// duplicate ID that must collapse to a single drop.
	mockCopiedManifestIndexEntries(t, []packed.ManifestIndexInfo{
		{IndexID: 5001, BuildID: 6001, IndexName: "vec_idx"},
		{IndexID: 5002, BuildID: 6002, IndexName: "dropped_idx"},
		{IndexID: 5002, BuildID: 6003, IndexName: "dropped_idx"},
	})

	var gotDrops []int64
	var gotIndexes []packed.ManifestIndexInfo
	var captured bool
	republished := packed.MarshalManifestPath("files/insert_log/100/200/300", 4)
	defer mockey.Mock(packed.ReplaceCopiedManifestIndexes).To(
		func(basePath string, baseVersion int64, storageConfig *indexpb.StorageConfig,
			drops []int64, indexes []packed.ManifestIndexInfo,
		) (string, error) {
			gotDrops, gotIndexes, captured = drops, indexes, true
			return republished, nil
		}).Build().UnPatch()

	got, publishedBuildIDs, err := republishCopiedManifestIndexes(
		context.TODO(), manifestPath, target, 4096, &indexpb.StorageConfig{}, indexInfos, false, nil)
	assert.NoError(t, err)
	assert.Equal(t, republished, got, "a new revision must be published")

	require.True(t, captured, "the commit must have been reached")
	assert.ElementsMatch(t, []int64{5001, 5002, 5002}, gotDrops,
		"all inherited entries reach the copied-manifest helper, which deduplicates IDs")
	require.Len(t, gotIndexes, 1, "only the re-derived target entries are installed")
	assert.Equal(t, int64(777), gotIndexes[0].IndexID)
	assert.Equal(t, int64(888), gotIndexes[0].BuildID)
	assert.Equal(t, []int64{888}, publishedBuildIDs)
}

func TestGenerateTargetIndexPath_SegmentRooted(t *testing.T) {
	source := &datapb.CopySegmentSource{CollectionId: 1, PartitionId: 2, SegmentId: 3, StorageVersion: storage.StorageV3}
	target := &datapb.CopySegmentTarget{
		CollectionId: 10, PartitionId: 20, SegmentId: 30, TargetRootPath: "target",
		NewBuildIds: map[int64]int64{100: 200}, IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED.Enum(),
	}
	for _, tc := range []struct {
		version indexpb.IndexStorePathVersion
		path    string
	}{
		{0, "source/index_files/100/4/2/3/index.bin"},
		{1, "source/index_v1/1/2/3/100/4/index.bin"},
		{2, "source/insert_log/1/2/3/_index/100/4/index.bin"},
	} {
		got, err := generateTargetIndexPath(tc.path, source, target, IndexTypeVectorScalarV0, tc.version)
		require.NoError(t, err)
		require.Equal(t, "target/insert_log/10/20/30/_index/200/4/index.bin", got)
		source.IndexFiles = []*indexpb.IndexFilePathInfo{{BuildID: 100, IndexStorePathVersion: tc.version, IndexFilePaths: []string{tc.path}}}
		infos, _, _, err := buildIndexInfoFromSource(source, target, map[string]string{tc.path: got})
		require.NoError(t, err)
		require.Equal(t, indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED, infos[200].GetIndexStorePathVersion())
	}
	_, err := generateTargetIndexPath("source/insert_log/1/2/3/_data/100/4/index.bin", source, target, IndexTypeVectorScalarV0, 2)
	require.Error(t, err)
	// An empty destination root addresses the bucket root, not the source root.
	target.TargetRootPath = ""
	got, err := generateTargetIndexPath("source/index_files/100/4/2/3/index.bin", source, target, IndexTypeVectorScalarV0, 0)
	require.NoError(t, err)
	require.Equal(t, "insert_log/10/20/30/_index/200/4/index.bin", got)

	// v2 also carries collection/partition/segment IDs, but it is not v1.
	// Converting it must keep the file offset after _index/build/version.
	target.TargetRootPath = "target"
	for _, tc := range []struct {
		version indexpb.IndexStorePathVersion
		want    string
	}{
		{0, "target/index_files/200/4/20/30/index.bin"},
		{1, "target/index_v1/10/20/30/200/4/index.bin"},
	} {
		target.IndexStorePathVersion = tc.version.Enum()
		got, err := generateTargetIndexPath("source/insert_log/1/2/3/_index/100/4/index.bin", source, target, IndexTypeVectorScalarV0, 2)
		require.NoError(t, err)
		require.Equal(t, tc.want, got)
	}
}

func TestBuildTargetManifestIndexes_RejectsExternalPaths(t *testing.T) {
	target := &datapb.CopySegmentTarget{TargetIndexes: []*datapb.CopySegmentTargetIndex{{IndexName: "idx", IndexId: 7}}}
	_, err := buildTargetManifestIndexes(context.TODO(), packed.MarshalManifestPath("files/insert_log/1/2/3", 1), target, 100,
		map[int64]*datapb.VectorScalarIndexInfo{5: {
			IndexName: "idx", BuildId: 5, Version: 1,
			IndexFilePaths: []string{"files/index_files/5/1/2/3/index.bin"},
		}})
	require.ErrorContains(t, err, "outside segment index directory")

	// The staged Restore pipeline must reject this layout before copying bytes.
	sourcePath := "files/index_files/5/1/2/3/index.bin"
	source := &datapb.CopySegmentSource{
		CollectionId: 1, PartitionId: 2, SegmentId: 3, StorageVersion: storage.StorageV3,
		ManifestPath: packed.MarshalManifestPath("files/insert_log/1/2/3", 1),
		IndexFiles: []*indexpb.IndexFilePathInfo{{
			IndexName: "idx", BuildID: 5,
			IndexVersion: 1, IndexFilePaths: []string{sourcePath},
		}},
	}
	target.CollectionId, target.PartitionId, target.SegmentId = 10, 20, 30
	target.TargetRootPath = "files"
	target.NewBuildIds = map[int64]int64{5: 50}
	defer mockey.Mock(collectSegmentFiles).Return(&SegmentFiles{VectorScalarIndex: []string{sourcePath}}, nil).Build().UnPatch()
	copied := 0
	targetStore := newCopySegmentStoreMock(t, func(context.Context, string, string, string, string) error {
		copied++
		return nil
	})
	_, writes, err := CopySegmentAndIndexFiles(context.Background(), make(chan struct{}, 1), nil, nil, nil, targetStore, "", "", source, target, nil)
	require.ErrorContains(t, err, "outside segment index directory")
	require.Zero(t, copied)
	require.Empty(t, writes)
}

// Export leaves both manifests immutable but only copies selected indexes.
// Exercise real native reads/commits to prove Restore can discard inherited
// entries whose artifacts are absent from the exported source bundle.
func TestCopySegmentAndIndexFiles_SelectedIndexesWithMissingInheritedArtifacts(t *testing.T) {
	paramtable.Init()
	for _, avroWins := range []bool{false, true} {
		t.Run(fmt.Sprintf("avro-wins-%t", avroWins), func(t *testing.T) {
			ctx := context.Background()
			root := t.TempDir()
			cm := storage.NewLocalChunkManager(objectstorage.RootPath(root))
			cfg := &indexpb.StorageConfig{StorageType: "local", RootPath: root}
			base := path.Join(root, "insert_log/100/10/1")
			makeEntry := func(id, build int64) packed.ManifestIndexInfo {
				prefix := path.Join(base, fmt.Sprintf("_index/%d/1", build))
				relative, err := packed.ManifestIndexRelativePath(base, prefix)
				require.NoError(t, err)
				return packed.ManifestIndexInfo{
					IndexID: id, BuildID: build, IndexVersion: 1, FieldID: 101,
					ColumnName: "vec", IndexName: "idx", IndexType: "HNSW", Path: relative, IndexFileKeys: []string{"index.bin"},
					IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED,
				}
			}
			manifest, err := packed.CommitManifestUpdates(packed.MarshalManifestPath(base, 0), cfg, &packed.ManifestUpdates{
				Indexes: []packed.ManifestIndexInfo{makeEntry(1001, 3001), makeEntry(9999, 4001)},
			})
			require.NoError(t, err)
			segment := &datapb.SegmentDescription{
				SegmentId: 1, PartitionId: 10, StorageVersion: storage.StorageV3,
				ManifestPath: manifest, ManifestHasIndex: proto.Bool(true),
			}
			build := int64(3001)
			if avroWins {
				build = 5001
				segment.IndexFiles = []*indexpb.IndexFilePathInfo{{
					SegmentID: 1, FieldID: 101, IndexID: 1001, BuildID: build,
					IndexVersion: 1, IndexName: "idx",
					IndexFilePaths:        []string{path.Join(root, "index_files/5001/1/10/1/index.bin")},
					IndexParams:           []*commonpb.KeyValuePair{{Key: "index_type", Value: "HNSW"}},
					IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_BUILD_ROOTED,
				}}
			}
			selectedPath := path.Join(root, fmt.Sprintf("index_files/%d/1/10/1/index.bin", build))
			require.NoError(t, cm.Write(ctx, selectedPath, []byte("selected-index")))
			source := &datapb.CopySegmentSource{
				CollectionId: 100, PartitionId: 10, SegmentId: 1,
				StorageVersion: storage.StorageV3, ManifestPath: manifest, ManifestHasIndex: proto.Bool(true), IndexFiles: segment.IndexFiles,
			}
			target := &datapb.CopySegmentTarget{
				CollectionId: 200, PartitionId: 20, SegmentId: 2, TargetRootPath: root,
				IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED.Enum(),
				NewBuildIds:           map[int64]int64{build: 9001},
				TargetIndexes:         []*datapb.CopySegmentTargetIndex{{IndexId: 2001, IndexName: "idx", IndexType: "HNSW", FieldId: 101, ColumnName: "vec"}},
			}
			var copied []string
			var copyMu sync.Mutex
			targetStore := newCopySegmentStoreMock(t, func(ctx context.Context, _, src, _, dst string) error {
				copyMu.Lock()
				defer copyMu.Unlock()
				copied = append(copied, src)
				return cm.Copy(ctx, src, dst)
			})
			indexRead := mockey.Mock(packed.GetManifestIndexInfos).To(func(string, *indexpb.StorageConfig) ([]packed.ManifestIndexInfo, error) {
				t.Error("Restore must reuse the preparation inventory, including drop resolution")
				return nil, merr.ErrServiceInternal
			}).Build()
			defer indexRead.UnPatch()
			result, _, err := CopySegmentAndIndexFiles(ctx, make(chan struct{}, 2), cm, cfg, cfg, targetStore, "", "", source, target, nil)
			indexRead.UnPatch()
			require.NoError(t, err)
			manifestObject, err := packed.ManifestFilePath(manifest)
			require.NoError(t, err)
			if avroWins {
				require.ElementsMatch(t, []string{manifestObject, selectedPath}, copied)
			} else {
				require.Equal(t, []string{manifestObject}, copied)
			}
			restored, err := packed.GetManifestIndexInfos(result.GetManifestPath(), cfg)
			require.NoError(t, err)
			if !avroWins {
				require.Empty(t, restored, "all inherited indexes must be removed when Avro is empty")
				require.Empty(t, result.IndexInfos)
				return
			}
			require.Len(t, restored, 1)
			require.Equal(t, int64(2001), restored[0].IndexID)
			require.Equal(t, int64(9001), restored[0].BuildID)
			data, err := cm.Read(ctx, path.Join(restored[0].Path, restored[0].IndexFileKeys[0]))
			require.NoError(t, err)
			require.Equal(t, "selected-index", string(data))
		})
	}
}

func TestCopySegmentAndIndexFiles_PinnedDataInventoryMatchesExport(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	root := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(root))
	cfg := &indexpb.StorageConfig{StorageType: "local", RootPath: root}
	base := path.Join(root, "insert_log/100/10/1")
	data, delta := path.Join(base, "_data/1.parquet"), path.Join(base, "_delta/2")
	text, jsonFile := path.Join(base, "_stats/text_index.100/index"), path.Join(base, "_stats/json_stats.101/keys")
	stats, bm25 := path.Join(root, "stats_log/100/10/1/100/3"), path.Join(root, "bm25_stats/100/10/1/100/4")
	orphan := path.Join(base, "_data/unreferenced")
	for _, file := range []string{data, delta, text, jsonFile, stats, bm25, orphan} {
		require.NoError(t, cm.Write(ctx, file, []byte(file)))
	}
	manifest, err := packed.CommitManifestUpdates(packed.MarshalManifestPath(base, 0), cfg, &packed.ManifestUpdates{
		ColumnGroups: []packed.ColumnGroupEntry{{Columns: []string{"100"}, Format: "parquet", Files: []packed.ColumnGroupFileEntry{{Path: data, EndIndex: 1}}}},
		DeltaLogs:    []packed.DeltaLogEntry{{Path: delta, NumEntries: 1}},
		Stats:        []packed.StatEntry{{Key: "text_index.100", Files: []string{text}}, {Key: "json_stats.101", Files: []string{jsonFile}}},
	})
	require.NoError(t, err)
	_, revision, err := packed.UnmarshalManifestPath(manifest)
	require.NoError(t, err)
	_, err = packed.CommitManifestUpdates(packed.MarshalManifestPath(base, revision), cfg, &packed.ManifestUpdates{Stats: []packed.StatEntry{{Key: "text_index.100", Files: []string{orphan}}}})
	require.NoError(t, err)
	logs := func(file string) []*datapb.FieldBinlog {
		return []*datapb.FieldBinlog{{FieldID: 100, Binlogs: []*datapb.Binlog{{LogPath: file, EntriesNum: 1}}}}
	}
	segment := &datapb.SegmentDescription{
		SegmentId: 1, PartitionId: 10, StorageVersion: storage.StorageV3,
		NumOfRows: 1, ManifestPath: manifest, ManifestHasIndex: proto.Bool(false),
		Deltalogs: logs(delta), Statslogs: logs(stats), Bm25Statslogs: logs(bm25),
	}
	// Keep a pathless logical summary alongside an already-referenced physical delta.
	segment.Deltalogs[0].Binlogs = append(segment.Deltalogs[0].Binlogs, &datapb.Binlog{LogID: 9, EntriesNum: 1})
	snapshot := &snapshotstorage.SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{Id: 1, CollectionId: 100},
		Collection:   &datapb.CollectionDescription{}, Segments: []*datapb.SegmentDescription{segment},
	}
	metadata, _, err := snapshotstorage.NewSnapshotWriter(cm).SaveToRootWithSize(ctx, snapshot, root, datapb.SnapshotLayout_SnapshotLayoutReferenced)
	require.NoError(t, err)
	loaded, err := snapshotstorage.NewSnapshotReader(cm).ReadSnapshot(ctx, metadata, true)
	require.NoError(t, err)
	// Both ordinary V3 paths must succeed without directory listing or a second
	// manifest read just for LOBs. All manifest reads and commits here are real.
	defer mockey.Mock((*storage.LocalChunkManager).WalkWithPrefix).Return(merr.ErrServiceInternal).Build().UnPatch()
	defer mockey.Mock(packed.GetManifestLobFiles).Return(nil, merr.ErrServiceInternal).Build().UnPatch()
	var exportFiles []string
	exportStore := &copySegmentExportStoreTarget{}
	recordExport := func(_ context.Context, _, src, _, dst string) error {
		if src != loaded.ManifestPaths[0] {
			exportFiles = append(exportFiles, src)
		}
		return nil
	}
	defer mockey.Mock((*copySegmentExportStoreTarget).Copy).To(
		func(ctx context.Context, req objectstore.CopyRequest) (int64, error) {
			srcBucket := req.Source.Bucket
			src := req.Source.Key
			dstBucket := req.Destination.Bucket
			dst := req.Destination.Key
			return 0, recordExport(ctx, srcBucket, src, dstBucket, dst)
		}).Build().UnPatch()
	_, err = snapshotstorage.ExecuteSnapshotExport(ctx, &datapb.SnapshotExportCopyRequest{
		TaskId: 1, SegmentEnd: 1,
		SnapshotId: 1, CollectionId: 100, SnapshotMetadataPath: metadata, SourceStorageConfig: cfg,
		TargetUri: path.Join(root, "export"),
	}, cm, &snapshotstorage.ResolvedForeignStorage{ForeignCM: cm, ForeignStorageConfig: cfg, TargetStore: exportStore}, make(chan struct{}, 1), nil)
	require.NoError(t, err)
	manifestObject, err := packed.ManifestFilePath(manifest)
	require.NoError(t, err)
	expected := []string{manifestObject, data, delta, text, jsonFile, stats, bm25}
	require.ElementsMatch(t, expected, exportFiles)
	source := &datapb.CopySegmentSource{
		CollectionId: 100, PartitionId: 10, SegmentId: 1, StorageVersion: storage.StorageV3,
		ManifestPath: manifest, NumOfRows: 1, ManifestHasIndex: proto.Bool(false),
		DeltaBinlogs: segment.Deltalogs, StatsBinlogs: segment.Statslogs, Bm25Binlogs: segment.Bm25Statslogs,
	}
	target := &datapb.CopySegmentTarget{CollectionId: 200, PartitionId: 20, SegmentId: 2, TargetRootPath: root}
	var restoreFiles []string
	var copyMu sync.Mutex
	targetStore := newCopySegmentStoreMock(t, func(ctx context.Context, _, src, _, dst string) error {
		copyMu.Lock()
		defer copyMu.Unlock()
		restoreFiles = append(restoreFiles, src)
		return cm.Copy(ctx, src, dst)
	})
	result, _, err := CopySegmentAndIndexFiles(ctx, make(chan struct{}, 2), cm, cfg, cfg, targetStore, "", "", source, target, nil)
	require.NoError(t, err)
	require.ElementsMatch(t, exportFiles, restoreFiles)
	require.Equal(t, int64(1), result.GetImportedRows())
	require.Len(t, result.GetDeltalogs()[0].GetBinlogs(), 2)
	require.Empty(t, result.GetDeltalogs()[0].GetBinlogs()[1].GetLogPath())
	targetInventory, err := snapshotstorage.ReadNativeManifestFiles(result.GetManifestPath(), cfg)
	require.NoError(t, err)
	for _, file := range append(append(targetInventory.ColumnFiles, targetInventory.DeltaFiles...), targetInventory.StatsFiles...) {
		_, err := cm.Read(ctx, file)
		require.NoError(t, err, file)
	}
	// Missing referenced objects remain in the inventory. Copy must fail rather
	// than silently omitting the object as a directory listing would.
	require.NoError(t, cm.Remove(ctx, data))
	target.SegmentId = 3
	result, _, err = CopySegmentAndIndexFiles(ctx, make(chan struct{}, 2), cm, cfg, cfg, targetStore, "", "", source, target, nil)
	require.Error(t, err)
	require.Nil(t, result)
}

func TestCopySegmentAndIndexFiles_DrainsAndTracksLateSuccess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	started, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	targetStore := newCopySegmentStoreMock(t, func(ctx context.Context, _, src, _, _ string) error {
		if strings.HasSuffix(src, "/10002") {
			<-started
			return merr.ErrIoTooManyRequests
		}
		close(started)
		<-ctx.Done()
		close(canceled)
		<-release
		// The provider confirmed the write even though another file canceled the plan.
		return nil
	})
	source := &datapb.CopySegmentSource{
		CollectionId: 111, PartitionId: 222, SegmentId: 333,
		InsertBinlogs: []*datapb.FieldBinlog{{FieldID: 1, Binlogs: []*datapb.Binlog{
			{LogPath: "files/insert_log/111/222/333/1/10001"}, {LogPath: "files/insert_log/111/222/333/1/10002"},
		}}},
	}
	target := &datapb.CopySegmentTarget{CollectionId: 444, PartitionId: 555, SegmentId: 666, TargetRootPath: "files"}
	permits := make(chan struct{}, 2)
	type outcome struct {
		result *datapb.CopySegmentResult
		files  []string
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, files, err := CopySegmentAndIndexFiles(ctx, permits, &struct{ storage.ChunkManager }{}, nil, nil, targetStore, "source", "target", source, target, nil)
		done <- outcome{result, files, err}
	}()
	select {
	case <-canceled:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case <-done:
		t.Error("returned before the active copy exited")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	got := <-done
	require.ErrorIs(t, got.err, merr.ErrIoTooManyRequests)
	require.Nil(t, got.result)
	require.Equal(t, []string{"files/insert_log/444/555/666/1/10001"}, got.files)
	require.Empty(t, permits)
}

func TestCopySegmentTask_SharedLOB(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	root := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(root))
	cfg := &indexpb.StorageConfig{StorageType: "local", RootPath: root}
	lob := path.Join(root, "insert_log/100/10/lobs/shared.bin")
	require.NoError(t, cm.Write(ctx, lob, []byte("shared text")))
	req := &datapb.CopySegmentRequest{JobID: 1, TaskID: 1, StorageConfig: cfg}
	inventory := make(map[string]*snapshotformat.NativeManifestFiles)
	for id := int64(1); id <= 2; id++ {
		base := path.Join(root, fmt.Sprintf("insert_log/100/10/%d", id))
		data := path.Join(base, "_data/1.parquet")
		require.NoError(t, cm.Write(ctx, data, []byte("data")))
		manifest, err := packed.CommitManifestUpdates(packed.MarshalManifestPath(base, 0), cfg, &packed.ManifestUpdates{ColumnGroups: []packed.ColumnGroupEntry{{Columns: []string{"100"}, Format: "parquet", Files: []packed.ColumnGroupFileEntry{{Path: data, EndIndex: 1}}}}})
		require.NoError(t, err)
		files, err := snapshotstorage.ReadNativeManifestFiles(manifest, cfg)
		require.NoError(t, err)
		files.LobFiles = []packed.LobFileInfo{{Path: lob}}
		inventory[manifest] = files
		req.Sources = append(req.Sources, &datapb.CopySegmentSource{CollectionId: 100, PartitionId: 10, SegmentId: id, StorageVersion: storage.StorageV3, ManifestPath: manifest, NumOfRows: 1})
		req.Targets = append(req.Targets, &datapb.CopySegmentTarget{CollectionId: 200, PartitionId: 20, SegmentId: id + 10, TargetRootPath: root})
	}
	var prepared atomic.Int64
	defer mockey.Mock(snapshotstorage.ReadNativeManifestFiles).To(func(manifest string, _ *indexpb.StorageConfig) (*snapshotformat.NativeManifestFiles, error) {
		prepared.Add(1)
		return inventory[manifest], nil
	}).Build().UnPatch()
	var lobCopies atomic.Int64
	targetStore := newCopySegmentStoreMock(t, func(ctx context.Context, _, src, _, dst string) error {
		assert.GreaterOrEqual(t, prepared.Load(), int64(1))
		if src == lob {
			lobCopies.Add(1)
		}
		return cm.Copy(ctx, src, dst)
	})
	manager := NewTaskManager()
	task := NewCopySegmentTask(ctx, make(chan struct{}, 4), req, manager, cm, cm, cfg, targetStore, "", "").(*CopySegmentTask)
	defer task.cancel()
	manager.Add(task)
	require.NoError(t, conc.BlockOnAll(task.Execute()...))
	latest := manager.Get(1).(*CopySegmentTask)
	require.NotEqual(t, datapb.ImportTaskStateV2_Failed, latest.GetState(), latest.GetReason())
	for _, result := range latest.segmentResults {
		require.EqualValues(t, 1, result.GetImportedRows())
	}
	require.EqualValues(t, 2, lobCopies.Load(), "shared LOB copies are idempotent across segment workers")
	require.Len(t, latest.segmentResults, 2)
	bytes, err := cm.Read(ctx, path.Join(root, "insert_log/200/20/lobs/shared.bin"))
	require.NoError(t, err)
	require.Equal(t, "shared text", string(bytes))
	var removed []string
	defer mockey.Mock((*storage.LocalChunkManager).MultiRemove).To(func(_ context.Context, paths []string) error { removed = paths; return nil }).Build().UnPatch()
	latest.CleanupCopiedFiles()
	// Other tasks may reference this partition-level LOB. Clean up only the
	// two segments' private data and manifest objects; LOB GC owns the shared file.
	require.Len(t, removed, 4)
	require.NotContains(t, removed, path.Join(root, "insert_log/200/20/lobs/shared.bin"))
}

// Exercise the real segment pipeline: an invalid native reference prevents
// writes for that segment. Valid peers may already have copied files, which
// must remain tracked for cleanup after all workers drain.
func TestCopySegmentTask_NativeReferenceValidationBeforeWrites(t *testing.T) {
	for _, kind := range []string{"valid", "valid lob", "bucket root", "column", "delta", "stats", "lob", "bucket alias", "endpoint", "scheme", "credentials", "entry", "read failure", "external read failure", "empty entry", "malformed entry", "empty base", "missing manifest"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			req := copyBatchRequestForTest()
			for _, source := range req.Sources {
				source.StorageVersion = storage.StorageV3
				source.NumOfRows = 1
				source.SourceRootPath = "s3://source/bundle/files"
				source.ManifestPath = packed.MarshalManifestPath(fmt.Sprintf("bundle/files/insert_log/100/10/%d", source.SegmentId), 1)
			}
			if kind == "bucket root" {
				for _, source := range req.Sources {
					source.SourceRootPath = "s3://source"
				}
			}
			if kind == "entry" {
				req.Sources[1].ManifestPath = packed.MarshalManifestPath("outside/insert_log/100/10/444", 1)
			}
			switch kind {
			case "external read failure":
				req.Sources[1].IsExternalCollection = true
			case "empty entry":
				req.Sources[1].ManifestPath = ""
			case "malformed entry":
				req.Sources[1].ManifestPath = "invalid-json-not-a-manifest"
			case "empty base":
				req.Sources[1].ManifestPath = packed.MarshalManifestPath("", 1)
			}
			var reads, copies, commits, invalidCopies, invalidCommits atomic.Int64
			defer mockey.Mock(snapshotstorage.ReadNativeManifestFiles).To(func(manifest string, _ *indexpb.StorageConfig) (*snapshotformat.NativeManifestFiles, error) {
				reads.Add(1)
				base, _, err := packed.UnmarshalManifestPath(manifest)
				if err != nil {
					return nil, err
				}
				files := &snapshotformat.NativeManifestFiles{ManifestFile: base + "/_manifest/manifest-1", ColumnFiles: []string{base + "/_data/1.parquet"}}
				if strings.HasSuffix(base, "/444") {
					switch kind {
					case "valid lob":
						files.LobFiles = []packed.LobFileInfo{{Path: "bundle/files/insert_log/100/10/lobs/1/_data/a"}}
					case "column":
						files.ColumnFiles = []string{"outside/insert_log/100/10/444/_data/1.parquet"}
					case "delta":
						files.DeltaFiles = []string{"outside/insert_log/100/10/444/_delta/1"}
					case "stats":
						files.StatsFiles = []string{"outside/insert_log/100/10/444/_stats/text_index.1/a"}
					case "lob":
						files.LobFiles = []packed.LobFileInfo{{Path: "outside/lob/100/10/1"}}
					case "bucket alias":
						files.ColumnFiles = append(files.ColumnFiles, "s3://other/"+files.ColumnFiles[0])
					case "endpoint":
						files.ColumnFiles = []string{"https://other.example/source/" + files.ColumnFiles[0]}
					case "scheme":
						files.ColumnFiles = []string{"gs://source/" + files.ColumnFiles[0]}
					case "credentials":
						files.ColumnFiles = []string{"s3://user:secret@source/" + files.ColumnFiles[0]}
					case "read failure", "external read failure":
						return nil, merr.ErrIoKeyNotFound
					case "missing manifest":
						files.ManifestFile = ""
					}
				}
				return files, nil
			}).Build().UnPatch()
			defer mockey.Mock(completeSegmentCopy).To(func(_ context.Context, _ *indexpb.StorageConfig, source *datapb.CopySegmentSource, target *datapb.CopySegmentTarget, _ *segmentCopyPlan) (*datapb.CopySegmentResult, []string, error) {
				commits.Add(1)
				if source.SegmentId == 444 {
					invalidCommits.Add(1)
				}
				return &datapb.CopySegmentResult{SegmentId: target.SegmentId}, nil, nil
			}).Build().UnPatch()
			targetStore := newCopySegmentStoreMock(t, func(_ context.Context, _, source, _, _ string) error {
				copies.Add(1)
				if !strings.Contains(source, "/333/") {
					invalidCopies.Add(1)
				}
				return nil
			})
			// A nil source manager also forbids falling back to directory listing.
			manager := NewTaskManager()
			task := NewCopySegmentTask(ctx, make(chan struct{}, 2), req, manager, nil, nil, nil, targetStore, "source", "target")
			manager.Add(task)
			err := conc.BlockOnAll(task.Execute()...)
			if kind == "valid" || kind == "valid lob" || kind == "bucket root" {
				require.NoError(t, err)
				require.EqualValues(t, 2, reads.Load(), "one native read per segment")
				expected := int64(4)
				if kind == "valid lob" {
					expected++
				}
				require.Equal(t, expected, copies.Load())
				require.EqualValues(t, 2, commits.Load())
			} else {
				require.Error(t, err)
				require.Zero(t, invalidCopies.Load())
				require.Zero(t, invalidCommits.Load())
				latest := manager.Get(1).(*CopySegmentTask)
				require.Equal(t, datapb.ImportTaskStateV2_Failed, latest.GetState())
				require.Len(t, latest.copiedFiles, int(copies.Load()), "retain every confirmed peer write for cleanup")
			}
		})
	}
}

func TestPrepareSegmentCopy_ExternalCollectionValidation(t *testing.T) {
	for _, kind := range []string{"valid", "outside lob", "missing objects", "empty segment"} {
		t.Run(kind, func(t *testing.T) {
			source := &datapb.CopySegmentSource{
				CollectionId: 100, PartitionId: 10, SegmentId: 1,
				StorageVersion: storage.StorageV3, IsExternalCollection: true, NumOfRows: 1,
				SourceRootPath: "s3://source/bundle/files", ManifestPath: packed.MarshalManifestPath("bundle/files/insert_log/100/10/1", 1),
			}
			target := &datapb.CopySegmentTarget{CollectionId: 200, PartitionId: 20, SegmentId: 2, TargetRootPath: "files"}
			manifest, err := packed.ManifestFilePath(source.ManifestPath)
			require.NoError(t, err)
			paths := []string{manifest}
			lobs := []packed.LobFileInfo{{Path: "bundle/files/insert_log/100/10/lobs/1/_data/a"}}
			if kind == "outside lob" {
				lobs[0].Path = "outside/insert_log/100/10/lobs/1/_data/a"
			}
			if kind == "missing objects" || kind == "empty segment" {
				paths = nil
				lobs = nil
			}
			if kind == "empty segment" {
				source.NumOfRows = 0
			}
			inventory := manifestInventoryForTest(paths, nil)
			inventory.LobFiles = lobs
			defer mockey.Mock(snapshotstorage.ReadNativeManifestFiles).Return(inventory, nil).Build().UnPatch()

			plan, err := prepareSegmentCopy(context.Background(), nil, nil, source, target)
			switch kind {
			case "valid":
				require.NoError(t, err)
				require.Len(t, plan.mappings, 2)
			case "empty segment":
				require.NoError(t, err)
				require.Empty(t, plan.mappings)
			default:
				require.ErrorIs(t, err, merr.ErrDataIntegrity)
				require.Empty(t, plan)
			}
		})
	}
}

// A prepared inventory is authoritative even when the Avro marker is missing
// or stale; only IDs are carried forward, never the selected artifact subset.
func TestRepublishCopiedManifestIndexes_PreparedInventory(t *testing.T) {
	for _, tc := range []struct {
		name       string
		ids        []int64
		knownEmpty bool
		commitErr  error
	}{
		{name: "empty inventory", ids: []int64{}},
		{name: "legacy known empty", knownEmpty: true},
		{name: "inherited entries", ids: []int64{10, 20}},
		{name: "inventory overrides stale marker", ids: []int64{10}, knownEmpty: true},
		{name: "commit failure", ids: []int64{10}, commitErr: merr.ErrIoTooManyRequests},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer mockey.Mock(packed.GetManifestIndexInfos).To(func(string, *indexpb.StorageConfig) ([]packed.ManifestIndexInfo, error) {
				t.Error("prepared inventory must avoid target enumeration")
				return nil, merr.ErrServiceInternal
			}).Build().UnPatch()
			commits := 0
			defer mockey.Mock(packed.ReplaceCopiedManifestIndexes).To(func(_ string, _ int64, _ *indexpb.StorageConfig, ids []int64, indexes []packed.ManifestIndexInfo) (string, error) {
				commits++
				require.Equal(t, tc.ids, ids)
				require.Empty(t, indexes)
				return "new-revision", tc.commitErr
			}).Build().UnPatch()
			manifest := packed.MarshalManifestPath("files/insert_log/100/200/300", 1)
			got, builds, err := republishCopiedManifestIndexes(context.Background(), manifest,
				&datapb.CopySegmentTarget{}, 0, nil, nil, tc.knownEmpty, tc.ids)
			require.Empty(t, builds)
			if tc.commitErr != nil {
				require.ErrorIs(t, err, tc.commitErr)
			} else {
				require.NoError(t, err)
			}
			if len(tc.ids) == 0 {
				require.Zero(t, commits)
				require.Equal(t, manifest, got)
			} else {
				require.Equal(t, 1, commits)
			}
		})
	}
}

func TestRepublishCopiedManifestIndexes_AcknowledgesCommittedBuilds(t *testing.T) {
	for _, failCommit := range []bool{false, true} {
		t.Run(fmt.Sprintf("commit-fails-%t", failCommit), func(t *testing.T) {
			manifest := packed.MarshalManifestPath("files/insert_log/100/200/300", 3)
			target := &datapb.CopySegmentTarget{TargetIndexes: []*datapb.CopySegmentTargetIndex{{
				IndexName: "vec_idx", IndexId: 777, FieldId: 101, ColumnName: "vector", IndexType: "HNSW",
			}}}
			infos := map[int64]*datapb.VectorScalarIndexInfo{888: {
				IndexId: 555, BuildId: 888, FieldId: 101, IndexName: "vec_idx",
				IndexFilePaths:        []string{"files/insert_log/100/200/300/_index/888/0/index.bin"},
				IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED,
			}}
			defer mockey.Mock(packed.ReplaceCopiedManifestIndexes).To(func(_ string, _ int64, _ *indexpb.StorageConfig, _ []int64, entries []packed.ManifestIndexInfo) (string, error) {
				require.Len(t, entries, 1)
				require.EqualValues(t, 777, entries[0].IndexID)
				if failCommit {
					return "", merr.ErrServiceUnavailable
				}
				return packed.MarshalManifestPath("files/insert_log/100/200/300", 4), nil
			}).Build().UnPatch()
			_, builds, err := republishCopiedManifestIndexes(context.Background(), manifest, target, 100, nil, infos, true, []int64{})
			if failCommit {
				require.ErrorIs(t, err, merr.ErrServiceUnavailable)
				require.Empty(t, builds)
			} else {
				require.NoError(t, err)
				require.Equal(t, []int64{888}, builds)
			}
			require.EqualValues(t, 555, infos[888].GetIndexId(), "source identity retains its existing RPC meaning")
		})
	}
}

// CopySegmentAndIndexFiles adapts the real Restore stages for single-segment fixtures.
func CopySegmentAndIndexFiles(
	ctx context.Context,
	permits chan struct{},
	sourceCM storage.ChunkManager,
	sourceStorageConfig *indexpb.StorageConfig,
	targetStorageConfig *indexpb.StorageConfig,
	targetStore objectstore.ObjectStore,
	sourceBucket string,
	targetBucket string,
	source *datapb.CopySegmentSource,
	target *datapb.CopySegmentTarget,
	logFields []mlog.Field,
) (*datapb.CopySegmentResult, []string, error) {
	if targetStore == nil {
		return nil, nil, merr.WrapErrServiceInternalMsg("cross-bucket targetStore is nil")
	}
	plan, err := prepareSegmentCopy(ctx, sourceCM, sourceStorageConfig, source, target)
	if err != nil {
		return nil, nil, err
	}
	items, err := segmentCopyFiles(plan)
	if err != nil {
		return nil, nil, err
	}
	copied, err := copySegmentFiles(ctx, permits, items, targetStore, sourceBucket, targetBucket, logFields)
	if err != nil {
		return nil, copied, err
	}
	result, created, err := completeSegmentCopy(ctx, targetStorageConfig, source, target, plan)
	return result, append(copied, created...), err
}

func TestCollectSegmentFilesExternalReferences(t *testing.T) {
	for _, mode := range []string{"external", "ordinary", "invalid-uri", "missing-host", "credentials", "foreign-index", "foreign-manifest", "wrong-native-root", "invalid-owned-path"} {
		t.Run(mode, func(t *testing.T) {
			base := "files/insert_log/1/2/3"
			native := &snapshotformat.NativeManifestFiles{
				ManifestFile: "s3://source/" + base + "/_metadata/manifest-1.avro",
				ColumnFiles:  []string{"s3://source/" + base + "/_data/a", base + "/_data/a", "s3://lake/table/data"},
				DeltaFiles:   []string{"s3://lake/table/delete", base + "/_delta/d"},
				StatsFiles:   []string{"s3://lake/table/stats", base + "/_stats/s"},
				LobFiles:     []packed.LobFileInfo{{Path: "s3://lake/lob"}, {Path: "files/insert_log/1/2/lobs/100/_data/l"}},
			}
			source := &datapb.CopySegmentSource{
				StorageVersion: storage.StorageV3, SegmentId: 3, NumOfRows: 1,
				IsExternalCollection: mode != "ordinary", SourceRootPath: "s3://source/files",
				ManifestPath: packed.MarshalManifestPath(base, 1),
				IndexFiles:   []*indexpb.IndexFilePathInfo{{IndexFilePaths: []string{"s3://source/files/index_files/i"}}},
			}
			switch mode {
			case "invalid-uri":
				native.ColumnFiles[0] = "s3://%zz/path"
			case "missing-host":
				native.ColumnFiles[0] = "s3:///path"
			case "credentials":
				native.ColumnFiles[0] = "s3://user:password@lake/path"
			case "foreign-index":
				source.IndexFiles[0].IndexFilePaths[0] = "s3://lake/files/index_files/i"
			case "foreign-manifest":
				native.ManifestFile = "s3://lake/" + base + "/_metadata/manifest-1.avro"
			case "wrong-native-root":
				native.StatsFiles[1] = "outside/stats"
			case "invalid-owned-path":
				source.SourceRootPath = ""
				native.StatsFiles[1] = "files/../escape"
			}
			before := append([]string(nil), native.ColumnFiles...)
			calls := 0
			defer mockey.Mock(snapshotstorage.ReadNativeManifestFiles).To(func(string, *indexpb.StorageConfig) (*snapshotformat.NativeManifestFiles, error) {
				calls++
				return native, nil
			}).Build().UnPatch()
			files, err := collectSegmentFiles(context.Background(), nil, &indexpb.StorageConfig{BucketName: "source"}, source)
			require.Equal(t, 1, calls)
			require.Equal(t, before, native.ColumnFiles)
			if mode != "external" {
				require.Error(t, err)
				require.Nil(t, files)
				return
			}
			require.NoError(t, err)
			require.ElementsMatch(t, []string{native.ManifestFile, base + "/_data/a", base + "/_delta/d", base + "/_stats/s"}, files.InsertBinlogs)
			require.Equal(t, []string{"files/insert_log/1/2/lobs/100/_data/l"}, files.LobFiles)
			require.Equal(t, []string{"s3://source/files/index_files/i"}, files.VectorScalarIndex)
			require.NotNil(t, files.ManifestIndexIDs)
		})
	}
}

func TestCopySegmentExternalPinnedManifest(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	root := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(root))
	cfg := &indexpb.StorageConfig{StorageType: "local", RootPath: root}
	base := path.Join(root, "insert_log/100/10/1")
	owned := path.Join(base, "_stats/text_index.100/data")
	orphan := path.Join(base, "_stats/unreferenced")
	require.NoError(t, cm.Write(ctx, owned, []byte("owned-stats")))
	require.NoError(t, cm.Write(ctx, orphan, []byte("not-in-snapshot")))
	externalData, externalDelta, externalStats := "s3://lake/table/data", "s3://lake/table/delete", "s3://lake/table/bloom"
	manifest, err := packed.CommitManifestUpdates(packed.MarshalManifestPath(base, 0), cfg, &packed.ManifestUpdates{
		ColumnGroups: []packed.ColumnGroupEntry{{Columns: []string{"100"}, Format: "parquet", Files: []packed.ColumnGroupFileEntry{{Path: externalData, EndIndex: 1}}}},
		DeltaLogs:    []packed.DeltaLogEntry{{Path: externalDelta, NumEntries: 1}},
		Stats:        []packed.StatEntry{{Key: "text_index.100", Files: []string{owned}}, {Key: "pk.100", Files: []string{externalStats}}},
	})
	require.NoError(t, err)
	original, err := snapshotstorage.ReadNativeManifestFiles(manifest, cfg)
	require.NoError(t, err)
	originalBytes, err := cm.Read(ctx, original.ManifestFile)
	require.NoError(t, err)
	_, revision, err := packed.UnmarshalManifestPath(manifest)
	require.NoError(t, err)
	_, err = packed.CommitManifestUpdates(packed.MarshalManifestPath(base, revision), cfg, &packed.ManifestUpdates{Stats: []packed.StatEntry{{Key: "text_index.100", Files: []string{orphan}}}})
	require.NoError(t, err)
	defer mockey.Mock((*storage.LocalChunkManager).WalkWithPrefix).Return(merr.ErrServiceInternal).Build().UnPatch()
	targetStore := newCopySegmentStoreMock(t, func(ctx context.Context, _, src, _, dst string) error {
		data, err := cm.Read(ctx, src)
		if err != nil {
			return err
		}
		return cm.Write(ctx, dst, data)
	})
	source := &datapb.CopySegmentSource{
		CollectionId: 100, PartitionId: 10, SegmentId: 1,
		StorageVersion: storage.StorageV3, ManifestPath: manifest, IsExternalCollection: true, NumOfRows: 1,
		InsertBinlogs: []*datapb.FieldBinlog{{FieldID: 100, Binlogs: []*datapb.Binlog{{EntriesNum: 1}}}},
	}
	target := &datapb.CopySegmentTarget{CollectionId: 200, PartitionId: 20, SegmentId: 2, TargetRootPath: root}
	result, copied, err := CopySegmentAndIndexFiles(ctx, make(chan struct{}, 2), cm, cfg, cfg, targetStore, "", "", source, target, nil)
	require.NoError(t, err)
	restored, err := snapshotstorage.ReadNativeManifestFiles(result.ManifestPath, cfg)
	require.NoError(t, err)
	targetBytes, err := cm.Read(ctx, restored.ManifestFile)
	require.NoError(t, err)
	require.Equal(t, originalBytes, targetBytes)
	require.Equal(t, []string{externalData}, restored.ColumnFiles)
	require.Equal(t, []string{externalDelta}, restored.DeltaFiles)
	targetStat := path.Join(root, "insert_log/200/20/2/_stats/text_index.100/data")
	require.ElementsMatch(t, []string{externalStats, targetStat}, restored.StatsFiles)
	require.ElementsMatch(t, []string{restored.ManifestFile, targetStat}, copied)
	content, err := cm.Read(ctx, targetStat)
	require.NoError(t, err)
	require.Equal(t, "owned-stats", string(content))
}
