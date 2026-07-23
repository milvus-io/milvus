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
	"fmt"
	"net/url"
	"path"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	snapshotstorage "github.com/milvus-io/milvus/internal/snapshotio/storage"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

func clearSegmentNonInsertFiles(segment *datapb.SegmentDescription) {
	segment.Statslogs = nil
	segment.Deltalogs = nil
	segment.Bm25Statslogs = nil
	segment.IndexFiles = nil
	segment.TextIndexFiles = nil
	segment.JsonKeyIndexFiles = nil
}

func TestSnapshotExporter_ExportCopiesFilesAndWritesSelfContainedMetadata(t *testing.T) {
	tempDir := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	ctx := context.Background()

	sourceBinlog := path.Join(tempDir, "files/insert_log/1/2/1001/field1/1")
	sourceIndex := path.Join(tempDir, "files/index_files/1001/2001/3001/index")
	assert.NoError(t, cm.Write(ctx, sourceBinlog, []byte("binlog")))
	assert.NoError(t, cm.Write(ctx, sourceIndex, []byte("index")))

	snapshotData := createTestSnapshotDataForMeta()
	snapshotData.SnapshotInfo.S3Location = "s3://source/snapshots/100/metadata/1.json"
	segment := snapshotData.Segments[0]
	segment.Binlogs = []*datapb.FieldBinlog{{
		FieldID: 1,
		Binlogs: []*datapb.Binlog{{
			LogID:   1,
			LogPath: sourceBinlog,
		}},
	}}
	segment.Statslogs = nil
	segment.Deltalogs = nil
	segment.Bm25Statslogs = nil
	segment.TextIndexFiles = nil
	segment.JsonKeyIndexFiles = nil
	segment.IndexFiles = []*indexpb.IndexFilePathInfo{{
		SegmentID:           segment.GetSegmentId(),
		FieldID:             2,
		IndexID:             2001,
		BuildID:             3001,
		IndexFilePaths:      []string{sourceIndex},
		SerializedSize:      uint64(len("index")),
		IndexVersion:        1,
		NumRows:             segment.GetNumOfRows(),
		IndexName:           "test_index",
		CurrentIndexVersion: 1,
	}}
	snapshotData.SegmentIDs = []int64{segment.GetSegmentId()}
	snapshotData.BuildIDs = []int64{3001}

	targetRoot := path.Join(tempDir, "exported")
	copier := newSnapshotExporterCopierMock(t, func(ctx context.Context, _, srcObject, _, dstObject string) error {
		data, err := cm.Read(ctx, srcObject)
		if err != nil {
			return err
		}
		return cm.Write(ctx, dstObject, data)
	})
	metadataURI, err := exportSnapshot(ctx, cm, cm, copier, "", "", snapshotData, targetRoot)
	assert.NoError(t, err)
	assert.Equal(t, path.Join(targetRoot, snapshotstorage.SnapshotRootPath, "100", snapshotstorage.SnapshotMetadataSubPath, "1.json"), metadataURI)

	copiedBinlog := path.Join(targetRoot, snapshotstorage.ExportedSnapshotFilesPath, "files/insert_log/1/2/1001/field1/1")
	copiedIndex := path.Join(targetRoot, snapshotstorage.ExportedSnapshotFilesPath, "files/index_files/1001/2001/3001/index")
	binlogData, err := cm.Read(ctx, copiedBinlog)
	assert.NoError(t, err)
	assert.Equal(t, []byte("binlog"), binlogData)
	indexData, err := cm.Read(ctx, copiedIndex)
	assert.NoError(t, err)
	assert.Equal(t, []byte("index"), indexData)

	readSnapshot, err := snapshotstorage.NewSnapshotReader(cm).ReadSnapshot(ctx, metadataURI, true)
	assert.NoError(t, err)
	assert.Equal(t, datapb.SnapshotLayout_SnapshotLayoutSelfContained, readSnapshot.Layout)
	assert.Equal(t, metadataURI, readSnapshot.SnapshotInfo.GetS3Location())
	assert.Equal(t, copiedBinlog, readSnapshot.Segments[0].GetBinlogs()[0].GetBinlogs()[0].GetLogPath())
	assert.Equal(t, copiedIndex, readSnapshot.Segments[0].GetIndexFiles()[0].GetIndexFilePaths()[0])
}

type snapshotExporterCopierTarget struct {
	storage.CrossBucketCopier
}

type copyCall struct {
	srcBucket string
	src       string
	dstBucket string
	dst       string
}

func newSnapshotExporterCopierMock(
	t *testing.T,
	copyFn func(context.Context, string, string, string, string) error,
) storage.CrossBucketCopier {
	t.Helper()
	target := &snapshotExporterCopierTarget{}
	mockCopy := mockey.Mock((*snapshotExporterCopierTarget).CopyCrossBucket).To(copyFn).Build()
	t.Cleanup(func() { mockCopy.UnPatch() })
	return target
}

func TestSnapshotExporter_CopiesFilesWithBoundedConcurrency(t *testing.T) {
	const configuredConcurrency = 2
	key := Params.DataCoordCfg.SnapshotExportCopyConcurrency.Key
	Params.Save(key, fmt.Sprint(configuredConcurrency))
	defer Params.Reset(key)

	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	snapshotData := createTestSnapshotDataForMeta()
	segment := snapshotData.Segments[0]
	binlogs := make([]*datapb.Binlog, configuredConcurrency+1)
	for i := range binlogs {
		binlogs[i] = &datapb.Binlog{
			LogID:   int64(i + 1),
			LogPath: fmt.Sprintf("files/insert_log/100/1/1001/%d", i+1),
		}
	}
	segment.Binlogs = []*datapb.FieldBinlog{{
		FieldID: 1,
		Binlogs: binlogs,
	}}
	segment.Statslogs = nil
	segment.Deltalogs = nil
	segment.Bm25Statslogs = nil
	segment.IndexFiles = nil
	segment.TextIndexFiles = nil
	segment.JsonKeyIndexFiles = nil

	release := make(chan struct{})
	started := make(chan struct{}, configuredConcurrency+1)
	copier := newSnapshotExporterCopierMock(t, func(ctx context.Context, _, _, _, _ string) error {
		started <- struct{}{}
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	result := make(chan error, 1)
	targetRoot := path.Join(t.TempDir(), "export-root")
	go func() {
		_, err := exportSnapshot(
			context.Background(),
			cm,
			cm,
			copier,
			"source-bucket",
			"target-bucket",
			snapshotData,
			targetRoot,
		)
		result <- err
	}()

	for range configuredConcurrency {
		select {
		case <-started:
		case <-time.After(time.Second):
			close(release)
			require.FailNow(t, "snapshot file copies did not run concurrently")
		}
	}
	select {
	case <-started:
		close(release)
		require.FailNow(t, "snapshot file copy concurrency exceeded its limit")
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	require.NoError(t, <-result)
}

func TestSnapshotExporter_CopyFailureDoesNotWriteMetadata(t *testing.T) {
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	snapshotData := createTestSnapshotDataForMeta()
	segment := snapshotData.Segments[0]
	segment.Binlogs = []*datapb.FieldBinlog{{
		FieldID: 1,
		Binlogs: []*datapb.Binlog{{
			LogID:   1,
			LogPath: "files/insert_log/100/1/1001/1",
		}},
	}}
	segment.Statslogs = nil
	segment.Deltalogs = nil
	segment.Bm25Statslogs = nil
	segment.IndexFiles = nil
	segment.TextIndexFiles = nil
	segment.JsonKeyIndexFiles = nil

	targetRoot := path.Join(t.TempDir(), "export-root")
	metadataPath := path.Join(
		targetRoot,
		snapshotstorage.SnapshotRootPath,
		"100",
		snapshotstorage.SnapshotMetadataSubPath,
		"1.json",
	)
	_, err := exportSnapshot(
		context.Background(),
		cm,
		cm,
		newSnapshotExporterCopierMock(t, func(context.Context, string, string, string, string) error {
			return errors.New("copy permission denied")
		}),
		"source-bucket",
		"target-bucket",
		snapshotData,
		targetRoot,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "copy permission denied")
	exists, existErr := cm.Exist(context.Background(), metadataPath)
	require.NoError(t, existErr)
	assert.False(t, exists)
}

func TestSnapshotExporter_CopyFailureLeavesObjectsUnpublished(t *testing.T) {
	key := Params.DataCoordCfg.SnapshotExportCopyConcurrency.Key
	Params.Save(key, "1")
	defer Params.Reset(key)

	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	snapshotData := createTestSnapshotDataForMeta()
	segment := snapshotData.Segments[0]
	segment.Binlogs = []*datapb.FieldBinlog{{
		FieldID: 1,
		Binlogs: []*datapb.Binlog{
			{LogPath: "files/insert_log/100/1/1001/1"},
			{LogPath: "files/insert_log/100/1/1001/2"},
		},
	}}
	clearSegmentNonInsertFiles(segment)

	targetRoot := path.Join(t.TempDir(), "export-root")
	firstTarget := snapshotstorage.ExportedSnapshotPath(
		cm,
		"files/insert_log/100/1/1001/1",
		targetRoot,
	)
	_, metadataPath := snapshotstorage.GetSnapshotPaths(targetRoot, 100, 1)
	copyCount := 0
	_, err := exportSnapshot(
		context.Background(),
		cm,
		cm,
		newSnapshotExporterCopierMock(t, func(ctx context.Context, _, _, _, dst string) error {
			copyCount++
			if copyCount == 2 {
				return errors.New("copy failed")
			}
			return cm.Write(ctx, dst, []byte("copied"))
		}),
		"source-bucket",
		"target-bucket",
		snapshotData,
		targetRoot,
	)

	require.Error(t, err)
	exists, existErr := cm.Exist(context.Background(), firstTarget)
	require.NoError(t, existErr)
	assert.True(t, exists)
	metadataExists, metadataErr := cm.Exist(context.Background(), metadataPath)
	require.NoError(t, metadataErr)
	assert.False(t, metadataExists)
}

func TestSnapshotExporter_AllowsSameBucketWithoutObjectOverlap(t *testing.T) {
	baseRoot := t.TempDir()
	sourceRoot := path.Join(baseRoot, "source")
	targetRoot := path.Join(baseRoot, "backup")
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(sourceRoot))
	snapshotData := createTestSnapshotDataForMeta()
	snapshotData.Segments = nil
	snapshotData.SegmentIDs = nil
	snapshotData.Indexes = nil

	metadataURI, err := exportSnapshot(
		context.Background(),
		cm,
		cm,
		newSnapshotExporterCopierMock(t, func(context.Context, string, string, string, string) error {
			return nil
		}),
		"shared-bucket",
		"shared-bucket",
		snapshotData,
		targetRoot,
	)

	require.NoError(t, err)
	assert.Equal(t, path.Join(targetRoot, "snapshots/100/metadata/1.json"), metadataURI)
}

func TestSnapshotExporter_ObjectKeyWithURLDelimitersReturnsWrittenMetadataPath(t *testing.T) {
	for _, delimiter := range []string{"?version=1", "#archive"} {
		t.Run(delimiter, func(t *testing.T) {
			baseRoot := t.TempDir()
			sourceRoot := path.Join(baseRoot, "source")
			targetRoot := path.Join(baseRoot, "backup"+delimiter)
			cm := storage.NewLocalChunkManager(objectstorage.RootPath(sourceRoot))
			snapshotData := createTestSnapshotDataForMeta()
			snapshotData.Segments = nil
			snapshotData.SegmentIDs = nil
			snapshotData.Indexes = nil

			metadataPath, err := exportSnapshot(
				context.Background(),
				cm,
				cm,
				newSnapshotExporterCopierMock(t, func(context.Context, string, string, string, string) error {
					return nil
				}),
				"shared-bucket",
				"shared-bucket",
				snapshotData,
				targetRoot,
			)

			require.NoError(t, err)
			expectedPath := path.Join(targetRoot, "snapshots/100/metadata/1.json")
			assert.Equal(t, expectedPath, metadataPath)
			exists, err := cm.Exist(context.Background(), metadataPath)
			require.NoError(t, err)
			assert.True(t, exists)
		})
	}
}

func TestSnapshotExporter_RejectsMetadataOverlapForObjectKeyWithURLDelimiter(t *testing.T) {
	baseRoot := t.TempDir()
	sourceRoot := path.Join(baseRoot, "source")
	targetRoot := path.Join(baseRoot, "backup?version=1")
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(sourceRoot))
	snapshotData := createTestSnapshotDataForMeta()
	snapshotData.Segments = nil
	snapshotData.SegmentIDs = nil
	snapshotData.Indexes = nil
	snapshotData.MetadataPath = path.Join(targetRoot, "snapshots/100/metadata/1.json")

	_, err := exportSnapshot(
		context.Background(),
		cm,
		cm,
		newSnapshotExporterCopierMock(t, func(context.Context, string, string, string, string) error {
			return nil
		}),
		"shared-bucket",
		"shared-bucket",
		snapshotData,
		targetRoot,
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "overlaps source snapshot object")
}

func TestSnapshotExporter_RejectsSameBucketObjectOverlap(t *testing.T) {
	baseRoot := t.TempDir()
	tests := []struct {
		name          string
		sourceRoot    string
		targetRoot    string
		prepareSource func(*snapshotstorage.SnapshotData, string)
	}{
		{
			name:       "target metadata overwrites source metadata",
			sourceRoot: path.Join(baseRoot, "source"),
			targetRoot: path.Join(baseRoot, "backup"),
			prepareSource: func(snapshot *snapshotstorage.SnapshotData, targetRoot string) {
				snapshot.MetadataPath = path.Join(targetRoot, "snapshots/100/metadata/1.json")
			},
		},
		{
			name:       "target segment manifest overwrites source manifest",
			sourceRoot: path.Join(baseRoot, "source"),
			targetRoot: path.Join(baseRoot, "backup"),
			prepareSource: func(snapshot *snapshotstorage.SnapshotData, targetRoot string) {
				snapshot.ManifestPaths = []string{
					path.Join(targetRoot, "snapshots/100/manifests/1/1001.avro"),
				}
			},
		},
		{
			name:       "target data reuses source object",
			sourceRoot: path.Join(baseRoot, "backup", snapshotstorage.ExportedSnapshotFilesPath),
			targetRoot: path.Join(baseRoot, "backup"),
			prepareSource: func(snapshot *snapshotstorage.SnapshotData, _ string) {
				segment := snapshot.Segments[0]
				segment.Binlogs = []*datapb.FieldBinlog{{
					FieldID: 1,
					Binlogs: []*datapb.Binlog{{
						LogPath: path.Join(baseRoot, "backup", "files", "insert_log/100/1/1001/1"),
					}},
				}}
				clearSegmentNonInsertFiles(segment)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cm := storage.NewLocalChunkManager(objectstorage.RootPath(tt.sourceRoot))
			snapshotData := createTestSnapshotDataForMeta()
			snapshotData.Indexes = nil
			tt.prepareSource(snapshotData, tt.targetRoot)

			copyCalled := false
			_, err := exportSnapshot(
				context.Background(),
				cm,
				cm,
				newSnapshotExporterCopierMock(t, func(context.Context, string, string, string, string) error {
					copyCalled = true
					return nil
				}),
				"shared-bucket",
				"shared-bucket",
				snapshotData,
				tt.targetRoot,
			)

			require.Error(t, err)
			assert.Contains(t, err.Error(), "overlaps source snapshot object")
			assert.False(t, copyCalled)
		})
	}
}

func TestSnapshotExporter_CrossBucketCopiesMatchingObjectKey(t *testing.T) {
	baseRoot := t.TempDir()
	sourceRoot := path.Join(baseRoot, "backup", snapshotstorage.ExportedSnapshotFilesPath)
	targetRoot := path.Join(baseRoot, "backup")
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(sourceRoot))
	snapshotData := createTestSnapshotDataForMeta()
	segment := snapshotData.Segments[0]
	sourcePath := path.Join(sourceRoot, "insert_log/100/1/1001/1")
	segment.Binlogs = []*datapb.FieldBinlog{{
		FieldID: 1,
		Binlogs: []*datapb.Binlog{{LogPath: sourcePath}},
	}}
	clearSegmentNonInsertFiles(segment)

	copyCalled := false
	_, err := exportSnapshot(
		context.Background(),
		cm,
		cm,
		newSnapshotExporterCopierMock(t, func(_ context.Context, srcBucket, src, dstBucket, dst string) error {
			copyCalled = true
			assert.Equal(t, "source-bucket", srcBucket)
			assert.Equal(t, "target-bucket", dstBucket)
			assert.Equal(t, sourcePath, src)
			assert.Equal(t, sourcePath, dst)
			return nil
		}),
		"source-bucket",
		"target-bucket",
		snapshotData,
		targetRoot,
	)

	require.NoError(t, err)
	assert.True(t, copyCalled)
}

func TestSnapshotExporter_FailedExportKeepsExistingTargetObject(t *testing.T) {
	ctx := context.Background()
	sourceRoot := t.TempDir()
	targetRoot := t.TempDir()
	sourceCM := storage.NewLocalChunkManager(objectstorage.RootPath(sourceRoot))
	targetCM := storage.NewLocalChunkManager(objectstorage.RootPath(targetRoot))

	stableSource := path.Join(sourceRoot, "files/insert_log/100/1/1001/1")
	failingSource := path.Join(sourceRoot, "files/insert_log/100/1/1001/2")
	require.NoError(t, sourceCM.Write(ctx, stableSource, []byte("stable")))
	require.NoError(t, sourceCM.Write(ctx, failingSource, []byte("fail")))

	snapshotData := createTestSnapshotDataForMeta()
	segment := snapshotData.Segments[0]
	segment.Binlogs = []*datapb.FieldBinlog{{
		FieldID: 1,
		Binlogs: []*datapb.Binlog{
			{LogPath: stableSource},
			{LogPath: failingSource},
		},
	}}
	clearSegmentNonInsertFiles(segment)

	existingTarget := snapshotstorage.ExportedSnapshotPath(sourceCM, stableSource, targetRoot)
	require.NoError(t, targetCM.Write(ctx, existingTarget, []byte("stable")))
	stableCopied := make(chan struct{})
	copier := newSnapshotExporterCopierMock(t, func(ctx context.Context, _, src, _, dst string) error {
		if src == failingSource {
			<-stableCopied
			return errors.New("copy failed")
		}
		data, err := sourceCM.Read(ctx, src)
		if err != nil {
			return err
		}
		if err := targetCM.Write(ctx, dst, data); err != nil {
			return err
		}
		close(stableCopied)
		return nil
	})

	_, err := exportSnapshot(
		ctx,
		sourceCM,
		targetCM,
		copier,
		"source-bucket",
		"target-bucket",
		snapshotData,
		targetRoot,
	)
	require.Error(t, err)

	data, readErr := targetCM.Read(ctx, existingTarget)
	require.NoError(t, readErr)
	assert.Equal(t, []byte("stable"), data)
}

func TestSnapshotExporter_ExportCrossBucketUsesTargetManagerAndCopier(t *testing.T) {
	sourceRoot := t.TempDir()
	targetRoot := t.TempDir()
	t.Chdir(targetRoot)
	sourceCM := storage.NewLocalChunkManager(objectstorage.RootPath(sourceRoot))
	targetCM := storage.NewLocalChunkManager(objectstorage.RootPath(targetRoot))
	var copyCalls []copyCall
	copier := newSnapshotExporterCopierMock(t, func(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject string) error {
		copyCalls = append(copyCalls, copyCall{
			srcBucket: srcBucket,
			src:       srcObject,
			dstBucket: dstBucket,
			dst:       dstObject,
		})
		data, err := sourceCM.Read(ctx, srcObject)
		if err != nil {
			return err
		}
		return targetCM.Write(ctx, dstObject, data)
	})

	ctx := context.Background()
	sourceBinlog := path.Join(sourceRoot, "files/insert_log/100/1/10/1")
	require.NoError(t, sourceCM.Write(ctx, sourceBinlog, []byte("data")))

	snapshotData := createTestSnapshotDataForMeta()
	snapshotData.SnapshotInfo.S3Location = "s3://local-bucket/snapshots/100/metadata/1.json"
	segment := snapshotData.Segments[0]
	segment.Binlogs = []*datapb.FieldBinlog{{
		FieldID: 1,
		Binlogs: []*datapb.Binlog{{
			LogID:   1,
			LogPath: sourceBinlog,
		}},
	}}
	segment.Statslogs = nil
	segment.Deltalogs = nil
	segment.Bm25Statslogs = nil
	segment.IndexFiles = nil
	segment.TextIndexFiles = nil
	segment.JsonKeyIndexFiles = nil
	snapshotData.SegmentIDs = []int64{segment.GetSegmentId()}
	snapshotData.BuildIDs = nil

	targetURI := "s3://foreign-bucket/export-root"
	expectedMetadataURI, err := url.JoinPath(targetURI, snapshotstorage.SnapshotRootPath, "100", snapshotstorage.SnapshotMetadataSubPath, "1.json")
	require.NoError(t, err)
	expectedCopiedBinlog := path.Join("export-root", snapshotstorage.ExportedSnapshotFilesPath, "files/insert_log/100/1/10/1")

	metadataURI, err := exportSnapshot(ctx, sourceCM, targetCM, copier, "local-bucket", "foreign-bucket", snapshotData, targetURI)
	require.NoError(t, err)
	assert.Equal(t, expectedMetadataURI, metadataURI)
	assert.Equal(t, []copyCall{{
		srcBucket: "local-bucket",
		src:       sourceBinlog,
		dstBucket: "foreign-bucket",
		dst:       expectedCopiedBinlog,
	}}, copyCalls)

	readSnapshot, err := snapshotstorage.NewSnapshotReader(targetCM).ReadSnapshot(ctx, metadataURI, true)
	require.NoError(t, err)
	assert.Equal(t, metadataURI, readSnapshot.SnapshotInfo.GetS3Location())
	assert.Equal(t, copyCalls[0].dst, readSnapshot.Segments[0].GetBinlogs()[0].GetBinlogs()[0].GetLogPath())
	copiedData, err := targetCM.Read(ctx, copyCalls[0].dst)
	require.NoError(t, err)
	assert.Equal(t, []byte("data"), copiedData)
}

func TestSnapshotExporter_ExportUsesSnapshotPrimitivesStrictly(t *testing.T) {
	tempDir := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	ctx := context.Background()

	sourceInsert := path.Join(tempDir, "files/insert_log/100/10/1001/1")
	sourceStats := path.Join(tempDir, "files/stats_log/100/10/1001/2")
	require.NoError(t, cm.Write(ctx, sourceInsert, []byte("insert")))
	require.NoError(t, cm.Write(ctx, sourceStats, []byte("stats")))

	snapshotData := createTestSnapshotDataForMeta()
	segment := snapshotData.Segments[0]
	segment.StorageVersion = 0
	segment.Binlogs = []*datapb.FieldBinlog{{
		FieldID: 10,
		Binlogs: []*datapb.Binlog{{
			LogID:   1,
			LogPath: sourceInsert,
		}},
	}}
	segment.Statslogs = []*datapb.FieldBinlog{{
		FieldID: 10,
		Binlogs: []*datapb.Binlog{{
			LogID:   2,
			LogPath: sourceStats,
		}},
	}}
	segment.Deltalogs = nil
	segment.Bm25Statslogs = nil
	segment.IndexFiles = nil
	segment.TextIndexFiles = nil
	segment.JsonKeyIndexFiles = nil

	targetPath := path.Join(tempDir, "exported")
	targetRoot := snapshotstorage.NormalizeSnapshotObjectPath(targetPath)
	metadataURI, err := url.JoinPath(targetPath,
		snapshotstorage.SnapshotRootPath,
		fmt.Sprintf("%d", snapshotData.SnapshotInfo.GetCollectionId()),
		snapshotstorage.SnapshotMetadataSubPath,
		fmt.Sprintf("%d.json", snapshotData.SnapshotInfo.GetId()))
	require.NoError(t, err)

	copier := newSnapshotExporterCopierMock(t, func(ctx context.Context, _, srcObject, _, dstObject string) error {
		data, err := cm.Read(ctx, srcObject)
		if err != nil {
			return err
		}
		return cm.Write(ctx, dstObject, data)
	})
	gotMetadataURI, err := exportSnapshot(ctx, cm, cm, copier, "", "", snapshotData, targetPath)
	require.NoError(t, err)
	assert.Equal(t, metadataURI, gotMetadataURI)

	readSnapshot, err := snapshotstorage.NewSnapshotReader(cm).ReadSnapshot(ctx, gotMetadataURI, true)
	require.NoError(t, err)
	require.Len(t, readSnapshot.Segments, 1)

	rewrittenInsert := readSnapshot.Segments[0].GetBinlogs()[0].GetBinlogs()[0].GetLogPath()
	rewrittenStats := readSnapshot.Segments[0].GetStatslogs()[0].GetBinlogs()[0].GetLogPath()
	assert.Equal(t, snapshotstorage.ExportedSnapshotPath(cm, sourceInsert, targetRoot), rewrittenInsert)
	assert.Equal(t, snapshotstorage.ExportedSnapshotPath(cm, sourceStats, targetRoot), rewrittenStats)

	insertData, err := cm.Read(ctx, rewrittenInsert)
	require.NoError(t, err)
	assert.Equal(t, []byte("insert"), insertData)
	statsData, err := cm.Read(ctx, rewrittenStats)
	require.NoError(t, err)
	assert.Equal(t, []byte("stats"), statsData)
}

func TestSnapshotExporter_ExportReturnsManifestLobError(t *testing.T) {
	tempDir := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	ctx := context.Background()

	basePath := path.Join(tempDir, "files/insert_log/1/2/1001")
	assert.NoError(t, cm.Write(ctx, path.Join(basePath, "manifest"), []byte("manifest")))

	snapshotData := createTestSnapshotDataForMeta()
	segment := snapshotData.Segments[0]
	segment.StorageVersion = storage.StorageV3
	segment.ManifestPath = packed.MarshalManifestPath(basePath, 1)

	mockGetLobFiles := mockey.Mock(packed.GetManifestLobFiles).Return(nil, errors.New("lob unavailable")).Build()
	defer mockGetLobFiles.UnPatch()

	copier := newSnapshotExporterCopierMock(t, func(ctx context.Context, _, srcObject, _, dstObject string) error {
		data, err := cm.Read(ctx, srcObject)
		if err != nil {
			return err
		}
		return cm.Write(ctx, dstObject, data)
	})
	_, err := exportSnapshot(ctx, cm, cm, copier, "", "", snapshotData, path.Join(tempDir, "exported"))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to list LOB files for segment 1001")
	assert.Contains(t, err.Error(), "lob unavailable")
}
