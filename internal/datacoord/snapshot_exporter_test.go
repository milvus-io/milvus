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
	"os"
	"path"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	snapshotstorage "github.com/milvus-io/milvus/internal/snapshotio/storage"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

type snapshotExporterCopierTarget struct{ storage.SizedCrossBucketCopier }

func clearSegmentNonInsertFiles(segment *datapb.SegmentDescription) {
	segment.Statslogs = nil
	segment.Deltalogs = nil
	segment.Bm25Statslogs = nil
	segment.IndexFiles = nil
	segment.TextIndexFiles = nil
	segment.JsonKeyIndexFiles = nil
}

func snapshotExportStorageConfig(bucket string) *indexpb.StorageConfig {
	cfg := &indexpb.StorageConfig{BucketName: bucket}
	if bucket != "" {
		cfg.Address = "s3.us-west-2.amazonaws.com"
		cfg.CloudProvider = objectstorage.CloudProviderAWS
		cfg.Region = "us-west-2"
		cfg.UseSSL = true
	}
	return cfg
}

func TestSnapshotExporter_L0CommitTimestamps(t *testing.T) {
	for _, commit := range []uint64{0, 300} {
		t.Run(fmt.Sprint(commit), func(t *testing.T) {
			ctx := context.Background()
			cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
			snapshot := createTestSnapshotDataForMeta()
			data := snapshot.Segments[0]
			clearSegmentNonInsertFiles(data)
			data.SegmentLevel, data.CommitTimestamp = datapb.SegmentLevel_L1, commit
			insertPath, deletePath := path.Join(cm.RootPath(), "files/insert"), path.Join(cm.RootPath(), "files/delete")
			require.NoError(t, cm.Write(ctx, insertPath, []byte("insert")))
			require.NoError(t, cm.Write(ctx, deletePath, []byte("delete")))
			data.Binlogs = []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{LogPath: insertPath}}}}
			snapshot.Segments = append(snapshot.Segments, &datapb.SegmentDescription{
				SegmentId: 1002, PartitionId: data.PartitionId, SegmentLevel: datapb.SegmentLevel_L0,
				Deltalogs: []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{LogPath: deletePath, EntriesNum: 1}}}},
			})
			snapshot.SegmentIDs = []int64{1001, 1002}
			copier := newSnapshotExporterCopierMock(t, func(ctx context.Context, _, src, _, dst string) error {
				bytes, err := cm.Read(ctx, src)
				if err != nil {
					return err
				}
				return cm.Write(ctx, dst, bytes)
			})
			sourceURI, err := snapshotstorage.NewSnapshotWriter(cm).Save(ctx, snapshot)
			require.NoError(t, err)
			req := &datapb.SnapshotExportCopyRequest{
				TaskId: 1, SegmentEnd: 2,
				SnapshotId: snapshot.SnapshotInfo.GetId(), CollectionId: snapshot.SnapshotInfo.GetCollectionId(),
				SnapshotMetadataPath: sourceURI, TargetUri: path.Join(cm.RootPath(), "export"),
				SourceStorageConfig: &indexpb.StorageConfig{},
			}
			resolved := &snapshotstorage.ResolvedForeignStorage{ForeignCM: cm, Copier: copier, ForeignStorageConfig: &indexpb.StorageConfig{}}
			copied, err := snapshotstorage.ExecuteSnapshotExport(ctx, req, cm, resolved, make(chan struct{}, 2), nil)
			require.NoError(t, err)
			final, err := snapshotstorage.FinalizeSnapshotExport(ctx, req, []*datapb.SnapshotExportCopyTask{{TaskId: 1, SegmentEnd: 2, Result: copied}}, cm, resolved)
			require.NoError(t, err)
			uri := final.MetadataURI
			err = commitSnapshotExportMetadata(ctx, cm, req.GetTargetUri(), uri)
			require.NoError(t, err)
			result, err := snapshotstorage.NewSnapshotReader(cm).ReadSnapshot(ctx, uri, true)
			require.NoError(t, err)
			require.Equal(t, commit, result.Segments[0].GetCommitTimestamp(), "export preserves captured timestamps without inferring omitted values")
			require.Zero(t, result.Segments[1].GetCommitTimestamp())
			copiedDelete := result.Segments[1].GetDeltalogs()[0].GetBinlogs()[0].GetLogPath()
			require.NotEqual(t, deletePath, copiedDelete)
			payload, err := cm.Read(ctx, copiedDelete)
			require.NoError(t, err)
			require.Equal(t, []byte("delete"), payload)
		})
	}
}

func newSnapshotExporterCopierMock(
	t *testing.T,
	copyFn func(context.Context, string, string, string, string) error,
) storage.CrossBucketCopier {
	t.Helper()
	target := &snapshotExporterCopierTarget{}
	metadataCopy := mockey.Mock((*snapshotExporterCopierTarget).CopyCrossBucket).To(copyFn).Build()
	mockCopy := mockey.Mock((*snapshotExporterCopierTarget).CopyCrossBucketAndGetSize).To(func(ctx context.Context, srcBucket, src, dstBucket, dst string) (int64, error) {
		if err := copyFn(ctx, srcBucket, src, dstBucket, dst); err != nil {
			return 0, err
		}
		// Real-file fixtures report their copied bytes; synthetic copy fixtures
		// only exercise scheduling and use zero-byte results.
		info, err := os.Stat(dst)
		if os.IsNotExist(err) {
			return 0, nil
		}
		if err != nil {
			return 0, err
		}
		return info.Size(), nil
	}).Build()
	t.Cleanup(func() { mockCopy.UnPatch(); metadataCopy.UnPatch() })
	return target
}
