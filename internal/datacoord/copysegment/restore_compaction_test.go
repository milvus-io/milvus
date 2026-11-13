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

package copysegment

import (
	"context"
	"fmt"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	kvdatacoord "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/snapshotio"
	snapshotstorage "github.com/milvus-io/milvus/internal/snapshotio/storage"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type legacyFingerprintSnapshotSource struct {
	SnapshotSource
}

func (*legacyFingerprintSnapshotSource) ReadExternalSnapshotDataWithChunkManager(ctx context.Context, cm storage.ChunkManager, location string, includeSegments bool) (*snapshotstorage.SnapshotData, error) {
	return snapshotstorage.NewSnapshotReader(cm).ReadSnapshot(ctx, location, includeSegments)
}

func TestLoadCopySnapshotPreservesLegacyFingerprintAfterRestart(t *testing.T) {
	paramtable.Init()
	for _, tc := range []struct {
		version int64
		origin  string
	}{{2, "source"}, {3, "source"}, {2, "old"}, {3, "old"}} {
		version, origin := tc.version, tc.origin
		t.Run(fmt.Sprintf("v%d/origin_%s", version, origin), func(t *testing.T) {
			ctx := t.Context()
			uri := "s3://source/root/snapshots/1/metadata/2.json"
			manifest := "s3://source/root/snapshots/1/manifests/20.avro"
			metadata := &datapb.SnapshotMetadata{
				FormatVersion: 6, Layout: datapb.SnapshotLayout_SnapshotLayoutSelfContained,
				SnapshotInfo: &datapb.SnapshotInfo{Id: 2, CollectionId: 1, S3Location: "s3://" + origin + "/root/snapshots/1/metadata/2.json"},
				Collection:   &datapb.CollectionDescription{Schema: &schemapb.CollectionSchema{}},
				ManifestList: []string{manifest}, SegmentIds: []int64{20},
			}
			segment := &datapb.SegmentDescription{
				SegmentId: 20, StorageVersion: version, SegmentLevel: datapb.SegmentLevel_L1,
				Statslogs: []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{LogPath: "s3://source/root/files/stats"}}}},
			}
			if version == 2 {
				segment.Binlogs = []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{LogPath: "s3://source/root/files/insert"}}}}
			} else {
				segment.ManifestPath = packed.MarshalManifestPath("s3://source/root/files/segment", 7)
				metadata.Storagev2ManifestList = []*datapb.StorageV2SegmentManifest{{SegmentId: 20, Manifest: segment.ManifestPath}}
			}
			raw, err := protojson.Marshal(metadata)
			require.NoError(t, err)
			avro, err := snapshotio.MarshalSegmentManifest(segment)
			require.NoError(t, err)
			cm := storage.NewLocalChunkManager()
			read := mockey.Mock((*storage.LocalChunkManager).Read).To(func(_ *storage.LocalChunkManager, _ context.Context, key string) ([]byte, error) {
				switch key {
				case "root/snapshots/1/metadata/2.json":
					return raw, nil
				case "root/snapshots/1/manifests/20.avro":
					return avro, nil
				default:
					return nil, merr.WrapErrDataIntegrityMsg("unexpected object key: %s", key)
				}
			}).Build()
			defer read.UnPatch()
			reader := snapshotstorage.NewSnapshotReader(cm)
			// Legacy same-prefix reads retained destination URI fields even with an old origin.
			legacySegment, err := reader.ReadSegmentManifest(ctx, manifest, 6)
			require.NoError(t, err)
			legacySegment.ManifestPath = segment.ManifestPath
			legacy := &snapshotstorage.SnapshotData{
				SnapshotInfo: proto.Clone(metadata.SnapshotInfo).(*datapb.SnapshotInfo), Collection: metadata.Collection,
				Segments: []*datapb.SegmentDescription{legacySegment}, ManifestPaths: metadata.ManifestList,
				SegmentIDs: metadata.SegmentIds, Layout: metadata.Layout,
			}
			legacy.SnapshotInfo.S3Location = uri
			fingerprint, err := snapshotstorage.SnapshotFingerprint(legacy)
			require.NoError(t, err)
			catalog := kvdatacoord.NewCatalog(meta.NewMetaMemoryKV(), "", "")
			require.NoError(t, catalog.SaveCopySegmentJob(ctx, &datapb.CopySegmentJob{
				JobId: 100, External: true, SnapshotS3Location: uri, SnapshotFingerprint: fingerprint,
				State: datapb.CopySegmentJobState_CopySegmentJobExecuting,
			}))
			resolve := mockey.Mock(snapshotstorage.ResolveForeignStorage).Return(&snapshotstorage.ResolvedForeignStorage{ForeignCM: cm}, nil).Build()
			defer resolve.UnPatch()
			source := &legacyFingerprintSnapshotSource{}
			restored, err := NewCopySegmentMeta(ctx, catalog, nil, nil, source, nil)
			require.NoError(t, err)
			job := restored.GetJob(ctx, 100)
			require.Nil(t, job.(*Job).SnapshotCache.data)
			task := CreateTestCopyTask(1, 200).(*Task)
			task.SnapshotMeta = source
			task.Task.Load().IdMappings[0].SourceSegmentId = 20
			_, err = AssembleCopySegmentRequest(task, job)
			require.NoError(t, err)
			require.True(t, proto.Equal(legacySegment, job.(*Job).SnapshotCache.data.Segments[0]))

			// Real content changes must still fail after another restart.
			segment.NumOfRows = 42
			avro, err = snapshotio.MarshalSegmentManifest(segment)
			require.NoError(t, err)
			restored, err = NewCopySegmentMeta(ctx, catalog, nil, nil, source, nil)
			require.NoError(t, err)
			_, err = AssembleCopySegmentRequest(task, restored.GetJob(ctx, 100))
			require.ErrorContains(t, err, "external snapshot changed after restore job creation")
		})
	}
}
