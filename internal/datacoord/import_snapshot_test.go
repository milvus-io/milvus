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
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	idallocator "github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/snapshotio"
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
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestPrepareSnapshotImportMetadataOnly(t *testing.T) {
	paramtable.Init()
	type sourceCM struct{ milvusstorage.ChunkManager }
	uri := "s3://source/root/snapshots/1/metadata/2.json"
	instance := mockey.Mock(snapshotstorage.ValidateInstanceSnapshotImportURI).Return(nil).Build()
	defer instance.UnPatch()
	physical := mockey.Mock(packed.GetDeltaLogPathsFromManifest).Return(nil, merr.ErrIoKeyNotFound).Build()
	defer physical.UnPatch()
	validation := mockey.Mock(snapshotstorage.ValidateExternalSnapshotDataFiles).Return(merr.ErrIoKeyNotFound).Build()
	defer validation.UnPatch()
	for _, layout := range []datapb.SnapshotLayout{datapb.SnapshotLayout_SnapshotLayoutReferenced, datapb.SnapshotLayout_SnapshotLayoutSelfContained} {
		for _, count := range []int{400, 1024} {
			t.Run(fmt.Sprintf("%s/%d", layout, count), func(t *testing.T) {
				snapshot := snapshotImportTestData(layout)
				metadata := &datapb.SnapshotMetadata{
					FormatVersion: int32(snapshotstorage.SnapshotFormatVersion),
					SnapshotInfo:  snapshot.SnapshotInfo, Collection: snapshot.Collection, Layout: layout,
				}
				metadata.SnapshotInfo.S3Location = uri
				for i := 0; i < count; i++ {
					metadata.ManifestList = append(metadata.ManifestList, fmt.Sprintf("root/snapshots/1/manifests/2/%d", i))
					metadata.SegmentIds = append(metadata.SegmentIds, int64(i+1))
				}
				payload, err := protojson.Marshal(metadata)
				require.NoError(t, err)
				reads := 0
				read := mockey.Mock((*sourceCM).Read).To(func(_ *sourceCM, _ context.Context, key string) ([]byte, error) {
					reads++
					require.Equal(t, "root/snapshots/1/metadata/2.json", key)
					return payload, nil
				}).Build()
				defer read.UnPatch()
				files, options, err := prepareSnapshotImportFiles(context.Background(), nil, &sourceCM{}, snapshotImportTestSchema(),
					[]*internalpb.ImportFile{{Paths: []string{uri}}}, snapshotImportTestOptions())
				require.NoError(t, err)
				require.Equal(t, 1, reads)
				require.Zero(t, physical.Times())
				require.Zero(t, validation.Times())
				require.True(t, importutilv2.IsSnapshotPreparation(files))
				require.NoError(t, importutilv2.ValidateSnapshotImportPlan(files, options, nil))
				require.ErrorIs(t, importutilv2.ValidateSnapshotImportTask(files, options, nil), merr.ErrServiceUnimplemented)
				// The same descriptor survives actual WAL encoding and CDC channel
				// rewriting without source I/O or interpreting metadata as row data.
				wal := message.NewImportMessageBuilderV1().WithHeader(&message.ImportMessageHeader{SnapshotSources: []*internalpb.SnapshotImportSource{files[0].SnapshotSource}}).
					WithBody(&msgpb.ImportMsg{Files: []*msgpb.ImportFile{{}}}).WithBroadcast([]string{"target"}).MustBuildBroadcast()
				msgID := walimplstest.NewTestMessageID(1)
				immutable := wal.WithBroadcastID(1).SplitIntoMutableMessage()[0].WithTimeTick(100).WithLastConfirmed(msgID).IntoImmutableMessage(msgID)
				replicated := message.MustNewReplicateMessage("source-cluster", immutable.IntoImmutableMessageProto())
				replicated.OverwriteReplicateVChannel("replica", []string{"replica"})
				replica := message.MustAsMutableImportMessageV1(replicated)
				bound, err := bindSnapshotImportSources(replica.MustBody().GetFiles(), replica.Header().GetSnapshotSources())
				require.NoError(t, err)
				require.True(t, proto.Equal(files[0], bound[0]))
				decoded := &datapb.SnapshotMetadata{}
				require.NoError(t, proto.Unmarshal(bound[0].SnapshotSource.SnapshotMetadata, decoded))
				require.Len(t, decoded.ManifestList, count)
			})
		}
	}
}

func TestPrepareSnapshotImportAdmissionErrors(t *testing.T) {
	paramtable.Init()
	type sourceCM struct{ milvusstorage.ChunkManager }
	for _, mode := range []string{"ordinary", "legacy_backup", "invalid_options", "nil_cm", "files", "empty_paths", "multiple_files", "multiple_paths", "invalid_uri", "bare_path", "instance", "resolve", "read", "metadata", "location", "boundary", "schema", "external_source", "encryption", "target", "mapping", "layout", "encoding"} {
		t.Run(mode, func(t *testing.T) {
			snapshot := snapshotImportTestData(datapb.SnapshotLayout_SnapshotLayoutReferenced)
			metadata := &datapb.SnapshotMetadata{
				FormatVersion: int32(snapshotstorage.SnapshotFormatVersion),
				SnapshotInfo:  snapshot.SnapshotInfo, Collection: snapshot.Collection, Layout: snapshot.Layout,
			}
			options := snapshotImportTestOptions()
			files := []*internalpb.ImportFile{{Paths: []string{"s3://source/root/snapshots/1/metadata/2.json"}}}
			var cm milvusstorage.ChunkManager = &sourceCM{}
			target := snapshotImportTestSchema()
			var instanceErr, resolveErr, readErr error
			switch mode {
			case "ordinary":
				options = nil
			case "legacy_backup":
				options = importutilv2.Options{{Key: importutilv2.BackupFlag, Value: "true"}, {Key: importutilv2.EZK, Value: "legacy-key"}}
				files[0].Paths = []string{"legacy/path"}
			case "invalid_options":
				options = append(options, &commonpb.KeyValuePair{Key: importutilv2.StorageVersion, Value: "3"})
			case "nil_cm":
				cm = nil
			case "files":
				files = nil
			case "empty_paths":
				files[0].Paths = nil
			case "multiple_files":
				files = append(files, proto.Clone(files[0]).(*internalpb.ImportFile))
			case "multiple_paths":
				files[0].Paths = append(files[0].Paths, files[0].Paths[0])
			case "invalid_uri":
				files[0].Paths[0] = "s3://["
			case "bare_path":
				files[0].Paths[0] = "root/snapshots/1/metadata/2.json"
			case "instance":
				instanceErr = merr.ErrParameterInvalid
			case "resolve":
				resolveErr = merr.ErrIoFailed
			case "read":
				readErr = merr.ErrIoKeyNotFound
			case "metadata":
				metadata.Collection = nil
			case "location":
				metadata.SnapshotInfo.Id = 99
			case "boundary":
				metadata.ManifestList = []string{"outside/segment"}
			case "schema":
				metadata.Collection.Schema = nil
			case "external_source":
				metadata.Collection.Schema.ExternalSource = "s3://other/table"
			case "encryption":
				metadata.Collection.Schema.Properties = []*commonpb.KeyValuePair{{Key: common.EncryptionEzIDKey, Value: "1"}}
			case "target":
				target.ExternalSource = "s3://other/table"
			case "mapping":
				options = append(options, &commonpb.KeyValuePair{Key: importutilv2.PartitionMapping, Value: `{"unknown":"target"}`})
			case "layout":
				metadata.Layout = datapb.SnapshotLayout(99)
			case "encoding":
				metadata.Collection.Schema.Name = "\xff"
			}
			instance := mockey.Mock(snapshotstorage.ValidateInstanceSnapshotImportURI).Return(instanceErr).Build()
			defer instance.UnPatch()
			resolve := mockey.Mock(importutilv2.ResolveSnapshotImportStorage).Return(cm, nil, resolveErr).Build()
			defer resolve.UnPatch()
			read := mockey.Mock((*snapshotstorage.SnapshotReader).ReadMetadata).Return(metadata, readErr).Build()
			defer read.UnPatch()
			got, normalized, err := prepareSnapshotImportFiles(context.Background(), nil, cm, target, files, options)
			if mode == "ordinary" || mode == "legacy_backup" {
				require.NoError(t, err)
				require.Equal(t, files, got)
				require.Same(t, files[0], got[0])
				require.Equal(t, options, normalized, "non-snapshot options must remain unchanged")
				require.Zero(t, read.Times())
				require.Zero(t, resolve.Times())
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestBroadcastSnapshotImportPreparationLimit(t *testing.T) {
	paramtable.Init()
	patchSnapshotImportInstance(t)
	ctx := context.Background()
	cm := milvusstorage.NewLocalChunkManager()
	server := &Server{meta: &meta{chunkManager: cm}}
	validation := mockey.Mock((*Server).validateImportRequest).Return(nil).Build()
	defer validation.UnPatch()
	start := mockey.Mock((*Server).startBroadcastWithCollectionID).Return(nil, merr.ErrIoFailed).Build()
	defer start.UnPatch()
	for _, layout := range []datapb.SnapshotLayout{
		datapb.SnapshotLayout_SnapshotLayoutReferenced,
		datapb.SnapshotLayout_SnapshotLayoutSelfContained,
	} {
		t.Run(layout.String(), func(t *testing.T) {
			snapshot := snapshotImportTestData(layout)
			metadata := &datapb.SnapshotMetadata{
				FormatVersion: int32(snapshotstorage.SnapshotFormatVersion),
				SnapshotInfo:  snapshot.SnapshotInfo, Collection: snapshot.Collection, Layout: layout,
			}
			metadata.SnapshotInfo.Description = strings.Repeat("x", importutilv2.SnapshotSourcePlanMaxBytes)
			read := mockey.Mock((*snapshotstorage.SnapshotReader).ReadMetadata).Return(metadata, nil).Build()
			defer read.UnPatch()
			files := []*internalpb.ImportFile{{Paths: []string{"s3://source/root/snapshots/1/metadata/2.json"}}}
			// Construction does not publish the descriptor. The real broadcast
			// entry point must reject it before acquiring the broadcast handle.
			captured, _, err := prepareSnapshotImportFiles(ctx, []int64{10}, cm, snapshotImportTestSchema(), files, snapshotImportTestOptions())
			require.NoError(t, err)
			require.Greater(t, proto.Size(captured[0]), importutilv2.SnapshotSourcePlanMaxBytes)
			_, _, err = server.broadcastImport(ctx, "target", 100, []int64{10}, files,
				snapshotImportTestOptions(), snapshotImportTestSchema(), 1000, []string{"target_v1"}, "")
			require.ErrorIs(t, err, merr.ErrImportFailed)
			require.ErrorContains(t, err, "snapshot preparation input exceeds 256 KiB")
			require.Zero(t, start.Times(), "oversized preparation must fail before broadcast")
		})
	}
}

func TestSnapshotPreparationPersistence(t *testing.T) {
	paramtable.Init()
	type sourceCM struct{ milvusstorage.ChunkManager }
	type catalog struct{ metastore.DataCoordCatalog }
	type alloc struct{ allocator.Allocator }
	instance := mockey.Mock(snapshotstorage.ValidateInstanceSnapshotImportURI).Return(nil).Build()
	defer instance.UnPatch()
	for _, mode := range []string{"success", "missing_segment", "save_retry", "fail_save", "alloc_retry", "cancel_allocate", "superseded", "aborted", "canceled", "invalid_metadata", "invalid_shared_scope"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			snapshot := snapshotImportTestData(datapb.SnapshotLayout_SnapshotLayoutReferenced)
			uri := "s3://source/root/snapshots/1/metadata/2.json"
			if mode == "invalid_shared_scope" {
				for _, segment := range snapshot.Segments {
					segment.ChannelName = "source"
				}
				snapshot.Segments[0].PartitionId = 0
				snapshot.Segments = append(snapshot.Segments, &datapb.SegmentDescription{
					SegmentId: 30, PartitionId: common.AllPartitionsID, ChannelName: "source",
					SegmentLevel: datapb.SegmentLevel_L0, StorageVersion: milvusstorage.StorageV1,
				})
			}
			metadata := &datapb.SnapshotMetadata{
				FormatVersion: int32(snapshotstorage.SnapshotFormatVersion),
				SnapshotInfo:  snapshot.SnapshotInfo, Collection: snapshot.Collection, Layout: snapshot.Layout,
			}
			objects := map[string][]byte{}
			for _, segment := range snapshot.Segments {
				segment.CommitTimestamp = 300
				key := fmt.Sprintf("root/snapshots/1/manifests/2/%d", segment.SegmentId)
				payload, err := snapshotio.MarshalSegmentManifest(segment)
				require.NoError(t, err)
				objects[key] = payload
				metadata.ManifestList = append(metadata.ManifestList, key)
				metadata.SegmentIds = append(metadata.SegmentIds, segment.SegmentId)
				if segment.ManifestPath != "" {
					metadata.Storagev2ManifestList = append(metadata.Storagev2ManifestList, &datapb.StorageV2SegmentManifest{
						SegmentId: segment.SegmentId, Manifest: segment.ManifestPath,
					})
				}
			}
			payload, err := protojson.Marshal(metadata)
			require.NoError(t, err)
			objects["root/snapshots/1/metadata/2.json"] = payload
			m := &importMeta{jobs: make(map[int64]ImportJob), tasks: newImportTasks(), catalog: &catalog{}}
			reads := 0
			read := mockey.Mock((*sourceCM).Read).To(func(_ *sourceCM, _ context.Context, key string) ([]byte, error) {
				reads++
				if reads > 1 {
					if mode == "missing_segment" || mode == "fail_save" {
						return nil, merr.ErrIoKeyNotFound
					}
					if mode == "aborted" {
						require.NoError(t, m.UpdateJob(ctx, 10, UpdateJobState(internalpb.ImportJobState_Failed)))
					}
					if mode == "canceled" {
						cancel()
					}
				}
				data, ok := objects[key]
				if !ok {
					return nil, merr.ErrIoKeyNotFound
				}
				return data, nil
			}).Build()
			defer read.UnPatch()
			files, options, err := prepareSnapshotImportFiles(ctx, nil, &sourceCM{}, snapshotImportTestSchema(),
				[]*internalpb.ImportFile{{Paths: []string{uri}}}, snapshotImportTestOptions())
			require.NoError(t, err)
			// Root metadata is no longer available after create. Preparation must
			// consume the captured bytes, not reopen this object or resolve latest.
			delete(objects, "root/snapshots/1/metadata/2.json")
			if mode == "invalid_metadata" {
				files[0].SnapshotSource.SnapshotMetadata = []byte{0xff}
			}
			job := &importJob{ImportJob: &datapb.ImportJob{
				JobID: 10, State: internalpb.ImportJobState_Pending,
				Files: files, Options: options, Schema: snapshotImportTestSchema(),
			}}
			m.jobs[10] = job
			failSave := mode == "save_retry" || mode == "fail_save"
			save := mockey.Mock((*catalog).SaveImportJob).To(func(_ *catalog, _ context.Context, _ *datapb.ImportJob) error {
				if failSave {
					failSave = false
					return merr.ErrIoFailed
				}
				return nil
			}).Build()
			defer save.UnPatch()
			failAlloc := mode == "alloc_retry"
			ids := mockey.Mock((*alloc).AllocN).To(func(_ *alloc, _ int64) (int64, int64, error) {
				if failAlloc {
					failAlloc = false
					return 0, 0, merr.ErrIoFailed
				}
				if mode == "cancel_allocate" {
					cancel()
				}
				if mode == "superseded" {
					require.NoError(t, m.UpdateJob(ctx, 10, func(current ImportJob) {
						current.(*importJob).Files = []*internalpb.ImportFile{{Paths: []string{"published"}}}
					}))
				}
				return 100, 200, nil
			}).Build()
			defer ids.UnPatch()
			checker := &importChecker{ctx: ctx, meta: &meta{chunkManager: &sourceCM{}}, importMeta: m, alloc: &alloc{}}
			checker.prepareSnapshotJob(ctx, job)
			if mode == "save_retry" || mode == "alloc_retry" || mode == "fail_save" {
				require.True(t, importutilv2.IsSnapshotPreparation(m.GetJob(ctx, 10).GetFiles()))
				require.Empty(t, m.tasks.listTasks())
				checker.prepareSnapshotJob(ctx, m.GetJob(ctx, 10))
			}
			got := m.GetJob(ctx, 10)
			switch mode {
			case "missing_segment", "invalid_metadata", "aborted", "fail_save", "invalid_shared_scope":
				require.Equal(t, internalpb.ImportJobState_Failed, got.GetState())
				require.True(t, importutilv2.IsSnapshotPreparation(got.GetFiles()))
				if mode == "invalid_shared_scope" {
					require.Positive(t, ids.Times(), "complete-plan validation must run after expansion and ID allocation")
					require.Contains(t, got.GetReason(), "invalid task-shared snapshot source descriptor")
				}
			case "canceled", "cancel_allocate":
				require.Equal(t, internalpb.ImportJobState_Pending, got.GetState())
				require.True(t, importutilv2.IsSnapshotPreparation(got.GetFiles()))
			case "superseded":
				require.Equal(t, []string{"published"}, got.GetFiles()[0].GetPaths())
			default:
				require.Equal(t, internalpb.ImportJobState_Pending, got.GetState(), got.GetReason())
				require.False(t, importutilv2.IsSnapshotPreparation(got.GetFiles()))
				require.Len(t, got.GetFiles(), 2)
				require.EqualValues(t, 100, got.GetFiles()[0].Id)
				require.NoError(t, importutilv2.ValidateSnapshotImportPlan(got.GetFiles(), got.GetOptions(), got.GetSnapshotL0Sources()))
			}
			require.Empty(t, m.tasks.listTasks(), "preparation must not create tasks before durable publication")
		})
	}
}

func TestSnapshotPreparationFinalPlanLimit(t *testing.T) {
	paramtable.Init()
	type sourceCM struct{ milvusstorage.ChunkManager }
	type catalog struct{ metastore.DataCoordCatalog }
	type alloc struct{ allocator.Allocator }
	instance := mockey.Mock(snapshotstorage.ValidateInstanceSnapshotImportURI).Return(nil).Build()
	defer instance.UnPatch()
	resolve := mockey.Mock(snapshotstorage.ResolveSnapshotReadStorage).Return(&snapshotstorage.ResolvedForeignStorage{
		ForeignCM: &sourceCM{}, ForeignStorageConfig: &indexpb.StorageConfig{BucketName: "source"},
	}, nil).Build()
	defer resolve.UnPatch()
	for _, mode := range []string{"plain", "external", "mapped", "external_mapped", "save_retry", "superseded", "canceled"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			snapshot := snapshotImportTestData(datapb.SnapshotLayout_SnapshotLayoutSelfContained)
			snapshot.SnapshotInfo.S3Location = "s3://source/root/snapshots/1/metadata/2.json"
			// The captured metadata is small, but relocating the bundle repeats
			// the long root in every expanded file. Zero commit timestamps and no
			// L0 deliberately bypass attachSnapshotImportSources' partial checks.
			newRoot := strings.Repeat("r", 700)
			uri := "s3://source/" + newRoot + "/snapshots/1/metadata/2.json"
			metadata := &datapb.SnapshotMetadata{
				FormatVersion: int32(snapshotstorage.SnapshotFormatVersion),
				SnapshotInfo:  snapshot.SnapshotInfo, Collection: snapshot.Collection, Layout: snapshot.Layout,
			}
			objects := make(map[string][]byte)
			const count = 400
			for i := 1; i <= count; i++ {
				segment := proto.Clone(snapshot.Segments[0]).(*datapb.SegmentDescription)
				segment.SegmentId = int64(i)
				segment.ManifestPath = packed.MarshalManifestPath(fmt.Sprintf("root/files/segment/%d", i), 1)
				key := fmt.Sprintf("root/snapshots/1/manifests/2/%d", i)
				payload, err := snapshotio.MarshalSegmentManifest(segment)
				require.NoError(t, err)
				objects[newRoot+strings.TrimPrefix(key, "root")] = payload
				metadata.ManifestList = append(metadata.ManifestList, key)
				metadata.SegmentIds = append(metadata.SegmentIds, segment.SegmentId)
				metadata.Storagev2ManifestList = append(metadata.Storagev2ManifestList, &datapb.StorageV2SegmentManifest{
					SegmentId: segment.SegmentId, Manifest: segment.ManifestPath,
				})
			}
			payload, err := protojson.Marshal(metadata)
			require.NoError(t, err)
			objects[newRoot+"/snapshots/1/metadata/2.json"] = payload
			read := mockey.Mock((*sourceCM).Read).To(func(_ *sourceCM, _ context.Context, key string) ([]byte, error) {
				data, ok := objects[key]
				require.True(t, ok, "unexpected object %s", key)
				return data, nil
			}).Build()
			defer read.UnPatch()
			options := snapshotImportTestOptions()
			if strings.Contains(mode, "external") {
				options = append(options, &commonpb.KeyValuePair{Key: importutilv2.ExternalSpec, Value: `{"extfs":{"region":"us-east-1"}}`})
			}
			if strings.Contains(mode, "mapped") {
				options = append(options, &commonpb.KeyValuePair{Key: importutilv2.PartitionMapping, Value: `{"source_partition":"X"}`})
			}
			files, options, err := prepareSnapshotImportFiles(ctx, []int64{200}, &sourceCM{}, snapshotImportTestSchema(),
				[]*internalpb.ImportFile{{Paths: []string{uri}}}, options)
			require.NoError(t, err)
			require.Less(t, proto.Size(files[0]), importutilv2.SnapshotSourcePlanMaxBytes)
			job := &importJob{ImportJob: &datapb.ImportJob{
				JobID: 10, State: internalpb.ImportJobState_Pending, PartitionIDs: []int64{200},
				Files: files, Options: options, Schema: snapshotImportTestSchema(),
			}}
			m := &importMeta{jobs: map[int64]ImportJob{10: job}, tasks: newImportTasks(), catalog: &catalog{}}
			failSave := mode == "save_retry"
			save := mockey.Mock((*catalog).SaveImportJob).To(func(_ *catalog, _ context.Context, saved *datapb.ImportJob) error {
				if failSave {
					failSave = false
					return merr.ErrIoFailed
				}
				if mode != "superseded" {
					require.Equal(t, internalpb.ImportJobState_Failed, saved.State)
					require.True(t, importutilv2.IsSnapshotPreparation(saved.Files), "invalid expanded files must never be persisted")
				}
				return nil
			}).Build()
			defer save.UnPatch()
			ids := mockey.Mock((*alloc).AllocN).To(func(_ *alloc, n int64) (int64, int64, error) {
				require.EqualValues(t, count, n, "the entire relocated plan must reach final validation")
				switch mode {
				case "superseded":
					require.NoError(t, m.UpdateJob(ctx, 10, func(current ImportJob) {
						current.(*importJob).Files = []*internalpb.ImportFile{{Paths: []string{"published"}}}
					}))
				case "canceled":
					cancel()
				}
				return 1 << 60, (1 << 60) + n, nil
			}).Build()
			defer ids.UnPatch()
			checker := &importChecker{ctx: ctx, meta: &meta{chunkManager: &sourceCM{}}, importMeta: m, alloc: &alloc{}}
			checker.prepareSnapshotJob(ctx, job)
			if mode == "save_retry" {
				require.Equal(t, internalpb.ImportJobState_Pending, m.GetJob(ctx, 10).GetState())
				checker.prepareSnapshotJob(ctx, m.GetJob(ctx, 10))
			}
			got := m.GetJob(ctx, 10)
			switch mode {
			case "superseded":
				require.Equal(t, internalpb.ImportJobState_Pending, got.GetState())
				require.Equal(t, []string{"published"}, got.GetFiles()[0].GetPaths())
			case "canceled":
				require.Equal(t, internalpb.ImportJobState_Pending, got.GetState())
				require.Zero(t, save.Times())
			default:
				require.Equal(t, internalpb.ImportJobState_Failed, got.GetState())
				require.Contains(t, got.GetReason(), "snapshot source plan exceeds 256 KiB")
			}
			require.Positive(t, ids.Times())
			require.Empty(t, m.tasks.listTasks())
		})
	}
}

func TestSnapshotPreparationBoundedAndCanceled(t *testing.T) {
	type sourceCM struct{ milvusstorage.ChunkManager }
	instance := mockey.Mock(snapshotstorage.ValidateInstanceSnapshotImportURI).Return(nil).Build()
	defer instance.UnPatch()
	started := make(chan struct{}, 4)
	read := mockey.Mock((*sourceCM).Read).To(func(_ *sourceCM, ctx context.Context, _ string) ([]byte, error) {
		started <- struct{}{}
		<-ctx.Done()
		return nil, ctx.Err()
	}).Build()
	defer read.UnPatch()
	snapshot := snapshotImportTestData(datapb.SnapshotLayout_SnapshotLayoutReferenced)
	payload, err := proto.Marshal(&datapb.SnapshotMetadata{
		FormatVersion: int32(snapshotstorage.SnapshotFormatVersion),
		SnapshotInfo:  snapshot.SnapshotInfo, Collection: snapshot.Collection, ManifestList: []string{"root/segment"},
	})
	require.NoError(t, err)
	options := append(snapshotImportTestOptions(), &commonpb.KeyValuePair{Key: importutilv2.SnapshotSourceURI, Value: "s3://source/root/snapshots/1/metadata/2.json"},
		&commonpb.KeyValuePair{Key: importutilv2.SnapshotLayout, Value: "referenced"})
	m := &importMeta{jobs: make(map[int64]ImportJob), tasks: newImportTasks()}
	checker := &importChecker{ctx: context.Background(), meta: &meta{chunkManager: &sourceCM{}}, importMeta: m, closeChan: make(chan struct{})}
	defer checker.Close()
	for i := int64(1); i <= 5; i++ {
		job := &importJob{ImportJob: &datapb.ImportJob{
			JobID: i, State: internalpb.ImportJobState_Pending, Options: options,
			Schema: snapshotImportTestSchema(), Files: []*internalpb.ImportFile{{SnapshotSource: &internalpb.SnapshotImportSource{
				Version: importutilv2.SnapshotPreparationVersion, SnapshotMetadata: payload,
			}}},
		}}
		m.jobs[i] = job
		checker.checkPendingJob(job)
		checker.checkPendingJob(job) // Repeated ticks cannot duplicate an attempt.
	}
	for i := 0; i < 4; i++ {
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatal("snapshot preparation did not start")
		}
	}
	checker.prepareMu.Lock()
	preparing := len(checker.preparing)
	checker.prepareMu.Unlock()
	require.Equal(t, 4, preparing)
	// Saturated source I/O must not prevent the same checker from scheduling
	// ordinary imports. Stop at the task-creation dependency, before catalog I/O.
	create := mockey.Mock(NewPreImportTasks).Return(nil, merr.ErrIoFailed).Build()
	defer create.UnPatch()
	checker.checkPendingJob(&importJob{ImportJob: &datapb.ImportJob{
		JobID: 6,
		Files: []*internalpb.ImportFile{{Paths: []string{"ordinary.parquet"}}},
	}})
	require.EqualValues(t, 1, create.Times())
	// Removing a job cancels its source read on the next state-machine tick.
	m.mu.Lock()
	delete(m.jobs, 1)
	m.mu.Unlock()
	checker.cancelInactiveSnapshotPreparations()
	checker.Close()
	checker.prepareWG.Wait()
	require.Empty(t, checker.preparing)
	require.Empty(t, m.tasks.listTasks())
	// A closed checker cannot start a late attempt.
	checker.scheduleSnapshotPreparation(m.jobs[5])
	require.Empty(t, checker.preparing)
}

func TestSnapshotPreparationDeadline(t *testing.T) {
	type sourceCM struct{ milvusstorage.ChunkManager }
	instance := mockey.Mock(snapshotstorage.ValidateInstanceSnapshotImportURI).Return(nil).Build()
	defer instance.UnPatch()
	read := mockey.Mock((*sourceCM).Read).To(func(_ *sourceCM, ctx context.Context, _ string) ([]byte, error) {
		return nil, ctx.Err()
	}).Build()
	defer read.UnPatch()
	snapshot := snapshotImportTestData(datapb.SnapshotLayout_SnapshotLayoutReferenced)
	payload, err := proto.Marshal(&datapb.SnapshotMetadata{
		FormatVersion: int32(snapshotstorage.SnapshotFormatVersion),
		SnapshotInfo:  snapshot.SnapshotInfo, Collection: snapshot.Collection, ManifestList: []string{"root/segment"},
	})
	require.NoError(t, err)
	job := &importJob{ImportJob: &datapb.ImportJob{
		JobID: 1, TimeoutTs: 1, State: internalpb.ImportJobState_Pending,
		Schema: snapshotImportTestSchema(), Options: append(snapshotImportTestOptions(),
			&commonpb.KeyValuePair{Key: importutilv2.SnapshotSourceURI, Value: "s3://source/root/snapshots/1/metadata/2.json"},
			&commonpb.KeyValuePair{Key: importutilv2.SnapshotLayout, Value: "referenced"}),
		Files: []*internalpb.ImportFile{{SnapshotSource: &internalpb.SnapshotImportSource{Version: importutilv2.SnapshotPreparationVersion, SnapshotMetadata: payload}}},
	}}
	checker := &importChecker{ctx: context.Background(), meta: &meta{chunkManager: &sourceCM{}}, closeChan: make(chan struct{})}
	defer checker.Close()
	checker.scheduleSnapshotPreparation(job)
	checker.prepareWG.Wait()
	require.True(t, importutilv2.IsSnapshotPreparation(job.GetFiles()))
	require.Equal(t, internalpb.ImportJobState_Pending, job.GetState(), "the existing timeout/GC loop owns the terminal transition")
}

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
	l0Sources, err := attachSnapshotImportSources(ctx, cm, "s3://source/"+strings.TrimPrefix(root, "/")+"/snapshots/1/metadata/2.json",
		snapshot, data, deltas, files)
	require.NoError(t, err)
	l0Source, err := importutilv2.SnapshotTaskL0Source(files, l0Sources)
	require.NoError(t, err)
	require.Equal(t, []string{key}, l0Source.LegacyL0Deltalogs, "URI/key aliases must be deduplicated")
	encoded, err := proto.Marshal(files[0])
	require.NoError(t, err)
	for phase := 0; phase < 2; phase++ {
		file := &internalpb.ImportFile{}
		require.NoError(t, proto.Unmarshal(encoded, file))
		readers := importutilv2.NewReaderFactory(ctx, cm, cfg, snapshotImportTestOptions())
		shared, err := readers.PrepareSnapshotDeletes(ctx, schema, []*internalpb.ImportFile{file}, l0Source, 1024, 1024)
		require.NoError(t, err)
		reader, err := readers.NewReader(ctx, schema, file, 1024, 1024, shared)
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
					ManifestPath: packed.MarshalManifestPath("root/files/l0", 1),
				})
			}
			packedPaths := []string{"root/delta"}
			if tc.name == "packed_alias" {
				deltas = []*datapb.SegmentDescription{{
					PartitionId: 10, ChannelName: "source", StorageVersion: milvusstorage.StorageV3,
					ManifestPath: packed.MarshalManifestPath("root/files/l0", 1),
				}}
				packedPaths = append(packedPaths, tc.uri)
			}
			patch := mockey.Mock(packed.GetDeltaLogPathsFromManifest).Return(packedPaths, nil).Build()
			defer patch.UnPatch()
			cm := milvusstorage.NewRemoteChunkManagerForTesting(nil, "source", "root")
			files := []*internalpb.ImportFile{{}}
			l0Sources, err := attachSnapshotImportSources(context.Background(), cm, tc.metadataURI, snapshot, data, deltas, files)
			if tc.name == "packed_alias" || tc.name == "decoder_alias" {
				require.NoError(t, err)
				require.Equal(t, []string{packed.MarshalManifestPath("root/files/l0", 1)}, l0Sources[0].ManifestL0Paths)
				require.Zero(t, patch.Times(), "physical decoder conflicts are checked by the DN")
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
			for _, segment := range snapshot.Segments {
				segment.ChannelName = "source"
			}
			snapshot.Segments = append(snapshot.Segments, &datapb.SegmentDescription{
				SegmentId: 99, PartitionId: common.AllPartitionsID, ChannelName: "source",
				SegmentLevel: datapb.SegmentLevel_L0, StorageVersion: milvusstorage.StorageV3,
				ManifestPath: packed.MarshalManifestPath("root/files/l0", 1),
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
			readSnapshot := mockey.Mock((*snapshotstorage.SnapshotReader).ReadSnapshotFromMetadata).Return(snapshot, nil).Build()
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
			files, _, l0Sources, err := expandSnapshotImportFiles(context.Background(), nil, milvusstorage.NewLocalChunkManager(), snapshotImportTestSchema(),
				"s3://source/root/snapshots/1/metadata/2.json", options, &datapb.SnapshotMetadata{})
			require.NoError(t, err)
			for _, file := range files {
				require.Empty(t, file.Paths)
				require.EqualValues(t, 6, file.SnapshotSource.Version)
				shared, err := importutilv2.SnapshotTaskL0Source([]*internalpb.ImportFile{file}, l0Sources)
				require.NoError(t, err)
				require.Equal(t, []string{packed.MarshalManifestPath("root/files/l0", 1)}, shared.ManifestL0Paths)
			}
			mappedOptions := append(append(importutilv2.Options(nil), options...), &commonpb.KeyValuePair{Key: importutilv2.PartitionMapping, Value: `{"source_partition":"X"}`})
			mappedShared, _, mappedSources, err := expandSnapshotImportFiles(context.Background(), []int64{200}, milvusstorage.NewLocalChunkManager(), snapshotImportTestSchema(),
				"s3://source/root/snapshots/1/metadata/2.json", mappedOptions, &datapb.SnapshotMetadata{})
			require.NoError(t, err)
			require.Len(t, mappedShared, len(files))
			for i, file := range mappedShared {
				expected := proto.Clone(files[i].GetSnapshotSource()).(*internalpb.SnapshotImportSource)
				expected.Version, expected.TargetPartitionId = 8, 200
				require.True(t, proto.Equal(expected, file.GetSnapshotSource()), "mapping must preserve the existing source context")
				require.Empty(t, file.Paths)
				shared, err := importutilv2.SnapshotTaskL0Source([]*internalpb.ImportFile{file}, mappedSources)
				require.NoError(t, err)
				require.Equal(t, []string{packed.MarshalManifestPath("root/files/l0", 1)}, shared.ManifestL0Paths)
			}
			// External jobs without L0 still need a typed descriptor so older
			// workers cannot ignore the source credentials and read target keys.
			snapshot.Segments = snapshot.Segments[:len(snapshot.Segments)-1]
			files, _, _, err = expandSnapshotImportFiles(context.Background(), nil, milvusstorage.NewLocalChunkManager(), snapshotImportTestSchema(),
				"s3://source/root/snapshots/1/metadata/2.json", options, &datapb.SnapshotMetadata{})
			require.NoError(t, err)
			for _, file := range files {
				require.Empty(t, file.Paths)
				require.EqualValues(t, 2, file.SnapshotSource.Version)
				require.Empty(t, file.SnapshotSource.ManifestL0Deltalogs)
			}
			mapped, _, _, err := expandSnapshotImportFiles(context.Background(), []int64{200}, milvusstorage.NewLocalChunkManager(), snapshotImportTestSchema(),
				"s3://source/root/snapshots/1/metadata/2.json", mappedOptions, &datapb.SnapshotMetadata{})
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
			files, _, _, err = expandSnapshotImportFiles(context.Background(), nil, milvusstorage.NewLocalChunkManager(), snapshotImportTestSchema(),
				"s3://source/root/snapshots/1/metadata/2.json", options, &datapb.SnapshotMetadata{})
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
	patch := mockey.Mock((*snapshotstorage.SnapshotReader).ReadSnapshotFromMetadata).To(func(*snapshotstorage.SnapshotReader, context.Context, string, *datapb.SnapshotMetadata, bool) (*snapshotstorage.SnapshotData, error) {
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
			files, _, _, err := expandSnapshotImportFiles(context.Background(), nil, milvusstorage.NewLocalChunkManager(), snapshotImportTestSchema(),
				uri, snapshotImportTestOptions(), &datapb.SnapshotMetadata{})
			require.ErrorIs(t, err, merr.ErrParameterInvalid)
			require.Nil(t, files)
			require.Zero(t, readCalls, "invalid source identity must fail before metadata IO")
		})
	}
	options := append(snapshotImportTestOptions(), &commonpb.KeyValuePair{Key: importutilv2.ExternalSpec, Value: `{"extfs":{"region":"us-east-1"}}`})
	_, _, _, err := expandSnapshotImportFiles(context.Background(), nil, milvusstorage.NewLocalChunkManager(), snapshotImportTestSchema(),
		"root/snapshots/1/metadata/2.json", options, &datapb.SnapshotMetadata{})
	require.ErrorIs(t, err, merr.ErrParameterInvalid, "extfs must not permit bare metadata keys either")
	require.Zero(t, readCalls)

	_, _, _, err = expandSnapshotImportFiles(context.Background(), nil, milvusstorage.NewLocalChunkManager(), snapshotImportTestSchema(),
		"minio://localhost:9000/source/root/snapshots/1/metadata/2.json", snapshotImportTestOptions(), &datapb.SnapshotMetadata{})
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
				read := mockey.Mock((*snapshotstorage.SnapshotReader).ReadSnapshotFromMetadata).Return(snapshot, nil).Build()
				defer read.UnPatch()
				validate := mockey.Mock(snapshotstorage.ValidateExternalSnapshotDataFiles).Return(nil).Build()
				defer validate.UnPatch()
				opts := append(snapshotImportTestOptions(), &commonpb.KeyValuePair{Key: importutilv2.PartitionMapping, Value: `{"A":"X","B":"Y","empty":"Z"}`})
				files, normalized, l0Sources, err := expandSnapshotImportFiles(context.Background(), []int64{200, 100, 300}, milvusstorage.NewLocalChunkManager(), snapshotImportTestSchema(),
					"s3://source/root/snapshots/1/metadata/2.json", opts, &datapb.SnapshotMetadata{})
				require.NoError(t, err)
				require.NoError(t, importutilv2.ValidateSnapshotImportFiles(files, normalized))
				require.Len(t, files, 2)
				for i, target := range []int64{200, 100} {
					source := files[i].GetSnapshotSource()
					version := 3
					if l0 {
						version += 4
					}
					require.EqualValues(t, version, source.GetVersion())
					require.Equal(t, target, source.GetTargetPartitionId())
					require.EqualValues(t, 100, source.GetSourceCommitTimestamp())
					require.Empty(t, files[i].GetPaths())
					if l0 {
						shared, err := importutilv2.SnapshotTaskL0Source(files[i:i+1], l0Sources)
						require.NoError(t, err)
						require.ElementsMatch(t, []string{fmt.Sprintf("root/files/delete-%d", (i+1)*10), "root/files/delete--1"}, shared.GetLegacyL0Deltalogs())
					}
				}
				// A data segment outside the declared source partition inventory
				// must fail instead of falling back to a default destination.
				snapshot.Segments[0].PartitionId = 999
				_, _, _, err = expandSnapshotImportFiles(context.Background(), []int64{200, 100, 300}, milvusstorage.NewLocalChunkManager(), snapshotImportTestSchema(),
					"s3://source/root/snapshots/1/metadata/2.json", opts, &datapb.SnapshotMetadata{})
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
			read := mockey.Mock((*snapshotstorage.SnapshotReader).ReadSnapshotFromMetadata).Return(snapshot, nil).Build()
			defer read.UnPatch()
			validate := mockey.Mock(snapshotstorage.ValidateExternalSnapshotDataFiles).Return(nil).Build()
			defer validate.UnPatch()
			files, _, l0Sources, err := expandSnapshotImportFiles(context.Background(), nil, milvusstorage.NewLocalChunkManager(), snapshotImportTestSchema(),
				"s3://source/root/snapshots/1/metadata/2.json", snapshotImportTestOptions(), &datapb.SnapshotMetadata{})
			require.NoError(t, err)
			require.Len(t, files, 2)
			byManifest := make(map[string][]string)
			for _, file := range files {
				shared, err := importutilv2.SnapshotTaskL0Source([]*internalpb.ImportFile{file}, l0Sources)
				require.NoError(t, err)
				byManifest[file.GetSnapshotSource().GetManifestPath()] = shared.GetLegacyL0Deltalogs()
			}
			require.ElementsMatch(t, []string{"root/files/local-10", "root/files/global"}, byManifest[snapshot.Segments[0].GetManifestPath()])
			require.ElementsMatch(t, []string{"root/files/local-20", "root/files/global"}, byManifest[snapshot.Segments[1].GetManifestPath()])
		})
	}
}

func TestExpandSnapshotImportL0(t *testing.T) {
	patchSnapshotImportInstance(t)
	for _, layout := range []datapb.SnapshotLayout{datapb.SnapshotLayout_SnapshotLayoutReferenced, datapb.SnapshotLayout_SnapshotLayoutSelfContained} {
		for _, mode := range []string{"legacy_v1", "legacy_v2", "packed_v3", "empty_marker", "unrelated", "unrelated_channel", "missing_commit", "missing_channel", "bad_version", "bad_manifest", "read_failure", "outside_root", "oversized"} {
			t.Run(layout.String()+"/"+mode, func(t *testing.T) {
				snapshot := snapshotImportTestData(layout)
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
				case "missing_commit":
					snapshot.Segments[1].CommitTimestamp = 0
				case "unrelated":
					delta.PartitionId = 999
					snapshot.Segments[1].CommitTimestamp = 0
				case "unrelated_channel":
					delta.ChannelName = "unselected_channel"
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
				readPatch := mockey.Mock((*snapshotstorage.SnapshotReader).ReadSnapshotFromMetadata).Return(snapshot, nil).Build()
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
				files, _, l0Sources, err := expandSnapshotImportFiles(context.Background(), nil, milvusstorage.NewLocalChunkManager(), snapshotImportTestSchema(),
					"s3://source/root/snapshots/1/metadata/2.json", snapshotImportTestOptions(), &datapb.SnapshotMetadata{})
				switch mode {
				case "missing_channel", "bad_version", "bad_manifest":
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
					require.Equal(t, snapshot.Segments[1].CommitTimestamp, files[0].SnapshotSource.SourceCommitTimestamp)
					shared, bindErr := importutilv2.SnapshotTaskL0Source(files[:1], l0Sources)
					require.NoError(t, bindErr)
					require.Empty(t, files[0].SnapshotSource.LegacyL0Deltalogs)
					require.Empty(t, files[0].SnapshotSource.ManifestL0Deltalogs)
					if delta.StorageVersion == milvusstorage.StorageV3 {
						require.Equal(t, []string{delta.ManifestPath}, shared.ManifestL0Paths)
						require.Zero(t, deltaPatch.Times(), "physical validation is deferred to the worker")
					} else if mode != "empty_marker" {
						require.Equal(t, []string{deltaPath}, shared.LegacyL0Deltalogs)
					}
					require.NotNil(t, files[1].SnapshotSource, "job-wide activation survives an empty per-file L0 list")
					require.Empty(t, files[1].SnapshotSource.LegacyL0Deltalogs)
					require.Empty(t, files[1].SnapshotSource.ManifestL0Deltalogs)
				}
			})
		}
	}
}

func TestExpandSnapshotImportPathValidationLinear(t *testing.T) {
	paramtable.Init()
	patchSnapshotImportInstance(t)
	for _, layout := range []datapb.SnapshotLayout{datapb.SnapshotLayout_SnapshotLayoutReferenced, datapb.SnapshotLayout_SnapshotLayoutSelfContained} {
		for _, count := range []int{10, 100} {
			t.Run(fmt.Sprintf("%s/%d", layout, count), func(t *testing.T) {
				snapshot := snapshotImportTestData(layout)
				snapshot.Segments = nil
				for i := 0; i < count; i++ {
					snapshot.ManifestPaths = append(snapshot.ManifestPaths, fmt.Sprintf("root/snapshots/1/manifests/2/%d", i))
					snapshot.Segments = append(snapshot.Segments, &datapb.SegmentDescription{
						SegmentId: int64(i + 1), PartitionId: 10, ChannelName: "source", SegmentLevel: datapb.SegmentLevel_L1,
						StorageVersion: milvusstorage.StorageV3, ManifestPath: packed.MarshalManifestPath(fmt.Sprintf("root/files/segment/%d", i), 1),
					}, &datapb.SegmentDescription{
						SegmentId: int64(count + i + 1), PartitionId: 10, ChannelName: "source", SegmentLevel: datapb.SegmentLevel_L0,
						StorageVersion: milvusstorage.StorageV2,
						Deltalogs:      []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{LogPath: fmt.Sprintf("root/files/l0/%d", i)}}}},
					})
				}
				read := mockey.Mock((*snapshotstorage.SnapshotReader).ReadSnapshotFromMetadata).Return(snapshot, nil).Build()
				defer read.UnPatch()
				var original func(string, *snapshotstorage.SnapshotData, []snapshotstorage.SnapshotFileRef) error
				manifestVisits, refVisits := 0, 0
				validate := mockey.Mock(snapshotstorage.ValidateExternalSnapshotPaths).Origin(&original).To(func(uri string, data *snapshotstorage.SnapshotData, refs []snapshotstorage.SnapshotFileRef) error {
					manifestVisits += len(data.ManifestPaths)
					refVisits += len(refs)
					return original(uri, data, refs)
				}).Build()
				defer validate.UnPatch()
				_, _, _, err := expandSnapshotImportFiles(context.Background(), nil, milvusstorage.NewLocalChunkManager(), snapshotImportTestSchema(),
					"s3://source/root/snapshots/1/metadata/2.json", snapshotImportTestOptions(), &datapb.SnapshotMetadata{})
				require.NoError(t, err)
				require.Equal(t, count, manifestVisits, "metadata references must be scanned once regardless of data/L0 counts")
				require.Equal(t, 2*count, refVisits, "every data and legacy L0 reference must still be checked")
				for _, invalid := range []string{"metadata_root", "data_root", "data_bucket", "l0_root", "l0_bucket", "l0_scheme"} {
					t.Run(invalid, func(t *testing.T) {
						metadataPath := snapshot.ManifestPaths[0]
						dataPath := snapshot.Segments[0].ManifestPath
						deleteLog := snapshot.Segments[1].Deltalogs[0].Binlogs[0]
						deletePath := deleteLog.LogPath
						defer func() {
							snapshot.ManifestPaths[0] = metadataPath
							snapshot.Segments[0].ManifestPath = dataPath
							deleteLog.LogPath = deletePath
						}()
						switch invalid {
						case "metadata_root":
							snapshot.ManifestPaths[0] = "escape/snapshots/1/manifests/2/0"
						case "data_root":
							snapshot.Segments[0].ManifestPath = packed.MarshalManifestPath("escape/files/data", 1)
						case "data_bucket":
							snapshot.Segments[0].ManifestPath = packed.MarshalManifestPath("s3://other/root/files/data", 1)
						case "l0_root":
							deleteLog.LogPath = "escape/files/delete"
						case "l0_bucket":
							deleteLog.LogPath = "s3://other/root/files/delete"
						case "l0_scheme":
							deleteLog.LogPath = "gs://source/root/files/delete"
						}
						_, _, _, err := expandSnapshotImportFiles(context.Background(), nil, milvusstorage.NewLocalChunkManager(), snapshotImportTestSchema(),
							"s3://source/root/snapshots/1/metadata/2.json", snapshotImportTestOptions(), &datapb.SnapshotMetadata{})
						require.Error(t, err, "batching must preserve original URI and root checks")
					})
				}
			})
		}
	}
}

func TestSnapshotImportL0PlanErrors(t *testing.T) {
	for _, mode := range []string{"large_manifest", "canceled", "empty_delete", "conflicting_decoder", "invalid_path", "invalid_manifest", "oversized_deletes", "conflicting_references"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			snapshot := snapshotImportTestData(datapb.SnapshotLayout_SnapshotLayoutReferenced)
			data := snapshot.Segments[:1]
			data[0].ChannelName = "source"
			metadataPath := "root/snapshots/1/metadata/2.json"
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
			case "invalid_manifest":
				deltas[0].StorageVersion = milvusstorage.StorageV3
				deltas[0].ManifestPath = packed.MarshalManifestPath("root/files/l0", packed.ManifestLatest)
			case "oversized_deletes":
				deltas[0].Deltalogs[0].Binlogs[0].LogPath = "root/" + strings.Repeat("x", importutilv2.SnapshotSourcePlanMaxBytes)
			case "conflicting_references":
				// A bucket-root bundle allows this literal JSON object key. It
				// must not collide with an exact-manifest inventory entry.
				metadataPath = "snapshots/1/metadata/2.json"
				manifest := packed.MarshalManifestPath("files/l0", 1)
				deltas[0].Deltalogs[0].Binlogs[0].LogPath = manifest
				deltas = append(deltas, &datapb.SegmentDescription{
					ChannelName: "source", PartitionId: 10, StorageVersion: milvusstorage.StorageV3, ManifestPath: manifest,
				})
			case "conflicting_decoder":
				deltas = append(deltas, &datapb.SegmentDescription{
					ChannelName: "source", PartitionId: 10, StorageVersion: milvusstorage.StorageV3,
					ManifestPath: packed.MarshalManifestPath("root/files/l0", 1),
				})
			}
			patch := mockey.Mock(packed.GetDeltaLogPathsFromManifest).Return(paths, nil).Build()
			defer patch.UnPatch()
			_, err := attachSnapshotImportSources(ctx, milvusstorage.NewLocalChunkManager(), metadataPath,
				snapshot, data, deltas, []*internalpb.ImportFile{{}})
			if mode == "conflicting_decoder" {
				require.NoError(t, err, "the worker detects conflicts after manifest resolution")
				require.Zero(t, patch.Times())
				return
			}
			require.Error(t, err)
			if mode == "canceled" {
				require.ErrorIs(t, err, context.Canceled)
			}
		})
	}
}

func TestExpandSnapshotImportSourcePlanErrors(t *testing.T) {
	patchSnapshotImportInstance(t)
	snapshot := snapshotImportTestData(datapb.SnapshotLayout_SnapshotLayoutReferenced)
	read := mockey.Mock((*snapshotstorage.SnapshotReader).ReadSnapshotFromMetadata).Return(snapshot, nil).Build()
	defer read.UnPatch()
	for _, mode := range []string{"missing_target_schema", "unresolved_mapping"} {
		t.Run(mode, func(t *testing.T) {
			schema := snapshotImportTestSchema()
			opts := snapshotImportTestOptions()
			if mode == "missing_target_schema" {
				schema = nil
			} else {
				opts = append(opts, &commonpb.KeyValuePair{Key: importutilv2.PartitionMapping, Value: `{"source_partition":"target"}`})
			}
			files, _, sources, err := expandSnapshotImportFiles(context.Background(), nil, milvusstorage.NewLocalChunkManager(), schema,
				"s3://source/root/snapshots/1/metadata/2.json", opts, &datapb.SnapshotMetadata{})
			require.Error(t, err)
			require.Nil(t, files)
			require.Nil(t, sources)
		})
	}
}

func TestSnapshotSharedL0PlanAndTasks(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	snapshot := snapshotImportTestData(datapb.SnapshotLayout_SnapshotLayoutReferenced)
	var data []*datapb.SegmentDescription
	var files []*internalpb.ImportFile
	for i := 0; i < 300; i++ {
		data = append(data, &datapb.SegmentDescription{
			SegmentId: int64(i + 1), PartitionId: 10, ChannelName: "source", CommitTimestamp: uint64(i + 100),
			ManifestPath: packed.MarshalManifestPath(fmt.Sprintf("root/data/%d", i), 7),
		})
		files = append(files, &internalpb.ImportFile{Id: int64(i + 1)})
	}
	var paths []string
	var logs []*datapb.Binlog
	for i := 0; i < 10; i++ {
		p := fmt.Sprintf("root/%s/%d", strings.Repeat("x", 120), i)
		paths = append(paths, p)
		logs = append(logs, &datapb.Binlog{LogPath: p})
	}
	sources, err := attachSnapshotImportSources(ctx, milvusstorage.NewLocalChunkManager(), "root/snapshots/1/metadata/2.json", snapshot, data,
		[]*datapb.SegmentDescription{{
			PartitionId: common.AllPartitionsID, ChannelName: "source", StorageVersion: milvusstorage.StorageV1,
			Deltalogs: []*datapb.FieldBinlog{{Binlogs: logs}},
		}}, files)
	require.NoError(t, err, "300 segments must not replicate the same 10 L0 paths 300 times")
	require.Len(t, sources, 2, "one empty partition scope plus one channel-wide inventory")
	options := snapshotImportTestOptions()
	for _, file := range files {
		require.Empty(t, file.SnapshotSource.LegacyL0Deltalogs)
	}
	// Pending preparation persists the expanded plan directly to the catalog;
	// only captured metadata crosses the WAL/CDC boundary.
	require.NoError(t, importutilv2.ValidateSnapshotImportPlan(files, options, sources))
	encoded, err := proto.Marshal(&datapb.ImportJob{
		JobID: 1, Files: files, Options: options, Schema: snapshotImportTestSchema(),
		PartitionIDs: []int64{20}, Vchannels: []string{"target"}, DataTs: 100, SnapshotL0Sources: sources,
	})
	require.NoError(t, err)
	require.Less(t, len(encoded), importutilv2.SnapshotSourcePlanMaxBytes)
	reloaded := &datapb.ImportJob{}
	require.NoError(t, proto.Unmarshal(encoded, reloaded))
	require.Len(t, reloaded.GetSnapshotL0Sources(), len(sources))
	for i, source := range reloaded.GetSnapshotL0Sources() {
		require.True(t, proto.Equal(sources[i], source), "catalog reload must preserve source scope identities")
	}
	job := &importJob{ImportJob: reloaded}
	type sharedAllocator struct{ allocator.Allocator }
	alloc := &sharedAllocator{}
	next := int64(1000)
	allocate := mockey.Mock((*sharedAllocator).AllocN).To(func(_ *sharedAllocator, n int64) (int64, int64, error) {
		start := next
		next += n
		return start, next, nil
	}).Build()
	defer allocate.UnPatch()
	type sharedMeta struct{ ImportMeta }
	tm := &sharedMeta{}
	getJob := mockey.Mock((*sharedMeta).GetJob).Return(job).Build()
	defer getJob.UnPatch()
	groups := groupPreImportFiles(reloaded.Files, 7)
	require.Len(t, groups, 43)
	preTasks, err := NewPreImportTasks(groups, job, alloc, tm)
	require.NoError(t, err)
	var stats []*datapb.ImportFileStats
	check := func(taskFiles []*internalpb.ImportFile, shared *internalpb.SnapshotImportL0Source) {
		require.NoError(t, importutilv2.ValidateSnapshotImportTask(taskFiles, options, shared))
		require.Equal(t, paths, shared.LegacyL0Deltalogs)
		for _, file := range taskFiles {
			require.Empty(t, file.SnapshotSource.LegacyL0Deltalogs)
			require.Equal(t, uint64(file.Id+99), file.SnapshotSource.SourceCommitTimestamp)
		}
	}
	for _, task := range preTasks {
		req, err := AssemblePreImportRequest(task, job)
		require.NoError(t, err)
		check(req.GetImportFiles(), req.GetSnapshotL0Source())
		// Retry reconstructs the same task-local inventory from the durable job.
		retry, err := AssemblePreImportRequest(task, job)
		require.NoError(t, err)
		require.True(t, proto.Equal(req, retry))
		for _, file := range req.ImportFiles {
			stats = append(stats, &datapb.ImportFileStats{ImportFile: file, TotalRows: 1, TotalMemorySize: 8})
		}
	}
	importGroups := RegroupImportFiles(job, stats, 1024)
	require.Len(t, importGroups, 3, "Import retains its own size limit, not PreImport's count limit")
	for i, group := range importGroups {
		task := &importTask{importMeta: tm}
		task.task.Store(&datapb.ImportTaskV2{JobID: 1, TaskID: int64(i), FileStats: group})
		req, err := AssembleImportRequest(task, job, &meta{}, alloc)
		require.NoError(t, err)
		check(req.Files, req.SnapshotL0Source)
	}
}

func TestSnapshotSharedL0AssemblyFailure(t *testing.T) {
	paramtable.Init()
	type failedSourceMeta struct{ ImportMeta }
	tm := &failedSourceMeta{}
	job := &importJob{ImportJob: &datapb.ImportJob{JobID: 1}}
	getJob := mockey.Mock((*failedSourceMeta).GetJob).Return(job).Build()
	defer getJob.UnPatch()
	updates := 0
	updateJob := mockey.Mock((*failedSourceMeta).UpdateJob).To(func(_ *failedSourceMeta, _ context.Context, _ int64, actions ...UpdateJobAction) error {
		updates++
		for _, action := range actions {
			action(job)
		}
		return nil
	}).Build()
	defer updateJob.UnPatch()
	stats := []*datapb.ImportFileStats{{ImportFile: &internalpb.ImportFile{SnapshotSource: &internalpb.SnapshotImportSource{
		Version: 5, SourceChannel: "source", SourcePartitionId: 10,
	}}}}
	pre := &preImportTask{importMeta: tm}
	pre.task.Store(&datapb.PreImportTask{JobID: 1, FileStats: stats})
	imp := &importTask{importMeta: tm}
	imp.task.Store(&datapb.ImportTaskV2{JobID: 1, FileStats: stats})
	// No worker/allocator is supplied: malformed immutable inventories must
	// fail before dispatch/allocation, not keep the job retrying until timeout.
	pre.CreateTaskOnWorker(1, nil)
	imp.CreateTaskOnWorker(1, nil)
	require.Equal(t, 2, updates)
	require.Equal(t, internalpb.ImportJobState_Failed, job.GetState())
	require.Contains(t, job.GetReason(), "lost its shared L0 inventory")
	require.Zero(t, pre.retryTimes)
	require.Zero(t, imp.retryTimes)
	updateJob.UnPatch()
	persistFailure := mockey.Mock((*failedSourceMeta).UpdateJob).Return(merr.ErrIoKeyNotFound).Build()
	defer persistFailure.UnPatch()
	pre.CreateTaskOnWorker(1, nil)
	imp.CreateTaskOnWorker(1, nil)
	require.Zero(t, pre.retryTimes, "catalog write failures must not dispatch a malformed task")
	require.Zero(t, imp.retryTimes)
}

func TestSnapshotSharedL0SourceGrouping(t *testing.T) {
	paramtable.Init()
	var files []*internalpb.ImportFile
	var stats []*datapb.ImportFileStats
	// Interleave two channels and two source partitions, all mapped to the
	// same target: grouping must use source identity, never the target ID.
	for i := 0; i < 4; i++ {
		for _, channel := range []string{"a", "b"} {
			for _, partition := range []int64{10, 20} {
				file := &internalpb.ImportFile{SnapshotSource: &internalpb.SnapshotImportSource{
					Version: 7, SourceChannel: channel, SourcePartitionId: partition, TargetPartitionId: 100,
				}}
				files = append(files, file)
				stats = append(stats, &datapb.ImportFileStats{ImportFile: file, TotalMemorySize: 8})
			}
		}
	}
	check := func(group []*internalpb.ImportFile) {
		for _, file := range group {
			require.Equal(t, group[0].SnapshotSource.SourceChannel, file.SnapshotSource.SourceChannel)
			require.Equal(t, group[0].SnapshotSource.SourcePartitionId, file.SnapshotSource.SourcePartitionId)
		}
	}
	pre := groupPreImportFiles(files, 3)
	require.Len(t, pre, 8, "group before chunking: each source scope splits as 3+1")
	for i, group := range pre {
		check(group)
		require.Len(t, group, []int{3, 1}[i%2])
	}
	job := &importJob{ImportJob: &datapb.ImportJob{PartitionIDs: []int64{100}, Vchannels: []string{"target"}}}
	groups := RegroupImportFiles(job, stats, 16)
	require.Len(t, groups, 8)
	for _, group := range groups {
		require.Len(t, group, 2)
		check([]*internalpb.ImportFile{group[0].ImportFile, group[1].ImportFile})
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
	files, err := bindSnapshotImportSources(decoded.MustBody().GetFiles(), decoded.Header().GetSnapshotSources())
	require.NoError(t, err)
	require.NoError(t, importutilv2.ValidateSnapshotImportPlan(files, opts, nil))
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
		req, err := AssemblePreImportRequest(task, job)
		require.NoError(t, err)
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
		{Version: 1, ManifestPath: packed.MarshalManifestPath("root/data/1", 7), SourceCommitTimestamp: 300},
		{Version: 1, ManifestPath: packed.MarshalManifestPath("root/data/2", 8)},
	}
	wal := message.NewImportMessageBuilderV1().WithHeader(&message.ImportMessageHeader{SnapshotSources: sources}).
		WithBody(&msgpb.ImportMsg{PartitionIDs: []int64{20, 10}, Files: []*msgpb.ImportFile{{Id: 1}, {Id: 2}}}).WithBroadcast([]string{"target_v1"}).MustBuildBroadcast()
	decoded, err := message.AsBroadcastImportMessageV1(message.NewBroadcastMutableMessageBeforeAppend(wal.Payload(), wal.Properties().ToRawMap()))
	require.NoError(t, err)
	files, err := bindSnapshotImportSources(decoded.MustBody().GetFiles(), decoded.Header().GetSnapshotSources())
	require.NoError(t, err)
	for i, file := range files {
		require.NotSame(t, decoded.Header().GetSnapshotSources()[i], file.SnapshotSource, "binding must not share mutable WAL descriptors")
	}
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
	for _, headers := range [][]*internalpb.SnapshotImportSource{sources[:1], {sources[0], nil}} {
		_, err := bindSnapshotImportSources(decoded.MustBody().GetFiles(), headers)
		require.ErrorIs(t, err, merr.ErrServiceInternal)
	}
	// Missing descriptors are valid for ordinary imports. Whether snapshot
	// options require them is checked by job creation, not by the WAL binder.
	files, err = bindSnapshotImportSources([]*msgpb.ImportFile{{Id: 1, Paths: []string{"legacy"}}}, nil)
	require.NoError(t, err)
	require.Equal(t, []*internalpb.ImportFile{{Id: 1, Paths: []string{"legacy"}}}, files)
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
	for _, mode := range []string{"valid", "preparation", "shared_without_preparation", "inline_legacy", "inline_manifest", "lost", "unknown", "mixed_paths", "nil_file", "binding_failure", "bad_timeout"} {
		t.Run(mode, func(t *testing.T) {
			req := &internalpb.ImportRequestInternal{
				JobID: 12, CollectionID: 1, PartitionIDs: []int64{10},
				Options: snapshotImportTestOptions(), Files: []*internalpb.ImportFile{{SnapshotSource: &internalpb.SnapshotImportSource{
					Version: 1, ManifestPath: packed.MarshalManifestPath("root/segment", 1), SourceCommitTimestamp: 300,
				}}},
			}
			var bindingErr error
			switch mode {
			case "preparation":
				metadata, err := proto.Marshal(&datapb.SnapshotMetadata{
					FormatVersion: int32(snapshotstorage.SnapshotFormatVersion),
					SnapshotInfo:  &datapb.SnapshotInfo{Id: 2, CollectionId: 1},
					Collection:    &datapb.CollectionDescription{Schema: snapshotImportTestSchema()},
					Layout:        datapb.SnapshotLayout_SnapshotLayoutReferenced,
				})
				require.NoError(t, err)
				req.Files[0].SnapshotSource = &internalpb.SnapshotImportSource{
					Version: importutilv2.SnapshotPreparationVersion, SnapshotMetadata: metadata,
				}
				req.Options = append(req.Options,
					&commonpb.KeyValuePair{Key: importutilv2.SnapshotSourceURI, Value: "s3://source/root/snapshots/1/metadata/2.json"},
					&commonpb.KeyValuePair{Key: importutilv2.SnapshotLayout, Value: "referenced"})
			case "inline_legacy":
				req.Files[0].SnapshotSource.LegacyL0Deltalogs = []string{"root/delete"}
			case "inline_manifest":
				req.Files[0].SnapshotSource.ManifestL0Deltalogs = []string{"root/delete"}
			case "shared_without_preparation":
				req.Files[0].SnapshotSource.Version = 5
				req.Files[0].SnapshotSource.SourceChannel = "source"
				req.Files[0].SnapshotSource.SourcePartitionId = 10
			case "lost":
				req.Files[0].SnapshotSource = nil
			case "unknown":
				req.Files[0].SnapshotSource.Version = 99
			case "mixed_paths":
				req.Files[0].Paths = []string{"legacy"}
			case "nil_file":
				req.Files[0] = nil
			case "binding_failure":
				bindingErr = merr.WrapErrServiceInternalMsg("descriptor cardinality mismatch")
				req.Files[0].SnapshotSource.Version = 99
			case "bad_timeout":
				req.Options = append(req.Options, &commonpb.KeyValuePair{Key: "timeout", Value: "invalid"})
			}
			resp, err := server.createImportJobFromAck(context.Background(), req, bindingErr)
			require.NoError(t, merr.CheckRPCCall(resp, err), "a malformed durable source must not retry the ACK forever")
			require.Equal(t, "12", resp.JobID)
			if mode != "nil_file" && mode != "binding_failure" {
				var sources []*internalpb.SnapshotImportSource
				if source := req.Files[0].SnapshotSource; source != nil {
					sources = []*internalpb.SnapshotImportSource{source}
				}
				wal := message.NewImportMessageBuilderV1().WithHeader(&message.ImportMessageHeader{
					SnapshotSources: sources,
				}).WithBody(&msgpb.ImportMsg{
					JobID: 12, CollectionID: 1, Schema: snapshotImportTestSchema(), Files: []*msgpb.ImportFile{{Paths: req.Files[0].Paths}},
					Options: funcutil.KeyValuePair2Map(req.Options),
				}).WithBroadcast([]string{"target_v1"}).MustBuildBroadcast()
				callback := &DDLCallbacks{Server: server}
				require.NoError(t, callback.importV1AckCallback(context.Background(), message.BroadcastResultImportMessageV1{
					Message: message.MustAsBroadcastImportMessageV1(wal), Results: map[string]*message.AppendResult{"target_v1": {TimeTick: 100}},
				}))
			}
			require.NotNil(t, saved)
			if mode == "valid" || mode == "preparation" {
				require.Equal(t, internalpb.ImportJobState_Pending, saved.GetState())
				require.Empty(t, saved.GetSnapshotL0Sources(), "L0 inventories are produced only by Pending preparation")
				if mode == "preparation" {
					require.True(t, importutilv2.IsSnapshotPreparation(saved.GetFiles()))
					require.True(t, proto.Equal(req.Files[0].SnapshotSource, saved.GetFiles()[0].GetSnapshotSource()))
				} else {
					require.EqualValues(t, 300, saved.GetFiles()[0].GetSnapshotSource().GetSourceCommitTimestamp())
				}
			} else {
				require.Equal(t, internalpb.ImportJobState_Failed, saved.GetState())
				require.NotEmpty(t, saved.GetReason())
				if strings.HasPrefix(mode, "inline_") {
					require.Contains(t, saved.GetReason(), "inline L0 are no longer supported")
				}
				if mode == "shared_without_preparation" {
					require.Contains(t, saved.GetReason(), "lost its shared L0 inventory")
				}
				if bindingErr != nil {
					require.Equal(t, bindingErr.Error(), saved.GetReason(), "binding errors take precedence over plan validation")
				}
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
		SnapshotInfo: &datapb.SnapshotInfo{Id: 2, CollectionId: 1},
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
				ManifestPath:   packed.MarshalManifestPath("root/files/segment/20", 8),
			},
			{
				SegmentId:      10,
				PartitionId:    10,
				SegmentLevel:   datapb.SegmentLevel_L1,
				StorageVersion: milvusstorage.StorageV3,
				ManifestPath:   packed.MarshalManifestPath("root/files/segment/10", 7),
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

func TestPrepareSnapshotImportOptions_Encryption(t *testing.T) {
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
			normalized, err := prepareSnapshotImportOptions(snapshotImportTestSchema(), schema, options)
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

func TestExpandSnapshotImportCapturedMetadata(t *testing.T) {
	paramtable.Init()
	patchSnapshotImportInstance(t)
	type sourceCM struct{ milvusstorage.ChunkManager }
	for _, layout := range []datapb.SnapshotLayout{
		datapb.SnapshotLayout_SnapshotLayoutReferenced,
		datapb.SnapshotLayout_SnapshotLayoutSelfContained,
	} {
		t.Run(layout.String(), func(t *testing.T) {
			ctx := context.Background()
			uri := "s3://source/root/snapshots/1/metadata/2.json"
			snapshot := snapshotImportTestData(layout)
			metadata := &datapb.SnapshotMetadata{
				FormatVersion: int32(snapshotstorage.SnapshotFormatVersion),
				SnapshotInfo:  snapshot.SnapshotInfo,
				Collection:    snapshot.Collection,
				Layout:        layout,
			}
			objects := make(map[string][]byte)
			for _, segment := range snapshot.Segments {
				key := fmt.Sprintf("root/snapshots/1/manifests/2/%d", segment.SegmentId)
				payload, err := snapshotio.MarshalSegmentManifest(segment)
				require.NoError(t, err)
				objects[key] = payload
				metadata.ManifestList = append(metadata.ManifestList, key)
				metadata.SegmentIds = append(metadata.SegmentIds, segment.SegmentId)
				metadata.Storagev2ManifestList = append(metadata.Storagev2ManifestList, &datapb.StorageV2SegmentManifest{
					SegmentId: segment.SegmentId, Manifest: segment.ManifestPath,
				})
			}
			original := proto.Clone(metadata)
			// Only segment descriptors remain accessible. The real reader must
			// use the captured metadata, including on a preparation retry.
			var reads []string
			read := mockey.Mock((*sourceCM).Read).To(func(_ *sourceCM, _ context.Context, key string) ([]byte, error) {
				reads = append(reads, key)
				payload, ok := objects[key]
				if !ok {
					return nil, merr.ErrIoKeyNotFound
				}
				return payload, nil
			}).Build()
			defer read.UnPatch()
			_, _, _, err := expandSnapshotImportFiles(ctx, nil, &sourceCM{}, snapshotImportTestSchema(), uri, snapshotImportTestOptions(), nil)
			require.ErrorIs(t, err, merr.ErrDataIntegrity)
			require.Empty(t, reads, "missing captured metadata must not fall back to storage")

			for attempt := 0; attempt < 2; attempt++ {
				result, _, _, err := expandSnapshotImportFiles(ctx, nil, &sourceCM{}, snapshotImportTestSchema(), uri, snapshotImportTestOptions(), metadata)
				require.NoError(t, err)
				require.Len(t, result, 2)
				require.Equal(t, []string{snapshot.Segments[1].ManifestPath}, result[0].Paths)
				require.Equal(t, []string{snapshot.Segments[0].ManifestPath}, result[1].Paths)
				require.Equal(t, metadata.ManifestList, reads)
				require.True(t, proto.Equal(original, metadata), "expansion must not mutate captured metadata")
				reads = nil
			}

			delete(objects, metadata.ManifestList[0])
			_, _, _, err = expandSnapshotImportFiles(ctx, nil, &sourceCM{}, snapshotImportTestSchema(), uri, snapshotImportTestOptions(), metadata)
			require.ErrorIs(t, err, merr.ErrIoKeyNotFound, "segment read failures must still propagate")
		})
	}
}

func TestExpandSnapshotImportFiles(t *testing.T) {
	patchSnapshotImportInstance(t)
	ctx := context.Background()
	cm := milvusstorage.NewLocalChunkManager()
	metadataPath := "s3://source/root/snapshots/1/metadata/2.json"
	targetSchema := snapshotImportTestSchema()

	for _, layout := range []datapb.SnapshotLayout{
		datapb.SnapshotLayout_SnapshotLayoutReferenced,
		datapb.SnapshotLayout_SnapshotLayoutSelfContained,
	} {
		t.Run(layout.String(), func(t *testing.T) {
			snapshot := snapshotImportTestData(layout)
			readPatch := mockey.Mock((*snapshotstorage.SnapshotReader).ReadSnapshotFromMetadata).
				Return(snapshot, nil).Build()
			defer readPatch.UnPatch()
			validatePatch := mockey.Mock(snapshotstorage.ValidateExternalSnapshotDataFiles).
				Return(nil).Build()
			defer validatePatch.UnPatch()

			result, _, _, err := expandSnapshotImportFiles(
				ctx,
				nil,
				cm,
				targetSchema,
				metadataPath,
				snapshotImportTestOptions(),
				&datapb.SnapshotMetadata{},
			)
			require.NoError(t, err)
			require.Len(t, result, 2)
			assert.Equal(t, snapshot.Segments[1].GetManifestPath(), result[0].GetPaths()[0])
			assert.Equal(t, snapshot.Segments[0].GetManifestPath(), result[1].GetPaths()[0])
		})
	}

	t.Run("implicit unique partition", func(t *testing.T) {
		snapshot := snapshotImportTestData(datapb.SnapshotLayout_SnapshotLayoutReferenced)
		readPatch := mockey.Mock((*snapshotstorage.SnapshotReader).ReadSnapshotFromMetadata).
			Return(snapshot, nil).Build()
		defer readPatch.UnPatch()
		validatePatch := mockey.Mock(snapshotstorage.ValidateExternalSnapshotDataFiles).
			Return(nil).Build()
		defer validatePatch.UnPatch()

		result, _, _, err := expandSnapshotImportFiles(
			ctx,
			nil,
			cm,
			targetSchema,
			metadataPath,
			snapshotImportTestOptions(),
			&datapb.SnapshotMetadata{},
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
			ManifestPath:   packed.MarshalManifestPath("root/files/segment/30", 9),
		})
		readPatch := mockey.Mock((*snapshotstorage.SnapshotReader).ReadSnapshotFromMetadata).
			Return(snapshot, nil).Build()
		defer readPatch.UnPatch()

		validatePatch := mockey.Mock(snapshotstorage.ValidateExternalSnapshotDataFiles).Return(nil).Build()
		defer validatePatch.UnPatch()
		files, _, _, err := expandSnapshotImportFiles(
			ctx,
			nil,
			cm,
			targetSchema,
			metadataPath,
			snapshotImportTestOptions(),
			&datapb.SnapshotMetadata{},
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
		readPatch := mockey.Mock((*snapshotstorage.SnapshotReader).ReadSnapshotFromMetadata).
			Return(snapshot, nil).Build()
		defer readPatch.UnPatch()
		validatePatch := mockey.Mock(snapshotstorage.ValidateExternalSnapshotDataFiles).
			Return(nil).Build()
		defer validatePatch.UnPatch()

		options := append(snapshotImportTestOptions(), &commonpb.KeyValuePair{
			Key: importutilv2.EZK, Value: snapshotImportTestEZK(10),
		})
		result, normalized, _, err := expandSnapshotImportFiles(
			ctx,
			nil,
			cm,
			targetSchema,
			metadataPath,
			options,
			&datapb.SnapshotMetadata{},
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
		readPatch := mockey.Mock((*snapshotstorage.SnapshotReader).ReadSnapshotFromMetadata).
			Return(snapshot, nil).Build()
		defer readPatch.UnPatch()

		_, _, _, err := expandSnapshotImportFiles(
			ctx,
			nil,
			cm,
			targetSchema,
			metadataPath,
			snapshotImportTestOptions(),
			&datapb.SnapshotMetadata{},
		)
		assert.ErrorIs(t, err, merr.ErrImportFailed)
		assert.ErrorContains(t, err, "requires ezk")

		options := append(snapshotImportTestOptions(), &commonpb.KeyValuePair{
			Key: importutilv2.EZK, Value: snapshotImportTestEZK(11),
		})
		_, _, _, err = expandSnapshotImportFiles(
			ctx,
			nil,
			cm,
			targetSchema,
			metadataPath,
			options,
			&datapb.SnapshotMetadata{},
		)
		assert.ErrorIs(t, err, merr.ErrImportFailed)
		assert.ErrorContains(t, err, "source requires zone 10")
	})

	t.Run("plaintext text source ignores malformed ezk", func(t *testing.T) {
		snapshot := snapshotImportTestData(datapb.SnapshotLayout_SnapshotLayoutReferenced)
		snapshot.Collection.Schema.Fields = append(snapshot.Collection.Schema.Fields, &schemapb.FieldSchema{
			FieldID: 102, Name: "body", DataType: schemapb.DataType_Text,
		})
		readPatch := mockey.Mock((*snapshotstorage.SnapshotReader).ReadSnapshotFromMetadata).
			Return(snapshot, nil).Build()
		defer readPatch.UnPatch()
		validatePatch := mockey.Mock(snapshotstorage.ValidateExternalSnapshotDataFiles).Return(nil).Build()
		defer validatePatch.UnPatch()

		options := append(snapshotImportTestOptions(), &commonpb.KeyValuePair{
			Key: importutilv2.EZK, Value: "not-base64",
		})
		files, normalized, _, err := expandSnapshotImportFiles(
			ctx,
			nil,
			cm,
			snapshot.Collection.Schema,
			metadataPath,
			options,
			&datapb.SnapshotMetadata{},
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
		readPatch := mockey.Mock((*snapshotstorage.SnapshotReader).ReadSnapshotFromMetadata).
			Return(snapshot, nil).Build()
		defer readPatch.UnPatch()

		targetWithText := proto.Clone(targetSchema).(*schemapb.CollectionSchema)
		targetWithText.Fields = append(targetWithText.Fields, proto.Clone(textField).(*schemapb.FieldSchema))
		options := append(snapshotImportTestOptions(), &commonpb.KeyValuePair{
			Key: importutilv2.EZK, Value: snapshotImportTestEZK(10),
		})
		_, _, _, err := expandSnapshotImportFiles(
			ctx,
			nil,
			cm,
			targetWithText,
			metadataPath,
			options,
			&datapb.SnapshotMetadata{},
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
				snapshot.Segments[0].ManifestPath = packed.MarshalManifestPath("root/files/segment/20", packed.ManifestLatest)
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
			readPatch := mockey.Mock((*snapshotstorage.SnapshotReader).ReadSnapshotFromMetadata).
				Return(snapshot, nil).Build()
			defer readPatch.UnPatch()

			_, _, _, err := expandSnapshotImportFiles(
				ctx,
				nil,
				cm,
				targetSchema,
				metadataPath,
				snapshotImportTestOptions(),
				&datapb.SnapshotMetadata{},
			)
			assert.ErrorIs(t, err, test.expected)
			assert.ErrorContains(t, err, test.message)
		})
	}
}

func TestPrepareSnapshotImportOptions_Schema(t *testing.T) {
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
	_, err := prepareSnapshotImportOptions(target, source, snapshotImportTestOptions())
	assert.NoError(t, err)

	source.Fields[1].Nullable = true
	_, err = prepareSnapshotImportOptions(target, source, snapshotImportTestOptions())
	assert.NoError(t, err, "backup import applies the target schema rather than requiring equality")
	source.Fields[0].AutoID = true
	source.Fields[1].IsPartitionKey = true
	source.Fields[1].IsClusteringKey = true
	source.Fields[1].DefaultValue = &schemapb.ValueField{Data: &schemapb.ValueField_StringData{StringData: "source"}}
	source.EnableDynamicField = true
	source.Fields = append(source.Fields, &schemapb.FieldSchema{FieldID: 102, Name: "source_only", DataType: schemapb.DataType_Int64})
	target.Fields = append(target.Fields, &schemapb.FieldSchema{FieldID: 103, Name: "target_only", DataType: schemapb.DataType_Int64, Nullable: true})
	_, err = prepareSnapshotImportOptions(target, source, snapshotImportTestOptions())
	assert.NoError(t, err)
	_, err = prepareSnapshotImportOptions(nil, source, snapshotImportTestOptions())
	assert.ErrorIs(t, err, merr.ErrImportSysFailed)
	_, err = prepareSnapshotImportOptions(target, nil, snapshotImportTestOptions())
	assert.ErrorIs(t, err, merr.ErrImportFailed)
	_, err = prepareSnapshotImportOptions(nil, nil, snapshotImportTestOptions())
	assert.ErrorContains(t, err, "snapshot source schema is missing", "source errors must precede target errors")

	for _, side := range []string{"source", "target"} {
		for _, mode := range []string{"field", "source", "spec"} {
			t.Run(side+"_external_"+mode, func(t *testing.T) {
				source, target := snapshotImportTestSchema(), snapshotImportTestSchema()
				external := source
				if side == "target" {
					external = target
				}
				switch mode {
				case "field":
					external.Fields[0].ExternalField = "pk"
				case "source":
					external.ExternalSource = "s3://bucket/data"
				case "spec":
					external.ExternalSpec = `{}`
				}
				_, err := prepareSnapshotImportOptions(target, source, snapshotImportTestOptions())
				require.ErrorIs(t, err, merr.ErrOperationNotSupported)
			})
		}
	}
}

func TestPrepareSnapshotImportOptions_FieldIdentity(t *testing.T) {
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
			_, err := prepareSnapshotImportOptions(target, source, snapshotImportTestOptions())
			require.NoError(t, err, "backup import uses physical IDs, not source field names")
			require.True(t, proto.Equal(before, source), "validation must not mutate snapshot metadata")
		})
	}
}

func TestExpandSnapshotImportFiles_AdmissionFailures(t *testing.T) {
	patchSnapshotImportInstance(t)
	for _, mode := range []string{
		"invalid_options", "nil_cm", "empty_uri", "bad_object_path", "nil_snapshot",
		"metadata_identity", "layout", "nil_schema", "nil_segment", "too_many_segments",
		"missing_manifest", "invalid_manifest", "duplicate_manifest", "invalid_files",
	} {
		t.Run(mode, func(t *testing.T) {
			snapshot := snapshotImportTestData(datapb.SnapshotLayout_SnapshotLayoutReferenced)
			var cm milvusstorage.ChunkManager = milvusstorage.NewLocalChunkManager()
			metadataURI := "s3://source/root/snapshots/1/metadata/2.json"
			options := snapshotImportTestOptions()
			var validationErr error
			wantErr := merr.ErrImportFailed
			switch mode {
			case "invalid_options":
				options = append(options, &commonpb.KeyValuePair{Key: "storage_version", Value: "3"})
			case "nil_cm":
				cm, wantErr = nil, merr.ErrServiceInternal
			case "empty_uri":
				metadataURI, wantErr = "  ", merr.ErrParameterInvalid
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
			read := mockey.Mock((*snapshotstorage.SnapshotReader).ReadSnapshotFromMetadata).Return(snapshot, nil).Build()
			defer read.UnPatch()
			validate := mockey.Mock(snapshotstorage.ValidateExternalSnapshotDataFiles).Return(validationErr).Build()
			defer validate.UnPatch()
			result, _, _, err := expandSnapshotImportFiles(context.Background(), nil, cm, snapshotImportTestSchema(), metadataURI, options, &datapb.SnapshotMetadata{})
			if mode == "invalid_files" {
				require.NoError(t, err, "physical validation is performed in PreImport")
				require.Len(t, result, 2)
				require.Zero(t, validate.Times())
				return
			}
			require.ErrorIs(t, err, wantErr)
			require.Nil(t, result)
		})
	}
}

func TestExpandSnapshotImportCommitTimestamps(t *testing.T) {
	patchSnapshotImportInstance(t)
	for _, layout := range []datapb.SnapshotLayout{datapb.SnapshotLayout_SnapshotLayoutReferenced, datapb.SnapshotLayout_SnapshotLayoutSelfContained} {
		for _, tc := range []struct {
			name    string
			commit  uint64
			deletes bool
			l0      bool
		}{
			{name: "zero_without_deletes"},
			{name: "nonzero_without_deletes", commit: 300},
			{name: "zero_with_manifest_deletes", deletes: true},
			{name: "nonzero_with_manifest_deletes", commit: 300, deletes: true},
			{name: "zero_with_l0", deletes: true, l0: true},
			{name: "nonzero_with_l0", commit: 300, deletes: true, l0: true},
			{name: "external_source"},
			{name: "partition_mapping"},
		} {
			t.Run(layout.String()+"/"+tc.name, func(t *testing.T) {
				snapshot := snapshotImportTestData(layout)
				snapshot.Segments[1].CommitTimestamp = tc.commit
				for _, segment := range snapshot.Segments {
					segment.ChannelName = "source"
				}
				if tc.l0 {
					snapshot.Segments = append(snapshot.Segments, &datapb.SegmentDescription{
						SegmentId: 30, PartitionId: 10, ChannelName: "source", SegmentLevel: datapb.SegmentLevel_L0,
						StorageVersion: milvusstorage.StorageV3, ManifestPath: packed.MarshalManifestPath("root/files/l0", 1),
					})
				}
				options := snapshotImportTestOptions()
				var targetPartitions []int64
				if tc.name == "external_source" {
					options = append(options, &commonpb.KeyValuePair{Key: importutilv2.ExternalSpec, Value: `{"extfs":{"region":"us-east-1"}}`})
					resolve := mockey.Mock(snapshotstorage.ResolveSnapshotReadStorage).Return(&snapshotstorage.ResolvedForeignStorage{
						ForeignCM:            milvusstorage.NewRemoteChunkManagerForTesting(nil, "source", "root"),
						ForeignStorageConfig: &indexpb.StorageConfig{BucketName: "source"},
					}, nil).Build()
					defer resolve.UnPatch()
				}
				if tc.name == "partition_mapping" {
					options = append(options, &commonpb.KeyValuePair{Key: importutilv2.PartitionMapping, Value: `{"source_partition":"target"}`})
					targetPartitions = []int64{200}
				}
				read := mockey.Mock((*snapshotstorage.SnapshotReader).ReadSnapshotFromMetadata).Return(snapshot, nil).Build()
				defer read.UnPatch()
				fileChecks, manifestChecks := 0, 0
				validate := mockey.Mock(snapshotstorage.ValidateExternalSnapshotDataFiles).To(func(context.Context, milvusstorage.ChunkManager, string, *snapshotstorage.SnapshotData, *indexpb.StorageConfig) error {
					fileChecks++
					return nil
				}).Build()
				defer validate.UnPatch()
				var paths []string
				if tc.deletes {
					paths = []string{"root/files/delta"}
				}
				delta := mockey.Mock(packed.GetDeltaLogPathsFromManifest).To(func(string, *indexpb.StorageConfig) ([]string, error) {
					manifestChecks++
					return paths, nil
				}).Build()
				defer delta.UnPatch()
				files, _, l0Sources, err := expandSnapshotImportFiles(context.Background(), targetPartitions, milvusstorage.NewLocalChunkManager(), snapshotImportTestSchema(),
					"s3://source/root/snapshots/1/metadata/2.json", options, &datapb.SnapshotMetadata{})
				require.NoError(t, err)
				if tc.l0 {
					require.Zero(t, manifestChecks, "L0 manifests are resolved by the worker")
					require.Len(t, l0Sources, 1)
				} else {
					require.Zero(t, manifestChecks, "do not probe data manifests to infer missing commit timestamps")
					require.Empty(t, l0Sources)
				}
				require.Zero(t, fileChecks)
				require.Len(t, files, 2)
				if tc.commit == 0 && !tc.l0 && tc.name != "external_source" && tc.name != "partition_mapping" {
					require.Nil(t, files[0].SnapshotSource)
					require.Len(t, files[0].Paths, 1)
				} else {
					require.Empty(t, files[0].Paths)
					require.EqualValues(t, tc.commit, files[0].GetSnapshotSource().GetSourceCommitTimestamp())
					require.NotNil(t, files[1].SnapshotSource, "timestamp context uses uniform job-wide descriptors")
				}
				if tc.name == "external_source" {
					require.EqualValues(t, 2, files[0].GetSnapshotSource().GetVersion())
				}
				if tc.name == "partition_mapping" {
					require.EqualValues(t, 200, files[0].GetSnapshotSource().GetTargetPartitionId())
				}
			})
		}
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
	validate := mockey.Mock(snapshotstorage.ValidateExternalSnapshotPaths).Return(nil).Build()
	defer validate.UnPatch()
	for _, layout := range []datapb.SnapshotLayout{datapb.SnapshotLayout_SnapshotLayoutReferenced, datapb.SnapshotLayout_SnapshotLayoutSelfContained} {
		for _, external := range []bool{false, true} {
			for _, tc := range []struct {
				name   string
				commit uint64
			}{
				{"captured_commit", 300},
				{"old_missing_commit", 0},
			} {
				t.Run(fmt.Sprintf("%s/external=%t/%s", layout, external, tc.name), func(t *testing.T) {
					snapshot := snapshotImportTestData(layout)
					snapshot.Collection.Schema = schema
					snapshot.Segments = []*datapb.SegmentDescription{{
						SegmentId: 20, PartitionId: 10, SegmentLevel: datapb.SegmentLevel_L1,
						StorageVersion: milvusstorage.StorageV3, ManifestPath: manifest, CommitTimestamp: tc.commit,
					}}
					read := mockey.Mock((*snapshotstorage.SnapshotReader).ReadSnapshotFromMetadata).Return(snapshot, nil).Build()
					defer read.UnPatch()
					uri := "s3://source/root/snapshots/1/metadata/2.json"
					options := snapshotImportTestOptions()
					if external {
						options = append(options, &commonpb.KeyValuePair{Key: importutilv2.ExternalSpec, Value: `{"extfs":{"region":"us-east-1"}}`})
					}
					files, _, _, err := expandSnapshotImportFiles(ctx, nil, cm, schema, uri, options, &datapb.SnapshotMetadata{})
					require.NoError(t, err)
					require.Len(t, files, 1)
					require.EqualValues(t, tc.commit, files[0].GetSnapshotSource().GetSourceCommitTimestamp())
					require.Empty(t, files[0].GetSnapshotSource().GetLegacyL0Deltalogs())
					require.Empty(t, files[0].GetSnapshotSource().GetManifestL0Deltalogs())
					if external {
						require.EqualValues(t, 2, files[0].GetSnapshotSource().GetVersion())
						options = append(options, &commonpb.KeyValuePair{Key: importutilv2.SnapshotSourceURI, Value: uri})
					}
					var sources []*internalpb.SnapshotImportSource
					if source := files[0].GetSnapshotSource(); source != nil {
						sources = []*internalpb.SnapshotImportSource{source}
					}
					bound, err := bindSnapshotImportSources([]*msgpb.ImportFile{{Paths: files[0].GetPaths()}}, sources)
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
						if tc.commit == 0 {
							// Accepted historical loss: all deletes are newer than raw
							// rowTs=100. The omitted commitTs=300 cannot be recovered,
							// so rows 1/2 are filtered too, unlike a complete snapshot.
							require.Zero(t, batch.GetRowNum())
						} else {
							require.Equal(t, []int64{1, 2}, batch.Data[100].(*milvusstorage.Int64FieldData).Data,
								"keep pre-commit and equal-commit deletes; apply post-commit deletes in both phases")
						}
						_, err = reader.Read()
						require.ErrorIs(t, err, io.EOF)
						reader.Close()
					}
				})
			}
		}
	}
}
