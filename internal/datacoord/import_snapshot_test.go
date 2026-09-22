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
	"strings"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/snapshotio"
	snapshotstorage "github.com/milvus-io/milvus/internal/snapshotio/storage"
	milvusstorage "github.com/milvus-io/milvus/internal/storage"
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
				require.NoError(t, importutilv2.ValidateSnapshotImportPlan(files, options))
				require.ErrorIs(t, importutilv2.ValidateSnapshotImportTask(files, options), merr.ErrServiceUnimplemented)
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
			files, _, err := expandSnapshotImportFiles(context.Background(), nil, milvusstorage.NewLocalChunkManager(), snapshotImportTestSchema(),
				uri, snapshotImportTestOptions(), &datapb.SnapshotMetadata{})
			require.ErrorIs(t, err, merr.ErrParameterInvalid)
			require.Nil(t, files)
			require.Zero(t, readCalls, "invalid source identity must fail before metadata IO")
		})
	}
	options := append(snapshotImportTestOptions(), &commonpb.KeyValuePair{Key: importutilv2.ExternalSpec, Value: `{"extfs":{"region":"us-east-1"}}`})
	_, _, err := expandSnapshotImportFiles(context.Background(), nil, milvusstorage.NewLocalChunkManager(), snapshotImportTestSchema(),
		"root/snapshots/1/metadata/2.json", options, &datapb.SnapshotMetadata{})
	require.ErrorIs(t, err, merr.ErrParameterInvalid, "extfs must not permit bare metadata keys either")
	require.Zero(t, readCalls)

	_, _, err = expandSnapshotImportFiles(context.Background(), nil, milvusstorage.NewLocalChunkManager(), snapshotImportTestSchema(),
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
			files, _, err := expandSnapshotImportFiles(context.Background(), nil, milvusstorage.NewLocalChunkManager(), schema,
				"s3://source/root/snapshots/1/metadata/2.json", opts, &datapb.SnapshotMetadata{})
			require.Error(t, err)
			require.Nil(t, files)
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

func TestSnapshotL0RequiresFolding(t *testing.T) {
	paramtable.Init()
	patchSnapshotImportInstance(t)
	snapshot := snapshotImportTestData(datapb.SnapshotLayout_SnapshotLayoutReferenced)
	snapshot.Segments = append(snapshot.Segments, &datapb.SegmentDescription{
		SegmentId: 30, PartitionId: 10, SegmentLevel: datapb.SegmentLevel_L0,
		StorageVersion: milvusstorage.StorageV2,
	})
	read := mockey.Mock((*snapshotstorage.SnapshotReader).ReadSnapshotFromMetadata).Return(snapshot, nil).Build()
	defer read.UnPatch()
	files, _, err := expandSnapshotImportFiles(context.Background(), nil,
		milvusstorage.NewLocalChunkManager(), snapshotImportTestSchema(),
		"s3://source/root/snapshots/1/metadata/2.json", snapshotImportTestOptions(), &datapb.SnapshotMetadata{})
	require.Nil(t, files)
	require.ErrorIs(t, err, merr.ErrOperationNotSupported)
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
			_, _, err := expandSnapshotImportFiles(ctx, nil, &sourceCM{}, snapshotImportTestSchema(), uri, snapshotImportTestOptions(), nil)
			require.ErrorIs(t, err, merr.ErrDataIntegrity)
			require.Empty(t, reads, "missing captured metadata must not fall back to storage")

			for attempt := 0; attempt < 2; attempt++ {
				result, _, err := expandSnapshotImportFiles(ctx, nil, &sourceCM{}, snapshotImportTestSchema(), uri, snapshotImportTestOptions(), metadata)
				require.NoError(t, err)
				require.Len(t, result, 2)
				require.Equal(t, []string{snapshot.Segments[1].ManifestPath}, result[0].Paths)
				require.Equal(t, []string{snapshot.Segments[0].ManifestPath}, result[1].Paths)
				require.Equal(t, metadata.ManifestList, reads)
				require.True(t, proto.Equal(original, metadata), "expansion must not mutate captured metadata")
				reads = nil
			}

			delete(objects, metadata.ManifestList[0])
			_, _, err = expandSnapshotImportFiles(ctx, nil, &sourceCM{}, snapshotImportTestSchema(), uri, snapshotImportTestOptions(), metadata)
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

			result, _, err := expandSnapshotImportFiles(
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

		result, _, err := expandSnapshotImportFiles(
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
		files, _, err := expandSnapshotImportFiles(
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
		result, normalized, err := expandSnapshotImportFiles(
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

		_, _, err := expandSnapshotImportFiles(
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
		_, _, err = expandSnapshotImportFiles(
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
		files, normalized, err := expandSnapshotImportFiles(
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
		_, _, err := expandSnapshotImportFiles(
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
			name: "reject l0 before folding is supported",
			mutate: func(snapshot *snapshotstorage.SnapshotData) {
				snapshot.Segments[0].SegmentLevel = datapb.SegmentLevel_L0
			},
			expected: merr.ErrOperationNotSupported,
			message:  "does not yet support L0 deletes",
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

			_, _, err := expandSnapshotImportFiles(
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
			result, _, err := expandSnapshotImportFiles(context.Background(), nil, cm, snapshotImportTestSchema(), metadataURI, options, &datapb.SnapshotMetadata{})
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
