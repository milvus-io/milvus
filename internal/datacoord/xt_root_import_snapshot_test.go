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

	"encoding/json"
	"fmt"

	"math"

	"strings"
	"testing"

	"github.com/bytedance/mockey"

	"github.com/stretchr/testify/require"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"

	"github.com/milvus-io/milvus/internal/datacoord/allocator"

	snapshotstorage "github.com/milvus-io/milvus/internal/snapshotio/storage"
	milvusstorage "github.com/milvus-io/milvus/internal/storage"

	"github.com/milvus-io/milvus/internal/util/importutilv2"

	"github.com/milvus-io/milvus/pkg/v3/mlog"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"

	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"

	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestValidateImportRequestInvalidSnapshotOptions(t *testing.T) {
	s := &Server{}
	err := newDDLCallbacks(s).ValidateImportRequest(context.Background(), nil, importutilv2.Options{{
		Key: importutilv2.SourceType, Value: importutilv2.SourceTypeSnapshot,
	}})
	require.ErrorIs(t, err, merr.ErrImportFailed)
}

func TestBroadcastSnapshotImportLargeMetadataReference(t *testing.T) {
	paramtable.Init()
	patchSnapshotImportInstance(t)
	ctx := context.Background()
	cm := milvusstorage.NewLocalChunkManager()
	server := &Server{meta: &meta{chunkManager: cm}}
	validation := mockey.Mock((*DDLCallbacks).ValidateImportRequest).Return(nil).Build()
	defer validation.UnPatch()
	start := mockey.Mock((*DDLCallbacks).StartBroadcastWithCollectionID).Return(nil, merr.ErrIoFailed).Build()
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
			// Large metadata is validated, but only its reference enters the WAL.
			captured, _, err := prepareSnapshotImportFiles(ctx, []int64{10}, cm, snapshotImportTestSchema(), files, snapshotImportTestOptions())
			require.NoError(t, err)
			require.Less(t, proto.Size(captured[0]), 32)
			before := start.Times()
			_, _, err = newDDLCallbacks(server).BroadcastImport(ctx, 100, []int64{10}, files,
				snapshotImportTestOptions(), 1000, []string{"target_v1"}, "",
				internalpb.ImportVersion_ImportVersionV2)
			require.ErrorIs(t, err, merr.ErrIoFailed)
			require.Equal(t, before+1, start.Times(), "metadata size must not reject a small WAL reference")
		})
	}
}

func TestCreateImportJobFromAckBackupListingError(t *testing.T) {
	s := &Server{meta: &meta{}}
	s.stateCode.Store(commonpb.StateCode_Healthy)
	listing := mockey.Mock(ListBinlogImportRequestFiles).Return([]*internalpb.ImportFile(nil), merr.ErrIoKeyNotFound).Build()
	defer listing.UnPatch()
	wal := message.NewImportMessageBuilderV1().WithHeader(&message.ImportMessageHeader{}).
		WithBody(&msgpb.ImportMsg{
			Files:   []*msgpb.ImportFile{{Paths: []string{"root/insert_log", "root/delta_log"}}},
			Options: map[string]string{importutilv2.BackupFlag: "true"},
		}).WithBroadcast([]string{"target_v1"}).MustBuildBroadcast()
	resp, err := s.createImportJobFromAck(context.Background(), message.BroadcastResultImportMessageV1{
		Message: message.MustAsBroadcastImportMessageV1(wal),
	})
	require.ErrorIs(t, merr.CheckRPCCall(resp, err), merr.ErrIoKeyNotFound)
	require.Empty(t, resp.GetJobID(), "failed source listing must not allocate a job")
}

func TestSnapshotImportAckCollectionUnavailable(t *testing.T) {
	type missingHandler struct{ Handler }
	type ackAllocator struct{ allocator.Allocator }
	allocation := mockey.Mock((*ackAllocator).AllocN).Return(int64(100), int64(110), nil).Build()
	defer allocation.UnPatch()
	server := &Server{handler: &missingHandler{}, allocator: &ackAllocator{}}
	server.stateCode.Store(commonpb.StateCode_Healthy)
	wal := message.NewImportMessageBuilderV1().WithHeader(&message.ImportMessageHeader{}).WithBody(&msgpb.ImportMsg{
		CollectionID: 1, Files: []*msgpb.ImportFile{{}}, Options: funcutil.KeyValuePair2Map(snapshotImportTestOptions()),
	}).WithBroadcast([]string{"target_v1"}).MustBuildBroadcast()
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"dropped", merr.ErrCollectionNotFound},
		{"missing_metadata", nil},
		{"temporarily_unavailable", merr.ErrServiceNotReady},
	} {
		t.Run(tc.name, func(t *testing.T) {
			get := mockey.Mock((*missingHandler).GetCollection).Return((*collectionInfo)(nil), tc.err).Build()
			defer get.UnPatch()
			callback := newDDLCallbacks(server)
			err := callback.importV1AckCallback(context.Background(), message.BroadcastResultImportMessageV1{
				Message: message.MustAsBroadcastImportMessageV1(wal), Results: map[string]*message.AppendResult{
					"target_v1": {TimeTick: 100}, funcutil.GetControlChannel("target"): {TimeTick: 100},
				},
			})
			if tc.err == merr.ErrServiceNotReady {
				require.ErrorIs(t, err, tc.err, "temporary metadata errors must retry the ACK")
			} else {
				require.NoError(t, err, "a dropped collection must not retry the ACK forever")
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
	read := mockey.Mock((*snapshotstorage.SnapshotReader).ReadMetadata).To(
		func(*snapshotstorage.SnapshotReader, context.Context, string) (*datapb.SnapshotMetadata, error) {
			t.Fatal("ACK must not perform source I/O")
			return nil, nil
		}).Build()
	defer read.UnPatch()
	var saved ImportJob
	var saveErr error
	metaPatch := mockey.Mock((*ackMeta).AddJob).To(func(_ *ackMeta, _ context.Context, job ImportJob) error {
		if saveErr != nil {
			return saveErr
		}
		saved = job
		return nil
	}).Build()
	defer metaPatch.UnPatch()
	server := &Server{handler: &ackHandler{}, allocator: &ackAllocator{}, importMeta: &ackMeta{}}
	server.stateCode.Store(commonpb.StateCode_Healthy)
	for _, mode := range []string{"valid", "missing_uri", "missing_layout", "mixed_paths", "missing_file", "multiple_files", "bad_timeout"} {
		t.Run(mode, func(t *testing.T) {
			saved = nil
			logs := mlog.CaptureGlobalLogs(t, &mlog.Config{Level: "info", Format: "json", DisableTimestamp: true})
			body := &msgpb.ImportMsg{
				JobID: 12, CollectionID: 1, PartitionIDs: []int64{10},
				Schema: snapshotImportTestSchema(), Files: []*msgpb.ImportFile{{}},
				Options: funcutil.KeyValuePair2Map(snapshotImportTestOptions()),
			}
			body.Options[importutilv2.SnapshotSourceURI] = "s3://source/root/snapshots/1/metadata/2.json"
			body.Options[importutilv2.SnapshotLayout] = "referenced"
			body.Options[importutilv2.ExternalSpec] = `{"extfs":{"access_key_value":"ack-storage-secret"}}`
			body.Options[importutilv2.EZK] = "ack-ezk-secret"
			switch mode {
			case "missing_uri":
				delete(body.Options, importutilv2.SnapshotSourceURI)
			case "missing_layout":
				delete(body.Options, importutilv2.SnapshotLayout)
			case "mixed_paths":
				body.Files[0].Paths = []string{"legacy"}
			case "missing_file":
				body.Files = nil
			case "multiple_files":
				body.Files = append(body.Files, &msgpb.ImportFile{})
			case "bad_timeout":
				body.Options["timeout"] = "invalid"
			}
			wal := message.NewImportMessageBuilderV1().WithHeader(&message.ImportMessageHeader{}).
				WithBody(body).WithBroadcast([]string{"target_v1"}).MustBuildBroadcast()
			result := message.BroadcastResultImportMessageV1{
				Message: message.MustAsBroadcastImportMessageV1(wal),
				Results: map[string]*message.AppendResult{"target_v1": {TimeTick: 100}},
			}
			callback := newDDLCallbacks(server)
			if mode == "missing_uri" {
				saveErr = merr.ErrServiceUnavailable
				require.ErrorIs(t, callback.importV1AckCallback(context.Background(), result), saveErr,
					"even a terminal source failure must retry until persisted")
				require.Nil(t, saved)
				saveErr = nil
			}
			require.NoError(t, callback.importV1AckCallback(context.Background(), result),
				"a malformed durable reference must not retry the ACK forever")
			require.NotNil(t, saved)
			require.EqualValues(t, 12, saved.GetJobID())
			require.Zero(t, read.Times())
			if mode == "valid" {
				require.Equal(t, internalpb.ImportJobState_Pending, saved.GetState())
				require.True(t, importutilv2.IsSnapshotPreparation(saved.GetFiles()))
				require.Empty(t, saved.GetSnapshotL0Sources())
				require.Equal(t, body.Options, funcutil.KeyValuePair2Map(saved.GetOptions()))
				ackLogs := 0
				for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
					if !strings.Contains(line, "creating import job from ack callback") && !strings.Contains(line, "add import job done") {
						continue
					}
					ackLogs++
					require.Less(t, len(line), 4096)
					var entry map[string]json.RawMessage
					require.NoError(t, json.Unmarshal([]byte(line), &entry))
					require.NotContains(t, entry, "files")
					require.JSONEq(t, "1", string(entry["fileNum"]))
					require.JSONEq(t, "12", string(entry["jobID"]))
					require.JSONEq(t, "1", string(entry["collectionID"]))
					require.JSONEq(t, "[9]", string(entry["snapshotSourceVersions"]))
				}
				require.Equal(t, 2, ackLogs)
			} else {
				require.Equal(t, internalpb.ImportJobState_Failed, saved.GetState())
				require.NotEmpty(t, saved.GetReason())
				require.NotEqual(t, uint64(math.MaxUint64), saved.GetCleanupTs())
			}
			require.NotContains(t, logs.String(), "ack-storage-secret")
			require.NotContains(t, logs.String(), "ack-ezk-secret")
			require.Equal(t, body.Options, result.Message.MustBody().GetOptions(), "logging must not mutate options")
		})
	}
}

func TestImportFilesLogFields(t *testing.T) {
	for _, tc := range []struct {
		name     string
		files    []*internalpb.ImportFile
		versions string
	}{
		{name: "empty"},
		{name: "ordinary", files: []*internalpb.ImportFile{nil, {Id: 1, Paths: []string{"ordinary.json"}}}},
		{
			name: "mixed_invalid_sources",
			files: []*internalpb.ImportFile{
				nil,
				{Paths: []string{"ordinary.json"}},
				{SnapshotSource: &internalpb.SnapshotImportSource{Version: 9, ManifestPath: "secret"}},
				{SnapshotSource: &internalpb.SnapshotImportSource{Version: 99, ManifestPath: "secret"}},
			},
			versions: "[9,99]",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := proto.Clone(&internalpb.ImportRequestInternal{Files: tc.files})
			logs := mlog.CaptureGlobalLogs(t, &mlog.Config{Level: "info", Format: "json", DisableTimestamp: true})
			mlog.Info(context.Background(), "file summary", importFilesLogFields(tc.files)...)
			var entry map[string]json.RawMessage
			require.NoError(t, json.Unmarshal([]byte(logs.String()), &entry))
			require.JSONEq(t, fmt.Sprint(len(tc.files)), string(entry["fileNum"]))
			if tc.versions == "" {
				require.Contains(t, entry, "files", "ordinary imports retain file diagnostics")
				require.NotContains(t, entry, "snapshotSourceVersions")
				if len(tc.files) != 0 {
					require.Contains(t, string(entry["files"]), "ordinary.json")
				}
			} else {
				require.NotContains(t, entry, "files")
				require.JSONEq(t, tc.versions, string(entry["snapshotSourceVersions"]))
				require.NotContains(t, logs.String(), "secret")
			}
			require.True(t, proto.Equal(before, &internalpb.ImportRequestInternal{Files: tc.files}))
		})
	}
}
