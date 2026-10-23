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

package ddl

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/bulkimport"
	snapshotstorage "github.com/milvus-io/milvus/internal/snapshotio/storage"
	milvusstorage "github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	streamingutil "github.com/milvus-io/milvus/internal/util/streamingutil/util"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
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
				snapshot := bulkimport.SnapshotImportTestData(layout)
				metadata := &datapb.SnapshotMetadata{
					FormatVersion: int32(snapshotstorage.SnapshotFormatVersion),
					SnapshotInfo:  snapshot.SnapshotInfo, Collection: snapshot.Collection, Layout: layout,
				}
				metadata.SnapshotInfo.S3Location = uri
				metadata.SnapshotInfo.Description = strings.Repeat("metadata-content", 20000)
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
				files, options, err := bulkimport.PrepareSnapshotImportFiles(context.Background(), nil, &sourceCM{}, bulkimport.SnapshotImportTestSchema(),
					[]*internalpb.ImportFile{{Paths: []string{uri}}}, bulkimport.SnapshotImportTestOptions())
				require.NoError(t, err)
				require.Equal(t, 1, reads)
				require.Zero(t, physical.Times())
				require.Zero(t, validation.Times())
				require.True(t, importutilv2.IsSnapshotPreparation(files))
				require.NoError(t, importutilv2.ValidateSnapshotImportPlan(files, options, nil))
				require.ErrorIs(t, importutilv2.ValidateSnapshotImportTask(files, options, nil), merr.ErrServiceUnimplemented)
				// Only the reference survives WAL encoding and CDC rewriting.
				wal := message.NewImportMessageBuilderV1().WithHeader(&message.ImportMessageHeader{}).
					WithBody(&msgpb.ImportMsg{Files: []*msgpb.ImportFile{{}}, Options: funcutil.KeyValuePair2Map(options)}).WithBroadcast([]string{"target"}).MustBuildBroadcast()
				msgID := walimplstest.NewTestMessageID(1)
				immutable := wal.WithBroadcastID(1).SplitIntoMutableMessage()[0].WithTimeTick(100).WithLastConfirmed(msgID).IntoImmutableMessage(msgID)
				replicated := message.MustNewReplicateMessage("source-cluster", immutable.IntoImmutableMessageProto())
				replicated.OverwriteReplicateVChannel("replica", []string{"replica"})
				replica := message.MustAsMutableImportMessageV1(replicated)
				require.Equal(t, funcutil.KeyValuePair2Map(options), replica.MustBody().GetOptions())
				bound := ImportFilesFromMessage(replica.MustBody().GetFiles(), funcutil.Map2KeyValuePair(replica.MustBody().GetOptions()))
				require.True(t, proto.Equal(files[0], bound[0]))
				require.Equal(t, uri, replica.MustBody().Options[importutilv2.SnapshotSourceURI])
				require.Less(t, wal.EstimateSize(), 2048, "WAL size must not scale with metadata size")
			})
		}
	}
}

func TestSnapshotImportWALReference(t *testing.T) {
	options := append(bulkimport.SnapshotImportTestOptions(),
		&commonpb.KeyValuePair{Key: importutilv2.SnapshotSourceURI, Value: "s3://source/root/snapshots/1/metadata/2.json"},
		&commonpb.KeyValuePair{Key: importutilv2.SnapshotLayout, Value: "referenced"})
	header := &message.ImportMessageHeader{CommitByCoordinator: true}
	wal := message.NewImportMessageBuilderV1().WithHeader(header).
		WithBody(&msgpb.ImportMsg{PartitionIDs: []int64{20, 10}, Files: []*msgpb.ImportFile{{Id: 1}}, Options: funcutil.KeyValuePair2Map(options)}).
		WithBroadcast([]string{"target_v1"}).MustBuildBroadcast()
	decoded, err := message.AsBroadcastImportMessageV1(message.NewBroadcastMutableMessageBeforeAppend(wal.Payload(), wal.Properties().ToRawMap()))
	require.NoError(t, err)
	files := ImportFilesFromMessage(decoded.MustBody().GetFiles(), funcutil.Map2KeyValuePair(decoded.MustBody().Options))
	require.NoError(t, importutilv2.ValidateSnapshotImportPlan(files, options, nil))
	require.True(t, proto.Equal(header, decoded.Header()), "the header must carry only the coordinator commit flag")
	encoded, err := proto.Marshal(&datapb.ImportJob{Files: files, Options: options, PartitionIDs: decoded.MustBody().GetPartitionIDs()})
	require.NoError(t, err)
	job := &datapb.ImportJob{}
	require.NoError(t, proto.Unmarshal(encoded, job))
	require.Equal(t, []int64{20, 10}, job.PartitionIDs)
	require.NoError(t, importutilv2.ValidateSnapshotImportPlan(job.Files, job.Options, nil))
	require.True(t, importutilv2.IsSnapshotPreparation(job.Files))

	// CDC rewrites the transport channel, but preserves the URI and
	// source options used independently by each cluster's Pending worker.
	msgID := walimplstest.NewTestMessageID(1)
	immutable := wal.WithBroadcastID(1).SplitIntoMutableMessage()[0].WithTimeTick(100).
		WithLastConfirmed(msgID).IntoImmutableMessage(msgID)
	replicated := message.MustNewReplicateMessage("source-cluster", immutable.IntoImmutableMessageProto())
	replicated.OverwriteReplicateVChannel("replica_v1", []string{"replica_v1"})
	replica := message.MustAsMutableImportMessageV1(replicated)
	require.True(t, proto.Equal(header, replica.Header()))
	require.Equal(t, decoded.MustBody().Options, replica.MustBody().Options)
	require.True(t, proto.Equal(files[0], ImportFilesFromMessage(replica.MustBody().Files, funcutil.Map2KeyValuePair(replica.MustBody().Options))[0]))

	ordinary := ImportFilesFromMessage([]*msgpb.ImportFile{{Id: 1, Paths: []string{"legacy"}}}, nil)
	require.Equal(t, []*internalpb.ImportFile{{Id: 1, Paths: []string{"legacy"}}}, ordinary)
}

func TestSnapshotImportMessageAdmission(t *testing.T) {
	for _, backend := range []message.WALName{message.WALNamePulsar, message.WALNameKafka, message.WALNameWoodpecker, message.WALNameRocksmq} {
		t.Run(backend.String(), func(t *testing.T) {
			selection := mockey.Mock(streamingutil.MustSelectWALName).Return(backend).Build()
			defer selection.UnPatch()
			for _, size := range []int{10, 512 * 1024} {
				msg := message.NewImportMessageBuilderV1().WithHeader(&message.ImportMessageHeader{}).
					WithBody(&msgpb.ImportMsg{Schema: &schemapb.CollectionSchema{Description: strings.Repeat("x", size)}}).
					WithBroadcast([]string{"v1"}).MustBuildBroadcast()
				err := ValidateSnapshotImportMessageSize(msg)
				if size == 10 {
					require.NoError(t, err)
				} else {
					require.ErrorIs(t, err, merr.ErrImportFailed)
				}
			}
		})
	}
}
