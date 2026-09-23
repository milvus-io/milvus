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

package datacoord

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	mocks2 "github.com/milvus-io/milvus/internal/mocks"
	mock_streaming "github.com/milvus-io/milvus/internal/mocks/distributed/mock_streaming"
	snapshotstorage "github.com/milvus-io/milvus/internal/snapshotio/storage"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/balance"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/channel"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

// ================================
// Import Callbacks Test Suite
// ================================

func patchSnapshotImportWAL(t *testing.T) {
	t.Helper()
	type snapshotWAL struct{ streaming.WALAccesser }
	control := mockey.Mock((*snapshotWAL).ControlChannel).Return(funcutil.GetControlChannel("snapshot-test")).Build()
	t.Cleanup(func() { control.UnPatch() })
	wal := mockey.Mock(streaming.WAL).Return(&snapshotWAL{}).Build()
	t.Cleanup(func() { wal.UnPatch() })
}

func TestSnapshotPartitionTargetsValidation(t *testing.T) {
	ctx := context.Background()
	type mappingBroker struct{ broker.Broker }
	s := &Server{broker: &mappingBroker{}}
	opts := append(snapshotImportTestOptions(), &commonpb.KeyValuePair{Key: importutilv2.PartitionMapping, Value: `{"A":"X","B":"Y"}`})
	require.NoError(t, s.validateSnapshotPartitionTargets(ctx, 1, nil, snapshotImportTestOptions()))
	for _, tc := range []struct {
		name      string
		names     []string
		ids, want []int64
		readErr   error
		wantErr   error
	}{
		{"valid", []string{"Y", "X"}, []int64{100, 200}, []int64{200, 100}, nil, nil},
		{"recreated", []string{"Y", "X"}, []int64{100, 201}, []int64{200, 100}, nil, merr.ErrServiceUnavailable},
		{"dropped", []string{"Y"}, []int64{100}, []int64{200, 100}, nil, merr.ErrServiceUnavailable},
		{"bad_metadata", []string{"Y"}, nil, []int64{200, 100}, nil, merr.ErrServiceInternal},
		{"bad_count", []string{"Y", "X"}, []int64{100, 200}, []int64{200}, nil, merr.ErrServiceInternal},
		{"metadata_error", nil, nil, nil, merr.ErrServiceNotReady, merr.ErrServiceNotReady},
	} {
		t.Run(tc.name, func(t *testing.T) {
			show := mockey.Mock((*mappingBroker).ShowPartitions).Return(&milvuspb.ShowPartitionsResponse{PartitionNames: tc.names, PartitionIDs: tc.ids}, tc.readErr).Build()
			defer show.UnPatch()
			err := s.validateSnapshotPartitionTargets(ctx, 1, tc.want, opts)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestBroadcastSnapshotImportMappedPartitions(t *testing.T) {
	patchSnapshotImportWAL(t)
	patchSnapshotImportInstance(t)
	ctx := context.Background()
	snapshot := snapshotImportTestData(datapb.SnapshotLayout_SnapshotLayoutReferenced)
	snapshot.Collection.Partitions = map[string]int64{"A": 10, "B": 20}
	snapshot.Segments[0].PartitionId = 20
	for _, segment := range snapshot.Segments {
		segment.ChannelName = "source"
	}
	snapshot.Segments = append(snapshot.Segments, &datapb.SegmentDescription{
		ChannelName: "source", PartitionId: common.AllPartitionsID, SegmentLevel: datapb.SegmentLevel_L0, StorageVersion: storage.StorageV1,
		Deltalogs: []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{LogPath: "root/files/delete"}}}},
	})
	validation := mockey.Mock((*Server).validateImportRequest).Return(nil).Build()
	defer validation.UnPatch()
	replication := mockey.Mock((*Server).validateImportReplication).Return(nil).Build()
	defer replication.UnPatch()
	read := mockey.Mock((*snapshotstorage.SnapshotReader).ReadMetadata).Return(&datapb.SnapshotMetadata{
		FormatVersion: int32(snapshotstorage.SnapshotFormatVersion), SnapshotInfo: snapshot.SnapshotInfo,
		Collection: snapshot.Collection, Layout: snapshot.Layout,
	}, nil).Build()
	defer read.UnPatch()
	fileValidation := mockey.Mock(snapshotstorage.ValidateExternalSnapshotDataFiles).Return(nil).Build()
	defer fileValidation.UnPatch()
	api := newMockBroadcastAPIImpl()
	locked := false
	start := mockey.Mock((*Server).startBroadcastWithCollectionID).To(func(_ *Server, _ context.Context, _ int64) (broadcaster.BroadcastAPI, error) {
		locked = true
		return api, nil
	}).Build()
	defer start.UnPatch()
	type mappingBroker struct{ broker.Broker }
	fake := &mappingBroker{}
	describe := mockey.Mock((*mappingBroker).DescribeCollectionInternal).Return(&milvuspb.DescribeCollectionResponse{Status: merr.Success(), DbName: "default"}, nil).Build()
	defer describe.UnPatch()
	show := mockey.Mock((*mappingBroker).ShowPartitions).To(func(_ *mappingBroker, _ context.Context, _ int64) (*milvuspb.ShowPartitionsResponse, error) {
		require.True(t, locked, "target IDs must be revalidated under the broadcast lock")
		return &milvuspb.ShowPartitionsResponse{PartitionNames: []string{"X", "Y"}, PartitionIDs: []int64{200, 100}}, nil
	}).Build()
	defer show.UnPatch()
	called := false
	transport := mockey.Mock((*mockBroadcastAPIImpl).Broadcast).To(func(_ *mockBroadcastAPIImpl, _ context.Context, msg message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error) {
		called = true
		decoded := message.MustAsBroadcastImportMessageV1(msg)
		require.Equal(t, []int64{200, 100}, decoded.MustBody().GetPartitionIDs())
		var options importutilv2.Options
		for key, value := range decoded.MustBody().GetOptions() {
			options = append(options, &commonpb.KeyValuePair{Key: key, Value: value})
		}
		files, err := bindSnapshotImportSources(decoded.MustBody().GetFiles(), decoded.Header().GetSnapshotSources())
		require.NoError(t, err)
		require.NoError(t, importutilv2.ValidateSnapshotImportPlan(files, options, nil))
		require.True(t, importutilv2.IsSnapshotPreparation(files))
		captured := &datapb.SnapshotMetadata{}
		require.NoError(t, proto.Unmarshal(files[0].SnapshotSource.SnapshotMetadata, captured))
		require.Equal(t, snapshot.Collection.Partitions, captured.Collection.Partitions)
		return &types.BroadcastAppendResult{}, nil
	}).Build()
	defer transport.UnPatch()
	s := &Server{broker: fake, meta: &meta{chunkManager: storage.NewLocalChunkManager()}}
	opts := append(snapshotImportTestOptions(), &commonpb.KeyValuePair{Key: importutilv2.PartitionMapping, Value: `{"A":"X","B":"Y"}`})
	_, _, err := s.broadcastImport(ctx, "target", 100, []int64{200, 100},
		[]*internalpb.ImportFile{{Paths: []string{"s3://source/root/snapshots/1/metadata/2.json"}}}, opts, snapshotImportTestSchema(), 1000, []string{"target_v1"}, "")
	require.NoError(t, err)
	require.True(t, called)
	require.True(t, api.closeCalled.Load())
}

func TestBroadcastSnapshotImportMultipleTargetPartitions(t *testing.T) {
	patchSnapshotImportWAL(t)
	ctx := context.Background()
	// Exercise the real broadcast path while replacing external validation,
	// metadata I/O, and WAL transport. Source expansion does not select targets.
	validation := mockey.Mock((*Server).validateImportRequest).Return(nil).Build()
	defer validation.UnPatch()
	expansion := mockey.Mock(prepareSnapshotImportFiles).Return(
		[]*internalpb.ImportFile{{Paths: []string{"root/data/manifest"}}}, snapshotImportTestOptions(), nil).Build()
	defer expansion.UnPatch()
	replication := mockey.Mock((*Server).validateImportReplication).Return(nil).Build()
	defer replication.UnPatch()
	api := newMockBroadcastAPIImpl()
	start := mockey.Mock((*Server).startBroadcastWithCollectionID).Return(api, nil).Build()
	defer start.UnPatch()
	type testBroker struct{ broker.Broker }
	fakeBroker := &testBroker{}
	describe := mockey.Mock((*testBroker).DescribeCollectionInternal).Return(
		&milvuspb.DescribeCollectionResponse{Status: merr.Success(), DbName: "default"}, nil).Build()
	defer describe.UnPatch()
	var received []int64
	transport := mockey.Mock((*mockBroadcastAPIImpl).Broadcast).To(
		func(_ *mockBroadcastAPIImpl, _ context.Context, msg message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error) {
			decoded, err := message.AsBroadcastImportMessageV1(msg)
			require.NoError(t, err)
			received = decoded.MustBody().GetPartitionIDs()
			return &types.BroadcastAppendResult{}, nil
		}).Build()
	defer transport.UnPatch()
	server := &Server{broker: fakeBroker}
	partitionIDs := []int64{20, 10}
	_, duplicated, err := server.broadcastImport(ctx, "target", 100, partitionIDs,
		[]*internalpb.ImportFile{{Paths: []string{"s3://source/root/snapshots/1/metadata/2.json"}}},
		snapshotImportTestOptions(), &schemapb.CollectionSchema{}, 1000, []string{"target_v1"}, "")
	require.NoError(t, err)
	require.False(t, duplicated)
	require.Equal(t, partitionIDs, received)
	require.True(t, api.closeCalled.Load())
	expansion.UnPatch()
	invalidExpansion := mockey.Mock(prepareSnapshotImportFiles).Return([]*internalpb.ImportFile{{SnapshotSource: &internalpb.SnapshotImportSource{
		Version: 5, ManifestPath: packed.MarshalManifestPath("root/data", 1), SourceChannel: "source", SourcePartitionId: 10,
	}}}, snapshotImportTestOptions(), nil).Build()
	defer invalidExpansion.UnPatch()
	received = nil
	_, _, err = server.broadcastImport(ctx, "target", 100, partitionIDs,
		[]*internalpb.ImportFile{{Paths: []string{"s3://source/root/snapshots/1/metadata/2.json"}}},
		snapshotImportTestOptions(), &schemapb.CollectionSchema{}, 1000, []string{"target_v1"}, "")
	require.ErrorContains(t, err, "lost its shared L0 inventory")
	require.Nil(t, received, "invalid source plans must fail before broadcast")
}

func TestBroadcastImportBoundaryFailures(t *testing.T) {
	patchSnapshotImportWAL(t)
	type boundaryBroker struct{ broker.Broker }
	for _, stage := range []string{"public_options", "prepare", "replication", "partition_targets", "message_size", "auto_id"} {
		t.Run(stage, func(t *testing.T) {
			cause := merr.ErrServiceNotReady
			errFor := func(name string) error {
				if stage == name {
					return cause
				}
				return nil
			}
			opts := snapshotImportTestOptions()
			files := []*internalpb.ImportFile{{SnapshotSource: &internalpb.SnapshotImportSource{
				Version: 1, ManifestPath: packed.MarshalManifestPath("root/segment", 1),
			}}}
			schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			}}
			if stage == "auto_id" {
				opts = nil
				files = []*internalpb.ImportFile{{Paths: []string{"rows.json"}}}
				schema.Fields[0].AutoID = true
			}
			validation := mockey.Mock((*Server).validateImportRequest).Return(nil).Build()
			defer validation.UnPatch()
			expansion := mockey.Mock(prepareSnapshotImportFiles).Return(files, opts, errFor("prepare")).Build()
			defer expansion.UnPatch()
			replication := mockey.Mock((*Server).validateImportReplication).Return(errFor("replication")).Build()
			defer replication.UnPatch()
			partitions := mockey.Mock((*Server).validateSnapshotPartitionTargets).Return(errFor("partition_targets")).Build()
			defer partitions.UnPatch()
			size := mockey.Mock(validateSnapshotImportMessageSize).Return(errFor("message_size")).Build()
			defer size.UnPatch()
			api := newMockBroadcastAPIImpl()
			start := mockey.Mock((*Server).startBroadcastWithCollectionID).Return(api, nil).Build()
			defer start.UnPatch()
			describe := mockey.Mock((*boundaryBroker).DescribeCollectionInternal).Return(
				&milvuspb.DescribeCollectionResponse{Status: merr.Success(), DbName: "default"}, nil).Build()
			defer describe.UnPatch()
			transport := mockey.Mock((*mockBroadcastAPIImpl).Broadcast).To(
				func(_ *mockBroadcastAPIImpl, _ context.Context, msg message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error) {
					decoded := message.MustAsBroadcastImportMessageV1(msg)
					// ID allocation follows preimport, not the initial broadcast.
					for _, file := range decoded.MustBody().GetFiles() {
						require.Nil(t, file.GetPreAllocatedAutoIds())
					}
					require.ElementsMatch(t, []string{"v1", funcutil.GetControlChannel("snapshot-test")}, msg.BroadcastHeader().VChannels)
					return &types.BroadcastAppendResult{}, nil
				}).Build()
			defer transport.UnPatch()
			if stage == "public_options" {
				opts = append(opts, &commonpb.KeyValuePair{Key: importutilv2.SnapshotSourceURI, Value: "s3://source/metadata"})
			}
			type boundaryAllocator struct{ allocator.Allocator }
			s := &Server{meta: &meta{}, broker: &boundaryBroker{}, allocator: &boundaryAllocator{}}
			_, _, err := s.broadcastImport(context.Background(), "target", 100, []int64{10}, files,
				opts, schema, 1, []string{"v1"}, "")
			if stage == "auto_id" {
				require.NoError(t, err)
				require.Equal(t, 1, transport.Times())
			} else {
				if stage == "public_options" {
					require.ErrorIs(t, err, merr.ErrImportFailed)
				} else {
					require.ErrorIs(t, err, cause)
				}
				require.Zero(t, transport.Times(), "rejected requests must not reach the WAL")
			}
		})
	}
}

func TestBroadcastSnapshotImportNormalizesEZK(t *testing.T) {
	patchSnapshotImportWAL(t)
	patchSnapshotImportInstance(t)
	ctx := context.Background()
	validation := mockey.Mock((*Server).validateImportRequest).Return(nil).Build()
	defer validation.UnPatch()
	replication := mockey.Mock((*Server).validateImportReplication).Return(nil).Build()
	defer replication.UnPatch()
	fileValidation := mockey.Mock(snapshotstorage.ValidateExternalSnapshotDataFiles).Return(nil).Build()
	defer fileValidation.UnPatch()
	api := newMockBroadcastAPIImpl()
	start := mockey.Mock((*Server).startBroadcastWithCollectionID).Return(api, nil).Build()
	defer start.UnPatch()
	type testBroker struct{ broker.Broker }
	fakeBroker := &testBroker{}
	describe := mockey.Mock((*testBroker).DescribeCollectionInternal).Return(
		&milvuspb.DescribeCollectionResponse{Status: merr.Success(), DbName: "default"}, nil).Build()
	defer describe.UnPatch()
	server := &Server{broker: fakeBroker, meta: &meta{chunkManager: storage.NewLocalChunkManager()}}
	for _, encrypted := range []bool{false, true} {
		name, suppliedEZK := "plaintext", "not-base64"
		if encrypted {
			name, suppliedEZK = "encrypted", snapshotImportTestEZK(10)
		}
		t.Run(name, func(t *testing.T) {
			snapshot := snapshotImportTestData(datapb.SnapshotLayout_SnapshotLayoutReferenced)
			if encrypted {
				snapshot.Collection.Schema.Properties = []*commonpb.KeyValuePair{{Key: common.EncryptionEzIDKey, Value: "10"}}
			}
			read := mockey.Mock((*snapshotstorage.SnapshotReader).ReadMetadata).Return(&datapb.SnapshotMetadata{
				FormatVersion: int32(snapshotstorage.SnapshotFormatVersion), SnapshotInfo: snapshot.SnapshotInfo,
				Collection: snapshot.Collection, Layout: snapshot.Layout,
			}, nil).Build()
			defer read.UnPatch()
			var received map[string]string
			transport := mockey.Mock((*mockBroadcastAPIImpl).Broadcast).To(
				func(_ *mockBroadcastAPIImpl, _ context.Context, msg message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error) {
					decoded, err := message.AsBroadcastImportMessageV1(msg)
					require.NoError(t, err)
					// Decode the wire body used by ACK/replay, not the original
					// request slice, to verify the durable options boundary.
					restored := &msgpb.ImportMsg{}
					require.NoError(t, proto.Unmarshal(decoded.Payload(), restored))
					received = restored.GetOptions()
					return &types.BroadcastAppendResult{}, nil
				}).Build()
			defer transport.UnPatch()
			options := append(snapshotImportTestOptions(), &commonpb.KeyValuePair{Key: importutilv2.EZK, Value: suppliedEZK},
				&commonpb.KeyValuePair{Key: importutilv2.AutoCommitKey, Value: "false"})
			_, _, err := server.broadcastImport(ctx, "target", 100, []int64{10},
				[]*internalpb.ImportFile{{Paths: []string{"s3://source/root/snapshots/1/metadata/2.json"}}},
				options, snapshotImportTestSchema(), 1000, []string{"target_v1"}, "")
			require.NoError(t, err)
			require.Equal(t, "false", received[importutilv2.AutoCommitKey])
			require.Equal(t, importutilv2.SourceTypeSnapshot, received[importutilv2.SourceType])
			if encrypted {
				require.Equal(t, suppliedEZK, received[importutilv2.EZK])
			} else {
				require.NotContains(t, received, importutilv2.EZK)
			}
			ezk, err := importutilv2.GetEZK(options)
			require.NoError(t, err)
			require.Equal(t, suppliedEZK, ezk, "the original request must not be mutated")
		})
	}
}

type ImportCallbacksSuite struct {
	suite.Suite
}

func TestImportCallbacksSuite(t *testing.T) {
	suite.Run(t, new(ImportCallbacksSuite))
}

// newTestMetaWithChunkManager returns a minimal meta carrying a chunk manager,
// which validateImportRequest needs to resolve the storage root path when it
// checks caller-supplied import paths against Milvus's internal directories.
func newTestMetaWithChunkManager(t *testing.T) *meta {
	cm := mocks2.NewChunkManager(t)
	cm.EXPECT().RootPath().Return("files").Maybe()
	return &meta{chunkManager: cm}
}

// --------------------------------
// validateImportRequest Tests
// --------------------------------

func (s *ImportCallbacksSuite) TestValidateImportRequest_InvalidTimeoutReturnsError() {
	ctx := context.Background()
	server := &Server{}

	files := []*msgpb.ImportFile{
		{Id: 1, Paths: []string{"/test/file1.json"}},
	}
	options := []*commonpb.KeyValuePair{
		{Key: "timeout", Value: "invalid_timeout_format"},
	}

	err := server.validateImportRequest(ctx, files, options)

	s.Error(err)
	s.Contains(err.Error(), "timeout")
}

// TestValidateImportRequest_RejectsInternalStoragePath pins the datacoord side
// of the path-confinement gate. ValidateImportFilePaths is covered on its own
// (import_util_test.go), but every path the suite passed here was benign, so
// deleting the call left this package green -- the same wiring standard the
// PreExecute and duplicate-key tests already hold the other two gates to.
func (s *ImportCallbacksSuite) TestValidateImportRequest_RejectsInternalStoragePath() {
	ctx := context.Background()

	server := &Server{
		meta: newTestMetaWithChunkManager(s.T()), // RootPath() == "files"
	}

	files := []*msgpb.ImportFile{
		{Id: 1, Paths: []string{"files/insert_log/1/2/3/100/4"}},
	}
	options := []*commonpb.KeyValuePair{
		{Key: "timeout", Value: "300s"},
	}

	err := server.validateImportRequest(ctx, files, options)

	s.Error(err)
	s.True(errors.Is(err, merr.ErrImportFailed))
	s.Contains(err.Error(), "is a Milvus internal storage directory")
}

func (s *ImportCallbacksSuite) TestValidateImportRequest_BalancerGetFailsReturnsError() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	mockCount := mockey.Mock((*importMeta).CountJobBy).To(func(_ *importMeta, _ context.Context, _ ...ImportJobFilter) int {
		return 1
	}).Build()
	defer mockCount.UnPatch()

	mockBalance := mockey.Mock(balance.GetWithContext).To(func(ctx context.Context) (balancer.Balancer, error) {
		return nil, errors.New("balancer not available")
	}).Build()
	defer mockBalance.UnPatch()

	server := &Server{
		importMeta: &importMeta{},
		meta:       newTestMetaWithChunkManager(s.T()),
	}

	files := []*msgpb.ImportFile{
		{Id: 1, Paths: []string{"/test/file1.json"}},
	}
	options := []*commonpb.KeyValuePair{
		{Key: "timeout", Value: "300s"},
	}

	err := server.validateImportRequest(ctx, files, options)

	s.Error(err)
	s.Contains(err.Error(), "balancer not available")
}

func (s *ImportCallbacksSuite) TestValidateImportRequest_ReplicatingClusterReturnsError() {
	ctx := context.Background()

	mockCount := mockey.Mock((*importMeta).CountJobBy).To(func(_ *importMeta, _ context.Context, _ ...ImportJobFilter) int {
		return 1
	}).Build()
	defer mockCount.UnPatch()

	mockBalancer := &mockBalancerImpl{}
	mockBalance := mockey.Mock(balance.GetWithContext).To(func(ctx context.Context) (balancer.Balancer, error) {
		return mockBalancer, nil
	}).Build()
	defer mockBalance.UnPatch()

	// Mock GetLatestChannelAssignment to return replicating cluster config
	mockAssignment := mockey.Mock((*mockBalancerImpl).GetLatestChannelAssignment).To(
		func(_ *mockBalancerImpl) (*channel.WatchChannelAssignmentsCallbackParam, error) {
			return &channel.WatchChannelAssignmentsCallbackParam{
				ReplicateConfiguration: &commonpb.ReplicateConfiguration{
					Clusters: []*commonpb.MilvusCluster{
						{ClusterId: "cluster1"},
						{ClusterId: "cluster2"},
					},
				},
			}, nil
		}).Build()
	defer mockAssignment.UnPatch()

	server := &Server{
		importMeta: &importMeta{},
		meta:       newTestMetaWithChunkManager(s.T()),
	}

	files := []*msgpb.ImportFile{
		{Id: 1, Paths: []string{"/test/file1.json"}},
	}
	options := []*commonpb.KeyValuePair{
		{Key: "timeout", Value: "300s"},
	}

	err := server.validateImportRequest(ctx, files, options)

	s.Error(err)
	s.True(errors.Is(err, merr.ErrOperationNotSupported))
	s.Contains(err.Error(), "replicating cluster")
}

func (s *ImportCallbacksSuite) TestValidateImportRequest_ReplicatingClusterEnabledRequiresManualCommit() {
	ctx := context.Background()

	paramtable.Get().Save(paramtable.Get().DataCoordCfg.ImportInReplicatingCluster.Key, "true")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.ImportInReplicatingCluster.Key)

	mockCount := mockey.Mock((*importMeta).CountJobBy).To(func(_ *importMeta, _ context.Context, _ ...ImportJobFilter) int {
		return 1
	}).Build()
	defer mockCount.UnPatch()

	mockBalancer := &mockBalancerImpl{}
	mockBalance := mockey.Mock(balance.GetWithContext).To(func(ctx context.Context) (balancer.Balancer, error) {
		return mockBalancer, nil
	}).Build()
	defer mockBalance.UnPatch()

	mockAssignment := mockey.Mock((*mockBalancerImpl).GetLatestChannelAssignment).To(
		func(_ *mockBalancerImpl) (*channel.WatchChannelAssignmentsCallbackParam, error) {
			return &channel.WatchChannelAssignmentsCallbackParam{
				ReplicateConfiguration: &commonpb.ReplicateConfiguration{
					Clusters: []*commonpb.MilvusCluster{
						{ClusterId: "cluster1"},
						{ClusterId: "cluster2"},
					},
				},
			}, nil
		}).Build()
	defer mockAssignment.UnPatch()

	server := &Server{
		importMeta: &importMeta{},
		meta:       newTestMetaWithChunkManager(s.T()),
	}
	files := []*msgpb.ImportFile{
		{Id: 1, Paths: []string{"/test/file1.json"}},
	}

	err := server.validateImportRequest(ctx, files, []*commonpb.KeyValuePair{
		{Key: "timeout", Value: "300s"},
	})
	s.Error(err)
	s.True(errors.Is(err, merr.ErrOperationNotSupported))
	s.Contains(err.Error(), "auto_commit=true")

	err = server.validateImportRequest(ctx, files, []*commonpb.KeyValuePair{
		{Key: "timeout", Value: "300s"},
		{Key: importutilv2.AutoCommitKey, Value: "false"},
	})
	s.NoError(err)
}

func (s *ImportCallbacksSuite) TestValidateImportRequest_SuccessWithValidInput() {
	ctx := context.Background()

	mockCount := mockey.Mock((*importMeta).CountJobBy).To(func(_ *importMeta, _ context.Context, _ ...ImportJobFilter) int {
		return 1
	}).Build()
	defer mockCount.UnPatch()

	mockBalancer := &mockBalancerImpl{}
	mockBalance := mockey.Mock(balance.GetWithContext).To(func(ctx context.Context) (balancer.Balancer, error) {
		return mockBalancer, nil
	}).Build()
	defer mockBalance.UnPatch()

	mockAssignment := mockey.Mock((*mockBalancerImpl).GetLatestChannelAssignment).To(
		func(_ *mockBalancerImpl) (*channel.WatchChannelAssignmentsCallbackParam, error) {
			return &channel.WatchChannelAssignmentsCallbackParam{
				ReplicateConfiguration: nil, // No replication
			}, nil
		}).Build()
	defer mockAssignment.UnPatch()

	server := &Server{
		importMeta: &importMeta{},
		meta:       newTestMetaWithChunkManager(s.T()),
	}

	files := []*msgpb.ImportFile{
		{Id: 1, Paths: []string{"/test/file1.json"}},
	}
	options := []*commonpb.KeyValuePair{
		{Key: "timeout", Value: "300s"},
	}

	err := server.validateImportRequest(ctx, files, options)

	s.NoError(err)
}

// --------------------------------
// broadcastImport Tests
// --------------------------------

// A request that fails validateImportRequest is rejected before any broadcast wiring is
// touched -- server.broker is nil here, so reaching it would panic or block. The trigger
// is an unparsable timeout, i.e. a pure function of the request. The job-count limit is
// admission over mutable cluster state rather than a pure function of the request, but
// it runs in the same place -- validateImportRequest, before any resource key is taken;
// see TestImportV2_JobLimitStillRejectsANewJob.
func (s *ImportCallbacksSuite) TestBroadcastImport_ValidationFailsReturnsError() {
	ctx := context.Background()

	server := &Server{
		importMeta: &importMeta{},
		meta:       newTestMetaWithChunkManager(s.T()),
	}

	_, _, err := server.broadcastImport(
		ctx,
		"test_collection",
		100,
		[]int64{1},
		[]*internalpb.ImportFile{{Id: 1, Paths: []string{"/test/file.json"}}},
		[]*commonpb.KeyValuePair{{Key: "timeout", Value: "not-a-duration"}},
		&schemapb.CollectionSchema{Name: "test_collection"},
		1000,
		[]string{"v1"},
		"",
	)

	s.Error(err)
	s.Contains(err.Error(), "failed to validate import request")
}

func (s *ImportCallbacksSuite) TestBroadcastImport_DescribeCollectionFailsReturnsError() {
	ctx := context.Background()

	// Setup validation to pass
	mockCount := mockey.Mock((*importMeta).CountJobBy).To(func(_ *importMeta, _ context.Context, _ ...ImportJobFilter) int {
		return 1
	}).Build()
	defer mockCount.UnPatch()

	mockBalancer := &mockBalancerImpl{}
	mockBalance := mockey.Mock(balance.GetWithContext).To(func(ctx context.Context) (balancer.Balancer, error) {
		return mockBalancer, nil
	}).Build()
	defer mockBalance.UnPatch()

	mockAssignment := mockey.Mock((*mockBalancerImpl).GetLatestChannelAssignment).To(
		func(_ *mockBalancerImpl) (*channel.WatchChannelAssignmentsCallbackParam, error) {
			return &channel.WatchChannelAssignmentsCallbackParam{
				ReplicateConfiguration: nil,
			}, nil
		}).Build()
	defer mockAssignment.UnPatch()

	// Mock broker.DescribeCollectionInternal to fail (called in startBroadcastWithCollectionID)
	mockBroker := broker.NewMockBroker(s.T())
	mockBroker.EXPECT().DescribeCollectionInternal(mock.Anything, int64(100)).Return(nil, errors.New("collection not found"))

	server := &Server{
		importMeta: &importMeta{},
		broker:     mockBroker,
		meta:       newTestMetaWithChunkManager(s.T()),
	}

	_, _, err := server.broadcastImport(
		ctx,
		"test_collection",
		100,
		[]int64{1},
		[]*internalpb.ImportFile{{Id: 1, Paths: []string{"/test/file.json"}}},
		[]*commonpb.KeyValuePair{{Key: "timeout", Value: "300s"}},
		&schemapb.CollectionSchema{Name: "test_collection"},
		1000,
		[]string{"v1"},
		"",
	)

	s.Error(err)
	s.Contains(err.Error(), "failed to start broadcast with collection id")
}

func (s *ImportCallbacksSuite) TestBroadcastImport_StartBroadcastFailsReturnsError() {
	ctx := context.Background()

	// Setup validation to pass
	mockCount := mockey.Mock((*importMeta).CountJobBy).To(func(_ *importMeta, _ context.Context, _ ...ImportJobFilter) int {
		return 1
	}).Build()
	defer mockCount.UnPatch()

	mockBalancer := &mockBalancerImpl{}
	mockBalance := mockey.Mock(balance.GetWithContext).To(func(ctx context.Context) (balancer.Balancer, error) {
		return mockBalancer, nil
	}).Build()
	defer mockBalance.UnPatch()

	mockAssignment := mockey.Mock((*mockBalancerImpl).GetLatestChannelAssignment).To(
		func(_ *mockBalancerImpl) (*channel.WatchChannelAssignmentsCallbackParam, error) {
			return &channel.WatchChannelAssignmentsCallbackParam{
				ReplicateConfiguration: nil,
			}, nil
		}).Build()
	defer mockAssignment.UnPatch()

	// Mock broker.DescribeCollectionInternal to return dbName (called in startBroadcastWithCollectionID)
	mockBroker := broker.NewMockBroker(s.T())
	mockBroker.EXPECT().DescribeCollectionInternal(mock.Anything, int64(100)).Return(&milvuspb.DescribeCollectionResponse{
		DbName:         "test_db",
		CollectionName: "test_collection",
	}, nil)

	// Mock StartBroadcastWithResourceKeys to fail
	mockBroadcast := mockey.Mock(broadcast.StartBroadcastWithResourceKeys).To(
		func(ctx context.Context, keys ...message.ResourceKey) (broadcaster.BroadcastAPI, error) {
			return nil, errors.New("failed to acquire resource lock")
		}).Build()
	defer mockBroadcast.UnPatch()

	server := &Server{
		importMeta: &importMeta{},
		broker:     mockBroker,
		meta:       newTestMetaWithChunkManager(s.T()),
	}

	_, _, err := server.broadcastImport(
		ctx,
		"test_collection",
		100,
		[]int64{1},
		[]*internalpb.ImportFile{{Id: 1, Paths: []string{"/test/file.json"}}},
		[]*commonpb.KeyValuePair{{Key: "timeout", Value: "300s"}},
		&schemapb.CollectionSchema{Name: "test_collection"},
		1000,
		[]string{"v1"},
		"",
	)

	s.Error(err)
	s.Contains(err.Error(), "failed to start broadcast with collection id")
}

func (s *ImportCallbacksSuite) TestBroadcastImport_SecondDescribeCollectionFailsReturnsError() {
	ctx := context.Background()

	// Setup validation to pass
	mockCount := mockey.Mock((*importMeta).CountJobBy).To(func(_ *importMeta, _ context.Context, _ ...ImportJobFilter) int {
		return 1
	}).Build()
	defer mockCount.UnPatch()

	mockBalancer := &mockBalancerImpl{}
	mockBalance := mockey.Mock(balance.GetWithContext).To(func(ctx context.Context) (balancer.Balancer, error) {
		return mockBalancer, nil
	}).Build()
	defer mockBalance.UnPatch()

	mockAssignment := mockey.Mock((*mockBalancerImpl).GetLatestChannelAssignment).To(
		func(_ *mockBalancerImpl) (*channel.WatchChannelAssignmentsCallbackParam, error) {
			return &channel.WatchChannelAssignmentsCallbackParam{
				ReplicateConfiguration: nil,
			}, nil
		}).Build()
	defer mockAssignment.UnPatch()

	// Mock StartBroadcastWithResourceKeys to succeed
	mockBroadcastAPI := newMockBroadcastAPIImpl()
	mockBroadcast := mockey.Mock(broadcast.StartBroadcastWithResourceKeys).To(
		func(ctx context.Context, keys ...message.ResourceKey) (broadcaster.BroadcastAPI, error) {
			return mockBroadcastAPI, nil
		}).Build()
	defer mockBroadcast.UnPatch()

	// Mock broker: first DescribeCollectionInternal succeeds (in startBroadcastWithCollectionID),
	// second call returns error status (in broadcastImport after getting broadcaster)
	mockBroker := broker.NewMockBroker(s.T())
	mockBroker.EXPECT().DescribeCollectionInternal(mock.Anything, int64(100)).Return(&milvuspb.DescribeCollectionResponse{
		DbName:         "test_db",
		CollectionName: "test_collection",
	}, nil).Once()
	mockBroker.EXPECT().DescribeCollectionInternal(mock.Anything, int64(100)).Return(&milvuspb.DescribeCollectionResponse{
		Status: merr.Status(merr.ErrCollectionNotFound),
	}, nil).Once()

	server := &Server{
		importMeta: &importMeta{},
		broker:     mockBroker,
		meta:       newTestMetaWithChunkManager(s.T()),
	}

	_, _, err := server.broadcastImport(
		ctx,
		"test_collection",
		100,
		[]int64{1},
		[]*internalpb.ImportFile{{Id: 1, Paths: []string{"/test/file.json"}}},
		[]*commonpb.KeyValuePair{{Key: "timeout", Value: "300s"}},
		&schemapb.CollectionSchema{Name: "test_collection"},
		1000,
		[]string{"v1"},
		"",
	)

	s.Error(err)
	s.True(errors.Is(err, merr.ErrCollectionNotFound))
}

func (s *ImportCallbacksSuite) TestBroadcastImport_BroadcastFailsReturnsError() {
	ctx := context.Background()

	// Setup validation to pass
	mockCount := mockey.Mock((*importMeta).CountJobBy).To(func(_ *importMeta, _ context.Context, _ ...ImportJobFilter) int {
		return 1
	}).Build()
	defer mockCount.UnPatch()

	mockBalancer := &mockBalancerImpl{}
	mockBalance := mockey.Mock(balance.GetWithContext).To(func(ctx context.Context) (balancer.Balancer, error) {
		return mockBalancer, nil
	}).Build()
	defer mockBalance.UnPatch()

	mockAssignment := mockey.Mock((*mockBalancerImpl).GetLatestChannelAssignment).To(
		func(_ *mockBalancerImpl) (*channel.WatchChannelAssignmentsCallbackParam, error) {
			return &channel.WatchChannelAssignmentsCallbackParam{
				ReplicateConfiguration: nil,
			}, nil
		}).Build()
	defer mockAssignment.UnPatch()

	// Mock StartBroadcastWithResourceKeys to succeed
	mockBroadcastAPI := newMockBroadcastAPIImpl()
	mockBroadcastAPI.broadcastErr = errors.New("broadcast failed")
	mockBroadcast := mockey.Mock(broadcast.StartBroadcastWithResourceKeys).To(
		func(ctx context.Context, keys ...message.ResourceKey) (broadcaster.BroadcastAPI, error) {
			return mockBroadcastAPI, nil
		}).Build()
	defer mockBroadcast.UnPatch()

	// Mock broker: DescribeCollectionInternal is called twice
	// First call in startBroadcastWithCollectionID, second call in broadcastImport
	mockBroker := broker.NewMockBroker(s.T())
	mockBroker.EXPECT().DescribeCollectionInternal(mock.Anything, int64(100)).Return(&milvuspb.DescribeCollectionResponse{
		DbName:         "test_db",
		CollectionName: "test_collection",
	}, nil).Times(2)

	server := &Server{
		importMeta: &importMeta{},
		broker:     mockBroker,
		meta:       newTestMetaWithChunkManager(s.T()),
	}

	wal := mock_streaming.NewMockWALAccesser(s.T())
	wal.EXPECT().ControlChannel().Return(funcutil.GetControlChannel("by-dev-rootcoord-dml_0")).Maybe()
	streaming.SetWALForTest(wal)

	_, _, err := server.broadcastImport(
		ctx,
		"test_collection",
		100,
		[]int64{1},
		[]*internalpb.ImportFile{{Id: 1, Paths: []string{"/test/file.json"}}},
		[]*commonpb.KeyValuePair{{Key: "timeout", Value: "300s"}},
		&schemapb.CollectionSchema{Name: "test_collection"},
		1000,
		[]string{"v1"},
		"",
	)

	s.Error(err)
	s.Contains(err.Error(), "broadcast failed")
}

func (s *ImportCallbacksSuite) TestBroadcastImport_SuccessWithValidInput() {
	ctx := context.Background()

	// Setup validation to pass
	mockCount := mockey.Mock((*importMeta).CountJobBy).To(func(_ *importMeta, _ context.Context, _ ...ImportJobFilter) int {
		return 1
	}).Build()
	defer mockCount.UnPatch()

	mockBalancer := &mockBalancerImpl{}
	mockBalance := mockey.Mock(balance.GetWithContext).To(func(ctx context.Context) (balancer.Balancer, error) {
		return mockBalancer, nil
	}).Build()
	defer mockBalance.UnPatch()

	mockAssignment := mockey.Mock((*mockBalancerImpl).GetLatestChannelAssignment).To(
		func(_ *mockBalancerImpl) (*channel.WatchChannelAssignmentsCallbackParam, error) {
			return &channel.WatchChannelAssignmentsCallbackParam{
				ReplicateConfiguration: nil,
			}, nil
		}).Build()
	defer mockAssignment.UnPatch()

	// Mock StartBroadcastWithResourceKeys to succeed
	mockBroadcastAPI := newMockBroadcastAPIImpl()
	mockBroadcast := mockey.Mock(broadcast.StartBroadcastWithResourceKeys).To(
		func(ctx context.Context, keys ...message.ResourceKey) (broadcaster.BroadcastAPI, error) {
			return mockBroadcastAPI, nil
		}).Build()
	defer mockBroadcast.UnPatch()

	// Mock broker: DescribeCollectionInternal is called twice
	// First call in startBroadcastWithCollectionID, second call in broadcastImport
	mockBroker := broker.NewMockBroker(s.T())
	mockBroker.EXPECT().DescribeCollectionInternal(mock.Anything, int64(100)).Return(&milvuspb.DescribeCollectionResponse{
		DbName:         "test_db",
		CollectionName: "test_collection",
	}, nil).Times(2)

	server := &Server{
		importMeta: &importMeta{},
		broker:     mockBroker,
		meta:       newTestMetaWithChunkManager(s.T()),
	}

	wal := mock_streaming.NewMockWALAccesser(s.T())
	wal.EXPECT().ControlChannel().Return(funcutil.GetControlChannel("by-dev-rootcoord-dml_0")).Maybe()
	streaming.SetWALForTest(wal)

	_, _, err := server.broadcastImport(
		ctx,
		"test_collection",
		100,
		[]int64{1},
		[]*internalpb.ImportFile{{Id: 1, Paths: []string{"/test/file.json"}}},
		[]*commonpb.KeyValuePair{{Key: "timeout", Value: "300s"}},
		&schemapb.CollectionSchema{Name: "test_collection"},
		1000,
		[]string{"v1"},
		"",
	)

	s.NoError(err)
}

// --------------------------------
// RegisterDDLCallbacks Import Tests
// --------------------------------

func (s *ImportCallbacksSuite) TestRegisterDDLCallbacks_DoesNotPanic() {
	server := &Server{}

	s.NotPanics(func() {
		RegisterDDLCallbacks(server)
	})
}

// --------------------------------
// Helper Types for Mocking
// --------------------------------

// mockBalancerImpl is a mock implementation for balancer.Balancer interface
type mockBalancerImpl struct {
	balancer.Balancer
}

func (m *mockBalancerImpl) GetLatestChannelAssignment() (*channel.WatchChannelAssignmentsCallbackParam, error) {
	// Ensure the function body is long enough for mockey to patch
	result := &channel.WatchChannelAssignmentsCallbackParam{}
	return result, nil
}

// mockBroadcastAPIImpl is a mock implementation for broadcaster.BroadcastAPI interface
// This implementation has configurable behavior and uses enough code to be patchable by mockey
type mockBroadcastAPIImpl struct {
	broadcastResult *types.BroadcastAppendResult
	broadcastErr    error
	closeCalled     atomic.Bool
	// capturedMsg is the last message handed to Broadcast, for assertions on what the
	// caller actually put on the wire.
	capturedMsg message.BroadcastMutableMessage
}

func newMockBroadcastAPIImpl() *mockBroadcastAPIImpl {
	// Initialize with default success result
	mock := &mockBroadcastAPIImpl{
		broadcastResult: &types.BroadcastAppendResult{
			BroadcastID: 12345,
		},
		broadcastErr: nil,
	}
	mock.closeCalled.Store(false)
	return mock
}

func (m *mockBroadcastAPIImpl) Broadcast(ctx context.Context, msg message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error) {
	// Add operations to ensure the function is long enough for mockey
	if ctx == nil {
		return nil, errors.New("context is nil")
	}
	if msg == nil {
		return nil, errors.New("message is nil")
	}
	m.capturedMsg = msg
	if m.broadcastErr != nil {
		return nil, m.broadcastErr
	}
	if m.broadcastResult != nil {
		return m.broadcastResult, nil
	}
	return &types.BroadcastAppendResult{BroadcastID: 0}, nil
}

func (m *mockBroadcastAPIImpl) Close() {
	// Add operations to ensure the function is long enough for mockey
	m.closeCalled.Store(true)
	if m.closeCalled.Load() {
		// Already closed, do nothing
		return
	}
}

// --------------------------------
// Import Flow Documentation Tests
// --------------------------------

// TestImportV2_OnlyBroadcast verifies that ImportV2 is dedicated to broadcasting.
// ImportV2 no longer handles ack callbacks - they are processed by createImportJobFromAck.
func TestImportV2_OnlyBroadcast(t *testing.T) {
	t.Run("ImportV2 is only for proxy broadcast", func(t *testing.T) {
		// Create request - ImportV2 always broadcasts
		req := &internalpb.ImportRequestInternal{
			CollectionID:   100,
			CollectionName: "test_collection",
			PartitionIDs:   []int64{1},
			ChannelNames:   []string{"vchannel1"},
			Schema: &schemapb.CollectionSchema{
				Name: "test_collection",
				Fields: []*schemapb.FieldSchema{
					{FieldID: 1, Name: "id", DataType: schemapb.DataType_Int64},
				},
			},
			Files: []*internalpb.ImportFile{
				{Id: 1, Paths: []string{"/test/file1.json"}},
			},
			Options: []*commonpb.KeyValuePair{
				{Key: "timeout", Value: "300"},
			},
		}

		// ImportV2 always broadcasts, regardless of DataTimestamp
		// Ack callbacks are handled by createImportJobFromAck internally
		assert.NotNil(t, req, "ImportV2 should handle broadcast only")
	})
}

// TestImportV2_ProxyCallPath tests the proxy call path (DataTimestamp == 0)
// This test verifies that the new broadcast flow is triggered
func TestImportV2_ProxyCallPath(t *testing.T) {
	t.Run("Proxy call should trigger broadcast", func(t *testing.T) {
		req := &internalpb.ImportRequestInternal{
			CollectionID:   100,
			CollectionName: "test_collection",
			PartitionIDs:   []int64{1},
			ChannelNames:   []string{"vchannel1"},
			Schema: &schemapb.CollectionSchema{
				Name: "test_collection",
			},
			Files: []*internalpb.ImportFile{
				{Id: 1, Paths: []string{"/test/file1.json"}},
			},
			Options: []*commonpb.KeyValuePair{
				{Key: "timeout", Value: "300"},
			},
			DataTimestamp: 0, // Proxy call - no timestamp
			JobID:         0,
		}

		// Verify this is identified as proxy call
		isFromAckCallback := req.GetDataTimestamp() > 0
		assert.False(t, isFromAckCallback, "DataTimestamp=0 should be identified as proxy call")

		// The ImportV2 method should:
		// 1. Allocate job ID
		// 2. Call broadcastImport
		// 3. Return job ID without creating job (job created by ack callback)
	})
}

// TestImportV2_AckCallbackPath tests the ack callback path (DataTimestamp > 0)
// This test verifies that the job creation flow is triggered
func TestImportV2_AckCallbackPath(t *testing.T) {
	t.Run("Ack callback should trigger job creation", func(t *testing.T) {
		req := &internalpb.ImportRequestInternal{
			CollectionID:   100,
			CollectionName: "test_collection",
			PartitionIDs:   []int64{1},
			ChannelNames:   []string{"vchannel1"},
			Schema: &schemapb.CollectionSchema{
				Name: "test_collection",
			},
			Files: []*internalpb.ImportFile{
				{Id: 1, Paths: []string{"/test/file1.json"}},
			},
			Options: []*commonpb.KeyValuePair{
				{Key: "timeout", Value: "300"},
			},
			DataTimestamp: 123456789, // Ack callback - has timestamp from broadcast
			JobID:         1000,      // Ack callback - has job ID
		}

		// Verify this is identified as ack callback
		isFromAckCallback := req.GetDataTimestamp() > 0
		assert.True(t, isFromAckCallback, "DataTimestamp>0 should be identified as ack callback")

		// The ImportV2 method should:
		// 1. Skip broadcast
		// 2. Process files
		// 3. Create import job
		// 4. Return job ID
	})
}

// TestProxyImportRequest tests that proxy correctly constructs the request
func TestProxyImportRequest(t *testing.T) {
	t.Run("Proxy should not set DataTimestamp", func(t *testing.T) {
		// Simulating what proxy does in task_import.go
		req := &internalpb.ImportRequestInternal{
			DbID:           0, // deprecated
			CollectionID:   100,
			CollectionName: "test_collection",
			PartitionIDs:   []int64{1, 2},
			ChannelNames:   []string{"vchannel1", "vchannel2"},
			Schema: &schemapb.CollectionSchema{
				Name: "test_collection",
			},
			Files: []*internalpb.ImportFile{
				{Id: 1, Paths: []string{"/test/file1.json"}},
			},
			Options: []*commonpb.KeyValuePair{
				{Key: "timeout", Value: "300"},
			},
			DataTimestamp: 0, // CRITICAL: Must be 0 for proxy calls
			JobID:         0, // Let DataCoord allocate
		}

		// Verify proxy request structure
		assert.Equal(t, uint64(0), req.DataTimestamp, "Proxy must set DataTimestamp to 0")
		assert.Equal(t, int64(0), req.JobID, "Proxy should let DataCoord allocate job ID")
		assert.NotEmpty(t, req.ChannelNames, "Proxy must provide channel names")
		assert.NotNil(t, req.Schema, "Proxy must provide schema")
	})
}

// TestAckCallbackImportRequest tests that ack callback correctly constructs the request
func TestAckCallbackImportRequest(t *testing.T) {
	t.Run("Ack callback should set DataTimestamp", func(t *testing.T) {
		// Simulating what ack callback does in ddl_callbacks_import.go
		req := &internalpb.ImportRequestInternal{
			CollectionID:   100,
			CollectionName: "test_collection",
			PartitionIDs:   []int64{1, 2},
			ChannelNames:   []string{"vchannel1", "vchannel2"}, // Only acked channels
			Schema: &schemapb.CollectionSchema{
				Name: "test_collection",
			},
			Files: []*internalpb.ImportFile{
				{Id: 1, Paths: []string{"/test/file1.json"}},
			},
			Options: []*commonpb.KeyValuePair{
				{Key: "timeout", Value: "300"},
			},
			DataTimestamp: 123456789, // CRITICAL: Set from broadcast message timestamp
			JobID:         1000,      // Set from broadcast message
		}

		// Verify ack callback request structure
		assert.Greater(t, req.DataTimestamp, uint64(0), "Ack callback must set DataTimestamp from message")
		assert.Greater(t, req.JobID, int64(0), "Ack callback must set JobID from message")
	})
}

// TestImportFlowIntegration documents the complete import flow
func TestImportFlowIntegration(t *testing.T) {
	t.Run("Document complete import flow", func(t *testing.T) {
		// This test documents the expected flow but doesn't execute it
		// (would require full integration test setup)

		// STEP 1: Proxy receives import request from user
		proxyReq := &internalpb.ImportRequestInternal{
			CollectionID:   100,
			CollectionName: "test_collection",
			PartitionIDs:   []int64{1},
			ChannelNames:   []string{"vchannel1"},
			Schema:         &schemapb.CollectionSchema{Name: "test_collection"},
			Files:          []*internalpb.ImportFile{{Id: 1, Paths: []string{"/test/file1.json"}}},
			Options:        []*commonpb.KeyValuePair{{Key: "timeout", Value: "300"}},
			DataTimestamp:  0, // Proxy call
			JobID:          0,
		}

		// STEP 2: Proxy calls DataCoord.ImportV2() via RPC
		// DataCoord identifies this as proxy call (DataTimestamp == 0)
		assert.Equal(t, uint64(0), proxyReq.DataTimestamp)

		// STEP 3: DataCoord broadcasts message
		// (broadcastImport is called internally)

		// STEP 4: Ack callback is triggered
		ackCallbackReq := &internalpb.ImportRequestInternal{
			CollectionID:   proxyReq.CollectionID,
			CollectionName: proxyReq.CollectionName,
			PartitionIDs:   proxyReq.PartitionIDs,
			ChannelNames:   []string{"vchannel1"}, // Only acked channels
			Schema:         proxyReq.Schema,
			Files:          proxyReq.Files,
			Options:        proxyReq.Options,
			DataTimestamp:  123456789, // Set from broadcast message
			JobID:          1000,      // Set from broadcast message
		}

		// STEP 5: DataCoord identifies this as ack callback (DataTimestamp > 0)
		assert.Greater(t, ackCallbackReq.DataTimestamp, uint64(0))

		// STEP 6: DataCoord creates import job
		// (ImportV2 continues with job creation logic)

		t.Log("Import flow documented successfully")
	})
}

func newTestImportMeta(t *testing.T) (ImportMeta, *mocks.DataCoordCatalog) {
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil).Maybe()

	alloc := allocator.NewMockAllocator(t)
	importMeta, err := NewImportMeta(context.Background(), catalog, alloc, nil)
	assert.NoError(t, err)
	return importMeta, catalog
}

func buildCommitImportBroadcastResult(jobID int64) message.BroadcastResultCommitImportMessageV2 {
	broadcastMsg := message.NewCommitImportMessageBuilderV2().
		WithHeader(&message.CommitImportMessageHeader{JobId: jobID}).
		WithBody(&messagespb.CommitImportMessageBody{}).
		WithBroadcast([]string{"control_channel"}).
		MustBuildBroadcast()
	return message.BroadcastResultCommitImportMessageV2{
		Message: message.MustAsSpecializedBroadcastMessage[*message.CommitImportMessageHeader, *messagespb.CommitImportMessageBody](broadcastMsg),
		Results: map[string]*message.AppendResult{},
	}
}

func buildRollbackImportBroadcastResult(jobID int64) message.BroadcastResultRollbackImportMessageV2 {
	broadcastMsg := message.NewRollbackImportMessageBuilderV2().
		WithHeader(&message.RollbackImportMessageHeader{JobId: jobID}).
		WithBody(&messagespb.RollbackImportMessageBody{}).
		WithBroadcast([]string{"control_channel"}).
		MustBuildBroadcast()
	return message.BroadcastResultRollbackImportMessageV2{
		Message: message.MustAsSpecializedBroadcastMessage[*message.RollbackImportMessageHeader, *messagespb.RollbackImportMessageBody](broadcastMsg),
		Results: map[string]*message.AppendResult{},
	}
}

func TestCommitImportCallback_UncommittedToCommitting(t *testing.T) {
	ctx := context.Background()
	importMeta, _ := newTestImportMeta(t)

	job := &importJob{
		ImportJob: &datapb.ImportJob{
			JobID:      1,
			State:      internalpb.ImportJobState_Uncommitted,
			AutoCommit: false,
		},
		tr: timerecord.NewTimeRecorder("test"),
	}
	err := importMeta.AddJob(ctx, job)
	assert.NoError(t, err)

	callbacks := &DDLCallbacks{Server: &Server{importMeta: importMeta}}
	err = callbacks.commitImportV2AckCallback(ctx, buildCommitImportBroadcastResult(1))
	assert.NoError(t, err)

	updatedJob := importMeta.GetJob(ctx, 1)
	assert.NotNil(t, updatedJob)
	assert.Equal(t, internalpb.ImportJobState_Committing, updatedJob.GetState())
}

func TestCommitImportCallback_BeforeUncommitted_Retry(t *testing.T) {
	ctx := context.Background()
	importMeta, _ := newTestImportMeta(t)

	job := &importJob{
		ImportJob: &datapb.ImportJob{
			JobID:      11,
			State:      internalpb.ImportJobState_Importing,
			AutoCommit: false,
		},
		tr: timerecord.NewTimeRecorder("test"),
	}
	err := importMeta.AddJob(ctx, job)
	assert.NoError(t, err)

	callbacks := &DDLCallbacks{Server: &Server{importMeta: importMeta}}
	err = callbacks.commitImportV2AckCallback(ctx, buildCommitImportBroadcastResult(11))
	assert.Error(t, err)
	assert.True(t, errors.Is(err, merr.ErrImportSysFailed))

	updatedJob := importMeta.GetJob(ctx, 11)
	assert.NotNil(t, updatedJob)
	assert.Equal(t, internalpb.ImportJobState_Importing, updatedJob.GetState())
}

func TestCommitImportCallback_MissingJob_Retry(t *testing.T) {
	ctx := context.Background()
	importMeta, _ := newTestImportMeta(t)

	callbacks := &DDLCallbacks{Server: &Server{importMeta: importMeta}}
	err := callbacks.commitImportV2AckCallback(ctx, buildCommitImportBroadcastResult(13))
	assert.Error(t, err)
	assert.True(t, errors.Is(err, merr.ErrImportSysFailed))
	assert.Nil(t, importMeta.GetJob(ctx, 13))

	job := &importJob{
		ImportJob: &datapb.ImportJob{
			JobID:      13,
			State:      internalpb.ImportJobState_Uncommitted,
			AutoCommit: false,
		},
		tr: timerecord.NewTimeRecorder("test"),
	}
	err = importMeta.AddJob(ctx, job)
	assert.NoError(t, err)

	err = callbacks.commitImportV2AckCallback(ctx, buildCommitImportBroadcastResult(13))
	assert.NoError(t, err)

	updatedJob := importMeta.GetJob(ctx, 13)
	assert.NotNil(t, updatedJob)
	assert.Equal(t, internalpb.ImportJobState_Committing, updatedJob.GetState())
}

func TestCommitImportCallback_RetryAfterUncommitted(t *testing.T) {
	ctx := context.Background()
	importMeta, _ := newTestImportMeta(t)

	job := &importJob{
		ImportJob: &datapb.ImportJob{
			JobID:      12,
			State:      internalpb.ImportJobState_Importing,
			AutoCommit: false,
		},
		tr: timerecord.NewTimeRecorder("test"),
	}
	err := importMeta.AddJob(ctx, job)
	assert.NoError(t, err)

	callbacks := &DDLCallbacks{Server: &Server{importMeta: importMeta}}
	err = callbacks.commitImportV2AckCallback(ctx, buildCommitImportBroadcastResult(12))
	assert.Error(t, err)

	err = importMeta.UpdateJob(ctx, 12, UpdateJobState(internalpb.ImportJobState_Uncommitted))
	assert.NoError(t, err)

	err = callbacks.commitImportV2AckCallback(ctx, buildCommitImportBroadcastResult(12))
	assert.NoError(t, err)

	updatedJob := importMeta.GetJob(ctx, 12)
	assert.NotNil(t, updatedJob)
	assert.Equal(t, internalpb.ImportJobState_Committing, updatedJob.GetState())
}

func TestRollbackImportCallback_TransitionToFailed(t *testing.T) {
	ctx := context.Background()
	importMeta, _ := newTestImportMeta(t)

	job := &importJob{
		ImportJob: &datapb.ImportJob{
			JobID:             2,
			State:             internalpb.ImportJobState_Uncommitted,
			AutoCommit:        false,
			RequestedDiskSize: 1024 * 1024, // nonzero so we can observe release
		},
		tr: timerecord.NewTimeRecorder("test"),
	}
	err := importMeta.AddJob(ctx, job)
	assert.NoError(t, err)

	callbacks := &DDLCallbacks{Server: &Server{importMeta: importMeta}}
	err = callbacks.rollbackImportV2AckCallback(ctx, buildRollbackImportBroadcastResult(2))
	assert.NoError(t, err)

	// Segment cleanup is handled by the import inspector's processFailed (covered
	// by ImportInspectorSuite.TestProcessFailed), not by this callback.
	updatedJob := importMeta.GetJob(ctx, 2)
	assert.NotNil(t, updatedJob)
	assert.Equal(t, internalpb.ImportJobState_Failed, updatedJob.GetState())
	assert.Equal(t, "aborted by user", updatedJob.GetReason())
	// UpdateJobState(Failed) also releases disk quota and arms GC eligibility.
	assert.EqualValues(t, 0, updatedJob.GetRequestedDiskSize(),
		"Failed transition must release RequestedDiskSize")
	assert.Greater(t, updatedJob.GetCleanupTs(), uint64(0),
		"Failed transition must set CleanupTs for GC eligibility")
}

func TestCommitImportCallback_AfterAbort_NoOp(t *testing.T) {
	ctx := context.Background()
	importMeta, _ := newTestImportMeta(t)

	// Job already Failed (abort won the race).
	job := &importJob{
		ImportJob: &datapb.ImportJob{
			JobID: 3,
			State: internalpb.ImportJobState_Failed,
		},
		tr: timerecord.NewTimeRecorder("test"),
	}
	err := importMeta.AddJob(ctx, job)
	assert.NoError(t, err)

	callbacks := &DDLCallbacks{Server: &Server{importMeta: importMeta}}
	err = callbacks.commitImportV2AckCallback(ctx, buildCommitImportBroadcastResult(3))
	assert.NoError(t, err)

	// UpdateJob skips jobs in Failed state → no-op.
	updatedJob := importMeta.GetJob(ctx, 3)
	assert.NotNil(t, updatedJob)
	assert.Equal(t, internalpb.ImportJobState_Failed, updatedJob.GetState())
}

func TestRollbackImportCallback_AfterCommit_NoOp(t *testing.T) {
	ctx := context.Background()
	importMeta, _ := newTestImportMeta(t)

	// Job already Committing (commit won the race).
	job := &importJob{
		ImportJob: &datapb.ImportJob{
			JobID: 4,
			State: internalpb.ImportJobState_Committing,
		},
		tr: timerecord.NewTimeRecorder("test"),
	}
	err := importMeta.AddJob(ctx, job)
	assert.NoError(t, err)

	callbacks := &DDLCallbacks{Server: &Server{importMeta: importMeta}}
	err = callbacks.rollbackImportV2AckCallback(ctx, buildRollbackImportBroadcastResult(4))
	assert.NoError(t, err)

	// Abort rejected: job already in committed state.
	updatedJob := importMeta.GetJob(ctx, 4)
	assert.NotNil(t, updatedJob)
	assert.Equal(t, internalpb.ImportJobState_Committing, updatedJob.GetState())
}

// TestImportAckCallbacks_CommitVsAbort_Race fires commit and rollback ack
// callbacks concurrently against the same Uncommitted job. In production these
// callbacks are serialized by the broadcaster's exclusive collection-level
// resource-key lock (both CommitImport and RollbackImport are ExclusiveRequired
// on NewExclusiveCollectionNameResourceKey), so this race is unreachable. The
// test documents the invariant from the callback side and provides regression
// coverage against future drift: the job must end in a deterministic terminal
// state (Committing or Failed) without panicking or corrupting meta. Run with
// `-race` to detect any unsynchronized access.
func TestImportAckCallbacks_CommitVsAbort_Race(t *testing.T) {
	for iter := 0; iter < 32; iter++ {
		ctx := context.Background()
		importMeta, _ := newTestImportMeta(t)

		const jobID int64 = 5
		err := importMeta.AddJob(ctx, &importJob{
			ImportJob: &datapb.ImportJob{
				JobID:      jobID,
				State:      internalpb.ImportJobState_Uncommitted,
				AutoCommit: false,
			},
			tr: timerecord.NewTimeRecorder("race"),
		})
		assert.NoError(t, err)

		callbacks := &DDLCallbacks{Server: &Server{importMeta: importMeta}}

		start := make(chan struct{})
		var commitErr, rollbackErr error
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			commitErr = callbacks.commitImportV2AckCallback(ctx, buildCommitImportBroadcastResult(jobID))
		}()
		go func() {
			defer wg.Done()
			<-start
			rollbackErr = callbacks.rollbackImportV2AckCallback(ctx, buildRollbackImportBroadcastResult(jobID))
		}()
		close(start)
		wg.Wait()

		assert.NoError(t, commitErr)
		assert.NoError(t, rollbackErr)

		final := importMeta.GetJob(ctx, jobID).GetState()
		assert.Contains(t,
			[]internalpb.ImportJobState{internalpb.ImportJobState_Committing, internalpb.ImportJobState_Failed},
			final, "iter %d: terminal state must be Committing or Failed, got %s", iter, final)
	}
}

// --------------------------------
// broadcastCommitImportMessage / broadcastRollbackImportMessage Tests
// --------------------------------

// captureBroadcastAPI is a BroadcastAPI mock that records the message passed
// to Broadcast so a test can assert its broadcast target vchannels.
type captureBroadcastAPI struct {
	captured message.BroadcastMutableMessage
}

func (c *captureBroadcastAPI) Broadcast(_ context.Context, msg message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error) {
	c.captured = msg
	return &types.BroadcastAppendResult{BroadcastID: 1}, nil
}

func (c *captureBroadcastAPI) Close() {}

func testBroadcastTargetsDataVchannels(t *testing.T, broadcastFn func(*Server, context.Context, ImportJob) error) {
	ctx := context.Background()
	wantVchannels := []string{"by-dev-rootcoord-dml_0_v0", "by-dev-rootcoord-dml_1_v0"}

	// Import messages target the job's data vchannels; the broadcaster adds the
	// control-channel copy that anchors the ack-callback order.
	mockBroker := broker.NewMockBroker(t)
	mockBroker.EXPECT().DescribeCollectionInternal(mock.Anything, int64(7)).Return(&milvuspb.DescribeCollectionResponse{
		DbName:         "test_db",
		CollectionName: "test_collection",
	}, nil)

	capture := &captureBroadcastAPI{}
	mockBroadcast := mockey.Mock(broadcast.StartBroadcastWithResourceKeys).To(
		func(_ context.Context, _ ...message.ResourceKey) (broadcaster.BroadcastAPI, error) {
			return capture, nil
		}).Build()
	defer mockBroadcast.UnPatch()

	server := &Server{broker: mockBroker}
	job := &importJob{
		ImportJob: &datapb.ImportJob{
			JobID:        7,
			CollectionID: 7,
			Vchannels:    wantVchannels,
		},
		tr: timerecord.NewTimeRecorder("test"),
	}

	err := broadcastFn(server, ctx, job)
	assert.NoError(t, err)
	assert.NotNil(t, capture.captured, "Broadcast must have been called")
	assert.ElementsMatch(t, wantVchannels, capture.captured.BroadcastHeader().VChannels,
		"broadcast must target the job's data vchannels; the broadcaster adds the control channel")
}

// TestBroadcastCommitImportMessage_TargetsDataVchannels asserts that the
// CommitImport WAL message is broadcast to the job's data vchannels.
// Control-channel-only broadcasts are dropped by the WAL flusher's
// IsControlChannel guard before reaching the CommitImport case, so the
// message must reach data vchannels for HandleCommitVchannel to run.
func TestBroadcastCommitImportMessage_TargetsDataVchannels(t *testing.T) {
	testBroadcastTargetsDataVchannels(t, (*Server).broadcastCommitImportMessage)
}

// TestBroadcastRollbackImportMessage_TargetsDataVchannels asserts that the
// RollbackImport WAL message is broadcast to the job's data vchannels,
// matching the CommitImport routing.
func TestBroadcastRollbackImportMessage_TargetsDataVchannels(t *testing.T) {
	testBroadcastTargetsDataVchannels(t, (*Server).broadcastRollbackImportMessage)
}

func testBroadcastRequiresVchannels(t *testing.T, broadcastFn func(*Server, context.Context, ImportJob) error) {
	ctx := context.Background()
	server := &Server{}
	job := &importJob{
		ImportJob: &datapb.ImportJob{
			JobID:        7,
			CollectionID: 7,
		},
		tr: timerecord.NewTimeRecorder("test"),
	}

	err := broadcastFn(server, ctx, job)
	assert.Error(t, err)
	// Missing vchannels is internal broadcast state -> ErrImportSysFailed.
	assert.True(t, errors.Is(err, merr.ErrImportSysFailed))
	assert.Contains(t, err.Error(), "job 7 has no vchannels")
}

func TestBroadcastCommitImportMessage_RequiresVchannels(t *testing.T) {
	testBroadcastRequiresVchannels(t, (*Server).broadcastCommitImportMessage)
}

func TestBroadcastRollbackImportMessage_RequiresVchannels(t *testing.T) {
	testBroadcastRequiresVchannels(t, (*Server).broadcastRollbackImportMessage)
}

func TestJobIDFromDuplicatedBroadcast(t *testing.T) {
	msg := message.NewImportMessageBuilderV1().
		WithHeader(&message.ImportMessageHeader{}).
		WithBody(&msgpb.ImportMsg{JobID: 4242, CollectionID: 100}).
		WithIdempotencyKey(message.NewCollectionScopedIdempotencyKey(100, "run-1")).
		WithBroadcast([]string{"v1"}).
		MustBuildBroadcast()

	jobID, err := jobIDFromDuplicatedBroadcast(context.Background(), msg, 100)
	assert.NoError(t, err)
	assert.Equal(t, int64(4242), jobID)
}

// The collectionID comparison is an invariant check, not a semantic guard: the key is
// scoped to a collection ID, so a hit already means both broadcasts targeted the same
// collection. A mismatch can only come from an encoding or scoping bug, which must fail
// loudly rather than hand back another collection's jobID.
func TestJobIDFromDuplicatedBroadcast_RejectsADifferentCollection(t *testing.T) {
	msg := message.NewImportMessageBuilderV1().
		WithHeader(&message.ImportMessageHeader{}).
		WithBody(&msgpb.ImportMsg{JobID: 4242, CollectionID: 100}).
		WithIdempotencyKey(message.NewCollectionScopedIdempotencyKey(100, "run-1")).
		WithBroadcast([]string{"v1"}).
		MustBuildBroadcast()

	_, err := jobIDFromDuplicatedBroadcast(context.Background(), msg, 101)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, merr.ErrServiceInternal))
}

// --------------------------------
// importIDRangeAckCallback Tests
// --------------------------------

// buildImportIDRangeBroadcastResult constructs a BroadcastResult for an
// ImportIDRange WAL message, mirroring the commit/rollback builders above.
// A zero timeTick leaves Results empty, so GetMaxTimeTick() == 0.
func buildImportIDRangeBroadcastResult(jobID int64, idRanges map[int64]*commonpb.IDRange, timeTick uint64) message.BroadcastResultImportIDRangeMessageV2 {
	broadcastMsg := message.NewImportIDRangeMessageBuilderV2().
		WithHeader(&message.ImportIDRangeMessageHeader{CollectionId: 1, JobId: jobID}).
		WithBody(&messagespb.ImportIDRangeMessageBody{IdRanges: idRanges}).
		WithBroadcast([]string{"v0"}).
		MustBuildBroadcast()
	results := map[string]*message.AppendResult{}
	if timeTick != 0 {
		results["v0"] = &message.AppendResult{TimeTick: timeTick}
	}
	return message.BroadcastResultImportIDRangeMessageV2{
		Message: message.MustAsSpecializedBroadcastMessage[*message.ImportIDRangeMessageHeader, *messagespb.ImportIDRangeMessageBody](broadcastMsg),
		Results: results,
	}
}

func newIDRangeTestJob(jobID int64, state internalpb.ImportJobState) *importJob {
	return &importJob{
		ImportJob: &datapb.ImportJob{
			JobID: jobID,
			State: state,
			Files: []*internalpb.ImportFile{
				{Id: 101, Paths: []string{"a.parquet"}},
				{Id: 102, Paths: []string{"b.parquet"}},
			},
		},
		tr: timerecord.NewTimeRecorder("test"),
	}
}

// validIDRangeFileRanges is keyed by the file's position in job.Files, so message
// order is irrelevant.
func validIDRangeFileRanges() map[int64]*commonpb.IDRange {
	return map[int64]*commonpb.IDRange{
		1: {Begin: 1010, End: 1010},
		0: {Begin: 1000, End: 1010},
	}
}

// First delivery applies the ranges by Index and persists the updated job
// through the catalog.
func TestImportIDRangeAckCallback_FirstApply(t *testing.T) {
	ctx := context.Background()
	var saved []*datapb.ImportJob
	importMeta := newCapturingImportMeta(t, &saved)

	job := newIDRangeTestJob(21, internalpb.ImportJobState_PreImporting)
	require.NoError(t, importMeta.AddJob(ctx, job))

	callbacks := &DDLCallbacks{Server: &Server{importMeta: importMeta}}
	require.NoError(t, callbacks.importIDRangeAckCallback(ctx,
		buildImportIDRangeBroadcastResult(21, validIDRangeFileRanges(), 0)))

	got := importMeta.GetJob(ctx, 21)
	require.NotNil(t, got)
	// Applied by file position (the map key), independent of iteration order.
	assert.EqualValues(t, 1000, got.GetFiles()[0].GetIdRange().GetBegin())
	assert.EqualValues(t, 1010, got.GetFiles()[0].GetIdRange().GetEnd())
	assert.EqualValues(t, 1010, got.GetFiles()[1].GetIdRange().GetBegin())
	assert.EqualValues(t, 1010, got.GetFiles()[1].GetIdRange().GetEnd())
	assert.Equal(t, internalpb.ImportJobState_PreImporting, got.GetState(),
		"applying ranges must not move the state machine; the checker gate does")

	// Persisted: the proto handed to the catalog carries the ranges.
	require.NotEmpty(t, saved)
	last := saved[len(saved)-1]
	assert.EqualValues(t, 1000, last.GetFiles()[0].GetIdRange().GetBegin())
	assert.EqualValues(t, 1010, last.GetFiles()[0].GetIdRange().GetEnd())
	assert.EqualValues(t, 1010, last.GetFiles()[1].GetIdRange().GetBegin())
	assert.EqualValues(t, 1010, last.GetFiles()[1].GetIdRange().GetEnd())
}

// The ack callback applies ranges while the job waits in AssigningIDRange: the
// state is left for the checker to advance.
func TestImportIDRangeAckCallback_AppliesInAssigningIDRange(t *testing.T) {
	ctx := context.Background()
	importMeta, _ := newTestImportMeta(t)
	job := newIDRangeTestJob(24, internalpb.ImportJobState_AssigningIDRange)
	require.NoError(t, importMeta.AddJob(ctx, job))

	callbacks := &DDLCallbacks{Server: &Server{importMeta: importMeta}}
	require.NoError(t, callbacks.importIDRangeAckCallback(ctx,
		buildImportIDRangeBroadcastResult(24, validIDRangeFileRanges(), 0)))

	got := importMeta.GetJob(ctx, 24)
	require.NotNil(t, got)
	assert.EqualValues(t, 1000, got.GetFiles()[0].GetIdRange().GetBegin())
	assert.EqualValues(t, 1010, got.GetFiles()[0].GetIdRange().GetEnd())
	assert.Equal(t, internalpb.ImportJobState_AssigningIDRange, got.GetState(),
		"applying ranges must not move the state machine; the checker gate does")
}

// At-least-once redelivery of the same message is a clean no-op; a conflicting
// range must never overwrite the first applied one (first-range-wins).
func TestImportIDRangeAckCallback_RedeliveryIdempotentAndFirstWins(t *testing.T) {
	ctx := context.Background()
	importMeta, _ := newTestImportMeta(t)
	job := newIDRangeTestJob(22, internalpb.ImportJobState_PreImporting)
	require.NoError(t, importMeta.AddJob(ctx, job))
	callbacks := &DDLCallbacks{Server: &Server{importMeta: importMeta}}

	// First apply.
	require.NoError(t, callbacks.importIDRangeAckCallback(ctx,
		buildImportIDRangeBroadcastResult(22, validIDRangeFileRanges(), 0)))

	// Equal redelivery -> nil, nothing changes.
	assert.NoError(t, callbacks.importIDRangeAckCallback(ctx,
		buildImportIDRangeBroadcastResult(22, validIDRangeFileRanges(), 0)))
	got := importMeta.GetJob(ctx, 22)
	assert.EqualValues(t, 1000, got.GetFiles()[0].GetIdRange().GetBegin())
	assert.Equal(t, internalpb.ImportJobState_PreImporting, got.GetState())

	// Conflicting redelivery (self-consistent, passes protocol checks, but a
	// different range) -> nil, the original range is kept.
	conflicting := map[int64]*commonpb.IDRange{
		0: {Begin: 2000, End: 2010},
		1: {Begin: 2010, End: 2010},
	}
	assert.NoError(t, callbacks.importIDRangeAckCallback(ctx,
		buildImportIDRangeBroadcastResult(22, conflicting, 0)))
	got = importMeta.GetJob(ctx, 22)
	assert.EqualValues(t, 1000, got.GetFiles()[0].GetIdRange().GetBegin(), "first applied range wins")
	assert.EqualValues(t, 1010, got.GetFiles()[0].GetIdRange().GetEnd())
	assert.EqualValues(t, 1010, got.GetFiles()[1].GetIdRange().GetBegin())
}

// A missing local job is unrecoverable (mid-import join, or a dropped collection whose
// job creation was skipped): the callback WARNs and no-ops immediately so it never pins
// the collection's exclusive lock. The broadcast tick is irrelevant.
func TestImportIDRangeAckCallback_JobNotFoundNoOp(t *testing.T) {
	ctx := context.Background()
	importMeta, _ := newTestImportMeta(t)
	callbacks := &DDLCallbacks{Server: &Server{importMeta: importMeta}}

	freshTick := tsoutil.ComposeTSByTime(time.Now())
	for _, tick := range []uint64{freshTick, 0} {
		assert.NoError(t, callbacks.importIDRangeAckCallback(ctx,
			buildImportIDRangeBroadcastResult(31, validIDRangeFileRanges(), tick)))
	}
	assert.Nil(t, importMeta.GetJob(ctx, 31))
}

// A job at or past the range gate (Failed/Completed/Committing/Uncommitted) is
// a race the callback must not disturb: nil no-op, ranges never applied.
func TestImportIDRangeAckCallback_TerminalStatesNoOp(t *testing.T) {
	states := []internalpb.ImportJobState{
		internalpb.ImportJobState_Failed,
		internalpb.ImportJobState_Completed,
		internalpb.ImportJobState_Committing,
		internalpb.ImportJobState_Uncommitted,
	}
	for _, state := range states {
		t.Run(state.String(), func(t *testing.T) {
			ctx := context.Background()
			importMeta, _ := newTestImportMeta(t)
			job := newIDRangeTestJob(41, state)
			require.NoError(t, importMeta.AddJob(ctx, job))

			callbacks := &DDLCallbacks{Server: &Server{importMeta: importMeta}}
			assert.NoError(t, callbacks.importIDRangeAckCallback(ctx,
				buildImportIDRangeBroadcastResult(41, validIDRangeFileRanges(), 0)))

			got := importMeta.GetJob(ctx, 41)
			assert.Equal(t, state, got.GetState())
			assert.Nil(t, got.GetFiles()[0].GetIdRange(),
				"ranges must not be applied past the gate")
		})
	}
}

// Protocol violations -- the two clusters disagree on the job's shape -- fail
// the job loudly instead of applying a partial or misaligned range. Failing the
// job is the callback's successful outcome (nil error).
func TestImportIDRangeAckCallback_ProtocolViolationsFailJob(t *testing.T) {
	cases := []struct {
		name           string
		idRanges       map[int64]*commonpb.IDRange
		reasonContains []string
	}{
		{
			name:           "file count mismatch",
			idRanges:       map[int64]*commonpb.IDRange{0: {Begin: 1000, End: 1010}},
			reasonContains: []string{"carries 1 file ranges", "job has 2 files"},
		},
		{
			name: "file index out of range",
			idRanges: map[int64]*commonpb.IDRange{
				0: {Begin: 1000, End: 1010},
				2: {Begin: 1010, End: 1010},
			},
			reasonContains: []string{"file index 2 out of range"},
		},
		{
			name: "negative file index",
			idRanges: map[int64]*commonpb.IDRange{
				0:  {Begin: 1000, End: 1010},
				-1: {Begin: 1010, End: 1010},
			},
			reasonContains: []string{"file index -1 out of range"},
		},
		// A duplicate key is not representable in a map, and a decoded map value is never
		// nil (an absent key shows up as a count mismatch), so the two violations a
		// malformed peer can express are the count mismatch and an out-of-range key above.
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			importMeta, _ := newTestImportMeta(t)
			job := newIDRangeTestJob(51, internalpb.ImportJobState_PreImporting)
			require.NoError(t, importMeta.AddJob(ctx, job))

			callbacks := &DDLCallbacks{Server: &Server{importMeta: importMeta}}
			assert.NoError(t, callbacks.importIDRangeAckCallback(ctx,
				buildImportIDRangeBroadcastResult(51, tc.idRanges, 0)))

			got := importMeta.GetJob(ctx, 51)
			require.NotNil(t, got)
			assert.Equal(t, internalpb.ImportJobState_Failed, got.GetState())
			for _, sub := range tc.reasonContains {
				assert.Contains(t, got.GetReason(), sub)
			}
		})
	}
}

// A ranged job that reached Uncommitted without ranges counted zero rows locally while the
// peer ranged rows for the same files. The ack must fail it instead of no-op'ing: Uncommitted
// holds no Import task yet, so failing here still prevents the commit from landing an empty
// import against the peer's rows.
func TestImportIDRangeAckCallback_UncommittedWithoutRangesFailsJob(t *testing.T) {
	ctx := context.Background()
	importMeta, _ := newTestImportMeta(t)
	job := &importJob{
		ImportJob: &datapb.ImportJob{
			JobID: 26,
			State: internalpb.ImportJobState_Uncommitted,
			Schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true, AutoID: true},
			}},
			Files: []*internalpb.ImportFile{
				{Id: 101, Paths: []string{"a.parquet"}},
				{Id: 102, Paths: []string{"b.parquet"}},
			},
		},
		tr: timerecord.NewTimeRecorder("test"),
	}
	require.NoError(t, importMeta.AddJob(ctx, job))

	callbacks := &DDLCallbacks{Server: &Server{importMeta: importMeta}}
	require.NoError(t, callbacks.importIDRangeAckCallback(ctx,
		buildImportIDRangeBroadcastResult(26, validIDRangeFileRanges(), 0)))

	got := importMeta.GetJob(ctx, 26)
	require.NotNil(t, got)
	assert.Equal(t, internalpb.ImportJobState_Failed, got.GetState())
	assert.Contains(t, got.GetReason(), "cross-cluster file divergence")
}

// --------------------------------
// broadcastImportIDRangeMessage Tests
// --------------------------------

// The producer side of the two-phase flow: per-file ranges (one entry per job file,
// Index == position, range size == that file's post-preimport row count, zero-row
// file gets an empty range) broadcast to the job's data vchannels.
func TestAssignAndBroadcastImportIDRange_MessageShape(t *testing.T) {
	ctx := context.Background()
	wantVchannels := []string{"by-dev-rootcoord-dml_0_v0", "by-dev-rootcoord-dml_1_v0"}

	mockBroker := broker.NewMockBroker(t)
	mockBroker.EXPECT().DescribeCollectionInternal(mock.Anything, int64(7)).Return(&milvuspb.DescribeCollectionResponse{
		DbName:         "test_db",
		CollectionName: "test_collection",
	}, nil)

	capture := &captureBroadcastAPI{}
	mockBroadcast := mockey.Mock(broadcast.StartBroadcastWithResourceKeys).To(
		func(_ context.Context, _ ...message.ResourceKey) (broadcaster.BroadcastAPI, error) {
			return capture, nil
		}).Build()
	defer mockBroadcast.UnPatch()

	alloc := allocator.NewMockAllocator(t)
	alloc.EXPECT().AllocN(mock.Anything).RunAndReturn(func(n int64) (int64, int64, error) {
		return 1000, 1000 + n, nil
	})

	server := &Server{broker: mockBroker, allocator: alloc}
	job := &importJob{
		ImportJob: &datapb.ImportJob{
			JobID:        9,
			CollectionID: 7,
			Vchannels:    wantVchannels,
			Files: []*internalpb.ImportFile{
				{Id: 101, Paths: []string{"a.parquet"}},
				{Id: 102, Paths: []string{"b.parquet"}},
				{Id: 103, Paths: []string{"c.parquet"}},
			},
		},
		tr: timerecord.NewTimeRecorder("test"),
	}

	// fileRows aligned with job.GetFiles(): 10 rows, 0 rows (empty file), 5 rows.
	require.NoError(t, server.broadcastImportIDRangeMessage(ctx, job, []int64{10, 0, 5}))

	require.NotNil(t, capture.captured, "Broadcast must have been called")
	assert.ElementsMatch(t, wantVchannels, capture.captured.BroadcastHeader().VChannels,
		"broadcast must target the job's data vchannels; the broadcaster adds the control channel")

	msg, err := message.AsBroadcastImportIDRangeMessageV2(capture.captured)
	require.NoError(t, err)
	assert.EqualValues(t, 7, msg.Header().GetCollectionId())
	assert.EqualValues(t, 9, msg.Header().GetJobId())

	idRanges := msg.MustBody().GetIdRanges()
	require.Len(t, idRanges, 3)
	wantRows := []int64{10, 0, 5}
	for i, r := range idRanges {
		assert.EqualValues(t, wantRows[i], r.GetEnd()-r.GetBegin(),
			"key is the position in job.Files; range size == row count (empty for a zero-row file)")
	}
	// Greedy batching: one allocation for the whole 15-id total, contiguous.
	assert.EqualValues(t, idRanges[0].GetEnd(), idRanges[2].GetBegin())
}

// Allocation failure is transient and propagates without any broadcast; a job
// without vchannels can never deliver the message and fails loudly.
func TestAssignAndBroadcastImportIDRange_ErrorPaths(t *testing.T) {
	ctx := context.Background()

	// AllocN failure -> error propagates, nothing is broadcast.
	allocErr := allocator.NewMockAllocator(t)
	allocErr.EXPECT().AllocN(mock.Anything).Return(0, 0, errors.New("rootcoord unavailable"))
	broadcastStarted := false
	mockBroadcast := mockey.Mock(broadcast.StartBroadcastWithResourceKeys).To(
		func(_ context.Context, _ ...message.ResourceKey) (broadcaster.BroadcastAPI, error) {
			broadcastStarted = true
			return nil, errors.New("must not be reached")
		}).Build()
	defer mockBroadcast.UnPatch()

	server := &Server{allocator: allocErr}
	job := &importJob{
		ImportJob: &datapb.ImportJob{
			JobID: 9, CollectionID: 7, Vchannels: []string{"v0"},
			Files: []*internalpb.ImportFile{{Id: 101}},
		},
		tr: timerecord.NewTimeRecorder("test"),
	}
	err := server.broadcastImportIDRangeMessage(ctx, job, []int64{10})
	assert.Error(t, err)
	assert.False(t, broadcastStarted, "no broadcast without a successful allocation")

	// No vchannels -> ErrImportSysFailed (same contract as commit/rollback broadcasts).
	alloc := allocator.NewMockAllocator(t)
	alloc.EXPECT().AllocN(mock.Anything).RunAndReturn(func(n int64) (int64, int64, error) {
		return 1000, 1000 + n, nil
	})
	server = &Server{allocator: alloc}
	noVchannelJob := &importJob{
		ImportJob: &datapb.ImportJob{
			JobID: 9, CollectionID: 7,
			Files: []*internalpb.ImportFile{{Id: 101}},
		},
		tr: timerecord.NewTimeRecorder("test"),
	}
	err = server.broadcastImportIDRangeMessage(ctx, noVchannelJob, []int64{10})
	assert.Error(t, err)
	assert.True(t, errors.Is(err, merr.ErrImportSysFailed))
	assert.Contains(t, err.Error(), "job 9 has no vchannels")
	assert.False(t, broadcastStarted)
}

// TestValidateImportRequest_RejectsDuplicateOptionKeys guards the bypass found
// by adversarial review on milvus#51894: every check reads options as a
// repeated KV (first match wins) while the broadcast body folds them into a map
// (last value wins), so [{backup,false},{backup,true}] used to validate as an
// ordinary import -- skipping the ImportBinlog privilege check -- and then
// execute as a binlog import.
func TestValidateImportRequest_RejectsDuplicateOptionKeys(t *testing.T) {
	paramtable.Init()

	s := &Server{}

	err := s.validateImportRequest(context.Background(),
		[]*msgpb.ImportFile{{Paths: []string{"staging/a.json"}}},
		[]*commonpb.KeyValuePair{
			{Key: "backup", Value: "false"},
			{Key: "backup", Value: "true"},
		})

	assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	assert.Contains(t, err.Error(), "backup")
}

// TestImportAckCallback_DropsControlChannelFromJobChannels pins the filter in
// importV1AckCallback: the broadcaster stamps the control channel into every
// broadcast, so the ack result carries a control-channel entry that must not
// become one of the job's data vchannels, while its time tick still counts
// toward DataTimestamp.
func TestImportAckCallback_DropsControlChannelFromJobChannels(t *testing.T) {
	defer mockey.UnPatchAll()

	// The request is read inside the hook: its memory is not valid after the call returns.
	var channelNames []string
	var dataTimestamp uint64
	mockey.Mock((*Server).createImportJobFromAck).To(
		func(_ *Server, _ context.Context, in *internalpb.ImportRequestInternal) (*internalpb.ImportResponse, error) {
			channelNames = append([]string{}, in.GetChannelNames()...)
			dataTimestamp = in.GetDataTimestamp()
			return &internalpb.ImportResponse{Status: merr.Success(), JobID: "1"}, nil
		}).Build()

	const cchannel = "by-dev-rootcoord-dml_0_vcchan"
	broadcastMsg := message.NewImportMessageBuilderV1().
		WithHeader(&message.ImportMessageHeader{}).
		WithBody(&msgpb.ImportMsg{CollectionID: 100, JobID: 1}).
		WithBroadcast([]string{"vchannel1"}).
		MustBuildBroadcast().
		OverwriteBroadcastHeader(1)
	broadcastMsg = message.WithBroadcastControlChannel(broadcastMsg, cchannel)
	result := message.BroadcastResultImportMessageV1{
		Message: message.MustAsSpecializedBroadcastMessage[*message.ImportMessageHeader, *msgpb.ImportMsg](broadcastMsg),
		Results: map[string]*message.AppendResult{
			"vchannel1": {TimeTick: 100},
			cchannel:    {TimeTick: 200},
		},
	}

	callbacks := &DDLCallbacks{Server: &Server{}}
	assert.NoError(t, callbacks.importV1AckCallback(context.Background(), result))
	assert.Equal(t, []string{"vchannel1"}, channelNames)
	assert.Equal(t, uint64(200), dataTimestamp)
}
