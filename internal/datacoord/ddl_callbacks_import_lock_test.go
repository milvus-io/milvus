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
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func lockedImportCollection(version int32, autoID bool) *milvuspb.DescribeCollectionResponse {
	return &milvuspb.DescribeCollectionResponse{
		Status: merr.Success(), CollectionID: 100, DbName: "db", CollectionName: "coll",
		VirtualChannelNames: []string{"v1"},
		Schema: &schemapb.CollectionSchema{Name: "coll", Version: version, Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true, AutoID: autoID},
		}},
	}
}

type lockedImportBroadcast struct {
	msg     message.BroadcastMutableMessage
	onClose func()
}

func (b *lockedImportBroadcast) Broadcast(_ context.Context, msg message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error) {
	b.msg = msg
	return &types.BroadcastAppendResult{BroadcastID: 1}, nil
}

func (b *lockedImportBroadcast) Close() {
	if b.onClose != nil {
		b.onClose()
	}
}

func newLockedImportServer(t *testing.T) (*Server, *broker.MockBroker) {
	t.Helper()
	// Replication topology is covered separately; exercise real request validation
	// and ImportV2 status projection here, with an existing job on the collection.
	p := mockey.Mock((*Server).validateImportReplication).Return(nil).Build()
	t.Cleanup(func() { p.UnPatch() })
	b := broker.NewMockBroker(t)
	s := &Server{broker: b, meta: &meta{}, importMeta: &importMeta{jobs: map[int64]ImportJob{
		9: &importJob{ImportJob: &datapb.ImportJob{JobID: 9, CollectionID: 100, State: internalpb.ImportJobState_Importing}},
	}}}
	s.stateCode.Store(commonpb.StateCode_Healthy)
	return s, b
}

func lockedImportRequest() *internalpb.ImportRequestInternal {
	return &internalpb.ImportRequestInternal{
		CollectionID: 100, CollectionName: "coll", PartitionIDs: []int64{1}, JobID: 10,
		Schema: lockedImportCollection(1, false).Schema, ChannelNames: []string{"v1"},
		Files: []*internalpb.ImportFile{{Id: 1, Paths: []string{"file.json"}}},
	}
}

func TestImportV2RechecksSchemaAfterBroadcastLock(t *testing.T) {
	for _, changed := range []bool{false, true} {
		t.Run(map[bool]string{false: "parallel import allowed", true: "DDL changed schema"}[changed], func(t *testing.T) {
			s, b := newLockedImportServer(t)
			before, current := lockedImportCollection(1, false), lockedImportCollection(1, false)
			if changed {
				current.Schema.Version = 2
				current.Schema.Fields = append(current.Schema.Fields, &schemapb.FieldSchema{FieldID: 101, Name: "new", DataType: schemapb.DataType_Int64, Nullable: true})
			}
			var lock sync.Mutex
			lock.Lock()
			var once sync.Once
			releaseDDL := func() { once.Do(lock.Unlock) }
			t.Cleanup(releaseDDL)
			atLock := make(chan struct{})
			api := &lockedImportBroadcast{onClose: lock.Unlock}
			p := mockey.Mock(broadcast.StartBroadcastWithResourceKeys).To(func(_ context.Context, keys ...message.ResourceKey) (broadcaster.BroadcastAPI, error) {
				require.ElementsMatch(t, []message.ResourceKey{message.NewSharedDBNameResourceKey("db"), message.NewExclusiveCollectionNameResourceKey("db", "coll")}, keys)
				close(atLock)
				lock.Lock()
				return api, nil
			}).Build()
			t.Cleanup(func() { p.UnPatch() })
			b.EXPECT().DescribeCollectionInternal(mock.Anything, int64(100)).Return(before, nil).Once()
			b.EXPECT().DescribeCollectionInternal(mock.Anything, int64(100)).Return(current, nil).Once()
			result := make(chan error, 1)
			go func() {
				resp, err := s.ImportV2(context.Background(), lockedImportRequest())
				result <- merr.CheckRPCCall(resp, err)
			}()
			select {
			case <-atLock:
			case err := <-result:
				t.Fatalf("import failed before lock acquisition: %v", err)
			case <-time.After(5 * time.Second):
				t.Fatal("import did not reach lock acquisition")
			}
			select {
			case err := <-result:
				t.Fatalf("import returned while the previous broadcast held its lock: %v", err)
			case <-time.After(20 * time.Millisecond):
			}
			releaseDDL()
			select {
			case err := <-result:
				if changed {
					require.ErrorIs(t, err, merr.ErrCollectionDDLImportConflict)
					require.False(t, merr.Status(err).GetRetriable())
					require.Nil(t, api.msg)
				} else {
					require.NoError(t, err)
					require.NotNil(t, api.msg)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("import did not resume after lock release")
			}
			require.True(t, lock.TryLock(), "import must release its lock")
			lock.Unlock()
		})
	}
}

func TestImportV2SchemaSnapshotValidation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*internalpb.ImportRequestInternal)
		want   error
	}{
		{name: "matching snapshot"},
		{name: "alias request", mutate: func(r *internalpb.ImportRequestInternal) { r.CollectionName = "alias" }},
		{name: "legacy missing version", mutate: func(r *internalpb.ImportRequestInternal) { r.Schema.Version = 0 }},
		{name: "stale version", mutate: func(r *internalpb.ImportRequestInternal) { r.Schema.Version = 1 }, want: merr.ErrCollectionDDLImportConflict},
		{name: "foreign schema at same version", mutate: func(r *internalpb.ImportRequestInternal) { r.Schema.Name = "other" }, want: merr.ErrCollectionDDLImportConflict},
		{name: "different fields at same version", mutate: func(r *internalpb.ImportRequestInternal) { r.Schema.Fields[0].DataType = schemapb.DataType_VarChar }, want: merr.ErrCollectionDDLImportConflict},
		{name: "legacy stale content", mutate: func(r *internalpb.ImportRequestInternal) { r.Schema.Version = 0; r.Schema.Fields[0].Name = "old" }, want: merr.ErrCollectionDDLImportConflict},
		{name: "foreign channels", mutate: func(r *internalpb.ImportRequestInternal) { r.ChannelNames = []string{"foreign"} }, want: merr.ErrCollectionDDLImportConflict},
		{name: "missing schema", mutate: func(r *internalpb.ImportRequestInternal) { r.Schema = nil }, want: merr.ErrImportSysFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, b := newLockedImportServer(t)
			coll := lockedImportCollection(2, false)
			// RootCoord returns system fields; Proxy intentionally removes them.
			coll.Schema.Fields = append([]*schemapb.FieldSchema{{FieldID: 0, Name: "RowID", DataType: schemapb.DataType_Int64}}, coll.Schema.Fields...)
			original := proto.Clone(coll)
			b.EXPECT().DescribeCollectionInternal(mock.Anything, int64(100)).Return(coll, nil).Twice()
			closed := false
			api := &lockedImportBroadcast{onClose: func() { closed = true }}
			p := mockey.Mock(broadcast.StartBroadcastWithResourceKeys).Return(api, nil).Build()
			t.Cleanup(func() { p.UnPatch() })
			req := lockedImportRequest()
			req.Schema.Version = 2
			if tc.mutate != nil {
				tc.mutate(req)
			}
			requestBefore := proto.Clone(req)
			resp, err := s.ImportV2(context.Background(), req)
			require.NoError(t, err)
			require.True(t, closed)
			require.True(t, proto.Equal(requestBefore, req), "request snapshot must not be mutated")
			require.True(t, proto.Equal(original, coll), "broker metadata must not be mutated")
			if tc.want != nil {
				require.ErrorIs(t, merr.Error(resp.GetStatus()), tc.want)
				require.False(t, resp.GetStatus().GetRetriable())
				require.Nil(t, api.msg)
				return
			}
			require.NoError(t, merr.CheckRPCCall(resp, err))
			body := message.MustAsMutableImportMessageV1(api.msg).MustBody()
			require.True(t, proto.Equal(lockedImportCollection(2, false).Schema, body.GetSchema()))
			require.Equal(t, req.PartitionIDs, body.GetPartitionIDs())
			require.Equal(t, req.ChannelNames, api.msg.BroadcastHeader().VChannels)
		})
	}
}

func TestImportV2AutoIDPreparationBeforeLock(t *testing.T) {
	for _, version := range []int32{1, 2} {
		t.Run(map[int32]string{1: "unchanged", 2: "schema changed"}[version], func(t *testing.T) {
			s, b := newLockedImportServer(t)
			before, current := lockedImportCollection(1, true), lockedImportCollection(version, true)
			held, allocated := false, false
			api := &lockedImportBroadcast{onClose: func() { require.True(t, held); held = false }}
			p := mockey.Mock(broadcast.StartBroadcastWithResourceKeys).To(func(context.Context, ...message.ResourceKey) (broadcaster.BroadcastAPI, error) {
				require.True(t, allocated)
				held = true
				return api, nil
			}).Build()
			t.Cleanup(func() { p.UnPatch() })
			b.EXPECT().DescribeCollectionInternal(mock.Anything, int64(100)).Return(before, nil).Once()
			b.EXPECT().DescribeCollectionInternal(mock.Anything, int64(100)).RunAndReturn(func(context.Context, int64) (*milvuspb.DescribeCollectionResponse, error) {
				require.True(t, held)
				return current, nil
			}).Once()
			sizingCalls := 0
			q := mockey.Mock(computeFileRowUpperBounds).To(func(_ context.Context, _ storage.ChunkManager, schema *schemapb.CollectionSchema, files []*internalpb.ImportFile) ([]fileSizing, error) {
				require.False(t, held)
				require.True(t, proto.Equal(before.Schema, schema))
				sizingCalls++
				return []fileSizing{{file: files[0], rows: 10}}, nil
			}).Build()
			t.Cleanup(func() { q.UnPatch() })
			a := allocator.NewMockAllocator(t)
			s.allocator = a
			a.EXPECT().AllocN(int64(10)).RunAndReturn(func(n int64) (int64, int64, error) {
				require.False(t, held)
				allocated = true
				return 1000, 1000 + n, nil
			}).Once()
			req := lockedImportRequest()
			req.Schema = before.Schema
			resp, err := s.ImportV2(context.Background(), req)
			require.NoError(t, err)
			require.Equal(t, 1, sizingCalls)
			require.False(t, held)
			if version != 1 {
				require.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrCollectionDDLImportConflict)
				require.False(t, resp.GetStatus().GetRetriable())
				require.Nil(t, api.msg)
				return
			}
			require.NoError(t, merr.CheckRPCCall(resp, err))
			body := message.MustAsMutableImportMessageV1(api.msg).MustBody()
			r := body.GetFiles()[0].GetPreAllocatedAutoIds()
			require.EqualValues(t, 10, r.GetEnd()-r.GetBegin())
		})
	}
}

func TestImportV2MetadataFailureReleasesBroadcastLock(t *testing.T) {
	for _, tc := range []struct {
		name      string
		current   *milvuspb.DescribeCollectionResponse
		err, want error
	}{
		{name: "describe failed", err: merr.ErrServiceUnavailable, want: merr.ErrServiceUnavailable},
		{name: "renamed collection", current: &milvuspb.DescribeCollectionResponse{Status: merr.Success(), DbName: "db", CollectionName: "renamed"}, want: merr.ErrCollectionDDLImportConflict},
		{name: "moved to another database", current: &milvuspb.DescribeCollectionResponse{Status: merr.Success(), DbName: "other_db", CollectionName: "coll"}, want: merr.ErrCollectionDDLImportConflict},
		{name: "schema unavailable", current: &milvuspb.DescribeCollectionResponse{Status: merr.Success(), DbName: "db", CollectionName: "coll"}, want: merr.ErrImportSysFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, b := newLockedImportServer(t)
			b.EXPECT().DescribeCollectionInternal(mock.Anything, int64(100)).Return(lockedImportCollection(1, false), nil).Once()
			b.EXPECT().DescribeCollectionInternal(mock.Anything, int64(100)).Return(tc.current, tc.err).Once()
			closed := false
			api := &lockedImportBroadcast{onClose: func() { closed = true }}
			p := mockey.Mock(broadcast.StartBroadcastWithResourceKeys).Return(api, nil).Build()
			t.Cleanup(func() { p.UnPatch() })
			resp, err := s.ImportV2(context.Background(), lockedImportRequest())
			require.NoError(t, err)
			require.ErrorIs(t, merr.Error(resp.GetStatus()), tc.want)
			require.Equal(t, merr.IsRetryableErr(tc.want), resp.GetStatus().GetRetriable())
			require.True(t, closed)
			require.Nil(t, api.msg)
		})
	}
}

func TestImportV2SizingFailurePreservesStatus(t *testing.T) {
	s, _ := newLockedImportServer(t)
	p := mockey.Mock(computeFileRowUpperBounds).Return(nil, merr.ErrIoTooManyRequests).Build()
	t.Cleanup(func() { p.UnPatch() })
	s.allocator = allocator.NewMockAllocator(t)
	req := lockedImportRequest()
	req.Schema = lockedImportCollection(1, true).Schema
	resp, err := s.ImportV2(context.Background(), req)
	require.NoError(t, err)
	require.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrIoTooManyRequests)
	require.True(t, resp.GetStatus().GetRetriable())
}
