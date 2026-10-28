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

	"github.com/blang/semver/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/metacache"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_broadcaster"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/pkindexpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	pkTestCollectionID = UniqueID(1)
	pkTestFieldID      = UniqueID(100)
	pkTestVChannel0    = "by-dev-rootcoord-dml_0_1v0"
	pkTestVChannel1    = "by-dev-rootcoord-dml_1_1v1"
)

// pkTestBroadcasts records the messages that the test broadcaster sends, and
// gives every data vchannel the ack result of outcomeOf.
type pkTestBroadcasts struct {
	mu        sync.Mutex
	msgs      []message.BroadcastMutableMessage
	outcomeOf func(vchannel string) *anypb.Any
}

func (b *pkTestBroadcasts) list() []message.BroadcastMutableMessage {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]message.BroadcastMutableMessage(nil), b.msgs...)
}

func pkAck(t *testing.T, outcome pkindexpb.CreateIndexAckResult_Outcome) *anypb.Any {
	extra, err := anypb.New(&pkindexpb.CreateIndexAckResult{Outcome: outcome})
	require.NoError(t, err)
	return extra
}

// initPKIndexStreamingSystem sets up a broadcaster that runs the ack
// callbacks at once, like initStreamingSystem, and records the messages.
func initPKIndexStreamingSystem(t *testing.T) *pkTestBroadcasts {
	initStreamingSystem(t)

	rec := &pkTestBroadcasts{outcomeOf: func(string) *anypb.Any { return nil }}
	bapi := mock_broadcaster.NewMockBroadcastAPI(t)
	bapi.EXPECT().Broadcast(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, msg message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error) {
		rec.mu.Lock()
		rec.msgs = append(rec.msgs, msg)
		rec.mu.Unlock()
		results := make(map[string]*message.AppendResult)
		for _, vchannel := range msg.BroadcastHeader().VChannels {
			result := &message.AppendResult{
				MessageID:              rmq.NewRmqID(1),
				TimeTick:               tsoutil.ComposeTSByTime(time.Now()),
				LastConfirmedMessageID: rmq.NewRmqID(1),
			}
			if !funcutil.IsControlChannel(vchannel) {
				result.Extra = rec.outcomeOf(vchannel)
			}
			results[vchannel] = result
		}
		if err := registry.CallMessageAckCallback(context.Background(), msg, results); err != nil {
			return nil, err
		}
		return &types.BroadcastAppendResult{}, nil
	}).Maybe()
	bapi.EXPECT().Close().Return().Maybe()

	mb := mock_broadcaster.NewMockBroadcaster(t)
	mb.EXPECT().WithResourceKeys(mock.Anything, mock.Anything).Return(bapi, nil).Maybe()
	mb.EXPECT().WithResourceKeys(mock.Anything, mock.Anything, mock.Anything).Return(bapi, nil).Maybe()
	mb.EXPECT().Close().Return().Maybe()
	broadcast.Release()
	broadcast.ResetBroadcaster()
	broadcast.Register(mb)
	return rec
}

// pkTestSession returns a session that lists one streaming node of version.
func pkTestSession(t *testing.T, version string) *sessionutil.MockSession {
	sessions := map[string]*sessionutil.Session{
		"1": {SessionRaw: sessionutil.SessionRaw{ServerID: 1}, Version: semver.MustParse(version)},
	}
	session := sessionutil.NewMockSession(t)
	session.EXPECT().GetSessions(mock.Anything, typeutil.StreamingNodeRole).Return(sessions, 0, nil).Maybe()
	session.EXPECT().GetSessionsWithVersionRange(typeutil.StreamingNodeRole, mock.Anything).RunAndReturn(
		func(_ string, r semver.Range) (map[string]*sessionutil.Session, int64, error) {
			in := make(map[string]*sessionutil.Session)
			for k, s := range sessions {
				if r(s.Version) {
					in[k] = s
				}
			}
			return in, 0, nil
		}).Maybe()
	return session
}

func pkTestSchema(external bool) *schemapb.CollectionSchema {
	schema := &schemapb.CollectionSchema{
		Name: "pk_index",
		Fields: []*schemapb.FieldSchema{
			{FieldID: pkTestFieldID, Name: "pk", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "8"}}},
		},
	}
	if external {
		schema.Fields[1].ExternalField = "vec"
	}
	return schema
}

// newPKIndexTestServer returns a datacoord server with an empty collection of
// two vchannels and a streaming node of the feature version.
func newPKIndexTestServer(t *testing.T, schema *schemapb.CollectionSchema) (*Server, *pkTestBroadcasts) {
	params := paramtable.Get()
	params.Save(params.StreamingCfg.PKIndexEnabled.Key, "true")
	t.Cleanup(func() { params.Reset(params.StreamingCfg.PKIndexEnabled.Key) })
	rec := initPKIndexStreamingSystem(t)

	catalog := catalogmocks.NewDataCoordCatalog(t)
	catalog.EXPECT().CreateIndex(mock.Anything, mock.Anything).Return(nil).Maybe()
	catalog.EXPECT().AlterIndexes(mock.Anything, mock.Anything).Return(nil).Maybe()
	metaStore := metacache.NewMetaStore(catalog)
	metaStore.PutCollection(&collectionInfo{ID: pkTestCollectionID, Schema: schema})

	s := &Server{
		meta: &meta{
			metaStore: metaStore,
			segments:  NewSegmentsInfo(metaStore),
			indexMeta: newSegmentIndexMeta(catalog),
		},
		allocator:           newMockAllocator(t),
		notifyIndexChan:     make(chan UniqueID, 16),
		pkIndexCompensateCh: make(chan UniqueID, 16),
		session:             pkTestSession(t, "3.1.0"),
	}
	RegisterDDLCallbacks(s)
	s.stateCode.Store(commonpb.StateCode_Healthy)

	mixCoord := mocks.NewMixCoord(t)
	mixCoord.EXPECT().DescribeCollectionInternal(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
		Status:              merr.Success(),
		CollectionID:        pkTestCollectionID,
		Schema:              schema,
		VirtualChannelNames: []string{pkTestVChannel0, pkTestVChannel1},
	}, nil).Maybe()
	s.broker = broker.NewCoordinatorBroker(mixCoord)
	return s, rec
}

// runningCopyJobs is a copy segment meta that reports n jobs for any filter.
type runningCopyJobs struct {
	CopySegmentMeta
	n int
}

func (m runningCopyJobs) CountJobBy(context.Context, ...CopySegmentJobFilter) int { return m.n }

func pkCreateRequest(indexType string) *indexpb.CreateIndexRequest {
	return &indexpb.CreateIndexRequest{
		CollectionID: pkTestCollectionID,
		FieldID:      pkTestFieldID,
		IndexName:    "pk_idx",
		IndexParams:  []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: indexType}},
		Timestamp:    100,
	}
}

func TestPKIndexCreateRejections(t *testing.T) {
	ctx := context.Background()

	t.Run("switch off", func(t *testing.T) {
		s, rec := newPKIndexTestServer(t, pkTestSchema(false))
		paramtable.Get().Save(paramtable.Get().StreamingCfg.PKIndexEnabled.Key, "false")
		status, err := s.CreateIndex(ctx, pkCreateRequest(common.PrimaryKeyIndexType))
		require.NoError(t, err)
		assert.ErrorIs(t, merr.Error(status), merr.ErrParameterInvalid)
		assert.Empty(t, rec.list())
	})

	t.Run("old streaming node", func(t *testing.T) {
		s, rec := newPKIndexTestServer(t, pkTestSchema(false))
		s.session = pkTestSession(t, "3.0.4")
		status, err := s.CreateIndex(ctx, pkCreateRequest(common.PrimaryKeyIndexType))
		require.NoError(t, err)
		assert.ErrorIs(t, merr.Error(status), merr.ErrServiceNotReady)
		assert.Empty(t, rec.list())
	})

	t.Run("not the primary key", func(t *testing.T) {
		s, rec := newPKIndexTestServer(t, pkTestSchema(false))
		req := pkCreateRequest(common.PrimaryKeyIndexType)
		req.FieldID = 101
		status, err := s.CreateIndex(ctx, req)
		require.NoError(t, err)
		assert.ErrorIs(t, merr.Error(status), merr.ErrParameterInvalid)
		assert.Empty(t, rec.list())
	})

	t.Run("external collection", func(t *testing.T) {
		s, rec := newPKIndexTestServer(t, pkTestSchema(true))
		status, err := s.CreateIndex(ctx, pkCreateRequest(common.PrimaryKeyIndexType))
		require.NoError(t, err)
		assert.ErrorIs(t, merr.Error(status), merr.ErrParameterInvalid)
		assert.Empty(t, rec.list())
	})

	t.Run("collection is not empty", func(t *testing.T) {
		s, rec := newPKIndexTestServer(t, pkTestSchema(false))
		s.meta.segments.SetSegment(9, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID: 9, CollectionID: pkTestCollectionID, State: commonpb.SegmentState_Flushed,
		}})
		status, err := s.CreateIndex(ctx, pkCreateRequest(common.PrimaryKeyIndexType))
		require.NoError(t, err)
		assert.ErrorIs(t, merr.Error(status), merr.ErrParameterInvalid)
		assert.Contains(t, status.GetReason(), "not empty")
		assert.Empty(t, rec.list())
	})

	t.Run("snapshot restore is running", func(t *testing.T) {
		s, rec := newPKIndexTestServer(t, pkTestSchema(false))
		s.copySegmentMeta = runningCopyJobs{n: 1}
		status, err := s.CreateIndex(ctx, pkCreateRequest(common.PrimaryKeyIndexType))
		require.NoError(t, err)
		assert.ErrorIs(t, merr.Error(status), merr.ErrParameterInvalid)
		assert.Empty(t, rec.list())
	})
}

func TestPKIndexCreateOfAnEmptyCollection(t *testing.T) {
	ctx := context.Background()
	s, rec := newPKIndexTestServer(t, pkTestSchema(false))
	// a dropped segment does not count, so a truncated collection is empty
	s.meta.segments.SetSegment(9, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: 9, CollectionID: pkTestCollectionID, State: commonpb.SegmentState_Dropped,
	}})
	rec.outcomeOf = func(string) *anypb.Any { return pkAck(t, pkindexpb.CreateIndexAckResult_ENABLED) }

	status, err := s.CreateIndex(ctx, pkCreateRequest(common.PrimaryKeyIndexType))
	require.NoError(t, err)
	require.NoError(t, merr.Error(status))

	msgs := rec.list()
	require.Len(t, msgs, 1)
	bh := msgs[0].BroadcastHeader()
	// the broadcaster adds the control channel later
	assert.ElementsMatch(t, []string{pkTestVChannel0, pkTestVChannel1}, bh.VChannels)
	assert.True(t, bh.AckSyncUp)
	header := message.MustAsBroadcastCreateIndexMessageV2(msgs[0]).Header()
	assert.Equal(t, common.PrimaryKeyIndexType, header.GetIndexType())

	indexes := s.meta.indexMeta.GetIndexesForCollection(pkTestCollectionID, "")
	require.Len(t, indexes, 1)
	assert.Equal(t, commonpb.IndexState_Finished, indexes[0].State)
	select {
	case <-s.pkIndexCompensateCh:
		t.Fatal("a finished index needs no compensation")
	default:
	}

	// the same request again is ignored
	status, err = s.CreateIndex(ctx, pkCreateRequest(common.PrimaryKeyIndexType))
	require.NoError(t, err)
	require.NoError(t, merr.Error(status))
	assert.Len(t, rec.list(), 1)
}

func TestPKIndexCreateOfANotEmptyVChannelFails(t *testing.T) {
	ctx := context.Background()
	s, rec := newPKIndexTestServer(t, pkTestSchema(false))
	rec.outcomeOf = func(vchannel string) *anypb.Any {
		if vchannel == pkTestVChannel1 {
			return pkAck(t, pkindexpb.CreateIndexAckResult_NOT_EMPTY)
		}
		return pkAck(t, pkindexpb.CreateIndexAckResult_ENABLED)
	}

	status, err := s.CreateIndex(ctx, pkCreateRequest(common.PrimaryKeyIndexType))
	require.NoError(t, err)
	assert.ErrorIs(t, merr.Error(status), merr.ErrParameterInvalid)
	assert.Contains(t, status.GetReason(), pkTestVChannel1)
	assert.NotContains(t, status.GetReason(), pkTestVChannel0)

	indexes := s.meta.indexMeta.GetIndexesForCollection(pkTestCollectionID, "")
	require.Len(t, indexes, 1)
	assert.Equal(t, commonpb.IndexState_Failed, indexes[0].State)
	assert.Equal(t, pkTestCollectionID, <-s.pkIndexCompensateCh)

	// a retry while the failed index is not dropped yet
	status, err = s.CreateIndex(ctx, pkCreateRequest(common.PrimaryKeyIndexType))
	require.NoError(t, err)
	assert.ErrorIs(t, merr.Error(status), merr.ErrServiceNotReady)
	assert.Len(t, rec.list(), 1)
}

func TestNonPKIndexCreateUsesTheControlChannelOnly(t *testing.T) {
	ctx := context.Background()
	s, rec := newPKIndexTestServer(t, pkTestSchema(false))
	s.session = nil // no version check for other index types

	req := pkCreateRequest("STL_SORT")
	status, err := s.CreateIndex(ctx, req)
	require.NoError(t, err)
	require.NoError(t, merr.Error(status))
	msgs := rec.list()
	require.Len(t, msgs, 1)
	assert.Empty(t, msgs[0].BroadcastHeader().VChannels, "only the control channel, which the broadcaster adds")
	assert.Equal(t, "STL_SORT", message.MustAsBroadcastCreateIndexMessageV2(msgs[0]).Header().GetIndexType())
	indexes := s.meta.indexMeta.GetIndexesForCollection(pkTestCollectionID, "")
	require.Len(t, indexes, 1)
	assert.Equal(t, commonpb.IndexState_IndexStateNone, indexes[0].State)
}

func TestPKIndexStateOf(t *testing.T) {
	cc := funcutil.GetControlChannel("by-dev-rootcoord-dml_0")
	enabled := pkAck(t, pkindexpb.CreateIndexAckResult_ENABLED)
	results := func(extras map[string]*anypb.Any) map[string]*message.AppendResult {
		out := map[string]*message.AppendResult{cc: {}}
		for vchannel, extra := range extras {
			out[vchannel] = &message.AppendResult{Extra: extra}
		}
		return out
	}

	state, reason := pkIndexStateOf(results(map[string]*anypb.Any{pkTestVChannel0: enabled, pkTestVChannel1: enabled}))
	assert.Equal(t, commonpb.IndexState_Finished, state)
	assert.Empty(t, reason)

	for name, extra := range map[string]*anypb.Any{
		"not empty": pkAck(t, pkindexpb.CreateIndexAckResult_NOT_EMPTY),
		"disabled":  pkAck(t, pkindexpb.CreateIndexAckResult_DISABLED),
		"no result": nil,
		"wrong":     func() *anypb.Any { a, _ := anypb.New(&indexpb.IndexInfo{}); return a }(),
	} {
		t.Run(name, func(t *testing.T) {
			state, reason := pkIndexStateOf(results(map[string]*anypb.Any{pkTestVChannel0: enabled, pkTestVChannel1: extra}))
			assert.Equal(t, commonpb.IndexState_Failed, state)
			assert.Contains(t, reason, pkTestVChannel1)
			assert.NotContains(t, reason, pkTestVChannel0)
		})
	}
}

func TestCanCreateIndexWhileAFailedPKIndexIsDropped(t *testing.T) {
	m := newPKIndexTestMeta()
	// collection 2 has a failed primary key index on field 100
	req := &indexpb.CreateIndexRequest{CollectionID: 2, FieldID: 100, IndexName: "pk",
		IndexParams: []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: common.PrimaryKeyIndexType}}}
	_, err := m.CanCreateIndex(req, false)
	assert.ErrorIs(t, err, merr.ErrServiceNotReady)

	// another field is not blocked
	other := &indexpb.CreateIndexRequest{CollectionID: 2, FieldID: 101, IndexName: "sort",
		IndexParams: []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "STL_SORT"}}}
	_, err = m.CanCreateIndex(other, false)
	assert.NoError(t, err)

	// once the drop is done, the create goes on
	m.indexes[2][2002].IsDeleted = true
	_, err = m.CanCreateIndex(req, false)
	assert.NoError(t, err)

	// a finished primary key index: the same request is ignored
	sameAsFinished := &indexpb.CreateIndexRequest{CollectionID: 1, FieldID: 100, IndexName: "pk",
		IndexParams: []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: common.PrimaryKeyIndexType}}}
	_, err = m.CanCreateIndex(sameAsFinished, false)
	assert.ErrorIs(t, err, errIndexOperationIgnored)
}
