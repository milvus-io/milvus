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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/mocks/distributed/mock_streaming"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestStaleBackfillSegments(t *testing.T) {
	segments := NewSegmentsInfo()
	// stale sealed data segment: schema_version below the watermark
	segments.SetSegment(1, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: 1, CollectionID: 100, State: commonpb.SegmentState_Flushed, SchemaVersion: 1,
	}})
	// fresh sealed data segment: already at the watermark
	segments.SetSegment(2, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: 2, CollectionID: 100, State: commonpb.SegmentState_Flushed, SchemaVersion: 2,
	}})
	// outside the sealed data population: growing / L0 / dropped
	segments.SetSegment(3, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: 3, CollectionID: 100, State: commonpb.SegmentState_Growing, SchemaVersion: 0,
	}})
	segments.SetSegment(4, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: 4, CollectionID: 100, State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L0, SchemaVersion: 0,
	}})
	segments.SetSegment(5, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: 5, CollectionID: 100, State: commonpb.SegmentState_Dropped, SchemaVersion: 0,
	}})
	s := &Server{meta: &meta{segments: segments, collections: newTestCollections(100)}}

	assert.ElementsMatch(t, []int64{1}, s.StaleBackfillSegments(context.Background(), 100, 2))
	assert.Empty(t, s.StaleBackfillSegments(context.Background(), 100, 1))
}

func TestSetBackfillAtomicGate(t *testing.T) {
	s := &Server{}
	gate := &fakeExternalGateRegistrar{}
	s.SetBackfillAtomicGate(gate)
	assert.Equal(t, BackfillAtomicGateRegistrar(gate), s.backfillGate)
}

// fakeExternalGateRegistrar captures RegisterExternal calls for assertions.
type fakeExternalGateRegistrar struct {
	calls []struct {
		collectionID int64
		source       string
		fieldIDs     []int64
	}
	err error // when set, RegisterExternal fails without recording
}

func (f *fakeExternalGateRegistrar) RegisterExternal(_ context.Context, collectionID, _ int64, source string, fieldIDs []int64) error {
	if f.err != nil {
		return f.err
	}
	f.calls = append(f.calls, struct {
		collectionID int64
		source       string
		fieldIDs     []int64
	}{collectionID, source, fieldIDs})
	return nil
}

func buildBatchUpdateManifestResult(t *testing.T, items []*messagespb.BatchUpdateManifestItem, fb *messagespb.FieldBackfill) message.BroadcastResultBatchUpdateManifestMessageV2 {
	msg := message.NewBatchUpdateManifestMessageBuilderV2().
		WithHeader(&message.BatchUpdateManifestMessageHeader{CollectionId: 100}).
		WithBody(&message.BatchUpdateManifestMessageBody{Items: items, FieldBackfill: fb}).
		WithBroadcast([]string{"control_channel"}).
		MustBuildBroadcast()
	return message.BroadcastResultBatchUpdateManifestMessageV2{
		Message: message.MustAsBroadcastBatchUpdateManifestMessageV2(msg),
	}
}

func TestBatchUpdateManifestV2AckCallback(t *testing.T) {
	ctx := context.Background()
	newServer := func(gate BackfillAtomicGateRegistrar) *Server {
		segments := NewSegmentsInfo()
		segments.SetSegment(1, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID: 1, CollectionID: 100, State: commonpb.SegmentState_Flushed,
			ManifestPath: packed.MarshalManifestPath("base", 1),
		}})
		segments.SetSegment(2, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID: 2, CollectionID: 100, State: commonpb.SegmentState_Flushed,
		}})
		return &Server{
			meta:         &meta{catalog: &stubCatalog{}, segments: segments, collections: newTestCollections(100)},
			allocator:    &stubAllocator{nextID: 9000},
			backfillGate: gate,
		}
	}

	t.Run("backfill batch applies and registers one gate with the commit source", func(t *testing.T) {
		gate := &fakeExternalGateRegistrar{}
		callbacks := &DDLCallbacks{Server: newServer(gate)}
		result := buildBatchUpdateManifestResult(t, []*messagespb.BatchUpdateManifestItem{
			{SegmentId: 1, ManifestVersion: 2}, // v3: manifest pointer advance
			{SegmentId: 2, V2ColumnGroups: &messagespb.BatchUpdateManifestV2ColumnGroups{
				ColumnGroups: map[int64]*datapb.FieldBinlog{0: {FieldID: 0}},
			}}, // v2: column-group upsert
			{SegmentId: 3, ManifestVersion: 2, V2ColumnGroups: &messagespb.BatchUpdateManifestV2ColumnGroups{
				ColumnGroups: map[int64]*datapb.FieldBinlog{0: {FieldID: 0}},
			}}, // both payloads -> skipped
			{SegmentId: 4}, // no payload -> skipped
		}, &messagespb.FieldBackfill{FieldIds: []int64{10}, Source: "backfillresult:s3://r.json"})

		require.NoError(t, callbacks.batchUpdateManifestV2AckCallback(ctx, result))
		require.Len(t, gate.calls, 1)
		assert.Equal(t, int64(100), gate.calls[0].collectionID)
		assert.Equal(t, "backfillresult:s3://r.json", gate.calls[0].source)
		assert.Equal(t, []int64{10}, gate.calls[0].fieldIDs)
	})

	t.Run("generic manifest update takes no gate action", func(t *testing.T) {
		gate := &fakeExternalGateRegistrar{}
		callbacks := &DDLCallbacks{Server: newServer(gate)}
		result := buildBatchUpdateManifestResult(t, []*messagespb.BatchUpdateManifestItem{
			{SegmentId: 1, ManifestVersion: 2},
		}, nil)

		require.NoError(t, callbacks.batchUpdateManifestV2AckCallback(ctx, result))
		assert.Empty(t, gate.calls)
	})

	t.Run("missing commit source falls back to the broadcast ID", func(t *testing.T) {
		gate := &fakeExternalGateRegistrar{}
		callbacks := &DDLCallbacks{Server: newServer(gate)}
		result := buildBatchUpdateManifestResult(t, []*messagespb.BatchUpdateManifestItem{
			{SegmentId: 1, ManifestVersion: 2},
		}, &messagespb.FieldBackfill{FieldIds: []int64{10}})

		require.NoError(t, callbacks.batchUpdateManifestV2AckCallback(ctx, result))
		require.Len(t, gate.calls, 1)
		assert.Contains(t, gate.calls[0].source, "batchmanifest:")
	})
}

// failingIDAllocator fails every AllocID call.
type failingIDAllocator struct {
	allocator.Allocator
}

func (f *failingIDAllocator) AllocID(_ context.Context) (typeutil.UniqueID, error) {
	return 0, errors.New("alloc down")
}

func TestBatchUpdateManifestV2AckCallback_ErrorPaths(t *testing.T) {
	ctx := context.Background()
	newSegments := func() *SegmentsInfo {
		segments := NewSegmentsInfo()
		segments.SetSegment(1, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID: 1, CollectionID: 100, State: commonpb.SegmentState_Flushed,
			ManifestPath: packed.MarshalManifestPath("base", 1),
		}})
		return segments
	}
	fb := &messagespb.FieldBackfill{FieldIds: []int64{10}, Source: "backfillresult:s3://r.json"}
	items := []*messagespb.BatchUpdateManifestItem{{SegmentId: 1, ManifestVersion: 2}}

	t.Run("apply failure returns the error for the ack to retry", func(t *testing.T) {
		server := &Server{
			meta: &meta{
				catalog:     &stubCatalog{alterSegmentErr: errors.New("catalog down")},
				segments:    newSegments(),
				collections: newTestCollections(100),
			},
			allocator:    &stubAllocator{},
			backfillGate: &fakeExternalGateRegistrar{},
		}
		callbacks := &DDLCallbacks{Server: server}
		assert.Error(t, callbacks.batchUpdateManifestV2AckCallback(ctx, buildBatchUpdateManifestResult(t, items, fb)))
	})

	t.Run("nil gate skips registration without failing the apply", func(t *testing.T) {
		server := &Server{
			meta:      &meta{catalog: &stubCatalog{}, segments: newSegments(), collections: newTestCollections(100)},
			allocator: &stubAllocator{},
		}
		callbacks := &DDLCallbacks{Server: server}
		assert.NoError(t, callbacks.batchUpdateManifestV2AckCallback(ctx, buildBatchUpdateManifestResult(t, items, fb)))
	})

	t.Run("roundID allocation failure returns error for the ack to retry", func(t *testing.T) {
		gate := &fakeExternalGateRegistrar{}
		server := &Server{
			meta:         &meta{catalog: &stubCatalog{}, segments: newSegments(), collections: newTestCollections(100)},
			allocator:    &failingIDAllocator{},
			backfillGate: gate,
		}
		callbacks := &DDLCallbacks{Server: server}
		assert.Error(t, callbacks.batchUpdateManifestV2AckCallback(ctx, buildBatchUpdateManifestResult(t, items, fb)))
		assert.Empty(t, gate.calls)
	})

	t.Run("registration failure returns error for the ack to retry", func(t *testing.T) {
		gate := &fakeExternalGateRegistrar{err: errors.New("etcd down")}
		server := &Server{
			meta:         &meta{catalog: &stubCatalog{}, segments: newSegments(), collections: newTestCollections(100)},
			allocator:    &stubAllocator{},
			backfillGate: gate,
		}
		callbacks := &DDLCallbacks{Server: server}
		assert.Error(t, callbacks.batchUpdateManifestV2AckCallback(ctx, buildBatchUpdateManifestResult(t, items, fb)))
	})

	t.Run("empty field list skips registration", func(t *testing.T) {
		gate := &fakeExternalGateRegistrar{}
		server := &Server{
			meta:         &meta{catalog: &stubCatalog{}, segments: newSegments(), collections: newTestCollections(100)},
			allocator:    &stubAllocator{},
			backfillGate: gate,
		}
		callbacks := &DDLCallbacks{Server: server}
		empty := &messagespb.FieldBackfill{Source: "backfillresult:s3://r.json"}
		assert.NoError(t, callbacks.batchUpdateManifestV2AckCallback(ctx, buildBatchUpdateManifestResult(t, items, empty)))
		assert.Empty(t, gate.calls)
	})
}

func TestCommitBackfillResult_GateFieldDerivationAndSource(t *testing.T) {
	ctx := context.Background()

	wal := mock_streaming.NewMockWALAccesser(t)
	wal.EXPECT().ControlChannel().Return("by-dev-control").Maybe()
	streaming.SetWALForTest(wal)

	mockLoad := mockey.Mock((*Server).loadBackfillResult).Return(&BackfillResult{
		Success:       true,
		CollectionID:  100,
		NewFieldNames: []string{"f_new"},
		Segments:      map[string]BackfillSegment{"1": {}},
	}, nil).Build()
	defer mockLoad.UnPatch()

	mockClassify := mockey.Mock((*Server).classifyBackfillSegments).Return(
		[]*messagespb.BatchUpdateManifestItem{{SegmentId: 1, ManifestVersion: 2}},
		nil,
	).Build()
	defer mockClassify.UnPatch()

	var gotFields []int64
	var gotSource string
	mockBroadcast := mockey.Mock(broadcastBackfillBatch).To(func(
		_ context.Context,
		_ *milvuspb.DescribeCollectionResponse,
		_ int64,
		_ []string,
		_ []*messagespb.BatchUpdateManifestItem,
		backfillFieldIDs []int64,
		backfillSource string,
	) error {
		gotFields = backfillFieldIDs
		gotSource = backfillSource
		return nil
	}).Build()
	defer mockBroadcast.UnPatch()

	b := broker.NewMockBroker(t)
	b.EXPECT().DescribeCollectionInternal(mock.Anything, int64(100)).Return(&milvuspb.DescribeCollectionResponse{
		Schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
			{FieldID: 7, Name: "old"},
			{FieldID: 10, Name: "f_new"},
		}},
	}, nil)

	s := &Server{broker: b, backfillGate: &fakeExternalGateRegistrar{}}
	s.stateCode.Store(commonpb.StateCode_Healthy)

	resp, err := s.CommitBackfillResult(ctx, &datapb.CommitBackfillResultRequest{ResultPath: "s3://bucket/r.json"})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	// The gate fields are derived from NewFieldNames x collection schema, and every
	// batch carries the commit-level source.
	assert.Equal(t, []int64{10}, gotFields)
	assert.Equal(t, "backfillresult:s3://bucket/r.json", gotSource)
}

func TestChannelCheckpointsAtLeast(t *testing.T) {
	ctx := context.Background()
	vch := "by-dev-rootcoord-dml_0_v1" // matches newTestCollections
	newServerWithCP := func(ts uint64) *Server {
		cps := newChannelCps()
		if ts > 0 {
			cps.checkpoints[vch] = &msgpb.MsgPosition{Timestamp: ts}
		}
		return &Server{meta: &meta{collections: newTestCollections(100), channelCPs: cps}}
	}

	// Checkpoint past the DDL tick -> reached.
	assert.True(t, newServerWithCP(50).ChannelCheckpointsAtLeast(ctx, 100, 42))
	// Checkpoint behind the tick -> not reached (pre-V data may still be growing).
	assert.False(t, newServerWithCP(41).ChannelCheckpointsAtLeast(ctx, 100, 42))
	// Missing checkpoint or unknown collection -> fail-closed.
	assert.False(t, newServerWithCP(0).ChannelCheckpointsAtLeast(ctx, 100, 42))
	assert.False(t, newServerWithCP(50).ChannelCheckpointsAtLeast(ctx, 999, 42))
}

func TestResolveBackfillGateFields(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk"},
		{FieldID: 101, Name: "vec"},
		{FieldID: 102, Name: "f_new"},
	}}
	v2 := func(fids ...int64) BackfillSegment {
		sv := int64(2)
		return BackfillSegment{Version: -1, StorageVersion: &sv,
			ColumnGroups: []BackfillV2ColumnGroup{{FieldIDs: fids}}}
	}
	v3 := BackfillSegment{Version: 7}

	t.Run("declaration is the source; the v2 payload must be consistent with it", func(t *testing.T) {
		fields, err := resolveBackfillGateFields(schema, &BackfillResult{
			NewFieldNames: []string{"f_new"},
			Segments:      map[string]BackfillSegment{"1": v2(102), "2": v2(102, 0 /* system, ignored */)},
		})
		require.NoError(t, err)
		assert.Equal(t, []int64{102}, fields)
	})

	t.Run("missing declaration rejects the commit (uniform for v2 and v3)", func(t *testing.T) {
		_, err := resolveBackfillGateFields(schema, &BackfillResult{
			Segments: map[string]BackfillSegment{"1": v2(102)},
		})
		assert.Error(t, err)
		_, err = resolveBackfillGateFields(schema, &BackfillResult{
			Segments: map[string]BackfillSegment{"1": v3},
		})
		assert.Error(t, err)
	})

	t.Run("declared name not in schema rejects the commit", func(t *testing.T) {
		_, err := resolveBackfillGateFields(schema, &BackfillResult{
			NewFieldNames: []string{"typo_field"},
			Segments:      map[string]BackfillSegment{"1": v3},
		})
		assert.Error(t, err)
	})

	t.Run("v2 group field unknown to the schema rejects the commit", func(t *testing.T) {
		_, err := resolveBackfillGateFields(schema, &BackfillResult{
			NewFieldNames: []string{"f_new"},
			Segments:      map[string]BackfillSegment{"1": v2(999)},
		})
		assert.Error(t, err)
	})

	t.Run("v2 group writing an undeclared field rejects the commit", func(t *testing.T) {
		_, err := resolveBackfillGateFields(schema, &BackfillResult{
			NewFieldNames: []string{"f_new"},
			Segments:      map[string]BackfillSegment{"1": v2(101 /* mutated but not declared */)},
		})
		assert.Error(t, err)
	})

	t.Run("declared field with no backing mutation rejects a v2-only commit", func(t *testing.T) {
		_, err := resolveBackfillGateFields(schema, &BackfillResult{
			NewFieldNames: []string{"f_new", "vec" /* nothing writes vec */},
			Segments:      map[string]BackfillSegment{"1": v2(102)},
		})
		assert.Error(t, err)
	})

	t.Run("with v3 entries the extra declared field may live in the opaque manifests", func(t *testing.T) {
		fields, err := resolveBackfillGateFields(schema, &BackfillResult{
			NewFieldNames: []string{"f_new", "vec"},
			Segments:      map[string]BackfillSegment{"1": v2(102), "2": v3},
		})
		require.NoError(t, err)
		assert.Equal(t, []int64{101, 102}, fields)
	})
}
