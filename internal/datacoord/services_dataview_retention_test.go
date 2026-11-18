package datacoord

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/dataview"
	catalogkv "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestGetDataViewRetention(t *testing.T) {
	s := &Server{}
	s.stateCode.Store(commonpb.StateCode_Initializing)
	req := &datapb.GetDataViewRetentionRequest{Pchannel: "p"}
	resp, err := s.GetDataViewRetention(t.Context(), req)
	require.NoError(t, err)
	require.Error(t, merr.Error(resp.Status))
	s.stateCode.Store(commonpb.StateCode_Healthy)
	resp, err = s.GetDataViewRetention(t.Context(), req)
	require.NoError(t, err)
	require.ErrorIs(t, merr.Error(resp.Status), merr.ErrServiceNotReady)
	views := []*viewpb.DataViewOfCollection{{CollectionId: 1, DataVersion: &viewpb.DataVersion{StreamingVersion: 1}, Shards: []*viewpb.DataViewOfShard{{Vchannel: "p_1v0", GrowingTransformStartAfterTimetick: 30, CompactedTransformStartAfterTimetick: 30}}}}
	patch := mockey.Mock((*catalogkv.Catalog).ListAllDataViews).Return(views, nil).Build()
	defer patch.UnPatch()
	s.dataViewManager, err = dataview.RecoverManager(t.Context(), &catalogkv.Catalog{}, func(context.Context, int64) (bool, error) { return true, nil }, nil, nil, nil)
	require.NoError(t, err)
	resp, err = s.GetDataViewRetention(t.Context(), &datapb.GetDataViewRetentionRequest{})
	require.NoError(t, err)
	require.ErrorIs(t, merr.Error(resp.Status), merr.ErrServiceInternal)
	resp, err = s.GetDataViewRetention(t.Context(), req)
	require.NoError(t, err)
	require.NoError(t, merr.Error(resp.Status))
	require.Equal(t, []int64{1}, resp.CollectionIds)
	require.Equal(t, []uint32{0}, resp.VchannelIndices)
	require.Equal(t, []uint64{30}, resp.TransformStartAfterTimeticks)
	resp, err = s.GetDataViewRetention(t.Context(), &datapb.GetDataViewRetentionRequest{Pchannel: "other"})
	require.NoError(t, err)
	require.NoError(t, merr.Error(resp.Status))
	require.Empty(t, resp.TransformStartAfterTimeticks)
}

// Exercise the real provider and wire representation with multiple collections,
// including multiple shards of one collection on the same PChannel.
func TestGetDataViewRetentionPacked(t *testing.T) {
	const pchannel = "by-dev-rootcoord-dml_0"
	views := make([]*viewpb.DataViewOfCollection, 0, 500)
	expected := make(map[string]uint64)
	for i := range 500 {
		id := int64(460000000000000000 + i)
		view := &viewpb.DataViewOfCollection{CollectionId: id, DataVersion: &viewpb.DataVersion{StreamingVersion: 1}}
		for index := range 2 {
			channel := funcutil.GetVirtualChannel(pchannel, id, index)
			tick := uint64(470000000000000000 + i + index)
			expected[channel] = tick
			view.Shards = append(view.Shards, &viewpb.DataViewOfShard{Vchannel: channel, GrowingTransformStartAfterTimetick: tick, CompactedTransformStartAfterTimetick: tick})
		}
		views = append(views, view)
	}
	patch := mockey.Mock((*catalogkv.Catalog).ListAllDataViews).Return(views, nil).Build()
	defer patch.UnPatch()
	manager, err := dataview.RecoverManager(t.Context(), &catalogkv.Catalog{}, func(context.Context, int64) (bool, error) { return true, nil }, nil, nil, nil)
	require.NoError(t, err)
	s := &Server{dataViewManager: manager}
	s.stateCode.Store(commonpb.StateCode_Healthy)
	response, err := s.GetDataViewRetention(t.Context(), &datapb.GetDataViewRetentionRequest{Pchannel: pchannel})
	require.NoError(t, err)
	require.NoError(t, merr.Error(response.Status))
	wire, err := proto.Marshal(response)
	require.NoError(t, err)
	decoded := &datapb.GetDataViewRetentionResponse{}
	require.NoError(t, proto.Unmarshal(wire, decoded))
	require.Len(t, decoded.CollectionIds, len(expected))
	require.Len(t, decoded.VchannelIndices, len(expected))
	require.Len(t, decoded.TransformStartAfterTimeticks, len(expected))
	actual := make(map[string]uint64)
	for i, id := range decoded.CollectionIds {
		actual[funcutil.GetVirtualChannel(pchannel, id, int(decoded.VchannelIndices[i]))] = decoded.TransformStartAfterTimeticks[i]
	}
	require.Equal(t, expected, actual)
	for _, name := range []string{"collection_ids", "vchannel_indices", "transform_start_after_timeticks"} {
		require.True(t, decoded.ProtoReflect().Descriptor().Fields().ByTextName(name).IsPacked())
	}
	// Encode the former map<string,uint64> field with its original field numbers.
	status, err := proto.Marshal(response.Status)
	require.NoError(t, err)
	oldWire := protowire.AppendBytes(protowire.AppendTag(nil, 1, protowire.BytesType), status)
	for channel, tick := range expected {
		entry := protowire.AppendString(protowire.AppendTag(nil, 1, protowire.BytesType), channel)
		entry = protowire.AppendVarint(protowire.AppendTag(entry, 2, protowire.VarintType), tick)
		oldWire = protowire.AppendBytes(protowire.AppendTag(oldWire, 2, protowire.BytesType), entry)
	}
	require.Less(t, len(wire), len(oldWire))
	t.Logf("1000 shards: map=%d bytes, packed=%d bytes, reduction=%.2f%% (protobuf, uncompressed)", len(oldWire), len(wire), 100*(1-float64(len(wire))/float64(len(oldWire))))
}

func TestGetDataViewRetentionInvalidChannel(t *testing.T) {
	for _, channel := range []string{"p_invalidv0", "p_1v4294967296"} {
		t.Run(channel, func(t *testing.T) {
			views := []*viewpb.DataViewOfCollection{{CollectionId: 1, DataVersion: &viewpb.DataVersion{StreamingVersion: 1}, Shards: []*viewpb.DataViewOfShard{{Vchannel: channel}}}}
			patch := mockey.Mock((*catalogkv.Catalog).ListAllDataViews).Return(views, nil).Build()
			defer patch.UnPatch()
			manager, err := dataview.RecoverManager(t.Context(), &catalogkv.Catalog{}, func(context.Context, int64) (bool, error) { return true, nil }, nil, nil, nil)
			require.NoError(t, err)
			s := &Server{dataViewManager: manager}
			s.stateCode.Store(commonpb.StateCode_Healthy)
			resp, err := s.GetDataViewRetention(t.Context(), &datapb.GetDataViewRetentionRequest{Pchannel: "p"})
			require.NoError(t, err)
			require.ErrorIs(t, merr.Error(resp.Status), merr.ErrServiceInternal)
			require.Empty(t, resp.CollectionIds)
			require.Empty(t, resp.VchannelIndices)
			require.Empty(t, resp.TransformStartAfterTimeticks)
		})
	}
}
