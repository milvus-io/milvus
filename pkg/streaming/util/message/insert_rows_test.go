package message_test

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestReadCompactInsertAndUpsertRows(t *testing.T) {
	keys := &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{7, 7, 9}}}}
	rows := &messagespb.InsertRows{
		PrimaryKeys:       keys,
		RowIdsEncoding:    &messagespb.InsertRows_DeltaRowIds{DeltaRowIds: &messagespb.DeltaEncodedRowIDs{Base: 100, Deltas: []int64{-3, 0}}},
		PartitionIndexes:  []uint32{0, 1, 0},
		RequestRowMapping: &messagespb.RequestRowMapping{RowOffsets: []uint32{4, 0, 7}},
	}
	insert, err := message.NewInsertMessageBuilderV2().WithVChannel("v1").WithHeader(&messagespb.InsertMessageHeaderV2{}).WithBody(&messagespb.InsertMessageBody{Rows: rows}).BuildMutable()
	require.NoError(t, err)
	immutable := insert.IntoImmutableMessage(walimplstest.NewTestMessageID(1))
	decoded, err := message.MustAsImmutableInsertMessageV2(immutable).Body()
	require.NoError(t, err)
	require.Equal(t, []int64{100, 97, 97}, decoded.GetRows().GetRowIds().GetData())
	require.Equal(t, []uint32{0, 1, 0}, decoded.GetRows().GetPartitionIndexes())
	require.Equal(t, []uint32{4, 0, 7}, decoded.GetRows().GetRequestRowMapping().GetRowOffsets())
	_, err = message.AsImmutableInsertMessageV1(immutable)
	require.Error(t, err, "V2 must not silently decode as a legacy Insert")

	upsertBody := &messagespb.UpsertMessageBody{Rows: rows, DeleteEffects: []*messagespb.UpsertDeleteEffect{
		{Scope: &messagespb.UpsertDeleteEffect_AllPartitions{AllPartitions: &messagespb.UpsertAllPartitions{}}, KeySource: &messagespb.UpsertDeleteEffect_AllInsertedKeys{AllInsertedKeys: &messagespb.UpsertAllInsertedKeys{}}},
		{Scope: &messagespb.UpsertDeleteEffect_PartitionId{PartitionId: 20}, KeySource: &messagespb.UpsertDeleteEffect_PrimaryKeys{PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{99}}}}}},
	}}
	original := proto.Clone(upsertBody)
	upsert, err := message.NewUpsertMessageBuilderV2().WithVChannel("v1").WithHeader(&messagespb.UpsertMessageHeader{}).WithBody(upsertBody).BuildMutable()
	require.NoError(t, err)
	read, err := message.MustAsImmutableUpsertMessageV2(upsert.IntoImmutableMessage(walimplstest.NewTestMessageID(2))).Body()
	require.NoError(t, err)
	require.Equal(t, []int64{100, 97, 97}, read.Rows.GetRowIds().GetData())
	require.Equal(t, []int64{7, 7, 9}, read.DeleteEffects[0].GetPrimaryKeys().GetIntId().GetData())
	require.NotNil(t, read.DeleteEffects[0].GetAllPartitions())
	require.Equal(t, []int64{99}, read.DeleteEffects[1].GetPrimaryKeys().GetIntId().GetData())
	require.Equal(t, int64(20), read.DeleteEffects[1].GetPartitionId())
	require.True(t, proto.Equal(original, upsertBody), "decoding must not mutate the source body")
}

func TestReadCompactRowsRejectsOverflow(t *testing.T) {
	for _, delta := range []*messagespb.DeltaEncodedRowIDs{
		{Base: math.MaxInt64, Deltas: []int64{1}},
		{Base: math.MinInt64, Deltas: []int64{-1}},
	} {
		msg, err := message.NewInsertMessageBuilderV2().WithVChannel("v1").WithHeader(&messagespb.InsertMessageHeaderV2{}).WithBody(&messagespb.InsertMessageBody{Rows: &messagespb.InsertRows{RowIdsEncoding: &messagespb.InsertRows_DeltaRowIds{DeltaRowIds: delta}}}).BuildMutable()
		require.NoError(t, err)
		decoded, err := message.MustAsImmutableInsertMessageV2(msg.IntoImmutableMessage(walimplstest.NewTestMessageID(1))).Body()
		require.Error(t, err)
		require.Nil(t, decoded)
	}
}
