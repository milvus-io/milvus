package dml

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"fmt"
	"math"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

var emptyDeletePlanSink [][]int

func TestUpsertEmptyDeletesSkipPKMap(t *testing.T) {
	keys := make([]int64, 1024)
	for i := range keys {
		keys[i] = int64(i)
	}
	inserted := &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: keys}}}
	allocations := testing.AllocsPerRun(5, func() {
		plan, err := matchUpsertDeletes(inserted, nil)
		require.NoError(t, err)
		emptyDeletePlanSink = plan
	})
	require.Nil(t, emptyDeletePlanSink)
	require.Zero(t, allocations, "empty effects do not need a matching plan")
}

func matchUpsertDeletesReference(inserted, deleted *schemapb.IDs) ([][]int, error) {
	rows := make([][]int, typeutil.GetSizeOfIDs(inserted))
	byInt := make(map[int64]int)
	byString := make(map[string]int)
	for row, key := range inserted.GetIntId().GetData() {
		if _, ok := byInt[key]; !ok {
			byInt[key] = row
		}
	}
	for row, key := range inserted.GetStrId().GetData() {
		if _, ok := byString[key]; !ok {
			byString[key] = row
		}
	}
	for offset, key := range deleted.GetIntId().GetData() {
		row, ok := byInt[key]
		if !ok {
			return nil, merr.WrapErrServiceInternalMsg("Upsert deletion has no corresponding preserved row")
		}
		rows[row] = append(rows[row], offset)
	}
	for offset, key := range deleted.GetStrId().GetData() {
		row, ok := byString[key]
		if !ok {
			return nil, merr.WrapErrServiceInternalMsg("Upsert deletion has no corresponding preserved row")
		}
		rows[row] = append(rows[row], offset)
	}
	return rows, nil
}

func TestUpsertRowNonColumnSizeEquivalent(t *testing.T) {
	integerKeys := []int64{math.MinInt64, -129, -128, -1, 0, 1, math.MaxInt64}
	for shift := uint(7); shift < 63; shift += 7 {
		boundary := int64(1) << shift
		integerKeys = append(integerKeys, boundary-1, boundary, boundary+1)
	}
	stringKeys := []string{"", "a", "中文", "😀"}
	for _, length := range []int{124, 125, 126, 127, 128, 129, 16379, 16380, 16381, 16382, 16383, 16384, 16385} {
		stringKeys = append(stringKeys, strings.Repeat("x", length))
	}
	keySets := []*schemapb.IDs{
		{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: integerKeys}}},
		{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: stringKeys}}},
	}
	rowIDs := integerKeys
	indexes := []uint32{0, 1, math.MaxUint32}
	for shift := uint(7); shift < 32; shift += 7 {
		boundary := uint32(1) << shift
		indexes = append(indexes, boundary-1, boundary, boundary+1)
	}
	for _, ids := range keySets {
		count := message.UpsertPrimaryKeyCount(ids)
		for row := 0; row < count; row++ {
			keys, err := idsByOffsets(ids, []int{row})
			require.NoError(t, err)
			for _, rowID := range rowIDs {
				for _, partitionIndex := range indexes {
					for _, offset := range indexes {
						nonColumnFields := &message.UpsertMessageBody{
							PrimaryKeys: keys, RowIds: []int64{rowID}, PartitionIndexes: []uint32{partitionIndex},
							RequestRowMapping: &messagespb.UpsertRequestRowMapping{RowOffsets: []uint32{offset}},
						}
						got, err := upsertRowNonColumnSize(ids, row, rowID, partitionIndex, offset)
						require.NoError(t, err)
						require.Equal(t, proto.Size(nonColumnFields), got, "row=%d rowID=%d partitionIndex=%d offset=%d", row, rowID, partitionIndex, offset)
					}
				}
			}
		}
	}
}

func TestUpsertRowNonColumnSizeErrorsEquivalent(t *testing.T) {
	keySets := []*schemapb.IDs{
		nil,
		{},
		{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}},
		{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"a"}}}},
	}
	for _, ids := range keySets {
		for _, row := range []int{-1, 1, 2} {
			_, want := idsByOffsets(ids, []int{row})
			_, got := upsertRowNonColumnSize(ids, row, 1, 1, 1)
			require.Error(t, want)
			require.Error(t, got)
			require.Equal(t, want.Error(), got.Error())
		}
	}
}

func TestUpsertEmptyDeletePlanShape(t *testing.T) {
	keySets := []*schemapb.IDs{
		nil,
		{},
		{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 1, -1}}}},
		{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"a", "a", ""}}}},
	}
	for _, ids := range keySets {
		for _, deleted := range []*schemapb.IDs{nil, {}, {IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{}}}, {IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{}}}} {
			rows, err := matchUpsertDeletes(ids, deleted)
			require.NoError(t, err)
			require.Nil(t, rows)
			for _, offsets := range rows {
				require.Nil(t, offsets)
			}
		}
	}
}

func upsertRowNonColumnProtoSize(ids *schemapb.IDs, row int, rowID int64, partitionIndex, requestOffset uint32) (int, error) {
	keys, err := idsByOffsets(ids, []int{row})
	if err != nil {
		return 0, err
	}
	return proto.Size(&message.UpsertMessageBody{
		PrimaryKeys: keys, RowIds: []int64{rowID}, PartitionIndexes: []uint32{partitionIndex},
		RequestRowMapping: &messagespb.UpsertRequestRowMapping{RowOffsets: []uint32{requestOffset}},
	}), nil
}

func TestUpsertPackingSizingPreservesMessages(t *testing.T) {
	// Register a private cipher and deterministic test nonces in a subprocess.
	// Production encryption continues to use random nonces.
	if os.Getenv("MILVUS_TEST_UPSERT_SIZING_EQUIVALENCE") == "" {
		executable, err := os.Executable()
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		cmd := exec.CommandContext(ctx, executable, "-test.run=^TestUpsertPackingSizingPreservesMessages$", "-test.v") // #nosec G204 -- Re-exec this test binary with a fixed test name.
		cmd.Env = append(os.Environ(), "MILVUS_TEST_UPSERT_SIZING_EQUIVALENCE=1")
		output, err := cmd.CombinedOutput()
		t.Logf("%s", output)
		require.NoError(t, err)
		return
	}
	block, err := aes.NewCipher(make([]byte, 32))
	require.NoError(t, err)
	aead, err := cipher.NewGCM(block)
	require.NoError(t, err)
	message.RegisterCipher(&upsertMessageTestCipher{aead: aead})
	encrypt := mockey.Mock((*upsertMessageTestCipher).Encrypt).To(func(c *upsertMessageTestCipher, plain []byte) ([]byte, error) {
		nonce := make([]byte, c.aead.NonceSize()) // Fixed only for byte-equivalence tests.
		return c.aead.Seal(nonce, nonce, plain, nil), nil
	}).Build()
	defer encrypt.UnPatch()
	split := &paramtable.Get().ProxyCfg.SplitChunkProxy
	previousSplit := split.SwapTempValue("true")
	defer split.SwapTempValue(previousSplit)
	limit := &paramtable.Get().PulsarCfg.MaxMessageSize
	previousLimit := limit.SwapTempValue("131072")
	defer limit.SwapTempValue(previousLimit)
	for _, multiChunk := range []bool{false, true} {
		for _, stringPK := range []bool{false, true} {
			for _, partitioned := range []bool{false, true} {
				for _, keyed := range []bool{false, true} {
					for _, effects := range []bool{false, true} {
						for _, encrypted := range []bool{false, true} {
							for _, useCAS := range []bool{false, true} {
								t.Run(fmt.Sprintf("chunks_%v/strings_%v/partitions_%v/keyed_%v/effects_%v/encrypted_%v/cas_%v", multiChunk, stringPK, partitioned, keyed, effects, encrypted, useCAS), func(t *testing.T) {
									count, payload := 32, "small"
									if multiChunk {
										count, payload = 512, strings.Repeat("x", 4096)
									}
									source, result := newIdempotentRepackInsertMsgWithPayload(count, payload)
									schema := upsertPackingSchema()
									// Preserve duplicates and delete occurrences across chunk boundaries.
									for row := range result.IDs.GetIntId().Data {
										result.IDs.GetIntId().Data[row] = int64(row / 4)
									}
									if stringPK {
										values := make([]string, count)
										for row := range values {
											values[row] = fmt.Sprintf("key-中文-%d", row/4)
										}
										result.IDs = &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: values}}}
										source.FieldsData[0] = stringFieldData(100, "pk", values)
										schema.Fields[0].DataType = schemapb.DataType_VarChar
									}
									cache := NewMockCache(t)
									var partitionKeys *schemapb.FieldData
									if partitioned {
										partitionKeys = source.FieldsData[1]
										cache.EXPECT().GetPartitions(mock.Anything, "db", "coll").Return(map[string]int64{"partition_0": 300, "partition_1": 301}, nil)
										cache.EXPECT().GetPartitionID(mock.Anything, "db", "coll", "partition_0").Return(int64(300), nil)
										cache.EXPECT().GetPartitionID(mock.Anything, "db", "coll", "partition_1").Return(int64(301), nil)
									} else {
										cache.EXPECT().GetPartitionID(mock.Anything, "db", "coll", "_default").Return(int64(200), nil)
									}
									var decoration *insertIdempotencyDecoration
									if keyed {
										decoration = &insertIdempotencyDecoration{key: "equivalence"}
									}
									var deleted *schemapb.IDs
									if effects {
										deleted, err = idsByOffsets(result.IDs, []int{0, 0, count / 2, count - 1})
										require.NoError(t, err)
									} else if useCAS { // Both nil and explicitly empty effects use the shortcut.
										deleted = &schemapb.IDs{}
									}
									var config *message.CipherConfig
									if encrypted {
										config = &message.CipherConfig{EzID: 7, CollectionID: 10}
									}
									var cas map[string]*messagespb.PartialUpdateCAS
									if useCAS {
										cas = map[string]*messagespb.PartialUpdateCAS{"p_100v0": {ReadTs: 1, ObservedPchannelTerm: 2}}
									}
									pack := func() ([]message.MutableMessage, error) {
										return repackUpsertMessages(t.Context(), cache, []string{"p_100v0"}, source, result.IDs, partitionKeys, schema, 1, config, cas, decoration, deleted, -1)
									}
									// Restore the former sizing/matching steps without copying the packer.
									sizePatch := mockey.Mock(upsertRowNonColumnSize).To(upsertRowNonColumnProtoSize).Build()
									effectsPatch := mockey.Mock(upsertDeleteEffectsSize).To(upsertDeleteEffectsProtoSize).Build()
									matchPatch := mockey.Mock(matchUpsertDeletes).To(matchUpsertDeletesReference).Build()
									want, wantErr := pack()
									matchPatch.UnPatch()
									effectsPatch.UnPatch()
									sizePatch.UnPatch()
									require.NoError(t, wantErr)
									got, gotErr := pack()
									require.NoError(t, gotErr)
									require.Len(t, got, len(want))
									if multiChunk {
										require.Greater(t, len(got), 1)
									} else {
										require.Len(t, got, 1)
									}
									for index := range got {
										require.Equal(t, want[index].IntoMessageProto().Payload, got[index].IntoMessageProto().Payload, "payload at chunk %d", index)
										require.Equal(t, want[index].IntoMessageProto().Properties, got[index].IntoMessageProto().Properties, "properties at chunk %d", index)
										require.True(t, proto.Equal(message.MustAsMutableUpsertMessageV2(want[index]).MustBody(), message.MustAsMutableUpsertMessageV2(got[index]).MustBody()))
									}
								})
							}
						}
					}
				}
			}
		}
	}
}

func TestUpsertPlainRowSizingDoesNotAllocate(t *testing.T) {
	ids := &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"中文"}}}}
	var size int
	var err error
	allocations := testing.AllocsPerRun(10, func() {
		size, err = upsertRowNonColumnSize(ids, 0, -1, math.MaxUint32, math.MaxUint32)
	})
	require.NoError(t, err)
	require.Positive(t, size)
	require.Zero(t, allocations)
}

func TestUpsertDeleteMatchingRemainsEquivalent(t *testing.T) {
	integerKeys := &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{-1, 7, 7, 0}}}}
	stringKeys := &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"", "中文", "中文", "a"}}}}
	missing := &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{999}}}}
	for _, inserted := range []*schemapb.IDs{nil, {}, integerKeys, stringKeys} {
		for _, deleted := range []*schemapb.IDs{nil, {}, integerKeys, stringKeys, missing} {
			want, wantErr := matchUpsertDeletesReference(inserted, deleted)
			got, gotErr := matchUpsertDeletes(inserted, deleted)
			if message.UpsertPrimaryKeyCount(deleted) == 0 {
				require.Nil(t, got)
			} else {
				require.Equal(t, want, got)
			}
			if wantErr != nil {
				require.EqualError(t, gotErr, wantErr.Error())
			} else {
				require.NoError(t, gotErr)
			}
		}
	}
}

func upsertDeleteEffectsProtoSize(ids *schemapb.IDs, offsets []int, partition int64) (int, error) {
	if len(offsets) == 0 {
		return 0, nil
	}
	keys, err := idsByOffsets(ids, offsets)
	if err != nil {
		return 0, err
	}
	return proto.Size(&message.UpsertMessageBody{DeleteEffects: []*messagespb.UpsertDeleteEffect{{PrimaryKeys: keys, Scope: &messagespb.UpsertDeleteEffect_PartitionId{PartitionId: partition}}}}), nil
}

func TestUpsertDeleteEffectsBudgetEquivalent(t *testing.T) {
	sets := []*schemapb.IDs{
		{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{math.MinInt64, -1, 0, 1, 127, 128, 16383, 16384, math.MaxInt64}}}},
		{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"", "a", "中文", strings.Repeat("x", 127), strings.Repeat("x", 128), strings.Repeat("x", 16383), strings.Repeat("x", 16384)}}}},
	}
	for _, ids := range sets {
		offsetSets := [][]int{{}, {0}, {0, 0}, {0, 2, 1, 2}, {1, 1, 1, 1, 1, 1, 1, 1, 1}}
		all := make([]int, message.UpsertPrimaryKeyCount(ids))
		for i := range all {
			all[i] = i
		}
		offsetSets = append(offsetSets, all)
		for _, offsets := range offsetSets {
			for _, partition := range []int64{-1, 0, 1, 127, 128, 16384, math.MaxInt64} {
				effectSize, err := upsertDeleteEffectsSize(ids, offsets, partition)
				require.NoError(t, err)
				for row := 0; row < message.UpsertPrimaryKeyCount(ids); row++ {
					for _, rowID := range []int64{-1, 0, 127, 128, math.MaxInt64} {
						for _, mapping := range []uint32{0, 127, 128, 16384, math.MaxUint32} {
							keys, err := idsByOffsets(ids, []int{row})
							require.NoError(t, err)
							body := &message.UpsertMessageBody{PrimaryKeys: keys, RowIds: []int64{rowID}, PartitionIndexes: []uint32{mapping}, RequestRowMapping: &messagespb.UpsertRequestRowMapping{RowOffsets: []uint32{mapping}}}
							if len(offsets) > 0 {
								deleted, err := idsByOffsets(ids, offsets)
								require.NoError(t, err)
								body.DeleteEffects = []*messagespb.UpsertDeleteEffect{{PrimaryKeys: deleted, Scope: &messagespb.UpsertDeleteEffect_PartitionId{PartitionId: partition}}}
							}
							size, err := upsertRowNonColumnSize(ids, row, rowID, mapping, mapping)
							require.NoError(t, err)
							require.Equal(t, proto.Size(body), size+effectSize)
						}
					}
				}
			}
		}
	}
}

func TestUpsertDeleteEffectsSizeErrorsEquivalent(t *testing.T) {
	sets := []*schemapb.IDs{nil, {}, {IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}}, {IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"a"}}}}}
	for _, ids := range sets {
		for _, offsets := range [][]int{{-1}, {1}, {0, 5}} {
			_, want := idsByOffsets(ids, offsets)
			_, got := upsertDeleteEffectsSize(ids, offsets, -1)
			require.Error(t, want)
			require.Error(t, got)
			require.Equal(t, want.Error(), got.Error())
		}
	}
}
