package dml

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

var emptyDeletePlanSink map[uint32][]int

func TestUpsertEmptyDeletesSkipPKMap(t *testing.T) {
	keys := make([]int64, 1024)
	for i := range keys {
		keys[i] = int64(i)
	}
	inserted := &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: keys}}}
	allocations := testing.AllocsPerRun(5, func() {
		plan, err := matchUpsertDeletes(inserted, nil, nil)
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

func TestUpsertEmptyDeletePlanShape(t *testing.T) {
	inserted := &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 1, -1}}}}
	for _, deleted := range []*schemapb.IDs{nil, {}, {IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{}}}, {IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{}}}} {
		rows, err := matchUpsertDeletes(inserted, deleted, nil)
		require.NoError(t, err)
		require.Nil(t, rows)
	}
}

func TestUpsertDeleteMatchingRemainsEquivalent(t *testing.T) {
	integerKeys := &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{-1, 7, 7, 0}}}}
	stringKeys := &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"", "中文", "中文", "a"}}}}
	reorderedInts := &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{7, 0, 7, -1, 7}}}}
	reorderedStrings := &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"中文", "a", "中文", ""}}}}
	missing := &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{999}}}}
	for _, inserted := range []*schemapb.IDs{nil, {}, integerKeys, stringKeys} {
		for _, deleted := range []*schemapb.IDs{nil, {}, integerKeys, stringKeys, reorderedInts, reorderedStrings, missing} {
			channels := make([]uint32, typeutil.GetSizeOfIDs(inserted))
			if typeutil.GetSizeOfIDs(inserted) > 0 {
				var err error
				channels, err = typeutil.HashPK2Channels(inserted, []string{"a", "b", "c"})
				require.NoError(t, err)
			}
			wantRows, wantErr := matchUpsertDeletesReference(inserted, deleted)
			var want map[uint32][]int
			if wantErr == nil {
				want = make(map[uint32][]int)
				for row, offsets := range wantRows {
					want[channels[row]] = append(want[channels[row]], offsets...)
				}
				for channel, offsets := range want {
					if len(offsets) == 0 {
						delete(want, channel)
					} else {
						sort.Ints(offsets)
					}
				}
			}
			got, gotErr := matchUpsertDeletes(inserted, deleted, channels)
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
