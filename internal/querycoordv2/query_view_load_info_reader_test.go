package querycoordv2

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/metastore/kv/queryview"
	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestQueryViewLoadInfoResolvesExactSelectedIndexes(t *testing.T) {
	stored := &viewpb.CollectionLoadConfig{
		CollectionID: 1, Version: 9,
		RequiredFields: []*viewpb.LoadFieldConfig{{FieldId: 100, IndexIds: []int64{200, 201}}, {FieldId: 101, IndexIds: []int64{202}}},
		PreloadFields:  []*viewpb.LoadFieldConfig{{FieldId: 100, IndexIds: []int64{200, 201}}},
	}
	catalogPatch := mockey.Mock(queryview.GetCollectionLoadInfo).To(func(context.Context, kv.MetaKv, int64, uint64) (*viewpb.CollectionLoadConfig, error) {
		return proto.Clone(stored).(*viewpb.CollectionLoadConfig), nil
	}).Build()
	defer catalogPatch.UnPatch()
	indexes := []*indexpb.IndexInfo{{FieldID: 100, IndexID: 201}, {FieldID: 100, IndexID: 200}, {FieldID: 101, IndexID: 202}, {FieldID: 100, IndexID: 999}}
	s := &queryViewLoadInfoReader{indexes: func(id int64, ids ...int64) []*indexpb.IndexInfo {
		require.EqualValues(t, 1, id)
		require.ElementsMatch(t, []int64{200, 201, 202}, ids)
		return indexes
	}}
	resp, err := s.GetLoadInfo(t.Context(), 1, 9)
	require.NoError(t, err)
	require.Len(t, resp.IndexInfoList, 3)
	require.EqualValues(t, 200, resp.IndexInfoList[0].IndexID)
	require.EqualValues(t, 202, resp.IndexInfoList[2].IndexID)
	resp.IndexInfoList[0].IndexID = 999
	resp.RequiredFields[0].IndexIds[0] = 999
	require.EqualValues(t, 200, indexes[1].IndexID)
	require.EqualValues(t, 200, stored.RequiredFields[0].IndexIds[0])
	// A newer index on the same field cannot replace a missing selected ID.
	indexes = indexes[1:]
	_, err = s.GetLoadInfo(t.Context(), 1, 9)
	require.ErrorIs(t, err, merr.ErrServiceNotReady)
	// Even a matching ID must belong to the selected field.
	indexes = append(indexes, &indexpb.IndexInfo{FieldID: 101, IndexID: 201})
	_, err = s.GetLoadInfo(t.Context(), 1, 9)
	require.ErrorIs(t, err, merr.ErrServiceNotReady)
}

func TestQueryViewLoadInfoRejectsInvalidHistory(t *testing.T) {
	for _, tc := range []struct {
		name string
		info *viewpb.CollectionLoadConfig
		err  error
		want error
	}{
		{name: "missing scope", info: &viewpb.CollectionLoadConfig{CollectionID: 1, Version: 9}, want: merr.ErrDataIntegrity},
		{name: "missing snapshot", want: merr.ErrServiceNotReady},
		{name: "catalog failure", err: merr.WrapErrServiceUnavailableMsg("catalog busy"), want: merr.ErrServiceUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			patch := mockey.Mock(queryview.GetCollectionLoadInfo).Return(tc.info, tc.err).Build()
			defer patch.UnPatch()
			s := &queryViewLoadInfoReader{indexes: func(int64, ...int64) []*indexpb.IndexInfo {
				t.Fatal("invalid history must fail before resolving indexes")
				return nil
			}}
			_, err := s.GetLoadInfo(t.Context(), 1, 9)
			require.ErrorIs(t, err, tc.want)
		})
	}
}
