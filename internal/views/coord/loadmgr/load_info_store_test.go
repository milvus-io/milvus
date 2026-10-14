package loadmgr

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/metastore/kv/queryview"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type loadInfoTestCatalog struct{ queryview.LoadInfoCatalog }

func (*loadInfoTestCatalog) GetLoadInfo(context.Context, int64, uint64) (*querypb.GetQueryViewLoadInfoResponse, error) {
	panic("unpatched")
}

func (*loadInfoTestCatalog) SaveLoadInfo(context.Context, *querypb.GetQueryViewLoadInfoResponse) error {
	panic("unpatched")
}

func TestLoadInfoHistorySurvivesConfigChangeAndRestart(t *testing.T) {
	mockey.PatchConvey("exact load snapshots retain historical index descriptors", t, func() {
		durable := make(map[loadInfoKey]*querypb.GetQueryViewLoadInfoResponse)
		mockey.Mock((*loadInfoTestCatalog).GetLoadInfo).To(func(_ *loadInfoTestCatalog, _ context.Context, id int64, version uint64) (*querypb.GetQueryViewLoadInfoResponse, error) {
			if info := durable[loadInfoKey{id, version}]; info != nil {
				return proto.Clone(info).(*querypb.GetQueryViewLoadInfoResponse), nil
			}
			return nil, nil
		}).Build()
		saves := mockey.Mock((*loadInfoTestCatalog).SaveLoadInfo).To(func(_ *loadInfoTestCatalog, _ context.Context, info *querypb.GetQueryViewLoadInfoResponse) error {
			durable[loadInfoKey{info.CollectionID, info.Version}] = proto.Clone(info).(*querypb.GetQueryViewLoadInfoResponse)
			return nil
		}).Build()
		indexes := []*indexpb.IndexInfo{{FieldID: 100, IndexID: 200, IndexName: "old"}}
		source := func(int64) []*indexpb.IndexInfo { return indexes }
		store := NewLoadInfoStore(&loadInfoTestCatalog{}, source)
		cfg := &LoadConfig{CollectionID: 1, PartitionIDs: []int64{10}, LoadFields: []*messagespb.LoadFieldConfig{{FieldId: 100, IndexId: 200}}}
		ctx := context.Background()
		require.NoError(t, store.Ensure(ctx, cfg))
		oldVersion := loadInfoVersion(cfg)
		indexes = []*indexpb.IndexInfo{{FieldID: 101, IndexID: 201, IndexName: "new"}}
		require.NoError(t, store.Ensure(ctx, cfg), "existing identity must not read current indexes")
		require.Equal(t, 1, saves.Times())
		cfg.PartitionIDs[0] = 11
		cfg.LoadFields = []*messagespb.LoadFieldConfig{{FieldId: 101, IndexId: 201}}
		require.NoError(t, store.Ensure(ctx, cfg))
		old, err := store.Get(ctx, 1, oldVersion)
		require.NoError(t, err)
		require.Equal(t, []int64{10}, old.PartitionIDs)
		require.Equal(t, "old", old.IndexInfoList[0].IndexName)
		old.IndexInfoList[0].IndexName = "caller mutation"
		again, err := store.Get(ctx, 1, oldVersion)
		require.NoError(t, err)
		require.Equal(t, "old", again.IndexInfoList[0].IndexName)
		restarted := NewLoadInfoStore(&loadInfoTestCatalog{}, source)
		old, err = restarted.Get(ctx, 1, oldVersion)
		require.NoError(t, err)
		require.Equal(t, "old", old.IndexInfoList[0].IndexName)
		require.NoError(t, restarted.Ensure(ctx, cfg))
		require.Equal(t, 2, saves.Times(), "recovery must not replace durable snapshots")
		_, err = restarted.Get(ctx, 1, 123)
		require.ErrorIs(t, err, merr.ErrServiceNotReady)
	})
}

func TestLoadInfoSnapshotRequiresMatchingIndex(t *testing.T) {
	mockey.PatchConvey("missing index descriptors cannot publish a config", t, func() {
		mockey.Mock((*loadInfoTestCatalog).GetLoadInfo).Return(nil, nil).Build()
		saves := mockey.Mock((*loadInfoTestCatalog).SaveLoadInfo).Return(nil).Build()
		store := NewLoadInfoStore(&loadInfoTestCatalog{}, func(int64) []*indexpb.IndexInfo { return []*indexpb.IndexInfo{{FieldID: 101, IndexID: 200}} })
		cfg := &LoadConfig{CollectionID: 1, LoadFields: []*messagespb.LoadFieldConfig{{FieldId: 100, IndexId: 200}}}
		require.ErrorIs(t, store.Ensure(context.Background(), cfg), merr.ErrServiceNotReady)
		require.Zero(t, saves.Times())
		cfg.LoadFields[0].IndexId = 0
		require.NoError(t, store.Ensure(context.Background(), cfg), "raw field loading requires no index")
		require.Equal(t, 1, saves.Times())
	})
}

func TestLoadInfoPersistenceFailureDoesNotPublishConfig(t *testing.T) {
	mockey.PatchConvey("failed durable snapshot remains retriable", t, func() {
		mockey.Mock((*loadInfoTestCatalog).GetLoadInfo).Return(nil, nil).Build()
		writes := 0
		mockey.Mock((*loadInfoTestCatalog).SaveLoadInfo).To(func(_ *loadInfoTestCatalog, _ context.Context, _ *querypb.GetQueryViewLoadInfoResponse) error {
			writes++
			return merr.WrapErrServiceNotReadyMsg("storage unavailable")
		}).Build()
		// Ensure must fail before touching the config catalog or publishing.
		store := &LoadConfigStore{loadInfos: NewLoadInfoStore(&loadInfoTestCatalog{}, func(int64) []*indexpb.IndexInfo { return nil })}
		cfg := &LoadConfig{CollectionID: 1, LoadFields: []*messagespb.LoadFieldConfig{{FieldId: 100}}}
		require.ErrorIs(t, store.putLocked(t.Context(), cfg), merr.ErrServiceNotReady)
		require.Nil(t, store.GetConfig(1))
		require.ErrorIs(t, store.putLocked(t.Context(), cfg), merr.ErrServiceNotReady)
		require.Equal(t, 2, writes, "failed persistence must not populate the immutable cache")
	})
}
