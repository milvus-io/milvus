package qvresource

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/hook"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func patchCollectionLifetime(t *testing.T, p *mockey.Mocker) {
	t.Helper()
	t.Cleanup(func() { p.UnPatch() })
}

func patchNativeCollections(t *testing.T) *mockey.Mocker {
	t.Helper()
	paramtable.Init()
	patchCollectionLifetime(t, mockey.Mock(segcore.CreateCCollection).To(func(req *segcore.CreateCCollectionRequest) (*segcore.CCollection, error) {
		return &segcore.CCollection{}, nil
	}).Build())
	patchCollectionLifetime(t, mockey.Mock((*segcore.CCollection).UpdateSchema).Return(nil).Build())
	patchCollectionLifetime(t, mockey.Mock((*segcore.CCollection).UpdateIndexMeta).Return(nil).Build())
	release := mockey.Mock((*segcore.CCollection).Release).Return().Build()
	patchCollectionLifetime(t, release)
	return release
}

func collectionMetadata(version int32) *fakeQVLoadMetadataProvider {
	return &fakeQVLoadMetadataProvider{collection: &milvuspb.DescribeCollectionResponse{DbName: "db", UpdateTimestamp: 999, Schema: &schemapb.CollectionSchema{Name: "c", Version: version, Fields: []*schemapb.FieldSchema{{FieldID: 100, Name: "pk", IsPrimaryKey: true, DataType: schemapb.DataType_Int64}}}}}
}

func collectionView(collectionID int64) *qviews.QueryViewAtQueryNode {
	return qviews.NewQueryViewAtQueryNode(&viewpb.QueryViewMeta{CollectionId: collectionID}, &viewpb.QueryViewOfQueryNode{}).(*qviews.QueryViewAtQueryNode)
}

func acquireRuntime(t *testing.T, m *queryViewCollectionRuntimeManager, id int64) *queryViewCollectionRuntimeGuard {
	t.Helper()
	g, _, err := m.Acquire(context.Background(), collectionView(id))
	require.NoError(t, err)
	return g.(*queryViewCollectionRuntimeGuard)
}

func TestCollectionRuntimeVersionIdentityAndLifetime(t *testing.T) {
	release := patchNativeCollections(t)
	meta := collectionMetadata(1)
	m := newQueryViewCollectionRuntimeManager(meta)
	v1 := acquireRuntime(t, m, 10)
	same := acquireRuntime(t, m, 10)
	require.Same(t, v1.CCollection(), same.CCollection())
	require.EqualValues(t, 1, v1.SchemaVersion())
	meta.collection.Schema.Version = 2
	v2 := acquireRuntime(t, m, 10)
	other := acquireRuntime(t, m, 20)
	require.NotSame(t, v1.CCollection(), v2.CCollection())
	require.Len(t, m.collections, 3)
	require.EqualValues(t, 1, v1.Schema().GetVersion(), "metadata ownership must be isolated")
	retained, err := v1.Retain()
	require.NoError(t, err)
	v1.Release()
	same.Release()
	require.Equal(t, 0, release.Times())
	require.NoError(t, retained.(qnview.CollectionIndexMetaUpdater).UpdateIndexMeta(context.Background(), nil))
	retained.Release()
	retained.Release()
	require.Equal(t, 1, release.Times())
	_, err = v1.Retain()
	require.ErrorIs(t, err, merr.ErrCollectionNotFound)
	v2.Release()
	other.Release()
	require.Empty(t, m.collections)
	require.Equal(t, 3, release.Times())
}

func TestCollectionRuntimeConcurrentAcquire(t *testing.T) {
	release := patchNativeCollections(t)
	m := newQueryViewCollectionRuntimeManager(collectionMetadata(3))
	anchor := acquireRuntime(t, m, 10)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			g, _, err := m.Acquire(context.Background(), collectionView(10))
			if err != nil {
				t.Error(err)
				return
			}
			g.Release()
		}()
	}
	wg.Wait()
	require.Len(t, m.collections, 1)
	require.Equal(t, 0, release.Times())
	anchor.Release()
	require.Equal(t, 1, release.Times())
}

func TestCollectionRuntimePinsLoadInfo(t *testing.T) {
	patchNativeCollections(t)
	meta := collectionMetadata(1)
	meta.loadFields = []int64{100}
	m := newQueryViewCollectionRuntimeManager(meta)
	g := acquireRuntime(t, m, 10)
	defer g.Release()
	owned := g.LoadInfo()
	owned.LoadFields[0].FieldId = 200
	require.EqualValues(t, 100, g.LoadInfo().LoadFields[0].FieldId)
	require.Equal(t, qnview.QueryViewLoadInfoVersion(0), g.LoadInfo().Version)
	meta.loadFields = nil
	all := acquireRuntime(t, m, 10)
	defer all.Release()
	require.EqualValues(t, 100, all.LoadInfo().LoadFields[0].FieldId)
}

func TestCollectionRuntimeMetadataErrors(t *testing.T) {
	for _, tc := range []struct {
		err   error
		retry bool
	}{{merr.WrapErrNodeNotMatch(1, 2), true}, {merr.WrapErrCollectionNotFound(1), false}, {merr.WrapErrParameterInvalidMsg("bad"), false}} {
		m := newQueryViewCollectionRuntimeManager(&fakeQVLoadMetadataProvider{err: tc.err})
		g, retry, err := m.Acquire(context.Background(), collectionView(1))
		require.Nil(t, g)
		require.ErrorIs(t, err, tc.err)
		require.Equal(t, tc.retry, retry)
	}
	m := newQueryViewCollectionRuntimeManager(&fakeQVLoadMetadataProvider{})
	_, _, err := m.Acquire(context.Background(), collectionView(1))
	require.Error(t, err)
	_, _, err = m.Acquire(context.Background(), nil)
	require.Error(t, err)
}

func TestCollectionIndexUpdateUsesPinnedSchema(t *testing.T) {
	paramtable.Init()
	patchCollectionLifetime(t, mockey.Mock(segcore.CreateCCollection).Return(&segcore.CCollection{}, nil).Build())
	patchCollectionLifetime(t, mockey.Mock((*segcore.CCollection).UpdateSchema).Return(nil).Build())
	patchCollectionLifetime(t, mockey.Mock((*segcore.CCollection).Release).Return().Build())
	var target *segcore.CCollection
	var indexMeta *segcorepb.CollectionIndexMeta
	patchCollectionLifetime(t, mockey.Mock((*segcore.CCollection).UpdateIndexMeta).To(func(c *segcore.CCollection, m *segcorepb.CollectionIndexMeta) error {
		target = c
		indexMeta = proto.Clone(m).(*segcorepb.CollectionIndexMeta)
		return nil
	}).Build())
	m := newQueryViewCollectionRuntimeManager(collectionMetadata(1))
	g := acquireRuntime(t, m, 1)
	defer g.Release()
	require.NoError(t, g.UpdateIndexMeta(context.Background(), []*indexpb.IndexInfo{{CollectionID: 1, FieldID: 100}}))
	require.Same(t, g.CCollection(), target)
	require.EqualValues(t, 100, indexMeta.IndexMetas[0].FieldID)
}

type runtimeCipher struct{ hook.Cipher }

func (*runtimeCipher) GetUnsafeKey(int64, int64) []byte { panic("mockey") }

func TestCollectionRuntimeCreationRollbackAndEncryptionLifetime(t *testing.T) {
	paramtable.Init()
	for _, stage := range []string{"create", "schema", "plugin", "success"} {
		t.Run(stage, func(t *testing.T) {
			failure := merr.WrapErrServiceInternalMsg("injected initialization failure")
			stageError := func(s string) error {
				if stage == s {
					return failure
				}
				return nil
			}
			patchCollectionLifetime(t, mockey.Mock(segcore.CreateCCollection).Return(&segcore.CCollection{}, stageError("create")).Build())
			patchCollectionLifetime(t, mockey.Mock((*segcore.CCollection).UpdateSchema).Return(stageError("schema")).Build())
			released := mockey.Mock((*segcore.CCollection).Release).Return().Build()
			patchCollectionLifetime(t, released)
			patchCollectionLifetime(t, mockey.Mock(hookutil.GetCipher).Return(&runtimeCipher{}).Build())
			patchCollectionLifetime(t, mockey.Mock((*runtimeCipher).GetUnsafeKey).Return([]byte("test-key")).Build())
			registered := mockey.Mock(segcore.PutOrRefPluginContext).Return(stageError("plugin")).Build()
			patchCollectionLifetime(t, registered)
			unregistered := mockey.Mock(segcore.UnRefPluginContext).Return(nil).Build()
			patchCollectionLifetime(t, unregistered)
			metadata := collectionMetadata(1)
			metadata.collection.Schema.Properties = []*commonpb.KeyValuePair{{Key: common.EncryptionEzIDKey, Value: "7"}}
			manager := newQueryViewCollectionRuntimeManager(metadata)
			first, _, err := manager.Acquire(context.Background(), collectionView(1))
			if stage != "success" {
				require.ErrorIs(t, err, failure)
				require.Nil(t, first)
				require.Empty(t, manager.collections)
				require.Zero(t, unregistered.Times(), "failed registration owns no plugin reference")
				if stage != "create" {
					require.Equal(t, 1, released.Times())
				}
				return
			}
			require.NoError(t, err)
			second := acquireRuntime(t, manager, 1)
			require.Equal(t, 1, registered.Times(), "one plugin reference per native collection, not per view")
			first.Release()
			require.Zero(t, unregistered.Times())
			second.Release()
			second.Release()
			require.Equal(t, 1, released.Times())
			require.Equal(t, 1, unregistered.Times())
		})
	}
}

func TestCollectionRuntimeRejectsMismatchedLoadInfo(t *testing.T) {
	for _, mismatch := range []qnview.QueryViewLoadInfo{{CollectionID: 2}, {CollectionID: 1, Version: 1}} {
		t.Run(fmt.Sprint(mismatch), func(t *testing.T) {
			patchCollectionLifetime(t, mockey.Mock((*fakeQVLoadMetadataProvider).GetQueryViewLoadInfo).Return(mismatch, nil).Build())
			manager := newQueryViewCollectionRuntimeManager(collectionMetadata(1))
			runtime, retry, err := manager.Acquire(context.Background(), collectionView(1))
			require.Error(t, err)
			require.False(t, retry)
			require.Nil(t, runtime)
			require.Empty(t, manager.collections)
		})
	}
}
