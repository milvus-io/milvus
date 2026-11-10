package segcore

import (
	"encoding/base64"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func cipherCollectionRequest(id int64, shared bool) *CreateCCollectionRequest {
	return &CreateCCollectionRequest{CollectionID: id, ShareSchema: shared, Schema: &schemapb.CollectionSchema{
		Name: "cipher-lifetime", Version: 1,
		Properties: []*commonpb.KeyValuePair{{Key: common.EncryptionEzIDKey, Value: "19"}},
		Fields:     []*schemapb.FieldSchema{{FieldID: 100, Name: "pk", IsPrimaryKey: true, DataType: schemapb.DataType_Int64}},
	}}
}

func TestCollectionCipherContextFollowsNativeConsumers(t *testing.T) {
	mockey.PatchConvey("old read bindings cannot revoke another binding or growing collection's key", t, func() {
		const collectionID = int64(991901)
		key := base64.StdEncoding.EncodeToString(make([]byte, 32))
		mockey.Mock(hookutil.GetCollectionPluginContext).Return(&indexcgopb.StoragePluginContext{EncryptionKey: key}, nil).Build()
		registered, released := 0, 0
		mockey.Mock(PutOrRefPluginContext).To(func(ez *hookutil.EZ, actual string) error {
			require.Equal(t, hookutil.EZ{EzID: 19, CollectionID: collectionID}, *ez)
			require.Equal(t, key, actual)
			registered++
			return nil
		}).Build()
		mockey.Mock(UnRefPluginContext).To(func(ez *hookutil.EZ) error {
			require.Equal(t, collectionID, ez.CollectionID)
			released++
			return nil
		}).Build()
		growing, err := CreateCCollection(cipherCollectionRequest(collectionID, false))
		require.NoError(t, err)
		oldView, err := CreateCCollection(cipherCollectionRequest(collectionID, true))
		require.NoError(t, err)
		nextView, err := CreateCCollection(cipherCollectionRequest(collectionID, true))
		require.NoError(t, err)
		require.Equal(t, 1, registered)
		oldView.Release()
		require.Zero(t, released)
		growing.Release()
		require.Zero(t, released, "the last QueryView still uses the cipher context")
		nextView.Release()
		require.Equal(t, 1, released)
		fresh, err := CreateCCollection(cipherCollectionRequest(collectionID, false))
		require.NoError(t, err)
		require.Equal(t, 2, registered, "a cold reload must reacquire its key")
		fresh.Release()
		require.Equal(t, 2, released)
	})
}

func TestCollectionCipherContextCreationFailure(t *testing.T) {
	for _, shared := range []bool{false, true} {
		t.Run(map[bool]string{false: "growing", true: "shared"}[shared], func(t *testing.T) {
			mockey.PatchConvey("a missing key or failed registration must not publish an unprotected collection", t, func() {
				const collectionID = int64(991902)
				unavailable := merr.WrapErrServiceUnavailableMsg("key unavailable")
				lookup := mockey.Mock(hookutil.GetCollectionPluginContext).Return(nil, unavailable).Build()
				register := mockey.Mock(PutOrRefPluginContext).Return(unavailable).Build()
				release := mockey.Mock(UnRefPluginContext).Return(nil).Build()
				request := cipherCollectionRequest(collectionID, shared)
				collection, err := CreateCCollection(request)
				require.Nil(t, collection)
				require.ErrorIs(t, err, merr.ErrServiceUnavailable)
				require.Zero(t, register.Times())
				lookup.Return(&indexcgopb.StoragePluginContext{EncryptionKey: "binary-key-in-base64"}, nil)
				collection, err = CreateCCollection(request)
				require.Nil(t, collection)
				require.ErrorIs(t, err, merr.ErrServiceUnavailable)
				require.Zero(t, release.Times())
				register.Return(nil)
				collection, err = CreateCCollection(request)
				require.NoError(t, err)
				collection.Release()
				require.Equal(t, 1, release.Times(), "failed creation must not leave a context reference")
			})
		})
	}
}
