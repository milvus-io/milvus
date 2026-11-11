package rootcoord

import (
	"context"
	"math"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestPrepareExternalRefreshVersion(t *testing.T) {
	coll := &model.Collection{CollectionID: 1, Name: "ext", State: etcdpb.CollectionState_CollectionCreated, SchemaVersion: 4, Fields: []*model.Field{{FieldID: 100, ExternalField: "id", DataType: schemapb.DataType_Int64}}, ExternalSource: "s3://old", ExternalSpec: `{"format":"parquet"}`}
	core := &Core{meta: &MetaTable{}}
	get := mockey.Mock((*MetaTable).GetCollectionByIDWithMaxTs).To(func(*MetaTable, context.Context, int64) (*model.Collection, error) { return coll, nil }).Build()
	defer get.UnPatch()
	schema, err := core.PrepareExternalRefreshSchema(t.Context(), 1, "", "")
	require.NoError(t, err)
	require.EqualValues(t, 4, schema.GetVersion())
	require.Equal(t, coll.ExternalSource, schema.GetExternalSource())
	schema, err = core.PrepareExternalRefreshSchema(t.Context(), 1, coll.ExternalSource, coll.ExternalSpec)
	require.NoError(t, err)
	require.EqualValues(t, 5, schema.GetVersion(), "explicit identical overrides still advance the version")
	schema, err = core.PrepareExternalRefreshSchema(t.Context(), 1, "s3://new", `{"format":"parquet"}`)
	require.NoError(t, err)
	require.Equal(t, "s3://new", schema.GetExternalSource())
	require.EqualValues(t, 4, coll.SchemaVersion)
	require.Equal(t, "s3://old", coll.ExternalSource)
	_, err = core.PrepareExternalRefreshSchema(t.Context(), 1, "s3://new", "")
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
	coll.SchemaVersion = math.MaxInt32
	_, err = core.PrepareExternalRefreshSchema(t.Context(), 1, "s3://new", `{}`)
	require.Error(t, err)
	coll.ExternalSource = ""
	coll.ExternalSpec = ""
	coll.Fields = []*model.Field{{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64}}
	_, err = core.PrepareExternalRefreshSchema(t.Context(), 1, "", "")
	require.Error(t, err)
	get.UnPatch()
	fail := mockey.Mock((*MetaTable).GetCollectionByIDWithMaxTs).Return(nil, merr.ErrCollectionNotFound).Build()
	defer fail.UnPatch()
	_, err = core.PrepareExternalRefreshSchema(t.Context(), 1, "", "")
	require.ErrorIs(t, err, merr.ErrCollectionNotFound)
}

func TestRefreshPublicationReusesOriginalFences(t *testing.T) {
	for _, failure := range []string{"prepare", "collection", "cache", "expire", "success"} {
		t.Run(failure, func(t *testing.T) {
			core := &Core{meta: &MetaTable{}}
			schema := &schemapb.CollectionSchema{Version: 2, ExternalSource: "s3://new"}
			msg := message.NewRefreshExternalCollectionMessageBuilderV2().WithHeader(&message.RefreshExternalCollectionMessageHeader{CollectionId: 1}).WithBody(&message.RefreshExternalCollectionMessageBody{Schema: schema}).WithBroadcast([]string{"v1"}).MustBuildBroadcast()
			result := message.BroadcastResultRefreshExternalCollectionMessageV2{Message: message.MustAsBroadcastRefreshExternalCollectionMessageV2(msg), Results: map[string]*message.AppendResult{"v1": {TimeTick: 100}}}
			prepare := mockey.Mock((*DDLCallback).prepareAndPublishCollectionSchema).To(func(_ *DDLCallback, _ context.Context, actual message.BroadcastResultAlterCollectionMessageV2) error {
				require.Equal(t, result.Results, actual.Results)
				require.True(t, proto.Equal(schema, actual.Message.MustBody().GetUpdates().GetSchema()))
				if failure == "prepare" {
					return merr.ErrServiceNotReady
				}
				return nil
			}).Build()
			defer prepare.UnPatch()
			var getErr, cacheErr, expireErr error
			if failure == "collection" {
				getErr = merr.ErrCollectionNotFound
			}
			if failure == "cache" {
				cacheErr = merr.ErrServiceNotReady
			}
			if failure == "expire" {
				expireErr = merr.ErrServiceNotReady
			}
			get := mockey.Mock((*MetaTable).GetCollectionByIDWithMaxTs).Return(&model.Collection{CollectionID: 1, Name: "ext"}, getErr).Build()
			defer get.UnPatch()
			cache := mockey.Mock((*Core).getCacheExpireForCollection).Return(&message.CacheExpirations{}, cacheErr).Build()
			defer cache.UnPatch()
			expire := mockey.Mock((*DDLCallback).ExpireCaches).Return(expireErr).Build()
			defer expire.UnPatch()
			err := core.PublishExternalRefreshSchema(t.Context(), result)
			if failure == "success" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}
