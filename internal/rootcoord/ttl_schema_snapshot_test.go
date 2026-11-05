package rootcoord

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/messageutil"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type (
	ttlSchemaMeta      struct{ IMetaTable }
	ttlSchemaBroadcast struct{ broadcaster.BroadcastAPI }
)

func TestTTLPropertiesVersionImmutableSnapshot(t *testing.T) {
	for _, tc := range []struct {
		name             string
		old, updates     map[string]string
		deletes          []string
		changed, ignored bool
	}{
		{name: "duration set", updates: map[string]string{common.CollectionTTLConfigKey: "30"}, changed: true},
		{name: "duration modify", old: map[string]string{common.CollectionTTLConfigKey: "30"}, updates: map[string]string{common.CollectionTTLConfigKey: "60"}, changed: true},
		{name: "duration delete", old: map[string]string{common.CollectionTTLConfigKey: "30"}, deletes: []string{common.CollectionTTLConfigKey}, changed: true},
		{name: "duration unchanged", old: map[string]string{common.CollectionTTLConfigKey: "30"}, updates: map[string]string{common.CollectionTTLConfigKey: "30"}, ignored: true},
		{name: "absent duration delete", deletes: []string{common.CollectionTTLConfigKey}, ignored: true},
		{name: "field set", updates: map[string]string{common.CollectionTTLFieldKey: "ttl"}, changed: true},
		{name: "field switch", old: map[string]string{common.CollectionTTLFieldKey: "ttl"}, updates: map[string]string{common.CollectionTTLFieldKey: "next"}, changed: true},
		{name: "field delete", old: map[string]string{common.CollectionTTLFieldKey: "ttl"}, deletes: []string{common.CollectionTTLFieldKey}, changed: true},
		{name: "field unchanged", old: map[string]string{common.CollectionTTLFieldKey: "ttl"}, updates: map[string]string{common.CollectionTTLFieldKey: "ttl"}, ignored: true},
		{name: "absent field delete", deletes: []string{common.CollectionTTLFieldKey}, ignored: true},
		{name: "both change once", updates: map[string]string{common.CollectionTTLFieldKey: "ttl", common.CollectionTTLConfigKey: "60"}, changed: true},
		{name: "metadata only", old: map[string]string{common.CollectionTTLConfigKey: "30"}, updates: map[string]string{common.TimezoneKey: "Asia/Shanghai"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			coll := &model.Collection{
				CollectionID: 1, Name: "test", DBName: "default", SchemaVersion: 7,
				VirtualChannelNames: []string{"p_1v0"}, Properties: common.NewKeyValuePairs(tc.old),
				ExternalSource: "s3://bucket/path", ExternalSpec: "external-spec",
				Fields: []*model.Field{{FieldID: 100, Name: "ttl"}, {FieldID: 101, Name: "next"}},
			}
			original := proto.Clone(coll.ToCollectionSchemaPB())
			api := &ttlSchemaBroadcast{}
			patch := mockey.Mock((*Core).startBroadcastWithAliasOrCollectionLock).Return(api, nil).Build()
			defer patch.UnPatch()
			patch = mockey.Mock((*ttlSchemaBroadcast).Close).Return().Build()
			defer patch.UnPatch()
			patch = mockey.Mock((*ttlSchemaMeta).GetCollectionByName).To(func(*ttlSchemaMeta, context.Context, string, string, typeutil.Timestamp, bool) (*model.Collection, error) {
				return coll, nil
			}).Build()
			defer patch.UnPatch()
			patch = mockey.Mock((*Core).getCacheExpireForCollection).Return(nil, nil).Build()
			defer patch.UnPatch()
			var sentHeader *messagespb.AlterCollectionMessageHeader
			var sentBody *messagespb.AlterCollectionMessageBody
			patch = mockey.Mock((*ttlSchemaBroadcast).Broadcast).To(func(_ *ttlSchemaBroadcast, _ context.Context, msg message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error) {
				alter := message.MustAsMutableAlterCollectionMessageV2(msg)
				// Copy pointer arguments while the patched call is active.
				sentHeader = proto.Clone(alter.Header()).(*messagespb.AlterCollectionMessageHeader)
				sentBody = proto.Clone(alter.MustBody()).(*messagespb.AlterCollectionMessageBody)
				return &types.BroadcastAppendResult{}, nil
			}).Build()
			defer patch.UnPatch()
			core := &Core{meta: &ttlSchemaMeta{}}
			err := core.broadcastAlterCollectionForAlterCollection(t.Context(), &milvuspb.AlterCollectionRequest{
				DbName: "default", CollectionName: "test", Properties: common.NewKeyValuePairs(tc.updates), DeleteKeys: tc.deletes,
			})
			if tc.ignored {
				require.ErrorIs(t, err, errIgnoredAlterCollection)
				require.Nil(t, sentHeader)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, sentHeader)
			require.Equal(t, tc.changed, messageutil.IsSchemaChange(sentHeader))
			if tc.changed {
				snapshot := sentBody.GetUpdates().GetSchema()
				require.EqualValues(t, 8, snapshot.GetVersion())
				require.Equal(t, coll.ExternalSource, snapshot.GetExternalSource())
				require.Equal(t, coll.ExternalSpec, snapshot.GetExternalSpec())
				require.True(t, common.CloneKeyValuePairs(snapshot.GetProperties()).Equal(sentBody.GetUpdates().GetProperties()))
			}
			require.True(t, proto.Equal(original, coll.ToCollectionSchemaPB()), "building the next snapshot must not mutate the published schema")
		})
	}
}
