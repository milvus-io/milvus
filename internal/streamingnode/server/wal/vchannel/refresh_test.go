package vchannel

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestRefreshSchemaHistoryAndLegacy(t *testing.T) {
	meta := schemaGCTestMeta()
	v := NewVChannelViewFromMeta(meta)
	makeMessage := func(schema *schemapb.CollectionSchema) message.ImmutableRefreshExternalCollectionMessageV2 {
		m := message.NewRefreshExternalCollectionMessageBuilderV2().WithVChannel("v1").WithHeader(&message.RefreshExternalCollectionMessageHeader{CollectionId: 1}).WithBody(&message.RefreshExternalCollectionMessageBody{Schema: schema}).MustBuildMutable().WithTimeTick(60).WithLastConfirmedUseMessageID().IntoImmutableMessage(walimplstest.NewTestMessageID(60))
		return message.MustAsImmutableRefreshExternalCollectionMessageV2(m)
	}
	require.False(t, v.ObserveRefreshExternalCollectionMessageV2(makeMessage(nil)))
	schemas := meta.GetCollectionInfo().GetSchemas()
	require.False(t, v.ObserveRefreshExternalCollectionMessageV2(makeMessage(schemas[len(schemas)-1].GetSchema())), "data-only refresh preserves schema history")
	target := &schemapb.CollectionSchema{Version: 6, Fields: []*schemapb.FieldSchema{{FieldID: 100, ExternalField: "id"}}, ExternalSource: "s3://new", ExternalSpec: `{"format":"parquet"}`}
	require.True(t, v.ObserveRefreshExternalCollectionMessageV2(makeMessage(target)))
	require.False(t, v.ObserveRefreshExternalCollectionMessageV2(makeMessage(target)))
	current, err := v.GetSchemaVersion(1, 6)
	require.NoError(t, err)
	require.True(t, proto.Equal(target, current.GetSchema()))
	_, changed := v.SchemaCleanupPlan(60, nil)
	require.False(t, changed, "external sealed views are not covered by SN segment references")
	old, err := v.GetSchemaVersion(1, 5)
	require.NoError(t, err)
	require.EqualValues(t, 5, old.GetSchema().GetVersion())
}

func TestRefreshRecoveryModuleInstallsTarget(t *testing.T) {
	module, err := NewModule(ModuleConfig{PChannel: "p1", VChannel: "v1", VChannelMeta: schemaGCTestMeta()})
	require.NoError(t, err)
	target := &schemapb.CollectionSchema{Version: 6, ExternalSource: "s3://new", Fields: []*schemapb.FieldSchema{{FieldID: 100, ExternalField: "id"}}}
	msg := message.NewRefreshExternalCollectionMessageBuilderV2().WithVChannel("v1").WithHeader(&message.RefreshExternalCollectionMessageHeader{CollectionId: 1}).WithBody(&message.RefreshExternalCollectionMessageBody{Schema: target}).MustBuildMutable().WithTimeTick(60).WithLastConfirmedUseMessageID().IntoImmutableMessage(walimplstest.NewTestMessageID(60))
	owned := message.NewOwnedImmutableMessage(msg, nil)
	defer owned.Release()
	retained := owned.Clone()
	defer retained.Release()
	require.True(t, module.ObserveMessage(t.Context(), retained))
	schema, err := module.vchannelView.GetSchemaVersion(1, 6)
	require.NoError(t, err)
	require.True(t, proto.Equal(target, schema.GetSchema()))
}
