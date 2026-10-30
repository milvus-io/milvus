package rootcoord

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	qvresource "github.com/milvus-io/milvus/internal/views/coord/resource"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestSchemaPublicationRetry(t *testing.T) {
	channel := funcutil.GetControlChannel("test")
	raw := message.NewAlterCollectionMessageBuilderV2().WithHeader(&messagespb.AlterCollectionMessageHeader{
		CollectionId: 1, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{message.FieldMaskCollectionSchema}},
	}).WithBody(&messagespb.AlterCollectionMessageBody{Updates: &messagespb.AlterCollectionMessageUpdates{
		Schema: &schemapb.CollectionSchema{Version: 2},
	}}).WithBroadcast([]string{channel}).MustBuildBroadcast()
	result := message.BroadcastResultAlterCollectionMessageV2{
		Message: message.MustAsBroadcastAlterCollectionMessageV2(raw),
		Results: map[string]*message.AppendResult{channel: {TimeTick: 100}},
	}
	for _, scenario := range []string{"prepare fails", "publication fails", "success", "published replay", "superseded replay"} {
		mockey.PatchConvey(scenario, t, func() {
			previous := qvresource.Resource()
			defer qvresource.Init(previous)
			qvresource.Init(&qvresource.Resources{ShardViews: &coordview.ShardViewRegistry{}})
			published := &model.Collection{CollectionID: 1, SchemaVersion: 1, UpdateTimestamp: 50}
			switch scenario {
			case "published replay":
				published.SchemaVersion, published.UpdateTimestamp = 2, 100
			case "superseded replay":
				published.SchemaVersion, published.UpdateTimestamp = 3, 120
			}
			mockey.Mock((*MetaTable).GetCollectionByIDWithMaxTs).Return(published, nil).Build()
			var order []string
			failure := merr.WrapErrServiceUnavailableMsg("injected transient failure")
			mockey.Mock((*qvresource.Resources).PrepareSchema).To(func(_ *qvresource.Resources, _ context.Context, _ int64, old, target *schemapb.CollectionSchema, _ []*messagespb.LoadFieldConfig) error {
				require.EqualValues(t, 1, old.GetVersion())
				require.EqualValues(t, 2, target.GetVersion())
				order = append(order, "prepare")
				if scenario == "prepare fails" {
					return failure
				}
				return nil
			}).Build()
			mockey.Mock((*MetaTable).AlterCollection).To(func(*MetaTable, context.Context, message.BroadcastResultAlterCollectionMessageV2) error {
				order = append(order, "publish")
				if scenario == "publication fails" {
					return failure
				}
				return nil
			}).Build()
			mockey.Mock((*coordview.ShardViewRegistry).PublishCollectionSchema).To(func(_ *coordview.ShardViewRegistry, id int64, version int32) {
				require.EqualValues(t, 1, id)
				if scenario == "superseded replay" {
					require.EqualValues(t, 3, version)
				} else {
					require.EqualValues(t, 2, version)
				}
				order = append(order, "retire")
			}).Build()
			callback := &DDLCallback{Core: newTestCore(withMeta(&MetaTable{}))}
			err := callback.prepareAndPublishCollectionSchema(t.Context(), result)
			switch scenario {
			case "prepare fails":
				require.ErrorIs(t, err, failure)
				require.Equal(t, []string{"prepare"}, order)
			case "publication fails":
				require.ErrorIs(t, err, failure)
				require.Equal(t, []string{"prepare", "publish"}, order)
			case "success":
				require.NoError(t, err)
				require.Equal(t, []string{"prepare", "publish", "retire"}, order)
			default:
				require.NoError(t, err)
				require.Equal(t, []string{"retire"}, order)
			}
		})
	}
}

func TestSchemaPublicationReplayAfterCollectionDrop(t *testing.T) {
	mockey.PatchConvey("dropped collection does not re-enter handoff or side effects", t, func() {
		mockey.Mock((*MetaTable).GetCollectionByIDWithMaxTs).Return(nil, merr.WrapErrCollectionNotFound(1)).Build()
		raw := message.NewAlterCollectionMessageBuilderV2().WithHeader(&messagespb.AlterCollectionMessageHeader{
			CollectionId: 1, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{message.FieldMaskCollectionSchema}},
		}).WithBody(&messagespb.AlterCollectionMessageBody{Updates: &messagespb.AlterCollectionMessageUpdates{
			Schema: &schemapb.CollectionSchema{Version: 2},
		}}).WithBroadcast([]string{funcutil.GetControlChannel("test")}).MustBuildBroadcast()
		callback := &DDLCallback{Core: newTestCore(withMeta(&MetaTable{}))}
		result := message.BroadcastResultAlterCollectionMessageV2{Message: message.MustAsBroadcastAlterCollectionMessageV2(raw)}
		require.ErrorIs(t, callback.prepareAndPublishCollectionSchema(t.Context(), result), errAlterCollectionNotFound)
		// Broker and QueryView resources are deliberately absent: none should be used.
		require.NoError(t, callback.alterCollectionV2AckCallback(t.Context(), result))
	})
}
