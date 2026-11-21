package rootcoord

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	dcmeta "github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/dataview"
	dckv "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	qvkv "github.com/milvus-io/milvus/internal/metastore/kv/queryview"
	rckv "github.com/milvus-io/milvus/internal/metastore/kv/rootcoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/coord/readiness"
	qvresource "github.com/milvus-io/milvus/internal/views/coord/resource"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
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
			mockey.Mock((*qvresource.Resources).PrepareSchema).To(func(_ *qvresource.Resources, _ context.Context, _ int64, old, target *schemapb.CollectionSchema) error {
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

func TestUnloadedAlterPublishesOnlyAfterDataViewCertification(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	store := dcmeta.NewMetaMemoryKV()
	created := collectionCreationWithSchemaVersion(1)
	collection := newCollectionModelWithMessage(created.Message.Header(), created.Message.MustBody(), created)
	id := collection.CollectionID
	mt := &MetaTable{
		ctx: ctx, catalog: rckv.NewCatalog(store), names: newNameDb(), aliases: newNameDb(),
		dbName2Meta: map[string]*model.Database{"default": {ID: 1, Name: "default"}},
		collID2Meta: map[int64]*model.Collection{}, partitionName2ID: map[int64]map[string]int64{},
	}
	require.NoError(t, mt.catalog.CreateCollection(ctx, collection, collection.CreateTime))
	mt.setCollectionLocked(collection)
	ownerStarted := make(chan struct{})
	ownerAvailable := make(chan struct{})
	manager, err := dataview.RecoverManager(ctx, dckv.NewCatalog(store, "", ""),
		func(context.Context, int64) (bool, error) { return true, nil }, nil, nil, nil,
		dataview.WithReadAdmissionSampler(func(ctx context.Context, _ string) (*viewpb.QueryPlanMVCC, error) {
			close(ownerStarted)
			select {
			case <-ownerAvailable:
				return &viewpb.QueryPlanMVCC{GrowingTimetick: 120, TransformingTimetick: 140}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}))
	require.NoError(t, err)
	_, err = manager.OnCreateCollection(ctx, dataview.CreateCollectionDataViewEvent{CollectionID: id, SchemaVersion: 1, VChannels: []string{"v1"}})
	require.NoError(t, err)
	configs, err := loadmgr.RecoverLoadConfigStore(ctx, loadmgr.LoadConfigStoreDependencies{Catalog: qvkv.NewCollectionLoadInfoCatalog(store)})
	require.NoError(t, err)
	previous := qvresource.Resource()
	notifications := readiness.NewNotifications()
	defer notifications.Close()
	defer qvresource.Init(previous)
	qvresource.Init(&qvresource.Resources{Reconciler: &testSchemaDataViews{}, LoadConfigs: configs, ShardViews: &coordview.ShardViewRegistry{}, Readiness: notifications})
	// Supply a completed materialization, exercising the real callback, unloaded
	// preparation, DataView manager, and RootCoord public metadata catalog.
	preparation := mockey.Mock((*testSchemaDataViews).PrepareDataViewSchema).To(func(_ *testSchemaDataViews, ctx context.Context, id int64, old, target *schemapb.CollectionSchema) error {
		_, err := manager.AdvanceSchema(ctx, id, target.GetVersion(), func(context.Context, int64, *viewpb.DataViewOfCollection) ([]dataview.LoadableSegment, error) {
			return []dataview.LoadableSegment{{SegmentID: 10, PartitionID: 1, VChannel: "v1", SchemaVersion: 2}}, nil
		})
		return err
	}).Build()
	defer preparation.UnPatch()
	target := collection.ToCollectionSchemaPB()
	target.Version = 2
	target.Functions = []*schemapb.FunctionSchema{{Name: "derived", OutputFieldIds: []int64{102}}}
	require.True(t, qvresource.SchemaRequiresMaterialization(collection.ToCollectionSchemaPB(), target))
	alter := buildAlterCollectionSchemaResult(id, target, 200)
	callback := &DDLCallback{Core: newTestCore(withMeta(mt))}
	first, cancelFirst := context.WithCancel(ctx)
	defer cancelFirst()
	done := make(chan error, 1)
	go func() { done <- callback.prepareAndPublishCollectionSchema(first, alter) }()
	select {
	case <-ownerStarted:
	case <-ctx.Done():
		t.Fatal("materializing ALTER did not reach post-commit certification")
	}
	cancelFirst()
	require.ErrorIs(t, <-done, context.Canceled)
	checkPublic := func(version int32) {
		coll, err := mt.GetCollectionByIDWithMaxTs(ctx, id)
		require.NoError(t, err)
		require.Equal(t, version, coll.SchemaVersion)
		require.Equal(t, version, manager.Published(id).SchemaVersion)
	}
	checkPublic(1)
	physical, err := manager.Latest(ctx, id)
	require.NoError(t, err)
	require.EqualValues(t, 2, physical.DataView().GetSchemaVersion())
	physical.Deref()
	retry, cancelRetry := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancelRetry()
	require.ErrorIs(t, callback.prepareAndPublishCollectionSchema(retry, alter), context.DeadlineExceeded)
	checkPublic(1)
	close(ownerAvailable)
	require.NoError(t, callback.prepareAndPublishCollectionSchema(ctx, alter))
	checkPublic(2)
	require.False(t, configs.Contains(id))
}
