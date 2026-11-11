package datacoord

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/ddl"
	"github.com/milvus-io/milvus/internal/datacoord/externalrefresh"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/dataview"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/internal/views/coord/resource"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type refreshTestSchemaManager struct{ ExternalRefreshSchemaManager }

func (*refreshTestSchemaManager) PrepareExternalRefreshSchema(context.Context, int64, string, string) (*schemapb.CollectionSchema, error) {
	panic("unpatched")
}

func (*refreshTestSchemaManager) PublishExternalRefreshSchema(context.Context, message.BroadcastResultRefreshExternalCollectionMessageV2) error {
	panic("unpatched")
}

type refreshTestAllocator struct{ allocator.Allocator }

func (*refreshTestAllocator) AllocID(context.Context) (int64, error) { return 7, nil }
func TestRefreshPreflightBeforeBroadcast(t *testing.T) {
	for _, failure := range []string{"schema", "explore", "broadcast", "accepted", "waiting"} {
		t.Run(failure, func(t *testing.T) {
			s := &Server{externalCollectionRefreshManager: &externalrefresh.Manager{}, allocator: &refreshTestAllocator{}, handler: &ServerHandler{}}
			s.SetExternalRefreshSchemaManager(&refreshTestSchemaManager{})
			s.stateCode.Store(commonpb.StateCode_Healthy)
			s.ddl = newDDLCallbacks(s)
			patches := []*mockey.Mocker{
				mockey.Mock((*ddl.Callbacks).StartBroadcastWithCollectionID).Return(&embeddedBroadcastAPI{}, nil).Build(),
				mockey.Mock((*embeddedBroadcastAPI).Close).Return().Build(),
				mockey.Mock((*externalrefresh.Manager).GetActiveJobByCollectionID).Return(nil).Build(),
				mockey.Mock((*ServerHandler).GetCollection).Return(&meta.CollectionInfo{ID: 1, VChannelNames: []string{"v1", "v2"}}, nil).Build(),
			}
			defer func() {
				for _, p := range patches {
					p.UnPatch()
				}
			}()
			var schemaErr, planErr error
			if failure == "schema" {
				schemaErr = merr.ErrParameterInvalid
			}
			if failure == "explore" {
				planErr = merr.ErrDataIntegrity
			}
			schema := &schemapb.CollectionSchema{Version: 2, ExternalSource: "s3://new"}
			patches = append(patches, mockey.Mock((*refreshTestSchemaManager).PrepareExternalRefreshSchema).Return(schema, schemaErr).Build())
			prepared := false
			patches = append(patches, mockey.Mock((*externalrefresh.Manager).PrepareRefresh).To(func(context.Context, int64, int64, string, *schemapb.CollectionSchema) ([]*datapb.ExternalCollectionRefreshTask, error) {
				prepared = true
				return []*datapb.ExternalCollectionRefreshTask{{TaskId: 8}}, planErr
			}).Build())
			broadcasts := 0
			patches = append(patches, mockey.Mock((*embeddedBroadcastAPI).Broadcast).To(func(_ context.Context, msg message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error) {
				broadcasts++
				require.True(t, prepared)
				body := message.MustAsBroadcastRefreshExternalCollectionMessageV2(msg).MustBody()
				require.True(t, proto.Equal(schema, body.GetSchema()))
				require.Len(t, body.GetTasks(), 1)
				if failure == "broadcast" {
					return nil, merr.ErrServiceNotReady
				}
				return &types.BroadcastAppendResult{}, nil
			}).Build())
			var jobErr error
			if failure == "waiting" {
				jobErr = merr.ErrServiceNotReady
			}
			patches = append(patches, mockey.Mock((*externalrefresh.Manager).GetJobProgress).Return(&datapb.ExternalCollectionRefreshJob{JobId: 7}, jobErr).Build())
			ctx := t.Context()
			if failure == "waiting" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			resp, err := s.RefreshExternalCollection(ctx, &datapb.RefreshExternalCollectionRequest{CollectionId: 1})
			require.NoError(t, err)
			if failure == "accepted" {
				require.NoError(t, merr.Error(resp.GetStatus()))
				require.EqualValues(t, 7, resp.GetJobId())
			} else {
				require.Error(t, merr.Error(resp.GetStatus()))
			}
			if failure == "schema" || failure == "explore" {
				require.Zero(t, broadcasts)
			} else {
				require.Equal(t, 1, broadcasts)
			}
		})
	}
}

type refreshTestViews struct{ dataview.Manager }

func (*refreshTestViews) PublishRefresh(context.Context, int64, int32, dataview.Projector) (*viewpb.DataVersion, error) {
	panic("unpatched")
}

func TestRefreshPublishesDataBeforeCoordAndWaits(t *testing.T) {
	for _, failure := range []string{"data", "schema", "ready", "none"} {
		t.Run(failure, func(t *testing.T) {
			s := &Server{dataViewManager: &refreshTestViews{}, meta: &meta.Meta{}, externalRefreshSchemaManager: &refreshTestSchemaManager{}}
			var order []string
			p1 := mockey.Mock((*refreshTestViews).PublishRefresh).To(func(context.Context, int64, int32, dataview.Projector) (*viewpb.DataVersion, error) {
				order = append(order, "data")
				if failure == "data" {
					return nil, merr.ErrServiceNotReady
				}
				return &viewpb.DataVersion{}, nil
			}).Build()
			defer p1.UnPatch()
			p2 := mockey.Mock((*refreshTestSchemaManager).PublishExternalRefreshSchema).To(func(context.Context, message.BroadcastResultRefreshExternalCollectionMessageV2) error {
				order = append(order, "schema")
				if failure == "schema" {
					return merr.ErrServiceNotReady
				}
				return nil
			}).Build()
			defer p2.UnPatch()
			p3 := mockey.Mock((*resource.Resources).Refresh).To(func(context.Context, int64) error {
				order = append(order, "ready")
				if failure == "ready" {
					return merr.ErrServiceNotReady
				}
				return nil
			}).Build()
			defer p3.UnPatch()
			msg := message.NewRefreshExternalCollectionMessageBuilderV2().WithHeader(&message.RefreshExternalCollectionMessageHeader{CollectionId: 1}).WithBody(&message.RefreshExternalCollectionMessageBody{Schema: &schemapb.CollectionSchema{Version: 2}}).WithBroadcast([]string{"v1"}).MustBuildBroadcast()
			err := s.publishExternalRefresh(t.Context(), message.BroadcastResultRefreshExternalCollectionMessageV2{Message: message.MustAsBroadcastRefreshExternalCollectionMessageV2(msg)})
			if failure == "none" {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, merr.ErrServiceNotReady)
			}
			switch failure {
			case "data":
				require.Equal(t, []string{"data"}, order)
			case "schema":
				require.Equal(t, []string{"data", "schema"}, order)
			default:
				require.Equal(t, []string{"data", "schema", "ready"}, order)
			}
		})
	}
}

func TestRefreshOwnsDataViewUntilCallbackCompletes(t *testing.T) {
	var target *broadcaster.SchemaTarget
	var targetErr error
	p := mockey.Mock(broadcast.GetPendingSchemaTarget).To(func(int64) (*broadcaster.SchemaTarget, error) { return target, targetErr }).Build()
	defer p.UnPatch()
	blocked, err := refreshOwnsDataView(t.Context(), 1)
	require.NoError(t, err)
	require.False(t, blocked)
	target = &broadcaster.SchemaTarget{RefreshJobID: 2}
	blocked, err = refreshOwnsDataView(t.Context(), 1)
	require.NoError(t, err)
	require.True(t, blocked)
	targetErr = merr.ErrServiceNotReady
	blocked, err = refreshOwnsDataView(t.Context(), 1)
	require.NoError(t, err)
	require.True(t, blocked, "recovery must not expose prepared data before broadcaster recovery")
}
