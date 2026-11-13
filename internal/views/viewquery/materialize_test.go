package viewquery_test

import (
	"context"
	"net"
	"sync/atomic"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	_ "google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	snexecutor "github.com/milvus-io/milvus/internal/streamingnode/server/viewquery"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/util/materialize"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/internal/views/viewquery"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func materializeRequest() *viewpb.MaterializeOnViewRequest {
	return &viewpb.MaterializeOnViewRequest{
		IpcOptions: &viewpb.MaterializeIPCOptions{FormatVersion: materialize.FormatVersion, SchemaFingerprint: 123},
		Request: &viewpb.MaterializeOnViewRequest_QueryByPk{QueryByPk: &viewpb.QueryOnViewRequest{
			ShardId:          &viewpb.ShardID{ReplicaId: 7, Vchannel: "v1"},
			Version:          &viewpb.QueryViewVersion{DataVersion: &viewpb.DataVersion{StreamingVersion: 3}, QueryVersion: 4},
			Mvcc:             &viewpb.QueryPlanMVCC{GrowingTimetick: 101, TransformingTimetick: 99},
			LegacyReq:        &internalpb.RetrieveRequest{CollectionID: 10, SchemaVersion: 2, Limit: typeutil.Unlimited},
			SegmentSelection: &viewpb.SegmentSelection{Mode: viewpb.SegmentSelection_Include, SegmentIds: []int64{42}},
		}},
	}
}

func TestMaterializeTaskOptionsPreserveQueryOwnership(t *testing.T) {
	owner := snview.NewSNQuerySegmentTasks(nil)
	options := &viewpb.MaterializeIPCOptions{SchemaFingerprint: 123, FormatVersion: materialize.FormatVersion}
	configured := &viewquery.MaterializeTasksWithOptions{QuerySegmentTasks: owner, Options: options}
	require.Same(t, options, viewquery.MaterializeOptions(configured))
	require.Nil(t, viewquery.MaterializeOptions(owner))
	require.Equal(t, owner.Tasks(), configured.Tasks())
	var released atomic.Int32
	patch := mockey.Mock((*snview.SNQuerySegmentTasks).Release).To(func(*snview.SNQuerySegmentTasks) { released.Add(1) }).Build()
	defer patch.UnPatch()
	configured.Release()
	require.EqualValues(t, 1, released.Load())
}

func TestMaterializeOnViewPinsOriginalQueryThroughExecution(t *testing.T) {
	for _, outcome := range []string{"success", "execution_error", "empty", "acquire_error"} {
		t.Run(outcome, func(t *testing.T) {
			owner := snview.NewSNQuerySegmentTasks([]snview.SNQuerySegmentTask{{}})
			if outcome == "empty" {
				owner = snview.NewSNQuerySegmentTasks(nil)
			}
			var released atomic.Int32
			release := mockey.Mock((*snview.SNQuerySegmentTasks).Release).To(func(*snview.SNQuerySegmentTasks) { released.Add(1) }).Build()
			defer release.UnPatch()
			originalRequest := materializeRequest()
			wire, err := proto.Marshal(originalRequest)
			require.NoError(t, err)
			request := &viewpb.MaterializeOnViewRequest{}
			require.NoError(t, proto.Unmarshal(wire, request))
			require.True(t, proto.Equal(originalRequest, request))
			acquire := mockey.Mock((*snview.SNQueryViewHandler).AcquireQuerySegmentTasks).To(func(_ *snview.SNQueryViewHandler, _ context.Context, shard qviews.ShardID, version qviews.QueryViewVersion, mvcc *viewpb.QueryPlanMVCC, req *internalpb.RetrieveRequest, selection *viewpb.SegmentSelection) (viewquery.QuerySegmentTasks, error) {
				require.Equal(t, qviews.FromProtoShardID(request.GetQueryByPk().ShardId), shard)
				require.Equal(t, qviews.FromProtoQueryViewVersion(request.GetQueryByPk().Version), version)
				require.True(t, proto.Equal(request.GetQueryByPk().Mvcc, mvcc))
				require.Same(t, request.GetQueryByPk().LegacyReq, req)
				require.True(t, proto.Equal(request.GetQueryByPk().SegmentSelection, selection))
				if outcome == "acquire_error" {
					return nil, viewerror.NewViewInvalidated("retired")
				}
				return owner, nil
			}).Build()
			defer acquire.UnPatch()
			execute := mockey.Mock((*snexecutor.Scheduler).Materialize).To(func(_ *snexecutor.Scheduler, _ context.Context, tasks viewquery.QuerySegmentTasks) (*internalpb.MaterializeResults, error) {
				configured := tasks.(*viewquery.MaterializeTasksWithOptions)
				require.Same(t, owner, configured.QuerySegmentTasks)
				require.Same(t, request.GetIpcOptions(), configured.Options)
				require.EqualValues(t, 123, configured.Options.GetSchemaFingerprint())
				require.Equal(t, materialize.FormatVersion, configured.Options.GetFormatVersion())
				require.Zero(t, released.Load(), "handles must survive encoding")
				if outcome == "execution_error" {
					return nil, context.Canceled
				}
				return &internalpb.MaterializeResults{Header: &internalpb.MaterializeHeader{Status: merr.Success()}}, nil
			}).Build()
			defer execute.UnPatch()
			server := viewquery.NewServer(&snview.SNQueryViewHandler{}, snexecutor.NewScheduler(nil))
			result, err := server.MaterializeOnView(t.Context(), request)
			switch outcome {
			case "acquire_error":
				require.True(t, viewerror.AsViewError(err).IsRetryable())
				require.Zero(t, released.Load())
			case "execution_error":
				require.ErrorIs(t, err, context.Canceled)
				require.EqualValues(t, 1, released.Load())
			default:
				require.NoError(t, err)
				require.Equal(t, materialize.FormatVersion, result.Header.FormatVersion)
				require.EqualValues(t, 1, released.Load())
			}
			if outcome == "empty" || outcome == "acquire_error" {
				require.Zero(t, execute.Times())
			}
		})
	}
}

func TestMaterializeOnViewRejectsUnsupportedBeforeAcquiring(t *testing.T) {
	for _, shape := range []string{"version", "limit", "iterator", "count", "missing_query", "missing_mvcc", "missing_ipc_options"} {
		t.Run(shape, func(t *testing.T) {
			req := materializeRequest()
			switch shape {
			case "version":
				req.IpcOptions.FormatVersion++
			case "limit":
				req.GetQueryByPk().LegacyReq.Limit = 1
			case "iterator":
				req.GetQueryByPk().LegacyReq.IsIterator = true
			case "count":
				req.GetQueryByPk().LegacyReq.IsCount = true
			case "missing_query":
				req.Request = nil
			case "missing_mvcc":
				req.GetQueryByPk().Mvcc = nil
			case "missing_ipc_options":
				req.IpcOptions = nil
			}
			_, err := viewquery.NewServer(nil, nil).MaterializeOnView(t.Context(), req)
			require.Error(t, err)
		})
	}
}

func TestMaterializeOnViewRejectsUnknownRequestImplementation(t *testing.T) {
	// Simulate a peer sending a future oneof member unknown to this binary.
	wire := protowire.AppendBytes(protowire.AppendTag(nil, 100, protowire.BytesType), []byte("native-request"))
	req := &viewpb.MaterializeOnViewRequest{}
	require.NoError(t, proto.Unmarshal(wire, req))
	require.Nil(t, req.GetRequest())
	require.NotEmpty(t, req.ProtoReflect().GetUnknown())
	_, err := viewquery.NewServer(nil, nil).MaterializeOnView(t.Context(), req)
	require.ErrorIs(t, err, merr.ErrServiceUnimplemented)
}

func TestMaterializeOnViewAcceptsCompressedRPC(t *testing.T) {
	owner := snview.NewSNQuerySegmentTasks(nil)
	acquire := mockey.Mock((*snview.SNQueryViewHandler).AcquireQuerySegmentTasks).Return(owner, nil).Build()
	defer acquire.UnPatch()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	viewpb.RegisterViewQueryServiceServer(server, viewquery.NewServer(&snview.SNQueryViewHandler{}, snexecutor.NewScheduler(nil)))
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	defer func() {
		server.Stop()
		<-served
	}()
	conn, err := grpc.NewClient("passthrough:///materialize", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	require.NoError(t, err)
	defer conn.Close()
	reply, err := viewpb.NewViewQueryServiceClient(conn).MaterializeOnView(t.Context(), materializeRequest(), grpc.UseCompressor("gzip"))
	require.NoError(t, err)
	require.Equal(t, materialize.FormatVersion, reply.Header.FormatVersion)
	require.Empty(t, reply.Body)
}
