package adaptor_test

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/service"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/lock"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/redo"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/replicate"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/registry"
	"github.com/milvus-io/milvus/internal/streamingnode/server/walmanager"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/interceptor"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type schemaRPCManager struct{ walmanager.Manager }

func (*schemaRPCManager) GetAvailableWAL(types.PChannelInfo) (wal.WAL, error) {
	panic("patch with mockey")
}

// The service and schema providers are real: CREATE/ALTER go through the WAL
// interceptors and recovery observer, and reads cross the native gRPC transport.
func TestCollectionSchemaRPCWithWAL(t *testing.T) {
	walimplstest.Reset()
	initResourceForTest(t)
	message.RegisterDefaultWALName(message.WALNameTest)
	opener, err := registry.MustGetBuilder(message.WALNameTest,
		redo.NewInterceptorBuilder(), lock.NewInterceptorBuilder(),
		replicate.NewInterceptorBuilder(), timetick.NewInterceptorBuilder(), shard.NewInterceptorBuilder(),
	).Build()
	require.NoError(t, err)
	defer opener.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	channel := types.PChannelInfo{Name: "schema-rpc", Term: 1, AccessMode: types.AccessModeRW}
	w, err := opener.Open(ctx, &wal.OpenOption{Channel: channel, DisableFlusher: true})
	require.NoError(t, err)
	defer w.Close()
	manager := mockey.Mock((*schemaRPCManager).GetAvailableWAL).To(func(_ *schemaRPCManager, requested types.PChannelInfo) (wal.WAL, error) {
		if requested.Term != channel.Term {
			return nil, status.NewUnmatchedChannelTerm(channel.Name, requested.Term, channel.Term)
		}
		return w, nil
	}).Build()
	defer manager.UnPatch()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer(grpc.UnaryInterceptor(interceptor.NewServiceUnaryServerInterceptor()))
	streamingpb.RegisterStreamingNodeHandlerServiceServer(server, service.NewHandlerService(&schemaRPCManager{}))
	go server.Serve(listener)
	defer server.Stop()
	conn, err := grpc.NewClient("passthrough:///schema-wal", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(interceptor.NewServiceUnaryClientInterceptor()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	require.NoError(t, err)
	defer conn.Close()
	client := streamingpb.NewStreamingNodeHandlerServiceClient(conn)
	const vchannel = "schema-rpc_1v0"
	schema := &schemapb.CollectionSchema{Name: "schema_rpc", Version: 0, Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, Name: "vector", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "2"}}},
	}}
	_, err = w.Append(ctx, message.NewCreateCollectionMessageBuilderV1().WithVChannel(vchannel).
		WithHeader(&message.CreateCollectionMessageHeader{CollectionId: 1, PartitionIds: []int64{2}}).
		WithBody(&msgpb.CreateCollectionRequest{CollectionID: 1, CollectionSchema: schema}).MustBuildMutable())
	require.NoError(t, err)
	req := &streamingpb.StreamingNodeGetCollectionSchemaRequest{
		Vchannel: vchannel,
		Pchannel: types.NewProtoFromPChannelInfo(channel),
		Selector: &streamingpb.StreamingNodeGetCollectionSchemaRequest_CurrentWrite{CurrentWrite: true},
	}
	current, err := client.GetCollectionSchema(ctx, req)
	require.NoError(t, err)
	require.Zero(t, current.Schema.Version)
	req.Selector = &streamingpb.StreamingNodeGetCollectionSchemaRequest_SchemaVersion{SchemaVersion: 0}
	exact, err := client.GetCollectionSchema(ctx, req)
	require.NoError(t, err, "the just-created version is immediately readable from write admission")
	require.Zero(t, exact.Schema.Version)
	// Historical lookup requires a live dependency, not unlimited retention.
	// Hold the real growing registration RPC while the WAL observer installs
	// the v0 SegmentView; its schema reference and unfinished handle pin v0.
	coord := resource.Resource().MixCoordClient().Get().(*mocks.MockMixCoordClient)
	coord.EXPECT().AllocSegment(mock.Anything, mock.Anything).Unset()
	registrationEntered, registrationRelease := make(chan struct{}, 1), make(chan struct{})
	releaseRegistration := sync.OnceFunc(func() { close(registrationRelease) })
	defer releaseRegistration() // release the real task before w.Close waits
	coord.EXPECT().AllocSegment(mock.Anything, mock.Anything).RunAndReturn(func(callCtx context.Context, _ *datapb.AllocSegmentRequest, _ ...grpc.CallOption) (*datapb.AllocSegmentResponse, error) {
		select {
		case registrationEntered <- struct{}{}:
		default:
		}
		select {
		case <-registrationRelease:
			return &datapb.AllocSegmentResponse{Status: merr.Success()}, nil
		case <-callCtx.Done():
			return nil, callCtx.Err()
		}
	}).Maybe()
	// A real insert drives the allocator-owned CreateSegment protocol.
	_, err = w.Append(ctx, message.NewInsertMessageBuilderV1().WithVChannel(vchannel).
		WithHeader(&message.InsertMessageHeader{CollectionId: 1, Partitions: []*message.PartitionSegmentAssignment{{PartitionId: 2, Rows: 1}}}).
		WithBody(&msgpb.InsertRequest{
			CollectionID: 1, NumRows: 1, Version: msgpb.InsertDataVersion_ColumnBased,
			RowIDs: []int64{10}, Timestamps: []uint64{1}, FieldsData: []*schemapb.FieldData{
				{FieldId: 100, FieldName: "id", Type: schemapb.DataType_Int64, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1}}}}}},
				{FieldId: 101, FieldName: "vector", Type: schemapb.DataType_FloatVector, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: 2, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2}}}}}},
			},
		}).MustBuildMutable())
	require.NoError(t, err)
	select {
	case <-registrationEntered:
	case <-ctx.Done():
		t.Fatal("growing schema dependency was not observed")
	}
	updated := proto.Clone(schema).(*schemapb.CollectionSchema)
	updated.Version = 1
	_, err = w.Append(ctx, message.NewAlterCollectionMessageBuilderV2().WithVChannel(vchannel).
		WithHeader(&message.AlterCollectionMessageHeader{CollectionId: 1, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{message.FieldMaskCollectionSchema}}}).
		WithBody(&message.AlterCollectionMessageBody{Updates: &message.AlterCollectionMessageUpdates{Schema: updated}}).MustBuildMutable())
	require.NoError(t, err)
	req.Selector = &streamingpb.StreamingNodeGetCollectionSchemaRequest_CurrentWrite{CurrentWrite: true}
	current, err = client.GetCollectionSchema(ctx, req)
	require.NoError(t, err)
	require.EqualValues(t, 1, current.Schema.Version)
	req.Selector = &streamingpb.StreamingNodeGetCollectionSchemaRequest_SchemaVersion{SchemaVersion: 1}
	exact, err = client.GetCollectionSchema(ctx, req)
	require.NoError(t, err, "the newly appended version is immediately readable from write admission")
	require.EqualValues(t, 1, exact.Schema.Version)
	req.Selector = &streamingpb.StreamingNodeGetCollectionSchemaRequest_SchemaVersion{SchemaVersion: 0}
	require.Eventually(t, func() bool {
		exact, err := client.GetCollectionSchema(ctx, req)
		return err == nil && exact.Schema.Version == 0
	}, 5*time.Second, time.Millisecond)
	req.Pchannel.Term++
	_, err = client.GetCollectionSchema(ctx, req)
	require.True(t, status.AsStreamingError(err).IsWrongStreamingNode())
}
