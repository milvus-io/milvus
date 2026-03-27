package vtproto_bench

import (
	"context"
	"crypto/rand"
	"fmt"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
)

// ---------------------------------------------------------------------------
// Codecs
// ---------------------------------------------------------------------------

// vtProtoMarshaler / vtProtoUnmarshaler — same interfaces as the production codec.
type vtProtoMarshaler interface{ MarshalVT() ([]byte, error) }
type vtProtoUnmarshaler interface{ UnmarshalVT([]byte) error }

// vtCodec uses vtprotobuf generated MarshalVT / UnmarshalVT.
type vtCodec struct{}

func (vtCodec) Name() string { return "vtproto-bench" }
func (vtCodec) Marshal(v interface{}) ([]byte, error) {
	return v.(vtProtoMarshaler).MarshalVT()
}
func (vtCodec) Unmarshal(data []byte, v interface{}) error {
	return v.(vtProtoUnmarshaler).UnmarshalVT(data)
}

// stdCodec uses google.golang.org/protobuf/proto Marshal / Unmarshal.
type stdCodec struct{}

func (stdCodec) Name() string { return "std-proto-bench" }
func (stdCodec) Marshal(v interface{}) ([]byte, error) {
	return proto.Marshal(v.(proto.Message))
}
func (stdCodec) Unmarshal(data []byte, v interface{}) error {
	return proto.Unmarshal(data, v.(proto.Message))
}

func init() {
	encoding.RegisterCodec(vtCodec{})
	encoding.RegisterCodec(stdCodec{})
}

// ---------------------------------------------------------------------------
// Generic echo service (no proto service definition needed)
// ---------------------------------------------------------------------------

const echoMethod = "/bench.Echo/Do"

// echoServiceDesc describes a trivial unary echo RPC.
var echoServiceDesc = grpc.ServiceDesc{
	ServiceName: "bench.Echo",
	HandlerType: (*interface{})(nil),
	Methods: []grpc.MethodDesc{{
		MethodName: "Do",
	}},
}

// newEchoServer starts a bufconn gRPC server that echoes the raw bytes back.
// The handler deserialises into `newMsg` (to simulate real work) then re-serialises.
func newEchoServer(lis *bufconn.Listener, codecName string, newMsg func() interface{}) *grpc.Server {
	var opts []grpc.ServerOption
	if codecName != "" {
		opts = append(opts, grpc.ForceServerCodec(encoding.GetCodec(codecName)))
	}
	srv := grpc.NewServer(opts...)

	// Register the handler with a copy of the service desc that carries it.
	desc := echoServiceDesc
	desc.Methods[0].Handler = func(
		srvIface interface{},
		ctx context.Context,
		dec func(interface{}) error,
		interceptor grpc.UnaryServerInterceptor,
	) (interface{}, error) {
		msg := newMsg()
		if err := dec(msg); err != nil {
			return nil, err
		}
		return msg, nil // echo back
	}
	srv.RegisterService(&desc, struct{}{})

	go srv.Serve(lis)
	return srv
}

// dialBufconn creates a client connection over the in-memory listener.
func dialBufconn(lis *bufconn.Listener, codecName string) *grpc.ClientConn {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if codecName != "" {
		opts = append(opts, grpc.WithDefaultCallOptions(grpc.ForceCodec(encoding.GetCodec(codecName))))
	}
	opts = append(opts, grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	}))
	cc, err := grpc.NewClient("passthrough:///bufconn", opts...)
	if err != nil {
		panic(err)
	}
	return cc
}

// ---------------------------------------------------------------------------
// Message constructors
// ---------------------------------------------------------------------------

func makeSearchResults(blobSize int) *internalpb.SearchResults {
	blob := make([]byte, blobSize)
	rand.Read(blob)
	return &internalpb.SearchResults{
		NumQueries:               10,
		TopK:                     100,
		MetricType:               "L2",
		SlicedBlob:               blob,
		SealedSegmentIDsSearched: []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		ChannelIDsSearched:       []string{"ch-0", "ch-1", "ch-2", "ch-3"},
	}
}

func makePlanNode(numPKs int) *planpb.PlanNode {
	values := make([]*planpb.GenericValue, numPKs)
	for i := range values {
		values[i] = &planpb.GenericValue{
			Val: &planpb.GenericValue_Int64Val{Int64Val: int64(i)},
		}
	}
	return &planpb.PlanNode{
		Node: &planpb.PlanNode_VectorAnns{
			VectorAnns: &planpb.VectorANNS{
				FieldId: 100,
				Predicates: &planpb.Expr{
					Expr: &planpb.Expr_TermExpr{
						TermExpr: &planpb.TermExpr{
							ColumnInfo: &planpb.ColumnInfo{
								FieldId:  0,
								DataType: 5,
							},
							Values: values,
						},
					},
				},
				QueryInfo: &planpb.QueryInfo{
					Topk:         100,
					MetricType:   "L2",
					SearchParams: `{"nprobe": 16}`,
				},
			},
		},
		OutputFieldIds: []int64{0, 1, 2, 3},
	}
}

func makeSearchResultData(numResults int) *schemapb.SearchResultData {
	scores := make([]float32, numResults)
	ids := make([]int64, numResults)
	for i := range scores {
		scores[i] = float32(i) * 0.1
		ids[i] = int64(i)
	}
	return &schemapb.SearchResultData{
		NumQueries: 10,
		TopK:       int64(numResults / 10),
		Scores:     scores,
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: ids},
			},
		},
		Topks: []int64{int64(numResults / 10)},
	}
}

// ---------------------------------------------------------------------------
// gRPC round-trip benchmarks
// ---------------------------------------------------------------------------

// benchGRPCRoundTrip is the core loop: sends `req` to the echo server and
// receives it back.  Both marshal+unmarshal happen on client AND server side,
// so a single iteration = 2×marshal + 2×unmarshal + in-memory transport.
func benchGRPCRoundTrip(b *testing.B, codecName string, req interface{}, newResp func() interface{}) {
	const bufSize = 16 * 1024 * 1024
	lis := bufconn.Listen(bufSize)

	srv := newEchoServer(lis, codecName, newResp)
	defer srv.Stop()

	cc := dialBufconn(lis, codecName)
	defer cc.Close()

	ctx := context.Background()

	// warm up
	resp := newResp()
	if err := cc.Invoke(ctx, echoMethod, req, resp); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		out := newResp()
		if err := cc.Invoke(ctx, echoMethod, req, out); err != nil {
			b.Fatal(err)
		}
	}
}

// --- SearchResults (internal type) ---

func BenchmarkGRPC_SearchResults_1KB_StdProto(b *testing.B) {
	msg := makeSearchResults(1024)
	benchGRPCRoundTrip(b, "std-proto-bench", msg, func() interface{} { return &internalpb.SearchResults{} })
}

func BenchmarkGRPC_SearchResults_1KB_VTProto(b *testing.B) {
	msg := makeSearchResults(1024)
	benchGRPCRoundTrip(b, "vtproto-bench", msg, func() interface{} { return &internalpb.SearchResults{} })
}

func BenchmarkGRPC_SearchResults_1MB_StdProto(b *testing.B) {
	msg := makeSearchResults(1024 * 1024)
	benchGRPCRoundTrip(b, "std-proto-bench", msg, func() interface{} { return &internalpb.SearchResults{} })
}

func BenchmarkGRPC_SearchResults_1MB_VTProto(b *testing.B) {
	msg := makeSearchResults(1024 * 1024)
	benchGRPCRoundTrip(b, "vtproto-bench", msg, func() interface{} { return &internalpb.SearchResults{} })
}

// --- PlanNode (2000 PKs TermExpr, common in filtered search) ---

func BenchmarkGRPC_PlanNode_2000PK_StdProto(b *testing.B) {
	msg := makePlanNode(2000)
	benchGRPCRoundTrip(b, "std-proto-bench", msg, func() interface{} { return &planpb.PlanNode{} })
}

func BenchmarkGRPC_PlanNode_2000PK_VTProto(b *testing.B) {
	msg := makePlanNode(2000)
	benchGRPCRoundTrip(b, "vtproto-bench", msg, func() interface{} { return &planpb.PlanNode{} })
}

// --- SearchResultData (external API type, 10K results) ---

func BenchmarkGRPC_SearchResultData_10K_StdProto(b *testing.B) {
	msg := makeSearchResultData(10000)
	benchGRPCRoundTrip(b, "std-proto-bench", msg, func() interface{} { return &schemapb.SearchResultData{} })
}

func BenchmarkGRPC_SearchResultData_10K_VTProto(b *testing.B) {
	msg := makeSearchResultData(10000)
	benchGRPCRoundTrip(b, "vtproto-bench", msg, func() interface{} { return &schemapb.SearchResultData{} })
}

// ---------------------------------------------------------------------------
// Pure marshal/unmarshal benchmarks (for comparison / isolating codec cost)
// ---------------------------------------------------------------------------

func BenchmarkMarshal_SearchResults_1MB_StdProto(b *testing.B) {
	msg := makeSearchResults(1024 * 1024)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = proto.Marshal(msg)
	}
}

func BenchmarkMarshal_SearchResults_1MB_VTProto(b *testing.B) {
	msg := makeSearchResults(1024 * 1024)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = msg.MarshalVT()
	}
}

func BenchmarkUnmarshal_SearchResults_1MB_StdProto(b *testing.B) {
	msg := makeSearchResults(1024 * 1024)
	data, _ := proto.Marshal(msg)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		out := &internalpb.SearchResults{}
		_ = proto.Unmarshal(data, out)
	}
}

func BenchmarkUnmarshal_SearchResults_1MB_VTProto(b *testing.B) {
	msg := makeSearchResults(1024 * 1024)
	data, _ := proto.Marshal(msg)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		out := &internalpb.SearchResults{}
		_ = out.UnmarshalVT(data)
	}
}

// ---------------------------------------------------------------------------
// Summary printer (run with: go test -bench=. -benchmem -count=1 -v)
// ---------------------------------------------------------------------------

func TestPrintBenchmarkGuide(t *testing.T) {
	fmt.Println(`
╔══════════════════════════════════════════════════════════════════╗
║  VTProto vs Standard Proto — gRPC Round-Trip Benchmark         ║
╠══════════════════════════════════════════════════════════════════╣
║                                                                  ║
║  Run:  go test -bench=. -benchmem -count=5 -timeout=10m         ║
║                                                                  ║
║  Each gRPC benchmark = full round-trip over bufconn:             ║
║    client marshal → transport → server unmarshal →               ║
║    server marshal → transport → client unmarshal                 ║
║    (2× marshal + 2× unmarshal per iteration)                     ║
║                                                                  ║
║  Compare *_StdProto vs *_VTProto pairs for the same message.    ║
╚══════════════════════════════════════════════════════════════════╝`)
}
