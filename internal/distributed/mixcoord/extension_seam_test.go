package grpcmixcoord

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/extension"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/netutil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// engineTestCoord is a coordinator that answers only what the seam is supposed
// to reach. It embeds mockMix, whose own embedded types.MixCoordComponent is
// nil, so any coordinator method the seam touches other than the ones defined
// here panics instead of quietly returning a zero value.
type engineTestCoord struct {
	loadPercentageOnlyCoord

	seenInvalidatedCollectionID int64
	seenDescribeReq             *milvuspb.DescribeCollectionRequest
}

func (c *engineTestCoord) InvalidateShardLeaderCache(_ context.Context, collectionID int64) error {
	c.seenInvalidatedCollectionID = collectionID
	return nil
}

type loadPercentageOnlyCoord struct {
	mockMix

	pct    int32
	pctErr error

	seenCollectionID int64
	seenRG           string
}

func (c *loadPercentageOnlyCoord) GetLoadPercentageByResourceGroup(_ context.Context, collectionID int64, rgName string) (int32, error) {
	c.seenCollectionID = collectionID
	c.seenRG = rgName
	return c.pct, c.pctErr
}

func (c *engineTestCoord) DescribeCollection(_ context.Context, req *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	c.seenDescribeReq = req
	return &milvuspb.DescribeCollectionResponse{CollectionID: 77}, nil
}

// recordingEngine records what each lifecycle step was handed and registers a
// gRPC service of its own, the way a real engine would.
type recordingEngine struct {
	startErr error
	stopErr  error

	seenRegistrar grpc.ServiceRegistrar
	seenCoord     extension.Coordinator
	seenExtras    extension.CoordinatorExtras
	startCount    int
	stopCount     int
}

func (e *recordingEngine) RegisterOnCoordinator(reg grpc.ServiceRegistrar) {
	e.seenRegistrar = reg
	reg.RegisterService(&grpc.ServiceDesc{
		ServiceName: "extension.test.EngineService",
		HandlerType: (*any)(nil),
	}, struct{}{})
}

func (e *recordingEngine) Start(_ context.Context, coord extension.Coordinator, extras extension.CoordinatorExtras) error {
	e.seenExtras = extras
	e.startCount++
	e.seenCoord = coord
	return e.startErr
}

func (e *recordingEngine) Stop() error {
	e.stopCount++
	return e.stopErr
}

type fakeEngineProvider struct{ engine extension.CoordinatorEngine }

func (fakeEngineProvider) Name() string                       { return "test" }
func (fakeEngineProvider) Requires() []extension.CapabilityID { return nil }
func (p fakeEngineProvider) Capabilities() extension.Capabilities {
	return extension.Capabilities{CoordinatorEngine: p.engine}
}

func installEngine(t *testing.T, e extension.CoordinatorEngine) {
	t.Helper()
	extension.ResetForTest()
	t.Cleanup(extension.ResetForTest)
	assert.NoError(t, extension.SetProvider(fakeEngineProvider{engine: e}))
}

func newTestServer(t *testing.T, coord *engineTestCoord) *Server {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	// The coordinator client is the loopback one the real server builds for
	// itself; an engine reaches the coordinator through it, so a test that
	// leaves it nil would let a regression that stops passing it go unnoticed.
	return &Server{
		ctx:            ctx,
		cancel:         cancel,
		mixCoord:       coord,
		mixCoordClient: mocks.NewMockMixCoordClient(t),
		grpcErrChan:    make(chan error),
	}
}

// TestCoordinatorEngineSeamIsInertWithoutProvider is the zero-behavior-change
// proof: with no provider installed, nothing is registered on the coordinator's
// gRPC server, no adapter is built over the coordinator, and stopping is a
// no-op.
func TestCoordinatorEngineSeamIsInertWithoutProvider(t *testing.T) {
	extension.ResetForTest()
	t.Cleanup(extension.ResetForTest)

	srv := grpc.NewServer()
	t.Cleanup(srv.Stop)
	before := len(srv.GetServiceInfo())
	registerCoordinatorEngineGRPC(srv)
	assert.Len(t, srv.GetServiceInfo(), before,
		"with no provider installed the coordinator gRPC server must carry exactly the services milvus itself registered")

	// mockMix implements neither per-resource-group method. If the seam built
	// the adapter regardless of the capability being absent, the type
	// assertions inside it would fail and this would return an error.
	assert.NoError(t, startCoordinatorEngine(context.Background(), &mockMix{}, nil),
		"with no provider installed the seam must not construct the adapter")

	// A coordinator that CAN answer the extras must still never be asked
	// them when no provider is installed.
	coord := &engineTestCoord{}
	assert.NoError(t, startCoordinatorEngine(context.Background(), coord, nil))
	assert.Zero(t, coord.seenRG,
		"with no provider installed nothing may ask the coordinator for per-resource-group load")
	assert.Zero(t, coord.seenInvalidatedCollectionID,
		"with no provider installed nothing may make the coordinator invalidate a proxy cache")

	assert.NotPanics(t, func() { stopCoordinatorEngine(context.Background()) })
}

// TestServerStartWithoutEngineLeavesNativeStartupUnchanged asserts the same at
// the real call site: Server.start() succeeds against a coordinator that
// cannot serve an engine at all.
func TestServerStartWithoutEngineLeavesNativeStartupUnchanged(t *testing.T) {
	extension.ResetForTest()
	t.Cleanup(extension.ResetForTest)

	svr := &Server{ctx: context.Background(), mixCoord: &mockMix{}}
	assert.NoError(t, svr.start())
}

// TestServerStartHandsEngineAdapterOverCoordinator asserts that Server.start()
// reaches the engine and that what the engine receives is a live view of the
// real coordinator, including the renamed per-resource-group load method.
func TestServerStartHandsEngineAdapterOverCoordinator(t *testing.T) {
	engine := &recordingEngine{}
	installEngine(t, engine)

	coord := &engineTestCoord{
		loadPercentageOnlyCoord: loadPercentageOnlyCoord{pct: 63},
	}
	svr := newTestServer(t, coord)
	assert.NoError(t, svr.start())
	assert.Equal(t, 1, engine.startCount, "the coordinator must start the engine exactly once")

	pct, err := engine.seenExtras.GetReplicaLoadPercentByRG(context.Background(), 42, "rg-a")
	assert.NoError(t, err)
	assert.Equal(t, int32(63), pct,
		"GetReplicaLoadPercentByRG must return what the coordinator computed, not a placeholder")
	assert.Equal(t, int64(42), coord.seenCollectionID,
		"the collection id must reach GetLoadPercentageByResourceGroup unchanged")
	assert.Equal(t, "rg-a", coord.seenRG,
		"the resource group name must reach GetLoadPercentageByResourceGroup unchanged")

	assert.NoError(t, engine.seenExtras.InvalidateShardLeaderCache(context.Background(), 44))
	assert.Equal(t, int64(44), coord.seenInvalidatedCollectionID,
		"the collection id must reach the coordinator's proxy fan-out unchanged, or some other collection's shard leaders are the ones the proxies forget")

	// The coordinator itself is handed over, not adapted: the engine gets the
	// proxy-shaped client milvus already holds, so there is no forwarding here
	// to test. What the test can still say is that it arrived - a nil would
	// leave an engine unable to reach the coordinator at all, and nothing else
	// in this file would notice.
	assert.NotNil(t, engine.seenCoord,
		"the engine must be handed the coordinator client, or it cannot load anything")
}

// TestServerStartFailsWhenCoordinatorCannotInvalidateShardLeaderCache asserts
// the same for the third method with no proto RPC behind it. An engine started
// over a coordinator that cannot invalidate would release collections the
// proxies go on routing queries to, and would only find out at the first
// release.
func TestServerStartFailsWhenCoordinatorCannotInvalidateShardLeaderCache(t *testing.T) {
	engine := &recordingEngine{}
	installEngine(t, engine)

	svr := &Server{ctx: context.Background(), mixCoord: &loadPercentageOnlyCoord{}}
	err := svr.start()
	assert.ErrorContains(t, err, "InvalidateShardLeaderCache")
	assert.Zero(t, engine.startCount,
		"an engine must not be started over a coordinator that cannot invalidate the proxy shard-leader cache")
}

// TestServerStartPropagatesEngineStartFailure asserts a failing engine aborts
// coordinator start-up rather than leaving the deployment running without its
// control plane.
func TestServerStartPropagatesEngineStartFailure(t *testing.T) {
	want := errors.New("engine failed to start")
	installEngine(t, &recordingEngine{startErr: want})

	svr := newTestServer(t, &engineTestCoord{})
	assert.ErrorIs(t, svr.start(), want)
}

// TestServerStartFailsWhenCoordinatorCannotReportPerResourceGroupLoad asserts
// the seam refuses a coordinator missing the one method that has no proto RPC
// behind it, and refuses it before the engine is started.
func TestServerStartFailsWhenCoordinatorCannotReportPerResourceGroupLoad(t *testing.T) {
	engine := &recordingEngine{}
	installEngine(t, engine)

	// mockMix implements types.MixCoordComponent but not
	// GetLoadPercentageByResourceGroup.
	svr := &Server{ctx: context.Background(), mixCoord: &mockMix{}}
	err := svr.start()
	assert.ErrorContains(t, err, "GetLoadPercentageByResourceGroup")
	assert.Zero(t, engine.startCount,
		"an engine must not be started over a coordinator that cannot answer per-resource-group load")
}

func TestServerStopStopsEngine(t *testing.T) {
	engine := &recordingEngine{stopErr: errors.New("engine stop failed")}
	installEngine(t, engine)

	svr := newTestServer(t, &engineTestCoord{})
	assert.NoError(t, svr.Stop(),
		"an engine that fails to stop must not fail the coordinator shutdown")
	assert.Equal(t, 1, engine.stopCount, "the coordinator must stop the engine exactly once")
}

// TestServerGrpcSetupRegistersEngineServices asserts the engine gets the real
// coordinator gRPC server, and gets it early enough that its services are
// visible on the server that then serves.
func TestServerGrpcSetupRegistersEngineServices(t *testing.T) {
	paramtable.Init()

	engine := &recordingEngine{}
	installEngine(t, engine)

	listener, err := netutil.NewListener(netutil.OptIP("127.0.0.1"), netutil.OptPort(0))
	assert.NoError(t, err)

	svr := newTestServer(t, &engineTestCoord{})
	svr.listener = listener
	t.Cleanup(func() {
		svr.grpcServer.Stop()
		svr.grpcWG.Wait()
		listener.Close()
		svr.cancel()
	})

	assert.NoError(t, svr.startGrpc())
	assert.Same(t, svr.grpcServer, engine.seenRegistrar,
		"the engine must be handed the coordinator's own gRPC server")
	assert.Contains(t, svr.grpcServer.GetServiceInfo(), "extension.test.EngineService",
		"a service the engine registers must be live on the serving coordinator server")
}

// activatableCoord is engineTestCoord plus the activation hook the real
// coordinator has: callbacks wait until fire() runs them.
type activatableCoord struct {
	engineTestCoord
	pending []func()
}

func (c *activatableCoord) OnActive(fn func()) { c.pending = append(c.pending, fn) }
func (c *activatableCoord) fire() {
	for _, fn := range c.pending {
		fn()
	}
	c.pending = nil
}

// On a coordinator with active-standby, the engine must not start until this
// replica is ACTIVE: before activation the sub-coordinators it reads are not
// initialized, and a standby replica running a second engine would be a
// second control plane doing resource-group accounting. A replica that never
// activates never starts it.
func TestEngineStartWaitsForActivation(t *testing.T) {
	engine := &recordingEngine{}
	installEngine(t, engine)

	coord := &activatableCoord{}
	svr := newTestServer(t, &coord.engineTestCoord)
	svr.mixCoord = coord // the notifier interface must be visible to the seam

	assert.NoError(t, svr.start())
	assert.Zero(t, engine.startCount,
		"the engine must not start on a replica that is not ACTIVE yet")

	coord.fire()
	assert.Equal(t, 1, engine.startCount, "activation is what starts the engine")
}

// The extras adapter carries the three answers that have no RPC, and nothing
// else. Narrowness matters here for the opposite reason it used to: the
// coordinator's own surface now reaches an engine directly, through
// extension.Coordinator, so anything still going through this adapter is by
// definition something that could not be reached that way. A coordinator
// leaking out of it would hide that fact.
func TestExtrasCarryOnlyWhatHasNoRPC(t *testing.T) {
	extras, err := newMixCoordExtras(&engineTestCoord{})
	require.NoError(t, err)

	_, leaks := extras.(types.MixCoordComponent)
	assert.False(t, leaks,
		"the extras adapter must not be a way back to the whole coordinator; what is reachable by RPC belongs on extension.Coordinator")

	type shardLeadersProvider interface {
		GetShardLeaders(ctx context.Context, req *querypb.GetShardLeadersRequest) (*querypb.GetShardLeadersResponse, error)
	}
	_, leaks = extras.(shardLeadersProvider)
	assert.False(t, leaks,
		"GetShardLeaders is a real RPC, so it reaches an engine through extension.Coordinator, not through here")
}
