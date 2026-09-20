// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package grpcmixcoord

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v3/extension"
)

type recordingEngine struct {
	startErr error
	stopErr  error

	mu         sync.Mutex
	seenCoord  extension.Coordinator
	startCount int
	stopCount  int
}

func (e *recordingEngine) Start(_ context.Context, coord extension.Coordinator) error {
	e.mu.Lock()
	e.startCount++
	e.seenCoord = coord
	e.mu.Unlock()
	return e.startErr
}

func (e *recordingEngine) Stop() error {
	e.mu.Lock()
	e.stopCount++
	e.mu.Unlock()
	return e.stopErr
}

func (e *recordingEngine) starts() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.startCount
}

func (e *recordingEngine) stops() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.stopCount
}

func (e *recordingEngine) coord() extension.Coordinator {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.seenCoord
}

func installEngine(t *testing.T, e extension.CoordinatorEngine) {
	t.Helper()
	extension.ResetForTest()
	resetEngineLifecycleForTest()
	t.Cleanup(extension.ResetForTest)
	t.Cleanup(resetEngineLifecycleForTest)
	extension.SetCoordinatorEngine(e)
}

func newTestServer(t *testing.T, coord *mockMix) *Server {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return &Server{
		ctx:            ctx,
		cancel:         cancel,
		mixCoord:       coord,
		mixCoordClient: mocks.NewMockMixCoordClient(t),
		grpcErrChan:    make(chan error),
		grpcServing:    make(chan struct{}),
		// Recovery never finishes here, so the server never tries to serve on
		// a listener a test has not bound; serveNow is what says it answers.
		recoveryWaiter: recoveryWaiterFunc(func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		}),
	}
}

// startedOn registers the seam on svr and returns the coordinator that fires
// activation, with the server already answering gRPC unless the caller says
// otherwise.
func serveNow(svr *Server) {
	svr.markGrpcServing()
}

// fatalRecorder stands in for the process exit the server does on a failed
// engine start.
type fatalRecorder struct {
	mu   sync.Mutex
	errs []error
	seen chan struct{}
}

func newFatalRecorder() *fatalRecorder {
	return &fatalRecorder{seen: make(chan struct{}, 1)}
}

func (f *fatalRecorder) record(err error) {
	f.mu.Lock()
	f.errs = append(f.errs, err)
	f.mu.Unlock()
	select {
	case f.seen <- struct{}{}:
	default:
	}
}

func (f *fatalRecorder) wait(t *testing.T) error {
	t.Helper()
	select {
	case <-f.seen:
	case <-time.After(5 * time.Second):
		t.Fatal("the engine start failure never reached the fatal path")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.errs[0]
}

// eventually waits for the engine's Start, which now runs on its own goroutine.
func waitStartCount(t *testing.T, e *recordingEngine, want int) {
	t.Helper()
	assert.Eventually(t, func() bool { return e.starts() == want }, 5*time.Second, 5*time.Millisecond,
		"the engine must have been started %d time(s), saw %d", want, e.starts())
}

func TestCoordinatorEngineSeamIsInertWithoutProvider(t *testing.T) {
	extension.ResetForTest()
	t.Cleanup(extension.ResetForTest)
	assert.NoError(t, startCoordinatorEngine(context.Background(), &mockMix{}, nil,
		func(context.Context) error { return nil }, func(error) {}))
	assert.NotPanics(t, func() { stopCoordinatorEngine(context.Background()) })
}

// A coordinator that does not report activation cannot say which replica is
// the one that runs the engine, and starting one on every replica would have
// two of them accounting for the same cluster.
func TestAnEngineNeedsACoordinatorThatReportsActivation(t *testing.T) {
	installEngine(t, &recordingEngine{})
	err := startCoordinatorEngine(context.Background(), &mockMix{}, nil,
		func(context.Context) error { return nil }, func(error) {})
	assert.ErrorContains(t, err, "does not report activation")
}

func TestServerStartWithoutEngineLeavesNativeStartupUnchanged(t *testing.T) {
	extension.ResetForTest()
	t.Cleanup(extension.ResetForTest)
	svr := newTestServer(t, &mockMix{})
	assert.NoError(t, svr.start())
}

func TestServerStartHandsTheCoordinatorClientToTheEngine(t *testing.T) {
	engine := &recordingEngine{}
	installEngine(t, engine)
	coord := &activatableCoord{}
	svr := newTestServer(t, &coord.mockMix)
	svr.mixCoord = coord
	require.NoError(t, svr.start())
	serveNow(svr)
	coord.fire()
	waitStartCount(t, engine, 1)
	assert.Same(t, svr.mixCoordClient, engine.coord(),
		"the engine must be handed the coordinator client, or it cannot load anything")
}

// A failed start cannot be returned: by the time it happens the startup path
// has returned, so it goes to the fatal path the server hands the seam.
func TestAFailedEngineStartReachesTheFatalPath(t *testing.T) {
	want := errors.New("engine failed to start")
	installEngine(t, &recordingEngine{startErr: want})
	coord := &activatableCoord{}
	fatal := newFatalRecorder()
	require.NoError(t, startCoordinatorEngine(context.Background(), coord, nil,
		func(context.Context) error { return nil }, fatal.record))
	coord.fire()
	assert.ErrorIs(t, fatal.wait(t), want)
}

func TestServerStopStopsEngine(t *testing.T) {
	engine := &recordingEngine{stopErr: errors.New("engine stop failed")}
	installEngine(t, engine)
	coord := &activatableCoord{}
	svr := newTestServer(t, &coord.mockMix)
	svr.mixCoord = coord
	require.NoError(t, svr.start())
	serveNow(svr)
	coord.fire()
	waitStartCount(t, engine, 1)
	assert.NoError(t, svr.Stop(),
		"an engine that fails to stop must not fail the coordinator shutdown")
	assert.Equal(t, 1, engine.stops(), "the coordinator must stop the engine exactly once")
}

// A standby is stopped without ever having been activated, so its engine was
// never started, and the seam must not call Stop on an engine it never
// started.
func TestStopWithoutStartIsANoOp(t *testing.T) {
	engine := &recordingEngine{}
	installEngine(t, engine)
	coord := &activatableCoord{}
	svr := newTestServer(t, &coord.mockMix)
	svr.mixCoord = coord
	assert.NoError(t, svr.start())
	serveNow(svr)
	require.Zero(t, engine.starts(), "not activated, so not started")

	assert.NoError(t, svr.Stop())
	assert.Zero(t, engine.stops(), "an engine that was never started is not stopped")

	coord.fire()
	assert.Never(t, func() bool { return engine.starts() > 0 }, 200*time.Millisecond, 10*time.Millisecond,
		"an activation after shutdown began must not start an engine nothing will stop")
}

// blockingEngine is an engine whose Start does not return until Stop is
// called, which is what a slow activation looks like from the seam.
type blockingEngine struct {
	recordingEngine
	starting chan struct{} // closed once Start is running
	release  chan struct{} // closed by Stop, which is what lets Start return
	once     sync.Once
}

func newBlockingEngine() *blockingEngine {
	return &blockingEngine{starting: make(chan struct{}), release: make(chan struct{})}
}

func (e *blockingEngine) Start(ctx context.Context, coord extension.Coordinator) error {
	close(e.starting)
	<-e.release
	return e.recordingEngine.Start(ctx, coord)
}

func (e *blockingEngine) Stop() error {
	e.once.Do(func() { close(e.release) })
	return e.recordingEngine.Stop()
}

// A shutdown must not wait for a slow Start: the engine's own Stop is what
// ends it, so holding a lock across Start would deadlock the coordinator's
// shutdown against its own activation.
func TestStopDoesNotWaitForASlowStart(t *testing.T) {
	engine := newBlockingEngine()
	installEngine(t, engine)
	coord := &activatableCoord{}
	svr := newTestServer(t, &coord.mockMix)
	svr.mixCoord = coord
	require.NoError(t, svr.start())
	serveNow(svr)
	coord.fire()

	select {
	case <-engine.starting:
	case <-time.After(5 * time.Second):
		t.Fatal("the activation never reached Start")
	}

	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		stopCoordinatorEngine(context.Background())
	}()
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop must return while Start is still running, not wait for it")
	}
	waitStartCount(t, &engine.recordingEngine, 1) // Stop is what let Start return
	assert.Equal(t, 1, engine.stops())
}

type activatableCoord struct {
	mockMix
	pending []func()
}

func (c *activatableCoord) OnActive(fn func()) { c.pending = append(c.pending, fn) }

func (c *activatableCoord) fire() {
	for _, fn := range c.pending {
		fn()
	}
	c.pending = nil
}

func TestEngineStartWaitsForActivation(t *testing.T) {
	engine := &recordingEngine{}
	installEngine(t, engine)
	coord := &activatableCoord{}
	svr := newTestServer(t, &coord.mockMix)
	svr.mixCoord = coord // the notifier interface must be visible to the seam
	assert.NoError(t, svr.start())
	serveNow(svr)
	assert.Zero(t, engine.starts(),
		"the engine must not start on a replica that is not ACTIVE yet")
	coord.fire()
	waitStartCount(t, engine, 1)
}

// The activation callbacks run to completion before the coordinator's recovery
// barrier opens, and gRPC Serve waits on that barrier. An engine started on
// that chain would block the service its own first call needs, so the seam
// must return from the callback and wait for serving on a goroutine of its
// own.
func TestActivationDoesNotWaitForTheEngine(t *testing.T) {
	engine := newBlockingEngine()
	installEngine(t, engine)
	coord := &activatableCoord{}
	svr := newTestServer(t, &coord.mockMix)
	svr.mixCoord = coord
	require.NoError(t, svr.start())
	t.Cleanup(func() { _ = engine.Stop() })

	serveNow(svr)
	returned := make(chan struct{})
	go func() {
		defer close(returned)
		coord.fire() // what enableExternalAccess does, before the barrier opens
	}()
	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("activation must return without waiting for the engine's Start")
	}
	select {
	case <-engine.starting:
	case <-time.After(5 * time.Second):
		t.Fatal("the engine must still be started, just not on the activation chain")
	}
}

// The engine's first act is usually to call the coordinator back, so it must
// not be started before the coordinator answers gRPC - which happens after the
// recovery barrier, later than activation.
func TestTheEngineWaitsUntilTheCoordinatorAnswers(t *testing.T) {
	engine := &recordingEngine{}
	installEngine(t, engine)
	coord := &activatableCoord{}
	svr := newTestServer(t, &coord.mockMix)
	svr.mixCoord = coord
	require.NoError(t, svr.start())

	coord.fire()
	assert.Never(t, func() bool { return engine.starts() > 0 }, 200*time.Millisecond, 10*time.Millisecond,
		"the engine must not start while gRPC is still waiting for recovery")

	serveNow(svr)
	waitStartCount(t, engine, 1)
}

// A coordinator that shuts down before it ever serves never starts its engine,
// and the goroutine waiting to start it goes away with the context.
func TestAShutdownBeforeServingNeverStartsTheEngine(t *testing.T) {
	engine := &recordingEngine{}
	installEngine(t, engine)
	coord := &activatableCoord{}
	svr := newTestServer(t, &coord.mockMix)
	svr.mixCoord = coord
	require.NoError(t, svr.start())

	coord.fire()
	svr.cancel()
	assert.Never(t, func() bool { return engine.starts() > 0 }, 200*time.Millisecond, 10*time.Millisecond,
		"a coordinator that never served must not start its engine")
	stopCoordinatorEngine(context.Background())
	assert.Zero(t, engine.stops(), "an engine that was never started is not stopped")
}

// A Start is claimed on one goroutine and entered a few instructions later. A
// Stop landing in that window must not reach an engine that has not started;
// the contract is that Stop follows Start, never the other way round.
func TestAStopBetweenClaimingAndEnteringStartCancelsIt(t *testing.T) {
	engine := &recordingEngine{}
	installEngine(t, engine)

	require.True(t, lifecycle.beginStart(), "the Start is claimed")
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		stopCoordinatorEngine(context.Background())
	}()
	assert.Never(t, func() bool {
		select {
		case <-stopped:
			return true
		default:
			return false
		}
	}, 100*time.Millisecond, 10*time.Millisecond,
		"Stop must wait for the claimed Start to be entered or given up")

	assert.False(t, lifecycle.enterStart(), "a Start claimed before a Stop must give itself up")
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop must return once the Start has given itself up")
	}
	assert.Zero(t, engine.stops(), "an engine that never entered Start must not be stopped")
	assert.Zero(t, engine.starts())
}
