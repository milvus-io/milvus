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

	seenCoord  extension.Coordinator
	startCount int
	stopCount  int
}

func (e *recordingEngine) Start(_ context.Context, coord extension.Coordinator) error {
	e.startCount++
	e.seenCoord = coord
	return e.startErr
}

func (e *recordingEngine) Stop() error {
	e.stopCount++
	return e.stopErr
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
	}
}

func TestCoordinatorEngineSeamIsInertWithoutProvider(t *testing.T) {
	extension.ResetForTest()
	t.Cleanup(extension.ResetForTest)
	assert.NoError(t, startCoordinatorEngine(context.Background(), &mockMix{}, nil))
	assert.NotPanics(t, func() { stopCoordinatorEngine(context.Background()) })
}

func TestServerStartWithoutEngineLeavesNativeStartupUnchanged(t *testing.T) {
	extension.ResetForTest()
	t.Cleanup(extension.ResetForTest)
	svr := &Server{ctx: context.Background(), mixCoord: &mockMix{}}
	assert.NoError(t, svr.start())
}

func TestServerStartHandsTheCoordinatorClientToTheEngine(t *testing.T) {
	engine := &recordingEngine{}
	installEngine(t, engine)
	svr := newTestServer(t, &mockMix{})
	assert.NoError(t, svr.start())
	assert.Equal(t, 1, engine.startCount, "the coordinator must start the engine exactly once")
	assert.Same(t, svr.mixCoordClient, engine.seenCoord,
		"the engine must be handed the coordinator client, or it cannot load anything")
}

func TestServerStartPropagatesEngineStartFailure(t *testing.T) {
	want := errors.New("engine failed to start")
	installEngine(t, &recordingEngine{startErr: want})
	svr := newTestServer(t, &mockMix{})
	assert.ErrorIs(t, svr.start(), want)
}

func TestServerStopStopsEngine(t *testing.T) {
	engine := &recordingEngine{stopErr: errors.New("engine stop failed")}
	installEngine(t, engine)
	svr := newTestServer(t, &mockMix{})
	assert.NoError(t, svr.start())
	assert.NoError(t, svr.Stop(),
		"an engine that fails to stop must not fail the coordinator shutdown")
	assert.Equal(t, 1, engine.stopCount, "the coordinator must stop the engine exactly once")
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
	require.Zero(t, engine.startCount, "not activated, so not started")

	assert.NoError(t, svr.Stop())
	assert.Zero(t, engine.stopCount, "an engine that was never started is not stopped")

	coord.fire()
	assert.Zero(t, engine.startCount, "an activation after shutdown began must not start an engine nothing will stop")
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

	activated := make(chan struct{})
	go func() {
		defer close(activated)
		coord.fire()
	}()
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
	select {
	case <-activated:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop must be what ends the Start")
	}
	assert.Equal(t, 1, engine.stopCount)
	assert.Equal(t, 1, engine.startCount)
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
	assert.Zero(t, engine.startCount,
		"the engine must not start on a replica that is not ACTIVE yet")
	coord.fire()
	assert.Equal(t, 1, engine.startCount, "activation is what starts the engine")
}
