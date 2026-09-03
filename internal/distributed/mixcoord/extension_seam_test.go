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
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

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
	assert.NoError(t, svr.Stop(),
		"an engine that fails to stop must not fail the coordinator shutdown")
	assert.Equal(t, 1, engine.stopCount, "the coordinator must stop the engine exactly once")
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
