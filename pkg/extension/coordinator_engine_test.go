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

package extension

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
)

type recordingServiceRegistrar struct{ names []string }

func (r *recordingServiceRegistrar) RegisterService(desc *grpc.ServiceDesc, _ any) {
	r.names = append(r.names, desc.ServiceName)
}

// fakeCoordinatorEngine records what each lifecycle step was handed.
type fakeCoordinatorEngine struct {
	startErr error
	stopErr  error

	seenRegistrar grpc.ServiceRegistrar
	seenCtx       context.Context
	seenCoord     Coordinator
	seenExtras    CoordinatorExtras
	stopped       bool
}

func (f *fakeCoordinatorEngine) RegisterOnCoordinator(reg grpc.ServiceRegistrar) {
	f.seenRegistrar = reg
	reg.RegisterService(&grpc.ServiceDesc{ServiceName: "extension.test.EngineService", HandlerType: (*any)(nil)}, struct{}{})
}

func (f *fakeCoordinatorEngine) Start(ctx context.Context, coord Coordinator, extras CoordinatorExtras) error {
	f.seenExtras = extras
	f.seenCtx = ctx
	f.seenCoord = coord
	return f.startErr
}

func (f *fakeCoordinatorEngine) Stop() error {
	f.stopped = true
	return f.stopErr
}

func TestCapabilitiesReportsCoordinatorEnginePresence(t *testing.T) {
	assert.False(t, Capabilities{}.has(CapCoordinatorEngine),
		"an empty table must not claim to supply the coordinator engine capability")
	assert.True(t, Capabilities{CoordinatorEngine: &fakeCoordinatorEngine{}}.has(CapCoordinatorEngine))
}

func TestCapCoordinatorEngineIsNotConfusedWithAnotherCapability(t *testing.T) {
	// A table that supplies only the index drain must not answer yes for the
	// coordinator engine, and vice versa: the has() switch must key on the
	// right field.
	assert.False(t, Capabilities{IndexDrain: stubIndexDrainer{}}.has(CapCoordinatorEngine))
	assert.False(t, Capabilities{CoordinatorEngine: &fakeCoordinatorEngine{}}.has(CapIndexDrain))
}

func TestSetProviderRejectsMissingCoordinatorEngineCapability(t *testing.T) {
	ResetForTest()
	t.Cleanup(ResetForTest)

	err := SetProvider(fakeProvider{
		name:     "testprovider",
		requires: []CapabilityID{CapCoordinatorEngine},
		caps:     Capabilities{},
	})
	assert.ErrorContains(t, err, string(CapCoordinatorEngine))
	assert.Nil(t, Caps().CoordinatorEngine, "a failed install must leave no trace")
}

func TestInstalledCoordinatorEngineIsReachableThroughCaps(t *testing.T) {
	ResetForTest()
	t.Cleanup(ResetForTest)

	engine := &fakeCoordinatorEngine{}
	assert.NoError(t, SetProvider(fakeProvider{
		name:     "testprovider",
		requires: []CapabilityID{CapCoordinatorEngine},
		caps:     Capabilities{CoordinatorEngine: engine},
	}))

	got := Caps().CoordinatorEngine
	assert.Same(t, engine, got)

	reg := &recordingServiceRegistrar{}
	got.RegisterOnCoordinator(reg)
	assert.Same(t, reg, engine.seenRegistrar,
		"the ServiceRegistrar must reach the implementation unchanged, or the engine would hang its services on some other server")
	assert.Equal(t, []string{"extension.test.EngineService"}, reg.names,
		"a service the engine registers must land on the registrar milvus supplied")

	type ctxKey struct{}
	ctx := context.WithValue(context.Background(), ctxKey{}, "coordinator")

	// The coordinator is milvus's generated client composition, so a test
	// double for it would be hundreds of methods long. That it is no longer
	// worth faking is the point of the type: nothing here narrows it, so
	// nothing here has to be kept in step with it. A nil carries the one
	// property this test is about - that whatever milvus passes arrives
	// unchanged - and the extras, which ARE narrow, are checked below.
	extras := fakeExtras{pct: -1}
	assert.NoError(t, got.Start(ctx, nil, extras))
	assert.Equal(t, "coordinator", engine.seenCtx.Value(ctxKey{}),
		"the context must reach the implementation unchanged")
	assert.Equal(t, CoordinatorExtras(extras), engine.seenExtras,
		"the extras passed to Start must reach the implementation unchanged")

	pct, err := engine.seenExtras.GetReplicaLoadPercentByRG(ctx, 1, "rg-a")
	assert.NoError(t, err)
	assert.Equal(t, int32(-1), pct,
		"-1 must survive the interface: it means no replica in this resource group, which is not 0")
}

// fakeExtras is the narrow half of the coordinator view - the three answers
// that have no RPC - which is small enough to be worth faking.
type fakeExtras struct {
	pct int32
}

func (f fakeExtras) GetReplicaLoadPercentByRG(context.Context, int64, string) (int32, error) {
	return f.pct, nil
}

func (fakeExtras) InvalidateShardLeaderCache(context.Context, int64) error { return nil }

func TestCoordinatorEngineLifecycleErrorsArePropagated(t *testing.T) {
	ResetForTest()
	t.Cleanup(ResetForTest)

	wantStartErr := errors.New("engine failed to start")
	wantStopErr := errors.New("engine failed to stop")
	engine := &fakeCoordinatorEngine{startErr: wantStartErr, stopErr: wantStopErr}
	assert.NoError(t, SetProvider(fakeProvider{name: "testprovider", caps: Capabilities{CoordinatorEngine: engine}}))

	startErr := Caps().CoordinatorEngine.Start(context.Background(), nil, fakeExtras{})
	assert.ErrorIs(t, startErr, wantStartErr,
		"an error from Start must survive install, Caps, and the call unwrapped and unreplaced")

	stopErr := Caps().CoordinatorEngine.Stop()
	assert.ErrorIs(t, stopErr, wantStopErr,
		"an error from Stop must survive install, Caps, and the call unwrapped and unreplaced")
	assert.True(t, engine.stopped)
}

// The Noop engine is the inert answer at every lifecycle step, so a form that
// embeds it and has no control plane to run does not fail coordinator start.
func TestNoopCoordinatorEngineIsInert(t *testing.T) {
	type embedder struct{ NoopCoordinatorEngine }
	var e CoordinatorEngine = embedder{}

	reg := &recordingServiceRegistrar{}
	e.RegisterOnCoordinator(reg)
	assert.Empty(t, reg.names, "the inert engine must register no service")
	assert.NoError(t, e.Start(context.Background(), nil, fakeExtras{}))
	assert.NoError(t, e.Stop())
}
