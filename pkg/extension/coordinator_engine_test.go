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
)

type fakeCoordinatorEngine struct {
	startErr error
	stopErr  error

	seenCtx   context.Context
	seenCoord Coordinator
	stopped   bool
}

func (f *fakeCoordinatorEngine) Start(ctx context.Context, coord Coordinator) error {
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

	type ctxKey struct{}
	ctx := context.WithValue(context.Background(), ctxKey{}, "coordinator")
	assert.NoError(t, got.Start(ctx, nil))
	assert.Equal(t, "coordinator", engine.seenCtx.Value(ctxKey{}),
		"the context must reach the implementation unchanged")
}

func TestCoordinatorEngineLifecycleErrorsArePropagated(t *testing.T) {
	ResetForTest()
	t.Cleanup(ResetForTest)
	wantStartErr := errors.New("engine failed to start")
	wantStopErr := errors.New("engine failed to stop")
	engine := &fakeCoordinatorEngine{startErr: wantStartErr, stopErr: wantStopErr}
	assert.NoError(t, SetProvider(fakeProvider{name: "testprovider", caps: Capabilities{CoordinatorEngine: engine}}))
	assert.ErrorIs(t, Caps().CoordinatorEngine.Start(context.Background(), nil), wantStartErr,
		"an error from Start must survive install, Caps, and the call unwrapped and unreplaced")
	assert.ErrorIs(t, Caps().CoordinatorEngine.Stop(), wantStopErr,
		"an error from Stop must survive install, Caps, and the call unwrapped and unreplaced")
	assert.True(t, engine.stopped)
}
