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

	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/extension"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// This file is the coordinator-side seam. It declares WHERE the coordinator
// consults the installed extension; the implementation lives outside this
// tree. With no provider installed every function here returns without
// touching anything, so a stock binary behaves exactly as the community build.

// coordinatorEngine returns the installed coordinator engine, or nil when none
// is installed and the native path applies.
func coordinatorEngine() extension.CoordinatorEngine {
	return extension.Caps().CoordinatorEngine
}

// The two interfaces below are extension.CoordinatorExtras as the coordinator
// spells it: the name differs for one, and the other lives on the concrete
// implementation that owns the proxy client manager.
//
// They are declared here and type-asserted rather than added to
// types.MixCoordComponent so that the native coordinator interface, and every
// generated mock of it, stays untouched by this seam. They are kept separate
// rather than merged into one so that a coordinator missing any one of them is
// refused by name.
type loadPercentageByResourceGroupProvider interface {
	GetLoadPercentageByResourceGroup(ctx context.Context, collectionID int64, rgName string) (int32, error)
}

type shardLeaderCacheInvalidatorProvider interface {
	InvalidateShardLeaderCache(ctx context.Context, collectionID int64) error
}

// mixCoordExtras adapts the coordinator to extension.CoordinatorExtras.
//
// It used to carry eleven methods. Eight of them were the coordinator's own
// RPCs, forwarded one by one purely so that an engine outside this repository
// could name the type: types.MixCoordClient is under internal/. That is no
// longer needed - extension.Coordinator is the composition of milvus's
// generated coordinator clients, which types.MixCoordClient satisfies as it
// stands - so what is left here is only what has no RPC at all.
type mixCoordExtras struct {
	loadPercentage   loadPercentageByResourceGroupProvider
	shardLeaderCache shardLeaderCacheInvalidatorProvider
}

func (c mixCoordExtras) GetReplicaLoadPercentByRG(ctx context.Context, collectionID int64, rgName string) (int32, error) {
	return c.loadPercentage.GetLoadPercentageByResourceGroup(ctx, collectionID, rgName)
}

func (c mixCoordExtras) InvalidateShardLeaderCache(ctx context.Context, collectionID int64) error {
	return c.shardLeaderCache.InvalidateShardLeaderCache(ctx, collectionID)
}

// newMixCoordExtras builds the extras adapter, refusing a coordinator that
// cannot answer either one. Failing here stops the process at
// start-up instead of letting the engine discover at its first readiness
// check, or at its first release, that the answer is missing.
func newMixCoordExtras(coord types.MixCoordComponent) (extension.CoordinatorExtras, error) {
	loadPercentage, ok := coord.(loadPercentageByResourceGroupProvider)
	if !ok {
		return nil, merr.WrapErrServiceInternal("extension: coordinator does not provide GetLoadPercentageByResourceGroup, cannot serve the coordinator engine")
	}
	shardLeaderCache, ok := coord.(shardLeaderCacheInvalidatorProvider)
	if !ok {
		return nil, merr.WrapErrServiceInternal("extension: coordinator does not provide InvalidateShardLeaderCache, cannot serve the coordinator engine")
	}
	return mixCoordExtras{
		loadPercentage:   loadPercentage,
		shardLeaderCache: shardLeaderCache,
	}, nil
}

// registerCoordinatorEngineGRPC lets the installed engine hang its own gRPC
// services on the coordinator's server. It must run while the server is being
// built: gRPC panics on a service registered after Serve begins.
func registerCoordinatorEngineGRPC(reg grpc.ServiceRegistrar) {
	engine := coordinatorEngine()
	if engine == nil {
		return
	}
	engine.RegisterOnCoordinator(reg)
}

// activeNotifier is the slice of the concrete coordinator the seam uses to
// defer the engine's start to activation. Implemented by mixCoordImpl; a
// coordinator that does not implement it (a test double) starts the engine
// immediately, which is also the pre-active-standby behavior.
type activeNotifier interface {
	OnActive(fn func())
}

// startCoordinatorEngine hands the installed engine its view of the running
// coordinator and starts it - once this replica is ACTIVE. The adapter is
// built (and the coordinator's capabilities verified) synchronously, so a
// coordinator that cannot serve the engine still fails startup immediately;
// the engine's own Start waits for activation, because (a) before it the
// sub-coordinators it reads are not initialized, and (b) a standby replica
// must not run a second engine - one control plane doing resource-group
// accounting per deployment, not one per replica. On a standby that never
// activates, Start never runs; gRPC registration (registerCoordinatorEngineGRPC)
// still happens on every replica so a failover serves the engine's services.
//
// An engine that fails to start at activation panics the process: the form
// declared the capability required, and a coordinator serving traffic without
// its engine would accept work nothing accounts for. The synchronous path
// (no OnActive - a test double) returns the error instead.
func startCoordinatorEngine(ctx context.Context, coord types.MixCoordComponent, client types.MixCoordClient) error {
	engine := coordinatorEngine()
	if engine == nil {
		return nil
	}
	extras, err := newMixCoordExtras(coord)
	if err != nil {
		return err
	}
	start := func() error {
		if err := engine.Start(ctx, client, extras); err != nil {
			return err
		}
		mlog.Info(ctx, "coordinator engine started")
		return nil
	}
	notifier, ok := coord.(activeNotifier)
	if !ok {
		return start()
	}
	notifier.OnActive(func() {
		// Serialized against stopCoordinatorEngine: a coordinator stopping
		// right as it activates must not run Stop concurrently with a Start
		// still in flight. The panic below fires after the state already
		// moved to Healthy - the engine cannot start earlier, because its
		// recovery reads coordinator state through the health gates - so the
		// window between Healthy and the process dying is accepted and kept
		// as short as a panic makes it.
		engineLifecycleMu.Lock()
		defer engineLifecycleMu.Unlock()
		if err := start(); err != nil {
			mlog.Panic(ctx, "coordinator engine failed to start on activation; a coordinator serving without its engine would accept work nothing accounts for", mlog.Err(err))
		}
	})
	return nil
}

// engineLifecycleMu serializes the engine's activation-time Start against
// stopCoordinatorEngine.
var engineLifecycleMu sync.Mutex

// stopCoordinatorEngine stops the installed engine. It runs on the shutdown
// path even when start-up failed, so an engine must tolerate being stopped
// without having been started.
func stopCoordinatorEngine(ctx context.Context) {
	engine := coordinatorEngine()
	if engine == nil {
		return
	}
	// Serialized against the activation-time Start; see startCoordinatorEngine.
	engineLifecycleMu.Lock()
	defer engineLifecycleMu.Unlock()
	if err := engine.Stop(); err != nil {
		mlog.Warn(ctx, "coordinator engine stop failed", mlog.Err(err))
	}
}
