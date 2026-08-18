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

// loadPercentageByResourceGroupProvider,
// shardLeaderReadinessByResourceGroupProvider and
// shardLeaderCacheInvalidatorProvider are the three parts of
// extension.MixCoord that the coordinator does not already expose under those
// names. types.MixCoordComponent covers the other eight methods verbatim;
// these three are in-process methods reached through mixCoordImpl.
//
// They are declared as interfaces and type-asserted rather than added to
// types.MixCoordComponent so that the native coordinator interface, and every
// generated mock of it, stays untouched by this seam. They are kept separate
// rather than merged into one interface so that a coordinator missing any one
// of them is refused by name.
type loadPercentageByResourceGroupProvider interface {
	GetLoadPercentageByResourceGroup(ctx context.Context, collectionID int64, rgName string) (int32, error)
}

type shardLeaderReadinessByResourceGroupProvider interface {
	GetShardLeaderReadinessByResourceGroup(ctx context.Context, collectionID int64, rgName string) (extension.ShardLeaderReadiness, error)
}

type shardLeaderCacheInvalidatorProvider interface {
	InvalidateShardLeaderCache(ctx context.Context, collectionID int64) error
}

// mixCoordEngineClient adapts the coordinator to extension.MixCoord.
//
// An adapter is needed because three of the methods the engine asks for are
// not on types.MixCoordComponent at all: the coordinator spells two of them
// GetLoadPercentageByResourceGroup and GetShardLeaderReadinessByResourceGroup,
// and carries the third, InvalidateShardLeaderCache, only on the concrete
// implementation that owns the proxy client manager. Everything else is
// forwarded by embedding, unchanged - unlike the proxy's
// mixCoordAdmissionClient, no signature narrowing is involved here, because
// types.MixCoordComponent is the server-side surface and carries no trailing
// ...grpc.CallOption.
type mixCoordEngineClient struct {
	types.MixCoordComponent
	loadPercentage       loadPercentageByResourceGroupProvider
	shardLeaderReadiness shardLeaderReadinessByResourceGroupProvider
	shardLeaderCache     shardLeaderCacheInvalidatorProvider
}

// GetReplicaLoadPercentByRG forwards to the coordinator's per-resource-group
// load percentage, preserving its contract: -1 for "no replica of this
// collection in this resource group", 0 for "a replica is there and carries
// nothing yet".
func (c mixCoordEngineClient) GetReplicaLoadPercentByRG(ctx context.Context, collectionID int64, rgName string) (int32, error) {
	return c.loadPercentage.GetLoadPercentageByResourceGroup(ctx, collectionID, rgName)
}

// GetShardLeadersByRG forwards to the coordinator's per-resource-group
// shard-leader readiness, which - unlike the collection-wide GetShardLeaders
// forwarded by embedding - reports only on the replicas that live in the named
// resource group.
func (c mixCoordEngineClient) GetShardLeadersByRG(ctx context.Context, collectionID int64, rgName string) (extension.ShardLeaderReadiness, error) {
	return c.shardLeaderReadiness.GetShardLeaderReadinessByResourceGroup(ctx, collectionID, rgName)
}

// InvalidateShardLeaderCache forwards to the coordinator's proxy fan-out. The
// engine needs it after a release it issued itself, so that the proxies stop
// routing queries to query nodes that no longer serve the collection.
func (c mixCoordEngineClient) InvalidateShardLeaderCache(ctx context.Context, collectionID int64) error {
	return c.shardLeaderCache.InvalidateShardLeaderCache(ctx, collectionID)
}

// newMixCoordEngineClient builds the adapter, refusing a coordinator that
// cannot answer any one of the three methods it has to supply itself. Failing
// here stops the process at start-up instead of letting the engine discover at
// its first readiness check, or at its first release, that the answer is
// missing.
func newMixCoordEngineClient(coord types.MixCoordComponent) (extension.MixCoord, error) {
	loadPercentage, ok := coord.(loadPercentageByResourceGroupProvider)
	if !ok {
		return nil, merr.WrapErrServiceInternal("extension: coordinator does not provide GetLoadPercentageByResourceGroup, cannot serve the coordinator engine")
	}
	shardLeaders, ok := coord.(shardLeaderReadinessByResourceGroupProvider)
	if !ok {
		return nil, merr.WrapErrServiceInternal("extension: coordinator does not provide GetShardLeaderReadinessByResourceGroup, cannot serve the coordinator engine")
	}
	shardLeaderCache, ok := coord.(shardLeaderCacheInvalidatorProvider)
	if !ok {
		return nil, merr.WrapErrServiceInternal("extension: coordinator does not provide InvalidateShardLeaderCache, cannot serve the coordinator engine")
	}
	return mixCoordEngineClient{
		MixCoordComponent:    coord,
		loadPercentage:       loadPercentage,
		shardLeaderReadiness: shardLeaders,
		shardLeaderCache:     shardLeaderCache,
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
// immediately, which is also the pre-active-standby behaviour.
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
func startCoordinatorEngine(ctx context.Context, coord types.MixCoordComponent) error {
	engine := coordinatorEngine()
	if engine == nil {
		return nil
	}
	client, err := newMixCoordEngineClient(coord)
	if err != nil {
		return err
	}
	start := func() error {
		if err := engine.Start(ctx, client); err != nil {
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
		if err := start(); err != nil {
			mlog.Panic(ctx, "coordinator engine failed to start on activation; a coordinator serving without its engine would accept work nothing accounts for", mlog.Err(err))
		}
	})
	return nil
}

// stopCoordinatorEngine stops the installed engine. It runs on the shutdown
// path even when start-up failed, so an engine must tolerate being stopped
// without having been started.
func stopCoordinatorEngine(ctx context.Context) {
	engine := coordinatorEngine()
	if engine == nil {
		return
	}
	if err := engine.Stop(); err != nil {
		mlog.Warn(ctx, "coordinator engine stop failed", mlog.Err(err))
	}
}
