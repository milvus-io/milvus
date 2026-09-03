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

	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
)

// Coordinator is milvus's coordinator, as a form reaches it.
//
// It is the composition of milvus's three generated coordinator clients and
// nothing else. That is deliberate, and it replaced a hand-picked interface of
// eleven methods with an adapter forwarding each one:
//
//   - milvus's own types.MixCoordClient satisfies this directly, so no adapter
//     exists to write, to test, or to forget a method in.
//   - Every coordinator RPC is already on it, so a form that needs one more
//     does not need a milvus change to reach it. The narrow interface made
//     each new call a pull request here.
//   - The three embedded interfaces live in this module and in milvus-proto,
//     both of which a form outside this repository can import. The narrow
//     interface existed largely because types.MixCoordClient is under
//     internal/ and cannot be named from outside - but Go interfaces are
//     structural, so a form declares its own equivalent and receives the
//     value.
//
// What is NOT here is as deliberate: the per-resource-group load percentage
// and shard-leader readiness that querycoord computes in process have no RPC,
// so they are not reachable this way. A form that needs to know whether a
// collection is servable on one resource group reads GetShardLeaders, whose
// response tags each leader with its replica's resource group, and decides
// for itself.
type Coordinator interface {
	rootcoordpb.RootCoordClient
	querypb.QueryCoordClient
	datapb.DataCoordClient
}

// CoordinatorExtras are the coordinator's answers that have no RPC.
//
// This interface is small on purpose, and every method on it is a statement
// about why an engine has to run inside the coordinator process rather than
// beside it. Everything reachable over the wire is on Coordinator; what is
// left here is what a form outside the process could not ask for:
//
//   - the per-resource-group load percentage is computed in querycoord and
//     never serialized. Shard-leader readiness used to be here too; it is not
//     any more, because GetShardLeaders tags each leader with its replica's
//     resource group and a form scopes the answer itself from that.
//   - shard-leader cache invalidation is a PROXY rpc that the coordinator fans
//     out. Reaching the proxies means discovering them, and milvus's service
//     discovery is under internal/.
//
// Each of these is a candidate for promotion to a real RPC. Until they are,
// this is the list of reasons an engine cannot move out, kept where it can be
// read rather than inferred from an adapter.
type CoordinatorExtras interface {
	// GetReplicaLoadPercentByRG reports how loaded collectionID is on rgName.
	// -1 means no replica of the collection lives in that resource group at
	// all, which is distinct from 0 (a replica is there and carries nothing
	// yet).
	GetReplicaLoadPercentByRG(ctx context.Context, collectionID int64, rgName string) (int32, error)

	// InvalidateShardLeaderCache drops every proxy's cached shard leaders for
	// one collection, so the next query resolves them again instead of routing
	// to query nodes that no longer serve it. An engine that releases a
	// collection on its own account needs this: milvus's own invalidation runs
	// only inside its release job, best-effort and asynchronously.
	InvalidateShardLeaderCache(ctx context.Context, collectionID int64) error
}

// CoordinatorEngine is control-plane machinery that runs inside the
// coordinator process and that milvus itself has no concept of, such as
// scaling query nodes on demand or accounting for what a resource group used.
//
// milvus decides WHEN each lifecycle step happens; what the engine does at
// each step is entirely the implementation's. With no provider installed the
// capability is nil and the coordinator does not construct an adapter, does
// not register anything on its gRPC server, and does not start anything, so a
// stock binary behaves exactly as the community build.
//
// The three methods are called in this order, and only in this order:
//
//  1. RegisterOnCoordinator, while the coordinator's gRPC server is being
//     built and before it serves. The services registered here share the
//     coordinator server's interceptors - cluster and server-id validation,
//     no authentication - so they are reachable by anything that can reach
//     the coordinator port: the deployment's network boundary is their access
//     control, exactly as it is for the coordinator's own services.
//  2. Start, after the coordinator itself is running and able to answer the
//     MixCoord calls the engine makes.
//  3. Stop, once, when the coordinator shuts down.
//
// Steps 1 and 2 are necessarily separate because gRPC forbids registering a
// service after Serve has begun, while an engine cannot usefully call into a
// coordinator that has not started. An implementation must therefore expect
// its own handlers to be reachable in the window between the two, and answer
// them as not-ready rather than touching state Start has yet to build.
//
// NoopCoordinatorEngine is the Noop base under the package evolution policy.
type CoordinatorEngine interface {
	// RegisterOnCoordinator lets the engine hang its own gRPC services on the
	// coordinator's server. milvus never learns which services those are,
	// which is what allows the engine's proto to live entirely outside this
	// repository.
	//
	// It is called before the server serves; registering later panics inside
	// gRPC.
	RegisterOnCoordinator(reg grpc.ServiceRegistrar)

	// Start hands the engine its view of the coordinator and starts it. The
	// coordinator is running by this point, so calls on coord are expected to
	// work.
	//
	// coord is passed here rather than at construction so that an engine can
	// never exist in a started state without one. An error aborts coordinator
	// start-up: an engine that failed to come up would leave the deployment
	// running without the control plane it depends on, which is worse than
	// not coming up at all.
	// ctx is the coordinator server's lifetime context. It is NOT the stop
	// signal: it is canceled only after Stop has already returned, so an
	// implementation shuts down on Stop and may use ctx merely as the parent
	// for the background work it starts.
	Start(ctx context.Context, coord Coordinator, extras CoordinatorExtras) error

	// Stop shuts the engine down. It is called on a graceful coordinator
	// shutdown even when Start failed or never ran, so it must tolerate both;
	// it is NOT called when the process exits because start-up itself failed
	// (the process just dies), so anything needing cleanup across a failed
	// boot must be crash-safe rather than Stop-dependent.
	Stop() error
}

// NoopCoordinatorEngine registers nothing, starts nothing and stops cleanly:
// the inert answer at every lifecycle step.
type NoopCoordinatorEngine struct{}

var _ CoordinatorEngine = NoopCoordinatorEngine{}

func (NoopCoordinatorEngine) RegisterOnCoordinator(grpc.ServiceRegistrar) {}

func (NoopCoordinatorEngine) Start(context.Context, Coordinator, CoordinatorExtras) error {
	return nil
}

func (NoopCoordinatorEngine) Stop() error { return nil }
