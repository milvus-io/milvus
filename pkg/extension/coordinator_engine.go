package extension

import (
	"context"

	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

// MixCoord is the slice of the coordinator API a CoordinatorEngine may call.
//
// It is deliberately narrow: eight of these eleven methods are the
// coordinator's own service surface (rootcoord + datacoord + querycoord), so
// an engine that holds this interface can read collection and index metadata,
// size resource groups, and load and release a collection per resource group -
// and nothing else. No mutation of user data, no cluster administration, no
// session or meta-store access.
//
// Narrowness has to be maintained to mean anything, so a method leaves this
// interface as soon as nothing calls it. The collection-wide GetShardLeaders
// was removed on those grounds once GetShardLeadersByRG replaced it; it comes
// back only alongside a caller that genuinely wants the flattened,
// resource-group-blind view.
//
// Every parameter and result is either committed proto or a plain struct
// declared in this package, which keeps the interface reachable from this
// module: pkg/v3 is a separate Go module and must not import internal/.
//
// The other three methods have no coordinator RPC behind them. Two of them
// exist for the same reason: the eight RPCs answer per collection, while a
// distribution that loads one collection into several resource groups
// independently has to ask per resource group. The third reaches the proxy
// fan-out that only the coordinator process holds a client manager for. All
// three map onto in-process methods with no proto request type:
//
//   - GetReplicaLoadPercentByRG maps onto querycoordv2 Server.GetLoadPercentageByResourceGroup.
//   - GetShardLeadersByRG maps onto querycoordv2 Server.GetShardLeaderReadinessByResourceGroup.
//   - InvalidateShardLeaderCache maps onto the coordinator's proxy client manager.
//
// The seam that supplies this interface is responsible for those mappings, and
// must fail loudly at coordinator start if the concrete coordinator cannot
// provide any one of them. An engine that silently read a collection-wide
// answer in place of a per-resource-group one would call a resource group ready
// before it holds any data, or before it has a shard leader of its own - which
// is the admission bug the first two methods exist to close.
//
// Returned percentages follow the querycoord contract: -1 means no replica of
// the collection lives in that resource group at all, which is distinct from 0
// (a replica is there and carries nothing yet).
type MixCoord interface {
	// Collection and index metadata.
	DescribeCollection(ctx context.Context, req *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error)
	DescribeIndex(ctx context.Context, req *indexpb.DescribeIndexRequest) (*indexpb.DescribeIndexResponse, error)

	// Resource-group capacity.
	DescribeResourceGroup(ctx context.Context, req *querypb.DescribeResourceGroupRequest) (*querypb.DescribeResourceGroupResponse, error)
	UpdateResourceGroups(ctx context.Context, req *querypb.UpdateResourceGroupsRequest) (*commonpb.Status, error)

	// Per-resource-group collection load and release.
	LoadCollection(ctx context.Context, req *querypb.LoadCollectionRequest) (*commonpb.Status, error)
	ReleaseCollection(ctx context.Context, req *querypb.ReleaseCollectionRequest) (*commonpb.Status, error)
	ShowLoadCollections(ctx context.Context, req *querypb.ShowCollectionsRequest) (*querypb.ShowCollectionsResponse, error)
	UpdateLoadConfig(ctx context.Context, req *querypb.UpdateLoadConfigRequest) (*commonpb.Status, error)

	// Per-resource-group load progress. See the type comment for the mapping
	// onto querycoordv2 Server.GetLoadPercentageByResourceGroup.
	GetReplicaLoadPercentByRG(ctx context.Context, collectionID int64, rgName string) (int32, error)

	// Shard-leader readiness, restricted to the replicas in one resource
	// group. See ShardLeaderReadiness, and the type comment above for the
	// mapping onto querycoordv2 Server.GetShardLeaderReadinessByResourceGroup.
	GetShardLeadersByRG(ctx context.Context, collectionID int64, rgName string) (ShardLeaderReadiness, error)

	// InvalidateShardLeaderCache drops every proxy's cached shard leaders for
	// one collection, so the next query resolves them again instead of routing
	// to query nodes that no longer serve it.
	//
	// An engine that releases a collection on its own account needs this: the
	// proxies learn of a release only through this invalidation, and the one
	// native milvus performs inside its release job is best-effort - failures
	// are logged and the release proceeds - and runs asynchronously, off the
	// acknowledgement of the release message rather than on the caller's
	// thread. An engine that has just released a collection therefore issues
	// its own rather than assuming milvus's landed.
	//
	// It fans out to every proxy the coordinator knows, and returns the first
	// failure. It is not the caller's job to retry: a proxy that missed the
	// invalidation refreshes its cache on the next query that fails to route.
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
	Start(ctx context.Context, coord MixCoord) error

	// Stop shuts the engine down. It is called on a graceful coordinator
	// shutdown even when Start failed or never ran, so it must tolerate both;
	// it is NOT called when the process exits because start-up itself failed
	// (the process just dies), so anything needing cleanup across a failed
	// boot must be crash-safe rather than Stop-dependent.
	Stop() error
}
