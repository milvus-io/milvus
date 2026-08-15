package extension

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

// fakeMixCoord is an inert MixCoord used to prove that the coordinator view
// handed to an engine is the very instance the caller passed in. Only the two
// per-resource-group methods return anything, because they are the ones whose
// result shape the interface contract pins down: -1 versus 0 for the load
// percentage, and Ready versus a reason for the shard-leader readiness.
type fakeMixCoord struct {
	pct       int32
	readiness ShardLeaderReadiness
}

func (fakeMixCoord) DescribeCollection(context.Context, *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	return nil, nil
}

func (fakeMixCoord) DescribeIndex(context.Context, *indexpb.DescribeIndexRequest) (*indexpb.DescribeIndexResponse, error) {
	return nil, nil
}

func (fakeMixCoord) DescribeResourceGroup(context.Context, *querypb.DescribeResourceGroupRequest) (*querypb.DescribeResourceGroupResponse, error) {
	return nil, nil
}

func (fakeMixCoord) UpdateResourceGroups(context.Context, *querypb.UpdateResourceGroupsRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (fakeMixCoord) LoadCollection(context.Context, *querypb.LoadCollectionRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (fakeMixCoord) ReleaseCollection(context.Context, *querypb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (fakeMixCoord) ShowLoadCollections(context.Context, *querypb.ShowCollectionsRequest) (*querypb.ShowCollectionsResponse, error) {
	return nil, nil
}

func (fakeMixCoord) UpdateLoadConfig(context.Context, *querypb.UpdateLoadConfigRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (f fakeMixCoord) GetReplicaLoadPercentByRG(context.Context, int64, string) (int32, error) {
	return f.pct, nil
}

func (f fakeMixCoord) GetShardLeadersByRG(context.Context, int64, string) (ShardLeaderReadiness, error) {
	return f.readiness, nil
}

func (fakeMixCoord) InvalidateShardLeaderCache(context.Context, int64) error {
	return nil
}

// recordingServiceRegistrar records the service descriptors registered on it.
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
	seenCoord     MixCoord
	stopped       bool
}

func (f *fakeCoordinatorEngine) RegisterOnCoordinator(reg grpc.ServiceRegistrar) {
	f.seenRegistrar = reg
	reg.RegisterService(&grpc.ServiceDesc{ServiceName: "extension.test.EngineService", HandlerType: (*any)(nil)}, struct{}{})
}

func (f *fakeCoordinatorEngine) Start(ctx context.Context, coord MixCoord) error {
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
	// A table that supplies only admission must not answer yes for the
	// coordinator engine, and vice versa: the has() switch must key on the
	// right field.
	assert.False(t, Capabilities{Admission: &fakeAdmissionChecker{}}.has(CapCoordinatorEngine))
	assert.False(t, Capabilities{CoordinatorEngine: &fakeCoordinatorEngine{}}.has(CapAdmission))
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
	coord := fakeMixCoord{pct: -1, readiness: ShardLeaderReadiness{
		Reason:        ShardLeadersReasonShardsWithoutLeader,
		TotalShards:   2,
		UnreadyShards: []string{"coll-dmc1"},
	}}
	assert.NoError(t, got.Start(ctx, coord))
	assert.Equal(t, "coordinator", engine.seenCtx.Value(ctxKey{}),
		"the context must reach the implementation unchanged")
	assert.Equal(t, MixCoord(coord), engine.seenCoord,
		"the MixCoord passed to Start must reach the implementation unchanged")

	pct, err := engine.seenCoord.GetReplicaLoadPercentByRG(ctx, 1, "rg-a")
	assert.NoError(t, err)
	assert.Equal(t, int32(-1), pct,
		"-1 must survive the interface: it means no replica in this resource group, which is not 0")

	readiness, err := engine.seenCoord.GetShardLeadersByRG(ctx, 1, "rg-a")
	assert.NoError(t, err)
	assert.False(t, readiness.Ready,
		"a not-ready verdict must survive the interface rather than degrading to the zero value of a bool nobody set")
	assert.Equal(t, ShardLeadersReasonShardsWithoutLeader, readiness.Reason,
		"the reason a resource group is not ready must survive the interface, or the caller can only log that it is not")
	assert.Equal(t, 2, readiness.TotalShards)
	assert.Equal(t, []string{"coll-dmc1"}, readiness.UnreadyShards,
		"the shards that are missing a leader must survive the interface")
}

func TestCoordinatorEngineLifecycleErrorsArePropagated(t *testing.T) {
	ResetForTest()
	t.Cleanup(ResetForTest)

	wantStartErr := errors.New("engine failed to start")
	wantStopErr := errors.New("engine failed to stop")
	engine := &fakeCoordinatorEngine{startErr: wantStartErr, stopErr: wantStopErr}
	assert.NoError(t, SetProvider(fakeProvider{name: "testprovider", caps: Capabilities{CoordinatorEngine: engine}}))

	startErr := Caps().CoordinatorEngine.Start(context.Background(), fakeMixCoord{})
	assert.ErrorIs(t, startErr, wantStartErr,
		"an error from Start must survive install, Caps, and the call unwrapped and unreplaced")

	stopErr := Caps().CoordinatorEngine.Stop()
	assert.ErrorIs(t, stopErr, wantStopErr,
		"an error from Stop must survive install, Caps, and the call unwrapped and unreplaced")
	assert.True(t, engine.stopped)
}
