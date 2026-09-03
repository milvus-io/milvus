//go:build test

package querycoordv2

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/querycoordv2/job"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/v3/extension"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// recordingScope is an extension.LoadPlacementScope that answers what the test
// told it to and reports what it was asked.
type recordingScope struct {
	answer     bool
	calls      int
	collection int64
	groups     []string
}

func (r *recordingScope) ScopedToNamedResourceGroups(_ context.Context, collectionID int64, resourceGroups []string) bool {
	r.calls++
	r.collection = collectionID
	r.groups = resourceGroups
	return r.answer
}

type scopeOnlyProvider struct{ scope extension.LoadPlacementScope }

func (scopeOnlyProvider) Name() string { return "test-scoped-load-form" }

func (scopeOnlyProvider) Requires() []extension.CapabilityID {
	return []extension.CapabilityID{extension.CapLoadPlacementScope}
}

func (p scopeOnlyProvider) Capabilities() extension.Capabilities {
	return extension.Capabilities{LoadPlacement: p.scope}
}

// installScope installs a provider carrying only the load-placement scope, and
// removes it again when the test ends.
func installScope(t *testing.T, scope extension.LoadPlacementScope) {
	t.Helper()
	extension.ResetForTest()
	t.Cleanup(extension.ResetForTest)
	require.NoError(t, extension.SetProvider(scopeOnlyProvider{scope: scope}))
}

// loadedIn builds a CurrentLoadConfig holding one replica per named resource
// group, which is the shape a form that loads per resource group produces.
func loadedIn(collectionID int64, resourceGroups ...string) job.CurrentLoadConfig {
	replicas := make(map[int64]*meta.Replica, len(resourceGroups))
	for i, rgName := range resourceGroups {
		id := int64(i + 1)
		replicas[id] = meta.NewReplica(&querypb.Replica{
			ID:            id,
			CollectionID:  collectionID,
			ResourceGroup: rgName,
		})
	}
	return job.CurrentLoadConfig{Replicas: replicas}
}

// The capability-nil guarantee for this seam: with no provider installed the
// placement is whatever AssignReplica produced, and nothing is consulted.
func TestCompletePlacementWithoutProviderReturnsInput(t *testing.T) {
	extension.ResetForTest()
	t.Cleanup(extension.ResetForTest)

	expected := map[string]int{"rg_1": 1}
	got := completePlacementForOutOfScopeResourceGroups(
		context.Background(), 7, []string{"rg_1"}, expected, loadedIn(7, "rg_0"))

	assert.Equal(t, map[string]int{"rg_1": 1}, got,
		"a sibling resource group's replica must not be carried over natively")
}

// An installed capability that answers false is the native reading, so it must
// produce the native result too.
func TestCompletePlacementWhenScopeSaysWholePlacement(t *testing.T) {
	scope := &recordingScope{answer: false}
	installScope(t, scope)

	expected := map[string]int{"rg_1": 1}
	got := completePlacementForOutOfScopeResourceGroups(
		context.Background(), 7, []string{"rg_1"}, expected, loadedIn(7, "rg_0"))

	assert.Equal(t, map[string]int{"rg_1": 1}, got)
	assert.Equal(t, 1, scope.calls, "the capability must be the thing that decided")
}

// The defect this seam exists for: a request naming one resource group must
// leave the collection's replica in the sibling resource group in place.
func TestCompletePlacementKeepsOutOfScopeResourceGroups(t *testing.T) {
	scope := &recordingScope{answer: true}
	installScope(t, scope)

	expected := map[string]int{"rg_1": 1}
	got := completePlacementForOutOfScopeResourceGroups(
		context.Background(), 7, []string{"rg_1"}, expected, loadedIn(7, "rg_0"))

	assert.Equal(t, map[string]int{"rg_0": 1, "rg_1": 1}, got,
		"the resource group the request did not name keeps the replica it holds")
	assert.Equal(t, map[string]int{"rg_1": 1}, expected,
		"the map AssignReplica returned must not be mutated")
	assert.Equal(t, int64(7), scope.collection)
	assert.Equal(t, []string{"rg_1"}, scope.groups,
		"the capability must see the resolved resource group list")
}

// The count for a named resource group is the request's, not the stored one:
// naming a resource group is exactly how a caller changes what it holds.
func TestCompletePlacementDoesNotOverrideNamedResourceGroups(t *testing.T) {
	scope := &recordingScope{answer: true}
	installScope(t, scope)

	current := loadedIn(7, "rg_0", "rg_1")
	got := completePlacementForOutOfScopeResourceGroups(
		context.Background(), 7, []string{"rg_1"}, map[string]int{"rg_1": 2}, current)

	assert.Equal(t, map[string]int{"rg_0": 1, "rg_1": 2}, got)
}

// A first load has nothing to keep, and must come out byte for byte as the
// request asked - including for a form whose scope always answers true.
func TestCompletePlacementOnFirstLoadIsUnchanged(t *testing.T) {
	scope := &recordingScope{answer: true}
	installScope(t, scope)

	expected := map[string]int{"rg_0": 1}
	got := completePlacementForOutOfScopeResourceGroups(
		context.Background(), 7, []string{"rg_0"}, expected, job.CurrentLoadConfig{})

	assert.Equal(t, map[string]int{"rg_0": 1}, got)
}

// Re-loading the only resource group that holds the collection changes nothing,
// which is what makes a repeated load a no-op rather than a rewrite.
func TestCompletePlacementWhenRequestNamesEveryLoadedResourceGroup(t *testing.T) {
	scope := &recordingScope{answer: true}
	installScope(t, scope)

	got := completePlacementForOutOfScopeResourceGroups(
		context.Background(), 7, []string{"rg_0"}, map[string]int{"rg_0": 1}, loadedIn(7, "rg_0"))

	assert.Equal(t, map[string]int{"rg_0": 1}, got)
}

// Several siblings are all kept, not just the first one found.
func TestCompletePlacementKeepsEverySibling(t *testing.T) {
	scope := &recordingScope{answer: true}
	installScope(t, scope)

	got := completePlacementForOutOfScopeResourceGroups(
		context.Background(), 7, []string{"rg_2"}, map[string]int{"rg_2": 1},
		loadedIn(7, "rg_0", "rg_1"))

	assert.Equal(t, map[string]int{"rg_0": 1, "rg_1": 1, "rg_2": 1}, got)
}

// Two replicas of the collection in one out-of-scope resource group are kept as
// two: carrying back a different number would itself be a placement change.
func TestCompletePlacementKeepsSiblingReplicaCount(t *testing.T) {
	scope := &recordingScope{answer: true}
	installScope(t, scope)

	got := completePlacementForOutOfScopeResourceGroups(
		context.Background(), 7, []string{"rg_1"}, map[string]int{"rg_1": 1},
		loadedIn(7, "rg_0", "rg_0"))

	assert.Equal(t, map[string]int{"rg_0": 2, "rg_1": 1}, got)
}

// stubBroadcaster stands in for the WAL broadcaster so the call-site test never
// touches streaming.
type stubBroadcaster struct{}

func (stubBroadcaster) Broadcast(context.Context, message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error) {
	return nil, nil
}
func (stubBroadcaster) Close() {}

// The seam is only worth anything if the load path uses what it returns.
// Everything above tests the function; this tests the wire. It captures the
// request the broadcast is built from and asserts the sibling resource group is
// in the placement it carries - so computing the completed placement and then
// dropping it on the floor fails here even though every other test still
// passes.
func TestLoadCollectionBroadcastAppliesTheCompletedPlacement(t *testing.T) {
	paramtable.Init()
	installScope(t, &recordingScope{answer: true})

	const collectionID = int64(7)
	broker := meta.NewMockBroker(t)
	broker.EXPECT().DescribeCollection(mock.Anything, collectionID).
		Return(&milvuspb.DescribeCollectionResponse{CollectionID: collectionID}, nil).Maybe()
	broker.EXPECT().GetPartitions(mock.Anything, collectionID).Return([]int64{1}, nil).Maybe()
	s := &Server{broker: broker}

	var captured *job.AlterLoadConfigRequest
	mockey.PatchConvey("a load naming one resource group carries the other's placement", t, func() {
		mockey.Mock((*Server).startBroadcastWithCollectionIDLock).
			Return(stubBroadcaster{}, nil).Build()
		mockey.Mock(utils.AssignReplica).Return(map[string]int{"rg_1": 1}, nil).Build()
		mockey.Mock((*Server).getCurrentLoadConfig).Return(loadedIn(collectionID, "rg_0")).Build()
		// Returning a nil message is the "load config unchanged" answer, which
		// ends the call before it broadcasts. The request is already built by
		// then, which is all this test is after.
		mockey.Mock(job.GenerateAlterLoadConfigMessage).To(
			func(_ context.Context, req *job.AlterLoadConfigRequest) (message.BroadcastMutableMessage, error) {
				captured = req
				return nil, nil
			}).Build()

		err := s.broadcastAlterLoadConfigCollectionV2ForLoadCollection(context.Background(),
			&querypb.LoadCollectionRequest{
				CollectionID:   collectionID,
				ReplicaNumber:  1,
				ResourceGroups: []string{"rg_1"},
			})
		assert.NoError(t, err)
	})

	require.NotNil(t, captured, "the broadcast request must have been built")
	assert.Equal(t, map[string]int{"rg_0": 1, "rg_1": 1},
		captured.Expected.ExpectedReplicaNumber,
		"the load path must broadcast the completed placement, not the one it asked for")
}
