package coordinator

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"

	"github.com/milvus-io/milvus/internal/querycoordv2"
)

// TestGetLoadPercentageByResourceGroupReachesQueryCoord asserts the
// coordinator's per-resource-group load progress is answered by querycoord
// itself, not by a stub: a Server that has not been initialized answers -1
// ("no replica of this collection lives in this resource group") without
// panicking, which is querycoord's own documented behavior for that state.
func TestGetLoadPercentageByResourceGroupReachesQueryCoord(t *testing.T) {
	s := &mixCoordImpl{queryCoordServer: &querycoordv2.Server{}}
	s.UpdateStateCode(commonpb.StateCode_Healthy)

	percentage, err := s.GetLoadPercentageByResourceGroup(context.Background(), 1, "rg-target")
	assert.NoError(t, err)
	assert.EqualValues(t, -1, percentage)
}

// TestGetLoadPercentageByResourceGroupForwardsArgumentsAndResult asserts the
// collection id, the resource group name, the percentage and the error all
// cross the delegation untouched. Answering a different collection or a
// different resource group here would let a caller mark a resource group
// ready on the strength of some other one's progress.
func TestGetLoadPercentageByResourceGroupForwardsArgumentsAndResult(t *testing.T) {
	mockey.PatchConvey("percentage and arguments are forwarded", t, func() {
		var (
			seenCollectionID int64
			seenRG           string
		)
		mockey.Mock((*querycoordv2.Server).GetLoadPercentageByResourceGroup).
			To(func(_ *querycoordv2.Server, _ context.Context, collectionID int64, rgName string) (int32, error) {
				seenCollectionID = collectionID
				seenRG = rgName
				return 63, nil
			}).Build()

		s := &mixCoordImpl{queryCoordServer: &querycoordv2.Server{}}
		s.UpdateStateCode(commonpb.StateCode_Healthy)
		percentage, err := s.GetLoadPercentageByResourceGroup(context.Background(), 42, "rg-a")

		assert.NoError(t, err)
		assert.EqualValues(t, 63, percentage)
		assert.EqualValues(t, 42, seenCollectionID)
		assert.Equal(t, "rg-a", seenRG)
	})

	mockey.PatchConvey("a querycoord error is not swallowed", t, func() {
		want := errors.New("collection failed to load")
		mockey.Mock((*querycoordv2.Server).GetLoadPercentageByResourceGroup).
			Return(int32(-1), want).Build()

		s := &mixCoordImpl{queryCoordServer: &querycoordv2.Server{}}
		s.UpdateStateCode(commonpb.StateCode_Healthy)
		percentage, err := s.GetLoadPercentageByResourceGroup(context.Background(), 42, "rg-a")

		assert.ErrorIs(t, err, want)
		assert.EqualValues(t, -1, percentage)
	})
}

// Before this replica is ACTIVE the per-resource-group questions must fail,
// not answer: an uninitialized querycoord reads as -1 / "nothing ready", which
// a caller cannot tell apart from "this resource group holds no replica".
func TestPerResourceGroupQuestionsAreHealthGated(t *testing.T) {
	s := &mixCoordImpl{queryCoordServer: &querycoordv2.Server{}}

	_, err := s.GetLoadPercentageByResourceGroup(context.Background(), 1, "rg")
	assert.Error(t, err, "a standby or initializing coordinator must refuse, not misreport")

	_, err = s.GetShardLeaderReadinessByResourceGroup(context.Background(), 1, "rg")
	assert.Error(t, err)
}

// OnActive is the activation hook the engine seam hangs off: registered
// before activation it waits, registered after it runs immediately, and on a
// replica that never activates it never runs.
func TestOnActiveRunsCallbacksExactlyOnActivation(t *testing.T) {
	s := &mixCoordImpl{}
	ran := 0
	s.OnActive(func() { ran++ })
	assert.Zero(t, ran, "a standby replica must not run activation work")

	s.onActiveMu.Lock()
	s.activated = true
	fns := s.onActive
	s.onActive = nil
	s.onActiveMu.Unlock()
	for _, fn := range fns {
		fn()
	}
	assert.Equal(t, 1, ran, "activation must run the registered callback")

	s.OnActive(func() { ran++ })
	assert.Equal(t, 2, ran, "registered after activation, the callback runs immediately")
}
