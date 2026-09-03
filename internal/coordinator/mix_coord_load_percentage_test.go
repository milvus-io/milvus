package coordinator

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/querycoordv2"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// TestGetLoadPercentageByResourceGroupReachesQueryCoord asserts the
// coordinator's per-resource-group load progress is answered by querycoord
// itself, not by a stub: a Server whose read stores are not wired up refuses
// with ErrServiceNotReady rather than panicking, which is querycoord's own
// documented behavior for that state. The Server is marked healthy first, so
// the refusal comes from the computation layer rather than from the health
// gate in front of it - that is what proves the call went all the way in.
func TestGetLoadPercentageByResourceGroupReachesQueryCoord(t *testing.T) {
	qc := &querycoordv2.Server{}
	// The per-resource-group entry points are gated on querycoord's OWN health,
	// not on the mixcoord's: an uninitialized Server refuses rather than
	// dereferencing stores it has not built yet.
	qc.UpdateStateCode(commonpb.StateCode_Healthy)
	s := &mixCoordImpl{queryCoordServer: qc}
	s.UpdateStateCode(commonpb.StateCode_Healthy)

	percentage, err := s.GetLoadPercentageByResourceGroup(context.Background(), 1, "rg-target")
	assert.ErrorIs(t, err, merr.ErrServiceNotReady)
	assert.EqualValues(t, -1, percentage, "the error path still reports the no-replica sentinel")
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
