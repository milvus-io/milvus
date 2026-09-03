package coordinator

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/querycoordv2"
	"github.com/milvus-io/milvus/pkg/v3/extension"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// recordingInterceptor records what each resource-group hook saw and answers
// what the test told it to answer.
type recordingInterceptor struct {
	createReplacement *milvuspb.CreateResourceGroupRequest
	update            extension.ResourceGroupUpdate
	updateErr         error

	seenCreate     *milvuspb.CreateResourceGroupRequest
	seenUpdate     *querypb.UpdateResourceGroupsRequest
	seenDrop       *milvuspb.DropResourceGroupRequest
	seenAfter      []extension.ResourceGroupUpdate
	seenFailedDrop []*milvuspb.DropResourceGroupRequest
}

func (i *recordingInterceptor) BeforeCreateResourceGroup(_ context.Context, req *milvuspb.CreateResourceGroupRequest) *milvuspb.CreateResourceGroupRequest {
	i.seenCreate = req
	return i.createReplacement
}

func (i *recordingInterceptor) BeforeUpdateResourceGroups(_ context.Context, req *querypb.UpdateResourceGroupsRequest) (extension.ResourceGroupUpdate, error) {
	i.seenUpdate = req
	return i.update, i.updateErr
}

func (i *recordingInterceptor) AfterUpdateResourceGroups(_ context.Context, update extension.ResourceGroupUpdate) {
	i.seenAfter = append(i.seenAfter, update)
}

func (i *recordingInterceptor) AfterDropResourceGroupFailed(_ context.Context, req *milvuspb.DropResourceGroupRequest) {
	i.seenFailedDrop = append(i.seenFailedDrop, req)
}

func (i *recordingInterceptor) BeforeDropResourceGroup(_ context.Context, req *milvuspb.DropResourceGroupRequest) {
	i.seenDrop = req
}

type seamProvider struct{ caps extension.Capabilities }

func (seamProvider) Name() string                           { return "test" }
func (seamProvider) Requires() []extension.CapabilityID     { return nil }
func (p seamProvider) Capabilities() extension.Capabilities { return p.caps }

func installCaps(t *testing.T, caps extension.Capabilities) {
	t.Helper()
	extension.ResetForTest()
	t.Cleanup(extension.ResetForTest)
	require.NoError(t, extension.SetProvider(seamProvider{caps: caps}))
}

func installInterceptor(t *testing.T, interceptor extension.ResourceGroupInterceptor) {
	t.Helper()
	installCaps(t, extension.Capabilities{ResourceGroups: interceptor})
}

// TestResourceGroupSeamIsInertWithoutProvider is the zero-behavior-change
// proof for the three resource-group call sites: querycoord receives the very
// request objects the caller passed in, and its answer reaches the caller
// unchanged.
func TestResourceGroupSeamIsInertWithoutProvider(t *testing.T) {
	extension.ResetForTest()
	t.Cleanup(extension.ResetForTest)

	mockey.PatchConvey("no provider installed", t, func() {
		var (
			createSeen *milvuspb.CreateResourceGroupRequest
			updateSeen *querypb.UpdateResourceGroupsRequest
			dropSeen   *milvuspb.DropResourceGroupRequest
		)
		mockey.Mock((*querycoordv2.Server).CreateResourceGroup).To(func(_ *querycoordv2.Server, _ context.Context, req *milvuspb.CreateResourceGroupRequest) (*commonpb.Status, error) {
			createSeen = req
			return merr.Success(), nil
		}).Build()
		mockey.Mock((*querycoordv2.Server).UpdateResourceGroups).To(func(_ *querycoordv2.Server, _ context.Context, req *querypb.UpdateResourceGroupsRequest) (*commonpb.Status, error) {
			updateSeen = req
			return merr.Success(), nil
		}).Build()
		mockey.Mock((*querycoordv2.Server).DropResourceGroup).To(func(_ *querycoordv2.Server, _ context.Context, req *milvuspb.DropResourceGroupRequest) (*commonpb.Status, error) {
			dropSeen = req
			return merr.Success(), nil
		}).Build()

		s := &mixCoordImpl{queryCoordServer: &querycoordv2.Server{}}
		createReq := &milvuspb.CreateResourceGroupRequest{ResourceGroup: "rg"}
		updateReq := &querypb.UpdateResourceGroupsRequest{}
		dropReq := &milvuspb.DropResourceGroupRequest{ResourceGroup: "rg"}

		status, err := s.CreateResourceGroup(context.Background(), createReq)
		require.NoError(t, err)
		assert.True(t, merr.Ok(status))
		assert.Same(t, createReq, createSeen, "querycoord must create exactly the request it was given")

		status, err = s.UpdateResourceGroups(context.Background(), updateReq)
		require.NoError(t, err)
		assert.True(t, merr.Ok(status))
		assert.Same(t, updateReq, updateSeen, "querycoord must apply exactly the request it was given")

		status, err = s.DropResourceGroup(context.Background(), dropReq)
		require.NoError(t, err)
		assert.True(t, merr.Ok(status))
		assert.Same(t, dropReq, dropSeen, "querycoord must drop exactly the request it was given")
	})
}

func TestCreateResourceGroupUsesInterceptorReplacement(t *testing.T) {
	mockey.PatchConvey("replacement request is what gets created", t, func() {
		replacement := &milvuspb.CreateResourceGroupRequest{ResourceGroup: "rg-sanitized"}
		interceptor := &recordingInterceptor{createReplacement: replacement}
		installInterceptor(t, interceptor)

		var seen *milvuspb.CreateResourceGroupRequest
		mockey.Mock((*querycoordv2.Server).CreateResourceGroup).To(func(_ *querycoordv2.Server, _ context.Context, req *milvuspb.CreateResourceGroupRequest) (*commonpb.Status, error) {
			seen = req
			return merr.Success(), nil
		}).Build()

		s := &mixCoordImpl{queryCoordServer: &querycoordv2.Server{}}
		original := &milvuspb.CreateResourceGroupRequest{ResourceGroup: "rg-raw"}
		_, err := s.CreateResourceGroup(context.Background(), original)
		require.NoError(t, err)
		assert.Same(t, original, interceptor.seenCreate, "the interceptor must see the caller's request")
		assert.Same(t, replacement, seen, "querycoord must create the replacement, not the original")
	})
}

func TestCreateResourceGroupKeepsOriginalWhenInterceptorSuppliesNone(t *testing.T) {
	mockey.PatchConvey("nil replacement keeps the original request", t, func() {
		installInterceptor(t, &recordingInterceptor{})

		var seen *milvuspb.CreateResourceGroupRequest
		mockey.Mock((*querycoordv2.Server).CreateResourceGroup).To(func(_ *querycoordv2.Server, _ context.Context, req *milvuspb.CreateResourceGroupRequest) (*commonpb.Status, error) {
			seen = req
			return merr.Success(), nil
		}).Build()

		s := &mixCoordImpl{queryCoordServer: &querycoordv2.Server{}}
		original := &milvuspb.CreateResourceGroupRequest{ResourceGroup: "rg-raw"}
		_, err := s.CreateResourceGroup(context.Background(), original)
		require.NoError(t, err)
		assert.Same(t, original, seen, "an observing interceptor must not have to echo the request back")
	})
}

func TestDropResourceGroupReachesInterceptorBeforeQueryCoord(t *testing.T) {
	mockey.PatchConvey("interceptor runs while the group still exists", t, func() {
		interceptor := &recordingInterceptor{}
		installInterceptor(t, interceptor)

		var interceptedBeforeDrop bool
		mockey.Mock((*querycoordv2.Server).DropResourceGroup).To(func(_ *querycoordv2.Server, _ context.Context, _ *milvuspb.DropResourceGroupRequest) (*commonpb.Status, error) {
			interceptedBeforeDrop = interceptor.seenDrop != nil
			return merr.Success(), nil
		}).Build()

		s := &mixCoordImpl{queryCoordServer: &querycoordv2.Server{}}
		req := &milvuspb.DropResourceGroupRequest{ResourceGroup: "rg"}
		_, err := s.DropResourceGroup(context.Background(), req)
		require.NoError(t, err)
		assert.Same(t, req, interceptor.seenDrop)
		assert.True(t, interceptedBeforeDrop, "the interceptor must run before querycoord drops the group")
		assert.Empty(t, interceptor.seenFailedDrop, "a committed drop must not be reported as failed")
	})
}

// A drop the coordinator refused is reported back: the interceptor's teardown
// already happened and cannot be undone, and without the report the group
// milvus still holds and the group the interceptor emptied diverge silently.
func TestDropResourceGroupFailureIsReportedToInterceptor(t *testing.T) {
	mockey.PatchConvey("querycoord refuses the drop", t, func() {
		interceptor := &recordingInterceptor{}
		installInterceptor(t, interceptor)
		mockey.Mock((*querycoordv2.Server).DropResourceGroup).Return(
			merr.Status(merr.WrapErrServiceInternal("drop refused")), nil).Build()

		s := &mixCoordImpl{queryCoordServer: &querycoordv2.Server{}}
		req := &milvuspb.DropResourceGroupRequest{ResourceGroup: "rg"}
		_, err := s.DropResourceGroup(context.Background(), req)
		require.NoError(t, err)
		require.Len(t, interceptor.seenFailedDrop, 1)
		assert.Same(t, req, interceptor.seenFailedDrop[0])
	})

	mockey.PatchConvey("querycoord call errors", t, func() {
		interceptor := &recordingInterceptor{}
		installInterceptor(t, interceptor)
		mockey.Mock((*querycoordv2.Server).DropResourceGroup).Return(nil, errors.New("unreachable")).Build()

		s := &mixCoordImpl{queryCoordServer: &querycoordv2.Server{}}
		_, err := s.DropResourceGroup(context.Background(), &milvuspb.DropResourceGroupRequest{ResourceGroup: "rg"})
		require.Error(t, err)
		require.Len(t, interceptor.seenFailedDrop, 1,
			"an errored drop is reported failed; if it committed after all, the group is gone and the report is a harmless warning")
	})
}

func TestUpdateResourceGroupsErrorAbortsBeforeQueryCoord(t *testing.T) {
	mockey.PatchConvey("interceptor error stops the update", t, func() {
		interceptor := &recordingInterceptor{updateErr: errors.New("ttl target missing")}
		installInterceptor(t, interceptor)

		forwarded := false
		mockey.Mock((*querycoordv2.Server).UpdateResourceGroups).To(func(_ *querycoordv2.Server, _ context.Context, _ *querypb.UpdateResourceGroupsRequest) (*commonpb.Status, error) {
			forwarded = true
			return merr.Success(), nil
		}).Build()

		s := &mixCoordImpl{queryCoordServer: &querycoordv2.Server{}}
		status, err := s.UpdateResourceGroups(context.Background(), &querypb.UpdateResourceGroupsRequest{})
		require.NoError(t, err, "a refused update is reported in the status, not as a transport error")
		assert.False(t, merr.Ok(status))
		assert.Contains(t, status.GetReason(), "ttl target missing")
		assert.False(t, forwarded, "querycoord must not apply an update the interceptor refused")
		assert.Empty(t, interceptor.seenAfter, "nothing committed, so nothing to report afterwards")
	})
}

func TestUpdateResourceGroupsAppliedSkipsQueryCoord(t *testing.T) {
	mockey.PatchConvey("interceptor already applied the update", t, func() {
		interceptor := &recordingInterceptor{update: extension.ResourceGroupUpdate{Applied: true, FollowUpGroups: []string{"rg"}}}
		installInterceptor(t, interceptor)

		forwarded := false
		mockey.Mock((*querycoordv2.Server).UpdateResourceGroups).To(func(_ *querycoordv2.Server, _ context.Context, _ *querypb.UpdateResourceGroupsRequest) (*commonpb.Status, error) {
			forwarded = true
			return merr.Success(), nil
		}).Build()

		s := &mixCoordImpl{queryCoordServer: &querycoordv2.Server{}}
		status, err := s.UpdateResourceGroups(context.Background(), &querypb.UpdateResourceGroupsRequest{})
		require.NoError(t, err)
		assert.True(t, merr.Ok(status), "an update the interceptor carried out is a success for the caller")
		assert.False(t, forwarded, "querycoord must not see an update that was already applied")
		assert.Empty(t, interceptor.seenAfter, "querycoord committed nothing, so the follow-up must not run")
	})
}

func TestUpdateResourceGroupsForwardsReplacementThenReports(t *testing.T) {
	mockey.PatchConvey("replacement is applied and the commit is reported", t, func() {
		replacement := &querypb.UpdateResourceGroupsRequest{}
		update := extension.ResourceGroupUpdate{Forward: replacement, FollowUpGroups: []string{"rg-1", "rg-2"}}
		interceptor := &recordingInterceptor{update: update}
		installInterceptor(t, interceptor)

		var (
			seen                *querypb.UpdateResourceGroupsRequest
			reportedBeforeApply bool
		)
		mockey.Mock((*querycoordv2.Server).UpdateResourceGroups).To(func(_ *querycoordv2.Server, _ context.Context, req *querypb.UpdateResourceGroupsRequest) (*commonpb.Status, error) {
			seen = req
			reportedBeforeApply = len(interceptor.seenAfter) > 0
			return merr.Success(), nil
		}).Build()

		s := &mixCoordImpl{queryCoordServer: &querycoordv2.Server{}}
		original := &querypb.UpdateResourceGroupsRequest{}
		status, err := s.UpdateResourceGroups(context.Background(), original)
		require.NoError(t, err)
		assert.True(t, merr.Ok(status))
		assert.Same(t, original, interceptor.seenUpdate, "the interceptor must see the caller's request")
		assert.Same(t, replacement, seen, "querycoord must apply the replacement, not the original")
		assert.False(t, reportedBeforeApply, "the follow-up must not run before the update commits")
		require.Len(t, interceptor.seenAfter, 1)
		assert.Equal(t, []string{"rg-1", "rg-2"}, interceptor.seenAfter[0].FollowUpGroups,
			"the follow-up must be handed back what the interceptor decided before the commit")
	})
}

func TestUpdateResourceGroupsRejectedByQueryCoordSkipsFollowUp(t *testing.T) {
	mockey.PatchConvey("failed commit reports nothing", t, func() {
		interceptor := &recordingInterceptor{update: extension.ResourceGroupUpdate{FollowUpGroups: []string{"rg-1"}}}
		installInterceptor(t, interceptor)

		mockey.Mock((*querycoordv2.Server).UpdateResourceGroups).Return(
			merr.Status(merr.WrapErrServiceInternal("querycoord refused")), nil,
		).Build()

		s := &mixCoordImpl{queryCoordServer: &querycoordv2.Server{}}
		status, err := s.UpdateResourceGroups(context.Background(), &querypb.UpdateResourceGroupsRequest{})
		require.NoError(t, err)
		assert.False(t, merr.Ok(status), "querycoord's refusal must reach the caller")
		assert.Empty(t, interceptor.seenAfter, "an update that did not commit must not be reported as one that did")
	})
}

func TestUpdateResourceGroupsTransportErrorSkipsFollowUp(t *testing.T) {
	mockey.PatchConvey("failed call reports nothing", t, func() {
		interceptor := &recordingInterceptor{update: extension.ResourceGroupUpdate{FollowUpGroups: []string{"rg-1"}}}
		installInterceptor(t, interceptor)

		mockey.Mock((*querycoordv2.Server).UpdateResourceGroups).Return(nil, errors.New("querycoord unreachable")).Build()

		s := &mixCoordImpl{queryCoordServer: &querycoordv2.Server{}}
		_, err := s.UpdateResourceGroups(context.Background(), &querypb.UpdateResourceGroupsRequest{})
		require.Error(t, err)
		assert.Empty(t, interceptor.seenAfter, "an update whose outcome is unknown must not be reported as committed")
	})
}
