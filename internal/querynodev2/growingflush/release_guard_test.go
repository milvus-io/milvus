package growingflush

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/mocks/distributed/mock_streaming"
	"github.com/milvus-io/milvus/internal/streamingnode/client/handler/registry"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type stubProvider struct{}

func (stubProvider) GetGrowingFlushSource(int64, *msgpb.MsgPosition) (syncmgr.GrowingFlushSource, syncmgr.GrowingSourceState) {
	return nil, syncmgr.GrowingSourceUnavailable
}

// setWALForTest installs a Local mock and restores the previous accesser.
func setWALForTest(t *testing.T, local *mock_streaming.MockLocal) {
	wal := mock_streaming.NewMockWALAccesser(t)
	wal.EXPECT().Local().Return(local).Maybe()
	previous := streaming.WAL()
	streaming.SetWALForTest(wal)
	t.Cleanup(func() { streaming.SetWALForTest(previous) })
}

// Without a local growing-source provider for the channel no local write buffer
// can owe a flush, so the guard must not even reach the WAL.
func TestCheckReleaseDebtNoProvider(t *testing.T) {
	local := mock_streaming.NewMockLocal(t)
	setWALForTest(t, local)

	assert.NoError(t, CheckReleaseDebt(context.Background(), 100, "ch-no-provider", []int64{1001}))
}

// Degenerate inputs are never a reason to block a release.
func TestCheckReleaseDebtDegenerateInputs(t *testing.T) {
	local := mock_streaming.NewMockLocal(t)
	setWALForTest(t, local)

	assert.NoError(t, CheckReleaseDebt(context.Background(), 100, "ch", nil))
	assert.NoError(t, CheckReleaseDebt(context.Background(), 100, "ch", []int64{0, -1}))
	assert.NoError(t, CheckReleaseDebt(context.Background(), 100, "", []int64{1001}))
	assert.NoError(t, CheckReleaseDebt(context.Background(), 0, "ch", []int64{1001}))
}

func TestCheckReleaseDebtPendingRefusesRelease(t *testing.T) {
	const channel = "ch-pending"
	registration := syncmgr.DefaultGrowingSourceRegistry().Register(channel, stubProvider{})
	defer syncmgr.DefaultGrowingSourceRegistry().Unregister(registration)

	local := mock_streaming.NewMockLocal(t)
	local.EXPECT().PrepareReleaseSegmentsIfLocal(mock.Anything, int64(100), channel, []int64{1001}).
		Return(true, nil).Once()
	setWALForTest(t, local)

	err := CheckReleaseDebt(context.Background(), 100, channel, []int64{1001, 1001, 0})
	assert.Error(t, err)
	// The refusal must stay retryable: the coordinator has to come back once the
	// nudged flush has settled the debt.
	assert.ErrorIs(t, err, merr.ErrServiceUnavailable)
}

func TestCheckReleaseDebtSettledAllowsRelease(t *testing.T) {
	const channel = "ch-settled"
	registration := syncmgr.DefaultGrowingSourceRegistry().Register(channel, stubProvider{})
	defer syncmgr.DefaultGrowingSourceRegistry().Unregister(registration)

	local := mock_streaming.NewMockLocal(t)
	local.EXPECT().PrepareReleaseSegmentsIfLocal(mock.Anything, int64(100), channel, []int64{1001}).
		Return(false, nil).Once()
	setWALForTest(t, local)

	assert.NoError(t, CheckReleaseDebt(context.Background(), 100, channel, []int64{1001}))
}

// Structural unavailability short-circuits to today's behavior; anything else
// (transient, or a real failure) refuses the release so it is retried.
func TestCheckReleaseDebtErrorClassification(t *testing.T) {
	const channel = "ch-errors"
	registration := syncmgr.DefaultGrowingSourceRegistry().Register(channel, stubProvider{})
	defer syncmgr.DefaultGrowingSourceRegistry().Unregister(registration)

	tests := []struct {
		name    string
		err     error
		wantErr bool
	}{
		{"no streaming node deployed", registry.ErrNoStreamingNodeDeployed, false},
		{"no preparer", registry.ErrNoReleaseManualFlushPreparer, false},
		{"channel not available", merr.WrapErrChannelNotAvailable("ch", "not local"), false},
		{"service unavailable", merr.WrapErrServiceUnavailable("transient"), true},
		{"unexpected", errors.New("boom"), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			local := mock_streaming.NewMockLocal(t)
			local.EXPECT().PrepareReleaseSegmentsIfLocal(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
				Return(false, tt.err).Once()
			setWALForTest(t, local)

			err := CheckReleaseDebt(context.Background(), 100, channel, []int64{1001})
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// merr.ErrChannelNotFound must stay OUT of the structural bucket. See the
// comment on IsPrepareStructurallyUnavailable: bufferManager.DropChannel
// detaches the write buffer from the manager map before running the final
// drop, so during that window a live buffer that may still owe a
// growing-source flush answers "channel not found".
func TestChannelNotFoundIsNeverStructural(t *testing.T) {
	err := merr.WrapErrChannelNotFound("ch-detaching")
	assert.False(t, IsPrepareStructurallyUnavailable(err),
		"ChannelNotFound must stay transient: DropChannel detaches a still-live write buffer before closing it")
	assert.False(t, IsPrepareStructurallyUnavailable(errors.Wrap(err, "prepare release")),
		"ChannelNotFound must stay transient even when wrapped")

	// ChannelNotAvailable is the structural sibling and must keep working, so
	// the assertion above cannot be satisfied by a blanket "nothing is
	// structural" regression.
	assert.True(t, IsPrepareStructurallyUnavailable(merr.WrapErrChannelNotAvailable("ch", "not local")))
}

// End-to-end conservative behavior: a progress query that answers
// ChannelNotFound must make CheckReleaseDebt refuse the release.
func TestCheckReleaseDebtChannelNotFoundRefusesRelease(t *testing.T) {
	const channel = "ch-not-found"
	registration := syncmgr.DefaultGrowingSourceRegistry().Register(channel, stubProvider{})
	defer syncmgr.DefaultGrowingSourceRegistry().Unregister(registration)

	local := mock_streaming.NewMockLocal(t)
	local.EXPECT().PrepareReleaseSegmentsIfLocal(mock.Anything, int64(100), channel, []int64{1001}).
		Return(false, merr.WrapErrChannelNotFound(channel)).Once()
	setWALForTest(t, local)

	err := CheckReleaseDebt(context.Background(), 100, channel, []int64{1001})
	assert.Error(t, err, "a detached-but-live write buffer may still owe a flush; the release must be refused")
	assert.ErrorIs(t, err, merr.ErrChannelNotFound)
}
