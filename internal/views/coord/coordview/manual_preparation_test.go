package coordview

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestManualPreparationExactOutcome(t *testing.T) {
	for _, scenario := range []string{"up then replaced", "preempted", "released", "node failure", "canceled", "shutdown"} {
		t.Run(scenario, func(t *testing.T) {
			catalog, syncer := newMockCatalog(), newMockSyncer()
			mgr := newTestManager(t, catalog, syncer)
			require.NoError(t, mgr.AddPreparing(t.Context(), testBuilder(1, 1, 1)))
			builder := mgr.PlacementBuilder()
			require.NotNil(t, builder)
			p, err := mgr.AddManualPreparing(t.Context(), builder, testVersion(1, 1, 1))
			require.NoError(t, err)
			mgr.waitFlush()
			require.Equal(t, testVersion(1, 1, 2), p.Version)
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			switch scenario {
			case "up then replaced":
				simulateNodeResponse(t, syncer, testQN1, p.Version, qviews.QueryViewStateReady)
				simulateNodeResponse(t, syncer, testSN, p.Version, qviews.QueryViewStateReady)
				simulateNodeResponse(t, syncer, testSN, p.Version, qviews.QueryViewStateUp)
				require.NoError(t, mgr.AddPreparing(t.Context(), testBuilder(1, 1, 1)))
				v := testVersion(1, 1, 3)
				simulateNodeResponse(t, syncer, testQN1, v, qviews.QueryViewStateReady)
				simulateNodeResponse(t, syncer, testSN, v, qviews.QueryViewStateReady)
				simulateNodeResponse(t, syncer, testSN, v, qviews.QueryViewStateUp)
				cancel() // Completed outcomes win over later caller cancellation.
				result := p.Wait(ctx)
				require.NoError(t, result.Err)
				require.Equal(t, qviews.QueryViewStateUp, result.State)
				return
			case "preempted":
				require.NoError(t, mgr.AddPreparing(t.Context(), testBuilder(1, 1, 1)))
			case "released":
				require.NoError(t, mgr.RequestRelease(t.Context()))
			case "node failure":
				simulateNodeResponse(t, syncer, testQN1, p.Version, qviews.QueryViewStateUnrecoverable)
			case "canceled":
				cancel()
				require.ErrorIs(t, p.Wait(ctx).Err, context.Canceled)
				require.Equal(t, p.Version, *mgr.Stats().PreparingVersion, "cancel only stops waiting")
				return
			case "shutdown":
				mgr.abortRecovery()
			}
			result := p.Wait(ctx)
			require.ErrorIs(t, result.Err, merr.ErrServiceUnavailable)
			if scenario != "shutdown" {
				require.Equal(t, qviews.QueryViewStateUnrecoverable, result.State)
			}
		})
	}
}

func TestManualPreparationRejectsStaleSource(t *testing.T) {
	mgr := newTestManager(t, newMockCatalog(), newMockSyncer())
	require.NoError(t, mgr.AddPreparing(t.Context(), testBuilder(1, 1, 1)))
	builder := mgr.PlacementBuilder()
	require.NotNil(t, builder)
	// A normal reconcile accepted a different placement after the snapshot.
	require.NoError(t, mgr.AddPreparing(t.Context(), testBuilder(1, 1, 1)))
	_, err := mgr.AddManualPreparing(t.Context(), builder, testVersion(1, 1, 1))
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	require.Equal(t, testVersion(1, 1, 2), *mgr.Stats().PreparingVersion)
}

func TestManualPreparationRechecksSourceAfterReference(t *testing.T) {
	mgr := newTestManager(t, newMockCatalog(), newMockSyncer())
	require.NoError(t, mgr.AddPreparing(t.Context(), testBuilder(1, 1, 1)))
	builder := mgr.PlacementBuilder()
	acquisitions, released := 0, 0
	get := mockey.Mock(stubDataViewRefProvider.Get).To(func(_ stubDataViewRefProvider, ctx context.Context, _ int64, version *viewpb.DataVersion) (qviews.DataViewRef, error) {
		dv := qviews.FromProtoDataVersion(version)
		acquisitions++
		if acquisitions == 1 {
			// Acquisition runs without the manager lock. A concurrent acceptance can
			// advance the source before the outer operation reacquires the lock.
			require.NoError(t, mgr.AddPreparing(ctx, testBuilder(1, 1, 1)))
		}
		return stubDataViewRef{version: dv}, nil
	}).Build()
	defer get.UnPatch()
	deref := mockey.Mock(stubDataViewRef.Deref).To(func(stubDataViewRef) { released++ }).Build()
	defer deref.UnPatch()
	_, err := mgr.AddManualPreparing(t.Context(), builder, testVersion(1, 1, 1))
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	require.Equal(t, 2, acquisitions)
	require.Equal(t, 1, released, "rejected preparation returns its acquired reference")
	require.Equal(t, testVersion(1, 1, 2), *mgr.Stats().PreparingVersion)
}

func TestManualPreparationRegistryClose(t *testing.T) {
	reg := newTestRegistry(t, newMockCatalog(), newMockSyncer())
	mgr := reg.Ensure(testShardID)
	require.NoError(t, mgr.AddPreparing(t.Context(), testBuilder(1, 1, 1)))
	p, err := mgr.AddManualPreparing(t.Context(), mgr.PlacementBuilder(), testVersion(1, 1, 1))
	require.NoError(t, err)
	reg.Close()
	require.Nil(t, mgr.PlacementBuilder())
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	require.ErrorIs(t, p.Wait(ctx).Err, merr.ErrServiceUnavailable)
	require.ErrorIs(t, mgr.AddPreparing(ctx, testBuilder(1, 1, 1)), merr.ErrServiceUnavailable)
}

func TestManualPreparationPendingWait(t *testing.T) {
	for _, shutdown := range []bool{false, true} {
		t.Run(map[bool]string{false: "serving", true: "lifetime canceled"}[shutdown], func(t *testing.T) {
			catalog, syncer := newMockCatalog(), newMockSyncer()
			mgr := newTestManager(t, catalog, syncer)
			lifetime, cancel := context.WithCancel(mgr.ctx)
			defer cancel()
			mgr.ctx = lifetime
			require.NoError(t, mgr.AddPreparing(t.Context(), testBuilder(1, 1, 1)))
			p, err := mgr.AddManualPreparing(t.Context(), mgr.PlacementBuilder(), testVersion(1, 1, 1))
			require.NoError(t, err)
			mgr.waitFlush()
			ctx, stop := context.WithTimeout(t.Context(), time.Second)
			defer stop()
			result := make(chan PreparationResult, 1)
			go func() { result <- p.Wait(ctx) }()
			select {
			case outcome := <-result:
				t.Fatalf("preparation completed before serving or shutdown: %+v", outcome)
			case <-time.After(10 * time.Millisecond):
			}
			if shutdown {
				cancel()
				require.ErrorIs(t, (<-result).Err, merr.ErrServiceUnavailable)
			} else {
				simulateNodeResponse(t, syncer, testQN1, p.Version, qviews.QueryViewStateReady)
				simulateNodeResponse(t, syncer, testSN, p.Version, qviews.QueryViewStateReady)
				simulateNodeResponse(t, syncer, testSN, p.Version, qviews.QueryViewStateUp)
				outcome := <-result
				require.NoError(t, outcome.Err)
				require.Equal(t, qviews.QueryViewStateUp, outcome.State)
			}
		})
	}
}
