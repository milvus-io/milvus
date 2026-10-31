package coordview

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
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
