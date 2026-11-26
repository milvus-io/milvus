//go:build test && dynamic

package snview

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/internal/views/viewquery"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

type recoveryWaitRuntime struct {
	mockQueryRuntime
	entered chan struct{}
	visible chan struct{}
}

func (r *recoveryWaitRuntime) WaitMVCCVisible(ctx context.Context, growing, transforming uint64) error {
	_ = r.mockQueryRuntime.WaitMVCCVisible(ctx, growing, transforming)
	close(r.entered)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-r.visible:
		return nil
	}
}

func TestSNHandler_Phase2RecoveryRPC(t *testing.T) {
	for _, operation := range []string{"search", "query"} {
		for _, event := range []string{"ready", "unrecoverable", "canceled", "shutdown"} {
			t.Run(operation+"/"+event, func(t *testing.T) {
				mockResolvePartitions(t)
				view := newFullSNViewWithState(1, viewpb.QueryViewState_QueryViewStateUp, 101)
				mgr := newMockResourceManager()
				runtime := &recoveryWaitRuntime{entered: make(chan struct{}), visible: make(chan struct{})}
				mgr.runtime = runtime
				h := recoverSNQueryViewHandler(context.Background(), testPChannel, newMockCatalog(), mgr, []*viewpb.QueryViewOfShard{view.IntoProto()})
				recovery, ok := mgr.getAcquired(view.QueryViewKey())
				require.True(t, ok)
				// Empty growing results exercise the real RPC server and task
				// provider without needing native segment execution.
				server := viewquery.NewServer(h, nil)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				result := make(chan error, 1)
				go func() {
					id := view.ShardID().IntoProto()
					version := view.Version().IntoProto()
					mvcc := &viewpb.QueryPlanMVCC{GrowingTimetick: 11, TransformingTimetick: 10}
					var err error
					if operation == "search" {
						_, err = server.SearchOnView(ctx, &viewpb.SearchOnViewRequest{
							ShardId: id, Version: version, Mvcc: mvcc,
							LegacyReq: &internalpb.SearchRequest{CollectionID: testCollectionID},
						})
					} else {
						_, err = server.QueryOnView(ctx, &viewpb.QueryOnViewRequest{
							ShardId: id, Version: version, Mvcc: mvcc,
							LegacyReq: &internalpb.RetrieveRequest{CollectionID: testCollectionID},
						})
					}
					result <- err
				}()
				waitForRecoveryWaiter(t, h)
				select {
				case <-runtime.entered:
					t.Fatal("runtime used before recovery completed")
				default:
				}
				switch event {
				case "ready":
					recovery.OnReady()
					select {
					case <-runtime.entered:
					case <-ctx.Done():
						t.Fatal("RPC did not resume after recovery")
					}
					require.EqualValues(t, 11, runtime.growingTimetick)
					require.EqualValues(t, 10, runtime.transformingTimetick)
					select {
					case err := <-result:
						t.Fatalf("RPC skipped MVCC visibility wait: %v", err)
					default:
					}
					close(runtime.visible)
				case "unrecoverable":
					recovery.OnUnrecoverable()
				case "canceled":
					cancel()
				case "shutdown":
					h.StopQueryAcquisition()
				}
				select {
				case err := <-result:
					switch event {
					case "ready":
						require.NoError(t, err)
					case "canceled":
						require.ErrorIs(t, err, context.Canceled)
					default:
						// Check that the real RPC projection preserves retryability.
						wire := viewerror.ConvertViewError(operation, viewerror.NewGRPCStatusFromError(err).Err())
						viewErr := viewerror.AsViewError(wire)
						require.True(t, viewErr.IsRetryable(), "%v", wire)
						if event == "shutdown" {
							require.True(t, viewErr.IsOnShutdown())
						} else {
							require.True(t, viewErr.IsViewInvalidated())
						}
					}
				case <-time.After(5 * time.Second):
					t.Fatal("RPC did not finish")
				}
			})
		}
	}
}
