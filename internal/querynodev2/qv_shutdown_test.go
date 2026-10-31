package querynodev2

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/querynodev2/pipeline"
	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/util"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestQueryViewNodeStopWaitsForMigrationAndDrain(t *testing.T) {
	for _, mode := range []string{"migrated", "timeout", "no_peers", "legacy_cluster_stopping", "rocksmq"} {
		t.Run(mode, func(t *testing.T) {
			mockey.PatchConvey("the node delegates lifecycle to Close and joins cleanup after cancellation", t, func() {
				paramtable.Init()
				params := paramtable.Get()
				require.NoError(t, params.Save(params.QueryNodeCfg.GracefulStopTimeout.Key, "1"))
				require.NoError(t, params.Save(params.QueryNodeCfg.MigrateDataTimeout.Key, "0s"))
				defer params.Reset(params.QueryNodeCfg.GracefulStopTimeout.Key)
				defer params.Reset(params.QueryNodeCfg.MigrateDataTimeout.Key)
				node := NewQueryNode(context.Background(), nil)
				node.UpdateStateCode(commonpb.StateCode_Healthy)
				node.session = &sessionutil.Session{}
				node.SetQueryViewHandler(qnview.NewQNQueryViewHandler(nil))
				mockey.Mock((*sessionutil.Session).GoingStop).Return(nil).Build()
				mockey.Mock((*sessionutil.Session).Stop).Return().Build()
				wal := message.WALNamePulsar
				if mode == "rocksmq" {
					wal = message.WALNameRocksmq
				}
				mockey.Mock(util.MustSelectWALName).Return(wal).Build()
				peers := mockey.Mock((*QueryNode).hasOtherActiveQueryNode).Return(mode != "no_peers" && mode != "legacy_cluster_stopping", nil).Build()
				if mode == "legacy_cluster_stopping" {
					node.pipelineManager = pipeline.NewManager(nil, nil, nil)
					mockey.Mock(mockey.GetMethod(node.pipelineManager, "Num")).Return(1).Build()
				}
				started, migrated, draining, finish := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
				closeError := make(chan error, 1)
				var calls atomic.Int32
				var drainOnce sync.Once
				mockey.Mock((*qnview.QNQueryViewHandler).Close).To(func(_ *qnview.QNQueryViewHandler, ctx context.Context) error {
					if calls.Add(1) == 1 {
						close(started)
						select {
						case <-ctx.Done():
							closeError <- ctx.Err()
							return ctx.Err()
						case <-migrated:
							closeError <- ctx.Err()
						}
					}
					drainOnce.Do(func() { close(draining) })
					<-finish
					return nil
				}).Build()
				var nativeClosed atomic.Bool
				mockey.Mock((*QueryNode).CloseSegcore).To(func(*QueryNode) { nativeClosed.Store(true) }).Build()
				done := make(chan struct{})
				go func() { _ = node.Stop(); close(done) }()
				select {
				case <-started:
				case <-time.After(5 * time.Second):
					t.Fatal("Close was not called")
				}
				if mode == "migrated" {
					// An empty legacy manager must neither cancel QueryView
					// migration nor reject queries before the handler finishes.
					select {
					case err := <-closeError:
						t.Fatalf("migration ended before Coord withdrawal: %v", err)
					case <-time.After(20 * time.Millisecond):
					}
					require.Equal(t, commonpb.StateCode_Healthy, node.lifetime.GetState())
					close(migrated)
				}
				select {
				case <-draining:
				case <-time.After(5 * time.Second):
					t.Fatal("cleanup was not joined")
				}
				require.False(t, nativeClosed.Load())
				switch mode {
				case "migrated":
					require.NoError(t, <-closeError)
				case "rocksmq", "legacy_cluster_stopping":
					require.ErrorIs(t, <-closeError, context.Canceled)
				default:
					require.ErrorIs(t, <-closeError, context.DeadlineExceeded)
				}
				if mode == "legacy_cluster_stopping" {
					require.Positive(t, peers.Times())
				} else {
					// QV-only shutdown uses Close's deadline, not the legacy peer heuristic.
					require.Zero(t, peers.Times())
				}
				close(finish)
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Fatal("Stop did not complete")
				}
				require.True(t, nativeClosed.Load())
				require.NoError(t, node.Stop())
			})
		})
	}
}
