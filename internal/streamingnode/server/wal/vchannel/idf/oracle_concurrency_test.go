package idf

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/queryresource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

// Seal observation, view preparation and lazy queries use different goroutines.
// Retiring growing stats must be serialized with both aggregate publication and
// the direct membership reads in these paths, including manual-flush scans.
func TestOracleConcurrentSealingPreparationAndQueries(t *testing.T) {
	for _, lazy := range []bool{false, true} {
		t.Run(map[bool]string{false: "eager", true: "lazy"}[lazy], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			r, resource := newTestOracle(t)
			if lazy {
				r = newLazyOracle(t)
			}
			const segments = 256
			values := make([]float32, segments)
			for i := range values {
				values[i] = 2
				r.ApplyLiveEvent(ctx, walview.VChannelResourceEvent{Message: bm25Insert(t, int64(i+1), 2)})
			}
			// A compaction replaces all sealed inputs in the target DataView.
			compacted := resource(1000, values...)
			patch := mockey.Mock((*Provider).getSealedBM25Resources).To(func(_ *Provider, _ context.Context, _ int64, _ string, version qviews.DataVersion, _ []int64, _ uint64) ([]*datapb.StreamingNodeBM25Resource, error) {
				if version.StreamingVersion < 11 {
					return nil, nil
				}
				return []*datapb.StreamingNodeBM25Resource{compacted}, nil
			}).Build()
			defer patch.UnPatch()
			defer r.Close()
			flush := message.NewManualFlushMessageBuilderV2().WithVChannel("v1").WithHeader(&message.ManualFlushMessageHeader{}).WithBody(&message.ManualFlushMessageBody{}).MustBuildMutable().WithTimeTick(20).WithLastConfirmedUseMessageID().IntoImmutableMessage(rmq.NewRmqID(20))
			r.ApplyLiveEvent(ctx, walview.VChannelResourceEvent{Message: flush})
			start := make(chan struct{})
			results := make(chan error, 2)
			var wg sync.WaitGroup
			wg.Add(3)
			go func() {
				defer wg.Done()
				<-start
				for id := int64(1); id <= segments; id++ {
					r.ApplyLiveEvent(ctx, walview.VChannelResourceEvent{SegmentSealed: &walview.SegmentSealedEvent{SegmentID: id, SealedAtDataVersion: qviews.DataVersion{StreamingVersion: 11}}})
					if id%16 == 0 {
						r.ApplyLiveEvent(ctx, walview.VChannelResourceEvent{Message: flush})
					}
				}
			}()
			go func() {
				defer wg.Done()
				<-start
				for version := int64(11); version < 75; version++ {
					if err := r.PrepareDataVersion(ctx, qviews.DataVersion{StreamingVersion: version}); err != nil && !errors.Is(err, nodescheduler.ErrDelay) {
						results <- err
						return
					}
				}
				results <- nil
			}()
			go func() {
				defer wg.Done()
				<-start
				for range 64 {
					if _, err := r.BuildIDFBatch(ctx, []queryresource.IDFRequest{{FieldID: 102}}); err != nil && !errors.Is(err, merr.ErrServiceNotReady) {
						results <- err
						return
					}
				}
				results <- nil
			}()
			close(start)
			wg.Wait()
			require.NoError(t, <-results)
			require.NoError(t, <-results)
			require.NoError(t, r.PrepareDataVersion(ctx, qviews.DataVersion{StreamingVersion: 75}))
			r.ApplyLiveEvent(ctx, walview.VChannelResourceEvent{Message: bm25Insert(t, 2000, 4)})
			batch, err := r.BuildIDFBatch(ctx, []queryresource.IDFRequest{{FieldID: 102}})
			require.NoError(t, err)
			require.Equal(t, int64(segments+1), r.currentStats[102].NumRow(), "each growing-to-sealed contribution is counted once")
			require.Equal(t, float64(segments*2+4)/float64(segments+1), batch[0].Avgdl)
			require.Len(t, r.growingStore.segments, 1, "retire old stats while retaining the new growing segment")
			require.Contains(t, r.growingStore.segments, int64(2000))
		})
	}
}

func TestOracleInsertConversionDoesNotBlockQueries(t *testing.T) {
	r, _ := newTestOracle(t)
	ctx := context.Background()
	r.ApplyLiveEvent(ctx, walview.VChannelResourceEvent{Message: bm25Insert(t, 20, 2)})
	started, finish := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(finish) })
	defer unblock()
	var original func(bm25Stats, *schemapb.CollectionSchema, walview.SegmentInsertMessage) error
	patch := mockey.Mock(collectGrowingInsertStats).Origin(&original).To(func(stats bm25Stats, schema *schemapb.CollectionSchema, insert walview.SegmentInsertMessage) error {
		close(started)
		<-finish
		return original(stats, schema, insert)
	}).Build()
	defer patch.UnPatch()
	inserted := make(chan struct{})
	msg := bm25Insert(t, 20, 6)
	go func() {
		r.ApplyLiveEvent(ctx, walview.VChannelResourceEvent{Message: msg})
		close(inserted)
	}()
	<-started
	queried := make(chan struct{})
	go func() {
		defer close(queried)
		batch, err := r.BuildIDFBatch(ctx, []queryresource.IDFRequest{{FieldID: 102}})
		if assert.NoError(t, err) {
			assert.Equal(t, float64(2), batch[0].Avgdl, "the private delta must not be published yet")
		}
	}()
	select {
	case <-queried:
	case <-time.After(time.Second):
		unblock()
		<-inserted
		<-queried
		t.Fatal("insert conversion blocked an IDF query")
	}
	unblock()
	<-inserted
	batch, err := r.BuildIDFBatch(ctx, []queryresource.IDFRequest{{FieldID: 102}})
	require.NoError(t, err)
	require.Equal(t, float64(4), batch[0].Avgdl)
	require.Equal(t, int64(2), r.currentStats[102].NumRow())
	require.Equal(t, int64(2), r.growingStore.segments[20].stats[102].NumRow())
}
