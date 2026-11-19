package balancer

import (
	"fmt"
	"testing"
	"time"

	"github.com/bytedance/mockey"

	"github.com/milvus-io/milvus/internal/views/coord/balancer/api"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/coord/viewfeedback"
	"github.com/milvus-io/milvus/internal/views/qviews"
)

type feedbackDataViews struct{ configs []*loadmgr.LoadConfig }

func (d feedbackDataViews) RegisterDataViewListener(listener api.DataViewListener) func() {
	for _, cfg := range d.configs {
		listener(cfg.CollectionID, &api.CollectionDataView{Shards: []*api.ShardDataView{{}}})
	}
	return func() {}
}

// Build a real immutable publication, including its RG totals. Retaining it
// after Close also exercises the same ownership used by a planning operation.
func priorityFeedbackSnapshot(t testing.TB, configs []*loadmgr.LoadConfig, samples map[int64]*viewfeedback.Snapshot) *viewfeedback.PlanningSnapshot {
	t.Helper()
	patch := mockey.Mock((*loadmgr.LoadConfigStore).RegisterLoadConfigListener).To(func(_ *loadmgr.LoadConfigStore, listener loadmgr.LoadConfigListener) func() {
		for _, cfg := range configs {
			listener(cfg.CollectionID, cfg, 1)
		}
		return func() {}
	}).Build()
	store := viewfeedback.New(&loadmgr.LoadConfigStore{}, feedbackDataViews{configs: configs})
	patch.UnPatch()
	defer store.Close()
	now := time.Now().UnixNano()
	window := qviews.ViewFeedbackWindow{StartUnixNanos: now - int64(5*time.Second), EndUnixNanos: now, Shards: make(map[qviews.ShardID]qviews.ViewFeedback)}
	for _, cfg := range configs {
		for _, replica := range cfg.Replicas {
			if sample := samples[replica.ReplicaID]; sample != nil {
				id := qviews.ShardID{ReplicaID: replica.ReplicaID, VChannel: fmt.Sprintf("p_%dv0", cfg.CollectionID)}
				window.Shards[id] = qviews.ViewFeedback{ShardID: id, RequestCount: uint64(sample.RequestRates.Last6Windows * 5), LastAccessUnixNanos: sample.LastAccessUnixNanos}
			}
		}
	}
	store.OnViewFeedback(window)
	return store.GetPlanningSnapshot()
}
