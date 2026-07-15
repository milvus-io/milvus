package snview

import (
	"context"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewquery"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

var _ viewquery.TaskProvider = (*SNQueryViewHandler)(nil)

func (h *SNQueryViewHandler) AcquireSearchSegmentTasks(
	ctx context.Context,
	shardID qviews.ShardID,
	version qviews.QueryViewVersion,
	mvcc *viewpb.QueryPlanMVCC,
	req *internalpb.SearchRequest,
) (viewquery.SearchSegmentTasks, error) {
	lease, err := h.AcquireUpView(ctx, shardID, version)
	if err != nil {
		return nil, err
	}
	defer lease.Release()

	if err := h.localOptimizer.OptimizeSearch(ctx, req); err != nil {
		return nil, err
	}
	runtime, err := h.queryRuntime(qviews.QueryViewKey{ShardID: shardID, QueryViewVersion: version})
	if err != nil {
		return nil, err
	}
	mlog.Debug(ctx, "acquire streamingnode search segment tasks wait mvcc",
		mlog.FieldCollectionID(req.GetCollectionID()),
		mlog.FieldVChannel(shardID.VChannel),
		mlog.Int64("replicaID", shardID.ReplicaID),
		mlog.Uint64("growingTimeTick", mvcc.GetGrowingTimetick()),
		mlog.Uint64("transformingTimeTick", mvcc.GetTransformingTimetick()),
	)
	if err := runtime.WaitMVCCVisible(ctx, mvcc.GetGrowingTimetick(), mvcc.GetTransformingTimetick()); err != nil {
		return nil, err
	}
	handles, err := runtime.AcquireGrowingSegmentHandles(ctx, selectedPartitionIDs(req.GetPartitionIDs()))
	if err != nil {
		return nil, err
	}
	mlog.Debug(ctx, "acquired streamingnode search segment tasks",
		mlog.FieldCollectionID(req.GetCollectionID()),
		mlog.FieldVChannel(shardID.VChannel),
		mlog.Int("segmentCount", len(handles)),
	)
	tasks := make([]SNSearchSegmentTask, 0, len(handles))
	for _, handle := range handles {
		tasks = append(tasks, SNSearchSegmentTask{
			Handle:   handle,
			Request:  req,
			MVCC:     mvcc,
			VChannel: lease.Meta.GetVchannel(),
		})
	}
	return NewSNSearchSegmentTasks(tasks), nil
}

func (h *SNQueryViewHandler) AcquireQuerySegmentTasks(
	ctx context.Context,
	shardID qviews.ShardID,
	version qviews.QueryViewVersion,
	mvcc *viewpb.QueryPlanMVCC,
	req *internalpb.RetrieveRequest,
) (viewquery.QuerySegmentTasks, error) {
	lease, err := h.AcquireUpView(ctx, shardID, version)
	if err != nil {
		return nil, err
	}
	defer lease.Release()

	if err := h.localOptimizer.OptimizeRetrieve(ctx, req); err != nil {
		return nil, err
	}
	runtime, err := h.queryRuntime(qviews.QueryViewKey{ShardID: shardID, QueryViewVersion: version})
	if err != nil {
		return nil, err
	}
	mlog.Debug(ctx, "acquire streamingnode query segment tasks wait mvcc",
		mlog.FieldCollectionID(req.GetCollectionID()),
		mlog.FieldVChannel(shardID.VChannel),
		mlog.Int64("replicaID", shardID.ReplicaID),
		mlog.Uint64("growingTimeTick", mvcc.GetGrowingTimetick()),
		mlog.Uint64("transformingTimeTick", mvcc.GetTransformingTimetick()),
	)
	if err := runtime.WaitMVCCVisible(ctx, mvcc.GetGrowingTimetick(), mvcc.GetTransformingTimetick()); err != nil {
		return nil, err
	}
	handles, err := runtime.AcquireGrowingSegmentHandles(ctx, selectedPartitionIDs(req.GetPartitionIDs()))
	if err != nil {
		return nil, err
	}
	mlog.Debug(ctx, "acquired streamingnode query segment tasks",
		mlog.FieldCollectionID(req.GetCollectionID()),
		mlog.FieldVChannel(shardID.VChannel),
		mlog.Int("segmentCount", len(handles)),
	)
	tasks := make([]SNQuerySegmentTask, 0, len(handles))
	for _, handle := range handles {
		tasks = append(tasks, SNQuerySegmentTask{
			Handle:   handle,
			Request:  req,
			MVCC:     mvcc,
			VChannel: lease.Meta.GetVchannel(),
		})
	}
	return NewSNQuerySegmentTasks(tasks), nil
}
