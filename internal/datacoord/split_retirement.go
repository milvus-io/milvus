package datacoord

import (
	"context"

	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func (s *Server) ListShardSplits() []*datapb.SplitShardTask {
	if s.splitStore == nil {
		return nil
	}
	return s.splitStore.List()
}

func (s *Server) RetireShardSplit(ctx context.Context, id int64) error {
	if s.splitStore == nil {
		return merr.WrapErrServiceNotReadyMsg("split lifecycle is not recovered")
	}
	task := s.splitStore.Get(id)
	if task == nil || task.GetState() >= datapb.SplitShardTaskState_SplitShardTaskRetired {
		return nil
	}
	if task.GetState() != datapb.SplitShardTaskState_SplitShardTaskAdopted {
		return merr.WrapErrServiceNotReadyMsg("split %d is not adopted", id)
	}
	msg := message.NewRetireSplitShardMessageBuilderV2().WithVChannel(task.GetSourceVchannel()).WithUnreplicable().
		WithHeader(&message.RetireSplitShardMessageHeader{CollectionId: task.GetCollectionId(), SplitTaskId: id}).
		WithBody(&message.RetireSplitShardMessageBody{}).MustBuildMutable()
	if _, err := streaming.WAL().RawAppend(ctx, msg); err != nil {
		return merr.Wrapf(err, "retire split source %s", task.GetSourceVchannel())
	}
	// A crash before this write resends the same idempotent local retirement.
	// Successful append is durable; consumption recovers SPLITTED from the WAL.
	return s.splitStore.Advance(ctx, id, datapb.SplitShardTaskState_SplitShardTaskRetired)
}
