package querycoordv2

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/querycoordv2/job"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

var errReleaseCollectionNotLoaded = errors.New("release collection not loaded")

// broadcastDropLoadConfigCollectionV2ForReleaseCollection broadcasts the drop load config message for release collection.
func (s *Server) broadcastDropLoadConfigCollectionV2ForReleaseCollection(ctx context.Context, req *querypb.ReleaseCollectionRequest) error {
	coll, err := s.broker.DescribeCollection(ctx, req.GetCollectionID())
	if err != nil {
		return err
	}
	broadcaster, err := broadcast.StartBroadcastWithResourceKeys(ctx,
		message.NewSharedDBNameResourceKey(coll.GetDbName()),
		message.NewExclusiveCollectionNameResourceKey(coll.GetDbName(), coll.GetCollectionName()),
	)
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	if !s.meta.CollectionManager.Exist(ctx, req.GetCollectionID()) {
		return errReleaseCollectionNotLoaded
	}
	msg := message.NewDropLoadConfigMessageBuilderV2().
		WithHeader(&message.DropLoadConfigMessageHeader{
			DbId:         coll.DbId,
			CollectionId: coll.CollectionID,
		}).
		WithBody(&message.DropLoadConfigMessageBody{}).
		WithBroadcast([]string{streaming.WAL().ControlChannel()}). // TODO: after we support query view in 3.0, we should broadcast the drop load config message to all vchannels.
		MustBuildBroadcast()

	_, err = broadcaster.Broadcast(ctx, msg)
	return err
}

func (s *Server) dropLoadConfigV2AckCallback(ctx context.Context, result message.BroadcastResultDropLoadConfigMessageV2) error {
	releaseJob := job.NewReleaseCollectionJob(ctx,
		result,
		s.dist,
		s.meta,
		s.broker,
		s.targetMgr,
		s.targetObserver,
		s.checkerController,
		s.proxyClientManager,
	)
	s.jobScheduler.Add(releaseJob)
	if err := releaseJob.Wait(); err != nil {
		return err
	}
	meta.GlobalFailedLoadCache.Remove(result.Message.Header().GetCollectionId())
	return nil
}
