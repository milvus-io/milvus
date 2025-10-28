package querycoordv2

import (
	"context"

	"github.com/milvus-io/milvus/internal/querycoordv2/job"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

// alterLoadConfigV2AckCallback is called when the put load config message is acknowledged
func (s *Server) alterLoadConfigV2AckCallback(ctx context.Context, result message.BroadcastResultAlterLoadConfigMessageV2) error {
	// currently, we only sent the put load config message to the control channel
	// TODO: after we support query view in 3.0, we should broadcast the put load config message to all vchannels.
	job := job.NewLoadCollectionJob(ctx, result, s.dist, s.meta, s.broker, s.targetMgr, s.targetObserver, s.collectionObserver, s.nodeMgr)
	if err := job.Execute(); err != nil {
		return err
	}
	meta.GlobalFailedLoadCache.Remove(result.Message.Header().GetCollectionId())
	return nil
}
