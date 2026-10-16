package adaptor

import (
	"context"

	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type writeSchemaProvider interface {
	GetWriteSchema(int64, string) (*streamingpb.CollectionSchemaOfVChannel, error)
}

func (w *walAdaptorImpl) GetCollectionSchema(ctx context.Context, req *streamingpb.StreamingNodeGetCollectionSchemaRequest) (*streamingpb.StreamingNodeGetCollectionSchemaResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !w.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, status.NewOnShutdownError("WAL is closing")
	}
	defer w.lifetime.Done()
	if w.rwWALImpls.Channel().AccessMode != types.AccessModeRW {
		return nil, status.NewUnrecoverableError("schema lookup requires the primary WAL")
	}
	var snapshot *streamingpb.CollectionSchemaOfVChannel
	var err error
	if req.GetCurrentWrite() {
		provider, ok := w.param.ShardManager.(writeSchemaProvider)
		if !ok {
			return nil, status.NewUnrecoverableError("WAL does not provide its write schema")
		}
		snapshot, err = provider.GetWriteSchema(req.GetCollectionId(), req.GetVchannel())
	} else {
		module := w.viewResourceManager.Module(req.GetVchannel())
		if module == nil {
			return nil, status.NewInner("schema history for vchannel %s is not ready", req.GetVchannel())
		}
		snapshot, err = module.GetSchemaVersion(req.GetCollectionId(), req.GetSchemaVersion())
	}
	if err != nil {
		return nil, err
	}
	return &streamingpb.StreamingNodeGetCollectionSchemaResponse{
		Schema: snapshot.GetSchema(), EffectiveTimeTick: snapshot.GetCheckpointTimeTick(),
	}, nil
}
