package adaptor

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type writeSchemaProvider interface {
	GetWriteSchema(int64, string) (*schemapb.CollectionSchema, error)
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
	// The handler validates the canonical vchannel before entering the WAL.
	collectionID := funcutil.GetCollectionIDFromVChannel(req.GetVchannel())
	schema, err := w.getWriteSchema(collectionID, req.GetVchannel())
	if !req.GetCurrentWrite() && (err != nil || schema.GetVersion() != req.GetSchemaVersion()) {
		// Write admission can lead the observer. Serve matching versions directly;
		// only historical versions need the recovery module.
		module := w.viewResourceManager.Module(req.GetVchannel())
		if module == nil {
			if err != nil {
				return nil, err
			}
			return nil, status.NewInner("schema history for vchannel %s is not ready", req.GetVchannel())
		}
		snapshot, historyErr := module.GetSchemaVersion(collectionID, req.GetSchemaVersion())
		schema, err = snapshot.GetSchema(), historyErr
	}
	if err != nil {
		return nil, err
	}
	return &streamingpb.StreamingNodeGetCollectionSchemaResponse{
		Schema: schema,
	}, nil
}

func (w *walAdaptorImpl) getWriteSchema(collectionID int64, vchannel string) (*schemapb.CollectionSchema, error) {
	provider, ok := w.param.ShardManager.(writeSchemaProvider)
	if !ok {
		return nil, status.NewUnrecoverableError("WAL does not provide its write schema")
	}
	return provider.GetWriteSchema(collectionID, vchannel)
}
