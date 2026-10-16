package service

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

type schemaProvider interface {
	GetCollectionSchema(context.Context, *streamingpb.StreamingNodeGetCollectionSchemaRequest) (*streamingpb.StreamingNodeGetCollectionSchemaResponse, error)
}

func (hs *handlerServiceImpl) GetCollectionSchema(ctx context.Context, req *streamingpb.StreamingNodeGetCollectionSchemaRequest) (*streamingpb.StreamingNodeGetCollectionSchemaResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if req.GetCollectionId() == 0 || req.GetVchannel() == "" || req.GetPchannel() == nil || req.GetPchannel().GetName() != funcutil.ToPhysicalChannel(req.GetVchannel()) {
		return nil, status.NewInvalidArgument("schema lookup requires collection, vchannel and matching pchannel")
	}
	switch selector := req.GetSelector().(type) {
	case *streamingpb.StreamingNodeGetCollectionSchemaRequest_CurrentWrite:
		if !selector.CurrentWrite {
			return nil, status.NewInvalidArgument("current write selector must be true")
		}
	case *streamingpb.StreamingNodeGetCollectionSchemaRequest_SchemaVersion:
		if selector.SchemaVersion < 0 {
			return nil, status.NewInvalidArgument("schema version must be nonnegative")
		}
	default:
		return nil, status.NewInvalidArgument("schema selector is required")
	}
	raw, err := hs.walManager.GetAvailableWAL(types.NewPChannelInfoFromProto(req.GetPchannel()))
	if err != nil {
		return nil, err
	}
	provider, ok := wal.Unwrap(raw).(schemaProvider)
	if !ok {
		return nil, status.NewUnrecoverableError("WAL does not support schema lookup")
	}
	return provider.GetCollectionSchema(ctx, req)
}
