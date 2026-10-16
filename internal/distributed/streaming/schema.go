package streaming

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/client/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

// SchemaSnapshot is an owned schema and its effective boundary on the channel.
// EffectiveTimeTick does not imply global DDL completion or persistence.
// Zero means the exact boundary is unknown for a legacy schema baseline;
// it must not be used as proof that a channel has crossed a schema boundary.
type SchemaSnapshot struct {
	Schema            *schemapb.CollectionSchema
	EffectiveTimeTick uint64
}

type GetCurrentWriteSchemaRequest struct {
	VChannel string
}

type GetSchemaVersionRequest struct {
	VChannel string
	Version  int32
}

// SchemaClient resolves schemas through WAL ownership without exposing routing
// or wire selectors. Calls retry transient failures until ctx ends.
type SchemaClient interface {
	// GetCurrentWriteSchema returns the schema currently used by write admission.
	GetCurrentWriteSchema(ctx context.Context, req GetCurrentWriteSchemaRequest) (*SchemaSnapshot, error)
	// GetSchemaVersion returns a retained exact version, including version zero.
	GetSchemaVersion(ctx context.Context, req GetSchemaVersionRequest) (*SchemaSnapshot, error)
}

type schemaClient struct {
	handlerClient handler.HandlerClient
}

func (c schemaClient) GetCurrentWriteSchema(ctx context.Context, req GetCurrentWriteSchemaRequest) (*SchemaSnapshot, error) {
	return c.getCollectionSchema(ctx, &streamingpb.StreamingNodeGetCollectionSchemaRequest{
		Vchannel: req.VChannel,
		Selector: &streamingpb.StreamingNodeGetCollectionSchemaRequest_CurrentWrite{CurrentWrite: true},
	})
}

func (c schemaClient) GetSchemaVersion(ctx context.Context, req GetSchemaVersionRequest) (*SchemaSnapshot, error) {
	return c.getCollectionSchema(ctx, &streamingpb.StreamingNodeGetCollectionSchemaRequest{
		Vchannel: req.VChannel,
		Selector: &streamingpb.StreamingNodeGetCollectionSchemaRequest_SchemaVersion{SchemaVersion: req.Version},
	})
}

func (c schemaClient) getCollectionSchema(ctx context.Context, req *streamingpb.StreamingNodeGetCollectionSchemaRequest) (*SchemaSnapshot, error) {
	resp, err := c.handlerClient.GetCollectionSchema(ctx, req)
	if err != nil {
		return nil, err
	}
	return &SchemaSnapshot{Schema: resp.GetSchema(), EffectiveTimeTick: resp.GetEffectiveTimeTick()}, nil
}
