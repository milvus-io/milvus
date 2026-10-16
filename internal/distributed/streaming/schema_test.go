package streaming

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/client/mock_handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

func TestWALSchemaClientUsesHandlerDiscovery(t *testing.T) {
	mockey.PatchConvey("schema API shares the WAL handler client", t, func() {
		handler := &mock_handler.MockHandlerClient{}
		w := &walAccesserImpl{handlerClient: handler}
		require.Same(t, handler, w.SchemaClient())
		req := &streamingpb.StreamingNodeGetCollectionSchemaRequest{CollectionId: 1, Vchannel: "p_1v0", Selector: &streamingpb.StreamingNodeGetCollectionSchemaRequest_SchemaVersion{SchemaVersion: 0}}
		mockey.Mock((*mock_handler.MockHandlerClient).GetCollectionSchema).To(func(_ *mock_handler.MockHandlerClient, _ context.Context, actual *streamingpb.StreamingNodeGetCollectionSchemaRequest) (*streamingpb.StreamingNodeGetCollectionSchemaResponse, error) {
			require.Same(t, req, actual)
			return &streamingpb.StreamingNodeGetCollectionSchemaResponse{Schema: &schemapb.CollectionSchema{}, EffectiveTimeTick: 42}, nil
		}).Build()
		response, err := w.SchemaClient().GetCollectionSchema(context.Background(), req)
		require.NoError(t, err)
		require.Zero(t, response.Schema.Version)
		require.EqualValues(t, 42, response.EffectiveTimeTick)
	})
}
