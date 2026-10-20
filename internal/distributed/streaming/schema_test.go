package streaming

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/client/mock_handler"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

func TestWALSchemaClientUsesHandlerDiscovery(t *testing.T) {
	mockey.PatchConvey("schema API shares the WAL handler client", t, func() {
		handler := &mock_handler.MockHandlerClient{}
		w := &walAccesserImpl{handlerClient: handler}
		var callErr error
		effectiveTick := uint64(42)
		mockey.Mock((*mock_handler.MockHandlerClient).GetCollectionSchema).To(func(_ *mock_handler.MockHandlerClient, _ context.Context, actual *streamingpb.StreamingNodeGetCollectionSchemaRequest) (*streamingpb.StreamingNodeGetCollectionSchemaResponse, error) {
			require.Equal(t, "p_1v0", actual.Vchannel)
			require.Nil(t, actual.Pchannel, "assignment belongs to the handler client")
			if callErr != nil {
				return nil, callErr
			}
			var version int32
			switch selector := actual.Selector.(type) {
			case *streamingpb.StreamingNodeGetCollectionSchemaRequest_CurrentWrite:
				require.True(t, selector.CurrentWrite)
				version = 5
			case *streamingpb.StreamingNodeGetCollectionSchemaRequest_SchemaVersion:
				version = selector.SchemaVersion
			default:
				t.Fatal("missing schema selector")
			}
			return &streamingpb.StreamingNodeGetCollectionSchemaResponse{Schema: &schemapb.CollectionSchema{Version: version}, EffectiveTimeTick: effectiveTick}, nil
		}).Build()
		response, err := w.SchemaClient().GetCurrentWriteSchema(context.Background(), GetCurrentWriteSchemaRequest{VChannel: "p_1v0"})
		require.NoError(t, err)
		require.EqualValues(t, 5, response.Schema.Version)
		require.EqualValues(t, 42, response.EffectiveTimeTick)
		for _, version := range []int32{0, 3} {
			response, err = w.SchemaClient().GetSchemaVersion(context.Background(), GetSchemaVersionRequest{VChannel: "p_1v0", Version: version})
			require.NoError(t, err)
			require.Equal(t, version, response.Schema.Version)
			require.EqualValues(t, 42, response.EffectiveTimeTick)
		}
		effectiveTick = 0 // Legacy baselines lack an exact effective boundary.
		response, err = w.SchemaClient().GetCurrentWriteSchema(t.Context(), GetCurrentWriteSchemaRequest{VChannel: "p_1v0"})
		require.NoError(t, err)
		require.Zero(t, response.EffectiveTimeTick)
		response, err = w.SchemaClient().GetSchemaVersion(t.Context(), GetSchemaVersionRequest{VChannel: "p_1v0", Version: 0})
		require.NoError(t, err)
		require.Zero(t, response.Schema.Version)
		require.Zero(t, response.EffectiveTimeTick)
		callErr = status.NewUnrecoverableError("schema retired")
		response, err = w.SchemaClient().GetSchemaVersion(context.Background(), GetSchemaVersionRequest{VChannel: "p_1v0", Version: 0})
		require.Nil(t, response)
		require.ErrorIs(t, err, callErr)
	})
}
