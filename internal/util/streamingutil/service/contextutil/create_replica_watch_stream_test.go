package contextutil

import (
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

func TestCreateReplicaWatchStreamMetadata(t *testing.T) {
	req := &streamingpb.CreateReplicaWatchStreamRequest{Pchannel: &streamingpb.PChannelInfo{Name: "p", Term: 3}}
	outgoing := WithCreateReplicaWatchStream(t.Context(), req)
	md, ok := metadata.FromOutgoingContext(outgoing)
	require.True(t, ok)
	got, err := GetCreateReplicaWatchStream(metadata.NewIncomingContext(t.Context(), md))
	require.NoError(t, err)
	require.True(t, proto.Equal(req, got))
	_, err = GetCreateReplicaWatchStream(t.Context())
	require.Error(t, err)
	for _, md := range []metadata.MD{{}, metadata.Pairs(createReplicaWatchStreamKey, "!bad-base64"), metadata.Pairs(createReplicaWatchStreamKey, base64.StdEncoding.EncodeToString([]byte{0xff}))} {
		_, err = GetCreateReplicaWatchStream(metadata.NewIncomingContext(t.Context(), md))
		require.Error(t, err)
	}
}
