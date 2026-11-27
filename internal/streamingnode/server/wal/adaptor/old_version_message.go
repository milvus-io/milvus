package adaptor

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

func newOldVersionImmutableMessage(ctx context.Context, pchannel string, lastConfirmed message.MessageID, msg message.ImmutableMessage) (message.ImmutableMessage, error) {
	return utility.ConvertOldVersionMessage(ctx, pchannel, lastConfirmed, msg, resource.Resource().VChannelTempStorage().GetVChannelByPChannelOfCollection)
}
