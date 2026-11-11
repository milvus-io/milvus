package messageutil

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

// CollectionSchemaTarget returns the immutable read/write schema installed by DDL.
func CollectionSchemaTarget(msg message.ImmutableMessage) *schemapb.CollectionSchema {
	switch msg.MessageType() {
	case message.MessageTypeAlterCollection:
		alter := message.MustAsImmutableAlterCollectionMessageV2(msg)
		if IsSchemaChange(alter.Header()) {
			return alter.MustBody().GetUpdates().GetSchema()
		}
	case message.MessageTypeRefreshExternalCollection:
		return message.MustAsImmutableRefreshExternalCollectionMessageV2(msg).MustBody().GetSchema()
	}
	return nil
}
