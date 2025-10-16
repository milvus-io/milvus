package messageutil

import "github.com/milvus-io/milvus/pkg/v2/streaming/util/message"

// IsSchemaChange checks if the put collection message is a schema change message.
func IsSchemaChange(header *message.AlterCollectionMessageHeader) bool {
	for _, path := range header.UpdateMask.GetPaths() {
		if path == message.FieldMaskCollectionSchema {
			return true
		}
	}
	return false
}
