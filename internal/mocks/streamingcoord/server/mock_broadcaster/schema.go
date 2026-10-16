//go:build test && dynamic

package mock_broadcaster

import "github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"

// GetPendingSchemaTarget is a mockey patch point for schema-aware tests.
func (*MockBroadcaster) GetPendingSchemaTarget(int64) (*broadcaster.SchemaTarget, error) {
	panic("GetPendingSchemaTarget must be patched with mockey")
}
