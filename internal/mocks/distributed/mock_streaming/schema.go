//go:build test && dynamic

package mock_streaming

import "github.com/milvus-io/milvus/internal/distributed/streaming"

// SchemaClient is a mockey patch point for schema-aware tests.
func (*MockWALAccesser) SchemaClient() streaming.SchemaClient {
	panic("SchemaClient must be patched with mockey")
}
