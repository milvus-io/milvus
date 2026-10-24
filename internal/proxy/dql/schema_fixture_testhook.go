//go:build test && dynamic

package dql

import "github.com/milvus-io/milvus/internal/proxy/metacache"

// SetQueryReadSchemaForTest supplies the state normally installed by PreExecute
// when a scheduler test replaces that phase to isolate retry behavior.
func SetQueryReadSchemaForTest(task *QueryTask, schema *metacache.SchemaInfo) {
	task.schema = schema
}
