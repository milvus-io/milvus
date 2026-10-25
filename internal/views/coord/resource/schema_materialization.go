package resource

import "github.com/milvus-io/milvus-proto/go-api/v3/schemapb"

// SchemaRequiresMaterialization distinguishes logical read changes from new
// derived historical values. Ordinary nullable/default additions use the
// existing read projection; deletions and parameter-only changes are logical.
// Output FieldIDs are lifecycle identities: drop/re-add never reuses them.
func SchemaRequiresMaterialization(published, target *schemapb.CollectionSchema) bool {
	outputs := make(map[int64]struct{})
	for _, function := range published.GetFunctions() {
		for _, id := range function.GetOutputFieldIds() {
			outputs[id] = struct{}{}
		}
	}
	for _, function := range target.GetFunctions() {
		for _, id := range function.GetOutputFieldIds() {
			if _, exists := outputs[id]; !exists {
				return true
			}
		}
	}
	return false
}
