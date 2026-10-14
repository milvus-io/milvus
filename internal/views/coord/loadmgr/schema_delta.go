package loadmgr

import (
	"context"
	"slices"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// ApplySchema reconciles loading intent under the existing collection guard.
// It does not broadcast or load a released collection. oldSchema is the public
// schema preceding this ALTER; target is the acknowledged Broadcaster schema.
func (s *LoadConfigStore) ApplySchema(ctx context.Context, collectionID int64, oldSchema, target *schemapb.CollectionSchema, bound []*messagespb.LoadFieldConfig) error {
	s.collectionLocks.Lock(collectionID)
	defer s.collectionLocks.Unlock(collectionID)
	current := s.GetConfig(collectionID)
	if current == nil {
		return nil
	}
	next := reconcileSchemaFields(current, oldSchema, target, bound)
	if equalSchemaLoadIntent(current, next) {
		return nil
	}
	return s.putLocked(ctx, next)
}

func reconcileSchemaFields(current *LoadConfig, oldSchema, target *schemapb.CollectionSchema, bound []*messagespb.LoadFieldConfig) *LoadConfig {
	next := current.Clone()
	loaded := make(map[int64]*messagespb.LoadFieldConfig, len(next.LoadFields))
	for _, field := range next.LoadFields {
		loaded[field.GetFieldId()] = field
	}
	oldFields := make(map[int64]bool)
	for _, field := range typeutil.GetAllFieldSchemas(oldSchema) {
		oldFields[field.GetFieldID()] = true
	}
	if next.LoadAllFields == nil {
		// Legacy messages did not record intent. Treat a complete old field set
		// as full loading; otherwise preserve the existing explicit subset.
		all := true
		for _, field := range typeutil.GetAllFieldSchemas(oldSchema) {
			if field.GetIsDynamic() && loaded[field.GetFieldID()] == nil {
				next.SkipLoadDynamicField = true
			}
			if field.GetFieldID() >= common.StartOfUserFieldID && !field.GetIsDynamic() && loaded[field.GetFieldID()] == nil {
				all = false
			}
		}
		next.LoadAllFields = proto.Bool(all)
	}
	desired := make(map[int64]*schemapb.FieldSchema)
	for _, field := range typeutil.GetAllFieldSchemas(target) {
		if field.GetFieldID() >= common.StartOfUserFieldID {
			desired[field.GetFieldID()] = field
		}
	}
	// Preserve order and immutable index identities for retained fields.
	next.LoadFields = slices.DeleteFunc(next.LoadFields, func(field *messagespb.LoadFieldConfig) bool {
		_, exists := desired[field.GetFieldId()]
		if !exists {
			delete(loaded, field.GetFieldId())
		}
		return !exists
	})
	indexes := make(map[int64]int64, len(bound))
	for _, field := range bound {
		indexes[field.GetFieldId()] = field.GetIndexId()
	}
	appendField := func(field *schemapb.FieldSchema) {
		id := field.GetFieldID()
		if loaded[id] != nil || id < common.StartOfUserFieldID || (field.GetIsDynamic() && next.SkipLoadDynamicField) {
			return
		}
		config := &messagespb.LoadFieldConfig{FieldId: id, IndexId: indexes[id]}
		loaded[id] = config
		next.LoadFields = append(next.LoadFields, config)
	}
	if *next.LoadAllFields {
		for _, field := range typeutil.GetAllFieldSchemas(target) {
			appendField(field)
		}
	} else {
		// A function output follows its loaded inputs. New unrelated ordinary
		// fields do not expand an explicitly partial load.
		for _, function := range target.GetFunctions() {
			inputsLoaded := len(function.GetInputFieldIds()) > 0
			for _, id := range function.GetInputFieldIds() {
				inputsLoaded = inputsLoaded && loaded[id] != nil
			}
			if inputsLoaded {
				for _, id := range function.GetOutputFieldIds() {
					if field := desired[id]; field != nil && !oldFields[id] {
						appendField(field)
					}
				}
			}
		}
	}
	return next
}

func equalSchemaLoadIntent(a, b *LoadConfig) bool {
	if (a.LoadAllFields == nil) != (b.LoadAllFields == nil) || (a.LoadAllFields != nil && *a.LoadAllFields != *b.LoadAllFields) || a.SkipLoadDynamicField != b.SkipLoadDynamicField {
		return false
	}
	return slices.EqualFunc(a.LoadFields, b.LoadFields, func(a, b *messagespb.LoadFieldConfig) bool { return proto.Equal(a, b) })
}
