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

// RequiredFieldsForSchema materializes the internal query scope. User load_fields
// are only preload hints and must never be used to construct this list.
func RequiredFieldsForSchema(schema *schemapb.CollectionSchema) []*messagespb.LoadFieldConfig {
	fields := make([]*messagespb.LoadFieldConfig, 0)
	for _, field := range typeutil.GetAllFieldSchemas(schema) {
		if field.GetFieldID() >= common.StartOfUserFieldID {
			fields = append(fields, &messagespb.LoadFieldConfig{FieldId: field.GetFieldID()})
		}
	}
	return fields
}

func reconcileSchemaFields(current *LoadConfig, oldSchema, target *schemapb.CollectionSchema, bound []*messagespb.LoadFieldConfig) *LoadConfig {
	next := current.Clone()
	next.RequiredFields = RequiredFieldsForSchema(target)
	desired := make(map[int64]bool, len(next.RequiredFields))
	for _, field := range next.RequiredFields {
		desired[field.GetFieldId()] = true
	}
	// Full-load intent is represented by an empty hint. Explicit hints retain
	// their surviving fields and never acquire additions from schema evolution.
	if next.LoadAllFields != nil && *next.LoadAllFields {
		next.LoadFields = nil
	} else {
		next.LoadFields = slices.DeleteFunc(next.LoadFields, func(field *messagespb.LoadFieldConfig) bool {
			return !desired[field.GetFieldId()]
		})
	}
	return next
}

func equalSchemaLoadIntent(a, b *LoadConfig) bool {
	if (a.LoadAllFields == nil) != (b.LoadAllFields == nil) || (a.LoadAllFields != nil && *a.LoadAllFields != *b.LoadAllFields) || a.SkipLoadDynamicField != b.SkipLoadDynamicField {
		return false
	}
	equal := func(a, b *messagespb.LoadFieldConfig) bool { return proto.Equal(a, b) }
	return slices.EqualFunc(a.LoadFields, b.LoadFields, equal) && slices.EqualFunc(a.RequiredFields, b.RequiredFields, equal)
}
