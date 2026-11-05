package loadmgr

import (
	"context"
	"slices"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// ApplySchema reconciles loading intent under the existing collection guard.
// It does not broadcast or load a released collection. oldSchema is the public
// schema preceding this ALTER; target is the acknowledged Broadcaster schema.
func (s *LoadConfigStore) ApplySchema(ctx context.Context, collectionID int64, oldSchema, target *schemapb.CollectionSchema, bound []*viewpb.LoadFieldConfig) error {
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
func RequiredFieldsForSchema(schema *schemapb.CollectionSchema) []*viewpb.LoadFieldConfig {
	fields := make([]*viewpb.LoadFieldConfig, 0)
	for _, field := range typeutil.GetAllFieldSchemas(schema) {
		if field.GetFieldID() >= common.StartOfUserFieldID {
			fields = append(fields, &viewpb.LoadFieldConfig{FieldId: field.GetFieldID()})
		}
	}
	return fields
}

// PreloadFieldIDsForSchema resolves external load intent at the Coordinator.
// Only the request boundary treats an omitted selection as schema defaults;
// persisted requirements and their consumers always interpret empty literally.
func PreloadFieldIDsForSchema(schema *schemapb.CollectionSchema, requested []int64) []int64 {
	selected := make(map[int64]bool, len(requested))
	for _, id := range requested {
		selected[id] = true
	}
	fields := make([]int64, 0)
	for _, field := range typeutil.GetAllFieldSchemas(schema) {
		if field.GetFieldID() < common.StartOfUserFieldID {
			continue
		}
		preload := selected[field.GetFieldID()]
		if len(requested) == 0 {
			preload = defaultPreload(field)
		}
		if preload {
			fields = append(fields, field.GetFieldID())
		}
	}
	return fields
}

func defaultPreload(field *schemapb.FieldSchema) bool {
	preload, err := common.ShouldFieldBeLoaded(field.GetTypeParams())
	// Match the existing API policy: an invalid skip_load value is ignored.
	return err != nil || preload
}

func reconcileSchemaFields(current *LoadConfig, oldSchema, target *schemapb.CollectionSchema, bound []*viewpb.LoadFieldConfig) *LoadConfig {
	next := current.Clone()
	next.RequiredFields = RequiredFieldsForSchema(target)
	previousFields := make(map[int64]bool)
	for _, field := range typeutil.GetAllFieldSchemas(oldSchema) {
		previousFields[field.GetFieldID()] = true
	}
	preloaded := make(map[int64]*viewpb.LoadFieldConfig, len(current.LoadFields))
	for _, field := range next.LoadFields {
		preloaded[field.GetFieldId()] = field
	}
	next.LoadFields = nil
	// Derive both scopes from the same target schema. Surviving fields keep their
	// explicit preload choice; additions follow their own schema defaults.
	for _, field := range typeutil.GetAllFieldSchemas(target) {
		id := field.GetFieldID()
		if id < common.StartOfUserFieldID {
			continue
		}
		if previous := preloaded[id]; previous != nil {
			next.LoadFields = append(next.LoadFields, previous)
		} else if !previousFields[id] && defaultPreload(field) {
			next.LoadFields = append(next.LoadFields, &viewpb.LoadFieldConfig{FieldId: id})
		}
	}
	return next
}

func equalSchemaLoadIntent(a, b *LoadConfig) bool {
	equal := func(a, b *viewpb.LoadFieldConfig) bool { return proto.Equal(a, b) }
	return slices.EqualFunc(a.LoadFields, b.LoadFields, equal) && slices.EqualFunc(a.RequiredFields, b.RequiredFields, equal)
}
