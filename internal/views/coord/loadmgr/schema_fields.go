package loadmgr

import (
	"context"
	"slices"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
)

// AddFunctionFields applies the field/index delta of the acknowledged schema
// DDL. Preload hints are unchanged. Derived fields are included only when their
// inputs are loaded. The collection guard prevents resurrecting released state.
func (s *LoadConfigStore) AddFunctionFields(ctx context.Context, collectionID int64, schema *schemapb.CollectionSchema, added []*messagespb.LoadFieldConfig) error {
	if len(added) == 0 {
		return nil
	}
	s.collectionLocks.Lock(collectionID)
	defer s.collectionLocks.Unlock(collectionID)
	cfg := s.GetConfig(collectionID)
	if cfg == nil {
		return nil
	}
	cfg = cfg.Clone()
	loaded := make(map[int64]bool, len(cfg.RequiredFields))
	for _, field := range cfg.RequiredFields {
		loaded[field.GetFieldId()] = true
	}
	changed := false
	for _, field := range added {
		if loaded[field.GetFieldId()] {
			continue
		}
		for _, function := range schema.GetFunctions() {
			if !slices.Contains(function.GetOutputFieldIds(), field.GetFieldId()) {
				continue
			}
			inputsLoaded := len(function.GetInputFieldIds()) > 0
			for _, id := range function.GetInputFieldIds() {
				inputsLoaded = inputsLoaded && loaded[id]
			}
			if inputsLoaded {
				cfg.RequiredFields = append(cfg.RequiredFields, &messagespb.LoadFieldConfig{FieldId: field.GetFieldId(), IndexId: field.GetIndexId()})
				loaded[field.GetFieldId()] = true
				changed = true
			}
			break
		}
	}
	if !changed {
		return nil
	}
	return s.putLocked(ctx, cfg)
}
