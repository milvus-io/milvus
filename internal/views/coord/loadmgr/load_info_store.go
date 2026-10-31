package loadmgr

import (
	"cmp"
	"context"
	"slices"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type loadInfoKey struct {
	collectionID int64
	version      uint64
}

// GetLoadInfo returns a copy of the exact immutable requirements. Version zero
// selects the active configuration atomically; it never selects a releasing one.
func (s *LoadConfigStore) GetLoadInfo(ctx context.Context, collectionID int64, version uint64) (*querypb.GetQueryViewLoadInfoResponse, error) {
	s.mu.RLock()
	if version == 0 {
		version = s.versions[collectionID]
		if version == 0 {
			s.mu.RUnlock()
			return nil, merr.WrapErrCollectionNotLoaded(collectionID)
		}
	}
	cfg := s.history[collectionID][version]
	if cfg == nil {
		s.mu.RUnlock()
		return nil, merr.WrapErrServiceNotReadyMsg("load requirements %d for collection %d are unavailable", version, collectionID)
	}
	owned := cfg.Clone()
	s.mu.RUnlock()
	// Definitions stay owned by index metadata. Resolve only this version's
	// selected IDs; neither new indexes nor current field selections may leak in.
	selected := make(map[int64]int64)
	var ids []int64
	for _, field := range owned.RequiredFields {
		for _, id := range field.GetIndexIds() {
			selected[id] = field.GetFieldId()
			ids = append(ids, id)
		}
	}
	var indexes []*indexpb.IndexInfo
	if len(ids) != 0 && s.indexes != nil {
		for _, info := range s.indexes(collectionID, ids...) {
			field, ok := selected[info.GetIndexID()]
			if ok && field == info.GetFieldID() {
				indexes = append(indexes, proto.Clone(info).(*indexpb.IndexInfo))
				delete(selected, info.GetIndexID())
			}
		}
	}
	if len(selected) != 0 {
		return nil, merr.WrapErrServiceNotReadyMsg("selected index metadata for load requirements %d of collection %d is unavailable", version, collectionID)
	}
	slices.SortFunc(indexes, func(a, b *indexpb.IndexInfo) int { return cmp.Compare(a.GetIndexID(), b.GetIndexID()) })
	return &querypb.GetQueryViewLoadInfoResponse{
		Status: merr.Success(), CollectionID: collectionID, Version: version,
		PartitionIDs: owned.PartitionIDs, RequiredFields: owned.RequiredFields,
		LoadFields: owned.LoadFields, IndexInfoList: indexes,
	}, nil
}

// AcquireLoadInfo pins requirements through final durable View removal. It is
// serialized with deletion, including creation before the View is persisted.
func (s *LoadConfigStore) AcquireLoadInfo(ctx context.Context, collectionID int64, version uint64) (func(), error) {
	s.collectionLocks.Lock(collectionID)
	defer s.collectionLocks.Unlock(collectionID)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	key := loadInfoKey{collectionID, version}
	s.mu.Lock()
	if version == 0 || s.history[collectionID][version] == nil {
		s.mu.Unlock()
		return nil, merr.WrapErrServiceNotReadyMsg("load requirements %d for collection %d are unavailable", version, collectionID)
	}
	s.references[key]++
	s.mu.Unlock()
	return sync.OnceFunc(func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.references[key] == 1 {
			delete(s.references, key)
		} else {
			s.references[key]--
		}
	}), nil
}

// EnableGC is called only after every recovered View acquired its reference.
func (s *LoadConfigStore) EnableGC() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gcEnabled = true
}

// CollectLoadInfo removes unreferenced history, retaining the latest version
// until Release's final removal. A failed catalog operation leaves it resident
// for retry. There is one pass per coordinator, never a per-View GC goroutine.
func (s *LoadConfigStore) CollectLoadInfo(ctx context.Context) error {
	s.mu.RLock()
	if !s.gcEnabled {
		s.mu.RUnlock()
		return nil
	}
	ids := make([]int64, 0, len(s.history))
	for id := range s.history {
		ids = append(ids, id)
	}
	s.mu.RUnlock()
	for _, id := range ids {
		if err := s.collectCollectionLoadInfo(ctx, id); err != nil {
			return err
		}
	}
	return nil
}

func (s *LoadConfigStore) collectCollectionLoadInfo(ctx context.Context, id int64) error {
	s.collectionLocks.Lock(id)
	defer s.collectionLocks.Unlock(id)
	s.mu.RLock()
	// Release owns deletion of an inactive collection, after node cleanup.
	if s.configs[id] == nil {
		s.mu.RUnlock()
		return nil
	}
	var obsolete []uint64
	for version := range s.history[id] {
		if version != s.versions[id] && s.references[loadInfoKey{id, version}] == 0 {
			obsolete = append(obsolete, version)
		}
	}
	s.mu.RUnlock()
	if len(obsolete) == 0 {
		return nil
	}
	if err := s.catalog.RemoveVersions(ctx, id, obsolete); err != nil {
		return err
	}
	s.mu.Lock()
	for _, version := range obsolete {
		delete(s.history[id], version)
	}
	s.mu.Unlock()
	return nil
}

func (s *LoadConfigStore) normalize(ctx context.Context, cfg *LoadConfig) (*LoadConfig, error) {
	cfg = cfg.Clone()
	if len(cfg.RequiredFields) == 0 && s.schema != nil {
		schema, err := s.schema(ctx, cfg.CollectionID)
		if err != nil {
			return nil, err
		}
		cfg.RequiredFields = RequiredFieldsForSchema(schema)
	}
	if len(cfg.RequiredFields) == 0 {
		return nil, merr.WrapErrServiceInternalMsg("collection %d has no required query fields", cfg.CollectionID)
	}
	fields := make(map[int64][]int64, len(cfg.RequiredFields))
	for _, field := range cfg.RequiredFields {
		fields[field.GetFieldId()] = nil
	}
	if s.indexes != nil {
		for _, index := range s.indexes(cfg.CollectionID) {
			if _, required := fields[index.GetFieldID()]; required {
				fields[index.GetFieldID()] = append(fields[index.GetFieldID()], index.GetIndexID())
			}
		}
	}
	for field, ids := range fields {
		slices.Sort(ids)
		fields[field] = slices.Compact(ids)
	}
	for _, scope := range [][]*viewpb.LoadFieldConfig{cfg.RequiredFields, cfg.LoadFields} {
		for _, field := range scope {
			field.IndexIds = slices.Clone(fields[field.FieldId])
		}
		slices.SortFunc(scope, func(a, b *viewpb.LoadFieldConfig) int { return cmp.Compare(a.FieldId, b.FieldId) })
	}
	slices.Sort(cfg.PartitionIDs)
	cfg.PartitionIDs = slices.Compact(cfg.PartitionIDs)
	slices.SortFunc(cfg.Replicas, func(a, b *ReplicaAssignment) int { return cmp.Compare(a.ReplicaID, b.ReplicaID) })
	return cfg, nil
}
