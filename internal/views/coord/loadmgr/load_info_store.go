package loadmgr

import (
	"context"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/kv/queryview"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type loadInfoKey struct {
	collectionID int64
	version      uint64
}

// LoadInfoStore persists exact field/partition scopes independently of the latest
// desired configuration. Index descriptors are resolved from current metadata
// on every read; a QueryView does not retain an index identity.
// There is no history deletion in this refactor.
type LoadInfoStore struct {
	catalog queryview.LoadInfoCatalog
	indexes func(int64) []*indexpb.IndexInfo
	schema  func(context.Context, int64) (*schemapb.CollectionSchema, error)
	locks   *lock.KeyLock[loadInfoKey]
	cache   sync.Map
}

func NewLoadInfoStore(catalog queryview.LoadInfoCatalog, indexes func(int64) []*indexpb.IndexInfo, schema ...func(context.Context, int64) (*schemapb.CollectionSchema, error)) *LoadInfoStore {
	s := &LoadInfoStore{catalog: catalog, indexes: indexes, locks: lock.NewKeyLock[loadInfoKey]()}
	if len(schema) > 0 {
		s.schema = schema[0]
	}
	return s
}

func (s *LoadInfoStore) Get(ctx context.Context, collectionID int64, version uint64) (*querypb.GetQueryViewLoadInfoResponse, error) {
	key := loadInfoKey{collectionID, version}
	if cached, ok := s.cache.Load(key); ok {
		return s.withCurrentIndexes(cached.(*querypb.GetQueryViewLoadInfoResponse)), nil
	}
	info, err := s.catalog.GetLoadInfo(ctx, collectionID, version)
	if err != nil {
		return nil, err
	}
	if info == nil {
		return nil, merr.WrapErrServiceNotReadyMsg("query view load snapshot %d for collection %d is unavailable", version, collectionID)
	}
	s.cache.Store(key, info)
	return s.withCurrentIndexes(info), nil
}

// Ensure persists a snapshot before its config can drive creation of a view.
// Only the field/partition scope is retained; indexes are resolved when read.
func (s *LoadInfoStore) Ensure(ctx context.Context, cfg *LoadConfig) error {
	key := loadInfoKey{cfg.CollectionID, loadInfoVersion(cfg)}
	s.locks.Lock(key)
	defer s.locks.Unlock(key)
	if _, ok := s.cache.Load(key); ok {
		return nil
	}
	existing, err := s.catalog.GetLoadInfo(ctx, key.collectionID, key.version)
	if err != nil {
		return err
	}
	if existing != nil {
		s.cache.Store(key, existing)
		return nil
	}
	owned := cfg.Clone()
	info := &querypb.GetQueryViewLoadInfoResponse{Status: merr.Success(), CollectionID: cfg.CollectionID, Version: key.version, PartitionIDs: owned.PartitionIDs, LoadFields: owned.LoadFields, RequiredFields: owned.RequiredFields, SkipLoadDynamicField: owned.SkipLoadDynamicField}
	for _, fields := range [][]*messagespb.LoadFieldConfig{info.LoadFields, info.RequiredFields} {
		for _, field := range fields {
			field.IndexId = 0
		}
	}
	if err := s.catalog.SaveLoadInfo(ctx, info); err != nil {
		return err
	}
	s.cache.Store(key, info)
	return nil
}

// QueryViews retain loading scope, never an index identity. IndexID zero means
// the newest available index; no index metadata is persisted in this history.
func (s *LoadInfoStore) withCurrentIndexes(stored *querypb.GetQueryViewLoadInfoResponse) *querypb.GetQueryViewLoadInfoResponse {
	info := proto.Clone(stored).(*querypb.GetQueryViewLoadInfoResponse)
	fields := make(map[int64]struct{}, len(info.RequiredFields))
	for _, field := range info.RequiredFields {
		field.IndexId = 0
		fields[field.FieldId] = struct{}{}
	}
	info.IndexInfoList = nil
	for _, index := range s.indexes(info.CollectionID) {
		if _, loaded := fields[index.GetFieldID()]; loaded || len(fields) == 0 {
			info.IndexInfoList = append(info.IndexInfoList, proto.Clone(index).(*indexpb.IndexInfo))
		}
	}
	return info
}

// normalize materializes legacy required scopes before desired state publication.
// Historical snapshots are resolved against their View schema by the worker.
func (s *LoadInfoStore) normalize(ctx context.Context, cfg *LoadConfig) (*LoadConfig, error) {
	cfg = cfg.Clone()
	if len(cfg.RequiredFields) == 0 && s.schema != nil {
		schema, err := s.schema(ctx, cfg.CollectionID)
		if err != nil {
			return nil, err
		}
		cfg.RequiredFields = RequiredFieldsForSchema(schema)
		if len(cfg.RequiredFields) == 0 {
			return nil, merr.WrapErrServiceInternalMsg("collection %d has no required query fields", cfg.CollectionID)
		}
	}
	if cfg.LoadAllFields != nil && *cfg.LoadAllFields {
		cfg.LoadFields = nil
	}
	return cfg, nil
}
