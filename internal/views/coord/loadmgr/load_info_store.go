package loadmgr

import (
	"context"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/metastore/kv/queryview"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type loadInfoKey struct {
	collectionID int64
	version      uint64
}

// LoadInfoStore serves exact persisted snapshots independently of the latest
// desired configuration. A QueryView's LoadInfoVersion is its durable reference.
// There is no history deletion in this refactor.
type LoadInfoStore struct {
	catalog queryview.LoadInfoCatalog
	indexes func(int64) []*indexpb.IndexInfo
	locks   *lock.KeyLock[loadInfoKey]
	cache   sync.Map
}

func NewLoadInfoStore(catalog queryview.LoadInfoCatalog, indexes func(int64) []*indexpb.IndexInfo) *LoadInfoStore {
	return &LoadInfoStore{catalog: catalog, indexes: indexes, locks: lock.NewKeyLock[loadInfoKey]()}
}

func (s *LoadInfoStore) Get(ctx context.Context, collectionID int64, version uint64) (*querypb.GetQueryViewLoadInfoResponse, error) {
	key := loadInfoKey{collectionID, version}
	if cached, ok := s.cache.Load(key); ok {
		return proto.Clone(cached.(*querypb.GetQueryViewLoadInfoResponse)).(*querypb.GetQueryViewLoadInfoResponse), nil
	}
	info, err := s.catalog.GetLoadInfo(ctx, collectionID, version)
	if err != nil {
		return nil, err
	}
	if info == nil {
		return nil, merr.WrapErrServiceNotReadyMsg("query view load snapshot %d for collection %d is unavailable", version, collectionID)
	}
	s.cache.Store(key, info)
	return proto.Clone(info).(*querypb.GetQueryViewLoadInfoResponse), nil
}

// Ensure persists a snapshot before its config can drive creation of a view.
// Existing snapshots are never refreshed from mutable index metadata.
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
	info := &querypb.GetQueryViewLoadInfoResponse{Status: merr.Success(), CollectionID: cfg.CollectionID, Version: key.version, PartitionIDs: owned.PartitionIDs, LoadFields: owned.LoadFields}
	available := make(map[int64]*indexpb.IndexInfo)
	for _, index := range s.indexes(cfg.CollectionID) {
		available[index.GetIndexID()] = index
	}
	for _, field := range cfg.LoadFields {
		if field.GetIndexId() == 0 {
			continue
		}
		index := available[field.GetIndexId()]
		if index == nil || index.GetFieldID() != field.GetFieldId() {
			return merr.WrapErrServiceNotReadyMsg("index %d for query view field %d is not ready", field.GetIndexId(), field.GetFieldId())
		}
		info.IndexInfoList = append(info.IndexInfoList, proto.Clone(index).(*indexpb.IndexInfo))
	}
	if err := s.catalog.SaveLoadInfo(ctx, info); err != nil {
		return err
	}
	s.cache.Store(key, info)
	return nil
}
