// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package proxy

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func normalizeReadDBName(database string) string {
	if database == "" {
		return defaultDB
	}
	return database
}

// collectionReadSnapshot is an immutable, request-scoped binding from the
// caller supplied collection name (which may be an alias) to one concrete
// collection.  All collection metadata used by a read request must come from
// info; callers must not independently resolve the alias again for schema or
// collection properties.
type collectionReadSnapshot struct {
	requestedDBName         string
	requestedCollectionName string
	databaseID              UniqueID
	databaseName            string
	collectionID            UniqueID
	canonicalName           string
	info                    *collectionInfo
}

func (s *collectionReadSnapshot) CollectionID() UniqueID {
	return s.collectionID
}

func (s *collectionReadSnapshot) DatabaseID() UniqueID {
	return s.databaseID
}

func (s *collectionReadSnapshot) DatabaseName() string {
	return s.databaseName
}

func (s *collectionReadSnapshot) RequestedCollectionName() string {
	return s.requestedCollectionName
}

func (s *collectionReadSnapshot) CanonicalName() string {
	return s.canonicalName
}

func (s *collectionReadSnapshot) Schema() *schemaInfo {
	return s.info.schema
}

func (s *collectionReadSnapshot) Info() *collectionInfo {
	return s.info
}

func (s *collectionReadSnapshot) matches(database, collectionName string) bool {
	return normalizeReadDBName(s.requestedDBName) == normalizeReadDBName(database) &&
		s.requestedCollectionName == collectionName
}

// readRequestSnapshot owns all state that must remain pinned across the
// internal attempts of one external Search/Query request.  The collection
// binding is available before task enqueue; the read timestamp is initialized
// once the first task has a BeginTs.
type readRequestSnapshot struct {
	collection *collectionReadSnapshot

	timestampMu      sync.RWMutex
	timestampPinned  bool
	consistencyLevel commonpb.ConsistencyLevel
	requestTS        Timestamp
	guaranteeTS      Timestamp

	partitionOnce sync.Once
	partitions    *partitionInfos
	partitionErr  error
}

func newReadRequestSnapshot(collection *collectionReadSnapshot) *readRequestSnapshot {
	return &readRequestSnapshot{collection: collection}
}

func (s *readRequestSnapshot) Collection() *collectionReadSnapshot {
	return s.collection
}

func (s *readRequestSnapshot) validateTarget(database, collectionName string) error {
	if s == nil || s.collection == nil {
		return merr.WrapErrServiceInternalMsg("read request snapshot has no collection binding")
	}
	if s.collection.matches(database, collectionName) {
		return nil
	}
	return merr.WrapErrServiceInternalMsg(
		"read request snapshot target mismatch: pinned %s/%s, requested %s/%s",
		s.collection.DatabaseName(), s.collection.RequestedCollectionName(), normalizeReadDBName(database), collectionName)
}

func (s *readRequestSnapshot) GetPinnedTimestamp() (commonpb.ConsistencyLevel, Timestamp, Timestamp, bool) {
	s.timestampMu.RLock()
	defer s.timestampMu.RUnlock()
	return s.consistencyLevel, s.requestTS, s.guaranteeTS, s.timestampPinned
}

func (s *readRequestSnapshot) PinTimestamp(consistencyLevel commonpb.ConsistencyLevel, requestTS, guaranteeTS Timestamp) {
	s.timestampMu.Lock()
	defer s.timestampMu.Unlock()
	if s.timestampPinned {
		return
	}
	s.consistencyLevel = consistencyLevel
	s.requestTS = requestTS
	s.guaranteeTS = guaranteeTS
	s.timestampPinned = true
}

type partitionInfosByIDCache interface {
	GetPartitionInfosByID(ctx context.Context, database string, collectionID int64) (*partitionInfos, error)
}

func (s *readRequestSnapshot) Partitions(ctx context.Context) (*partitionInfos, error) {
	if s == nil || s.collection == nil {
		return nil, merr.WrapErrServiceInternalMsg("read request snapshot has no collection binding")
	}
	s.partitionOnce.Do(func() {
		if globalMetaCache == nil {
			s.partitionErr = merr.WrapErrServiceNotReady(paramtable.GetRole(), paramtable.GetNodeID(), "initialization")
			return
		}
		if cache, ok := globalMetaCache.(partitionInfosByIDCache); ok {
			s.partitions, s.partitionErr = cache.GetPartitionInfosByID(
				ctx,
				s.collection.DatabaseName(),
				s.collection.CollectionID(),
			)
			return
		}

		// Generated test mocks implement Cache only. Keep their fallback bound
		// to the canonical collection name; production MetaCache always uses the
		// id-only path above.
		partitions, err := globalMetaCache.GetPartitions(ctx, s.collection.DatabaseName(), s.collection.CanonicalName())
		if err != nil {
			s.partitionErr = err
			return
		}
		infos := make([]*partitionInfo, 0, len(partitions))
		for name, id := range partitions {
			infos = append(infos, &partitionInfo{name: name, partitionID: id})
		}
		s.partitions = parsePartitionsInfo(infos, s.collection.Schema().IsPartitionKeyCollection())
	})
	if s.partitions == nil && s.partitionErr == nil {
		return nil, merr.WrapErrServiceInternalMsg(
			"partition metadata snapshot is incomplete for collection %d", s.collection.CollectionID())
	}
	return s.partitions, s.partitionErr
}

func (s *readRequestSnapshot) PartitionInfo(ctx context.Context, partitionName string) (*partitionInfo, error) {
	if partitionName == "" {
		partitionName = Params.CommonCfg.DefaultPartitionName.GetValue()
	}
	partitions, err := s.Partitions(ctx)
	if err != nil {
		return nil, err
	}
	if info, ok := partitions.name2Info[partitionName]; ok {
		return info, nil
	}
	return nil, merr.WrapErrAsInputError(merr.WrapErrPartitionNotFound(partitionName))
}

type readRequestSnapshotContextKey struct{}

type readRequestSnapshotResult struct {
	snapshot *readRequestSnapshot
	err      error
}

func withReadRequestSnapshotResult(ctx context.Context, result *readRequestSnapshotResult) context.Context {
	return context.WithValue(ctx, readRequestSnapshotContextKey{}, result)
}

func readRequestSnapshotFromContext(ctx context.Context) (*readRequestSnapshot, error, bool) {
	result, ok := ctx.Value(readRequestSnapshotContextKey{}).(*readRequestSnapshotResult)
	if !ok || result == nil {
		return nil, nil, false
	}
	return result.snapshot, result.err, true
}

func resolveCollectionReadSnapshot(ctx context.Context, database, collectionName string) (*collectionReadSnapshot, error) {
	if err := validateCollectionName(collectionName); err != nil {
		return nil, err
	}
	if globalMetaCache == nil {
		return nil, merr.WrapErrServiceNotReady(paramtable.GetRole(), paramtable.GetNodeID(), "initialization")
	}
	info, err := globalMetaCache.GetCollectionInfo(ctx, database, collectionName, 0)
	if err != nil {
		return nil, err
	}
	if info == nil || !info.isCollectionCached() || info.schema.CollectionSchema == nil {
		return nil, merr.WrapErrServiceInternalMsg(
			"collection metadata snapshot is incomplete for %s/%s",
			normalizeReadDBName(database), collectionName)
	}

	canonicalName := collectionName
	if info.schema != nil && info.schema.GetName() != "" {
		canonicalName = info.schema.GetName()
	}
	databaseName := info.dbName
	if databaseName == "" {
		databaseName = normalizeReadDBName(database)
	}

	return &collectionReadSnapshot{
		requestedDBName:         database,
		requestedCollectionName: collectionName,
		databaseID:              info.dbID,
		databaseName:            databaseName,
		collectionID:            info.collID,
		canonicalName:           canonicalName,
		info:                    info,
	}, nil
}

func ensureReadRequestSnapshot(ctx context.Context, database, collectionName string) (context.Context, *readRequestSnapshot, error) {
	if snapshot, err, ok := readRequestSnapshotFromContext(ctx); ok {
		if snapshot == nil {
			if err == nil {
				err = merr.WrapErrServiceInternalMsg("read request snapshot result has neither snapshot nor error")
			}
			return ctx, snapshot, err
		}
		if targetErr := snapshot.validateTarget(database, collectionName); targetErr != nil {
			return ctx, nil, targetErr
		}
		return ctx, snapshot, err
	}

	collection, err := resolveCollectionReadSnapshot(ctx, database, collectionName)
	if err != nil {
		ctx = withReadRequestSnapshotResult(ctx, &readRequestSnapshotResult{err: err})
		return ctx, nil, err
	}
	snapshot := newReadRequestSnapshot(collection)
	ctx = withReadRequestSnapshotResult(ctx, &readRequestSnapshotResult{snapshot: snapshot})
	return ctx, snapshot, nil
}

func getReadRequestTarget(req any) (string, string, bool) {
	switch request := req.(type) {
	case *milvuspb.SearchRequest:
		return request.GetDbName(), request.GetCollectionName(), true
	case *milvuspb.QueryRequest:
		return request.GetDbName(), request.GetCollectionName(), true
	case *milvuspb.HybridSearchRequest:
		return request.GetDbName(), request.GetCollectionName(), true
	default:
		return "", "", false
	}
}

func ensureReadRequestSnapshotForRequest(ctx context.Context, req any) (context.Context, *readRequestSnapshot, error, bool) {
	database, collectionName, ok := getReadRequestTarget(req)
	if !ok {
		return ctx, nil, nil, false
	}
	ctx, snapshot, err := ensureReadRequestSnapshot(ctx, database, collectionName)
	return ctx, snapshot, err, true
}

// EnsureReadRequestSnapshotForRequest pins the collection metadata used by a
// Search, Query, or HybridSearch request and returns the context that carries
// that binding. HTTP adapters use this before request preprocessing so schema
// conversion, RBAC, rate limiting, and the Proxy method all observe the same
// alias target. Non-read requests are returned unchanged.
func EnsureReadRequestSnapshotForRequest(ctx context.Context, req any) (context.Context, error, bool) {
	ctx, _, err, ok := ensureReadRequestSnapshotForRequest(ctx, req)
	return ctx, err, ok
}
