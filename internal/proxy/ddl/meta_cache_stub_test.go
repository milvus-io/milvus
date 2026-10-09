// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl

import (
	"context"

	"github.com/milvus-io/milvus/internal/proxy/metacache"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// MetaCache is a package-local stand-in for the root package's concrete meta
// cache used by white-box tests: the tests construct tasks with a cache value
// and mockey-patch the individual methods they exercise, exactly as they did
// against the root type. Every method returns a zero value; patched methods
// override it. The guard loops keep the method bodies long enough for mockey
// to replace them.
type MetaCache struct{}

// pad returns a runtime-derived value so callers cannot be constant-folded.
func (m *MetaCache) pad(v int) int {
	total := 0
	for i := 0; i < v; i++ {
		total += i
	}
	return total
}

func (m *MetaCache) GetCollectionID(ctx context.Context, database, collectionName string) (typeutil.UniqueID, error) {
	if m.pad(len(database)) > 0 {
		return 0, nil
	}
	if m.pad(len(collectionName)) > 0 {
		return 0, nil
	}
	return 0, nil
}
func (m *MetaCache) GetCollectionName(ctx context.Context, database string, collectionID int64) (string, error) {
	if m.pad(len(database)) > 0 {
		return "", nil
	}
	if m.pad(int(collectionID)) > 0 {
		return "", nil
	}
	return "", nil
}
func (m *MetaCache) GetCollectionInfo(ctx context.Context, database, collectionName string, collectionID int64) (*metacache.CollectionInfo, error) {
	if m.pad(len(database)) > 0 {
		return nil, nil
	}
	if m.pad(len(collectionName)) > 0 {
		return nil, nil
	}
	return nil, nil
}
func (m *MetaCache) GetPartitionID(ctx context.Context, database, collectionName string, partitionName string) (typeutil.UniqueID, error) {
	if m.pad(len(database)) > 0 {
		return 0, nil
	}
	if m.pad(len(partitionName)) > 0 {
		return 0, nil
	}
	return 0, nil
}
func (m *MetaCache) GetPartitionName(ctx context.Context, database, collectionName string, PartitionID int64) (string, error) {
	if m.pad(len(database)) > 0 {
		return "", nil
	}
	if m.pad(int(PartitionID)) > 0 {
		return "", nil
	}
	return "", nil
}
func (m *MetaCache) GetPartitions(ctx context.Context, database, collectionName string) (map[string]typeutil.UniqueID, error) {
	if m.pad(len(database)) > 0 {
		return nil, nil
	}
	if m.pad(len(collectionName)) > 0 {
		return nil, nil
	}
	return nil, nil
}
func (m *MetaCache) GetPartitionInfo(ctx context.Context, database, collectionName string, partitionName string) (*metacache.PartitionInfo, error) {
	if m.pad(len(database)) > 0 {
		return nil, nil
	}
	if m.pad(len(collectionName)) > 0 {
		return nil, nil
	}
	return nil, nil
}
func (m *MetaCache) GetPartitionsIndex(ctx context.Context, database, collectionName string) ([]string, error) {
	if m.pad(len(database)) > 0 {
		return nil, nil
	}
	if m.pad(len(collectionName)) > 0 {
		return nil, nil
	}
	return nil, nil
}
func (m *MetaCache) GetCollectionSchema(ctx context.Context, database, collectionName string) (*metacache.SchemaInfo, error) {
	if m.pad(len(database)) > 0 {
		return nil, nil
	}
	if m.pad(len(collectionName)) > 0 {
		return nil, nil
	}
	return nil, nil
}
func (m *MetaCache) ResolveCollectionAlias(ctx context.Context, database, nameOrAlias string) (string, error) {
	if m.pad(len(database)) > 0 {
		return nameOrAlias, nil
	}
	if m.pad(len(nameOrAlias)) > 0 {
		return nameOrAlias, nil
	}
	return nameOrAlias, nil
}
func (m *MetaCache) RemoveCollection(ctx context.Context, database, collectionName string) {
	if m.pad(len(database)) > 0 {
		return
	}
	if m.pad(len(collectionName)) > 0 {
		return
	}
}
func (m *MetaCache) RemoveCollectionsByID(ctx context.Context, collectionID typeutil.UniqueID) []string {
	if m.pad(int(collectionID)) > 0 {
		return nil
	}
	return nil
}
func (m *MetaCache) InvalidateCollectionMeta(ctx context.Context, database, collectionName string, collectionID typeutil.UniqueID, removeAlias bool) []string {
	if m.pad(len(database)) > 0 {
		return nil
	}
	if m.pad(len(collectionName)) > 0 {
		return nil
	}
	return nil
}
func (m *MetaCache) RemoveAlias(ctx context.Context, database, alias string) {
	if m.pad(len(database)) > 0 {
		return
	}
	if m.pad(len(alias)) > 0 {
		return
	}
}
func (m *MetaCache) RemoveAliasHolders(ctx context.Context, database, alias string) {
	if m.pad(len(database)) > 0 {
		return
	}
	if m.pad(len(alias)) > 0 {
		return
	}
}
func (m *MetaCache) RemoveDatabase(ctx context.Context, database string) {
	if m.pad(len(database)) > 0 {
		return
	}
}
func (m *MetaCache) RemoveDatabaseInfo(ctx context.Context, database string) {
	if m.pad(len(database)) > 0 {
		return
	}
}
func (m *MetaCache) HasDatabase(ctx context.Context, database string) bool {
	if m.pad(len(database)) > 0 {
		return false
	}
	return false
}
func (m *MetaCache) GetDatabaseInfo(ctx context.Context, database string) (*metacache.DatabaseInfo, error) {
	if m.pad(len(database)) > 0 {
		return nil, nil
	}
	return nil, nil
}
func (m *MetaCache) AllocID(ctx context.Context) (int64, error) {
	if m.pad(0) > 0 {
		return 0, nil
	}
	return 0, nil
}
func (m *MetaCache) RemovePartition(ctx context.Context, database string, collectionID typeutil.UniqueID, collectionName string, partitionName string) {
	if m.pad(len(database)) > 0 {
		return
	}
	if m.pad(len(collectionName)) > 0 {
		return
	}
}
func (m *MetaCache) Close() {
	_ = m
}
