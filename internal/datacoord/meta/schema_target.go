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

package meta

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

// CollectionForPreparation overlays pending intent on an immutable cache
// snapshot. Neither the public cache nor previously issued jobs are mutated.
func (m *Meta) CollectionForPreparation(ctx context.Context, collection *CollectionInfo) (*CollectionInfo, error) {
	if collection == nil || m.PreparationSchema == nil {
		return collection, nil
	}
	schema, err := m.PreparationSchema(ctx, collection.ID, collection.Schema)
	if err != nil {
		return nil, err
	}
	copy := *collection
	copy.Schema = schema
	if schema != collection.Schema {
		copy.Properties = funcutil.KeyValuePair2Map(schema.GetProperties())
	}
	return &copy, nil
}

// CollectionsForPreparation leaves collections whose callback is still being
// recovered for the next policy pass. Other collections continue progressing.
// GC and snapshot enumeration keep using the full committed metadata cache.
func (m *Meta) CollectionsForPreparation(ctx context.Context) []*CollectionInfo {
	collections := m.GetCollections()
	ready := make([]*CollectionInfo, 0, len(collections))
	for _, collection := range collections {
		prepared, err := m.CollectionForPreparation(ctx, collection)
		if err != nil {
			mlog.Debug(ctx, "collection schema target is not ready for compaction planning", mlog.FieldCollectionID(collection.ID), mlog.Err(err))
			continue
		}
		ready = append(ready, prepared)
	}
	return ready
}
