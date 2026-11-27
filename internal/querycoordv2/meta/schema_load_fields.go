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

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// UpdateLoadFieldsForSchema adds this ALTER's new, default-preloaded fields.
// Existing exclusions and unrelated stale metadata are left unchanged. The
// caller must hold the original ALTER's collection broadcast guard and apply
// this before publishing target as the collection's public schema.
func (m *CollectionManager) UpdateLoadFieldsForSchema(ctx context.Context, collectionID int64, previous, target *schemapb.CollectionSchema) error {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	collection := m.collections[collectionID]
	if collection == nil {
		return nil
	}
	// Legacy empty means all fields, including future additions. Keep that
	// meaning instead of narrowing it to just the new fields.
	if len(collection.LoadFields) == 0 {
		return nil
	}

	loaded := typeutil.NewSet(collection.GetLoadFields()...)
	next := collection.Clone()
	for _, id := range common.GetAddedLoadFieldIDs(previous, target) {
		if !loaded.Contain(id) {
			next.LoadFields = append(next.LoadFields, id)
			loaded.Insert(id)
		}
	}
	if len(next.LoadFields) == len(collection.LoadFields) {
		return nil
	}
	// Save the clone before making it visible. A failed save leaves readers
	// and the next retry on the preceding immutable load configuration.
	return m.putCollection(ctx, true, next)
}
