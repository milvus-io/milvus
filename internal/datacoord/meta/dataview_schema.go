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
	"github.com/milvus-io/milvus/internal/views/coord/resource"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// checkCompactionSchemaPublication prevents any physical replacement (sort,
// merge, clustering or schema-bump) from removing columns before the direct
// DDL has published its logical read schema. The task retries its metadata
// commit; direct DDL never waits for that task. Derived-output materialization
// is deliberately allowed before publication, because DDL waits for its result.
func (m *Meta) checkCompactionSchemaPublication(ctx context.Context, collectionID int64, target *schemapb.CollectionSchema) error {
	if m.DataViewManager == nil {
		return nil
	}
	ref, err := m.DataViewManager.Latest(ctx, collectionID)
	if err != nil {
		return err
	}
	if ref == nil {
		return merr.WrapErrServiceNotReadyMsg("collection %d DataView is not initialized", collectionID)
	}
	defer ref.Deref()
	if ref.DataView().GetSchemaVersion() >= target.GetVersion() {
		return nil
	}
	published := m.GetCollection(collectionID)
	if published != nil && resource.SchemaRequiresMaterialization(published.Schema, target) {
		return nil
	}
	return merr.WrapErrServiceNotReadyMsg("collection %d must publish DataView schema %d before committing physical cleanup", collectionID, target.GetVersion())
}
