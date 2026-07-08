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

package rootcoord

import (
	"github.com/milvus-io/milvus/pkg/v3/metastore"
	"github.com/milvus-io/milvus/pkg/v3/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Catalog returns the underlying persistence catalog. It exists so that
// rootcoord integration tests living in internal/ (which cannot reach the
// unexported field) can reach the backing store directly.
func (mt *MetaTable) Catalog() metastore.RootCoordCatalog {
	return mt.catalog
}

// NewMetaTableWithCatalog builds a MetaTable backed by the given catalog with
// empty (but initialized) in-memory caches, WITHOUT running reload() — so it
// neither reads the backend nor creates the default database. It initializes the
// same maps reload() does (including the file-resource maps) so that any method
// is safe to call on the returned instance; only the cached *contents* are empty.
// rootcoord tests in internal/ (which cannot set the unexported fields) use it to
// get a blank MetaTable in place of a struct literal.
func NewMetaTableWithCatalog(catalog metastore.RootCoordCatalog) *MetaTable {
	return &MetaTable{
		catalog:               catalog,
		names:                 newNameDb(),
		aliases:               newNameDb(),
		dbName2Meta:           make(map[string]*model.Database),
		collID2Meta:           make(map[typeutil.UniqueID]*model.Collection),
		partitionName2ID:      make(map[int64]map[string]int64),
		fileResourceRefCnt:    make(map[int64]int),
		fileResourceName2Meta: make(map[string]*internalpb.FileResourceInfo),
		fileResourceID2Meta:   make(map[int64]*internalpb.FileResourceInfo),
	}
}
