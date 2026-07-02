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
	"context"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/tso"
	coordmeta "github.com/milvus-io/milvus/pkg/v3/coordmeta/rootcoord"
)

// MetaTable / IMetaTable now live in the shared pkg/v3/coordmeta/rootcoord
// package so the pooled catalog service can reuse the same implementation.
// These aliases keep internal/rootcoord call sites (root_coord.go and the ~625
// usages) working unchanged.
type (
	MetaTable  = coordmeta.MetaTable
	IMetaTable = coordmeta.IMetaTable
)

// NewMetaTable wires the production adapters (hookutil cipher + streamingcoord
// channel stats — both internal-only) into the shared pkg MetaTable. milvus
// standalone uses this; the pooled catalog service calls
// coordmeta.NewMetaTable directly with its own injected dependencies.
func NewMetaTable(ctx context.Context, catalog metastore.RootCoordCatalog, tsoAllocator tso.Allocator) (*MetaTable, error) {
	return coordmeta.NewMetaTable(ctx, catalog, tsoAllocator, hookutilCipherHelper{}, channelStatsAdapter{})
}
