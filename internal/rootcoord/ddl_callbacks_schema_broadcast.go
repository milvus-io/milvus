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

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/internal/util/schemautil"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

// broadcastSchemaChange is the default admission + broadcast path for schema-mutating DDLs
// that have no pre-broadcast side effects. It runs the structural admission gate
// (schemautil.ValidateSchemaEvolution) on newSchema against the committed schema, and only if
// that passes does it put the already built message on the WAL.
//
// Callbacks that reserve analyzer file-resource references first call validateSchemaChange,
// then perform the reservation, and finally call broadcastValidatedSchemaChange. This keeps
// admission ahead of side effects while ensuring every schema change is vetted before WAL.
func (c *Core) broadcastSchemaChange(
	ctx context.Context,
	bc broadcaster.BroadcastAPI,
	oldColl *model.Collection,
	newSchema *schemapb.CollectionSchema,
	msg message.BroadcastMutableMessage,
) error {
	if err := c.validateSchemaChange(ctx, oldColl, newSchema); err != nil {
		return err
	}
	return c.broadcastValidatedSchemaChange(ctx, bc, msg)
}

// validateSchemaChange runs admission before callers perform any pre-broadcast side effects,
// such as reserving analyzer file-resource references. Callers with such side effects must use
// this method first, then call broadcastValidatedSchemaChange after building the final message.
func (c *Core) validateSchemaChange(
	ctx context.Context,
	oldColl *model.Collection,
	newSchema *schemapb.CollectionSchema,
) error {
	// Environment precondition: refuse the change outright while the cluster still has a
	// pre-3.0 component (e.g. mid rolling upgrade) that cannot honor the write/read/DDL
	// protocol the change relies on. Checked before the structural gate so a mixed-version
	// cluster is rejected regardless of what the schema edit is.
	if err := c.checkClusterVersionForSchemaChange(ctx); err != nil {
		return err
	}
	if err := schemautil.ValidateSchemaEvolution(oldColl.ToCollectionSchemaPB(), newSchema); err != nil {
		return err
	}
	return nil
}

func (c *Core) broadcastValidatedSchemaChange(
	ctx context.Context,
	bc broadcaster.BroadcastAPI,
	msg message.BroadcastMutableMessage,
) error {
	if _, err := bc.Broadcast(ctx, msg); err != nil {
		return err
	}
	return nil
}
