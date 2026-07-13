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

// broadcastSchemaChange is the SINGLE admission + broadcast path for every schema-mutating
// DDL (add/drop field, add struct field, add/drop/alter function, enable/disable dynamic
// field). It runs the structural admission gate (schemautil.ValidateSchemaEvolution) on
// newSchema against the committed schema, and only if that passes does it put the already
// built message on the WAL.
//
// Every schema-changing callback broadcasts through here instead of calling the broadcaster
// directly. That is what makes the gate impossible to bypass: no callback can land a schema
// change on the WAL that the gate has not vetted, and a callback added later gets the gate
// for free instead of having to remember to insert it (the bug this replaced -- add-field,
// add-struct-field and the dynamic-field paths each reached the WAL ungated).
func (c *Core) broadcastSchemaChange(
	ctx context.Context,
	bc broadcaster.BroadcastAPI,
	oldColl *model.Collection,
	newSchema *schemapb.CollectionSchema,
	msg message.BroadcastMutableMessage,
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
	if _, err := bc.Broadcast(ctx, msg); err != nil {
		return err
	}
	return nil
}
