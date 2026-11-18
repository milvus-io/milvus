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

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message/messageutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

type schemaLoadFieldsUpdateFunc func(context.Context, int64, *schemapb.CollectionSchema, *schemapb.CollectionSchema) error

// SetSchemaLoadFieldsUpdater installs Coordinator's in-process load metadata
// updater before WAL callbacks start. It must not broadcast or acquire the
// collection broadcast lock: the original ALTER already owns that lock.
func (c *Core) SetSchemaLoadFieldsUpdater(updater func(context.Context, int64, *schemapb.CollectionSchema, *schemapb.CollectionSchema) error) {
	c.schemaLoadFieldsUpdater = updater
}

// Update load metadata before publishing the schema. If either write fails,
// the original ALTER callback retries against the same preceding public schema.
// This also runs on a CDC secondary; it derives local load state without an
// additional RPC, WAL message, or cluster-local ID in the replicated payload.
func (c *DDLCallback) updateSchemaLoadFields(ctx context.Context, result message.BroadcastResultAlterCollectionMessageV2) error {
	if c.schemaLoadFieldsUpdater == nil || !messageutil.IsSchemaChange(result.Message.Header()) {
		return nil
	}
	id := result.Message.Header().GetCollectionId()
	published, err := c.meta.GetCollectionByIDWithMaxTs(ctx, id)
	if err != nil {
		if errors.Is(err, merr.ErrCollectionNotFound) {
			return nil
		}
		return merr.Wrap(err, "get preceding schema for load fields")
	}
	target := result.Message.MustBody().GetUpdates().GetSchema()
	if err := c.schemaLoadFieldsUpdater(ctx, id, published.ToCollectionSchemaPB(), target); err != nil {
		return merr.Wrap(err, "update load fields for collection schema")
	}
	return nil
}
