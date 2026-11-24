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

package shard

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func (impl *shardInterceptor) handleUpdateMessage(ctx context.Context, msg message.MutableMessage, appendOp interceptors.Append) (message.MessageID, error) {
	update, err := message.AsMutableUpdateMessageV2(msg)
	if err != nil {
		return nil, status.NewInvalidArgument("invalid UPDATE message: %s", err)
	}
	h := update.Header()
	if h.SchemaVersion == nil {
		return nil, status.NewInvalidArgument("UPDATE requires an explicit schema version")
	}
	if h.GetCollectionId() <= 0 || funcutil.GetCollectionIDFromVChannel(msg.VChannel()) != h.GetCollectionId() {
		return nil, status.NewInvalidArgument("UPDATE collection does not match its VChannel")
	}
	// Reuse the existing explicit-version checker; no insert or segment
	// allocation occurs. The enclosing WAL lock fences concurrent schema DDL.
	version, err := impl.shardManager.CheckIfCollectionSchemaVersionMatch(&message.InsertMessageHeader{
		CollectionId: h.GetCollectionId(), SchemaVersion: h.SchemaVersion,
	})
	if errors.Is(err, shards.ErrCollectionSchemaVersionNotMatch) {
		return nil, status.NewSchemaVersionMismatch("update schema version %d differs from current version %d", h.GetSchemaVersion(), version)
	}
	if err != nil {
		return nil, status.NewUnrecoverableError("cannot validate update schema: %s", err)
	}
	body, err := update.Body(ctx)
	if err != nil {
		if errors.Is(err, message.ErrMalformedBody) {
			return nil, status.NewInvalidArgument("invalid UPDATE payload: %s", err)
		}
		return nil, err
	}
	if body.GetPartitionId() != common.AllPartitionsID {
		if body.GetPartitionId() <= 0 {
			return nil, status.NewInvalidArgument("invalid UPDATE partition scope")
		}
		if err := impl.shardManager.CheckIfPartitionExists(shards.PartitionUniqueKey{CollectionID: h.GetCollectionId(), PartitionID: body.GetPartitionId()}); err != nil {
			return nil, status.NewUnrecoverableError("cannot validate update partition: %s", err)
		}
	}
	for _, assignment := range body.GetAssignments() {
		literal := assignment.GetLiteral()
		if literal.GetValue() == nil {
			return nil, status.NewInvalidArgument("UPDATE assignment requires an explicit literal")
		}
		if n, ok := literal.GetValue().(*schemapb.UpdateLiteral_SetNull); ok && !n.SetNull {
			return nil, status.NewInvalidArgument("UPDATE set_null must be true")
		}
	}
	// Replicated updates bypass the target Proxy's field eligibility checks.
	if len(body.GetAssignments()) > 0 {
		schema := impl.shardManager.GetCollectionSchemaSnapshot(h.GetCollectionId())
		if schema == nil {
			return nil, status.NewUnrecoverableError("cannot validate UPDATE fields: collection schema is unavailable")
		}
		fields := lo.KeyBy(typeutil.GetAllFieldSchemas(schema), (*schemapb.FieldSchema).GetFieldID)
		restricted := typeutil.PKUpdateFieldRestriction(schema)
		for _, assignment := range body.GetAssignments() {
			field := fields[assignment.GetFieldId()]
			if field == nil {
				return nil, status.NewInvalidArgument("UPDATE references unknown field ID %d", assignment.GetFieldId())
			}
			if restricted(field) {
				return nil, status.NewInvalidArgument("field %s cannot be updated", field.GetName())
			}
			if !typeutil.SupportsPKUpdateType(field) {
				return nil, status.NewInvalidArgument("field %s has unsupported update type %s", field.GetName(), field.GetDataType())
			}
		}
	}
	return appendOp(ctx, msg)
}
