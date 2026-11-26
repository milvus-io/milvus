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

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/timestamptz"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// broadcastAlterCollectionForAddField broadcasts the put collection message for add field.
func (c *Core) broadcastAlterCollectionForAddField(ctx context.Context, req *milvuspb.AddCollectionFieldRequest) error {
	broadcaster, err := c.startBroadcastWithAliasOrCollectionLock(ctx, req.GetDbName(), req.GetCollectionName())
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	// check if the collection is created.
	coll, err := c.meta.GetCollectionByName(ctx, req.GetDbName(), req.GetCollectionName(), typeutil.MaxTimestamp)
	if err != nil {
		return err
	}

	// check if the field schema is illegal.
	fieldSchema := &schemapb.FieldSchema{}
	if err = proto.Unmarshal(req.Schema, fieldSchema); err != nil {
		return merr.Wrap(err, "failed to unmarshal field schema")
	}
	if err := checkFieldSchema([]*schemapb.FieldSchema{fieldSchema}); err != nil {
		return merr.Wrap(err, "failed to check field schema")
	}
	if fieldSchema.GetDataType() == schemapb.DataType_Timestamptz {
		timezone, exist := funcutil.TryGetAttrByKeyFromRepeatedKV(common.TimezoneKey, coll.Properties)
		if !exist {
			timezone = common.DefaultTimezone
		}
		if err := timestamptz.CheckAndRewriteTimestampTzDefaultValueForFieldSchema(fieldSchema, timezone); err != nil {
			return merr.WrapErrParameterInvalidErr(err, "invalid default value of field, name: %s", fieldSchema.Name)
		}
	}

	// check if the field already exists
	for _, field := range coll.Fields {
		if field.Name == fieldSchema.Name {
			// TODO: idempotency check here.
			return merr.WrapErrParameterInvalidMsg("field already exists, name: %s", fieldSchema.Name)
		}
	}

	// assign a new field id.
	fieldSchema.FieldID = nextFieldID(coll)
	// build new collection schema.
	schema := coll.ToCollectionSchemaPB()
	schema.Version = coll.SchemaVersion + 1
	schema.Fields = append(schema.Fields, fieldSchema)

	if err := c.validateChangedAnalyzers(ctx, schema, coll.ToCollectionSchemaPB()); err != nil {
		return err
	}
	cacheExpirations, err := c.getCacheExpireForCollection(ctx, req.GetDbName(), req.GetCollectionName())
	if err != nil {
		return err
	}

	channels := make([]string, 0, len(coll.VirtualChannelNames)+1)
	channels = append(channels, streaming.WAL().ControlChannel())
	channels = append(channels, coll.VirtualChannelNames...)
	// broadcast the put collection v2 message.
	msg := message.NewAlterCollectionMessageBuilderV2().
		WithHeader(&messagespb.AlterCollectionMessageHeader{
			DbId:         coll.DBID,
			CollectionId: coll.CollectionID,
			UpdateMask: &fieldmaskpb.FieldMask{
				Paths: []string{message.FieldMaskCollectionSchema},
			},
			CacheExpirations: cacheExpirations,
		}).
		WithBody(&messagespb.AlterCollectionMessageBody{
			Updates: &messagespb.AlterCollectionMessageUpdates{
				Schema: schema,
			},
		}).
		WithBroadcast(channels).
		MustBuildBroadcast()
	if _, err := broadcaster.Broadcast(ctx, msg); err != nil {
		return err
	}
	return nil
}
