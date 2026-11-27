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

	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func (c *Core) broadcastAlterCollectionV2ForAlterCollectionField(ctx context.Context, req *milvuspb.AlterCollectionFieldRequest) error {
	broadcaster, err := c.startBroadcastWithAliasOrCollectionLock(ctx, req.GetDbName(), req.GetCollectionName())
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	coll, err := c.meta.GetCollectionByName(ctx, req.GetDbName(), req.GetCollectionName(), typeutil.MaxTimestamp)
	if err != nil {
		return err
	}

	oldFieldProperties, err := GetFieldProperties(coll, req.GetFieldName())
	if err != nil {
		return err
	}
	oldFieldPropertiesMap := common.CloneKeyValuePairs(oldFieldProperties).ToMap()
	var desc *commonpb.KeyValuePair = nil
	for _, prop := range req.GetProperties() {
		// field.description is a special property to change field's description, skip it here, apply it later.
		if prop.GetKey() == common.FieldDescriptionKey {
			desc = prop
			continue
		}
		oldFieldPropertiesMap[prop.GetKey()] = prop.GetValue()
	}

	for _, deleteKey := range req.GetDeleteKeys() {
		delete(oldFieldPropertiesMap, deleteKey)
	}

	newFieldProperties := common.NewKeyValuePairs(oldFieldPropertiesMap)
	propertiesChanged := !newFieldProperties.Equal(oldFieldProperties)
	if !propertiesChanged && desc == nil {
		// if there's no change, return nil directly to promise idempotent.
		return errIgnoredAlterCollection
	}

	// build new collection schema.
	schema := coll.ToCollectionSchemaPB()
	schema.Version = coll.SchemaVersion + 1
	for _, field := range schema.Fields {
		if field.Name == req.GetFieldName() {
			field.TypeParams = newFieldProperties
			if desc != nil {
				field.Description = desc.GetValue()
			}
			break
		}
	}
	if err := c.validateChangedAnalyzers(ctx, schema, coll.ToCollectionSchemaPB()); err != nil {
		return err
	}
	cacheExpirations, err := c.getCacheExpireForCollection(ctx, req.GetDbName(), req.GetCollectionName())
	if err != nil {
		return err
	}

	header := &messagespb.AlterCollectionMessageHeader{
		DbId:         coll.DBID,
		CollectionId: coll.CollectionID,
		UpdateMask: &fieldmaskpb.FieldMask{
			Paths: []string{message.FieldMaskCollectionSchema},
		},
		CacheExpirations: cacheExpirations,
	}
	body := &messagespb.AlterCollectionMessageBody{
		Updates: &messagespb.AlterCollectionMessageUpdates{
			Schema: schema,
		},
	}

	channels := make([]string, 0, len(coll.VirtualChannelNames)+1)
	channels = append(channels, streaming.WAL().ControlChannel())
	channels = append(channels, coll.VirtualChannelNames...)
	msg := message.NewAlterCollectionMessageBuilderV2().
		WithHeader(header).
		WithBody(body).
		WithBroadcast(channels).
		MustBuildBroadcast()
	if _, err := broadcaster.Broadcast(ctx, msg); err != nil {
		return err
	}
	return nil
}
