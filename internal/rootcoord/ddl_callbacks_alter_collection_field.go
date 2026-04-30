package rootcoord

import (
	"context"

	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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
	if newFieldProperties.Equal(oldFieldProperties) && desc == nil {
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
