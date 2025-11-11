package rootcoord

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message/ce"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// broadcastAlterCollectionForAlterCollection broadcasts the put collection message for alter collection.
func (c *Core) broadcastAlterCollectionForAlterCollection(ctx context.Context, req *milvuspb.AlterCollectionRequest) error {
	if req.GetCollectionName() == "" {
		return merr.WrapErrParameterInvalidMsg("alter collection failed, collection name does not exists")
	}

	if len(req.GetProperties()) == 0 && len(req.GetDeleteKeys()) == 0 {
		return merr.WrapErrParameterInvalidMsg("no properties or delete keys provided")
	}

	if len(req.GetProperties()) > 0 && len(req.GetDeleteKeys()) > 0 {
		return merr.WrapErrParameterInvalidMsg("can not provide properties and deletekeys at the same time")
	}

	if hookutil.ContainsCipherProperties(req.GetProperties(), req.GetDeleteKeys()) {
		return merr.WrapErrParameterInvalidMsg("can not alter cipher related properties")
	}

	if funcutil.SliceContain(req.GetDeleteKeys(), common.EnableDynamicSchemaKey) {
		return merr.WrapErrParameterInvalidMsg("cannot delete key %s, dynamic field schema could support set to true/false", common.EnableDynamicSchemaKey)
	}

	// Validate timezone
	tz, exist := funcutil.TryGetAttrByKeyFromRepeatedKV(common.TimezoneKey, req.GetProperties())
	if exist && !funcutil.IsTimezoneValid(tz) {
		return merr.WrapErrParameterInvalidMsg("unknown or invalid IANA Time Zone ID: %s", tz)
	}

	isEnableDynamicSchema, targetValue, err := common.IsEnableDynamicSchema(req.GetProperties())
	if err != nil {
		return merr.WrapErrParameterInvalidMsg("invalid dynamic schema property value: %s", req.GetProperties()[0].GetValue())
	}
	if isEnableDynamicSchema {
		// if there's dynamic schema property, it will add a new dynamic field into the collection.
		// the property cannot be seen at collection properties, only add a new field into the collection.
		return c.broadcastAlterCollectionForAlterDynamicField(ctx, req, targetValue)
	}

	broadcaster, err := c.startBroadcastWithAliasOrCollectionLock(ctx, req.GetDbName(), req.GetCollectionName())
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	// check if the collection exists
	coll, err := c.meta.GetCollectionByName(ctx, req.GetDbName(), req.GetCollectionName(), typeutil.MaxTimestamp)
	if err != nil {
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
			Paths: []string{},
		},
		CacheExpirations: cacheExpirations,
	}
	udpates := &messagespb.AlterCollectionMessageUpdates{}

	// Apply the properties to override the existing properties.
	newProperties := common.CloneKeyValuePairs(coll.Properties).ToMap()
	for _, prop := range req.GetProperties() {
		switch prop.GetKey() {
		case common.CollectionDescription:
			if prop.GetValue() != coll.Description {
				udpates.Description = prop.GetValue()
				header.UpdateMask.Paths = append(header.UpdateMask.Paths, message.FieldMaskCollectionDescription)
			}
		case common.ConsistencyLevel:
			if lv, ok := unmarshalConsistencyLevel(prop.GetValue()); ok && lv != coll.ConsistencyLevel {
				udpates.ConsistencyLevel = lv
				header.UpdateMask.Paths = append(header.UpdateMask.Paths, message.FieldMaskCollectionConsistencyLevel)
			}
		default:
			newProperties[prop.GetKey()] = prop.GetValue()
		}
	}
	for _, deleteKey := range req.GetDeleteKeys() {
		delete(newProperties, deleteKey)
	}
	// Check if the properties are changed.
	newPropsKeyValuePairs := common.NewKeyValuePairs(newProperties)
	if !newPropsKeyValuePairs.Equal(coll.Properties) {
		udpates.Properties = newPropsKeyValuePairs
		header.UpdateMask.Paths = append(header.UpdateMask.Paths, message.FieldMaskCollectionProperties)
	}

	// if there's no change, return nil directly to promise idempotent.
	if len(header.UpdateMask.Paths) == 0 {
		return errIgnoredAlterCollection
	}

	// fill the put load config if rg or replica number is changed.
	udpates.AlterLoadConfig = c.getAlterLoadConfigOfAlterCollection(coll.Properties, udpates.Properties)

	channels := make([]string, 0, len(coll.VirtualChannelNames)+1)
	channels = append(channels, streaming.WAL().ControlChannel())
	channels = append(channels, coll.VirtualChannelNames...)
	msg := message.NewAlterCollectionMessageBuilderV2().
		WithHeader(header).
		WithBody(&messagespb.AlterCollectionMessageBody{
			Updates: udpates,
		}).
		WithBroadcast(channels).
		MustBuildBroadcast()
	if _, err := broadcaster.Broadcast(ctx, msg); err != nil {
		return err
	}
	return nil
}

// broadcastAlterCollectionForAlterDynamicField broadcasts the put collection message for alter dynamic field.
func (c *Core) broadcastAlterCollectionForAlterDynamicField(ctx context.Context, req *milvuspb.AlterCollectionRequest, targetValue bool) error {
	if len(req.GetProperties()) != 1 {
		return merr.WrapErrParameterInvalidMsg("cannot alter dynamic schema with other properties at the same time")
	}
	broadcaster, err := c.startBroadcastWithAliasOrCollectionLock(ctx, req.GetDbName(), req.GetCollectionName())
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	coll, err := c.meta.GetCollectionByName(ctx, req.GetDbName(), req.GetCollectionName(), typeutil.MaxTimestamp)
	if err != nil {
		return err
	}

	// return nil for no-op
	if coll.EnableDynamicField == targetValue {
		return errIgnoredAlterCollection
	}

	// not support disabling since remove field not support yet.
	if !targetValue {
		return merr.WrapErrParameterInvalidMsg("dynamic schema cannot supported to be disabled")
	}

	// convert to add $meta json field, nullable, default value `{}`
	fieldSchema := &schemapb.FieldSchema{
		Name:      common.MetaFieldName,
		DataType:  schemapb.DataType_JSON,
		IsDynamic: true,
		Nullable:  true,
		DefaultValue: &schemapb.ValueField{
			Data: &schemapb.ValueField_BytesData{
				BytesData: []byte("{}"),
			},
		},
	}
	if err := checkFieldSchema([]*schemapb.FieldSchema{fieldSchema}); err != nil {
		return err
	}

	fieldSchema.FieldID = nextFieldID(coll)
	schema := &schemapb.CollectionSchema{
		Name:               coll.Name,
		Description:        coll.Description,
		AutoID:             coll.AutoID,
		Fields:             model.MarshalFieldModels(coll.Fields),
		StructArrayFields:  model.MarshalStructArrayFieldModels(coll.StructArrayFields),
		Functions:          model.MarshalFunctionModels(coll.Functions),
		EnableDynamicField: targetValue,
		Properties:         coll.Properties,
		Version:            coll.SchemaVersion + 1,
	}
	schema.Fields = append(schema.Fields, fieldSchema)

	channels := make([]string, 0, len(coll.VirtualChannelNames)+1)
	channels = append(channels, streaming.WAL().ControlChannel())
	channels = append(channels, coll.VirtualChannelNames...)
	cacheExpirations, err := c.getCacheExpireForCollection(ctx, req.GetDbName(), req.GetCollectionName())
	if err != nil {
		return err
	}
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

// getCacheExpireForCollection gets the cache expirations for collection.
func (c *Core) getCacheExpireForCollection(ctx context.Context, dbName string, collectionNameOrAlias string) (*message.CacheExpirations, error) {
	coll, err := c.meta.GetCollectionByName(ctx, dbName, collectionNameOrAlias, typeutil.MaxTimestamp)
	if err != nil {
		return nil, err
	}
	aliases, err := c.meta.ListAliases(ctx, dbName, coll.Name, typeutil.MaxTimestamp)
	if err != nil {
		return nil, err
	}
	builder := ce.NewBuilder()
	builder.WithLegacyProxyCollectionMetaCache(
		ce.OptLPCMDBName(dbName),
		ce.OptLPCMCollectionName(coll.Name),
		ce.OptLPCMCollectionID(coll.CollectionID),
		ce.OptLPCMMsgType(commonpb.MsgType_AlterCollection),
	)
	for _, alias := range aliases {
		builder.WithLegacyProxyCollectionMetaCache(
			ce.OptLPCMDBName(dbName),
			ce.OptLPCMCollectionName(alias),
			ce.OptLPCMCollectionID(coll.CollectionID),
			ce.OptLPCMMsgType(commonpb.MsgType_AlterAlias),
		)
	}
	return builder.Build(), nil
}

// getAlterLoadConfigOfAlterCollection gets the alter load config of alter collection.
func (c *Core) getAlterLoadConfigOfAlterCollection(oldProps []*commonpb.KeyValuePair, newProps []*commonpb.KeyValuePair) *message.AlterLoadConfigOfAlterCollection {
	oldReplicaNumber, _ := common.CollectionLevelReplicaNumber(oldProps)
	oldResourceGroups, _ := common.CollectionLevelResourceGroups(oldProps)
	newReplicaNumber, _ := common.CollectionLevelReplicaNumber(newProps)
	newResourceGroups, _ := common.CollectionLevelResourceGroups(newProps)
	left, right := lo.Difference(oldResourceGroups, newResourceGroups)
	rgChanged := len(left) > 0 || len(right) > 0
	replicaChanged := oldReplicaNumber != newReplicaNumber
	if !replicaChanged && !rgChanged {
		return nil
	}

	return &message.AlterLoadConfigOfAlterCollection{
		ReplicaNumber:  int32(newReplicaNumber),
		ResourceGroups: newResourceGroups,
	}
}

func (c *DDLCallback) alterCollectionV2AckCallback(ctx context.Context, result message.BroadcastResultAlterCollectionMessageV2) error {
	header := result.Message.Header()
	body := result.Message.MustBody()
	if err := c.meta.AlterCollection(ctx, result); err != nil {
		if errors.Is(err, errAlterCollectionNotFound) {
			log.Ctx(ctx).Warn("alter a non-existent collection, ignore it", log.FieldMessage(result.Message))
			return nil
		}
		return errors.Wrap(err, "failed to alter collection")
	}
	if body.Updates.AlterLoadConfig != nil {
		resp, err := c.mixCoord.UpdateLoadConfig(ctx, &querypb.UpdateLoadConfigRequest{
			CollectionIDs:  []int64{header.CollectionId},
			ReplicaNumber:  body.Updates.AlterLoadConfig.ReplicaNumber,
			ResourceGroups: body.Updates.AlterLoadConfig.ResourceGroups,
		})
		if err := merr.CheckRPCCall(resp, err); err != nil {
			return errors.Wrap(err, "failed to update load config")
		}
	}
	if err := c.broker.BroadcastAlteredCollection(ctx, header.CollectionId); err != nil {
		return errors.Wrap(err, "failed to broadcast altered collection")
	}
	return c.ExpireCaches(ctx, header, result.GetControlChannelResult().TimeTick)
}
