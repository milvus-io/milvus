package rootcoord

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message/ce"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
	"github.com/samber/lo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// broadcastAlterCollectionV2ForAlterCollection broadcasts the put collection v2 message for alter collection.
func (c *Core) broadcastAlterCollectionV2ForAlterCollection(ctx context.Context, req *milvuspb.AlterCollectionRequest) error {
	if len(req.GetProperties()) == 0 && len(req.GetDeleteKeys()) == 0 {
		return errors.New("The collection properties to alter and keys to delete must not be empty at the same time")
	}

	if len(req.GetProperties()) > 0 && len(req.GetDeleteKeys()) > 0 {
		return errors.New("can not provide properties and deletekeys at the same time")
	}

	if hookutil.ContainsCipherProperties(req.GetProperties(), req.GetDeleteKeys()) {
		log.Info("skip to alter collection due to cipher properties were detected in the properties")
		return errors.New("can not alter cipher related properties")
	}

	ok, targetValue, err := common.IsEnableDynamicSchema(req.GetProperties())
	if err != nil {
		return merr.WrapErrParameterInvalidMsg("invalid dynamic schema property value: %s", req.GetProperties()[0].GetValue())
	}
	if ok {
		// if there's dynamic schema property, it will add a new dynamic field into the collection.
		// the property cannot be seen at collection properties, only add a new field into the collection.
		return c.broadcastAlterCollectionV2ForAlterDynamicField(ctx, req, targetValue)
	}

	broadcaster, err := broadcast.StartBroadcastWithResourceKeys(ctx,
		message.NewSharedDBNameResourceKey(req.GetDbName()),
		message.NewExclusiveCollectionNameResourceKey(req.GetDbName(), req.GetCollectionName()),
	)
	if err != nil {
		return err
	}

	coll, err := c.meta.GetCollectionByName(ctx, req.DbName, req.CollectionName, typeutil.MaxTimestamp)
	if err != nil {
		return err
	}
	if coll.State != etcdpb.CollectionState_CollectionCreated {
		return errors.Errorf("collection is not created, can not alter collection, state: %s", coll.State.String())
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
		return nil
	}

	// fill the put load config if rg or replica number is changed.
	udpates.AlterLoadConfig = c.getPutLoadConfigOfPutCollection(ctx, coll.Properties, udpates.Properties)

	channels := make([]string, 0, len(coll.VirtualChannelNames)+1)
	channels = append(channels, streaming.WAL().ControlChannel())
	for _, vchannel := range coll.VirtualChannelNames {
		channels = append(channels, vchannel)
	}
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

// broadcastAlterCollectionV2ForAlterDynamicField broadcasts the put collection v2 message for alter dynamic field.
func (c *Core) broadcastAlterCollectionV2ForAlterDynamicField(ctx context.Context, req *milvuspb.AlterCollectionRequest, targetValue bool) error {
	if len(req.GetProperties()) != 1 {
		return merr.WrapErrParameterInvalidMsg("cannot alter dynamic schema with other properties")
	}

	broadcaster, err := broadcast.StartBroadcastWithResourceKeys(ctx,
		message.NewSharedDBNameResourceKey(req.GetDbName()),
		message.NewExclusiveCollectionNameResourceKey(req.GetDbName(), req.GetCollectionName()),
	)
	if err != nil {
		return err
	}

	coll, err := c.meta.GetCollectionByName(ctx, req.GetDbName(), req.GetCollectionName(), typeutil.MaxTimestamp)
	if err != nil {
		log.Ctx(ctx).Warn("get collection failed during alter dynamic schema",
			zap.String("collectionName", req.GetCollectionName()))
		return err
	}

	// return nil for no-op
	if coll.EnableDynamicField == targetValue {
		return nil
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
	}
	schema.Fields = append(schema.Fields, fieldSchema)

	channels := make([]string, 0, len(coll.VirtualChannelNames)+1)
	channels = append(channels, streaming.WAL().ControlChannel())
	for _, vchannel := range coll.VirtualChannelNames {
		channels = append(channels, vchannel)
	}
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
func (c *Core) getCacheExpireForCollection(ctx context.Context, dbName string, collectionName string) (*message.CacheExpirations, error) {
	coll, err := c.meta.GetCollectionByName(ctx, dbName, collectionName, typeutil.MaxTimestamp)
	if err != nil {
		return nil, err
	}
	aliases, err := c.meta.ListAliases(ctx, dbName, collectionName, typeutil.MaxTimestamp)
	if err != nil {
		return nil, err
	}
	builder := ce.NewBuilder()
	builder.WithLegacyProxyCollectionMetaCache(
		ce.OptLPCMDBName(dbName),
		ce.OptLPCMCollectionName(collectionName),
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

// getPutLoadConfigOfPutCollection gets the put load config of put collection.
func (c *Core) getPutLoadConfigOfPutCollection(ctx context.Context, oldProps []*commonpb.KeyValuePair, newProps []*commonpb.KeyValuePair) *message.AlterLoadConfigOfAlterCollection {
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

func (c *DDLCallback) putCollectionV2AckCallback(ctx context.Context, result message.BroadcastResultAlterCollectionMessageV2) error {
	header := result.Message.Header()
	body := result.Message.MustBody()
	if err := c.meta.AlterCollection(ctx, result); err != nil {
		return err
	}

	if body.Updates.AlterLoadConfig != nil {
		resp, err := c.mixCoord.UpdateLoadConfig(ctx, &querypb.UpdateLoadConfigRequest{
			CollectionIDs:  []int64{header.CollectionId},
			ReplicaNumber:  body.Updates.AlterLoadConfig.ReplicaNumber,
			ResourceGroups: body.Updates.AlterLoadConfig.ResourceGroups,
		})
		return merr.CheckRPCCall(resp, err)
	}
	if err := c.ExpireCaches(ctx, header, result.GetControlChannelResult().TimeTick); err != nil {
		return err
	}
	return nil
}
