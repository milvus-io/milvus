package rootcoord

import (
	"context"
	"strconv"

	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	streamingbroadcaster "github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func hasAnalyzerFieldParamMutation(req *milvuspb.AlterCollectionFieldRequest) bool {
	for _, prop := range req.GetProperties() {
		if prop.GetKey() == common.EnableAnalyzerKey || prop.GetKey() == common.AnalyzerParamKey {
			return true
		}
	}
	for _, key := range req.GetDeleteKeys() {
		if key == common.EnableAnalyzerKey || key == common.AnalyzerParamKey {
			return true
		}
	}
	return false
}

func validateAlterCollectionFieldAnalyzerParams(req *milvuspb.AlterCollectionFieldRequest) error {
	for _, prop := range req.GetProperties() {
		if prop.GetKey() != common.EnableAnalyzerKey {
			continue
		}
		if _, err := strconv.ParseBool(prop.GetValue()); err != nil {
			return merr.WrapErrParameterInvalidMsg("%s should be a boolean, but got %s", prop.GetKey(), prop.GetValue())
		}
	}
	return nil
}

func getAlterCollectionField(schema *schemapb.CollectionSchema, fieldName string) *schemapb.FieldSchema {
	for _, field := range schema.GetFields() {
		if field.GetName() == fieldName {
			return field
		}
	}
	return nil
}

func validateAlterCollectionFieldAnalyzerMutation(schema *schemapb.CollectionSchema, fieldName string) error {
	field := getAlterCollectionField(schema, fieldName)
	if field == nil {
		return merr.WrapErrParameterInvalidMsg("field not found: %s", fieldName)
	}
	if !typeutil.IsStringType(field.GetDataType()) {
		return merr.WrapErrParameterInvalidMsg("can not alter analyzer params for non-string field %s", fieldName)
	}
	fieldHelper := typeutil.CreateFieldSchemaHelper(field)
	if fieldHelper.EnableMatch() || typeutil.IsBm25FunctionInputField(schema, field) {
		return merr.WrapErrParameterInvalidMsg(
			"can not alter analyzer params for field %s after text match is enabled or BM25 function depends on it",
			fieldName,
		)
	}
	if fieldHelper.HasAnalyzerParams() && !fieldHelper.EnableAnalyzer() {
		return merr.WrapErrParameterInvalidMsg("field %s with analyzer_params must also set enable_analyzer to true", fieldName)
	}
	return nil
}

func (c *Core) broadcastAlterCollectionV2ForAlterCollectionField(ctx context.Context, req *milvuspb.AlterCollectionFieldRequest) error {
	broadcastAPI, err := c.startBroadcastWithAliasOrCollectionLock(ctx, req.GetDbName(), req.GetCollectionName())
	if err != nil {
		return err
	}
	defer broadcastAPI.Close()

	coll, err := c.meta.GetCollectionByName(ctx, req.GetDbName(), req.GetCollectionName(), typeutil.MaxTimestamp, false)
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

	analyzerFieldParamMutated := hasAnalyzerFieldParamMutation(req)
	if analyzerFieldParamMutated {
		if err := validateAlterCollectionFieldAnalyzerParams(req); err != nil {
			return err
		}
		if err := validateAlterCollectionFieldAnalyzerMutation(schema, req.GetFieldName()); err != nil {
			return err
		}
		fileResourceIds, err := c.validateSchemaAnalyzerFileResources(ctx, schema, includeInactiveAnalyzer)
		if err != nil {
			return err
		}
		schema.FileResourceIds = fileResourceIds
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
	addedFileResourceIds := []int64(nil)
	if analyzerFieldParamMutated {
		addedFileResourceIds, _ := diffFileResourceIDs(coll.FileResourceIds, schema.FileResourceIds)
		// Reserve newly referenced file resources immediately before handing the
		// message to the broadcaster. MetaTable.AlterCollection consumes the
		// reservation when the schema is applied; replay/replicated tasks without
		// a local reservation add the refCnt there.
		if err := reserveAlterCollectionFileResourceRefs(c.meta, coll.CollectionID, addedFileResourceIds); err != nil {
			return err
		}
	}
	if _, err := broadcastAPI.Broadcast(ctx, msg); err != nil {
		if streamingbroadcaster.IsBroadcastTaskNotCreated(err) {
			rollbackAlterCollectionFileResourceRefs(ctx, c.meta, coll.CollectionID, addedFileResourceIds)
		}
		return err
	}
	return nil
}
