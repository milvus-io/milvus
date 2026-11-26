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

package dql

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/agg"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	typeutil2 "github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/contextutil"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/timestamptz"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// strongTS / boundedTS are magic guarantee-timestamp sentinels for the
// Bounded and Strong consistency mappings.
const (
	strongTS  = 0
	boundedTS = 2
)

// isAlpha returns true if the byte is an ASCII letter.
func isAlpha(c uint8) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

// isNumber returns true if the byte is an ASCII digit.
func isNumber(c uint8) bool {
	return c >= '0' && c <= '9'
}

// validateLimit checks that a search/query limit is within the configured range.
func validateLimit(limit int64, largeTopKEnabled bool) error {
	topKLimit := paramtable.Get().QuotaConfig.TopKLimit.GetAsInt64()
	if largeTopKEnabled {
		topKLimit = paramtable.Get().QuotaConfig.LargeTopKLimit.GetAsInt64()
	}
	if limit <= 0 || limit > topKLimit {
		return merr.WrapErrParameterInvalidMsg("it should be in range [1, %d], but got %d", topKLimit, limit)
	}
	return nil
}

// validatePartitionTag validates a partition tag string. It is duplicated here
// because the DDL and DML task groups also need it and each sub-package keeps
// its own copy of small shared helpers (see DEPENDENCIES.md).
func ValidatePartitionTag(partitionTag string, strictCheck bool) error {
	partitionTag = strings.TrimSpace(partitionTag)

	invalidMsg := "Invalid partition name: " + partitionTag + ". "
	if partitionTag == "" {
		msg := invalidMsg + "Partition name should not be empty."
		return merr.WrapErrParameterInvalidMsg("%s", msg)
	}
	if len(partitionTag) > paramtable.Get().ProxyCfg.MaxNameLength.GetAsInt() {
		msg := invalidMsg + "The length of a partition name must be less than " + paramtable.Get().ProxyCfg.MaxNameLength.GetValue() + " characters."
		return merr.WrapErrParameterInvalidMsg("%s", msg)
	}

	if strictCheck {
		firstChar := partitionTag[0]
		if firstChar != '_' && !isAlpha(firstChar) && !isNumber(firstChar) {
			msg := invalidMsg + "The first character of a partition name must be an underscore or letter."
			return merr.WrapErrParameterInvalidMsg("%s", msg)
		}

		tagSize := len(partitionTag)
		for i := 1; i < tagSize; i++ {
			c := partitionTag[i]
			if c != '_' && !isAlpha(c) && !isNumber(c) && c != '-' {
				msg := invalidMsg + "Partition name can only contain numbers, letters and underscores."
				return merr.WrapErrParameterInvalidMsg("%s", msg)
			}
		}
	}

	return nil
}

// namespaceForPlan returns nil when partition-mode namespacing is enabled so
// plan building skips the namespace field, else passes it through.
func NamespaceForPlan(schema *schemapb.CollectionSchema, namespace *string) *string {
	if NamespacePartitionModeEnabled(schema) {
		return nil
	}
	return namespace
}

// namespacePartitionModeEnabled reports whether the collection runs in
// partition-mode namespacing, which renders the plan namespace redundant.
func NamespacePartitionModeEnabled(schema *schemapb.CollectionSchema) bool {
	return schema != nil && schema.GetEnableNamespace() && common.IsNamespaceModePartition(schema.GetProperties()...)
}

func validateMaxQueryResultWindow(offset int64, limit int64, largeTopKEnabled bool) error {
	if offset < 0 {
		return merr.WrapErrParameterInvalidMsg("%s [%d] is invalid, should be gte than 0", OffsetKey, offset)
	}
	if limit <= 0 {
		return merr.WrapErrParameterInvalidMsg("%s [%d] is invalid, should be greater than 0", LimitKey, limit)
	}

	depth := offset + limit
	maxQueryResultWindow := paramtable.Get().QuotaConfig.MaxQueryResultWindow.GetAsInt64()
	if largeTopKEnabled {
		maxQueryResultWindow = paramtable.Get().QuotaConfig.LargeMaxQueryResultWindow.GetAsInt64()
	}
	if depth <= 0 || depth > maxQueryResultWindow {
		return merr.WrapErrParameterInvalidMsg("(offset+limit) should be in range [1, %d], but got %d", maxQueryResultWindow, depth)
	}
	return nil
}

func ValidateCollectionNameOrAlias(entity, entityType string) error {
	if entity == "" {
		return merr.WrapErrParameterInvalidMsg("collection %s should not be empty", entityType)
	}

	invalidMsg := fmt.Sprintf("Invalid collection %s: %s.", entityType, entity)
	if len(entity) > paramtable.Get().ProxyCfg.MaxNameLength.GetAsInt() {
		return merr.WrapErrParameterInvalidMsg("%s the length of a collection %s must be less than %s characters", invalidMsg, entityType,
			paramtable.Get().ProxyCfg.MaxNameLength.GetValue())
	}

	firstChar := entity[0]
	if firstChar != '_' && !isAlpha(firstChar) {
		return merr.WrapErrParameterInvalidMsg("%s the first character of a collection %s must be an underscore or letter", invalidMsg, entityType)
	}

	for i := 1; i < len(entity); i++ {
		c := entity[i]
		if c != '_' && !isAlpha(c) && !isNumber(c) {
			return merr.WrapErrParameterInvalidMsg("%s collection %s can only contain numbers, letters and underscores", invalidMsg, entityType)
		}
	}
	return nil
}

func ValidateCollectionName(collName string) error {
	return ValidateCollectionNameOrAlias(collName, "name")
}

// return value.
func translateOutputFields(outputFields []string, schema *schemaInfo, removePkField bool) ([]string, []string, []string, []agg.AggregateBase, bool, error) {
	b, err := bindOutputFields(outputFields, schema, removePkField)
	if err != nil {
		return nil, nil, nil, nil, false, err
	}
	return b.resultFieldNames, b.userOutputFields, b.userDynamicFields, b.aggregates, b.userRequestedPkFieldExplicitly, nil
}

// bindOutputFields parses every output_fields item with the selectItem grammar
// rule, resolves it against the schema, and builds the alias table. isSearch
// removes the primary key from the stored-field list (Search returns it as
// IDs) and applies the Search-only alias rules.
func bindOutputFields(outputFields []string, schema *schemaInfo, isSearch bool) (*outputBinding, error) {
	var primaryFieldName string
	allFieldNameMap := make(map[string]*schemapb.FieldSchema)
	structSubFieldNames := make(map[string]bool)
	resultFieldNameMap := make(map[string]bool)
	userOutputFieldsMap := make(map[string]bool)
	userDynamicFieldsMap := make(map[string]bool)
	useAllDyncamicFields := false
	b := &outputBinding{
		resultFieldNames:  make([]string, 0),
		userOutputFields:  make([]string, 0),
		userDynamicFields: make([]string, 0),
		aggregates:        make([]agg.AggregateBase, 0),
		projection:        &outputProjection{keepSource: make(map[string]bool)},
	}
	for _, field := range schema.Fields {
		if field.IsPrimaryKey {
			primaryFieldName = field.Name
		}
		allFieldNameMap[field.Name] = field
	}

	// User may specify a struct array field or some specific fields in the struct array field
	for _, subStruct := range schema.StructArrayFields {
		for _, field := range subStruct.Fields {
			allFieldNameMap[field.Name] = field
			structSubFieldNames[field.Name] = true
		}
	}

	structArrayNameToFields := make(map[string][]*schemapb.FieldSchema)
	for _, subStruct := range schema.StructArrayFields {
		structArrayNameToFields[subStruct.Name] = subStruct.Fields
	}

	names := newOutputNameSet()
	seenSource := make(map[string]bool)
	hasAlias := false

	for _, rawItem := range outputFields {
		trimmed := strings.TrimSpace(rawItem)
		if trimmed == "*" {
			b.userRequestedPkFieldExplicitly = true
			for fieldName, field := range allFieldNameMap {
				if schema.CanRetrieveRawFieldData(field) {
					resultFieldNameMap[fieldName] = true
					userOutputFieldsMap[fieldName] = true
					b.projection.keepSource[fieldName] = true
					if err := names.claim(fieldName, "field:"+fieldName, false); err != nil {
						return nil, err
					}
				}
			}
			useAllDyncamicFields = true
			b.sourceOutputFields = append(b.sourceOutputFields, rawItem)
			continue
		}

		// A name that is exactly a schema field or a struct array field needs
		// no parsing. This also keeps fields whose name is a grammar keyword
		// (created before such names were rejected) retrievable.
		item := planparserv2.SelectItem{Kind: planparserv2.SelectItemField, Source: trimmed}
		_, isField := allFieldNameMap[trimmed]
		_, isStructArray := structArrayNameToFields[trimmed]
		if !isField && !isStructArray {
			var err error
			item, err = planparserv2.ParseSelectItem(trimmed)
			if err != nil {
				mlog.Info(context.TODO(), "parse output field failed", mlog.String("output field", trimmed), mlog.Err(err))
				// A name that does not parse keeps the error text it had
				// before output fields went through the grammar; the parser's
				// message is appended.
				var syntaxErr *planparserv2.SelectItemSyntaxError
				if errors.As(err, &syntaxErr) {
					if schema.EnableDynamicField {
						return nil, merr.WrapErrParameterInvalidMsg("parse output field name failed: %s: %s", trimmed, syntaxErr.Detail)
					}
					return nil, merr.WrapErrParameterInvalidMsg("field %s not exist: %s", trimmed, syntaxErr.Detail)
				}
				return nil, err
			}
		}
		source := item.Source
		if item.Alias == "" || !seenSource[source] {
			if item.Alias == "" {
				b.sourceOutputFields = append(b.sourceOutputFields, rawItem)
			} else {
				b.sourceOutputFields = append(b.sourceOutputFields, source)
			}
		}
		seenSource[source] = true
		if item.Alias != "" {
			hasAlias = true
			if err := validateOutputAlias(item, primaryFieldName, isSearch); err != nil {
				return nil, err
			}
		}

		switch item.Kind {
		case planparserv2.SelectItemVirtualColumn:
			if err := checkVirtualColumnRequestKind(item, isSearch); err != nil {
				return nil, err
			}
			if !virtualColumnImplemented(item, isSearch) {
				return nil, merr.WrapErrParameterInvalidMsg("virtual column %s is not supported in output fields yet", canonicalVirtualColumnText(item))
			}
			if err := b.addVirtualColumn(names, userOutputFieldsMap, item); err != nil {
				return nil, err
			}
			continue

		case planparserv2.SelectItemGrepMatches:
			if isSearch {
				return nil, merr.WrapErrParameterInvalidMsg("%s is only available in a query", planparserv2.GrepMatchesFunctionName)
			}
			grepField, ok := allFieldNameMap[item.GrepField]
			if !ok {
				return nil, merr.WrapErrParameterInvalidMsg("field %s of %s does not exist", item.GrepField, planparserv2.GrepMatchesFunctionName)
			}
			if err := b.addGrepMatches(names, userOutputFieldsMap, item, grepField); err != nil {
				return nil, err
			}
			continue

		case planparserv2.SelectItemAggregate:
			aggregateName, aggFieldName := item.AggregateOp, item.AggregateArg
			if aggField, ok := allFieldNameMap[aggFieldName]; ok {
				aggFuncs, aggErr := agg.NewAggregate(aggregateName, aggField.GetFieldID(), source, aggField.GetDataType())
				if aggErr != nil {
					return nil, aggErr
				}
				b.aggregates = append(b.aggregates, aggFuncs...)
			} else if aggFieldName == "*" {
				// only count(*) is allowed
				if aggregateName != "count" {
					return nil, merr.WrapErrParameterInvalidMsg("%s(*) is not supported, only count(*) is allowed", aggregateName)
				}
				if err := agg.ValidateAggFieldType(aggregateName, schemapb.DataType_None); err != nil {
					return nil, err
				}
				aggFuncs, aggErr := agg.NewAggregate(aggregateName, 0, source, schemapb.DataType_None)
				if aggErr != nil {
					return nil, aggErr
				}
				b.aggregates = append(b.aggregates, aggFuncs...)
			} else {
				return nil, merr.WrapErrParameterInvalidMsg("target field %s for aggregation:%s does not exist", aggFieldName, aggregateName)
			}
			if err := b.addOutput(names, userOutputFieldsMap, source, item.Alias, "agg:"+source); err != nil {
				return nil, err
			}
			continue
		}

		// Field reference.
		if source == primaryFieldName {
			b.userRequestedPkFieldExplicitly = true
		}
		if structArrayField, ok := structArrayNameToFields[source]; ok {
			if item.Alias != "" {
				return nil, merr.WrapErrParameterInvalidMsg("struct array field %s cannot take an alias", source)
			}
			for _, field := range structArrayField {
				if schema.CanRetrieveRawFieldData(field) {
					resultFieldNameMap[field.Name] = true
					userOutputFieldsMap[field.Name] = true
					if err := names.claim(field.Name, "field:"+field.Name, false); err != nil {
						return nil, err
					}
				}
			}
			continue
		}
		if field, ok := allFieldNameMap[source]; ok {
			if !schema.CanRetrieveRawFieldData(field) {
				return nil, merr.WrapErrParameterInvalidMsg("not allowed to retrieve raw data of field %s", source)
			}
			if item.Alias != "" {
				if structSubFieldNames[source] {
					return nil, merr.WrapErrParameterInvalidMsg("struct sub-field %s cannot take an alias", source)
				}
				if field.GetIsDynamic() {
					return nil, merr.WrapErrParameterInvalidMsg("the dynamic field %s cannot take an alias", source)
				}
			}
			if field.GetIsDynamic() {
				b.projection.wholeDynamic = true
			}
			resultFieldNameMap[source] = true
			if err := b.addOutput(names, userOutputFieldsMap, source, item.Alias, "field:"+source); err != nil {
				return nil, err
			}
			continue
		}
		if !schema.EnableDynamicField {
			return nil, merr.WrapErrParameterInvalidMsg("field %s not exist", source)
		}
		dynamicNestedPath := source
		err := planparserv2.ParseIdentifier(schema.SchemaHelper, source, func(expr *planpb.Expr) error {
			columnInfo := expr.GetColumnExpr().GetInfo()
			// there must be no error here
			dynamicField, _ := schema.SchemaHelper.GetDynamicField()
			// only $meta["xxx"] is allowed for now
			if dynamicField.GetFieldID() != columnInfo.GetFieldId() {
				return merr.WrapErrParameterInvalidMsg("not support getting subkeys of json field yet")
			}
			nestedPaths := columnInfo.GetNestedPath()
			// $meta["A"]["B"] not allowed for now
			if len(nestedPaths) != 1 {
				return merr.WrapErrParameterInvalidMsg("not support getting multiple level of dynamic field for now")
			}
			// The key is the output name for both forms: "dyn_field" and
			// `$meta["dyn_field"]`.
			dynamicNestedPath = nestedPaths[0]
			return nil
		})
		if err != nil {
			mlog.Info(context.TODO(), "parse output field name failed", mlog.String("field name", source), mlog.Err(err))
			return nil, merr.WrapErrParameterInvalidMsg("parse output field name failed: %s", source)
		}
		if item.Alias != "" {
			return nil, merr.WrapErrParameterInvalidMsg("dynamic field key %s cannot take an alias", dynamicNestedPath)
		}
		if err := names.claim(dynamicNestedPath, "dynamic:"+dynamicNestedPath, false); err != nil {
			return nil, err
		}
		resultFieldNameMap[common.MetaFieldName] = true
		userOutputFieldsMap[dynamicNestedPath] = true
		userDynamicFieldsMap[dynamicNestedPath] = true
	}

	if useAllDyncamicFields {
		b.projection.wholeDynamic = true
	}
	if !hasAlias {
		// Keep the request's own list, untouched, for the aggregation field map.
		b.sourceOutputFields = outputFields
	}

	if isSearch {
		delete(resultFieldNameMap, primaryFieldName)
		delete(userOutputFieldsMap, primaryFieldName)
	} else {
		// A Query returns the primary key column in every row result, so an
		// alias of it is a second column, not a rename.
		b.projection.keepSource[primaryFieldName] = true
	}

	for fieldName := range resultFieldNameMap {
		b.resultFieldNames = append(b.resultFieldNames, fieldName)
	}
	for fieldName := range userOutputFieldsMap {
		b.userOutputFields = append(b.userOutputFields, fieldName)
	}
	if !useAllDyncamicFields {
		for fieldName := range userDynamicFieldsMap {
			b.userDynamicFields = append(b.userDynamicFields, fieldName)
		}
	}
	return b, nil
}

func IsPartitionKeyMode(ctx context.Context, metaCache Cache, dbName string, colName string) (bool, error) {
	colSchema, err := metaCache.GetCollectionSchema(ctx, dbName, colName)
	if err != nil {
		return false, err
	}

	for _, fieldSchema := range colSchema.GetFields() {
		if fieldSchema.IsPartitionKey {
			return true, nil
		}
	}

	return false, nil
}

func AssignNamespacePartitionKey(ctx context.Context, metaCache Cache, dbName string, collName string, schema *schemapb.CollectionSchema, namespace *string) ([]string, error) {
	if namespace == nil {
		return nil, nil
	}

	return AssignPartitionKeys(ctx, metaCache, dbName, collName, schema, []*planpb.GenericValue{
		{Val: &planpb.GenericValue_StringVal{StringVal: *namespace}},
	})
}

func NamespacePartitionKeyMode(schema *schemapb.CollectionSchema) bool {
	return common.UsesNamespacePartitionKey(schema)
}

func NamespacePartitionKeyModeEnabled(schema *schemapb.CollectionSchema) bool {
	return namespaceShardingEnabled(schema) && NamespacePartitionKeyMode(schema)
}

func resolveNamespacePartitionNames(schema *schemapb.CollectionSchema, namespace *string, partitionNames []string) ([]string, bool, error) {
	if err := common.CheckNamespace(schema, namespace); err != nil {
		return nil, false, err
	}
	if !NamespacePartitionModeEnabled(schema) {
		return partitionNames, false, nil
	}

	namespacePartitionName := *namespace
	if err := ValidatePartitionTag(namespacePartitionName, true); err != nil {
		return nil, true, err
	}
	if len(partitionNames) == 0 {
		return []string{namespacePartitionName}, true, nil
	}
	if len(partitionNames) == 1 && partitionNames[0] == namespacePartitionName {
		return partitionNames, true, nil
	}
	return nil, true, merr.WrapErrParameterInvalidMsg("partition names %v mismatch namespace %q", partitionNames, namespacePartitionName)
}

// filtering and result formatting use the same timezone.
func resolveTimezone(ctx context.Context, params []*commonpb.KeyValuePair, colInfo *collectionInfo) (string, error) {
	timezone, _ := funcutil.TryGetAttrByKeyFromRepeatedKV(common.TimezoneKey, params)
	if timezone != "" {
		if !timestamptz.IsTimezoneValid(timezone) {
			mlog.Info(ctx, "get invalid timezone from request", mlog.String("timezone", timezone))
			return "", merr.WrapErrParameterInvalidMsg("unknown or invalid IANA Time Zone ID: %s", timezone)
		}
		mlog.Debug(ctx, "determine timezone from request", mlog.String("user defined timezone", timezone))
		return timezone, nil
	}
	timezone = GetColTimezone(colInfo)
	mlog.Debug(ctx, "determine timezone from collection", mlog.String("collection timezone", timezone))
	return timezone, nil
}

func ValidateTextStorageV3Enabled(schema *schemapb.CollectionSchema) error {
	if err := typeutil.ValidateTextRequiresStorageV3(schema, paramtable.Get().CommonCfg.UseLoonFFI.GetAsBool()); err != nil {
		return merr.WrapErrParameterInvalidMsg("%s", err.Error())
	}
	return nil
}

func AssignPartitionKeys(ctx context.Context, metaCache Cache, dbName string, collName string, schema *schemapb.CollectionSchema, keys []*planpb.GenericValue) ([]string, error) {
	partitionNames, err := metaCache.GetPartitionsIndex(ctx, dbName, collName)
	if err != nil {
		return nil, err
	}

	partitionKeyFieldSchema, err := typeutil.GetPartitionKeyFieldSchema(schema)
	if err != nil {
		return nil, err
	}

	hashedPartitionNames, err := typeutil2.HashKey2Partitions(partitionKeyFieldSchema, keys, partitionNames)
	return hashedPartitionNames, err
}

func GetCurUserFromContext(ctx context.Context) (string, error) {
	return contextutil.GetCurUserFromContext(ctx)
}

func ParseGuaranteeTsFromConsistency(ts, tMax typeutil.Timestamp, consistency commonpb.ConsistencyLevel) typeutil.Timestamp {
	switch consistency {
	case commonpb.ConsistencyLevel_Strong:
		ts = tMax
	case commonpb.ConsistencyLevel_Bounded:
		ratio := paramtable.Get().CommonCfg.GracefulTime.GetAsDuration(time.Millisecond)
		ts = tsoutil.AddPhysicalDurationOnTs(tMax, -ratio)
	case commonpb.ConsistencyLevel_Eventually:
		ts = 1
	}
	return ts
}

func parseGuaranteeTs(ts, tMax typeutil.Timestamp) typeutil.Timestamp {
	switch ts {
	case strongTS:
		ts = tMax
	case boundedTS:
		ratio := paramtable.Get().CommonCfg.GracefulTime.GetAsDuration(time.Millisecond)
		ts = tsoutil.AddPhysicalDurationOnTs(tMax, -ratio)
	}
	return ts
}

func NamespaceShardingChannel(schema *schemapb.CollectionSchema, namespace *string, channelNames []string) (string, bool, error) {
	channelID, ok, err := NamespaceShardingChannelID(schema, namespace, channelNames)
	if !ok || err != nil {
		return "", ok, err
	}
	return channelNames[channelID], true, nil
}

func preferredNodeForChannel(preferredNodes map[string]int64, channel string) int64 {
	if preferredNodes == nil {
		return 0
	}
	preferredNodeID, ok := preferredNodes[channel]
	if !ok {
		return 0
	}
	return preferredNodeID
}

func reconstructStructFieldDataForQuery(results *milvuspb.QueryResults, schema *schemapb.CollectionSchema) {
	fieldsData, outputFields := reconstructStructFieldData(
		results.FieldsData,
		results.OutputFields,
		schema,
	)
	results.FieldsData = fieldsData
	results.OutputFields = outputFields
}

func getMaxMvccTsFromChannels(channelsTs map[string]uint64, beginTs typeutil.Timestamp) typeutil.Timestamp {
	maxTs := typeutil.Timestamp(0)
	for _, ts := range channelsTs {
		if ts > maxTs {
			maxTs = ts
		}
	}

	if maxTs == 0 {
		mlog.Warn(context.TODO(), "no channel ts found, use beginTs instead")
		return beginTs
	}

	return maxTs
}

func extractFieldsFromResults(results []*schemapb.FieldData, timezone string, fieldList []string) error {
	targetLocation, err := time.LoadLocation(timezone)
	if err != nil {
		mlog.Error(context.TODO(), "invalid timezone", mlog.String("timezone", timezone), mlog.Err(err))
		return merr.WrapErrParameterInvalidMsg("got invalid timezone: %s", timezone)
	}

	for _, fieldData := range results {
		if fieldData.GetType() != schemapb.DataType_Timestamptz {
			continue
		}

		scalarField := fieldData.GetScalars()
		if scalarField == nil || scalarField.GetTimestamptzData() == nil {
			if longData := scalarField.GetLongData(); longData != nil && len(longData.GetData()) > 0 {
				mlog.Warn(context.TODO(), "field data is not Timestamptz data, but found LongData instead", mlog.String("fieldName", fieldData.GetFieldName()))
				return merr.WrapErrParameterInvalidMsg("field data for '%s' is not Timestamptz data", fieldData.GetFieldName())
			}
			continue
		}

		utcTimestamps := scalarField.GetTimestamptzData().GetData()
		extractedResults := make([]*schemapb.ScalarField, 0, len(fieldList))

		for _, ts := range utcTimestamps {
			t := time.UnixMicro(ts).UTC()
			localTime := t.In(targetLocation)

			values, err := extractFields(localTime, fieldList)
			if err != nil {
				return err
			}
			valuesScalarField := &schemapb.ScalarField_LongData{
				LongData: &schemapb.LongArray{
					Data: values,
				},
			}
			extractedResults = append(extractedResults, &schemapb.ScalarField{
				Data: valuesScalarField,
			})
		}

		fieldData.GetScalars().Data = &schemapb.ScalarField_ArrayData{
			ArrayData: &schemapb.ArrayArray{
				Data:        extractedResults,
				ElementType: schemapb.DataType_Int64,
			},
		}
		fieldData.Type = schemapb.DataType_Array
	}
	return nil
}

func timestamptzUTC2IsoStr(results []*schemapb.FieldData, colTimezone string) error {
	location, err := time.LoadLocation(colTimezone)
	if err != nil {
		mlog.Error(context.TODO(), "invalid timezone", mlog.String("timezone", colTimezone), mlog.Err(err))
		return merr.WrapErrParameterInvalidMsg("got invalid default timezone: %s", colTimezone)
	}

	for _, fieldData := range results {
		if fieldData.GetType() != schemapb.DataType_Timestamptz {
			continue
		}

		scalarField := fieldData.GetScalars()

		// Guard against nil scalars or missing timestamp data
		if scalarField == nil || scalarField.GetTimestamptzData() == nil {
			if longData := scalarField.GetLongData(); longData != nil && len(longData.GetData()) > 0 {
				mlog.Warn(context.TODO(), "field data is not Timestamptz data", mlog.String("fieldName", fieldData.GetFieldName()))
				return merr.WrapErrParameterInvalidMsg("field data for '%s' is not Timestamptz data", fieldData.GetFieldName())
			}
			// Handle the case of an empty field (e.g., all nulls), skip if no data to process.
			continue
		}

		utcTimestamps := scalarField.GetTimestamptzData().GetData()
		isoStrings := make([]string, len(utcTimestamps))

		// CORE CHANGE: Use the optimized formatting function
		for i, ts := range utcTimestamps {
			// 1. Convert Unix Microsecond (UTC) to a time.Time object (still in UTC).
			t := time.UnixMicro(ts).UTC()

			// 2. Adjust the time object to the target location.
			localTime := t.In(location)

			// 3. Format using the optimized logic (max 6 digits, no trailing zeros)
			isoStrings[i] = timestamptz.FormatTimeMicroWithoutTrailingZeros(localTime)
		}

		// Replace the TimestamptzData with the new StringData in place.
		fieldData.GetScalars().Data = &schemapb.ScalarField_StringData{
			StringData: &schemapb.StringArray{
				Data: isoStrings,
			},
		}
	}
	return nil
}

// FormatTimestamptzFields converts internal TIMESTAMPTZ values to query-result strings.
func FormatTimestamptzFields(results []*schemapb.FieldData, colTimezone string) error {
	return timestamptzUTC2IsoStr(results, colTimezone)
}

func validateNQLimit(limit int64) error {
	nqLimit := paramtable.Get().QuotaConfig.NQLimit.GetAsInt64()
	if limit <= 0 || limit > nqLimit {
		return merr.WrapErrParameterInvalidMsg("nq (number of search vector per search request) should be in range [1, %d], but got %d", nqLimit, limit)
	}
	return nil
}

func getBM25FunctionOfAnnsField(fieldID int64, functions []*schemapb.FunctionSchema) (*schemapb.FunctionSchema, bool) {
	return lo.Find(functions, func(function *schemapb.FunctionSchema) bool {
		return function.GetType() == schemapb.FunctionType_BM25 && function.OutputFieldIds[0] == fieldID
	})
}

func extractFields(t time.Time, fieldList []string) ([]int64, error) {
	extractedValues := make([]int64, 0, len(fieldList))
	for _, field := range fieldList {
		var val int64
		switch strings.ToLower(field) {
		case common.TszYear:
			val = int64(t.Year())
		case common.TszMonth:
			val = int64(t.Month())
		case common.TszDay:
			val = int64(t.Day())
		case common.TszHour:
			val = int64(t.Hour())
		case common.TszMinute:
			val = int64(t.Minute())
		case common.TszSecond:
			val = int64(t.Second())
		case common.TszMicrosecond:
			val = int64(t.Nanosecond() / 1000)
		default:
			return nil, merr.WrapErrParameterInvalidMsg("unsupported field for extraction: %s, fields should be seprated by ',' or ' '", field)
		}
		extractedValues = append(extractedValues, val)
	}
	return extractedValues, nil
}

func reconstructStructFieldData(
	fieldsData []*schemapb.FieldData,
	outputFields []string,
	schema *schemapb.CollectionSchema,
) ([]*schemapb.FieldData, []string) {
	if len(outputFields) == 1 && outputFields[0] == "count(*)" {
		return fieldsData, outputFields
	}

	if len(schema.StructArrayFields) == 0 {
		return fieldsData, outputFields
	}

	regularFieldIDs := make(map[int64]interface{})
	subFieldToStructMap := make(map[int64]int64)
	groupedStructFields := make(map[int64][]*schemapb.FieldData)
	structFieldNames := make(map[int64]string)
	reconstructedOutputFields := make([]string, 0, len(fieldsData))

	// record all regular field IDs
	for _, field := range schema.Fields {
		regularFieldIDs[field.GetFieldID()] = nil
	}

	// build the mapping from sub-field ID to struct field ID
	for _, structField := range schema.StructArrayFields {
		for _, subField := range structField.GetFields() {
			subFieldToStructMap[subField.GetFieldID()] = structField.GetFieldID()
		}
		structFieldNames[structField.GetFieldID()] = structField.GetName()
	}

	newFieldsData := make([]*schemapb.FieldData, 0, len(fieldsData))
	for _, field := range fieldsData {
		fieldID := field.GetFieldId()
		if _, ok := regularFieldIDs[fieldID]; ok {
			newFieldsData = append(newFieldsData, field)
			reconstructedOutputFields = append(reconstructedOutputFields, field.GetFieldName())
		} else if structFieldID, ok := subFieldToStructMap[fieldID]; ok {
			groupedStructFields[structFieldID] = append(groupedStructFields[structFieldID], field)
		} else {
			newFieldsData = append(newFieldsData, field)
			reconstructedOutputFields = append(reconstructedOutputFields, field.GetFieldName())
		}
	}

	for structFieldID, fields := range groupedStructFields {
		// Restore original field names (from "structName[fieldName]" to "fieldName")
		// for the user-facing response.
		for _, field := range fields {
			originalName, err := extractOriginalFieldName(field.FieldName)
			if err != nil {
				mlog.Error(context.TODO(), "failed to extract original field name from struct field",
					mlog.String("fieldName", field.FieldName),
					mlog.Err(err))
			} else {
				field.FieldName = originalName
			}
		}

		newFieldsData = append(newFieldsData, &schemapb.FieldData{
			FieldName: structFieldNames[structFieldID],
			FieldId:   structFieldID,
			Type:      schemapb.DataType_ArrayOfStruct,
			Field:     &schemapb.FieldData_StructArrays{StructArrays: &schemapb.StructArrayField{Fields: fields}},
		})
		reconstructedOutputFields = append(reconstructedOutputFields, structFieldNames[structFieldID])
	}

	return newFieldsData, reconstructedOutputFields
}

// enableMultipleVectorFields indicates whether to enable multiple vector fields.
const enableMultipleVectorFields = true

// defaultMaxSearchRequest is the maximum number of ann search requests in a hybrid search.
const defaultMaxSearchRequest = 1024

func namespaceShardingEnabled(schema *schemapb.CollectionSchema) bool {
	if schema == nil || !schema.GetEnableNamespace() {
		return false
	}
	enabled, err := common.IsNamespaceShardingEnabled(schema.GetProperties()...)
	return err == nil && enabled
}

func GetColTimezone(colInfo *collectionInfo) string {
	timezone, _ := funcutil.TryGetAttrByKeyFromRepeatedKV(common.TimezoneKey, colInfo.Properties)
	if timezone == "" {
		timezone = common.DefaultTimezone
	}
	return timezone
}

func NamespaceShardingChannelID(schema *schemapb.CollectionSchema, namespace *string, channelNames []string) (uint32, bool, error) {
	if namespace == nil || !NamespacePartitionKeyModeEnabled(schema) {
		return 0, false, nil
	}
	if len(channelNames) == 0 {
		return 0, false, merr.WrapErrServiceInternalMsg("no virtual channels available for namespace sharding")
	}
	return typeutil.HashNamespace2Channels(*namespace, channelNames), true, nil
}

func extractOriginalFieldName(transformedName string) (string, error) {
	idx := strings.Index(transformedName, "[")
	if idx == -1 {
		return "", merr.WrapErrParameterInvalidMsg("not a transformed struct field name: %s", transformedName)
	}

	if !strings.HasSuffix(transformedName, "]") {
		return "", merr.WrapErrParameterInvalidMsg("invalid struct field format: %s, missing closing bracket", transformedName)
	}

	if idx == 0 {
		return "", merr.WrapErrParameterInvalidMsg("invalid struct field format: %s, missing struct name", transformedName)
	}

	fieldName := transformedName[idx+1 : len(transformedName)-1]
	if fieldName == "" {
		return "", merr.WrapErrParameterInvalidMsg("invalid struct field format: %s, empty field name", transformedName)
	}

	return fieldName, nil
}

func preferredNodeFromConcurrentMap(preferredNodes *typeutil.ConcurrentMap[string, int64], channel string) int64 {
	if preferredNodes == nil {
		return 0
	}
	preferredNodeID, ok := preferredNodes.Get(channel)
	if !ok {
		return 0
	}
	return preferredNodeID
}

func reconstructStructFieldDataForSearch(results *milvuspb.SearchResults, schema *schemapb.CollectionSchema) {
	if results.Results == nil {
		return
	}
	fieldsData, outputFields := reconstructStructFieldData(
		results.Results.FieldsData,
		results.Results.OutputFields,
		schema,
	)
	results.Results.FieldsData = fieldsData
	results.Results.OutputFields = outputFields
}
