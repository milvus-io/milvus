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
	"fmt"
	"strconv"

	"github.com/cockroachdb/errors"
	"github.com/twpayne/go-geom/encoding/wkb"
	"github.com/twpayne/go-geom/encoding/wkt"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/coordinator/snmanager"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type createCollectionTask struct {
	*Core
	Req    *milvuspb.CreateCollectionRequest
	header *message.CreateCollectionMessageHeader
	body   *message.CreateCollectionRequest
}

func (t *createCollectionTask) validate(ctx context.Context) error {
	if t.Req == nil {
		return errors.New("empty requests")
	}
	Params := paramtable.Get()

	// 1. check shard number
	shardsNum := t.Req.GetShardsNum()
	var cfgMaxShardNum int32
	if Params.CommonCfg.PreCreatedTopicEnabled.GetAsBool() {
		cfgMaxShardNum = int32(len(Params.CommonCfg.TopicNames.GetAsStrings()))
	} else {
		cfgMaxShardNum = Params.RootCoordCfg.DmlChannelNum.GetAsInt32()
	}
	if shardsNum > cfgMaxShardNum {
		return fmt.Errorf("shard num (%d) exceeds max configuration (%d)", shardsNum, cfgMaxShardNum)
	}

	cfgShardLimit := Params.ProxyCfg.MaxShardNum.GetAsInt32()
	if shardsNum > cfgShardLimit {
		return fmt.Errorf("shard num (%d) exceeds system limit (%d)", shardsNum, cfgShardLimit)
	}

	// 2. check db-collection capacity
	db2CollIDs := t.meta.ListAllAvailCollections(ctx)
	if err := t.checkMaxCollectionsPerDB(ctx, db2CollIDs); err != nil {
		return err
	}

	// 3. check total collection number
	totalCollections := 0
	for _, collIDs := range db2CollIDs {
		totalCollections += len(collIDs)
	}

	maxCollectionNum := Params.QuotaConfig.MaxCollectionNum.GetAsInt()
	if totalCollections >= maxCollectionNum {
		log.Ctx(ctx).Warn("unable to create collection because the number of collection has reached the limit", zap.Int("max_collection_num", maxCollectionNum))
		return merr.WrapErrCollectionNumLimitExceeded(t.Req.GetDbName(), maxCollectionNum)
	}

	// 4. check collection * shard * partition
	var newPartNum int64 = 1
	if t.Req.GetNumPartitions() > 0 {
		newPartNum = t.Req.GetNumPartitions()
	}
	return checkGeneralCapacity(ctx, 1, newPartNum, t.Req.GetShardsNum(), t.Core)
}

// checkMaxCollectionsPerDB DB properties take precedence over quota configurations for max collections.
func (t *createCollectionTask) checkMaxCollectionsPerDB(ctx context.Context, db2CollIDs map[int64][]int64) error {
	Params := paramtable.Get()

	collIDs, ok := db2CollIDs[t.header.DbId]
	if !ok {
		log.Ctx(ctx).Warn("can not found DB ID", zap.String("collection", t.Req.GetCollectionName()), zap.String("dbName", t.Req.GetDbName()))
		return merr.WrapErrDatabaseNotFound(t.Req.GetDbName(), "failed to create collection")
	}

	db, err := t.meta.GetDatabaseByName(ctx, t.Req.GetDbName(), typeutil.MaxTimestamp)
	if err != nil {
		log.Ctx(ctx).Warn("can not found DB ID", zap.String("collection", t.Req.GetCollectionName()), zap.String("dbName", t.Req.GetDbName()))
		return merr.WrapErrDatabaseNotFound(t.Req.GetDbName(), "failed to create collection")
	}

	check := func(maxColNumPerDB int) error {
		if len(collIDs) >= maxColNumPerDB {
			log.Ctx(ctx).Warn("unable to create collection because the number of collection has reached the limit in DB", zap.Int("maxCollectionNumPerDB", maxColNumPerDB))
			return merr.WrapErrCollectionNumLimitExceeded(t.Req.GetDbName(), maxColNumPerDB)
		}
		return nil
	}

	maxColNumPerDBStr := db.GetProperty(common.DatabaseMaxCollectionsKey)
	if maxColNumPerDBStr != "" {
		maxColNumPerDB, err := strconv.Atoi(maxColNumPerDBStr)
		if err != nil {
			log.Ctx(ctx).Warn("parse value of property fail", zap.String("key", common.DatabaseMaxCollectionsKey),
				zap.String("value", maxColNumPerDBStr), zap.Error(err))
			return fmt.Errorf("parse value of property fail, key:%s, value:%s", common.DatabaseMaxCollectionsKey, maxColNumPerDBStr)
		}
		return check(maxColNumPerDB)
	}

	maxColNumPerDB := Params.QuotaConfig.MaxCollectionNumPerDB.GetAsInt()
	return check(maxColNumPerDB)
}

func checkGeometryDefaultValue(value string) error {
	geomT, err := wkt.Unmarshal(value)
	if err != nil {
		log.Warn("invalid default value for geometry field", zap.Error(err))
		return merr.WrapErrParameterInvalidMsg("invalid default value for geometry field")
	}
	_, err = wkb.Marshal(geomT, wkb.NDR)
	if err != nil {
		log.Warn("invalid default value for geometry field", zap.Error(err))
		return merr.WrapErrParameterInvalidMsg("invalid default value for geometry field")
	}

	return nil
}

// checkAndRewriteTimestampTzDefaultValue processes the collection schema to validate
// and rewrite default values for TIMESTAMPTZ fields.
//
// Background:
//  1. TIMESTAMPTZ default values are initially stored as user-provided ISO 8601 strings
//     (in ValueField.GetStringData()).
//  2. Milvus stores TIMESTAMPTZ data internally as UTC microseconds (int64).
//
// Logic:
// The function iterates through all fields of type DataType_Timestamptz. For each field
// with a default value:
//  1. It retrieves the collection's default timezone if no offset is present in the string.
//  2. It calls ValidateAndReturnUnixMicroTz to validate the string (including the UTC
//     offset range check) and convert it to the absolute UTC microsecond (int64) value.
//  3. It rewrites the ValueField, setting the LongData field with the calculated int64
//     value, thereby replacing the initial string representation.
func checkAndRewriteTimestampTzDefaultValue(schema *schemapb.CollectionSchema) error {
	// 1. Get the collection-level default timezone.
	// Assuming common.TimezoneKey and common.DefaultTimezone are defined constants.
	timezone, exist := funcutil.TryGetAttrByKeyFromRepeatedKV(common.TimezoneKey, schema.GetProperties())
	if !exist {
		timezone = common.DefaultTimezone
	}

	for _, fieldSchema := range schema.GetFields() {
		// Only process TIMESTAMPTZ fields.
		if fieldSchema.GetDataType() != schemapb.DataType_Timestamptz {
			continue
		}

		defaultValue := fieldSchema.GetDefaultValue()
		if defaultValue == nil {
			continue
		}

		// 2. Read the default value as a string (the input format).
		// We expect the default value to be set in string_data initially.
		stringTz := defaultValue.GetStringData()
		if stringTz == "" {
			// Skip or handle empty string default values if necessary.
			continue
		}

		// 3. Validate the string and convert it to UTC microsecond (int64).
		// This also performs the critical UTC offset range validation.
		utcMicro, err := funcutil.ValidateAndReturnUnixMicroTz(stringTz, timezone)
		if err != nil {
			// If validation fails (e.g., invalid format or illegal offset), return error immediately.
			return err
		}

		// 4. Rewrite the default value to store the UTC microsecond (int64).
		// By setting ValueField_LongData, the oneof field in the protobuf structure
		// automatically switches from string_data to long_data.
		defaultValue.Data = &schemapb.ValueField_LongData{
			LongData: utcMicro,
		}

		// The original string_data field is now cleared due to the oneof nature,
		// and the default value is correctly represented as an int64 microsecond value.
	}
	return nil
}

func hasSystemFields(schema *schemapb.CollectionSchema, systemFields []string) bool {
	for _, f := range schema.GetFields() {
		if funcutil.SliceContain(systemFields, f.GetName()) {
			return true
		}
	}
	return false
}

func (t *createCollectionTask) validateSchema(ctx context.Context, schema *schemapb.CollectionSchema) error {
	log.Ctx(ctx).With(zap.String("CollectionName", t.Req.CollectionName))
	if t.Req.GetCollectionName() != schema.GetName() {
		log.Ctx(ctx).Error("collection name not matches schema name", zap.String("SchemaName", schema.Name))
		msg := fmt.Sprintf("collection name = %s, schema.Name=%s", t.Req.GetCollectionName(), schema.Name)
		return merr.WrapErrParameterInvalid("collection name matches schema name", "don't match", msg)
	}

	if err := checkFieldSchema(schema.GetFields()); err != nil {
		return err
	}

	// Validate default
	if err := checkAndRewriteTimestampTzDefaultValue(schema); err != nil {
		return err
	}

	if err := checkStructArrayFieldSchema(schema.GetStructArrayFields()); err != nil {
		return err
	}

	if hasSystemFields(schema, []string{RowIDFieldName, TimeStampFieldName, MetaFieldName, NamespaceFieldName}) {
		log.Ctx(ctx).Error("schema contains system field",
			zap.String("RowIDFieldName", RowIDFieldName),
			zap.String("TimeStampFieldName", TimeStampFieldName),
			zap.String("MetaFieldName", MetaFieldName),
			zap.String("NamespaceFieldName", NamespaceFieldName))
		msg := fmt.Sprintf("schema contains system field: %s, %s, %s, %s", RowIDFieldName, TimeStampFieldName, MetaFieldName, NamespaceFieldName)
		return merr.WrapErrParameterInvalid("schema don't contains system field", "contains", msg)
	}

	if err := validateStructArrayFieldDataType(schema.GetStructArrayFields()); err != nil {
		return err
	}

	// check analyzer was vaild
	analyzerInfos := make([]*querypb.AnalyzerInfo, 0)
	for _, field := range schema.GetFields() {
		err := validateAnalyzer(schema, field, &analyzerInfos)
		if err != nil {
			return err
		}
	}

	// validate analyzer params at any streaming node
	if len(analyzerInfos) > 0 {
		resp, err := t.mixCoord.ValidateAnalyzer(t.ctx, &querypb.ValidateAnalyzerRequest{
			AnalyzerInfos: analyzerInfos,
		})
		if err != nil {
			return err
		}

		if err := merr.Error(resp); err != nil {
			return err
		}
	}

	return validateFieldDataType(schema.GetFields())
}

func (t *createCollectionTask) assignFieldAndFunctionID(schema *schemapb.CollectionSchema) error {
	name2id := map[string]int64{}
	idx := 0
	for _, field := range schema.GetFields() {
		field.FieldID = int64(idx + StartOfUserFieldID)
		idx++

		name2id[field.GetName()] = field.GetFieldID()
	}

	for _, structArrayField := range schema.GetStructArrayFields() {
		structArrayField.FieldID = int64(idx + StartOfUserFieldID)
		idx++

		for _, field := range structArrayField.GetFields() {
			field.FieldID = int64(idx + StartOfUserFieldID)
			idx++
			// Also register sub-field names in name2id map
			name2id[field.GetName()] = field.GetFieldID()
		}
	}

	for fidx, function := range schema.GetFunctions() {
		function.InputFieldIds = make([]int64, len(function.InputFieldNames))
		function.Id = int64(fidx) + StartOfUserFunctionID
		for idx, name := range function.InputFieldNames {
			fieldId, ok := name2id[name]
			if !ok {
				return fmt.Errorf("input field %s of function %s not found", name, function.GetName())
			}
			function.InputFieldIds[idx] = fieldId
		}

		function.OutputFieldIds = make([]int64, len(function.OutputFieldNames))
		for idx, name := range function.OutputFieldNames {
			fieldId, ok := name2id[name]
			if !ok {
				return fmt.Errorf("output field %s of function %s not found", name, function.GetName())
			}
			function.OutputFieldIds[idx] = fieldId
		}
	}

	return nil
}

func (t *createCollectionTask) appendDynamicField(ctx context.Context, schema *schemapb.CollectionSchema) {
	if schema.EnableDynamicField {
		schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
			Name:        MetaFieldName,
			Description: "dynamic schema",
			DataType:    schemapb.DataType_JSON,
			IsDynamic:   true,
		})
		log.Ctx(ctx).Info("append dynamic field", zap.String("collection", schema.Name))
	}
}

func (t *createCollectionTask) appendConsistecyLevel() {
	if ok, _ := getConsistencyLevel(t.Req.Properties...); ok {
		return
	}
	for _, p := range t.Req.Properties {
		if p.GetKey() == common.ConsistencyLevel {
			// if there's already a consistency level, overwrite it.
			p.Value = strconv.Itoa(int(t.Req.ConsistencyLevel))
			return
		}
	}
	// append consistency level into schema properties
	t.Req.Properties = append(t.Req.Properties, &commonpb.KeyValuePair{
		Key:   common.ConsistencyLevel,
		Value: strconv.Itoa(int(t.Req.ConsistencyLevel)),
	})
}

func (t *createCollectionTask) handleNamespaceField(ctx context.Context, schema *schemapb.CollectionSchema) error {
	if !Params.CommonCfg.EnableNamespace.GetAsBool() {
		return nil
	}

	hasIsolation := hasIsolationProperty(t.Req.Properties...)
	_, err := typeutil.GetPartitionKeyFieldSchema(schema)
	hasPartitionKey := err == nil
	enabled, has, err := common.ParseNamespaceProp(t.Req.Properties...)
	if err != nil {
		return err
	}
	if !has || !enabled {
		return nil
	}

	if hasIsolation {
		iso, err := common.IsPartitionKeyIsolationKvEnabled(t.Req.Properties...)
		if err != nil {
			return err
		}
		if !iso {
			return merr.WrapErrCollectionIllegalSchema(t.Req.CollectionName,
				"isolation property is false when namespace enabled")
		}
	}

	if hasPartitionKey {
		return merr.WrapErrParameterInvalidMsg("namespace is not supported with partition key mode")
	}

	schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
		Name:           common.NamespaceFieldName,
		IsPartitionKey: true,
		DataType:       schemapb.DataType_VarChar,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: common.MaxLengthKey, Value: fmt.Sprintf("%d", paramtable.Get().ProxyCfg.MaxVarCharLength.GetAsInt())},
		},
	})
	t.Req.Properties = append(t.Req.Properties, &commonpb.KeyValuePair{
		Key:   common.PartitionKeyIsolationKey,
		Value: "true",
	})
	log.Ctx(ctx).Info("added namespace field",
		zap.String("collectionName", t.Req.CollectionName),
		zap.String("fieldName", common.NamespaceFieldName))
	return nil
}

func hasIsolationProperty(props ...*commonpb.KeyValuePair) bool {
	for _, p := range props {
		if p.GetKey() == common.PartitionKeyIsolationKey {
			return true
		}
	}
	return false
}

func (t *createCollectionTask) appendSysFields(schema *schemapb.CollectionSchema) {
	schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
		FieldID:      int64(RowIDField),
		Name:         RowIDFieldName,
		IsPrimaryKey: false,
		Description:  "row id",
		DataType:     schemapb.DataType_Int64,
	})
	schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
		FieldID:      int64(TimeStampField),
		Name:         TimeStampFieldName,
		IsPrimaryKey: false,
		Description:  "time stamp",
		DataType:     schemapb.DataType_Int64,
	})
}

func (t *createCollectionTask) prepareSchema(ctx context.Context) error {
	if err := t.validateSchema(ctx, t.body.CollectionSchema); err != nil {
		return err
	}
	t.appendConsistecyLevel()
	t.appendDynamicField(ctx, t.body.CollectionSchema)
	if err := t.handleNamespaceField(ctx, t.body.CollectionSchema); err != nil {
		return err
	}

	if err := t.assignFieldAndFunctionID(t.body.CollectionSchema); err != nil {
		return err
	}

	// Validate timezone
	tz, exist := funcutil.TryGetAttrByKeyFromRepeatedKV(common.TimezoneKey, t.Req.GetProperties())
	if exist && !funcutil.IsTimezoneValid(tz) {
		return merr.WrapErrParameterInvalidMsg("unknown or invalid IANA Time Zone ID: %s", tz)
	}

	// Set properties for persistent
	t.body.CollectionSchema.Properties = t.Req.GetProperties()
	t.body.CollectionSchema.Version = 0
	t.appendSysFields(t.body.CollectionSchema)
	return nil
}

func (t *createCollectionTask) assignCollectionID() error {
	var err error
	t.header.CollectionId, err = t.idAllocator.AllocOne()
	t.body.CollectionID = t.header.CollectionId
	return err
}

func (t *createCollectionTask) assignPartitionIDs(ctx context.Context) error {
	Params := paramtable.Get()

	partitionNames := make([]string, 0, t.Req.GetNumPartitions())
	defaultPartitionName := Params.CommonCfg.DefaultPartitionName.GetValue()

	if _, err := typeutil.GetPartitionKeyFieldSchema(t.body.CollectionSchema); err == nil {
		// only when enabling partition key mode, we allow to create multiple partitions.
		partitionNums := t.Req.GetNumPartitions()
		// double check, default num of physical partitions should be greater than 0
		if partitionNums <= 0 {
			return errors.New("the specified partitions should be greater than 0 if partition key is used")
		}

		cfgMaxPartitionNum := Params.RootCoordCfg.MaxPartitionNum.GetAsInt64()
		if partitionNums > cfgMaxPartitionNum {
			return fmt.Errorf("partition number (%d) exceeds max configuration (%d), collection: %s",
				partitionNums, cfgMaxPartitionNum, t.Req.CollectionName)
		}

		for i := int64(0); i < partitionNums; i++ {
			partitionNames = append(partitionNames, fmt.Sprintf("%s_%d", defaultPartitionName, i))
		}
	} else {
		// compatible with old versions <= 2.2.8
		partitionNames = append(partitionNames, defaultPartitionName)
	}

	// allocate partition ids
	start, end, err := t.idAllocator.Alloc(uint32(len(partitionNames)))
	if err != nil {
		return err
	}
	t.header.PartitionIds = make([]int64, len(partitionNames))
	t.body.PartitionIDs = make([]int64, len(partitionNames))
	for i := start; i < end; i++ {
		t.header.PartitionIds[i-start] = i
		t.body.PartitionIDs[i-start] = i
	}
	t.body.PartitionNames = partitionNames

	log.Ctx(ctx).Info("assign partitions when create collection",
		zap.String("collectionName", t.Req.GetCollectionName()),
		zap.Int64s("partitionIds", t.header.PartitionIds),
		zap.Strings("partitionNames", t.body.PartitionNames))
	return nil
}

func (t *createCollectionTask) assignChannels(ctx context.Context) error {
	vchannels, err := snmanager.StaticStreamingNodeManager.AllocVirtualChannels(ctx, balancer.AllocVChannelParam{
		CollectionID: t.header.GetCollectionId(),
		Num:          int(t.Req.GetShardsNum()),
	})
	if err != nil {
		return err
	}

	for _, vchannel := range vchannels {
		t.body.PhysicalChannelNames = append(t.body.PhysicalChannelNames, funcutil.ToPhysicalChannel(vchannel))
		t.body.VirtualChannelNames = append(t.body.VirtualChannelNames, vchannel)
	}
	return nil
}

func (t *createCollectionTask) Prepare(ctx context.Context) error {
	t.body.Base = &commonpb.MsgBase{
		MsgType: commonpb.MsgType_CreateCollection,
	}

	db, err := t.meta.GetDatabaseByName(ctx, t.Req.GetDbName(), typeutil.MaxTimestamp)
	if err != nil {
		return err
	}
	// set collection timezone
	properties := t.Req.GetProperties()
	_, ok := funcutil.TryGetAttrByKeyFromRepeatedKV(common.TimezoneKey, properties)
	if !ok {
		dbTz, ok2 := funcutil.TryGetAttrByKeyFromRepeatedKV(common.TimezoneKey, db.Properties)
		if !ok2 {
			dbTz = common.DefaultTimezone
		}
		timezoneKV := &commonpb.KeyValuePair{Key: common.TimezoneKey, Value: dbTz}
		t.Req.Properties = append(properties, timezoneKV)
	}

	if hookutil.GetEzPropByDBProperties(db.Properties) != nil {
		t.Req.Properties = append(t.Req.Properties, hookutil.GetEzPropByDBProperties(db.Properties))
	}

	t.header.DbId = db.ID
	t.body.DbID = t.header.DbId
	if err := t.validate(ctx); err != nil {
		return err
	}

	if err := t.prepareSchema(ctx); err != nil {
		return err
	}

	if err := t.assignCollectionID(); err != nil {
		return err
	}

	if err := t.assignPartitionIDs(ctx); err != nil {
		return err
	}

	if err := t.assignChannels(ctx); err != nil {
		return err
	}

	return t.validateIfCollectionExists(ctx)
}

func (t *createCollectionTask) validateIfCollectionExists(ctx context.Context) error {
	// Check if the collection name duplicates an alias.
	if _, err := t.meta.DescribeAlias(ctx, t.Req.GetDbName(), t.Req.GetCollectionName(), typeutil.MaxTimestamp); err == nil {
		err2 := fmt.Errorf("collection name [%s] conflicts with an existing alias, please choose a unique name", t.Req.GetCollectionName())
		log.Ctx(ctx).Warn("create collection failed", zap.String("database", t.Req.GetDbName()), zap.Error(err2))
		return err2
	}

	// Check if the collection already exists.
	existedCollInfo, err := t.meta.GetCollectionByName(ctx, t.Req.GetDbName(), t.Req.GetCollectionName(), typeutil.MaxTimestamp)
	if err == nil {
		newCollInfo := newCollectionModel(t.header, t.body, 0)
		if equal := existedCollInfo.Equal(*newCollInfo); !equal {
			return fmt.Errorf("create duplicate collection with different parameters, collection: %s", t.Req.GetCollectionName())
		}
		return errIgnoredCreateCollection
	}
	return nil
}

func validateMultiAnalyzerParams(params string, coll *schemapb.CollectionSchema, fieldSchema *schemapb.FieldSchema, infos *[]*querypb.AnalyzerInfo) error {
	var m map[string]json.RawMessage
	var analyzerMap map[string]json.RawMessage
	var mFileName string

	err := json.Unmarshal([]byte(params), &m)
	if err != nil {
		return err
	}

	mfield, ok := m["by_field"]
	if !ok {
		return fmt.Errorf("multi analyzer params now must set by_field to specify with field decide analyzer")
	}

	err = json.Unmarshal(mfield, &mFileName)
	if err != nil {
		return fmt.Errorf("multi analyzer params by_field must be string but now: %s", mfield)
	}

	// check field exist
	fieldExist := false
	for _, field := range coll.GetFields() {
		if field.GetName() == mFileName {
			// only support string field now
			if field.GetDataType() != schemapb.DataType_VarChar {
				return fmt.Errorf("multi analyzer params now only support by string field, but field %s is not string", field.GetName())
			}
			fieldExist = true
			break
		}
	}

	if !fieldExist {
		return fmt.Errorf("multi analyzer dependent field %s not exist in collection %s", string(mfield), coll.GetName())
	}

	if value, ok := m["alias"]; ok {
		mapping := map[string]string{}
		err = json.Unmarshal(value, &mapping)
		if err != nil {
			return fmt.Errorf("multi analyzer alias must be string map but now: %s", value)
		}
	}

	analyzers, ok := m["analyzers"]
	if !ok {
		return fmt.Errorf("multi analyzer params must set analyzers ")
	}

	err = json.Unmarshal(analyzers, &analyzerMap)
	if err != nil {
		return fmt.Errorf("unmarshal analyzers failed: %s", err)
	}

	hasDefault := false
	for name, bytes := range analyzerMap {
		*infos = append(*infos, &querypb.AnalyzerInfo{
			Name:   name,
			Field:  fieldSchema.GetName(),
			Params: string(bytes),
		})
		if name == "default" {
			hasDefault = true
		}
	}

	if !hasDefault {
		return fmt.Errorf("multi analyzer must set default analyzer for all unknown value")
	}
	return nil
}

func validateAnalyzer(collSchema *schemapb.CollectionSchema, fieldSchema *schemapb.FieldSchema, analyzerInfos *[]*querypb.AnalyzerInfo) error {
	h := typeutil.CreateFieldSchemaHelper(fieldSchema)
	if !h.EnableMatch() && !typeutil.IsBm25FunctionInputField(collSchema, fieldSchema) {
		return nil
	}

	if !h.EnableAnalyzer() {
		return fmt.Errorf("field %s which has enable_match or is input of BM25 function must also enable_analyzer", fieldSchema.Name)
	}

	if params, ok := h.GetMultiAnalyzerParams(); ok {
		if h.EnableMatch() {
			return fmt.Errorf("multi analyzer now only support for bm25, but now field %s enable match", fieldSchema.Name)
		}
		if h.HasAnalyzerParams() {
			return fmt.Errorf("field %s analyzer params should be none if has multi analyzer params", fieldSchema.Name)
		}

		return validateMultiAnalyzerParams(params, collSchema, fieldSchema, analyzerInfos)
	}

	for _, kv := range fieldSchema.GetTypeParams() {
		if kv.GetKey() == "analyzer_params" {
			*analyzerInfos = append(*analyzerInfos, &querypb.AnalyzerInfo{
				Field:  fieldSchema.GetName(),
				Params: kv.GetValue(),
			})
		}
	}
	// return nil when use default analyzer
	return nil
}
