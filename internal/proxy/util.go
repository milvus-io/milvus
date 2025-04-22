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

package proxy

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/zap"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/ctokenizer"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/internal/util/indexparamcheck"
	typeutil2 "github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/contextutil"
	"github.com/milvus-io/milvus/pkg/v2/util/crypto"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metric"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

const (
	strongTS  = 0
	boundedTS = 2

	// enableMultipleVectorFields indicates whether to enable multiple vector fields.
	enableMultipleVectorFields = true

	defaultMaxArrayCapacity = 4096

	defaultMaxSearchRequest = 1024

	// DefaultArithmeticIndexType name of default index type for scalar field
	DefaultArithmeticIndexType = indexparamcheck.IndexINVERTED

	// DefaultStringIndexType name of default index type for varChar/string field
	DefaultStringIndexType = indexparamcheck.IndexINVERTED

	defaultRRFParamsValue = 60
	maxRRFParamsValue     = 16384
)

var logger = log.L().WithOptions(zap.Fields(zap.String("role", typeutil.ProxyRole)))

// isAlpha check if c is alpha.
func isAlpha(c uint8) bool {
	if (c < 'A' || c > 'Z') && (c < 'a' || c > 'z') {
		return false
	}
	return true
}

// isNumber check if c is a number.
func isNumber(c uint8) bool {
	if c < '0' || c > '9' {
		return false
	}
	return true
}

func validateMaxQueryResultWindow(offset int64, limit int64) error {
	if offset < 0 {
		return fmt.Errorf("%s [%d] is invalid, should be gte than 0", OffsetKey, offset)
	}
	if limit <= 0 {
		return fmt.Errorf("%s [%d] is invalid, should be greater than 0", LimitKey, limit)
	}

	depth := offset + limit
	maxQueryResultWindow := Params.QuotaConfig.MaxQueryResultWindow.GetAsInt64()
	if depth <= 0 || depth > maxQueryResultWindow {
		return fmt.Errorf("(offset+limit) should be in range [1, %d], but got %d", maxQueryResultWindow, depth)
	}
	return nil
}

func validateLimit(limit int64) error {
	topKLimit := Params.QuotaConfig.TopKLimit.GetAsInt64()
	if limit <= 0 || limit > topKLimit {
		return fmt.Errorf("it should be in range [1, %d], but got %d", topKLimit, limit)
	}
	return nil
}

func validateNQLimit(limit int64) error {
	nqLimit := Params.QuotaConfig.NQLimit.GetAsInt64()
	if limit <= 0 || limit > nqLimit {
		return fmt.Errorf("nq (number of search vector per search request) should be in range [1, %d], but got %d", nqLimit, limit)
	}
	return nil
}

func validateCollectionNameOrAlias(entity, entityType string) error {
	entity = strings.TrimSpace(entity)

	if entity == "" {
		return merr.WrapErrParameterInvalidMsg("collection %s should not be empty", entityType)
	}

	invalidMsg := fmt.Sprintf("Invalid collection %s: %s. ", entityType, entity)
	if len(entity) > Params.ProxyCfg.MaxNameLength.GetAsInt() {
		return merr.WrapErrParameterInvalidMsg("%s the length of a collection %s must be less than %s characters", invalidMsg, entityType,
			Params.ProxyCfg.MaxNameLength.GetValue())
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

func ValidatePrivilegeGroupName(groupName string) error {
	if groupName == "" {
		return merr.WrapErrPrivilegeGroupNameInvalid("privilege group name should not be empty")
	}

	if len(groupName) > Params.ProxyCfg.MaxNameLength.GetAsInt() {
		return merr.WrapErrPrivilegeGroupNameInvalid(
			"the length of a privilege group name %s must be less than %s characters", groupName, Params.ProxyCfg.MaxNameLength.GetValue())
	}

	firstChar := groupName[0]
	if firstChar != '_' && !isAlpha(firstChar) {
		return merr.WrapErrPrivilegeGroupNameInvalid(
			"the first character of a privilege group name %s must be an underscore or letter", groupName)
	}

	for i := 1; i < len(groupName); i++ {
		c := groupName[i]
		if c != '_' && !isAlpha(c) && !isNumber(c) {
			return merr.WrapErrParameterInvalidMsg(
				"privilege group name %s can only contain numbers, letters and underscores", groupName)
		}
	}
	return nil
}

func ValidateResourceGroupName(entity string) error {
	if entity == "" {
		return errors.New("resource group name couldn't be empty")
	}

	invalidMsg := fmt.Sprintf("Invalid resource group name %s.", entity)
	if len(entity) > Params.ProxyCfg.MaxNameLength.GetAsInt() {
		return merr.WrapErrParameterInvalidMsg("%s the length of a resource group name must be less than %s characters",
			invalidMsg, Params.ProxyCfg.MaxNameLength.GetValue())
	}

	firstChar := entity[0]
	if firstChar != '_' && !isAlpha(firstChar) {
		return merr.WrapErrParameterInvalidMsg("%s the first character of a resource group name must be an underscore or letter", invalidMsg)
	}

	for i := 1; i < len(entity); i++ {
		c := entity[i]
		if c != '_' && !isAlpha(c) && !isNumber(c) {
			return merr.WrapErrParameterInvalidMsg("%s resource group name can only contain numbers, letters and underscores", invalidMsg)
		}
	}
	return nil
}

func ValidateDatabaseName(dbName string) error {
	if dbName == "" {
		return merr.WrapErrDatabaseNameInvalid(dbName, "database name couldn't be empty")
	}

	if len(dbName) > Params.ProxyCfg.MaxNameLength.GetAsInt() {
		return merr.WrapErrDatabaseNameInvalid(dbName,
			fmt.Sprintf("the length of a database name must be less than %d characters", Params.ProxyCfg.MaxNameLength.GetAsInt()))
	}

	firstChar := dbName[0]
	if firstChar != '_' && !isAlpha(firstChar) {
		return merr.WrapErrDatabaseNameInvalid(dbName,
			"the first character of a database name must be an underscore or letter")
	}

	for i := 1; i < len(dbName); i++ {
		c := dbName[i]
		if c != '_' && !isAlpha(c) && !isNumber(c) {
			return merr.WrapErrDatabaseNameInvalid(dbName,
				"database name can only contain numbers, letters and underscores")
		}
	}
	return nil
}

// ValidateCollectionAlias returns true if collAlias is a valid alias name for collection, otherwise returns false.
func ValidateCollectionAlias(collAlias string) error {
	return validateCollectionNameOrAlias(collAlias, "alias")
}

func validateCollectionName(collName string) error {
	return validateCollectionNameOrAlias(collName, "name")
}

func validatePartitionTag(partitionTag string, strictCheck bool) error {
	partitionTag = strings.TrimSpace(partitionTag)

	invalidMsg := "Invalid partition name: " + partitionTag + ". "
	if partitionTag == "" {
		msg := invalidMsg + "Partition name should not be empty."
		return errors.New(msg)
	}
	if len(partitionTag) > Params.ProxyCfg.MaxNameLength.GetAsInt() {
		msg := invalidMsg + "The length of a partition name must be less than " + Params.ProxyCfg.MaxNameLength.GetValue() + " characters."
		return errors.New(msg)
	}

	if strictCheck {
		firstChar := partitionTag[0]
		if firstChar != '_' && !isAlpha(firstChar) && !isNumber(firstChar) {
			msg := invalidMsg + "The first character of a partition name must be an underscore or letter."
			return errors.New(msg)
		}

		tagSize := len(partitionTag)
		for i := 1; i < tagSize; i++ {
			c := partitionTag[i]
			if c != '_' && !isAlpha(c) && !isNumber(c) && c != '-' {
				msg := invalidMsg + "Partition name can only contain numbers, letters and underscores."
				return errors.New(msg)
			}
		}
	}

	return nil
}

func validateFieldName(fieldName string) error {
	fieldName = strings.TrimSpace(fieldName)

	if fieldName == "" {
		return merr.WrapErrFieldNameInvalid(fieldName, "field name should not be empty")
	}

	invalidMsg := "Invalid field name: " + fieldName + ". "
	if len(fieldName) > Params.ProxyCfg.MaxNameLength.GetAsInt() {
		msg := invalidMsg + "The length of a field name must be less than " + Params.ProxyCfg.MaxNameLength.GetValue() + " characters."
		return merr.WrapErrFieldNameInvalid(fieldName, msg)
	}

	firstChar := fieldName[0]
	if firstChar != '_' && !isAlpha(firstChar) {
		msg := invalidMsg + "The first character of a field name must be an underscore or letter."
		return merr.WrapErrFieldNameInvalid(fieldName, msg)
	}

	fieldNameSize := len(fieldName)
	for i := 1; i < fieldNameSize; i++ {
		c := fieldName[i]
		if c != '_' && !isAlpha(c) && !isNumber(c) {
			msg := invalidMsg + "Field name can only contain numbers, letters, and underscores."
			return merr.WrapErrFieldNameInvalid(fieldName, msg)
		}
	}
	if _, ok := common.FieldNameKeywords[fieldName]; ok {
		msg := invalidMsg + fmt.Sprintf("%s is keyword in milvus.", fieldName)
		return merr.WrapErrFieldNameInvalid(fieldName, msg)
	}
	return nil
}

func validateDimension(field *schemapb.FieldSchema) error {
	exist := false
	var dim int64
	for _, param := range field.TypeParams {
		if param.Key == common.DimKey {
			exist = true
			tmp, err := strconv.ParseInt(param.Value, 10, 64)
			if err != nil {
				return err
			}
			dim = tmp
			break
		}
	}
	if typeutil.IsSparseFloatVectorType(field.DataType) {
		if exist {
			return fmt.Errorf("dim should not be specified for sparse vector field %s(%d)", field.GetName(), field.FieldID)
		}
		return nil
	}
	if !exist {
		return errors.Newf("dimension is not defined in field type params of field %s, check type param `dim` for vector field", field.GetName())
	}

	if dim <= 1 {
		return fmt.Errorf("invalid dimension: %d. should be in range 2 ~ %d", dim, Params.ProxyCfg.MaxDimension.GetAsInt())
	}

	if typeutil.IsFloatVectorType(field.DataType) {
		if dim > Params.ProxyCfg.MaxDimension.GetAsInt64() {
			return fmt.Errorf("invalid dimension: %d of field %s. float vector dimension should be in range 2 ~ %d", dim, field.GetName(), Params.ProxyCfg.MaxDimension.GetAsInt())
		}
	} else {
		if dim%8 != 0 {
			return fmt.Errorf("invalid dimension: %d of field %s. binary vector dimension should be multiple of 8. ", dim, field.GetName())
		}
		if dim > Params.ProxyCfg.MaxDimension.GetAsInt64()*8 {
			return fmt.Errorf("invalid dimension: %d of field %s. binary vector dimension should be in range 2 ~ %d", dim, field.GetName(), Params.ProxyCfg.MaxDimension.GetAsInt()*8)
		}
	}
	return nil
}

func validateMaxLengthPerRow(collectionName string, field *schemapb.FieldSchema) error {
	exist := false
	for _, param := range field.TypeParams {
		if param.Key != common.MaxLengthKey {
			continue
		}

		maxLengthPerRow, err := strconv.ParseInt(param.Value, 10, 64)
		if err != nil {
			return err
		}

		defaultMaxVarCharLength := Params.ProxyCfg.MaxVarCharLength.GetAsInt64()
		if maxLengthPerRow > defaultMaxVarCharLength || maxLengthPerRow <= 0 {
			return merr.WrapErrParameterInvalidMsg("the maximum length specified for a VarChar field(%s) should be in (0, %d], but got %d instead", field.GetName(), defaultMaxVarCharLength, maxLengthPerRow)
		}
		exist = true
	}
	// if not exist type params max_length, return error
	if !exist {
		return fmt.Errorf("type param(max_length) should be specified for varChar field(%s) of collection %s", field.GetName(), collectionName)
	}

	return nil
}

func validateMaxCapacityPerRow(collectionName string, field *schemapb.FieldSchema) error {
	exist := false
	for _, param := range field.TypeParams {
		if param.Key != common.MaxCapacityKey {
			continue
		}

		maxCapacityPerRow, err := strconv.ParseInt(param.Value, 10, 64)
		if err != nil {
			return fmt.Errorf("the value for %s of field %s must be an integer", common.MaxCapacityKey, field.GetName())
		}
		if maxCapacityPerRow > defaultMaxArrayCapacity || maxCapacityPerRow <= 0 {
			return errors.New("the maximum capacity specified for a Array should be in (0, 4096]")
		}
		exist = true
	}
	// if not exist type params max_length, return error
	if !exist {
		return fmt.Errorf("type param(max_capacity) should be specified for array field %s of collection %s", field.GetName(), collectionName)
	}

	return nil
}

func validateVectorFieldMetricType(field *schemapb.FieldSchema) error {
	if !typeutil.IsVectorType(field.DataType) {
		return nil
	}
	for _, params := range field.IndexParams {
		if params.Key == common.MetricTypeKey {
			return nil
		}
	}
	return fmt.Errorf(`index param "metric_type" is not specified for index float vector %s`, field.GetName())
}

func validateDuplicatedFieldName(fields []*schemapb.FieldSchema) error {
	names := make(map[string]bool)
	for _, field := range fields {
		_, ok := names[field.Name]
		if ok {
			return errors.Newf("duplicated field name %s found", field.GetName())
		}
		names[field.Name] = true
	}
	return nil
}

func validateElementType(dataType schemapb.DataType) error {
	switch dataType {
	case schemapb.DataType_Bool, schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32,
		schemapb.DataType_Int64, schemapb.DataType_Float, schemapb.DataType_Double, schemapb.DataType_VarChar:
		return nil
	case schemapb.DataType_String:
		return errors.New("string data type not supported yet, please use VarChar type instead")
	case schemapb.DataType_None:
		return errors.New("element data type None is not valid")
	}
	return fmt.Errorf("element type %s is not supported", dataType.String())
}

func validateFieldType(schema *schemapb.CollectionSchema) error {
	for _, field := range schema.GetFields() {
		switch field.GetDataType() {
		case schemapb.DataType_String:
			return errors.New("string data type not supported yet, please use VarChar type instead")
		case schemapb.DataType_None:
			return errors.New("data type None is not valid")
		case schemapb.DataType_Array:
			if err := validateElementType(field.GetElementType()); err != nil {
				return err
			}
		}
	}
	return nil
}

// ValidateFieldAutoID call after validatePrimaryKey
func ValidateFieldAutoID(coll *schemapb.CollectionSchema) error {
	idx := -1
	for i, field := range coll.Fields {
		if field.AutoID {
			if idx != -1 {
				return fmt.Errorf("only one field can speficy AutoID with true, field name = %s, %s", coll.Fields[idx].Name, field.Name)
			}
			idx = i
			if !field.IsPrimaryKey {
				return fmt.Errorf("only primary field can speficy AutoID with true, field name = %s", field.Name)
			}
		}
	}
	return nil
}

func ValidateField(field *schemapb.FieldSchema, schema *schemapb.CollectionSchema) error {
	// validate field name
	var err error
	if err := validateFieldName(field.Name); err != nil {
		return err
	}
	// validate dense vector field type parameters
	isVectorType := typeutil.IsVectorType(field.DataType)
	if isVectorType {
		err = validateDimension(field)
		if err != nil {
			return err
		}
	}
	// valid max length per row parameters
	// if max_length not specified, return error
	if field.DataType == schemapb.DataType_VarChar ||
		(field.GetDataType() == schemapb.DataType_Array && field.GetElementType() == schemapb.DataType_VarChar) {
		err = validateMaxLengthPerRow(schema.Name, field)
		if err != nil {
			return err
		}
	}
	// valid max capacity for array per row parameters
	// if max_capacity not specified, return error
	if field.DataType == schemapb.DataType_Array {
		if err = validateMaxCapacityPerRow(schema.Name, field); err != nil {
			return err
		}
	}
	// TODO should remove the index params in the field schema
	indexParams := funcutil.KeyValuePair2Map(field.GetIndexParams())
	if err = ValidateAutoIndexMmapConfig(isVectorType, indexParams); err != nil {
		return err
	}

	if err := validateAnalyzer(schema, field); err != nil {
		return err
	}
	return nil
}

func validateMultiAnalyzerParams(params string, coll *schemapb.CollectionSchema) error {
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
	for name, params := range analyzerMap {
		if err := ctokenizer.ValidateTokenizer(string(params)); err != nil {
			return fmt.Errorf("analyzer %s params invalid: %s", name, err)
		}
		if name == "default" {
			hasDefault = true
		}
	}

	if !hasDefault {
		return fmt.Errorf("multi analyzer must set default analyzer for all unknown value")
	}
	return nil
}

func validateAnalyzer(collSchema *schemapb.CollectionSchema, fieldSchema *schemapb.FieldSchema) error {
	h := typeutil.CreateFieldSchemaHelper(fieldSchema)
	if !h.EnableMatch() && !wasBm25FunctionInputField(collSchema, fieldSchema) {
		return nil
	}

	if !h.EnableAnalyzer() {
		return fmt.Errorf("field %s is set to enable match or bm25 function but not enable analyzer", fieldSchema.Name)
	}

	if params, ok := h.GetMultiAnalyzerParams(); ok {
		if h.EnableMatch() {
			return fmt.Errorf("multi analyzer now only support for bm25, but now field %s enable match", fieldSchema.Name)
		}
		if h.HasAnalyzerParams() {
			return fmt.Errorf("field %s analyzer params should be none if has multi analyzer params", fieldSchema.Name)
		}

		return validateMultiAnalyzerParams(params, collSchema)
	}

	for _, kv := range fieldSchema.GetTypeParams() {
		if kv.GetKey() == "analyzer_params" {
			return ctokenizer.ValidateTokenizer(kv.Value)
		}
	}
	// return nil when use default analyzer
	return nil
}

func validatePrimaryKey(coll *schemapb.CollectionSchema) error {
	idx := -1
	for i, field := range coll.Fields {
		if field.IsPrimaryKey {
			if idx != -1 {
				return fmt.Errorf("there are more than one primary key, field name = %s, %s", coll.Fields[idx].Name, field.Name)
			}

			// The type of the primary key field can only be int64 and varchar
			if field.DataType != schemapb.DataType_Int64 && field.DataType != schemapb.DataType_VarChar {
				return errors.New("the data type of primary key should be Int64 or VarChar")
			}

			// varchar field do not support autoID
			// If autoID is required, it is recommended to use int64 field as the primary key
			//if field.DataType == schemapb.DataType_VarChar {
			//	if field.AutoID {
			//		return errors.New("autoID is not supported when the VarChar field is the primary key")
			//	}
			//}

			idx = i
		}
	}
	if idx == -1 {
		return errors.New("primary key is not specified")
	}
	return nil
}

func validateDynamicField(coll *schemapb.CollectionSchema) error {
	for _, field := range coll.Fields {
		if field.IsDynamic {
			return errors.New("cannot explicitly set a field as a dynamic field")
		}
	}
	return nil
}

// RepeatedKeyValToMap transfer the kv pairs to map.
func RepeatedKeyValToMap(kvPairs []*commonpb.KeyValuePair) (map[string]string, error) {
	resMap := make(map[string]string)
	for _, kv := range kvPairs {
		_, ok := resMap[kv.Key]
		if ok {
			return nil, fmt.Errorf("duplicated param key: %s", kv.Key)
		}
		resMap[kv.Key] = kv.Value
	}
	return resMap, nil
}

// isVector check if dataType belongs to vector type.
func isVector(dataType schemapb.DataType) (bool, error) {
	switch dataType {
	case schemapb.DataType_Bool, schemapb.DataType_Int8,
		schemapb.DataType_Int16, schemapb.DataType_Int32,
		schemapb.DataType_Int64,
		schemapb.DataType_Float, schemapb.DataType_Double:
		return false, nil

	case schemapb.DataType_FloatVector, schemapb.DataType_BinaryVector, schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector, schemapb.DataType_SparseFloatVector:
		return true, nil
	}

	return false, fmt.Errorf("invalid data type: %d", dataType)
}

func validateMetricType(dataType schemapb.DataType, metricTypeStrRaw string) error {
	metricTypeStr := strings.ToUpper(metricTypeStrRaw)
	switch metricTypeStr {
	case metric.L2, metric.IP, metric.COSINE:
		if typeutil.IsFloatVectorType(dataType) {
			return nil
		}
	case metric.JACCARD, metric.HAMMING, metric.SUBSTRUCTURE, metric.SUPERSTRUCTURE:
		if dataType == schemapb.DataType_BinaryVector {
			return nil
		}
	}
	return fmt.Errorf("data_type %s mismatch with metric_type %s", dataType.String(), metricTypeStrRaw)
}

func validateSchema(coll *schemapb.CollectionSchema) error {
	autoID := coll.AutoID
	primaryIdx := -1
	idMap := make(map[int64]int)    // fieldId -> idx
	nameMap := make(map[string]int) // name -> idx
	for idx, field := range coll.Fields {
		// check system field
		if field.FieldID < 100 {
			// System Fields, not injected yet
			return fmt.Errorf("fieldID(%d) that is less than 100 is reserved for system fields: %s", field.FieldID, field.Name)
		}

		// primary key detector
		if field.IsPrimaryKey {
			if autoID {
				return errors.New("autoId forbids primary key")
			} else if primaryIdx != -1 {
				return fmt.Errorf("there are more than one primary key, field name = %s, %s", coll.Fields[primaryIdx].Name, field.Name)
			}
			if field.DataType != schemapb.DataType_Int64 {
				return errors.New("type of primary key should be int64")
			}
			primaryIdx = idx
		}
		// check unique
		elemIdx, ok := idMap[field.FieldID]
		if ok {
			return fmt.Errorf("duplicate field ids: %d", coll.Fields[elemIdx].FieldID)
		}
		idMap[field.FieldID] = idx
		elemIdx, ok = nameMap[field.Name]
		if ok {
			return fmt.Errorf("duplicate field names: %s", coll.Fields[elemIdx].Name)
		}
		nameMap[field.Name] = idx

		isVec, err3 := isVector(field.DataType)
		if err3 != nil {
			return err3
		}

		if isVec {
			indexKv, err1 := RepeatedKeyValToMap(field.IndexParams)
			if err1 != nil {
				return err1
			}
			typeKv, err2 := RepeatedKeyValToMap(field.TypeParams)
			if err2 != nil {
				return err2
			}
			if !typeutil.IsSparseFloatVectorType(field.DataType) {
				dimStr, ok := typeKv[common.DimKey]
				if !ok {
					return fmt.Errorf("dim not found in type_params for vector field %s(%d)", field.Name, field.FieldID)
				}
				dim, err := strconv.Atoi(dimStr)
				if err != nil || dim < 0 {
					return fmt.Errorf("invalid dim; %s", dimStr)
				}
			}

			metricTypeStr, ok := indexKv[common.MetricTypeKey]
			if ok {
				err4 := validateMetricType(field.DataType, metricTypeStr)
				if err4 != nil {
					return err4
				}
			}
			// in C++, default type will be specified
			// do nothing
		} else {
			if len(field.IndexParams) != 0 {
				return fmt.Errorf("index params is not empty for scalar field: %s(%d)", field.Name, field.FieldID)
			}
			if len(field.TypeParams) != 0 {
				return fmt.Errorf("type params is not empty for scalar field: %s(%d)", field.Name, field.FieldID)
			}
		}
	}

	if !autoID && primaryIdx == -1 {
		return errors.New("primary key is required for non autoid mode")
	}

	return nil
}

func validateFunction(coll *schemapb.CollectionSchema) error {
	nameMap := lo.SliceToMap(coll.GetFields(), func(field *schemapb.FieldSchema) (string, *schemapb.FieldSchema) {
		return field.GetName(), field
	})
	usedOutputField := typeutil.NewSet[string]()
	usedFunctionName := typeutil.NewSet[string]()

	for _, function := range coll.GetFunctions() {
		if err := checkFunctionBasicParams(function); err != nil {
			return err
		}

		if usedFunctionName.Contain(function.GetName()) {
			return fmt.Errorf("duplicate function name: %s", function.GetName())
		}

		usedFunctionName.Insert(function.GetName())
		inputFields := []*schemapb.FieldSchema{}
		for _, name := range function.GetInputFieldNames() {
			inputField, ok := nameMap[name]
			if !ok {
				return fmt.Errorf("function input field not found: %s", name)
			}
			if inputField.GetNullable() {
				return fmt.Errorf("function input field cannot be nullable: function %s, field %s", function.GetName(), inputField.GetName())
			}
			inputFields = append(inputFields, inputField)
		}

		if err := checkFunctionInputField(function, inputFields); err != nil {
			return err
		}

		outputFields := make([]*schemapb.FieldSchema, len(function.GetOutputFieldNames()))
		for i, name := range function.GetOutputFieldNames() {
			outputField, ok := nameMap[name]
			if !ok {
				return fmt.Errorf("function output field not found: %s", name)
			}

			if outputField.GetIsPrimaryKey() {
				return fmt.Errorf("function output field cannot be primary key: function %s, field %s", function.GetName(), outputField.GetName())
			}

			if outputField.GetIsPartitionKey() || outputField.GetIsClusteringKey() {
				return fmt.Errorf("function output field cannot be partition key or clustering key: function %s, field %s", function.GetName(), outputField.GetName())
			}

			if outputField.GetNullable() {
				return fmt.Errorf("function output field cannot be nullable: function %s, field %s", function.GetName(), outputField.GetName())
			}

			outputField.IsFunctionOutput = true
			outputFields[i] = outputField
			if usedOutputField.Contain(name) {
				return fmt.Errorf("duplicate function output field: function %s, field %s", function.GetName(), name)
			}
			usedOutputField.Insert(name)
		}

		if err := checkFunctionOutputField(function, outputFields); err != nil {
			return err
		}
	}
	return nil
}

func checkFunctionOutputField(function *schemapb.FunctionSchema, fields []*schemapb.FieldSchema) error {
	switch function.GetType() {
	case schemapb.FunctionType_BM25:
		if len(fields) != 1 {
			return fmt.Errorf("BM25 function only need 1 output field, but got %d", len(fields))
		}

		if !typeutil.IsSparseFloatVectorType(fields[0].GetDataType()) {
			return fmt.Errorf("BM25 function output field must be a SparseFloatVector field, but got %s", fields[0].DataType.String())
		}
	default:
		return errors.New("check output field for unknown function type")
	}
	return nil
}

func checkFunctionInputField(function *schemapb.FunctionSchema, fields []*schemapb.FieldSchema) error {
	switch function.GetType() {
	case schemapb.FunctionType_BM25:
		if len(fields) != 1 || fields[0].DataType != schemapb.DataType_VarChar {
			return fmt.Errorf("BM25 function input field must be a VARCHAR field, got %d field with type %s",
				len(fields), fields[0].DataType.String())
		}
		h := typeutil.CreateFieldSchemaHelper(fields[0])
		if !h.EnableAnalyzer() {
			return errors.New("BM25 function input field must set enable_analyzer to true")
		}

	default:
		return errors.New("check input field with unknown function type")
	}
	return nil
}

func checkFunctionBasicParams(function *schemapb.FunctionSchema) error {
	if function.GetName() == "" {
		return errors.New("function name cannot be empty")
	}
	if len(function.GetInputFieldNames()) == 0 {
		return fmt.Errorf("function input field names cannot be empty, function: %s", function.GetName())
	}
	if len(function.GetOutputFieldNames()) == 0 {
		return fmt.Errorf("function output field names cannot be empty, function: %s", function.GetName())
	}
	for _, input := range function.GetInputFieldNames() {
		if input == "" {
			return fmt.Errorf("function input field name cannot be empty string, function: %s", function.GetName())
		}
		// if input occurs more than once, error
		if lo.Count(function.GetInputFieldNames(), input) > 1 {
			return fmt.Errorf("each function input field should be used exactly once in the same function, function: %s, input field: %s", function.GetName(), input)
		}
	}
	for _, output := range function.GetOutputFieldNames() {
		if output == "" {
			return fmt.Errorf("function output field name cannot be empty string, function: %s", function.GetName())
		}
		if lo.Count(function.GetInputFieldNames(), output) > 0 {
			return fmt.Errorf("a single field cannot be both input and output in the same function, function: %s, field: %s", function.GetName(), output)
		}
		if lo.Count(function.GetOutputFieldNames(), output) > 1 {
			return fmt.Errorf("each function output field should be used exactly once in the same function, function: %s, output field: %s", function.GetName(), output)
		}
	}
	switch function.GetType() {
	case schemapb.FunctionType_BM25:
		if len(function.GetParams()) != 0 {
			return errors.New("BM25 function accepts no params")
		}
	default:
		return errors.New("check function params with unknown function type")
	}
	return nil
}

// validateMultipleVectorFields check if schema has multiple vector fields.
func validateMultipleVectorFields(schema *schemapb.CollectionSchema) error {
	vecExist := false
	var vecName string

	for i := range schema.Fields {
		name := schema.Fields[i].Name
		dType := schema.Fields[i].DataType
		isVec := typeutil.IsVectorType(dType)
		if isVec && vecExist && !enableMultipleVectorFields {
			return fmt.Errorf(
				"multiple vector fields is not supported, fields name: %s, %s",
				vecName,
				name,
			)
		} else if isVec {
			vecExist = true
			vecName = name
		}
	}

	return nil
}

func validateLoadFieldsList(schema *schemapb.CollectionSchema) error {
	var vectorCnt int
	for _, field := range schema.Fields {
		shouldLoad, err := common.ShouldFieldBeLoaded(field.GetTypeParams())
		if err != nil {
			return err
		}
		// shoud load field, skip other check
		if shouldLoad {
			if typeutil.IsVectorType(field.GetDataType()) {
				vectorCnt++
			}
			continue
		}

		if field.IsPrimaryKey {
			return merr.WrapErrParameterInvalidMsg("Primary key field %s cannot skip loading", field.GetName())
		}

		if field.IsPartitionKey {
			return merr.WrapErrParameterInvalidMsg("Partition Key field %s cannot skip loading", field.GetName())
		}

		if field.IsClusteringKey {
			return merr.WrapErrParameterInvalidMsg("Clustering Key field %s cannot skip loading", field.GetName())
		}
	}

	if vectorCnt == 0 {
		return merr.WrapErrParameterInvalidMsg("cannot config all vector field(s) skip loading")
	}

	return nil
}

// parsePrimaryFieldData2IDs get IDs to fill grpc result, for example insert request, delete request etc.
func parsePrimaryFieldData2IDs(fieldData *schemapb.FieldData) (*schemapb.IDs, error) {
	primaryData := &schemapb.IDs{}
	switch fieldData.Field.(type) {
	case *schemapb.FieldData_Scalars:
		scalarField := fieldData.GetScalars()
		switch scalarField.Data.(type) {
		case *schemapb.ScalarField_LongData:
			primaryData.IdField = &schemapb.IDs_IntId{
				IntId: scalarField.GetLongData(),
			}
		case *schemapb.ScalarField_StringData:
			primaryData.IdField = &schemapb.IDs_StrId{
				StrId: scalarField.GetStringData(),
			}
		default:
			return nil, merr.WrapErrParameterInvalidMsg("currently only support DataType Int64 or VarChar as PrimaryField")
		}
	default:
		return nil, merr.WrapErrParameterInvalidMsg("currently not support vector field as PrimaryField")
	}

	return primaryData, nil
}

// autoGenPrimaryFieldData generate primary data when autoID == true
func autoGenPrimaryFieldData(fieldSchema *schemapb.FieldSchema, data interface{}) (*schemapb.FieldData, error) {
	var fieldData schemapb.FieldData
	fieldData.FieldName = fieldSchema.Name
	fieldData.Type = fieldSchema.DataType
	switch data := data.(type) {
	case []int64:
		switch fieldData.Type {
		case schemapb.DataType_Int64:
			fieldData.Field = &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{
							Data: data,
						},
					},
				},
			}
		case schemapb.DataType_VarChar:
			strIDs := make([]string, len(data))
			for i, v := range data {
				strIDs[i] = strconv.FormatInt(v, 10)
			}
			fieldData.Field = &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: strIDs,
						},
					},
				},
			}
		default:
			return nil, errors.New("currently only support autoID for int64 and varchar PrimaryField")
		}
	default:
		return nil, errors.New("currently only int64 is supported as the data source for the autoID of a PrimaryField")
	}

	return &fieldData, nil
}

func autoGenDynamicFieldData(data [][]byte) *schemapb.FieldData {
	return &schemapb.FieldData{
		FieldName: common.MetaFieldName,
		Type:      schemapb.DataType_JSON,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_JsonData{
					JsonData: &schemapb.JSONArray{
						Data: data,
					},
				},
			},
		},
		IsDynamic: true,
	}
}

// fillFieldPropertiesBySchema set fieldID to fieldData according FieldSchemas
func fillFieldPropertiesBySchema(columns []*schemapb.FieldData, schema *schemapb.CollectionSchema) error {
	fieldName2Schema := make(map[string]*schemapb.FieldSchema)

	expectColumnNum := 0
	for _, field := range schema.GetFields() {
		fieldName2Schema[field.Name] = field
		if !field.GetIsFunctionOutput() {
			expectColumnNum++
		}
	}

	if len(columns) != expectColumnNum {
		return fmt.Errorf("len(columns) mismatch the expectColumnNum, expectColumnNum: %d, len(columns): %d",
			expectColumnNum, len(columns))
	}

	for _, fieldData := range columns {
		if fieldSchema, ok := fieldName2Schema[fieldData.FieldName]; ok {
			fieldData.FieldId = fieldSchema.FieldID
			fieldData.Type = fieldSchema.DataType

			// Set the ElementType because it may not be set in the insert request.
			if fieldData.Type == schemapb.DataType_Array {
				fd, ok := fieldData.Field.(*schemapb.FieldData_Scalars)
				if !ok {
					return fmt.Errorf("field convert FieldData_Scalars fail in fieldData, fieldName: %s,"+
						" collectionName:%s", fieldData.FieldName, schema.Name)
				}
				fd.Scalars.GetArrayData().ElementType = fieldSchema.ElementType
			}
		} else {
			return fmt.Errorf("fieldName %v not exist in collection schema", fieldData.FieldName)
		}
	}

	return nil
}

func ValidateUsername(username string) error {
	username = strings.TrimSpace(username)

	if username == "" {
		return merr.WrapErrParameterInvalidMsg("username must be not empty")
	}

	if len(username) > Params.ProxyCfg.MaxUsernameLength.GetAsInt() {
		return merr.WrapErrParameterInvalidMsg("invalid username %s with length %d, the length of username must be less than %d", username, len(username), Params.ProxyCfg.MaxUsernameLength.GetValue())
	}

	firstChar := username[0]
	if !isAlpha(firstChar) {
		return merr.WrapErrParameterInvalidMsg("invalid user name %s, the first character must be a letter, but got %s", username, string(firstChar))
	}

	usernameSize := len(username)
	for i := 1; i < usernameSize; i++ {
		c := username[i]
		if c != '_' && !isAlpha(c) && !isNumber(c) {
			return merr.WrapErrParameterInvalidMsg("invalid user name %s, username must contain only numbers, letters and underscores, but got %s", username, c)
		}
	}
	return nil
}

func ValidatePassword(password string) error {
	if len(password) < Params.ProxyCfg.MinPasswordLength.GetAsInt() || len(password) > Params.ProxyCfg.MaxPasswordLength.GetAsInt() {
		return merr.WrapErrParameterInvalidRange(Params.ProxyCfg.MinPasswordLength.GetAsInt(),
			Params.ProxyCfg.MaxPasswordLength.GetAsInt(),
			len(password), "invalid password length")
	}
	return nil
}

func ReplaceID2Name(oldStr string, id int64, name string) string {
	return strings.ReplaceAll(oldStr, strconv.FormatInt(id, 10), name)
}

func parseGuaranteeTsFromConsistency(ts, tMax typeutil.Timestamp, consistency commonpb.ConsistencyLevel) typeutil.Timestamp {
	switch consistency {
	case commonpb.ConsistencyLevel_Strong:
		ts = tMax
	case commonpb.ConsistencyLevel_Bounded:
		ratio := Params.CommonCfg.GracefulTime.GetAsDuration(time.Millisecond)
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
		ratio := Params.CommonCfg.GracefulTime.GetAsDuration(time.Millisecond)
		ts = tsoutil.AddPhysicalDurationOnTs(tMax, -ratio)
	}
	return ts
}

func getMaxMvccTsFromChannels(channelsTs map[string]uint64, beginTs typeutil.Timestamp) typeutil.Timestamp {
	maxTs := typeutil.Timestamp(0)
	for _, ts := range channelsTs {
		if ts > maxTs {
			maxTs = ts
		}
	}

	if maxTs == 0 {
		log.Warn("no channel ts found, use beginTs instead")
		return beginTs
	}

	return maxTs
}

func validateName(entity string, nameType string) error {
	entity = strings.TrimSpace(entity)

	if entity == "" {
		return merr.WrapErrParameterInvalid("not empty", entity, nameType+" should be not empty")
	}

	if len(entity) > Params.ProxyCfg.MaxNameLength.GetAsInt() {
		return merr.WrapErrParameterInvalidRange(0,
			Params.ProxyCfg.MaxNameLength.GetAsInt(),
			len(entity),
			fmt.Sprintf("the length of %s must be not greater than limit", nameType))
	}

	firstChar := entity[0]
	if firstChar != '_' && !isAlpha(firstChar) {
		return merr.WrapErrParameterInvalid('_',
			firstChar,
			fmt.Sprintf("the first character of %s must be an underscore or letter", nameType))
	}

	for i := 1; i < len(entity); i++ {
		c := entity[i]
		if c != '_' && c != '$' && !isAlpha(c) && !isNumber(c) {
			return merr.WrapErrParameterInvalidMsg("%s can only contain numbers, letters, dollars and underscores, found %c at %d", nameType, c, i)
		}
	}
	return nil
}

func ValidateRoleName(entity string) error {
	return validateName(entity, "role name")
}

func IsDefaultRole(roleName string) bool {
	for _, defaultRole := range util.DefaultRoles {
		if defaultRole == roleName {
			return true
		}
	}
	return false
}

func ValidateObjectName(entity string) error {
	if util.IsAnyWord(entity) {
		return nil
	}
	return validateName(entity, "object name")
}

func ValidateCollectionName(entity string) error {
	if util.IsAnyWord(entity) {
		return nil
	}
	return validateName(entity, "collection name")
}

func ValidateObjectType(entity string) error {
	return validateName(entity, "ObjectType")
}

func ValidatePrivilege(entity string) error {
	if util.IsAnyWord(entity) {
		return nil
	}
	return validateName(entity, "Privilege")
}

func GetCurUserFromContext(ctx context.Context) (string, error) {
	return contextutil.GetCurUserFromContext(ctx)
}

func GetCurUserFromContextOrDefault(ctx context.Context) string {
	username, _ := GetCurUserFromContext(ctx)
	return username
}

func GetCurDBNameFromContextOrDefault(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return util.DefaultDBName
	}
	dbNameData := md[strings.ToLower(util.HeaderDBName)]
	if len(dbNameData) < 1 || dbNameData[0] == "" {
		return util.DefaultDBName
	}
	return dbNameData[0]
}

func NewContextWithMetadata(ctx context.Context, username string, dbName string) context.Context {
	dbKey := strings.ToLower(util.HeaderDBName)
	if dbName != "" {
		ctx = contextutil.AppendToIncomingContext(ctx, dbKey, dbName)
	}
	if username != "" {
		originValue := fmt.Sprintf("%s%s%s", username, util.CredentialSeperator, username)
		authKey := strings.ToLower(util.HeaderAuthorize)
		authValue := crypto.Base64Encode(originValue)
		ctx = contextutil.AppendToIncomingContext(ctx, authKey, authValue)
	}
	return ctx
}

func AppendUserInfoForRPC(ctx context.Context) context.Context {
	curUser, _ := GetCurUserFromContext(ctx)
	if curUser != "" {
		originValue := fmt.Sprintf("%s%s%s", curUser, util.CredentialSeperator, curUser)
		authKey := strings.ToLower(util.HeaderAuthorize)
		authValue := crypto.Base64Encode(originValue)
		ctx = metadata.AppendToOutgoingContext(ctx, authKey, authValue)
	}
	return ctx
}

func GetRole(username string) ([]string, error) {
	if globalMetaCache == nil {
		return []string{}, merr.WrapErrServiceUnavailable("internal: Milvus Proxy is not ready yet. please wait")
	}
	return globalMetaCache.GetUserRole(username), nil
}

func PasswordVerify(ctx context.Context, username, rawPwd string) bool {
	return passwordVerify(ctx, username, rawPwd, globalMetaCache)
}

func VerifyAPIKey(rawToken string) (string, error) {
	hoo := hookutil.GetHook()
	user, err := hoo.VerifyAPIKey(rawToken)
	if err != nil {
		log.Warn("fail to verify apikey", zap.String("api_key", rawToken), zap.Error(err))
		return "", merr.WrapErrParameterInvalidMsg("invalid apikey: [%s]", rawToken)
	}
	return user, nil
}

// PasswordVerify verify password
func passwordVerify(ctx context.Context, username, rawPwd string, globalMetaCache Cache) bool {
	// it represents the cache miss if Sha256Password is empty within credInfo, which shall be updated first connection.
	// meanwhile, generating Sha256Password depends on raw password and encrypted password will not cache.
	credInfo, err := globalMetaCache.GetCredentialInfo(ctx, username)
	if err != nil {
		log.Ctx(ctx).Error("found no credential", zap.String("username", username), zap.Error(err))
		return false
	}

	// hit cache
	sha256Pwd := crypto.SHA256(rawPwd, credInfo.Username)
	if credInfo.Sha256Password != "" {
		return sha256Pwd == credInfo.Sha256Password
	}

	// miss cache, verify against encrypted password from etcd
	if err := bcrypt.CompareHashAndPassword([]byte(credInfo.EncryptedPassword), []byte(rawPwd)); err != nil {
		log.Ctx(ctx).Error("Verify password failed", zap.Error(err))
		return false
	}

	// update cache after miss cache
	credInfo.Sha256Password = sha256Pwd
	log.Ctx(ctx).Debug("get credential miss cache, update cache with", zap.Any("credential", credInfo))
	globalMetaCache.UpdateCredential(credInfo)
	return true
}

func translatePkOutputFields(schema *schemapb.CollectionSchema) ([]string, []int64) {
	pkNames := []string{}
	fieldIDs := []int64{}
	for _, field := range schema.Fields {
		if field.IsPrimaryKey {
			pkNames = append(pkNames, field.GetName())
			fieldIDs = append(fieldIDs, field.GetFieldID())
		}
	}
	return pkNames, fieldIDs
}

func recallCal[T string | int64](results []T, gts []T) float32 {
	hit := 0
	total := 0
	for _, r := range results {
		total++
		for _, gt := range gts {
			if r == gt {
				hit++
				break
			}
		}
	}
	return float32(hit) / float32(total)
}

func computeRecall(results *schemapb.SearchResultData, gts *schemapb.SearchResultData) error {
	if results.GetNumQueries() != gts.GetNumQueries() {
		return fmt.Errorf("num of queries is inconsistent between search results(%d) and ground truth(%d)", results.GetNumQueries(), gts.GetNumQueries())
	}

	switch results.GetIds().GetIdField().(type) {
	case *schemapb.IDs_IntId:
		switch gts.GetIds().GetIdField().(type) {
		case *schemapb.IDs_IntId:
			currentResultIndex := int64(0)
			currentGTIndex := int64(0)
			recalls := make([]float32, 0, results.GetNumQueries())
			for i := 0; i < int(results.GetNumQueries()); i++ {
				currentResultTopk := results.GetTopks()[i]
				currentGTTopk := gts.GetTopks()[i]
				recalls = append(recalls, recallCal(results.GetIds().GetIntId().GetData()[currentResultIndex:currentResultIndex+currentResultTopk],
					gts.GetIds().GetIntId().GetData()[currentGTIndex:currentGTIndex+currentGTTopk]))
				currentResultIndex += currentResultTopk
				currentGTIndex += currentGTTopk
			}
			results.Recalls = recalls
			return nil
		case *schemapb.IDs_StrId:
			return errors.New("pk type is inconsistent between search results(int64) and ground truth(string)")
		default:
			return errors.New("unsupported pk type")
		}

	case *schemapb.IDs_StrId:
		switch gts.GetIds().GetIdField().(type) {
		case *schemapb.IDs_StrId:
			currentResultIndex := int64(0)
			currentGTIndex := int64(0)
			recalls := make([]float32, 0, results.GetNumQueries())
			for i := 0; i < int(results.GetNumQueries()); i++ {
				currentResultTopk := results.GetTopks()[i]
				currentGTTopk := gts.GetTopks()[i]
				recalls = append(recalls, recallCal(results.GetIds().GetStrId().GetData()[currentResultIndex:currentResultIndex+currentResultTopk],
					gts.GetIds().GetStrId().GetData()[currentGTIndex:currentGTIndex+currentGTTopk]))
				currentResultIndex += currentResultTopk
				currentGTIndex += currentGTTopk
			}
			results.Recalls = recalls
			return nil
		case *schemapb.IDs_IntId:
			return errors.New("pk type is inconsistent between search results(string) and ground truth(int64)")
		default:
			return errors.New("unsupported pk type")
		}
	default:
		return errors.New("unsupported pk type")
	}
}

// Support wildcard in output fields:
//
//	"*" - all fields
//
// For example, A and B are scalar fields, C and D are vector fields, duplicated fields will automatically be removed.
//
//	output_fields=["*"] 	 ==> [A,B,C,D]
//	output_fields=["*",A] 	 ==> [A,B,C,D]
//	output_fields=["*",C]    ==> [A,B,C,D]
//
// 4th return value is true if user requested pk field explicitly or using wildcard.
// if removePkField is true, pk field will not be include in the first(resultFieldNames)/second(userOutputFields)
// return value.
func translateOutputFields(outputFields []string, schema *schemaInfo, removePkField bool) ([]string, []string, []string, bool, error) {
	var primaryFieldName string
	var dynamicField *schemapb.FieldSchema
	allFieldNameMap := make(map[string]*schemapb.FieldSchema)
	resultFieldNameMap := make(map[string]bool)
	resultFieldNames := make([]string, 0)
	userOutputFieldsMap := make(map[string]bool)
	userOutputFields := make([]string, 0)
	userDynamicFieldsMap := make(map[string]bool)
	userDynamicFields := make([]string, 0)
	useAllDyncamicFields := false
	for _, field := range schema.Fields {
		if field.IsPrimaryKey {
			primaryFieldName = field.Name
		}
		if field.IsDynamic {
			dynamicField = field
		}
		allFieldNameMap[field.Name] = field
	}

	userRequestedPkFieldExplicitly := false

	for _, outputFieldName := range outputFields {
		outputFieldName = strings.TrimSpace(outputFieldName)
		if outputFieldName == primaryFieldName {
			userRequestedPkFieldExplicitly = true
		}
		if outputFieldName == "*" {
			userRequestedPkFieldExplicitly = true
			for fieldName, field := range allFieldNameMap {
				// skip Cold field and fields that can't be output
				if schema.IsFieldLoaded(field.GetFieldID()) && schema.CanRetrieveRawFieldData(field) {
					resultFieldNameMap[fieldName] = true
					userOutputFieldsMap[fieldName] = true
				}
			}
			useAllDyncamicFields = true
		} else {
			if field, ok := allFieldNameMap[outputFieldName]; ok {
				if !schema.CanRetrieveRawFieldData(field) {
					return nil, nil, nil, false, fmt.Errorf("not allowed to retrieve raw data of field %s", outputFieldName)
				}
				if schema.IsFieldLoaded(field.GetFieldID()) {
					resultFieldNameMap[outputFieldName] = true
					userOutputFieldsMap[outputFieldName] = true
				} else {
					return nil, nil, nil, false, fmt.Errorf("field %s is not loaded", outputFieldName)
				}
			} else {
				if schema.EnableDynamicField {
					if schema.IsFieldLoaded(dynamicField.GetFieldID()) {
						schemaH, err := typeutil.CreateSchemaHelper(schema.CollectionSchema)
						if err != nil {
							return nil, nil, nil, false, err
						}
						err = planparserv2.ParseIdentifier(schemaH, outputFieldName, func(expr *planpb.Expr) error {
							if len(expr.GetColumnExpr().GetInfo().GetNestedPath()) == 1 &&
								expr.GetColumnExpr().GetInfo().GetNestedPath()[0] == outputFieldName {
								return nil
							}
							return errors.New("not support getting subkeys of json field yet")
						})
						if err != nil {
							log.Info("parse output field name failed", zap.String("field name", outputFieldName))
							return nil, nil, nil, false, fmt.Errorf("parse output field name failed: %s", outputFieldName)
						}
						resultFieldNameMap[common.MetaFieldName] = true
						userOutputFieldsMap[outputFieldName] = true
						userDynamicFieldsMap[outputFieldName] = true
					} else {
						// TODO after cold field be able to fetched with chunk cache, this check shall be removed
						return nil, nil, nil, false, fmt.Errorf("field %s cannot be returned since dynamic field not loaded", outputFieldName)
					}
				} else {
					return nil, nil, nil, false, fmt.Errorf("field %s not exist", outputFieldName)
				}
			}
		}
	}

	if removePkField {
		delete(resultFieldNameMap, primaryFieldName)
		delete(userOutputFieldsMap, primaryFieldName)
	}

	for fieldName := range resultFieldNameMap {
		resultFieldNames = append(resultFieldNames, fieldName)
	}
	for fieldName := range userOutputFieldsMap {
		userOutputFields = append(userOutputFields, fieldName)
	}
	if !useAllDyncamicFields {
		for fieldName := range userDynamicFieldsMap {
			userDynamicFields = append(userDynamicFields, fieldName)
		}
	}

	return resultFieldNames, userOutputFields, userDynamicFields, userRequestedPkFieldExplicitly, nil
}

func validateIndexName(indexName string) error {
	indexName = strings.TrimSpace(indexName)

	if indexName == "" {
		return nil
	}
	invalidMsg := "Invalid index name: " + indexName + ". "
	if len(indexName) > Params.ProxyCfg.MaxNameLength.GetAsInt() {
		msg := invalidMsg + "The length of a index name must be less than " + Params.ProxyCfg.MaxNameLength.GetValue() + " characters."
		return errors.New(msg)
	}

	firstChar := indexName[0]
	if firstChar != '_' && !isAlpha(firstChar) {
		msg := invalidMsg + "The first character of a index name must be an underscore or letter."
		return errors.New(msg)
	}

	indexNameSize := len(indexName)
	for i := 1; i < indexNameSize; i++ {
		c := indexName[i]
		if c != '_' && !isAlpha(c) && !isNumber(c) {
			msg := invalidMsg + "Index name can only contain numbers, letters, and underscores."
			return errors.New(msg)
		}
	}
	return nil
}

func isCollectionLoaded(ctx context.Context, qc types.QueryCoordClient, collID int64) (bool, error) {
	// get all loading collections
	resp, err := qc.ShowCollections(ctx, &querypb.ShowCollectionsRequest{
		CollectionIDs: nil,
	})
	if err != nil {
		return false, err
	}
	if resp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		return false, merr.Error(resp.GetStatus())
	}

	for _, loadedCollID := range resp.GetCollectionIDs() {
		if collID == loadedCollID {
			return true, nil
		}
	}
	return false, nil
}

func isPartitionLoaded(ctx context.Context, qc types.QueryCoordClient, collID int64, partID int64) (bool, error) {
	// get all loading collections
	resp, err := qc.ShowPartitions(ctx, &querypb.ShowPartitionsRequest{
		CollectionID: collID,
		PartitionIDs: []int64{partID},
	})
	if err := merr.CheckRPCCall(resp, err); err != nil {
		// qc returns error if partition not loaded
		if errors.Is(err, merr.ErrPartitionNotLoaded) {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

func checkFieldsDataBySchema(schema *schemapb.CollectionSchema, insertMsg *msgstream.InsertMsg, inInsert bool) error {
	log := log.With(zap.String("collection", schema.GetName()))
	primaryKeyNum := 0
	autoGenFieldNum := 0

	dataNameSet := typeutil.NewSet[string]()
	for _, data := range insertMsg.FieldsData {
		fieldName := data.GetFieldName()
		if dataNameSet.Contain(fieldName) {
			return merr.WrapErrParameterInvalidMsg("duplicated field %s found", fieldName)
		}
		dataNameSet.Insert(fieldName)
	}

	for _, fieldSchema := range schema.Fields {
		if fieldSchema.AutoID && !fieldSchema.IsPrimaryKey {
			log.Warn("not primary key field, but set autoID true", zap.String("field", fieldSchema.GetName()))
			return merr.WrapErrParameterInvalidMsg("only primary key could be with AutoID enabled")
		}

		if fieldSchema.IsPrimaryKey {
			primaryKeyNum++
		}
		if fieldSchema.GetDefaultValue() != nil && fieldSchema.IsPrimaryKey {
			return merr.WrapErrParameterInvalidMsg("primary key can't be with default value")
		}
		if (fieldSchema.IsPrimaryKey && fieldSchema.AutoID && !Params.ProxyCfg.SkipAutoIDCheck.GetAsBool() && inInsert) || fieldSchema.GetIsFunctionOutput() {
			// when inInsert, no need to pass when pk is autoid and SkipAutoIDCheck is false
			autoGenFieldNum++
		}
		if _, ok := dataNameSet[fieldSchema.GetName()]; !ok {
			if (fieldSchema.IsPrimaryKey && fieldSchema.AutoID && !Params.ProxyCfg.SkipAutoIDCheck.GetAsBool() && inInsert) || fieldSchema.GetIsFunctionOutput() {
				// autoGenField
				continue
			}

			if fieldSchema.GetDefaultValue() == nil && !fieldSchema.GetNullable() {
				log.Warn("no corresponding fieldData pass in", zap.String("fieldSchema", fieldSchema.GetName()))
				return merr.WrapErrParameterInvalidMsg("fieldSchema(%s) has no corresponding fieldData pass in", fieldSchema.GetName())
			}
			// when use default_value or has set Nullable
			// it's ok that no corresponding fieldData found
			dataToAppend := &schemapb.FieldData{
				Type:      fieldSchema.GetDataType(),
				FieldName: fieldSchema.GetName(),
			}
			insertMsg.FieldsData = append(insertMsg.FieldsData, dataToAppend)
		}
	}

	if primaryKeyNum > 1 {
		log.Warn("more than 1 primary keys not supported",
			zap.Int64("primaryKeyNum", int64(primaryKeyNum)))
		return merr.WrapErrParameterInvalidMsg("more than 1 primary keys not supported, got %d", primaryKeyNum)
	}

	expectedNum := len(schema.Fields)
	actualNum := len(insertMsg.FieldsData) + autoGenFieldNum

	if expectedNum != actualNum {
		log.Warn("the number of fields is not the same as needed", zap.Int("expected", expectedNum), zap.Int("actual", actualNum))
		return merr.WrapErrParameterInvalid(expectedNum, actualNum, "more fieldData has pass in")
	}

	return nil
}

func checkPrimaryFieldData(schema *schemapb.CollectionSchema, insertMsg *msgstream.InsertMsg) (*schemapb.IDs, error) {
	log := log.With(zap.String("collectionName", insertMsg.CollectionName))
	rowNums := uint32(insertMsg.NRows())
	// TODO(dragondriver): in fact, NumRows is not trustable, we should check all input fields
	if insertMsg.NRows() <= 0 {
		return nil, merr.WrapErrParameterInvalid("invalid num_rows", fmt.Sprint(rowNums), "num_rows should be greater than 0")
	}

	if err := checkFieldsDataBySchema(schema, insertMsg, true); err != nil {
		return nil, err
	}

	primaryFieldSchema, err := typeutil.GetPrimaryFieldSchema(schema)
	if err != nil {
		log.Error("get primary field schema failed", zap.Any("schema", schema), zap.Error(err))
		return nil, err
	}
	if primaryFieldSchema.GetNullable() {
		return nil, merr.WrapErrParameterInvalidMsg("primary field not support null")
	}
	var primaryFieldData *schemapb.FieldData
	// when checkPrimaryFieldData in insert

	skipAutoIDCheck := Params.ProxyCfg.SkipAutoIDCheck.GetAsBool() &&
		primaryFieldSchema.AutoID &&
		typeutil.IsPrimaryFieldDataExist(insertMsg.GetFieldsData(), primaryFieldSchema)

	if !primaryFieldSchema.AutoID || skipAutoIDCheck {
		primaryFieldData, err = typeutil.GetPrimaryFieldData(insertMsg.GetFieldsData(), primaryFieldSchema)
		if err != nil {
			log.Info("get primary field data failed", zap.Error(err))
			return nil, err
		}
	} else {
		// check primary key data not exist
		if typeutil.IsPrimaryFieldDataExist(insertMsg.GetFieldsData(), primaryFieldSchema) {
			return nil, merr.WrapErrParameterInvalidMsg(fmt.Sprintf("can not assign primary field data when auto id enabled %v", primaryFieldSchema.Name))
		}
		// if autoID == true, currently support autoID for int64 and varchar PrimaryField
		primaryFieldData, err = autoGenPrimaryFieldData(primaryFieldSchema, insertMsg.GetRowIDs())
		if err != nil {
			log.Info("generate primary field data failed when autoID == true", zap.Error(err))
			return nil, err
		}
		// if autoID == true, set the primary field data
		// insertMsg.fieldsData need append primaryFieldData
		insertMsg.FieldsData = append(insertMsg.FieldsData, primaryFieldData)
	}

	// parse primaryFieldData to result.IDs, and as returned primary keys
	ids, err := parsePrimaryFieldData2IDs(primaryFieldData)
	if err != nil {
		log.Warn("parse primary field data to IDs failed", zap.Error(err))
		return nil, err
	}

	return ids, nil
}

// for some varchar with analzyer
// we need check char format before insert it to message queue
// now only support utf-8
func checkVarcharFormat(schema *schemapb.CollectionSchema, insertMsg *msgstream.InsertMsg) error {
	checkeFields := lo.FilterMap(schema.GetFields(), func(field *schemapb.FieldSchema, _ int) (int64, bool) {
		if field.DataType == schemapb.DataType_VarChar {
			return field.GetFieldID(), true
		}

		return 0, false
	})

	if len(checkeFields) == 0 {
		return nil
	}

	for _, fieldData := range insertMsg.FieldsData {
		if !lo.Contains(checkeFields, fieldData.GetFieldId()) {
			continue
		}

		strData := fieldData.GetScalars().GetStringData()
		for row, data := range strData.GetData() {
			ok := utf8.ValidString(data)
			if !ok {
				log.Warn("string field data not utf-8 format", zap.String("messageVersion", strData.ProtoReflect().Descriptor().Syntax().GoString()))
				return merr.WrapErrAsInputError(fmt.Errorf("input with analyzer should be utf-8 format, but row: %d not utf-8 format. data: %s", row, data))
			}
		}
	}
	return nil
}

func checkUpsertPrimaryFieldData(schema *schemapb.CollectionSchema, insertMsg *msgstream.InsertMsg) (*schemapb.IDs, *schemapb.IDs, error) {
	log := log.With(zap.String("collectionName", insertMsg.CollectionName))
	rowNums := uint32(insertMsg.NRows())
	// TODO(dragondriver): in fact, NumRows is not trustable, we should check all input fields
	if insertMsg.NRows() <= 0 {
		return nil, nil, merr.WrapErrParameterInvalid("invalid num_rows", fmt.Sprint(rowNums), "num_rows should be greater than 0")
	}

	if err := checkFieldsDataBySchema(schema, insertMsg, false); err != nil {
		return nil, nil, err
	}

	primaryFieldSchema, err := typeutil.GetPrimaryFieldSchema(schema)
	if err != nil {
		log.Error("get primary field schema failed", zap.Any("schema", schema), zap.Error(err))
		return nil, nil, err
	}
	if primaryFieldSchema.GetNullable() {
		return nil, nil, merr.WrapErrParameterInvalidMsg("primary field not support null")
	}
	// get primaryFieldData whether autoID is true or not
	var primaryFieldData *schemapb.FieldData
	var newPrimaryFieldData *schemapb.FieldData

	primaryFieldID := primaryFieldSchema.FieldID
	primaryFieldName := primaryFieldSchema.Name
	for i, field := range insertMsg.GetFieldsData() {
		if field.FieldId == primaryFieldID || field.FieldName == primaryFieldName {
			primaryFieldData = field
			if primaryFieldSchema.AutoID {
				// use the passed pk as new pk when autoID == false
				// automatic generate pk as new pk wehen autoID == true
				newPrimaryFieldData, err = autoGenPrimaryFieldData(primaryFieldSchema, insertMsg.GetRowIDs())
				if err != nil {
					log.Info("generate new primary field data failed when upsert", zap.Error(err))
					return nil, nil, err
				}
				insertMsg.FieldsData = append(insertMsg.GetFieldsData()[:i], insertMsg.GetFieldsData()[i+1:]...)
				insertMsg.FieldsData = append(insertMsg.FieldsData, newPrimaryFieldData)
			}
			break
		}
	}
	// must assign primary field data when upsert
	if primaryFieldData == nil {
		return nil, nil, merr.WrapErrParameterInvalidMsg(fmt.Sprintf("must assign pk when upsert, primary field: %v", primaryFieldName))
	}

	// parse primaryFieldData to result.IDs, and as returned primary keys
	ids, err := parsePrimaryFieldData2IDs(primaryFieldData)
	if err != nil {
		log.Warn("parse primary field data to IDs failed", zap.Error(err))
		return nil, nil, err
	}
	if !primaryFieldSchema.GetAutoID() {
		return ids, ids, nil
	}
	newIds, err := parsePrimaryFieldData2IDs(newPrimaryFieldData)
	if err != nil {
		log.Warn("parse primary field data to IDs failed", zap.Error(err))
		return nil, nil, err
	}
	return newIds, ids, nil
}

func getPartitionKeyFieldData(fieldSchema *schemapb.FieldSchema, insertMsg *msgstream.InsertMsg) (*schemapb.FieldData, error) {
	if len(insertMsg.GetPartitionName()) > 0 && !Params.ProxyCfg.SkipPartitionKeyCheck.GetAsBool() {
		return nil, errors.New("not support manually specifying the partition names if partition key mode is used")
	}

	for _, fieldData := range insertMsg.GetFieldsData() {
		if fieldData.GetFieldId() == fieldSchema.GetFieldID() {
			return fieldData, nil
		}
	}

	return nil, errors.New("partition key not specify when insert")
}

func getCollectionProgress(
	ctx context.Context,
	queryCoord types.QueryCoordClient,
	msgBase *commonpb.MsgBase,
	collectionID int64,
) (loadProgress int64, refreshProgress int64, err error) {
	resp, err := queryCoord.ShowCollections(ctx, &querypb.ShowCollectionsRequest{
		Base: commonpbutil.UpdateMsgBase(
			msgBase,
			commonpbutil.WithMsgType(commonpb.MsgType_ShowCollections),
		),
		CollectionIDs: []int64{collectionID},
	})
	if err != nil {
		log.Ctx(ctx).Warn("fail to show collections",
			zap.Int64("collectionID", collectionID),
			zap.Error(err),
		)
		return
	}

	err = merr.Error(resp.GetStatus())
	if err != nil {
		log.Ctx(ctx).Warn("fail to show collections",
			zap.Int64("collectionID", collectionID),
			zap.Error(err))
		return
	}

	loadProgress = resp.GetInMemoryPercentages()[0]
	if len(resp.GetRefreshProgress()) > 0 { // Compatibility for new Proxy with old QueryCoord
		refreshProgress = resp.GetRefreshProgress()[0]
	}

	return
}

func getPartitionProgress(
	ctx context.Context,
	queryCoord types.QueryCoordClient,
	msgBase *commonpb.MsgBase,
	partitionNames []string,
	collectionName string,
	collectionID int64,
	dbName string,
) (loadProgress int64, refreshProgress int64, err error) {
	IDs2Names := make(map[int64]string)
	partitionIDs := make([]int64, 0)
	for _, partitionName := range partitionNames {
		var partitionID int64
		partitionID, err = globalMetaCache.GetPartitionID(ctx, dbName, collectionName, partitionName)
		if err != nil {
			return
		}
		IDs2Names[partitionID] = partitionName
		partitionIDs = append(partitionIDs, partitionID)
	}

	var resp *querypb.ShowPartitionsResponse
	resp, err = queryCoord.ShowPartitions(ctx, &querypb.ShowPartitionsRequest{
		Base: commonpbutil.UpdateMsgBase(
			msgBase,
			commonpbutil.WithMsgType(commonpb.MsgType_ShowPartitions),
		),
		CollectionID: collectionID,
		PartitionIDs: partitionIDs,
	})
	if err != nil {
		log.Ctx(ctx).Warn("fail to show partitions", zap.Int64("collection_id", collectionID),
			zap.String("collection_name", collectionName),
			zap.Strings("partition_names", partitionNames),
			zap.Error(err))
		return
	}

	err = merr.Error(resp.GetStatus())
	if err != nil {
		err = merr.Error(resp.GetStatus())
		log.Ctx(ctx).Warn("fail to show partitions",
			zap.String("collectionName", collectionName),
			zap.Strings("partitionNames", partitionNames),
			zap.Error(err))
		return
	}

	for _, p := range resp.InMemoryPercentages {
		loadProgress += p
	}
	loadProgress /= int64(len(partitionIDs))

	if len(resp.GetRefreshProgress()) > 0 { // Compatibility for new Proxy with old QueryCoord
		refreshProgress = resp.GetRefreshProgress()[0]
	}

	return
}

func isPartitionKeyMode(ctx context.Context, dbName string, colName string) (bool, error) {
	colSchema, err := globalMetaCache.GetCollectionSchema(ctx, dbName, colName)
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

func hasPartitionKeyModeField(schema *schemapb.CollectionSchema) bool {
	for _, fieldSchema := range schema.GetFields() {
		if fieldSchema.IsPartitionKey {
			return true
		}
	}
	return false
}

// getDefaultPartitionsInPartitionKeyMode only used in partition key mode
func getDefaultPartitionsInPartitionKeyMode(ctx context.Context, dbName string, collectionName string) ([]string, error) {
	partitions, err := globalMetaCache.GetPartitions(ctx, dbName, collectionName)
	if err != nil {
		return nil, err
	}

	// Make sure the order of the partition names got every time is the same
	partitionNames, _, err := typeutil.RearrangePartitionsForPartitionKey(partitions)
	if err != nil {
		return nil, err
	}

	return partitionNames, nil
}

func assignChannelsByPK(pks *schemapb.IDs, channelNames []string, insertMsg *msgstream.InsertMsg) map[string][]int {
	insertMsg.HashValues = typeutil.HashPK2Channels(pks, channelNames)

	// groupedHashKeys represents the dmChannel index
	channel2RowOffsets := make(map[string][]int) //   channelName to count
	// assert len(it.hashValues) < maxInt
	for offset, channelID := range insertMsg.HashValues {
		channelName := channelNames[channelID]
		if _, ok := channel2RowOffsets[channelName]; !ok {
			channel2RowOffsets[channelName] = []int{}
		}
		channel2RowOffsets[channelName] = append(channel2RowOffsets[channelName], offset)
	}

	return channel2RowOffsets
}

func assignPartitionKeys(ctx context.Context, dbName string, collName string, keys []*planpb.GenericValue) ([]string, error) {
	partitionNames, err := globalMetaCache.GetPartitionsIndex(ctx, dbName, collName)
	if err != nil {
		return nil, err
	}

	schema, err := globalMetaCache.GetCollectionSchema(ctx, dbName, collName)
	if err != nil {
		return nil, err
	}

	partitionKeyFieldSchema, err := typeutil.GetPartitionKeyFieldSchema(schema.CollectionSchema)
	if err != nil {
		return nil, err
	}

	hashedPartitionNames, err := typeutil2.HashKey2Partitions(partitionKeyFieldSchema, keys, partitionNames)
	return hashedPartitionNames, err
}

func ErrWithLog(logger *log.MLogger, msg string, err error) error {
	wrapErr := errors.Wrap(err, msg)
	if logger != nil {
		logger.Warn(msg, zap.Error(err))
		return wrapErr
	}
	log.Warn(msg, zap.Error(err))
	return wrapErr
}

func verifyDynamicFieldData(schema *schemapb.CollectionSchema, insertMsg *msgstream.InsertMsg) error {
	for _, field := range insertMsg.FieldsData {
		if field.GetFieldName() == common.MetaFieldName {
			if !schema.EnableDynamicField {
				return fmt.Errorf("without dynamic schema enabled, the field name cannot be set to %s", common.MetaFieldName)
			}
			for _, rowData := range field.GetScalars().GetJsonData().GetData() {
				jsonData := make(map[string]interface{})
				if err := json.Unmarshal(rowData, &jsonData); err != nil {
					log.Info("insert invalid dynamic data, milvus only support json map",
						zap.ByteString("data", rowData),
						zap.Error(err),
					)
					return merr.WrapErrIoFailedReason(err.Error())
				}
				if _, ok := jsonData[common.MetaFieldName]; ok {
					return fmt.Errorf("cannot set json key to: %s", common.MetaFieldName)
				}
				for _, f := range schema.GetFields() {
					if _, ok := jsonData[f.GetName()]; ok {
						log.Info("dynamic field name include the static field name", zap.String("fieldName", f.GetName()))
						return fmt.Errorf("dynamic field name cannot include the static field name: %s", f.GetName())
					}
				}
			}
		}
	}

	return nil
}

func checkDynamicFieldData(schema *schemapb.CollectionSchema, insertMsg *msgstream.InsertMsg) error {
	for _, data := range insertMsg.FieldsData {
		if data.IsDynamic {
			data.FieldName = common.MetaFieldName
			return verifyDynamicFieldData(schema, insertMsg)
		}
	}
	defaultData := make([][]byte, insertMsg.NRows())
	for i := range defaultData {
		defaultData[i] = []byte("{}")
	}
	dynamicData := autoGenDynamicFieldData(defaultData)
	insertMsg.FieldsData = append(insertMsg.FieldsData, dynamicData)
	return nil
}

func SendReplicateMessagePack(ctx context.Context, replicateMsgStream msgstream.MsgStream, request interface{ GetBase() *commonpb.MsgBase }) {
	if replicateMsgStream == nil || request == nil {
		log.Ctx(ctx).Warn("replicate msg stream or request is nil", zap.Any("request", request))
		return
	}
	msgBase := request.GetBase()
	ts := msgBase.GetTimestamp()
	if msgBase.GetReplicateInfo().GetIsReplicate() {
		ts = msgBase.GetReplicateInfo().GetMsgTimestamp()
	}
	getBaseMsg := func(ctx context.Context, ts uint64) msgstream.BaseMsg {
		return msgstream.BaseMsg{
			Ctx:            ctx,
			HashValues:     []uint32{0},
			BeginTimestamp: ts,
			EndTimestamp:   ts,
		}
	}

	var tsMsg msgstream.TsMsg
	switch r := request.(type) {
	case *milvuspb.CreateDatabaseRequest:
		tsMsg = &msgstream.CreateDatabaseMsg{
			BaseMsg:               getBaseMsg(ctx, ts),
			CreateDatabaseRequest: r,
		}
	case *milvuspb.DropDatabaseRequest:
		tsMsg = &msgstream.DropDatabaseMsg{
			BaseMsg:             getBaseMsg(ctx, ts),
			DropDatabaseRequest: r,
		}
	case *milvuspb.AlterDatabaseRequest:
		tsMsg = &msgstream.AlterDatabaseMsg{
			BaseMsg:              getBaseMsg(ctx, ts),
			AlterDatabaseRequest: r,
		}
	case *milvuspb.FlushRequest:
		tsMsg = &msgstream.FlushMsg{
			BaseMsg:      getBaseMsg(ctx, ts),
			FlushRequest: r,
		}
	case *milvuspb.LoadCollectionRequest:
		tsMsg = &msgstream.LoadCollectionMsg{
			BaseMsg:               getBaseMsg(ctx, ts),
			LoadCollectionRequest: r,
		}
	case *milvuspb.ReleaseCollectionRequest:
		tsMsg = &msgstream.ReleaseCollectionMsg{
			BaseMsg:                  getBaseMsg(ctx, ts),
			ReleaseCollectionRequest: r,
		}
	case *milvuspb.CreateIndexRequest:
		tsMsg = &msgstream.CreateIndexMsg{
			BaseMsg:            getBaseMsg(ctx, ts),
			CreateIndexRequest: r,
		}
	case *milvuspb.DropIndexRequest:
		tsMsg = &msgstream.DropIndexMsg{
			BaseMsg:          getBaseMsg(ctx, ts),
			DropIndexRequest: r,
		}
	case *milvuspb.LoadPartitionsRequest:
		tsMsg = &msgstream.LoadPartitionsMsg{
			BaseMsg:               getBaseMsg(ctx, ts),
			LoadPartitionsRequest: r,
		}
	case *milvuspb.ReleasePartitionsRequest:
		tsMsg = &msgstream.ReleasePartitionsMsg{
			BaseMsg:                  getBaseMsg(ctx, ts),
			ReleasePartitionsRequest: r,
		}
	case *milvuspb.AlterIndexRequest:
		tsMsg = &msgstream.AlterIndexMsg{
			BaseMsg:           getBaseMsg(ctx, ts),
			AlterIndexRequest: r,
		}
	case *milvuspb.CreateCredentialRequest:
		tsMsg = &msgstream.CreateUserMsg{
			BaseMsg:                 getBaseMsg(ctx, ts),
			CreateCredentialRequest: r,
		}
	case *milvuspb.UpdateCredentialRequest:
		tsMsg = &msgstream.UpdateUserMsg{
			BaseMsg:                 getBaseMsg(ctx, ts),
			UpdateCredentialRequest: r,
		}
	case *milvuspb.DeleteCredentialRequest:
		tsMsg = &msgstream.DeleteUserMsg{
			BaseMsg:                 getBaseMsg(ctx, ts),
			DeleteCredentialRequest: r,
		}
	case *milvuspb.CreateRoleRequest:
		tsMsg = &msgstream.CreateRoleMsg{
			BaseMsg:           getBaseMsg(ctx, ts),
			CreateRoleRequest: r,
		}
	case *milvuspb.DropRoleRequest:
		tsMsg = &msgstream.DropRoleMsg{
			BaseMsg:         getBaseMsg(ctx, ts),
			DropRoleRequest: r,
		}
	case *milvuspb.OperateUserRoleRequest:
		tsMsg = &msgstream.OperateUserRoleMsg{
			BaseMsg:                getBaseMsg(ctx, ts),
			OperateUserRoleRequest: r,
		}
	case *milvuspb.OperatePrivilegeRequest:
		tsMsg = &msgstream.OperatePrivilegeMsg{
			BaseMsg:                 getBaseMsg(ctx, ts),
			OperatePrivilegeRequest: r,
		}
	default:
		log.Warn("unknown request", zap.Any("request", request))
		return
	}
	msgPack := &msgstream.MsgPack{
		BeginTs: ts,
		EndTs:   ts,
		Msgs:    []msgstream.TsMsg{tsMsg},
	}
	msgErr := replicateMsgStream.Produce(ctx, msgPack)
	// ignore the error if the msg stream failed to produce the msg,
	// because it can be manually fixed in this error
	if msgErr != nil {
		log.Warn("send replicate msg failed", zap.Any("pack", msgPack), zap.Error(msgErr))
	}
}

func GetCachedCollectionSchema(ctx context.Context, dbName string, colName string) (*schemaInfo, error) {
	if globalMetaCache != nil {
		return globalMetaCache.GetCollectionSchema(ctx, dbName, colName)
	}
	return nil, merr.WrapErrServiceNotReady(paramtable.GetRole(), paramtable.GetNodeID(), "initialization")
}

func CheckDatabase(ctx context.Context, dbName string) bool {
	if globalMetaCache != nil {
		return globalMetaCache.HasDatabase(ctx, dbName)
	}
	return false
}

func SetReportValue(status *commonpb.Status, value int) {
	if value <= 0 {
		return
	}
	if !merr.Ok(status) {
		return
	}
	if status.ExtraInfo == nil {
		status.ExtraInfo = make(map[string]string)
	}
	status.ExtraInfo["report_value"] = strconv.Itoa(value)
}

func GetCostValue(status *commonpb.Status) int {
	if status == nil || status.ExtraInfo == nil {
		return 0
	}
	value, err := strconv.Atoi(status.ExtraInfo["report_value"])
	if err != nil {
		return 0
	}
	return value
}

// GetRequestInfo returns collection name and rateType of request and return tokens needed.
func GetRequestInfo(ctx context.Context, req proto.Message) (int64, map[int64][]int64, internalpb.RateType, int, error) {
	switch r := req.(type) {
	case *milvuspb.InsertRequest:
		dbID, collToPartIDs, err := getCollectionAndPartitionID(ctx, req.(reqPartName))
		return dbID, collToPartIDs, internalpb.RateType_DMLInsert, proto.Size(r), err
	case *milvuspb.UpsertRequest:
		dbID, collToPartIDs, err := getCollectionAndPartitionID(ctx, req.(reqPartName))
		return dbID, collToPartIDs, internalpb.RateType_DMLInsert, proto.Size(r), err
	case *milvuspb.DeleteRequest:
		dbID, collToPartIDs, err := getCollectionAndPartitionID(ctx, req.(reqPartName))
		return dbID, collToPartIDs, internalpb.RateType_DMLDelete, proto.Size(r), err
	case *milvuspb.ImportRequest:
		dbID, collToPartIDs, err := getCollectionAndPartitionID(ctx, req.(reqPartName))
		return dbID, collToPartIDs, internalpb.RateType_DMLBulkLoad, proto.Size(r), err
	case *milvuspb.SearchRequest:
		dbID, collToPartIDs, err := getCollectionAndPartitionIDs(ctx, req.(reqPartNames))
		return dbID, collToPartIDs, internalpb.RateType_DQLSearch, int(r.GetNq()), err
	case *milvuspb.QueryRequest:
		dbID, collToPartIDs, err := getCollectionAndPartitionIDs(ctx, req.(reqPartNames))
		return dbID, collToPartIDs, internalpb.RateType_DQLQuery, 1, err // think of the query request's nq as 1
	case *milvuspb.CreateCollectionRequest:
		dbID, collToPartIDs := getCollectionID(req.(reqCollName))
		return dbID, collToPartIDs, internalpb.RateType_DDLCollection, 1, nil
	case *milvuspb.DropCollectionRequest:
		dbID, collToPartIDs := getCollectionID(req.(reqCollName))
		return dbID, collToPartIDs, internalpb.RateType_DDLCollection, 1, nil
	case *milvuspb.LoadCollectionRequest:
		dbID, collToPartIDs := getCollectionID(req.(reqCollName))
		return dbID, collToPartIDs, internalpb.RateType_DDLCollection, 1, nil
	case *milvuspb.ReleaseCollectionRequest:
		dbID, collToPartIDs := getCollectionID(req.(reqCollName))
		return dbID, collToPartIDs, internalpb.RateType_DDLCollection, 1, nil
	case *milvuspb.CreatePartitionRequest:
		dbID, collToPartIDs := getCollectionID(req.(reqCollName))
		return dbID, collToPartIDs, internalpb.RateType_DDLPartition, 1, nil
	case *milvuspb.DropPartitionRequest:
		dbID, collToPartIDs := getCollectionID(req.(reqCollName))
		return dbID, collToPartIDs, internalpb.RateType_DDLPartition, 1, nil
	case *milvuspb.LoadPartitionsRequest:
		dbID, collToPartIDs := getCollectionID(req.(reqCollName))
		return dbID, collToPartIDs, internalpb.RateType_DDLPartition, 1, nil
	case *milvuspb.ReleasePartitionsRequest:
		dbID, collToPartIDs := getCollectionID(req.(reqCollName))
		return dbID, collToPartIDs, internalpb.RateType_DDLPartition, 1, nil
	case *milvuspb.CreateIndexRequest:
		dbID, collToPartIDs := getCollectionID(req.(reqCollName))
		return dbID, collToPartIDs, internalpb.RateType_DDLIndex, 1, nil
	case *milvuspb.DropIndexRequest:
		dbID, collToPartIDs := getCollectionID(req.(reqCollName))
		return dbID, collToPartIDs, internalpb.RateType_DDLIndex, 1, nil
	case *milvuspb.FlushRequest:
		db, err := globalMetaCache.GetDatabaseInfo(ctx, r.GetDbName())
		if err != nil {
			return util.InvalidDBID, map[int64][]int64{}, 0, 0, err
		}

		collToPartIDs := make(map[int64][]int64, 0)
		for _, collectionName := range r.GetCollectionNames() {
			collectionID, err := globalMetaCache.GetCollectionID(ctx, r.GetDbName(), collectionName)
			if err != nil {
				return util.InvalidDBID, map[int64][]int64{}, 0, 0, err
			}
			collToPartIDs[collectionID] = []int64{}
		}
		return db.dbID, collToPartIDs, internalpb.RateType_DDLFlush, 1, nil
	case *milvuspb.ManualCompactionRequest:
		dbName := GetCurDBNameFromContextOrDefault(ctx)
		dbInfo, err := globalMetaCache.GetDatabaseInfo(ctx, dbName)
		if err != nil {
			return util.InvalidDBID, map[int64][]int64{}, 0, 0, err
		}
		return dbInfo.dbID, map[int64][]int64{
			r.GetCollectionID(): {},
		}, internalpb.RateType_DDLCompaction, 1, nil
	case *milvuspb.CreateDatabaseRequest:
		log.Info("rate limiter CreateDatabaseRequest")
		return util.InvalidDBID, map[int64][]int64{}, internalpb.RateType_DDLDB, 1, nil
	case *milvuspb.DropDatabaseRequest:
		log.Info("rate limiter DropDatabaseRequest")
		return util.InvalidDBID, map[int64][]int64{}, internalpb.RateType_DDLDB, 1, nil
	case *milvuspb.AlterDatabaseRequest:
		return util.InvalidDBID, map[int64][]int64{}, internalpb.RateType_DDLDB, 1, nil
	default: // TODO: support more request
		if req == nil {
			return util.InvalidDBID, map[int64][]int64{}, 0, 0, errors.New("null request")
		}
		log.RatedWarn(60, "not supported request type for rate limiter", zap.String("type", reflect.TypeOf(req).String()))
		return util.InvalidDBID, map[int64][]int64{}, 0, 0, nil
	}
}

// GetFailedResponse returns failed response.
func GetFailedResponse(req any, err error) any {
	switch req.(type) {
	case *milvuspb.InsertRequest, *milvuspb.DeleteRequest, *milvuspb.UpsertRequest:
		return failedMutationResult(err)
	case *milvuspb.ImportRequest:
		return &milvuspb.ImportResponse{
			Status: merr.Status(err),
		}
	case *milvuspb.SearchRequest:
		return &milvuspb.SearchResults{
			Status: merr.Status(err),
		}
	case *milvuspb.QueryRequest:
		return &milvuspb.QueryResults{
			Status: merr.Status(err),
		}
	case *milvuspb.CreateCollectionRequest, *milvuspb.DropCollectionRequest,
		*milvuspb.LoadCollectionRequest, *milvuspb.ReleaseCollectionRequest,
		*milvuspb.CreatePartitionRequest, *milvuspb.DropPartitionRequest,
		*milvuspb.LoadPartitionsRequest, *milvuspb.ReleasePartitionsRequest,
		*milvuspb.CreateIndexRequest, *milvuspb.DropIndexRequest,
		*milvuspb.CreateDatabaseRequest, *milvuspb.DropDatabaseRequest,
		*milvuspb.AlterDatabaseRequest:
		return merr.Status(err)
	case *milvuspb.FlushRequest:
		return &milvuspb.FlushResponse{
			Status: merr.Status(err),
		}
	case *milvuspb.ManualCompactionRequest:
		return &milvuspb.ManualCompactionResponse{
			Status: merr.Status(err),
		}
	}
	return nil
}

func GetReplicateID(ctx context.Context, database, collectionName string) (string, error) {
	if globalMetaCache == nil {
		return "", merr.WrapErrServiceUnavailable("internal: Milvus Proxy is not ready yet. please wait")
	}
	colInfo, err := globalMetaCache.GetCollectionInfo(ctx, database, collectionName, 0)
	if err != nil {
		return "", err
	}
	if colInfo.replicateID != "" {
		return colInfo.replicateID, nil
	}
	dbInfo, err := globalMetaCache.GetDatabaseInfo(ctx, database)
	if err != nil {
		return "", err
	}
	replicateID, _ := common.GetReplicateID(dbInfo.properties)
	return replicateID, nil
}
