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
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/proto/planpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/types"
	typeutil2 "github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/crypto"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

const (
	strongTS  = 0
	boundedTS = 2

	// enableMultipleVectorFields indicates whether to enable multiple vector fields.
	enableMultipleVectorFields = false

	defaultMaxVarCharLength = 65535

	// DefaultIndexType name of default index type for scalar field
	DefaultIndexType = "STL_SORT"

	// DefaultStringIndexType name of default index type for varChar/string field
	DefaultStringIndexType = "Trie"
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

func validateTopKLimit(limit int64) error {
	topKLimit := Params.QuotaConfig.TopKLimit.GetAsInt64()
	if limit <= 0 || limit > topKLimit {
		return fmt.Errorf("should be in range [1, %d], but got %d", topKLimit, limit)
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
		return fmt.Errorf("collection %s should not be empty", entityType)
	}

	invalidMsg := fmt.Sprintf("Invalid collection %s: %s. ", entityType, entity)
	if len(entity) > Params.ProxyCfg.MaxNameLength.GetAsInt() {
		return fmt.Errorf("%s the length of a collection %s must be less than %s characters", invalidMsg, entityType,
			Params.ProxyCfg.MaxNameLength.GetValue())
	}

	firstChar := entity[0]
	if firstChar != '_' && !isAlpha(firstChar) {
		return fmt.Errorf("%s the first character of a collection %s must be an underscore or letter", invalidMsg, entityType)
	}

	for i := 1; i < len(entity); i++ {
		c := entity[i]
		if c != '_' && !isAlpha(c) && !isNumber(c) {
			return fmt.Errorf("%s collection %s can only contain numbers, letters and underscores", invalidMsg, entityType)
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
		return fmt.Errorf("%s the length of a resource group name must be less than %s characters",
			invalidMsg, Params.ProxyCfg.MaxNameLength.GetValue())
	}

	firstChar := entity[0]
	if firstChar != '_' && !isAlpha(firstChar) {
		return fmt.Errorf("%s the first character of a resource group name must be an underscore or letter", invalidMsg)
	}

	for i := 1; i < len(entity); i++ {
		c := entity[i]
		if c != '_' && !isAlpha(c) && !isNumber(c) {
			return fmt.Errorf("%s resource group name can only contain numbers, letters and underscores", invalidMsg)
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
			if c != '_' && !isAlpha(c) && !isNumber(c) {
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
		return errors.New("field name should not be empty")
	}

	invalidMsg := "Invalid field name: " + fieldName + ". "
	if len(fieldName) > Params.ProxyCfg.MaxNameLength.GetAsInt() {
		msg := invalidMsg + "The length of a field name must be less than " + Params.ProxyCfg.MaxNameLength.GetValue() + " characters."
		return errors.New(msg)
	}

	firstChar := fieldName[0]
	if firstChar != '_' && !isAlpha(firstChar) {
		msg := invalidMsg + "The first character of a field name must be an underscore or letter."
		return errors.New(msg)
	}

	fieldNameSize := len(fieldName)
	for i := 1; i < fieldNameSize; i++ {
		c := fieldName[i]
		if c != '_' && !isAlpha(c) && !isNumber(c) {
			msg := invalidMsg + "Field name cannot only contain numbers, letters, and underscores."
			return errors.New(msg)
		}
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
	if !exist {
		return errors.New("dimension is not defined in field type params, check type param `dim` for vector field")
	}

	if dim <= 0 || dim > Params.ProxyCfg.MaxDimension.GetAsInt64() {
		return fmt.Errorf("invalid dimension: %d. should be in range 1 ~ %d", dim, Params.ProxyCfg.MaxDimension.GetAsInt())
	}
	if field.DataType == schemapb.DataType_BinaryVector && dim%8 != 0 {
		return fmt.Errorf("invalid dimension: %d. should be multiple of 8. ", dim)
	}
	return nil
}

func validateMaxLengthPerRow(collectionName string, field *schemapb.FieldSchema) error {
	exist := false
	for _, param := range field.TypeParams {
		if param.Key != common.MaxLengthKey {
			return fmt.Errorf("type param key(max_length) should be specified for varChar field, not %s", param.Key)
		}

		maxLengthPerRow, err := strconv.ParseInt(param.Value, 10, 64)
		if err != nil {
			return err
		}
		if maxLengthPerRow > defaultMaxVarCharLength || maxLengthPerRow <= 0 {
			return fmt.Errorf("the maximum length specified for a VarChar shoule be in (0, 65535]")
		}
		exist = true
	}
	// if not exist type params max_length, return error
	if !exist {
		return fmt.Errorf("type param(max_length) should be specified for varChar field of collection %s", collectionName)
	}

	return nil
}

func validateVectorFieldMetricType(field *schemapb.FieldSchema) error {
	if (field.DataType != schemapb.DataType_FloatVector) && (field.DataType != schemapb.DataType_BinaryVector) {
		return nil
	}
	for _, params := range field.IndexParams {
		if params.Key == common.MetricTypeKey {
			return nil
		}
	}
	return errors.New("vector float without metric_type")
}

func validateDuplicatedFieldName(fields []*schemapb.FieldSchema) error {
	names := make(map[string]bool)
	for _, field := range fields {
		_, ok := names[field.Name]
		if ok {
			return errors.New("duplicated field name")
		}
		names[field.Name] = true
	}
	return nil
}

func validateFieldType(schema *schemapb.CollectionSchema) error {
	for _, field := range schema.GetFields() {
		switch field.GetDataType() {
		case schemapb.DataType_String:
			return errors.New("string data type not supported yet, please use VarChar type instead")
		case schemapb.DataType_None:
			return errors.New("data type None is not valid")
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
			//		return fmt.Errorf("autoID is not supported when the VarChar field is the primary key")
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
			return fmt.Errorf("cannot explicitly set a field as a dynamic field")
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

	case schemapb.DataType_FloatVector, schemapb.DataType_BinaryVector:
		return true, nil
	}

	return false, fmt.Errorf("invalid data type: %d", dataType)
}

func validateMetricType(dataType schemapb.DataType, metricTypeStrRaw string) error {
	metricTypeStr := strings.ToUpper(metricTypeStrRaw)
	switch metricTypeStr {
	case "L2", "IP":
		if dataType == schemapb.DataType_FloatVector {
			return nil
		}
	case "JACCARD", "HAMMING", "TANIMOTO", "SUBSTRUCTURE", "SUBPERSTURCTURE":
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
				return fmt.Errorf("autoId forbids primary key")
			} else if primaryIdx != -1 {
				return fmt.Errorf("there are more than one primary key, field name = %s, %s", coll.Fields[primaryIdx].Name, field.Name)
			}
			if field.DataType != schemapb.DataType_Int64 {
				return fmt.Errorf("type of primary key shoule be int64")
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
			dimStr, ok := typeKv[common.DimKey]
			if !ok {
				return fmt.Errorf("dim not found in type_params for vector field %s(%d)", field.Name, field.FieldID)
			}
			dim, err := strconv.Atoi(dimStr)
			if err != nil || dim < 0 {
				return fmt.Errorf("invalid dim; %s", dimStr)
			}

			metricTypeStr, ok := indexKv[common.MetricTypeKey]
			if ok {
				err4 := validateMetricType(field.DataType, metricTypeStr)
				if err4 != nil {
					return err4
				}
			} else {
				// in C++, default type will be specified
				// do nothing
			}
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
		return fmt.Errorf("primary key is required for non autoid mode")
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
		isVec := dType == schemapb.DataType_BinaryVector || dType == schemapb.DataType_FloatVector
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
			return nil, errors.New("currently only support DataType Int64 or VarChar as PrimaryField")
		}
	default:
		return nil, errors.New("currently not support vector field as PrimaryField")
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
			strIds := make([]string, len(data))
			for i, v := range data {
				strIds[i] = strconv.FormatInt(v, 10)
			}
			fieldData.Field = &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: strIds,
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

// fillFieldIDBySchema set fieldID to fieldData according FieldSchemas
func fillFieldIDBySchema(columns []*schemapb.FieldData, schema *schemapb.CollectionSchema) error {
	if len(columns) != len(schema.GetFields()) {
		return fmt.Errorf("len(columns) mismatch the len(fields), len(columns): %d, len(fields): %d",
			len(columns), len(schema.GetFields()))
	}
	fieldName2Schema := make(map[string]*schemapb.FieldSchema)
	for _, field := range schema.GetFields() {
		fieldName2Schema[field.Name] = field
	}

	for _, fieldData := range columns {
		if fieldSchema, ok := fieldName2Schema[fieldData.FieldName]; ok {
			fieldData.FieldId = fieldSchema.FieldID
			fieldData.Type = fieldSchema.DataType
		} else {
			return fmt.Errorf("fieldName %v not exist in collection schema", fieldData.FieldName)
		}
	}

	return nil
}

func ValidateUsername(username string) error {
	username = strings.TrimSpace(username)

	if username == "" {
		return errors.New("username should not be empty")
	}

	invalidMsg := "Invalid username: " + username + ". "
	if len(username) > Params.ProxyCfg.MaxUsernameLength.GetAsInt() {
		msg := invalidMsg + "The length of username must be less than " + Params.ProxyCfg.MaxUsernameLength.GetValue() + " characters."
		return errors.New(msg)
	}

	firstChar := username[0]
	if !isAlpha(firstChar) {
		msg := invalidMsg + "The first character of username must be a letter."
		return errors.New(msg)
	}

	usernameSize := len(username)
	for i := 1; i < usernameSize; i++ {
		c := username[i]
		if c != '_' && !isAlpha(c) && !isNumber(c) {
			msg := invalidMsg + "Username should only contain numbers, letters, and underscores."
			return errors.New(msg)
		}
	}
	return nil
}

func ValidatePassword(password string) error {
	if len(password) < Params.ProxyCfg.MinPasswordLength.GetAsInt() || len(password) > Params.ProxyCfg.MaxPasswordLength.GetAsInt() {
		msg := "The length of password must be great than " + Params.ProxyCfg.MinPasswordLength.GetValue() +
			" and less than " + Params.ProxyCfg.MaxPasswordLength.GetValue() + " characters."
		return errors.New(msg)
	}
	return nil
}

func validateTravelTimestamp(travelTs, tMax typeutil.Timestamp) error {
	durationSeconds := tsoutil.CalculateDuration(tMax, travelTs) / 1000
	if durationSeconds > Params.CommonCfg.RetentionDuration.GetAsInt64() {

		durationIn := time.Second * time.Duration(durationSeconds)
		durationSupport := time.Second * time.Duration(Params.CommonCfg.RetentionDuration.GetAsInt64())
		return fmt.Errorf("only support to travel back to %v so far, but got %v", durationSupport, durationIn)
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

func validateName(entity string, nameType string) error {
	entity = strings.TrimSpace(entity)

	if entity == "" {
		return fmt.Errorf("%s should not be empty", nameType)
	}

	invalidMsg := fmt.Sprintf("invalid %s: %s. ", nameType, entity)
	if len(entity) > Params.ProxyCfg.MaxNameLength.GetAsInt() {
		msg := invalidMsg + fmt.Sprintf("the length of %s must be less than ", nameType) + Params.ProxyCfg.MaxNameLength.GetValue() + " characters."
		return errors.New(msg)
	}

	firstChar := entity[0]
	if firstChar != '_' && !isAlpha(firstChar) {
		msg := invalidMsg + fmt.Sprintf("the first character of %s must be an underscore or letter.", nameType)
		return errors.New(msg)
	}

	for i := 1; i < len(entity); i++ {
		c := entity[i]
		if c != '_' && c != '$' && !isAlpha(c) && !isNumber(c) {
			msg := invalidMsg + fmt.Sprintf("%s can only contain numbers, letters, dollars and underscores.", nameType)
			return errors.New(msg)
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
	return validateName(entity, "role name")
}

func ValidateObjectType(entity string) error {
	return validateName(entity, "ObjectType")
}

func ValidatePrincipalName(entity string) error {
	return validateName(entity, "PrincipalName")
}

func ValidatePrincipalType(entity string) error {
	return validateName(entity, "PrincipalType")
}

func ValidatePrivilege(entity string) error {
	if util.IsAnyWord(entity) {
		return nil
	}
	return validateName(entity, "Privilege")
}

func GetCurUserFromContext(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", fmt.Errorf("fail to get md from the context")
	}
	authorization := md[strings.ToLower(util.HeaderAuthorize)]
	if len(authorization) < 1 {
		return "", fmt.Errorf("fail to get authorization from the md, authorize:[%s]", util.HeaderAuthorize)
	}
	token := authorization[0]
	rawToken, err := crypto.Base64Decode(token)
	if err != nil {
		return "", fmt.Errorf("fail to decode the token, token: %s", token)
	}
	secrets := strings.SplitN(rawToken, util.CredentialSeperator, 2)
	if len(secrets) < 2 {
		return "", fmt.Errorf("fail to get user info from the raw token, raw token: %s", rawToken)
	}
	username := secrets[0]
	return username, nil
}

func GetRole(username string) ([]string, error) {
	if globalMetaCache == nil {
		return []string{}, merr.WrapErrServiceUnavailable("internal: Milvus Proxy is not ready yet. please wait")
	}
	return globalMetaCache.GetUserRole(username), nil
}

// PasswordVerify verify password
func passwordVerify(ctx context.Context, username, rawPwd string, globalMetaCache Cache) bool {
	// it represents the cache miss if Sha256Password is empty within credInfo, which shall be updated first connection.
	// meanwhile, generating Sha256Password depends on raw password and encrypted password will not cache.
	credInfo, err := globalMetaCache.GetCredentialInfo(ctx, username)
	if err != nil {
		log.Error("found no credential", zap.String("username", username), zap.Error(err))
		return false
	}

	// hit cache
	sha256Pwd := crypto.SHA256(rawPwd, credInfo.Username)
	if credInfo.Sha256Password != "" {
		return sha256Pwd == credInfo.Sha256Password
	}

	// miss cache, verify against encrypted password from etcd
	if err := bcrypt.CompareHashAndPassword([]byte(credInfo.EncryptedPassword), []byte(rawPwd)); err != nil {
		log.Error("Verify password failed", zap.Error(err))
		return false
	}

	// update cache after miss cache
	credInfo.Sha256Password = sha256Pwd
	log.Debug("get credential miss cache, update cache with", zap.Any("credential", credInfo))
	globalMetaCache.UpdateCredential(credInfo)
	return true
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
func translateOutputFields(outputFields []string, schema *schemapb.CollectionSchema, addPrimary bool) ([]string, []string, error) {
	var primaryFieldName string
	allFieldNameMap := make(map[string]bool)
	resultFieldNameMap := make(map[string]bool)
	resultFieldNames := make([]string, 0)
	userOutputFieldsMap := make(map[string]bool)
	userOutputFields := make([]string, 0)

	for _, field := range schema.Fields {
		if field.IsPrimaryKey {
			primaryFieldName = field.Name
		}
		allFieldNameMap[field.Name] = true
	}

	for _, outputFieldName := range outputFields {
		outputFieldName = strings.TrimSpace(outputFieldName)
		if outputFieldName == "*" {
			for fieldName := range allFieldNameMap {
				resultFieldNameMap[fieldName] = true
				userOutputFieldsMap[fieldName] = true
			}
		} else {
			if _, ok := allFieldNameMap[outputFieldName]; ok {
				resultFieldNameMap[outputFieldName] = true
				userOutputFieldsMap[outputFieldName] = true
			} else {
				if schema.EnableDynamicField {
					schemaH, err := typeutil.CreateSchemaHelper(schema)
					if err != nil {
						return nil, nil, err
					}
					err = planparserv2.ParseIdentifier(schemaH, outputFieldName, func(expr *planpb.Expr) error {
						if len(expr.GetColumnExpr().GetInfo().GetNestedPath()) == 1 &&
							expr.GetColumnExpr().GetInfo().GetNestedPath()[0] == outputFieldName {
							return nil
						}
						return fmt.Errorf("not suppot getting subkeys of json field yet")
					})
					if err != nil {
						log.Info("parse output field name failed", zap.String("field name", outputFieldName))
						return nil, nil, fmt.Errorf("parse output field name failed: %s", outputFieldName)
					}
					resultFieldNameMap[common.MetaFieldName] = true
					userOutputFieldsMap[outputFieldName] = true
				} else {
					return nil, nil, fmt.Errorf("field %s not exist", outputFieldName)
				}
			}
		}
	}

	if addPrimary {
		resultFieldNameMap[primaryFieldName] = true
		userOutputFieldsMap[primaryFieldName] = true
	}

	for fieldName := range resultFieldNameMap {
		resultFieldNames = append(resultFieldNames, fieldName)
	}
	for fieldName := range userOutputFieldsMap {
		userOutputFields = append(userOutputFields, fieldName)
	}
	return resultFieldNames, userOutputFields, nil
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
			msg := invalidMsg + "Index name cannot only contain numbers, letters, and underscores."
			return errors.New(msg)
		}
	}
	return nil
}

func isCollectionLoaded(ctx context.Context, qc types.QueryCoord, collID int64) (bool, error) {
	// get all loading collections
	resp, err := qc.ShowCollections(ctx, &querypb.ShowCollectionsRequest{
		CollectionIDs: nil,
	})
	if err != nil {
		return false, err
	}
	if resp.Status.ErrorCode != commonpb.ErrorCode_Success {
		return false, errors.New(resp.Status.Reason)
	}

	for _, loadedCollID := range resp.GetCollectionIDs() {
		if collID == loadedCollID {
			return true, nil
		}
	}
	return false, nil
}

func isPartitionLoaded(ctx context.Context, qc types.QueryCoord, collID int64, partIDs []int64) (bool, error) {
	// get all loading collections
	resp, err := qc.ShowPartitions(ctx, &querypb.ShowPartitionsRequest{
		CollectionID: collID,
		PartitionIDs: nil,
	})
	if err != nil {
		return false, err
	}
	if resp.Status.ErrorCode != commonpb.ErrorCode_Success {
		return false, errors.New(resp.Status.Reason)
	}

	for _, loadedPartID := range resp.GetPartitionIDs() {
		for _, partID := range partIDs {
			if partID == loadedPartID {
				return true, nil
			}
		}
	}
	return false, nil
}

func fillFieldsDataBySchema(schema *schemapb.CollectionSchema, insertMsg *msgstream.InsertMsg) error {
	neededFieldsNum := 0
	isPrimaryKeyNum := 0

	dataNameSet := typeutil.NewSet[string]()
	for _, data := range insertMsg.FieldsData {
		fieldName := data.GetFieldName()
		if dataNameSet.Contain(fieldName) {
			return merr.WrapErrParameterDuplicateFieldData(fieldName, "The FieldDatas parameter being passed contains duplicate data for a field.")
		}
		dataNameSet.Insert(fieldName)
	}

	for _, fieldSchema := range schema.Fields {
		if fieldSchema.AutoID && !fieldSchema.IsPrimaryKey {
			log.Error("not primary key field, but set autoID true", zap.String("fieldSchemaName", fieldSchema.GetName()))
			return merr.WrapErrParameterInvalid("only primary key field can set autoID true", "")
		}
		if fieldSchema.GetDefaultValue() != nil && fieldSchema.IsPrimaryKey {
			return merr.WrapErrParameterInvalid("no default data", "", "pk field schema can not set default value")
		}
		if !fieldSchema.AutoID {
			neededFieldsNum++
		}
		// if has no field pass in, consider use default value
		// so complete it with field schema
		if _, ok := dataNameSet[fieldSchema.GetName()]; !ok {
			// primary key can not use default value
			if fieldSchema.IsPrimaryKey {
				isPrimaryKeyNum++
				continue
			}
			dataToAppend := &schemapb.FieldData{
				Type:      fieldSchema.GetDataType(),
				FieldName: fieldSchema.GetName(),
			}
			insertMsg.FieldsData = append(insertMsg.FieldsData, dataToAppend)
		}
	}

	if isPrimaryKeyNum > 1 {
		log.Error("the number of passed primary key fields is more than 1",
			zap.Int64("primaryKeyNum", int64(isPrimaryKeyNum)),
			zap.String("CollectionSchemaName", schema.GetName()))
		return merr.WrapErrParameterInvalid("0 or 1", fmt.Sprint(isPrimaryKeyNum), "the number of passed primary key fields is more than 1")
	}

	if len(insertMsg.FieldsData) != neededFieldsNum {
		log.Error("the length of passed fields is not equal to needed",
			zap.Int("expectFieldNumber", neededFieldsNum),
			zap.Int("passFieldNumber", len(insertMsg.FieldsData)),
			zap.String("CollectionSchemaName", schema.GetName()))
		return merr.WrapErrParameterInvalid(neededFieldsNum, len(insertMsg.FieldsData), "the length of passed fields is equal to needed")
	}

	return nil
}

func checkPrimaryFieldData(schema *schemapb.CollectionSchema, result *milvuspb.MutationResult, insertMsg *msgstream.InsertMsg, inInsert bool) (*schemapb.IDs, error) {
	rowNums := uint32(insertMsg.NRows())
	// TODO(dragondriver): in fact, NumRows is not trustable, we should check all input fields
	if insertMsg.NRows() <= 0 {
		return nil, merr.WrapErrParameterInvalid("invalid num_rows", fmt.Sprint(rowNums), "num_rows should be greater than 0")
	}

	if err := fillFieldsDataBySchema(schema, insertMsg); err != nil {
		return nil, err
	}

	primaryFieldSchema, err := typeutil.GetPrimaryFieldSchema(schema)
	if err != nil {
		log.Error("get primary field schema failed", zap.String("collectionName", insertMsg.CollectionName), zap.Any("schema", schema), zap.Error(err))
		return nil, err
	}
	// get primaryFieldData whether autoID is true or not
	var primaryFieldData *schemapb.FieldData
	if inInsert {
		// when checkPrimaryFieldData in insert
		if !primaryFieldSchema.AutoID {
			primaryFieldData, err = typeutil.GetPrimaryFieldData(insertMsg.GetFieldsData(), primaryFieldSchema)
			if err != nil {
				log.Info("get primary field data failed", zap.String("collectionName", insertMsg.CollectionName), zap.Error(err))
				return nil, err
			}
		} else {
			// check primary key data not exist
			if typeutil.IsPrimaryFieldDataExist(insertMsg.GetFieldsData(), primaryFieldSchema) {
				return nil, fmt.Errorf("can not assign primary field data when auto id enabled %v", primaryFieldSchema.Name)
			}
			// if autoID == true, currently support autoID for int64 and varchar PrimaryField
			primaryFieldData, err = autoGenPrimaryFieldData(primaryFieldSchema, insertMsg.GetRowIDs())
			if err != nil {
				log.Info("generate primary field data failed when autoID == true", zap.String("collectionName", insertMsg.CollectionName), zap.Error(err))
				return nil, err
			}
			// if autoID == true, set the primary field data
			// insertMsg.fieldsData need append primaryFieldData
			insertMsg.FieldsData = append(insertMsg.FieldsData, primaryFieldData)
		}
	} else {
		// when checkPrimaryFieldData in upsert
		if primaryFieldSchema.AutoID {
			// upsert has not supported when autoID == true
			log.Info("can not upsert when auto id enabled",
				zap.String("primaryFieldSchemaName", primaryFieldSchema.Name))
			result.Status.ErrorCode = commonpb.ErrorCode_UpsertAutoIDTrue
			return nil, fmt.Errorf("upsert can not assign primary field data when auto id enabled %v", primaryFieldSchema.Name)
		}
		primaryFieldData, err = typeutil.GetPrimaryFieldData(insertMsg.GetFieldsData(), primaryFieldSchema)
		if err != nil {
			log.Error("get primary field data failed when upsert", zap.String("collectionName", insertMsg.CollectionName), zap.Error(err))
			return nil, err
		}
	}

	// parse primaryFieldData to result.IDs, and as returned primary keys
	ids, err := parsePrimaryFieldData2IDs(primaryFieldData)
	if err != nil {
		log.Warn("parse primary field data to IDs failed", zap.String("collectionName", insertMsg.CollectionName), zap.Error(err))
		return nil, err
	}

	return ids, nil
}

func getPartitionKeyFieldData(fieldSchema *schemapb.FieldSchema, insertMsg *msgstream.InsertMsg) (*schemapb.FieldData, error) {
	if len(insertMsg.GetPartitionName()) > 0 {
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
	queryCoord types.QueryCoord,
	msgBase *commonpb.MsgBase,
	collectionID int64,
) (loadProgress int64, refreshProgress int64, err error) {
	resp, err := queryCoord.ShowCollections(ctx, &querypb.ShowCollectionsRequest{
		Base: commonpbutil.UpdateMsgBase(
			msgBase,
			commonpbutil.WithMsgType(commonpb.MsgType_DescribeCollection),
		),
		CollectionIDs: []int64{collectionID},
	})
	if err != nil {
		log.Warn("fail to show collections", zap.Int64("collection_id", collectionID), zap.Error(err))
		return
	}

	if resp.Status.ErrorCode == commonpb.ErrorCode_InsufficientMemoryToLoad {
		err = ErrInsufficientMemory
		log.Warn("detected insufficientMemoryError when getCollectionProgress", zap.Int64("collection_id", collectionID), zap.String("reason", resp.GetStatus().GetReason()))
		return
	}

	if resp.Status.ErrorCode != commonpb.ErrorCode_Success {
		err = merr.Error(resp.GetStatus())
		log.Warn("fail to show collections", zap.Int64("collection_id", collectionID),
			zap.String("reason", resp.Status.Reason))
		return
	}

	if len(resp.InMemoryPercentages) == 0 {
		errMsg := "fail to show collections from the querycoord, no data"
		err = errors.New(errMsg)
		log.Warn(errMsg, zap.Int64("collection_id", collectionID))
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
	queryCoord types.QueryCoord,
	msgBase *commonpb.MsgBase,
	partitionNames []string,
	collectionName string,
	collectionID int64,
) (loadProgress int64, refreshProgress int64, err error) {
	IDs2Names := make(map[int64]string)
	partitionIDs := make([]int64, 0)
	for _, partitionName := range partitionNames {
		var partitionID int64
		partitionID, err = globalMetaCache.GetPartitionID(ctx, collectionName, partitionName)
		if err != nil {
			return
		}
		IDs2Names[partitionID] = partitionName
		partitionIDs = append(partitionIDs, partitionID)
	}
	resp, err := queryCoord.ShowPartitions(ctx, &querypb.ShowPartitionsRequest{
		Base: commonpbutil.UpdateMsgBase(
			msgBase,
			commonpbutil.WithMsgType(commonpb.MsgType_ShowPartitions),
		),
		CollectionID: collectionID,
		PartitionIDs: partitionIDs,
	})
	if err != nil {
		log.Warn("fail to show partitions", zap.Int64("collection_id", collectionID),
			zap.String("collection_name", collectionName),
			zap.Strings("partition_names", partitionNames),
			zap.Error(err))
		return
	}
	if resp.GetStatus().GetErrorCode() == commonpb.ErrorCode_InsufficientMemoryToLoad {
		err = ErrInsufficientMemory
		log.Warn("detected insufficientMemoryError when getPartitionProgress",
			zap.Int64("collection_id", collectionID),
			zap.String("collection_name", collectionName),
			zap.Strings("partition_names", partitionNames),
			zap.String("reason", resp.GetStatus().GetReason()),
		)
		return
	}

	if resp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		err = merr.Error(resp.GetStatus())
		log.Warn("fail to show partitions",
			zap.String("collection_name", collectionName),
			zap.Strings("partition_names", partitionNames),
			zap.String("reason", resp.Status.Reason))
		return
	}

	if len(resp.InMemoryPercentages) != len(partitionIDs) {
		errMsg := "fail to show partitions from the querycoord, invalid data num"
		err = errors.New(errMsg)
		log.Warn(errMsg, zap.Int64("collection_id", collectionID),
			zap.String("collection_name", collectionName),
			zap.Strings("partition_names", partitionNames))
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

func isPartitionKeyMode(ctx context.Context, colName string) (bool, error) {
	colSchema, err := globalMetaCache.GetCollectionSchema(ctx, colName)
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

// getDefaultPartitionNames only used in partition key mode
func getDefaultPartitionNames(ctx context.Context, collectionName string) ([]string, error) {
	partitions, err := globalMetaCache.GetPartitions(ctx, collectionName)
	if err != nil {
		return nil, err
	}

	// Make sure the order of the partition names got every time is the same
	partitionNames := make([]string, len(partitions))
	for partitionName := range partitions {
		splits := strings.Split(partitionName, "_")
		if len(splits) < 2 {
			err = fmt.Errorf("bad default partion name in partition ket mode: %s", partitionName)
			return nil, err
		}
		index, err := strconv.ParseInt(splits[len(splits)-1], 10, 64)
		if err != nil {
			return nil, err
		}

		partitionNames[index] = partitionName
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

func assignPartitionKeys(ctx context.Context, collName string, keys []*planpb.GenericValue) ([]string, error) {
	partitionNames, err := getDefaultPartitionNames(ctx, collName)
	if err != nil {
		return nil, err
	}

	schema, err := globalMetaCache.GetCollectionSchema(ctx, collName)
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

func memsetLoop[T any](v T, numRows int) []T {
	ret := make([]T, 0, numRows)
	for i := 0; i < numRows; i++ {
		ret = append(ret, v)
	}

	return ret
}
