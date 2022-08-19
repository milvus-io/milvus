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
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"golang.org/x/crypto/bcrypt"

	"github.com/milvus-io/milvus/internal/log"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/util"
	"github.com/milvus-io/milvus/internal/util/crypto"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

const strongTS = 0
const boundedTS = 2

// enableMultipleVectorFields indicates whether to enable multiple vector fields.
const enableMultipleVectorFields = false

// maximum length of variable-length strings
const maxVarCharLengthKey = "max_length"
const defaultMaxVarCharLength = 65535

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

func validateTopK(topK int64) error {
	// TODO make this configurable
	if topK <= 0 || topK >= 16385 {
		return fmt.Errorf("limit should be in range [1, 16385], but got %d", topK)
	}
	return nil
}

func validateCollectionNameOrAlias(entity, entityType string) error {
	entity = strings.TrimSpace(entity)

	if entity == "" {
		return fmt.Errorf("collection %s should not be empty", entityType)
	}

	invalidMsg := fmt.Sprintf("Invalid collection %s: %s. ", entityType, entity)
	if int64(len(entity)) > Params.ProxyCfg.MaxNameLength {
		msg := invalidMsg + fmt.Sprintf("The length of a collection %s must be less than ", entityType) +
			strconv.FormatInt(Params.ProxyCfg.MaxNameLength, 10) + " characters."
		return errors.New(msg)
	}

	firstChar := entity[0]
	if firstChar != '_' && !isAlpha(firstChar) {
		msg := invalidMsg + fmt.Sprintf("The first character of a collection %s must be an underscore or letter.", entityType)
		return errors.New(msg)
	}

	for i := 1; i < len(entity); i++ {
		c := entity[i]
		if c != '_' && c != '$' && !isAlpha(c) && !isNumber(c) {
			msg := invalidMsg + fmt.Sprintf("Collection %s can only contain numbers, letters, dollars and underscores.", entityType)
			return errors.New(msg)
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

	if int64(len(partitionTag)) > Params.ProxyCfg.MaxNameLength {
		msg := invalidMsg + "The length of a partition tag must be less than " +
			strconv.FormatInt(Params.ProxyCfg.MaxNameLength, 10) + " characters."
		return errors.New(msg)
	}

	if strictCheck {
		firstChar := partitionTag[0]
		if firstChar != '_' && !isAlpha(firstChar) && !isNumber(firstChar) {
			msg := invalidMsg + "The first character of a partition tag must be an underscore or letter."
			return errors.New(msg)
		}

		tagSize := len(partitionTag)
		for i := 1; i < tagSize; i++ {
			c := partitionTag[i]
			if c != '_' && c != '$' && !isAlpha(c) && !isNumber(c) {
				msg := invalidMsg + "Partition tag can only contain numbers, letters, dollars and underscores."
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
	if int64(len(fieldName)) > Params.ProxyCfg.MaxNameLength {
		msg := invalidMsg + "The length of a field name must be less than " +
			strconv.FormatInt(Params.ProxyCfg.MaxNameLength, 10) + " characters."
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
		if param.Key == "dim" {
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

	if dim <= 0 || dim > Params.ProxyCfg.MaxDimension {
		return fmt.Errorf("invalid dimension: %d. should be in range 1 ~ %d", dim, Params.ProxyCfg.MaxDimension)
	}
	if field.DataType == schemapb.DataType_BinaryVector && dim%8 != 0 {
		return fmt.Errorf("invalid dimension: %d. should be multiple of 8. ", dim)
	}
	return nil
}

func validateMaxLengthPerRow(collectionName string, field *schemapb.FieldSchema) error {
	exist := false
	for _, param := range field.TypeParams {
		if param.Key != maxVarCharLengthKey {
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
		if params.Key == "metric_type" {
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

//ValidateFieldAutoID call after validatePrimaryKey
func ValidateFieldAutoID(coll *schemapb.CollectionSchema) error {
	var idx = -1
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
			if field.DataType == schemapb.DataType_VarChar {
				if field.AutoID {
					return fmt.Errorf("autoID is not supported when the VarChar field is the primary key")
				}
			}

			idx = i
		}
	}
	if idx == -1 {
		return errors.New("primary key is not specified")
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
			dimStr, ok := typeKv["dim"]
			if !ok {
				return fmt.Errorf("dim not found in type_params for vector field %s(%d)", field.Name, field.FieldID)
			}
			dim, err := strconv.Atoi(dimStr)
			if err != nil || dim < 0 {
				return fmt.Errorf("invalid dim; %s", dimStr)
			}

			metricTypeStr, ok := indexKv["metric_type"]
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
		if fieldSchema.DataType != schemapb.DataType_Int64 {
			return nil, errors.New("the data type of the data and the schema do not match")
		}
		fieldData.Field = &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{
						Data: data,
					},
				},
			},
		}
	default:
		return nil, errors.New("currently only support autoID for int64 PrimaryField")
	}

	return &fieldData, nil
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
	if int64(len(username)) > Params.ProxyCfg.MaxUsernameLength {
		msg := invalidMsg + "The length of username must be less than " +
			strconv.FormatInt(Params.ProxyCfg.MaxUsernameLength, 10) + " characters."
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
	if int64(len(password)) < Params.ProxyCfg.MinPasswordLength || int64(len(password)) > Params.ProxyCfg.MaxPasswordLength {
		msg := "The length of password must be great than " + strconv.FormatInt(Params.ProxyCfg.MinPasswordLength, 10) +
			" and less than " + strconv.FormatInt(Params.ProxyCfg.MaxPasswordLength, 10) + " characters."
		return errors.New(msg)
	}
	return nil
}

func validateTravelTimestamp(travelTs, tMax typeutil.Timestamp) error {
	durationSeconds := tsoutil.CalculateDuration(tMax, travelTs) / 1000
	if durationSeconds > Params.CommonCfg.RetentionDuration {
		duration := time.Second * time.Duration(durationSeconds)
		return fmt.Errorf("only support to travel back to %s so far", duration.String())
	}
	return nil
}

func ReplaceID2Name(oldStr string, id int64, name string) string {
	return strings.ReplaceAll(oldStr, strconv.FormatInt(id, 10), name)
}

func parseGuaranteeTs(ts, tMax typeutil.Timestamp) typeutil.Timestamp {
	switch ts {
	case strongTS:
		ts = tMax
	case boundedTS:
		ratio := time.Duration(-Params.CommonCfg.GracefulTime)
		ts = tsoutil.AddPhysicalDurationOnTs(tMax, ratio*time.Millisecond)
	}
	return ts
}

func validateName(entity string, nameType string) error {
	entity = strings.TrimSpace(entity)

	if entity == "" {
		return fmt.Errorf("%s should not be empty", nameType)
	}

	invalidMsg := fmt.Sprintf("invalid %s: %s. ", nameType, entity)
	if int64(len(entity)) > Params.ProxyCfg.MaxNameLength {
		msg := invalidMsg + fmt.Sprintf("the length of %s must be less than ", nameType) +
			strconv.FormatInt(Params.ProxyCfg.MaxNameLength, 10) + " characters."
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
		return []string{}, ErrProxyNotReady()
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
