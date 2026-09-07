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

package milvusclient

import (
	"encoding/json"
	"fmt"

	"github.com/samber/lo"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/client/v3/entity"
	"github.com/milvus-io/milvus/client/v3/index"
	"github.com/milvus-io/milvus/client/v3/internal/merr"
)

// CreateCollectionOption is the interface builds CreateCollectionRequest.
type CreateCollectionOption interface {
	// Request is the method returns the composed request.
	Request() *milvuspb.CreateCollectionRequest
	// Indexes is the method returns IndexOption to create
	Indexes() []CreateIndexOption
	IsFast() bool
}

// createCollectionOption contains all the parameters to create collection.
type createCollectionOption struct {
	name     string
	shardNum int32

	// fast create collection params
	varcharPK            bool
	varcharPKMaxLength   int
	pkFieldName          string
	vectorFieldName      string
	dim                  int64
	autoID               bool
	enabledDynamicSchema bool

	// advanced create collection params
	schema           *entity.Schema
	consistencyLevel entity.ConsistencyLevel
	properties       map[string]string

	// partition key
	numPartitions int64

	indexOptions []CreateIndexOption

	// is fast create collection
	isFast bool
	// fast creation with index
	metricType entity.MetricType
}

func (opt *createCollectionOption) WithAutoID(autoID bool) *createCollectionOption {
	opt.autoID = autoID
	return opt
}

func (opt *createCollectionOption) WithShardNum(shardNum int32) *createCollectionOption {
	opt.shardNum = shardNum
	return opt
}

func (opt *createCollectionOption) WithDynamicSchema(dynamicSchema bool) *createCollectionOption {
	opt.enabledDynamicSchema = dynamicSchema
	return opt
}

func (opt *createCollectionOption) WithVarcharPK(varcharPK bool, maxLen int) *createCollectionOption {
	opt.varcharPK = varcharPK
	opt.varcharPKMaxLength = maxLen
	return opt
}

func (opt *createCollectionOption) WithIndexOptions(indexOpts ...CreateIndexOption) *createCollectionOption {
	opt.indexOptions = indexOpts
	return opt
}

func (opt *createCollectionOption) WithProperty(key string, value any) *createCollectionOption {
	opt.properties[key] = fmt.Sprintf("%v", value)
	return opt
}

func (opt *createCollectionOption) WithConsistencyLevel(cl entity.ConsistencyLevel) *createCollectionOption {
	opt.consistencyLevel = cl
	return opt
}

func (opt *createCollectionOption) WithMetricType(metricType entity.MetricType) *createCollectionOption {
	opt.metricType = metricType
	return opt
}

func (opt *createCollectionOption) WithPKFieldName(name string) *createCollectionOption {
	opt.pkFieldName = name
	return opt
}

func (opt *createCollectionOption) WithVectorFieldName(name string) *createCollectionOption {
	opt.vectorFieldName = name
	return opt
}

func (opt *createCollectionOption) WithNumPartitions(numPartitions int64) *createCollectionOption {
	opt.numPartitions = numPartitions
	return opt
}

// Validate runs client-side sanity checks against the user-provided schema. Invoked automatically
// from Client.CreateCollection via interface assertion.
func (opt *createCollectionOption) Validate() error {
	if opt.schema == nil {
		return nil
	}
	return opt.schema.Validate()
}

func (opt *createCollectionOption) Request() *milvuspb.CreateCollectionRequest {
	// fast create collection
	if opt.isFast {
		var pkField *entity.Field
		if opt.varcharPK {
			pkField = entity.NewField().WithDataType(entity.FieldTypeVarChar).WithMaxLength(int64(opt.varcharPKMaxLength))
		} else {
			pkField = entity.NewField().WithDataType(entity.FieldTypeInt64)
		}
		pkField = pkField.WithName(opt.pkFieldName).WithIsPrimaryKey(true).WithIsAutoID(opt.autoID)
		opt.schema = entity.NewSchema().
			WithName(opt.name).
			WithAutoID(opt.autoID).
			WithDynamicFieldEnabled(opt.enabledDynamicSchema).
			WithField(pkField).
			WithField(entity.NewField().WithName(opt.vectorFieldName).WithDataType(entity.FieldTypeFloatVector).WithDim(opt.dim))
	}

	var schemaBytes []byte
	if opt.schema != nil {
		opt.schema.WithName(opt.name)
		schemaProto := opt.schema.ProtoMessage()
		schemaBytes, _ = proto.Marshal(schemaProto)
	}

	return &milvuspb.CreateCollectionRequest{
		DbName:           "", // reserved fields, not used for now
		CollectionName:   opt.name,
		Schema:           schemaBytes,
		ShardsNum:        opt.shardNum,
		ConsistencyLevel: commonpb.ConsistencyLevel(opt.consistencyLevel),
		NumPartitions:    opt.numPartitions,
		Properties:       entity.MapKvPairs(opt.properties),
	}
}

func (opt *createCollectionOption) Indexes() []CreateIndexOption {
	// fast create
	if opt.isFast && opt.indexOptions == nil {
		return []CreateIndexOption{
			NewCreateIndexOption(opt.name, opt.vectorFieldName, index.NewGenericIndex("", map[string]string{})),
		}
	}
	return opt.indexOptions
}

func (opt *createCollectionOption) IsFast() bool {
	return opt.isFast
}

// SimpleCreateCollectionOptions returns a CreateCollectionOption with default fast collection options.
func SimpleCreateCollectionOptions(name string, dim int64) *createCollectionOption {
	return &createCollectionOption{
		name:     name,
		shardNum: 1,

		pkFieldName:          "id",
		vectorFieldName:      "vector",
		autoID:               true,
		dim:                  dim,
		enabledDynamicSchema: true,
		consistencyLevel:     entity.DefaultConsistencyLevel,
		properties:           make(map[string]string),

		isFast:     true,
		metricType: entity.COSINE,
	}
}

// NewCreateCollectionOption returns a CreateCollectionOption with customized collection schema
func NewCreateCollectionOption(name string, collectionSchema *entity.Schema) *createCollectionOption {
	return &createCollectionOption{
		name:             name,
		shardNum:         1,
		schema:           collectionSchema,
		consistencyLevel: entity.DefaultConsistencyLevel,
		properties:       make(map[string]string),

		metricType: entity.COSINE,
	}
}

type ListCollectionOption interface {
	Request() *milvuspb.ShowCollectionsRequest
}

type listCollectionOption struct{}

func (opt *listCollectionOption) Request() *milvuspb.ShowCollectionsRequest {
	return &milvuspb.ShowCollectionsRequest{}
}

func NewListCollectionOption() *listCollectionOption {
	return &listCollectionOption{}
}

// DescribeCollectionOption is the interface builds DescribeCollection request.
type DescribeCollectionOption interface {
	// Request is the method returns the composed request.
	Request() *milvuspb.DescribeCollectionRequest
}

type describeCollectionOption struct {
	name string
}

func (opt *describeCollectionOption) Request() *milvuspb.DescribeCollectionRequest {
	return &milvuspb.DescribeCollectionRequest{
		CollectionName: opt.name,
	}
}

// NewDescribeCollectionOption composes a describeCollectionOption with provided collection name.
func NewDescribeCollectionOption(name string) *describeCollectionOption {
	return &describeCollectionOption{
		name: name,
	}
}

// HasCollectionOption is the interface to build DescribeCollectionRequest.
type HasCollectionOption interface {
	Request() *milvuspb.DescribeCollectionRequest
}

type hasCollectionOpt struct {
	name string
}

func (opt *hasCollectionOpt) Request() *milvuspb.DescribeCollectionRequest {
	return &milvuspb.DescribeCollectionRequest{
		CollectionName: opt.name,
	}
}

func NewHasCollectionOption(name string) HasCollectionOption {
	return &hasCollectionOpt{
		name: name,
	}
}

// The DropCollectionOption interface builds DropCollectionRequest.
type DropCollectionOption interface {
	Request() *milvuspb.DropCollectionRequest
}

type dropCollectionOption struct {
	name string
}

func (opt *dropCollectionOption) Request() *milvuspb.DropCollectionRequest {
	return &milvuspb.DropCollectionRequest{
		CollectionName: opt.name,
	}
}

func NewDropCollectionOption(name string) *dropCollectionOption {
	return &dropCollectionOption{
		name: name,
	}
}

type RenameCollectionOption interface {
	Request() *milvuspb.RenameCollectionRequest
}

type renameCollectionOption struct {
	oldCollectionName string
	newCollectionName string
}

func (opt *renameCollectionOption) Request() *milvuspb.RenameCollectionRequest {
	return &milvuspb.RenameCollectionRequest{
		OldName: opt.oldCollectionName,
		NewName: opt.newCollectionName,
	}
}

func NewRenameCollectionOption(oldName, newName string) *renameCollectionOption {
	return &renameCollectionOption{
		oldCollectionName: oldName,
		newCollectionName: newName,
	}
}

type AlterCollectionPropertiesOption interface {
	Request() *milvuspb.AlterCollectionRequest
}

type alterCollectionPropertiesOption struct {
	collectionName string
	properties     map[string]string
}

func (opt *alterCollectionPropertiesOption) WithProperty(key string, value any) *alterCollectionPropertiesOption {
	opt.properties[key] = fmt.Sprintf("%v", value)
	return opt
}

func (opt *alterCollectionPropertiesOption) Request() *milvuspb.AlterCollectionRequest {
	return &milvuspb.AlterCollectionRequest{
		CollectionName: opt.collectionName,
		Properties:     entity.MapKvPairs(opt.properties),
	}
}

func NewAlterCollectionPropertiesOption(collection string) *alterCollectionPropertiesOption {
	return &alterCollectionPropertiesOption{collectionName: collection, properties: make(map[string]string)}
}

type DropCollectionPropertiesOption interface {
	Request() *milvuspb.AlterCollectionRequest
}

type dropCollectionPropertiesOption struct {
	collectionName string
	keys           []string
}

func (opt *dropCollectionPropertiesOption) Request() *milvuspb.AlterCollectionRequest {
	return &milvuspb.AlterCollectionRequest{
		CollectionName: opt.collectionName,
		DeleteKeys:     opt.keys,
	}
}

func NewDropCollectionPropertiesOption(collection string, propertyKeys ...string) *dropCollectionPropertiesOption {
	return &dropCollectionPropertiesOption{
		collectionName: collection,
		keys:           propertyKeys,
	}
}

type AlterCollectionFieldPropertiesOption interface {
	Request() *milvuspb.AlterCollectionFieldRequest
}

type alterCollectionFieldPropertiesOption struct {
	collectionName string
	fieldName      string
	properties     map[string]string
}

func (opt *alterCollectionFieldPropertiesOption) WithProperty(key string, value any) *alterCollectionFieldPropertiesOption {
	opt.properties[key] = fmt.Sprintf("%v", value)
	return opt
}

func (opt *alterCollectionFieldPropertiesOption) Request() *milvuspb.AlterCollectionFieldRequest {
	return &milvuspb.AlterCollectionFieldRequest{
		CollectionName: opt.collectionName,
		FieldName:      opt.fieldName,
		Properties:     entity.MapKvPairs(opt.properties),
	}
}

func NewAlterCollectionFieldPropertiesOption(collectionName string, fieldName string) *alterCollectionFieldPropertiesOption {
	return &alterCollectionFieldPropertiesOption{
		collectionName: collectionName,
		fieldName:      fieldName,
		properties:     make(map[string]string),
	}
}

// TruncateCollectionOption is the interface builds TruncateCollectionRequest.
type TruncateCollectionOption interface {
	Request() *milvuspb.TruncateCollectionRequest
}

type truncateCollectionOption struct {
	name string
}

func (opt *truncateCollectionOption) Request() *milvuspb.TruncateCollectionRequest {
	return &milvuspb.TruncateCollectionRequest{
		CollectionName: opt.name,
	}
}

func NewTruncateCollectionOption(name string) *truncateCollectionOption {
	return &truncateCollectionOption{name: name}
}

type GetCollectionOption interface {
	Request() *milvuspb.GetCollectionStatisticsRequest
}

type getCollectionStatsOption struct {
	collectionName string
}

func (opt *getCollectionStatsOption) Request() *milvuspb.GetCollectionStatisticsRequest {
	return &milvuspb.GetCollectionStatisticsRequest{
		CollectionName: opt.collectionName,
	}
}

func NewGetCollectionStatsOption(collectionName string) *getCollectionStatsOption {
	return &getCollectionStatsOption{collectionName: collectionName}
}

type alterCollectionSchemaOption interface {
	Request() *milvuspb.AlterCollectionSchemaRequest
	Validate() error
}

func newAlterCollectionSchemaAddRequest(collectionName string, field *schemapb.FieldSchema, function *schemapb.FunctionSchema) *milvuspb.AlterCollectionSchemaRequest {
	addRequest := &milvuspb.AlterCollectionSchemaRequest_AddRequest{}
	if field != nil {
		addRequest.FieldInfos = []*milvuspb.AlterCollectionSchemaRequest_FieldInfo{{FieldSchema: field}}
	}
	if function != nil {
		addRequest.FuncSchema = []*schemapb.FunctionSchema{function}
	}
	return &milvuspb.AlterCollectionSchemaRequest{
		CollectionName: collectionName,
		Action: &milvuspb.AlterCollectionSchemaRequest_Action{
			Op: &milvuspb.AlterCollectionSchemaRequest_Action_AddRequest{AddRequest: addRequest},
		},
	}
}

func newAlterCollectionSchemaDropRequest(collectionName string, dropRequest *milvuspb.AlterCollectionSchemaRequest_DropRequest) *milvuspb.AlterCollectionSchemaRequest {
	return &milvuspb.AlterCollectionSchemaRequest{
		CollectionName: collectionName,
		Action: &milvuspb.AlterCollectionSchemaRequest_Action{
			Op: &milvuspb.AlterCollectionSchemaRequest_Action_DropRequest{DropRequest: dropRequest},
		},
	}
}

type AddFunctionFieldOption interface {
	alterCollectionSchemaOption
	isAddFunctionFieldOption()
}

type addFunctionFieldOption struct {
	collectionName string
	fieldSch       *entity.Field
	function       *entity.Function
	boundIndex     index.Index
	indexName      string
}

func (*addFunctionFieldOption) isAddFunctionFieldOption() {}

func (opt *addFunctionFieldOption) Request() *milvuspb.AlterCollectionSchemaRequest {
	req := newAlterCollectionSchemaAddRequest(opt.collectionName, opt.fieldSch.ProtoMessage(), opt.function.ProtoMessage())
	fieldInfo := req.GetAction().GetAddRequest().GetFieldInfos()[0]
	fieldInfo.IndexName = opt.indexName
	fieldInfo.ExtraParams = entity.MapKvPairs(opt.boundIndex.Params())
	return req
}

func getBoundIndexType(params map[string]string) (string, error) {
	indexType := params[index.IndexTypeKey]
	legacyParams, hasLegacyParams := params[index.ParamsKey]
	if !hasLegacyParams {
		return indexType, nil
	}
	var decoded map[string]any
	if err := json.Unmarshal([]byte(legacyParams), &decoded); err != nil {
		return "", merr.WrapErrParameterInvalidErr(err, "invalid legacy bound index params")
	}
	legacyIndexType := ""
	if rawIndexType, ok := decoded[index.IndexTypeKey]; ok {
		var valid bool
		legacyIndexType, valid = rawIndexType.(string)
		if !valid || legacyIndexType == "" {
			return "", merr.WrapErrParameterInvalidMsg("legacy bound index param %q must be a non-empty string", index.IndexTypeKey)
		}
	}
	for key := range decoded {
		if _, duplicated := params[key]; duplicated {
			return "", merr.WrapErrParameterInvalidMsg("duplicated bound index param %q", key)
		}
	}
	if indexType == "" {
		indexType = legacyIndexType
	}
	return indexType, nil
}

func (opt *addFunctionFieldOption) Validate() error {
	if opt == nil {
		return merr.WrapErrParameterMissingMsg("add function field option is nil")
	}
	if opt.collectionName == "" {
		return merr.WrapErrParameterMissingMsg("collection name is required")
	}
	if opt.fieldSch == nil {
		return merr.WrapErrParameterMissingMsg("field schema is required")
	}
	if opt.fieldSch.Name == "" {
		return merr.WrapErrParameterMissingMsg("field name is required")
	}
	if opt.function == nil {
		return merr.WrapErrParameterMissingMsg("function schema is required")
	}
	if opt.function.Name == "" {
		return merr.WrapErrParameterMissingMsg("function name is required")
	}
	switch opt.function.Type {
	case entity.FunctionTypeBM25:
		if opt.fieldSch.DataType != entity.FieldTypeSparseVector {
			return merr.WrapErrParameterInvalidMsg("add function field requires SparseFloatVector output field for BM25, got %s", opt.fieldSch.DataType.String())
		}
	case entity.FunctionTypeMinHash:
		if opt.fieldSch.DataType != entity.FieldTypeBinaryVector {
			return merr.WrapErrParameterInvalidMsg("add function field requires BinaryVector output field for MinHash, got %s", opt.fieldSch.DataType.String())
		}
	default:
		return merr.WrapErrParameterInvalidMsg("add function field only supports BM25 and MinHash, got %s", opt.function.Type.String())
	}
	if lo.IsNil(opt.boundIndex) {
		return merr.WrapErrParameterMissingMsg("bound index option is required")
	}
	indexType, err := getBoundIndexType(opt.boundIndex.Params())
	if err != nil {
		return err
	}
	if indexType == "" {
		return merr.WrapErrParameterMissingMsg("explicit index type is required for the bound index")
	}
	return nil
}

func (opt *addFunctionFieldOption) WithIndexName(indexName string) *addFunctionFieldOption {
	opt.indexName = indexName
	return opt
}

func NewAddFunctionFieldOption(collectionName string, field *entity.Field, function *entity.Function, boundIndex index.Index) *addFunctionFieldOption {
	return &addFunctionFieldOption{collectionName: collectionName, fieldSch: field, function: function, boundIndex: boundIndex}
}

type DropCollectionFieldOption interface {
	alterCollectionSchemaOption
	isDropCollectionFieldOption()
}

type dropCollectionFieldOption struct {
	collectionName string
	fieldName      string
	fieldID        int64
	byID           bool
}

func (*dropCollectionFieldOption) isDropCollectionFieldOption() {}

func (opt *dropCollectionFieldOption) Request() *milvuspb.AlterCollectionSchemaRequest {
	dropRequest := &milvuspb.AlterCollectionSchemaRequest_DropRequest{}
	if opt.byID {
		dropRequest.Identifier = &milvuspb.AlterCollectionSchemaRequest_DropRequest_FieldId{FieldId: opt.fieldID}
	} else {
		dropRequest.Identifier = &milvuspb.AlterCollectionSchemaRequest_DropRequest_FieldName{FieldName: opt.fieldName}
	}
	return newAlterCollectionSchemaDropRequest(opt.collectionName, dropRequest)
}

func (opt *dropCollectionFieldOption) Validate() error {
	if opt == nil {
		return merr.WrapErrParameterMissingMsg("drop collection field option is nil")
	}
	if opt.collectionName == "" {
		return merr.WrapErrParameterMissingMsg("collection name is required")
	}
	if opt.byID {
		if opt.fieldID <= 0 {
			return merr.WrapErrParameterInvalidMsg("field id must be greater than 0")
		}
		return nil
	}
	if opt.fieldName == "" {
		return merr.WrapErrParameterMissingMsg("field name is required")
	}
	return nil
}

func NewDropCollectionFieldOption(collectionName, fieldName string) *dropCollectionFieldOption {
	return &dropCollectionFieldOption{collectionName: collectionName, fieldName: fieldName}
}

func NewDropCollectionFieldByIDOption(collectionName string, fieldID int64) *dropCollectionFieldOption {
	return &dropCollectionFieldOption{collectionName: collectionName, fieldID: fieldID, byID: true}
}

type DropFunctionFieldOption interface {
	alterCollectionSchemaOption
	isDropFunctionFieldOption()
}

type dropFunctionFieldOption struct {
	collectionName string
	functionName   string
}

func (*dropFunctionFieldOption) isDropFunctionFieldOption() {}

func (opt *dropFunctionFieldOption) Request() *milvuspb.AlterCollectionSchemaRequest {
	return newAlterCollectionSchemaDropRequest(opt.collectionName, &milvuspb.AlterCollectionSchemaRequest_DropRequest{
		Identifier:               &milvuspb.AlterCollectionSchemaRequest_DropRequest_FunctionName{FunctionName: opt.functionName},
		DropFunctionOutputFields: true,
	})
}

func (opt *dropFunctionFieldOption) Validate() error {
	if opt == nil {
		return merr.WrapErrParameterMissingMsg("drop function field option is nil")
	}
	if opt.collectionName == "" {
		return merr.WrapErrParameterMissingMsg("collection name is required")
	}
	if opt.functionName == "" {
		return merr.WrapErrParameterMissingMsg("function name is required")
	}
	return nil
}

func NewDropFunctionFieldOption(collectionName, functionName string) *dropFunctionFieldOption {
	return &dropFunctionFieldOption{collectionName: collectionName, functionName: functionName}
}

type AddCollectionFieldOption interface {
	Request() *milvuspb.AddCollectionFieldRequest
	// Validate validates the option before sending request
	Validate() error
}

type addCollectionFieldOption struct {
	collectionName string
	fieldSch       *entity.Field
}

func (c *addCollectionFieldOption) Request() *milvuspb.AddCollectionFieldRequest {
	bs, _ := proto.Marshal(c.fieldSch.ProtoMessage())
	return &milvuspb.AddCollectionFieldRequest{
		CollectionName: c.collectionName,
		Schema:         bs,
	}
}

// Validate validates the option before sending request
func (c *addCollectionFieldOption) Validate() error {
	// Vector fields must be nullable when adding to existing collection
	if c.fieldSch.DataType.IsVectorType() && !c.fieldSch.Nullable {
		return fmt.Errorf("adding vector field to existing collection requires nullable=true, field name = %s", c.fieldSch.Name)
	}
	return nil
}

func NewAddCollectionFieldOption(collectionName string, field *entity.Field) *addCollectionFieldOption {
	return &addCollectionFieldOption{
		collectionName: collectionName,
		fieldSch:       field,
	}
}

type AddCollectionStructFieldOption interface {
	Request() *milvuspb.AddCollectionStructFieldRequest
	// Validate validates the option before sending request
	Validate() error
}

type addCollectionStructFieldOption struct {
	collectionName string
	fieldSch       *entity.Field
}

func (c *addCollectionStructFieldOption) Request() *milvuspb.AddCollectionStructFieldRequest {
	collSchema := entity.NewSchema().WithField(c.fieldSch).ProtoMessage()
	return &milvuspb.AddCollectionStructFieldRequest{
		CollectionName:         c.collectionName,
		StructArrayFieldSchema: collSchema.GetStructArrayFields()[0],
	}
}

// Validate validates the option before sending request
func (c *addCollectionStructFieldOption) Validate() error {
	if c == nil {
		return fmt.Errorf("add collection struct field option is nil")
	}
	if c.fieldSch == nil {
		return fmt.Errorf("struct array field schema is required")
	}
	if c.fieldSch.DataType != entity.FieldTypeArray || c.fieldSch.ElementType != entity.FieldTypeStruct {
		return fmt.Errorf("adding struct array field requires data type Array and element type Struct, field name = %s", c.fieldSch.Name)
	}
	if c.fieldSch.StructSchema == nil {
		return fmt.Errorf("struct schema is required for struct array field %s", c.fieldSch.Name)
	}
	return c.fieldSch.StructSchema.Validate(c.fieldSch.Name)
}

func NewAddCollectionStructFieldOption(collectionName string, field *entity.Field) *addCollectionStructFieldOption {
	return &addCollectionStructFieldOption{
		collectionName: collectionName,
		fieldSch:       field,
	}
}
