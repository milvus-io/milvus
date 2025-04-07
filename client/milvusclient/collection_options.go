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
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
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
