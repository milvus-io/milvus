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

package httpserver

import (
	"strconv"

	"github.com/gin-gonic/gin"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

type EmptyReq struct{}

func (req *EmptyReq) GetDbName() string { return "" }

type DatabaseReq struct {
	DbName string `json:"dbName"`
}

func (req *DatabaseReq) GetDbName() string { return req.DbName }

type DatabaseReqRequiredName struct {
	DbName string `json:"dbName" binding:"required"`
}

func (req *DatabaseReqRequiredName) GetDbName() string { return req.DbName }

type DatabaseReqWithProperties struct {
	DbName     string                 `json:"dbName" binding:"required"`
	Properties map[string]interface{} `json:"properties"`
}

func (req *DatabaseReqWithProperties) GetDbName() string { return req.DbName }

type DropDatabasePropertiesReq struct {
	DbName       string   `json:"dbName" binding:"required"`
	PropertyKeys []string `json:"propertyKeys"`
}

func (req *DropDatabasePropertiesReq) GetDbName() string { return req.DbName }

type CollectionNameReq struct {
	DbName         string   `json:"dbName"`
	CollectionName string   `json:"collectionName" binding:"required"`
	PartitionNames []string `json:"partitionNames"` // get partitions load state
}

func (req *CollectionNameReq) GetDbName() string {
	return req.DbName
}

func (req *CollectionNameReq) GetCollectionName() string {
	return req.CollectionName
}

func (req *CollectionNameReq) GetPartitionNames() []string {
	return req.PartitionNames
}

type CollectionReqWithProperties struct {
	DbName         string                 `json:"dbName"`
	CollectionName string                 `json:"collectionName" binding:"required"`
	Properties     map[string]interface{} `json:"properties"`
}

func (req *CollectionReqWithProperties) GetDbName() string { return req.DbName }

func (req *CollectionReqWithProperties) GetCollectionName() string {
	return req.CollectionName
}

type OptionalCollectionNameReq struct {
	DbName         string `json:"dbName"`
	CollectionName string `json:"collectionName"`
}

func (req *OptionalCollectionNameReq) GetDbName() string {
	return req.DbName
}

func (req *OptionalCollectionNameReq) GetCollectionName() string {
	return req.CollectionName
}

type RenameCollectionReq struct {
	DbName            string `json:"dbName"`
	CollectionName    string `json:"collectionName" binding:"required"`
	NewCollectionName string `json:"newCollectionName" binding:"required"`
	NewDbName         string `json:"newDbName"`
}

func (req *RenameCollectionReq) GetDbName() string { return req.DbName }

type DropCollectionPropertiesReq struct {
	DbName         string   `json:"dbName"`
	CollectionName string   `json:"collectionName" binding:"required"`
	PropertyKeys   []string `json:"propertyKeys"`
}

func (req *DropCollectionPropertiesReq) GetDbName() string { return req.DbName }

func (req *DropCollectionPropertiesReq) GetCollectionName() string {
	return req.CollectionName
}

type CompactReq struct {
	DbName         string `json:"dbName"`
	CollectionName string `json:"collectionName" binding:"required"`
	IsClustering   bool   `json:"isClustering"`
}

func (req *CompactReq) GetDbName() string { return req.DbName }

func (req *CompactReq) GetCollectionName() string {
	return req.CollectionName
}

type GetCompactionStateReq struct {
	JobID int64 `json:"jobID"`
}

type FlushReq struct {
	DbName         string `json:"dbName"`
	CollectionName string `json:"collectionName" binding:"required"`
}

func (req *FlushReq) GetDbName() string { return req.DbName }

func (req *FlushReq) GetCollectionName() string {
	return req.CollectionName
}

type CollectionFieldReqWithParams struct {
	DbName         string                 `json:"dbName"`
	CollectionName string                 `json:"collectionName" binding:"required"`
	FieldName      string                 `json:"fieldName" binding:"required"`
	FieldParams    map[string]interface{} `json:"fieldParams"`
}

func (req *CollectionFieldReqWithParams) GetDbName() string { return req.DbName }

func (req *CollectionFieldReqWithParams) GetCollectionName() string {
	return req.CollectionName
}

func (req *CollectionFieldReqWithParams) GetFieldName() string {
	return req.FieldName
}

type PartitionReq struct {
	// CollectionNameReq
	DbName         string `json:"dbName"`
	CollectionName string `json:"collectionName" binding:"required"`
	PartitionName  string `json:"partitionName" binding:"required"`
}

func (req *PartitionReq) GetDbName() string         { return req.DbName }
func (req *PartitionReq) GetCollectionName() string { return req.CollectionName }
func (req *PartitionReq) GetPartitionName() string  { return req.PartitionName }

type ImportReq struct {
	DbName         string            `json:"dbName"`
	CollectionName string            `json:"collectionName" binding:"required"`
	PartitionName  string            `json:"partitionName"`
	Files          [][]string        `json:"files" binding:"required"`
	Options        map[string]string `json:"options"`
}

func (req *ImportReq) GetDbName() string {
	return req.DbName
}

func (req *ImportReq) GetCollectionName() string {
	return req.CollectionName
}

func (req *ImportReq) GetPartitionName() string {
	return req.PartitionName
}

func (req *ImportReq) GetFiles() [][]string {
	return req.Files
}

func (req *ImportReq) GetOptions() map[string]string {
	return req.Options
}

type JobIDReq struct {
	JobID string `json:"jobId" binding:"required"`
}

func (req *JobIDReq) GetJobID() string { return req.JobID }

type QueryReqV2 struct {
	DbName         string                 `json:"dbName"`
	CollectionName string                 `json:"collectionName" binding:"required"`
	PartitionNames []string               `json:"partitionNames"`
	OutputFields   []string               `json:"outputFields"`
	Filter         string                 `json:"filter"`
	Limit          int32                  `json:"limit"`
	Offset         int32                  `json:"offset"`
	ExprParams     map[string]interface{} `json:"exprParams"`
}

func (req *QueryReqV2) GetDbName() string { return req.DbName }

type CollectionIDReq struct {
	DbName         string      `json:"dbName"`
	CollectionName string      `json:"collectionName" binding:"required"`
	PartitionName  string      `json:"partitionName"`
	PartitionNames []string    `json:"partitionNames"`
	OutputFields   []string    `json:"outputFields"`
	ID             interface{} `json:"id" binding:"required"`
}

func (req *CollectionIDReq) GetDbName() string { return req.DbName }

type CollectionFilterReq struct {
	DbName         string                 `json:"dbName"`
	CollectionName string                 `json:"collectionName" binding:"required"`
	PartitionName  string                 `json:"partitionName"`
	Filter         string                 `json:"filter" binding:"required"`
	ExprParams     map[string]interface{} `json:"exprParams"`
}

func (req *CollectionFilterReq) GetDbName() string { return req.DbName }

type CollectionDataReq struct {
	DbName         string                   `json:"dbName"`
	CollectionName string                   `json:"collectionName" binding:"required"`
	PartitionName  string                   `json:"partitionName"`
	Data           []map[string]interface{} `json:"data" binding:"required"`
}

func (req *CollectionDataReq) GetDbName() string { return req.DbName }

type SearchReqV2 struct {
	DbName           string                 `json:"dbName"`
	CollectionName   string                 `json:"collectionName" binding:"required"`
	Data             []interface{}          `json:"data" binding:"required"`
	AnnsField        string                 `json:"annsField"`
	PartitionNames   []string               `json:"partitionNames"`
	Filter           string                 `json:"filter"`
	GroupByField     string                 `json:"groupingField"`
	GroupSize        int32                  `json:"groupSize"`
	StrictGroupSize  bool                   `json:"strictGroupSize"`
	Limit            int32                  `json:"limit"`
	Offset           int32                  `json:"offset"`
	OutputFields     []string               `json:"outputFields"`
	SearchParams     map[string]interface{} `json:"searchParams"`
	ConsistencyLevel string                 `json:"consistencyLevel"`
	ExprParams       map[string]interface{} `json:"exprParams"`
	// not use Params any more, just for compatibility
	Params map[string]float64 `json:"params"`
}

func (req *SearchReqV2) GetDbName() string { return req.DbName }

type Rand struct {
	Strategy string                 `json:"strategy"`
	Params   map[string]interface{} `json:"params"`
}

type SubSearchReq struct {
	Data         []interface{}          `json:"data" binding:"required"`
	AnnsField    string                 `json:"annsField"`
	Filter       string                 `json:"filter"`
	GroupByField string                 `json:"groupingField"`
	MetricType   string                 `json:"metricType"`
	Limit        int32                  `json:"limit"`
	Offset       int32                  `json:"offset"`
	SearchParams map[string]interface{} `json:"params"`
	ExprParams   map[string]interface{} `json:"exprParams"`
}

type HybridSearchReq struct {
	DbName           string         `json:"dbName"`
	CollectionName   string         `json:"collectionName" binding:"required"`
	PartitionNames   []string       `json:"partitionNames"`
	Search           []SubSearchReq `json:"search"`
	Rerank           Rand           `json:"rerank"`
	Limit            int32          `json:"limit"`
	GroupByField     string         `json:"groupingField"`
	GroupSize        int32          `json:"groupSize"`
	StrictGroupSize  bool           `json:"strictGroupSize"`
	OutputFields     []string       `json:"outputFields"`
	ConsistencyLevel string         `json:"consistencyLevel"`
}

func (req *HybridSearchReq) GetDbName() string { return req.DbName }

type ReturnErrMsg struct {
	Code    int32  `json:"code"`
	Message string `json:"message"`
}

type PartitionsReq struct {
	// CollectionNameReq
	DbName         string   `json:"dbName"`
	CollectionName string   `json:"collectionName" binding:"required"`
	PartitionNames []string `json:"partitionNames" binding:"required"`
}

func (req *PartitionsReq) GetDbName() string { return req.DbName }

type UserReq struct {
	UserName string `json:"userName" binding:"required"`
}

func (req *UserReq) GetUserName() string { return req.UserName }

type BaseGetter interface {
	GetBase() *commonpb.MsgBase
}
type UserNameGetter interface {
	GetUserName() string
}
type RoleNameGetter interface {
	GetRoleName() string
}
type IndexNameGetter interface {
	GetIndexName() string
}
type AliasNameGetter interface {
	GetAliasName() string
}
type FilesGetter interface {
	GetFiles() [][]string
}
type OptionsGetter interface {
	GetOptions() map[string]string
}
type JobIDGetter interface {
	GetJobID() string
}

type PasswordReq struct {
	UserName string `json:"userName" binding:"required"`
	Password string `json:"password" binding:"required"`
}

type NewPasswordReq struct {
	UserName    string `json:"userName" binding:"required"`
	Password    string `json:"password" binding:"required"`
	NewPassword string `json:"newPassword" binding:"required"`
}

type UserRoleReq struct {
	UserName string `json:"userName" binding:"required"`
	RoleName string `json:"roleName" binding:"required"`
}

type RoleReq struct {
	DbName   string `json:"dbName"`
	RoleName string `json:"roleName" binding:"required"`
}

func (req *RoleReq) GetDbName() string { return req.DbName }

func (req *RoleReq) GetRoleName() string {
	return req.RoleName
}

type PrivilegeGroupReq struct {
	PrivilegeGroupName string   `json:"privilegeGroupName" binding:"required"`
	Privileges         []string `json:"privileges"`
}

type GrantV2Req struct {
	RoleName       string `json:"roleName" binding:"required"`
	DbName         string `json:"dbName"`
	CollectionName string `json:"collectionName"`
	Privilege      string `json:"privilege" binding:"required"`
}

type GrantReq struct {
	RoleName   string `json:"roleName" binding:"required"`
	ObjectType string `json:"objectType" binding:"required"`
	ObjectName string `json:"objectName" binding:"required"`
	Privilege  string `json:"privilege" binding:"required"`
	DbName     string `json:"dbName"`
}

func (req *GrantReq) GetDbName() string { return req.DbName }

type IndexParam struct {
	FieldName  string                 `json:"fieldName" binding:"required"`
	IndexName  string                 `json:"indexName"`
	MetricType string                 `json:"metricType"`
	IndexType  string                 `json:"indexType"`
	Params     map[string]interface{} `json:"params"`
}

type IndexParamReq struct {
	DbName         string       `json:"dbName"`
	CollectionName string       `json:"collectionName" binding:"required"`
	IndexParams    []IndexParam `json:"indexParams" binding:"required"`
}

func (req *IndexParamReq) GetDbName() string { return req.DbName }

type IndexReq struct {
	DbName         string `json:"dbName"`
	CollectionName string `json:"collectionName" binding:"required"`
	IndexName      string `json:"indexName" binding:"required"`
}

func (req *IndexReq) GetDbName() string { return req.DbName }
func (req *IndexReq) GetCollectionName() string {
	return req.CollectionName
}

func (req *IndexReq) GetIndexName() string {
	return req.IndexName
}

type IndexReqWithProperties struct {
	DbName         string                 `json:"dbName"`
	CollectionName string                 `json:"collectionName" binding:"required"`
	IndexName      string                 `json:"indexName" binding:"required"`
	Properties     map[string]interface{} `json:"properties"`
}

func (req *IndexReqWithProperties) GetDbName() string { return req.DbName }

func (req *IndexReqWithProperties) GetCollectionName() string {
	return req.CollectionName
}

func (req *IndexReqWithProperties) GetIndexName() string {
	return req.IndexName
}

type DropIndexPropertiesReq struct {
	DbName         string   `json:"dbName"`
	CollectionName string   `json:"collectionName" binding:"required"`
	IndexName      string   `json:"indexName" binding:"required"`
	PropertyKeys   []string `json:"propertyKeys"`
}

func (req *DropIndexPropertiesReq) GetDbName() string { return req.DbName }

func (req *DropIndexPropertiesReq) GetCollectionName() string {
	return req.CollectionName
}

func (req *DropIndexPropertiesReq) GetIndexName() string {
	return req.IndexName
}

type FieldSchema struct {
	FieldName         string                 `json:"fieldName" binding:"required"`
	DataType          string                 `json:"dataType" binding:"required"`
	ElementDataType   string                 `json:"elementDataType"`
	IsPrimary         bool                   `json:"isPrimary"`
	IsPartitionKey    bool                   `json:"isPartitionKey"`
	IsClusteringKey   bool                   `json:"isClusteringKey"`
	ElementTypeParams map[string]interface{} `json:"elementTypeParams" binding:"required"`
	Nullable          bool                   `json:"nullable" binding:"required"`
	DefaultValue      interface{}            `json:"defaultValue" binding:"required"`
}

type FunctionSchema struct {
	FunctionName     string                 `json:"name" binding:"required"`
	Description      string                 `json:"description"`
	FunctionType     string                 `json:"type" binding:"required"`
	InputFieldNames  []string               `json:"inputFieldNames" binding:"required"`
	OutputFieldNames []string               `json:"outputFieldNames" binding:"required"`
	Params           map[string]interface{} `json:"params"`
}

type CollectionSchema struct {
	Fields             []FieldSchema    `json:"fields"`
	Functions          []FunctionSchema `json:"functions"`
	AutoId             bool             `json:"autoID"`
	EnableDynamicField bool             `json:"enableDynamicField"`
}

type CollectionReq struct {
	DbName           string                 `json:"dbName"`
	CollectionName   string                 `json:"collectionName" binding:"required"`
	Dimension        int32                  `json:"dimension"`
	IDType           string                 `json:"idType"`
	AutoID           bool                   `json:"autoID"`
	MetricType       string                 `json:"metricType"`
	PrimaryFieldName string                 `json:"primaryFieldName"`
	VectorFieldName  string                 `json:"vectorFieldName"`
	Schema           CollectionSchema       `json:"schema"`
	IndexParams      []IndexParam           `json:"indexParams"`
	Params           map[string]interface{} `json:"params"`
	Description      string                 `json:"description"`
}

func (req *CollectionReq) GetDbName() string { return req.DbName }

type AliasReq struct {
	DbName    string `json:"dbName"`
	AliasName string `json:"aliasName" binding:"required"`
}

func (req *AliasReq) GetDbName() string { return req.DbName }

func (req *AliasReq) GetAliasName() string {
	return req.AliasName
}

type AliasCollectionReq struct {
	DbName         string `json:"dbName"`
	CollectionName string `json:"collectionName" binding:"required"`
	AliasName      string `json:"aliasName" binding:"required"`
}

func (req *AliasCollectionReq) GetDbName() string { return req.DbName }

func (req *AliasCollectionReq) GetCollectionName() string {
	return req.CollectionName
}

func (req *AliasCollectionReq) GetAliasName() string {
	return req.AliasName
}

func wrapperReturnHas(has bool) gin.H {
	return gin.H{HTTPReturnCode: merr.Code(nil), HTTPReturnData: gin.H{HTTPReturnHas: has}}
}

func wrapperReturnList(names []string) gin.H {
	if names == nil {
		return gin.H{HTTPReturnCode: merr.Code(nil), HTTPReturnData: []string{}}
	}
	return gin.H{HTTPReturnCode: merr.Code(nil), HTTPReturnData: names}
}

func wrapperReturnRowCount(pairs []*commonpb.KeyValuePair) gin.H {
	rowCountValue := "0"
	for _, keyValue := range pairs {
		if keyValue.Key == "row_count" {
			rowCountValue = keyValue.GetValue()
		}
	}
	rowCount, err := strconv.ParseInt(rowCountValue, 10, 64)
	if err != nil {
		return gin.H{HTTPReturnCode: merr.Code(nil), HTTPReturnData: gin.H{HTTPReturnRowCount: rowCountValue}}
	}
	return gin.H{HTTPReturnCode: merr.Code(nil), HTTPReturnData: gin.H{HTTPReturnRowCount: rowCount}}
}

func wrapperReturnDefault() gin.H {
	return gin.H{HTTPReturnCode: merr.Code(nil), HTTPReturnData: gin.H{}}
}

func wrapperReturnDefaultWithCost(cost int) gin.H {
	return gin.H{HTTPReturnCode: merr.Code(nil), HTTPReturnData: gin.H{}, HTTPReturnCost: cost}
}

type ResourceGroupNodeFilter struct {
	NodeLabels map[string]string `json:"node_labels" binding:"required"`
}

func (req *ResourceGroupNodeFilter) GetNodeLabels() map[string]string {
	return req.NodeLabels
}

type ResourceGroupLimit struct {
	NodeNum int32 `json:"node_num" binding:"required"`
}

func (req *ResourceGroupLimit) GetNodeNum() int32 {
	return req.NodeNum
}

type ResourceGroupTransfer struct {
	ResourceGroup string `json:"resource_group" binding:"required"`
}

func (req *ResourceGroupTransfer) GetResourceGroup() string {
	return req.ResourceGroup
}

type ResourceGroupConfig struct {
	Requests     *ResourceGroupLimit      `json:"requests" binding:"required"`
	Limits       *ResourceGroupLimit      `json:"limits" binding:"required"`
	TransferFrom []*ResourceGroupTransfer `json:"transfer_from"`
	TransferTo   []*ResourceGroupTransfer `json:"transfer_to"`
	NodeFilter   *ResourceGroupNodeFilter `json:"node_filter"`
}

func (req *ResourceGroupConfig) GetRequests() *ResourceGroupLimit {
	return req.Requests
}

func (req *ResourceGroupConfig) GetLimits() *ResourceGroupLimit {
	return req.Limits
}

func (req *ResourceGroupConfig) GetTransferFrom() []*ResourceGroupTransfer {
	return req.TransferFrom
}

func (req *ResourceGroupConfig) GetTransferTo() []*ResourceGroupTransfer {
	return req.TransferTo
}

func (req *ResourceGroupConfig) GetNodeFilter() *ResourceGroupNodeFilter {
	return req.NodeFilter
}

type ResourceGroupReq struct {
	Name   string               `json:"name" binding:"required"`
	Config *ResourceGroupConfig `json:"config"`
}

func (req *ResourceGroupReq) GetName() string {
	return req.Name
}

func (req *ResourceGroupReq) GetConfig() *ResourceGroupConfig {
	return req.Config
}

type UpdateResourceGroupReq struct {
	ResourceGroups map[string]*ResourceGroupConfig `json:"resource_groups" binding:"required"`
}

func (req *UpdateResourceGroupReq) GetResourceGroups() map[string]*ResourceGroupConfig {
	return req.ResourceGroups
}

type TransferReplicaReq struct {
	SourceRgName   string `json:"sourceRgName" binding:"required"`
	TargetRgName   string `json:"targetRgName" binding:"required"`
	CollectionName string `json:"collectionName" binding:"required"`
	ReplicaNum     int64  `json:"replicaNum" binding:"required"`
}

func (req *TransferReplicaReq) GetSourceRgName() string {
	return req.SourceRgName
}

func (req *TransferReplicaReq) GetTargetRgName() string {
	return req.TargetRgName
}

func (req *TransferReplicaReq) GetCollectionName() string {
	return req.CollectionName
}

func (req *TransferReplicaReq) GetReplicaNum() int64 {
	return req.ReplicaNum
}
