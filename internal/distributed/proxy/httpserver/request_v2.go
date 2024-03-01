package httpserver

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
)

type DatabaseReq struct {
	DbName string `json:"dbName"`
}

func (req *DatabaseReq) GetDbName() string { return req.DbName }

type CollectionNameReq struct {
	DbName         string   `json:"dbName"`
	CollectionName string   `json:"collectionName" binding:"required"`
	Limit          int32    `json:"limit"`          // list import jobs
	PartitionNames []string `json:"partitionNames"` // get partitions load state
}

func (req *CollectionNameReq) GetDbName() string {
	return req.DbName
}

func (req *CollectionNameReq) GetCollectionName() string {
	return req.CollectionName
}

func (req *CollectionNameReq) GetLimit() int32 {
	return req.Limit
}

func (req *CollectionNameReq) GetPartitionNames() []string {
	return req.PartitionNames
}

type RenameCollectionReq struct {
	DbName            string `json:"dbName"`
	CollectionName    string `json:"collectionName" binding:"required"`
	NewCollectionName string `json:"newCollectionName" binding:"required"`
	NewDbName         string `json:"newDbName"`
}

func (req *RenameCollectionReq) GetDbName() string { return req.DbName }

type PartitionReq struct {
	// CollectionNameReq
	DbName         string `json:"dbName"`
	CollectionName string `json:"collectionName" binding:"required"`
	PartitionName  string `json:"partitionName" binding:"required"`
}

func (req *PartitionReq) GetDbName() string         { return req.DbName }
func (req *PartitionReq) GetCollectionName() string { return req.CollectionName }
func (req *PartitionReq) GetPartitionName() string  { return req.PartitionName }

type DataFilesReq struct {
	DbName         string   `json:"dbName"`
	CollectionName string   `json:"collectionName" binding:"required"`
	Files          []string `json:"files" binding:"required"`
}

func (req *DataFilesReq) GetDbName() string {
	return req.DbName
}

func (req *DataFilesReq) GetCollectionName() string {
	return req.CollectionName
}

func (req *DataFilesReq) GetFileNames() []string {
	return req.Files
}

type TaskIDReq struct {
	TaskID int64 `json:"taskID" binding:"required"`
}

func (req *TaskIDReq) GetTaskID() int64 { return req.TaskID }

type QueryReqV2 struct {
	DbName         string   `json:"dbName"`
	CollectionName string   `json:"collectionName" binding:"required"`
	PartitionNames []string `json:"partitionNames"`
	OutputFields   []string `json:"outputFields"`
	Filter         string   `json:"filter" binding:"required"`
	Limit          int32    `json:"limit"`
	Offset         int32    `json:"offset"`
}

func (req *QueryReqV2) GetDbName() string { return req.DbName }

type CollectionIDOutputReq struct {
	DbName         string      `json:"dbName"`
	CollectionName string      `json:"collectionName" binding:"required"`
	PartitionName  string      `json:"partitionName"`
	PartitionNames []string    `json:"partitionNames"`
	OutputFields   []string    `json:"outputFields"`
	ID             interface{} `json:"id" binding:"required"`
}

func (req *CollectionIDOutputReq) GetDbName() string { return req.DbName }

type CollectionIDFilterReq struct {
	DbName         string      `json:"dbName"`
	CollectionName string      `json:"collectionName" binding:"required"`
	PartitionName  string      `json:"partitionName"`
	ID             interface{} `json:"id"`
	Filter         string      `json:"filter"`
}

func (req *CollectionIDFilterReq) GetDbName() string { return req.DbName }

type CollectionDataReq struct {
	DbName         string                   `json:"dbName"`
	CollectionName string                   `json:"collectionName" binding:"required"`
	PartitionName  string                   `json:"partitionName"`
	Data           []map[string]interface{} `json:"data" binding:"required"`
}

func (req *CollectionDataReq) GetDbName() string { return req.DbName }

type SearchReqV2 struct {
	DbName         string             `json:"dbName"`
	CollectionName string             `json:"collectionName" binding:"required"`
	Vector         [][]float32        `json:"vector"`
	PartitionNames []string           `json:"partitionNames"`
	Filter         string             `json:"filter"`
	GroupByField   string             `json:"groupingField"`
	Limit          int32              `json:"limit"`
	Offset         int32              `json:"offset"`
	OutputFields   []string           `json:"outputFields"`
	Params         map[string]float64 `json:"params"`
}

func (req *SearchReqV2) GetDbName() string { return req.DbName }

type Rand struct {
	Strategy string                 `json:"strategy"`
	Params   map[string]interface{} `json:"params"`
}

type SubSearchReq struct {
	Vector        [][]float32        `json:"vector"`
	AnnsField     string             `json:"annsField"`
	Filter        string             `json:"filter"`
	GroupByField  string             `json:"groupingField"`
	MetricType    string             `json:"metricType"`
	Limit         int32              `json:"limit"`
	Offset        int32              `json:"offset"`
	IgnoreGrowing bool               `json:"ignoreGrowing"`
	Params        map[string]float64 `json:"params"`
}

type HybridSearchReq struct {
	DbName         string         `json:"dbName"`
	CollectionName string         `json:"collectionName" binding:"required"`
	PartitionNames []string       `json:"partitionNames"`
	Search         []SubSearchReq `json:"search"`
	Rerank         Rand           `json:"rerank"`
	Limit          int32          `json:"limit"`
	OutputFields   []string       `json:"outputFields"`
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
type LimitGetter interface {
	GetLimit() int32
}
type FileNamesGetter interface {
	GetFileNames() []string
}
type TaskIDGetter interface {
	GetTaskID() int64
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
	RoleName string `json:"roleName" binding:"required"`
}

func (req *RoleReq) GetRoleName() string {
	return req.RoleName
}

type GrantReq struct {
	RoleName   string `json:"roleName" binding:"required"`
	ObjectType string `json:"objectType" binding:"required"`
	ObjectName string `json:"objectName" binding:"required"`
	Privilege  string `json:"privilege" binding:"required"`
	DbName     string `json:"dbName"`
}

type IndexParam struct {
	FieldName   string            `json:"fieldName" binding:"required"`
	IndexName   string            `json:"indexName" binding:"required"`
	MetricType  string            `json:"metricType" binding:"required"`
	IndexConfig map[string]string `json:"indexConfig"`
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

type FieldSchema struct {
	FieldName         string            `json:"fieldName" binding:"required"`
	DataType          string            `json:"dataType" binding:"required"`
	ElementDataType   string            `json:"elementDataType"`
	IsPrimary         bool              `json:"isPrimary"`
	IsPartitionKey    bool              `json:"isPartitionKey"`
	ElementTypeParams map[string]string `json:"elementTypeParams" binding:"required"`
}

type CollectionSchema struct {
	Fields             []FieldSchema `json:"fields"`
	AutoId             bool          `json:"autoID"`
	EnableDynamicField bool          `json:"enableDynamicField"`
}

type CollectionReq struct {
	DbName         string           `json:"dbName"`
	CollectionName string           `json:"collectionName" binding:"required"`
	Dimension      int32            `json:"dimension"`
	MetricType     string           `json:"metricType"`
	Schema         CollectionSchema `json:"schema"`
	IndexParams    []IndexParam     `json:"indexParams"`
}

func (req *CollectionReq) GetDbName() string { return req.DbName }

type AliasReq struct {
	DbName    string `json:"dbName"`
	AliasName string `json:"aliasName" binding:"required"`
}

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
	return gin.H{HTTPReturnCode: http.StatusOK, HTTPReturnData: gin.H{HTTPReturnHas: has}}
}

func wrapperReturnList(names []string) gin.H {
	if names == nil {
		return gin.H{HTTPReturnCode: http.StatusOK, HTTPReturnData: []string{}}
	}
	return gin.H{HTTPReturnCode: http.StatusOK, HTTPReturnData: names}
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
		return gin.H{HTTPReturnCode: http.StatusOK, HTTPReturnData: gin.H{HTTPReturnRowCount: rowCountValue}}
	}
	return gin.H{HTTPReturnCode: http.StatusOK, HTTPReturnData: gin.H{HTTPReturnRowCount: rowCount}}
}

func wrapperReturnDefault() gin.H {
	return gin.H{HTTPReturnCode: http.StatusOK, HTTPReturnData: gin.H{}}
}
