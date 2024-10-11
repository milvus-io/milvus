package httpserver

import (
	"strconv"

	"github.com/gin-gonic/gin"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

type DatabaseReq struct {
	DbName string `json:"dbName"`
}

func (req *DatabaseReq) GetDbName() string { return req.DbName }

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
	DbName         string   `json:"dbName"`
	CollectionName string   `json:"collectionName" binding:"required"`
	PartitionNames []string `json:"partitionNames"`
	OutputFields   []string `json:"outputFields"`
	Filter         string   `json:"filter"`
	Limit          int32    `json:"limit"`
	Offset         int32    `json:"offset"`
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
	DbName         string `json:"dbName"`
	CollectionName string `json:"collectionName" binding:"required"`
	PartitionName  string `json:"partitionName"`
	Filter         string `json:"filter" binding:"required"`
}

func (req *CollectionFilterReq) GetDbName() string { return req.DbName }

type CollectionDataReq struct {
	DbName         string                   `json:"dbName"`
	CollectionName string                   `json:"collectionName" binding:"required"`
	PartitionName  string                   `json:"partitionName"`
	Data           []map[string]interface{} `json:"data" binding:"required"`
}

func (req *CollectionDataReq) GetDbName() string { return req.DbName }

type searchParams struct {
	// not use metricType any more, just for compatibility
	MetricType    string                 `json:"metricType"`
	Params        map[string]interface{} `json:"params"`
	IgnoreGrowing bool                   `json:"ignore_growing"`
}

type SearchReqV2 struct {
	DbName           string        `json:"dbName"`
	CollectionName   string        `json:"collectionName" binding:"required"`
	Data             []interface{} `json:"data" binding:"required"`
	AnnsField        string        `json:"annsField"`
	PartitionNames   []string      `json:"partitionNames"`
	Filter           string        `json:"filter"`
	GroupByField     string        `json:"groupingField"`
	GroupSize        int32         `json:"groupSize"`
	GroupStrictSize  bool          `json:"groupStrictSize"`
	Limit            int32         `json:"limit"`
	Offset           int32         `json:"offset"`
	OutputFields     []string      `json:"outputFields"`
	SearchParams     searchParams  `json:"searchParams"`
	ConsistencyLevel string        `json:"consistencyLevel"`
	// not use Params any more, just for compatibility
	Params map[string]float64 `json:"params"`
}

func (req *SearchReqV2) GetDbName() string { return req.DbName }

type Rand struct {
	Strategy string                 `json:"strategy"`
	Params   map[string]interface{} `json:"params"`
}

type SubSearchReq struct {
	Data         []interface{} `json:"data" binding:"required"`
	AnnsField    string        `json:"annsField"`
	Filter       string        `json:"filter"`
	GroupByField string        `json:"groupingField"`
	MetricType   string        `json:"metricType"`
	Limit        int32         `json:"limit"`
	Offset       int32         `json:"offset"`
	SearchParams searchParams  `json:"searchParams"`
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
	GroupStrictSize  bool           `json:"groupStrictSize"`
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
	IndexName  string                 `json:"indexName" binding:"required"`
	MetricType string                 `json:"metricType" binding:"required"`
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

type FieldSchema struct {
	FieldName         string                 `json:"fieldName" binding:"required"`
	DataType          string                 `json:"dataType" binding:"required"`
	ElementDataType   string                 `json:"elementDataType"`
	IsPrimary         bool                   `json:"isPrimary"`
	IsPartitionKey    bool                   `json:"isPartitionKey"`
	ElementTypeParams map[string]interface{} `json:"elementTypeParams" binding:"required"`
	Nullable          bool                   `json:"nullable" binding:"required"`
	DefaultValue      interface{}            `json:"defaultValue" binding:"required"`
}

type CollectionSchema struct {
	Fields             []FieldSchema `json:"fields"`
	AutoId             bool          `json:"autoID"`
	EnableDynamicField bool          `json:"enableDynamicField"`
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
