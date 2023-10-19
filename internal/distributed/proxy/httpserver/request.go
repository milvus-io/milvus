package httpserver

type CreateCollectionReq struct {
	DbName         string `json:"dbName"`
	CollectionName string `json:"collectionName" validate:"required"`
	Dimension      int32  `json:"dimension" validate:"required"`
	Description    string `json:"description"`
	MetricType     string `json:"metricType"`
	PrimaryField   string `json:"primaryField"`
	VectorField    string `json:"vectorField"`
}

type DropCollectionReq struct {
	DbName         string `json:"dbName"`
	CollectionName string `json:"collectionName" validate:"required"`
}

type QueryReq struct {
	DbName         string   `json:"dbName"`
	CollectionName string   `json:"collectionName" validate:"required"`
	OutputFields   []string `json:"outputFields"`
	Filter         string   `json:"filter" validate:"required"`
	Limit          int32    `json:"limit"`
	Offset         int32    `json:"offset"`
}

type GetReq struct {
	DbName         string      `json:"dbName"`
	CollectionName string      `json:"collectionName" validate:"required"`
	OutputFields   []string    `json:"outputFields"`
	ID             interface{} `json:"id" validate:"required"`
}

type DeleteReq struct {
	DbName         string      `json:"dbName"`
	CollectionName string      `json:"collectionName" validate:"required"`
	ID             interface{} `json:"id"`
	Filter         string      `json:"filter"`
}

type InsertReq struct {
	DbName         string                   `json:"dbName"`
	CollectionName string                   `json:"collectionName" validate:"required"`
	Data           []map[string]interface{} `json:"data" validate:"required"`
}

type SingleInsertReq struct {
	DbName         string                 `json:"dbName"`
	CollectionName string                 `json:"collectionName" validate:"required"`
	Data           map[string]interface{} `json:"data" validate:"required"`
}

type UpsertReq struct {
	DbName         string                   `json:"dbName"`
	CollectionName string                   `json:"collectionName" validate:"required"`
	Data           []map[string]interface{} `json:"data" validate:"required"`
}

type SingleUpsertReq struct {
	DbName         string                 `json:"dbName"`
	CollectionName string                 `json:"collectionName" validate:"required"`
	Data           map[string]interface{} `json:"data" validate:"required"`
}

type SearchReq struct {
	DbName         string    `json:"dbName"`
	CollectionName string    `json:"collectionName" validate:"required"`
	Filter         string    `json:"filter"`
	Limit          int32     `json:"limit"`
	Offset         int32     `json:"offset"`
	OutputFields   []string  `json:"outputFields"`
	Vector         []float32 `json:"vector"`
}
