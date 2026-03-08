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

type CreateCollectionReq struct {
	DbName             string `json:"dbName"`
	CollectionName     string `json:"collectionName" validate:"required"`
	Dimension          int32  `json:"dimension" validate:"required"`
	Description        string `json:"description"`
	MetricType         string `json:"metricType"`
	PrimaryField       string `json:"primaryField"`
	VectorField        string `json:"vectorField"`
	EnableDynamicField bool   `json:"enableDynamicField"`
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
	PartialUpdate  bool                     `json:"partialUpdate"`
}

type SingleUpsertReq struct {
	DbName         string                 `json:"dbName"`
	CollectionName string                 `json:"collectionName" validate:"required"`
	Data           map[string]interface{} `json:"data" validate:"required"`
	PartialUpdate  bool                   `json:"partialUpdate"`
}

type SearchReq struct {
	DbName         string             `json:"dbName"`
	CollectionName string             `json:"collectionName" validate:"required"`
	Filter         string             `json:"filter"`
	Limit          int32              `json:"limit"`
	Offset         int32              `json:"offset"`
	OutputFields   []string           `json:"outputFields"`
	Vector         []float32          `json:"vector"`
	Params         map[string]float64 `json:"params"`
}
