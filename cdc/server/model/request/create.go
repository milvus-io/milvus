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

package request

import "github.com/milvus-io/milvus/cdc/server/model"

//go:generate easytags $GOFILE json,mapstructure

type CreateRequest struct {
	MilvusConnectParam model.MilvusConnectParam `json:"milvus_connect_param" mapstructure:"milvus_connect_param"`
	CollectionInfos    []model.CollectionInfo   `json:"collection_infos" mapstructure:"collection_infos"`
	BufferConfig       model.BufferConfig       `json:"buffer_config" mapstructure:"buffer_config"`
}

type CreateResponse struct {
	TaskID string `json:"task_id" mapstructure:"task_id"`
}
