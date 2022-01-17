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
	"encoding/json"
)

type dummyRequestType struct {
	RequestType string `json:"request_type"`
}

func parseDummyRequestType(str string) (*dummyRequestType, error) {
	drt := &dummyRequestType{}
	if err := json.Unmarshal([]byte(str), &drt); err != nil {
		return nil, err
	}
	return drt, nil
}

type dummyQueryRequest struct {
	RequestType    string   `json:"request_type"`
	DbName         string   `json:"dbname"`
	CollectionName string   `json:"collection_name"`
	PartitionNames []string `json:"partition_names"`
	Expr           string   `json:"expr"`
	OutputFields   []string `json:"output_fields"`
}

func parseDummyQueryRequest(str string) (*dummyQueryRequest, error) {
	dr := &dummyQueryRequest{}

	if err := json.Unmarshal([]byte(str), &dr); err != nil {
		return nil, err
	}
	return dr, nil
}
