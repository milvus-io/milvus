/*
 * # Licensed to the LF AI & Data foundation under one
 * # or more contributor license agreements. See the NOTICE file
 * # distributed with this work for additional information
 * # regarding copyright ownership. The ASF licenses this file
 * # to you under the Apache License, Version 2.0 (the
 * # "License"); you may not use this file except in compliance
 * # with the License. You may obtain a copy of the License at
 * #
 * #     http://www.apache.org/licenses/LICENSE-2.0
 * #
 * # Unless required by applicable law or agreed to in writing, software
 * # distributed under the License is distributed on an "AS IS" BASIS,
 * # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * # See the License for the specific language governing permissions and
 * # limitations under the License.
 */

package rerank

import (
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

// Segment scorer (boost) constants — used by plan_parser_v2.go
const (
	BoostName = "boost"
	FilterKey = "filter"
	WeightKey = "weight"
)

// Internal parameter key
const (
	reranker string = "reranker"
)

// GetRerankName extracts the reranker name from a FunctionSchema.
func GetRerankName(funcSchema *schemapb.FunctionSchema) string {
	for _, param := range funcSchema.Params {
		switch strings.ToLower(param.Key) {
		case reranker:
			return strings.ToLower(param.Value)
		default:
		}
	}
	return ""
}
