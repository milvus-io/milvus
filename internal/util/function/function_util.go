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

package function

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

// Determine whether the column corresponding to outputIDs contains functions, except bm25 function,
// if outputIDs is empty, check all cols
func HasNonBM25Functions(functions []*schemapb.FunctionSchema, outputIDs []int64) bool {
	for _, fSchema := range functions {
		switch fSchema.GetType() {
		case schemapb.FunctionType_BM25:
		case schemapb.FunctionType_Unknown:
		default:
			if len(outputIDs) == 0 {
				return true
			} else {
				for _, id := range outputIDs {
					for _, fOutputID := range fSchema.GetOutputFieldIds() {
						if fOutputID == id {
							return true
						}
					}
				}
			}
		}
	}
	return false
}
