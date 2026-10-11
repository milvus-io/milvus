// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package httpserver

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func rejectRESTIngestion(schema *schemapb.CollectionSchema) error {
	if len(schema.GetFunctionChains()) == 0 {
		return nil
	}
	return merr.WrapErrAsInputError(merr.WrapErrOperationNotSupportedMsg("REST insert does not support collection ingestion function chains; use the SDK or gRPC insertion API"))
}

// The typed REST requests do not bind snake_case FunctionChain fields, or
// collection schema chains. Reject those keys explicitly instead of silently
// accepting a collection/search request with its transformation discarded.
func rejectIgnoredRESTChainFields(c *gin.Context, allowCamel bool) error {
	raw, exists := c.Get(gin.BodyBytesKey)
	if !exists {
		return nil
	}
	bytes, ok := raw.([]byte)
	if !ok {
		return merr.WrapErrServiceInternalMsg("REST cached request body is not bytes")
	}
	var root map[string]json.RawMessage
	if err := json.Unmarshal(bytes, &root); err != nil {
		return merr.WrapErrParameterInvalidMsg("REST request body is malformed")
	}
	var inspect func(map[string]json.RawMessage) bool
	inspect = func(object map[string]json.RawMessage) bool {
		if _, exists := object["function_chains"]; exists {
			return true
		}
		if _, exists := object["functionChains"]; exists && !allowCamel {
			return true
		}
		if schema, exists := object["schema"]; exists {
			var nested map[string]json.RawMessage
			if json.Unmarshal(schema, &nested) == nil && inspect(nested) {
				return true
			}
		}
		if search, exists := object["search"]; exists {
			var children []map[string]json.RawMessage
			if json.Unmarshal(search, &children) == nil {
				for _, child := range children {
					if inspect(child) {
						return true
					}
				}
			}
		}
		return false
	}
	if inspect(root) {
		return merr.WrapErrAsInputError(merr.WrapErrOperationNotSupportedMsg("this REST FunctionChain input is unsupported; use the SDK or gRPC API"))
	}
	return nil
}

func abortRESTTransform(c *gin.Context, err error) {
	HTTPAbortReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(err), HTTPReturnMessage: err.Error()})
}
