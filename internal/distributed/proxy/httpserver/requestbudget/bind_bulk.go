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

package requestbudget

import (
	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// BindBulkJSON preserves Gin's metadata validation while leaving the data rows
// raw for the schema-aware conversion that follows. It does not cache a copy
// of the complete request body in gin.BodyBytesKey.
func BindBulkJSON(c *gin.Context, req any, maxUnitBytes, maxBodyBytes int) ([]string, error) {
	if c.Request.ContentLength > int64(maxBodyBytes) {
		return nil, merr.WrapErrParameterTooLarge("REST JSON request body")
	}
	rows := make([]string, 0)
	metadata, err := DecodeBulkJSON(c.Request.Context(), c.Request.Body, maxUnitBytes, maxBodyBytes, func(row []byte) error {
		rows = append(rows, string(row))
		return nil
	})
	if err != nil {
		return nil, err
	}
	if err := c.Request.Context().Err(); err != nil {
		return nil, err
	}
	if err := binding.JSON.BindBody(metadata, req); err != nil {
		return nil, err
	}
	return rows, c.Request.Context().Err()
}
