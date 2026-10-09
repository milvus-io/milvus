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

import (
	"io"

	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"

	"github.com/milvus-io/milvus/internal/distributed/proxy/httpserver/requestbudget"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// bindBudgetJSONUnit bounds a non-bulk request before its single Sonic call.
// The byte cap is a capacity policy, independent of the elapsed-time budget.
func bindBudgetJSONUnit(c *gin.Context, req any, maxUnitBytes int) error {
	if maxUnitBytes <= 0 || maxUnitBytes > requestbudget.MaxJSONUnitBytes {
		return merr.WrapErrServiceInternalMsg("invalid REST JSON unit limit")
	}
	body, err := io.ReadAll(io.LimitReader(c.Request.Body, int64(maxUnitBytes)+1))
	if err != nil {
		return err
	}
	c.Set(gin.BodyBytesKey, body)
	if err := c.Request.Context().Err(); err != nil {
		return err
	}
	if len(body) > maxUnitBytes {
		return merr.WrapErrParameterInvalidMsg("REST JSON request unit exceeds the %d byte limit", maxUnitBytes)
	}
	if err := binding.JSON.BindBody(body, req); err != nil {
		return err
	}
	return c.Request.Context().Err()
}

// bindBudgetBulkRows preserves the original JSON for schema-sensitive row
// conversion, but avoids a single unbounded Sonic decode of a large bulk data
// array. A fast path may decode the whole body only up to one JSON unit.
func bindBudgetBulkRows(c *gin.Context, req any, fastBodyLimit int, assignRows func([]map[string]any)) error {
	if fastBodyLimit <= 0 || fastBodyLimit > requestbudget.MaxJSONUnitBytes {
		return merr.WrapErrServiceInternalMsg("invalid REST bulk fast body limit")
	}
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		return err
	}
	c.Set(gin.BodyBytesKey, body)
	if err := c.Request.Context().Err(); err != nil {
		return err
	}
	if len(body) <= fastBodyLimit {
		if err := binding.JSON.BindBody(body, req); err != nil {
			return err
		}
		return c.Request.Context().Err()
	}
	metadata, rows, err := requestbudget.DecodeDataRows(c.Request.Context(), body, requestbudget.MaxJSONUnitBytes)
	if err != nil {
		return err
	}
	if err := binding.JSON.BindBody(metadata, req); err != nil {
		return err
	}
	assignRows(rows)
	return c.Request.Context().Err()
}
