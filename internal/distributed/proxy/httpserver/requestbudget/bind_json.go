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
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// MaxJSONBodyBytes is a provisional per-request limit for V2 insert/upsert.
// Benchmark and compatibility evidence are required before making it final.
const MaxJSONBodyBytes = 256 << 20

// BindJSONWithLimit retains Gin's ordinary JSON binding and body cache while
// bounding the bytes read from one request.
func BindJSONWithLimit(c *gin.Context, req any, maxBodyBytes int) error {
	if c.Request.ContentLength > int64(maxBodyBytes) {
		return merr.WrapErrParameterTooLarge("REST JSON request body")
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, int64(maxBodyBytes))
	if err := c.ShouldBindBodyWith(req, binding.JSON); err != nil {
		var sizeErr *http.MaxBytesError
		if errors.As(err, &sizeErr) {
			return merr.WrapErrParameterTooLarge("REST JSON request body")
		}
		return err
	}
	return c.Request.Context().Err()
}
