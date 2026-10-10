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
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

const budgetDataRowsKey = "budgetDataRows"

// collectionDataRows keeps the legacy body path for routes not using the
// request budget. The budget path never stores a full body in Gin.
func collectionDataRows(c *gin.Context) []gjson.Result {
	if raw, ok := c.Get(budgetDataRowsKey); ok {
		strings := raw.([]string)
		rows := make([]gjson.Result, 0, len(strings))
		for _, row := range strings {
			rows = append(rows, gjson.Parse(row))
		}
		return rows
	}
	body, _ := c.Get(gin.BodyBytesKey)
	return gjson.GetBytes(body.([]byte), HTTPRequestData).Array()
}
