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
	"bufio"
	"context"
	"io"
	"sort"

	"github.com/gin-gonic/gin"

	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// EncodeResponseRows preserves the REST JSON envelope but marshals each data
// row separately. A canceled request can stop before the next Sonic call.
// One row remains a non-interruptible unit, bounded by maxUnitBytes.
func EncodeResponseRows(ctx context.Context, output io.Writer, fields map[string]any, maxUnitBytes int) error {
	if maxUnitBytes <= 0 {
		return merr.WrapErrServiceInternalMsg("invalid REST JSON response unit limit")
	}
	var rows []map[string]any
	switch data := fields["data"].(type) {
	case []map[string]any:
		rows = data
	case []gin.H:
		rows = make([]map[string]any, len(data))
		for i, row := range data {
			rows[i] = map[string]any(row)
		}
	default:
		return merr.WrapErrServiceInternalMsg("REST JSON response data is not a row array")
	}
	keys := make([]string, 0, len(fields))
	for key := range fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	buffer := bufio.NewWriterSize(output, 32<<10)
	if _, err := buffer.WriteString("{"); err != nil {
		return err
	}
	for index, key := range keys {
		if err := ctx.Err(); err != nil {
			return err
		}
		if index != 0 {
			if err := buffer.WriteByte(','); err != nil {
				return err
			}
		}
		name, err := json.Marshal(key)
		if err != nil {
			return err
		}
		if _, err := buffer.Write(name); err != nil {
			return err
		}
		if err := buffer.WriteByte(':'); err != nil {
			return err
		}
		if key == "data" {
			if err := encodeDataRows(ctx, buffer, rows, maxUnitBytes); err != nil {
				return err
			}
			continue
		}
		value, err := json.Marshal(fields[key])
		if err != nil {
			return err
		}
		if len(value) > maxUnitBytes {
			return merr.WrapErrServiceInternalMsg("REST JSON response metadata exceeds %d bytes", maxUnitBytes)
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, err := buffer.Write(value); err != nil {
			return err
		}
	}
	if _, err := buffer.WriteString("}\n"); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return buffer.Flush()
}

func encodeDataRows(ctx context.Context, output *bufio.Writer, rows []map[string]any, maxUnitBytes int) error {
	if err := output.WriteByte('['); err != nil {
		return err
	}
	for index, row := range rows {
		if err := ctx.Err(); err != nil {
			return err
		}
		if index != 0 {
			if err := output.WriteByte(','); err != nil {
				return err
			}
		}
		value, err := json.Marshal(row)
		if err != nil {
			return err
		}
		if len(value) > maxUnitBytes {
			return merr.WrapErrServiceInternalMsg("REST JSON response row exceeds %d bytes", maxUnitBytes)
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, err := output.Write(value); err != nil {
			return err
		}
	}
	return output.WriteByte(']')
}
