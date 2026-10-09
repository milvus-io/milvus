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
	"bufio"
	"context"
	"encoding"
	stdjson "encoding/json"
	"errors"
	"net/http"
	"reflect"
	"sort"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/milvus-io/milvus/internal/distributed/proxy/httpserver/requestbudget"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// budgetJSONRender keeps the REST envelope, descends through ordinary maps,
// and encodes array elements as separate Sonic units. A timeout can stop
// between units without changing Sonic. One unit remains non-interruptible;
// the post-encode byte check does not prove a pre-encode capacity bound.
type budgetJSONRender struct {
	Data gin.H
	Ctx  context.Context
}

func renderBudgetJSON(c *gin.Context, status int, data gin.H) {
	c.Status(status)
	err := (budgetJSONRender{Data: data, Ctx: c.Request.Context()}).Render(c.Writer)
	if err == nil {
		return
	}
	if c.Writer.Written() {
		// A partially sent JSON response cannot be replaced by a timeout or
		// capacity-error object. Stop only this response/stream instead.
		_ = http.NewResponseController(c.Writer).SetWriteDeadline(time.Now())
		_ = c.Error(err)
		c.Abort()
		return
	}
	status = http.StatusInternalServerError
	code := merr.Code(err)
	message := err.Error()
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		status = http.StatusRequestTimeout
		code = merr.TimeoutCode
		message = "request timeout"
	}
	c.AbortWithStatusJSON(status, gin.H{"code": code, "message": message})
}

func (r budgetJSONRender) WriteContentType(w http.ResponseWriter) {
	if len(w.Header().Values("Content-Type")) == 0 {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
	}
}

func (r budgetJSONRender) Render(w http.ResponseWriter) error {
	r.WriteContentType(w)
	ctx := r.Ctx
	if ctx == nil {
		ctx = context.Background()
	}
	buffer := bufio.NewWriterSize(w, 32<<10)
	keys := make([]string, 0, len(r.Data))
	for key := range r.Data {
		keys = append(keys, key)
	}
	sort.Strings(keys)
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
		if err := writeBudgetJSONValue(ctx, buffer, reflect.ValueOf(r.Data[key]), 0); err != nil {
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

func writeBudgetJSONValue(ctx context.Context, buffer *bufio.Writer, value reflect.Value, depth int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if depth > 1000 {
		return merr.WrapErrServiceInternalMsg("REST JSON response nesting exceeds 1000 levels")
	}
	for value.IsValid() && (value.Kind() == reflect.Interface || value.Kind() == reflect.Pointer) && !value.IsNil() {
		if budgetCustomMarshaler(value) {
			break
		}
		value = value.Elem()
	}
	if value.IsValid() && !budgetCustomMarshaler(value) {
		switch value.Kind() {
		case reflect.Slice, reflect.Array:
			if value.Kind() != reflect.Slice || value.Type().Elem().Kind() != reflect.Uint8 {
				if value.Kind() == reflect.Slice && value.IsNil() {
					break
				}
				if err := buffer.WriteByte('['); err != nil {
					return err
				}
				for index := 0; index < value.Len(); index++ {
					if index != 0 {
						if err := buffer.WriteByte(','); err != nil {
							return err
						}
					}
					// An array item is the bounded Sonic unit. In particular,
					// encoding an entire row avoids per-scalar calls for vectors.
					if err := writeBudgetJSONLeaf(ctx, buffer, value.Index(index)); err != nil {
						return err
					}
				}
				return buffer.WriteByte(']')
			}
		case reflect.Map:
			if value.Type().Key().Kind() == reflect.String && !value.IsNil() {
				if err := buffer.WriteByte('{'); err != nil {
					return err
				}
				fields := value.MapRange()
				for index := 0; fields.Next(); index++ {
					if err := ctx.Err(); err != nil {
						return err
					}
					key := fields.Key()
					if index != 0 {
						if err := buffer.WriteByte(','); err != nil {
							return err
						}
					}
					name, err := json.Marshal(key.String())
					if err != nil {
						return err
					}
					if _, err := buffer.Write(name); err != nil {
						return err
					}
					if err := buffer.WriteByte(':'); err != nil {
						return err
					}
					if err := writeBudgetJSONValue(ctx, buffer, fields.Value(), depth+1); err != nil {
						return err
					}
				}
				return buffer.WriteByte('}')
			}
		}
	}
	return writeBudgetJSONLeaf(ctx, buffer, value)
}

func writeBudgetJSONLeaf(ctx context.Context, buffer *bufio.Writer, value reflect.Value) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var current any
	if value.IsValid() {
		current = value.Interface()
	}
	encoded, err := json.Marshal(current)
	if err != nil {
		return err
	}
	if len(encoded) > requestbudget.MaxJSONUnitBytes {
		return merr.WrapErrServiceInternalMsg("REST JSON response unit exceeds the %d byte limit", requestbudget.MaxJSONUnitBytes)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err = buffer.Write(encoded)
	return err
}

func budgetCustomMarshaler(value reflect.Value) bool {
	if !value.IsValid() || !value.CanInterface() {
		return false
	}
	current := value.Interface()
	_, jsonMarshaler := current.(stdjson.Marshaler)
	_, textMarshaler := current.(encoding.TextMarshaler)
	return jsonMarshaler || textMarshaler
}
