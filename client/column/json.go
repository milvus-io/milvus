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

package column

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/client/v2/entity"
)

var _ (Column) = (*ColumnVarChar)(nil)

type ColumnJSONBytes struct {
	*genericColumnBase[[]byte]
	isDynamic bool
}

func NewColumnJSONBytes(name string, values [][]byte) *ColumnJSONBytes {
	return &ColumnJSONBytes{
		genericColumnBase: &genericColumnBase[[]byte]{
			name:      name,
			fieldType: entity.FieldTypeJSON,
			values:    values,
		},
	}
}

func (c *ColumnJSONBytes) Slice(start, end int) Column {
	return &ColumnJSONBytes{
		genericColumnBase: c.genericColumnBase.slice(start, end),
	}
}

func (c *ColumnJSONBytes) WithIsDynamic(isDynamic bool) *ColumnJSONBytes {
	c.isDynamic = isDynamic
	return c
}

func (c *ColumnJSONBytes) FieldData() *schemapb.FieldData {
	fd := c.genericColumnBase.FieldData()
	fd.IsDynamic = c.isDynamic
	return fd
}

// AppendValue append value into column.
func (c *ColumnJSONBytes) AppendValue(i interface{}) error {
	var v []byte
	switch raw := i.(type) {
	case []byte:
		v = raw
	default:
		k := reflect.TypeOf(i).Kind()
		if k == reflect.Ptr {
			k = reflect.TypeOf(i).Elem().Kind()
		}
		switch k {
		case reflect.Struct:
			fallthrough
		case reflect.Map:
			bs, err := json.Marshal(raw)
			if err != nil {
				return err
			}
			v = bs
		default:
			return fmt.Errorf("expect json compatible type([]byte, struct, map), got %T", i)
		}
	}
	c.values = append(c.values, v)

	return nil
}
