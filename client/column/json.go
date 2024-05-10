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

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/client/v2/entity"
)

var _ (Column) = (*ColumnJSONBytes)(nil)

// ColumnJSONBytes column type for JSON.
// all items are marshaled json bytes.
type ColumnJSONBytes struct {
	ColumnBase
	name      string
	values    [][]byte
	isDynamic bool
}

// Name returns column name.
func (c *ColumnJSONBytes) Name() string {
	return c.name
}

// Type returns column entity.FieldType.
func (c *ColumnJSONBytes) Type() entity.FieldType {
	return entity.FieldTypeJSON
}

// Len returns column values length.
func (c *ColumnJSONBytes) Len() int {
	return len(c.values)
}

// Get returns value at index as interface{}.
func (c *ColumnJSONBytes) Get(idx int) (interface{}, error) {
	if idx < 0 || idx > c.Len() {
		return nil, errors.New("index out of range")
	}
	return c.values[idx], nil
}

func (c *ColumnJSONBytes) GetAsString(idx int) (string, error) {
	bs, err := c.ValueByIdx(idx)
	if err != nil {
		return "", err
	}
	return string(bs), nil
}

// FieldData return column data mapped to schemapb.FieldData.
func (c *ColumnJSONBytes) FieldData() *schemapb.FieldData {
	fd := &schemapb.FieldData{
		Type:      schemapb.DataType_JSON,
		FieldName: c.name,
		IsDynamic: c.isDynamic,
	}

	fd.Field = &schemapb.FieldData_Scalars{
		Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_JsonData{
				JsonData: &schemapb.JSONArray{
					Data: c.values,
				},
			},
		},
	}

	return fd
}

// ValueByIdx returns value of the provided index.
func (c *ColumnJSONBytes) ValueByIdx(idx int) ([]byte, error) {
	if idx < 0 || idx >= c.Len() {
		return nil, errors.New("index out of range")
	}
	return c.values[idx], nil
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
			return fmt.Errorf("expect json compatible type([]byte, struct[}, map], got %T)", i)
		}
	}
	c.values = append(c.values, v)

	return nil
}

// Data returns column data.
func (c *ColumnJSONBytes) Data() [][]byte {
	return c.values
}

func (c *ColumnJSONBytes) WithIsDynamic(isDynamic bool) *ColumnJSONBytes {
	c.isDynamic = isDynamic
	return c
}

// NewColumnJSONBytes composes a Column with json bytes.
func NewColumnJSONBytes(name string, values [][]byte) *ColumnJSONBytes {
	return &ColumnJSONBytes{
		name:   name,
		values: values,
	}
}
