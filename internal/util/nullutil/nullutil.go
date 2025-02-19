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

package nullutil

import (
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

func CheckValidData(validData []bool, schema *schemapb.FieldSchema, numRows int) error {
	expectedNum := 0
	// if nullable, the length of ValidData is numRows
	if schema.GetNullable() {
		expectedNum = numRows
	}
	if len(validData) != expectedNum {
		msg := fmt.Sprintf("the length of valid_data of field(%s) is wrong", schema.GetName())
		return merr.WrapErrParameterInvalid(expectedNum, len(validData), msg)
	}
	return nil
}

func GetDefaultValue(field *schemapb.FieldSchema) (any, error) {
	if field.GetDefaultValue() != nil {
		switch field.GetDataType() {
		case schemapb.DataType_Bool:
			return field.GetDefaultValue().GetBoolData(), nil
		case schemapb.DataType_Int8:
			return int8(field.GetDefaultValue().GetIntData()), nil
		case schemapb.DataType_Int16:
			return int16(field.GetDefaultValue().GetIntData()), nil
		case schemapb.DataType_Int32:
			return field.GetDefaultValue().GetIntData(), nil
		case schemapb.DataType_Int64:
			return field.GetDefaultValue().GetLongData(), nil
		case schemapb.DataType_Float:
			return field.GetDefaultValue().GetFloatData(), nil
		case schemapb.DataType_Double:
			return field.GetDefaultValue().GetDoubleData(), nil
		case schemapb.DataType_String, schemapb.DataType_VarChar:
			return field.GetDefaultValue().GetStringData(), nil
		default:
			msg := fmt.Sprintf("type (%s) not support default_value", field.GetDataType().String())
			return nil, merr.WrapErrParameterInvalidMsg(msg)
		}
	}
	return nil, nil
}
