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

package transformio

import (
	"github.com/apache/arrow/go/v17/arrow"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Validate physical type even when there are no non-null values. Per-value
// range checks alone cannot detect a wrong type in an empty/all-null column.
func validateScalarOutputType(values arrow.Array, expected schemapb.DataType) error {
	if values == nil {
		return merr.WrapErrFunctionFailedMsg("scalar output array is nil")
	}
	id := values.DataType().ID()
	integer := id == arrow.INT8 || id == arrow.INT16 || id == arrow.INT32 || id == arrow.INT64
	ok := false
	switch expected {
	case schemapb.DataType_Bool:
		ok = id == arrow.BOOL
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32, schemapb.DataType_Int64, schemapb.DataType_Timestamptz, schemapb.DataType_Date, schemapb.DataType_Time:
		ok = integer
	case schemapb.DataType_Float, schemapb.DataType_Double:
		ok = id == arrow.FLOAT32 || id == arrow.FLOAT64
	case schemapb.DataType_String, schemapb.DataType_VarChar, schemapb.DataType_Text:
		ok = id == arrow.STRING || id == arrow.LARGE_STRING
	case schemapb.DataType_Decimal, schemapb.DataType_UUID:
		ok = id == arrow.FIXED_SIZE_BINARY || id == arrow.BINARY || id == arrow.LARGE_BINARY
	case schemapb.DataType_Geometry, schemapb.DataType_Mol:
		ok = id == arrow.BINARY || id == arrow.LARGE_BINARY || id == arrow.STRING
	case schemapb.DataType_JSON:
		ok = true // Explicit JSON binding also accepts structured Arrow values.
	case schemapb.DataType_Array:
		_, ok = values.DataType().(arrow.ListLikeType)
	}
	if !ok {
		return typeMismatch(expected, values)
	}
	return nil
}

func validateScalarInputType(field *schemapb.ScalarField, expected schemapb.DataType) error {
	if field == nil || field.GetData() == nil {
		return merr.WrapErrParameterInvalidMsg("scalar payload is missing")
	}
	ok := false
	switch expected {
	case schemapb.DataType_Bool:
		_, ok = field.GetData().(*schemapb.ScalarField_BoolData)
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		_, ok = field.GetData().(*schemapb.ScalarField_IntData)
	case schemapb.DataType_Int64:
		_, ok = field.GetData().(*schemapb.ScalarField_LongData)
	case schemapb.DataType_Timestamptz:
		_, ok = field.GetData().(*schemapb.ScalarField_TimestamptzData)
	case schemapb.DataType_Float:
		_, ok = field.GetData().(*schemapb.ScalarField_FloatData)
	case schemapb.DataType_Double:
		_, ok = field.GetData().(*schemapb.ScalarField_DoubleData)
	case schemapb.DataType_String, schemapb.DataType_VarChar, schemapb.DataType_Text:
		_, ok = field.GetData().(*schemapb.ScalarField_StringData)
	case schemapb.DataType_JSON:
		_, ok = field.GetData().(*schemapb.ScalarField_JsonData)
	case schemapb.DataType_Geometry:
		_, ok = field.GetData().(*schemapb.ScalarField_GeometryData)
	case schemapb.DataType_Mol:
		_, ok = field.GetData().(*schemapb.ScalarField_MolData)
	case schemapb.DataType_Date:
		_, ok = field.GetData().(*schemapb.ScalarField_DateData)
	case schemapb.DataType_Time:
		_, ok = field.GetData().(*schemapb.ScalarField_TimeData)
	case schemapb.DataType_Decimal, schemapb.DataType_UUID:
		_, ok = field.GetData().(*schemapb.ScalarField_BytesData)
	case schemapb.DataType_Array:
		_, ok = field.GetData().(*schemapb.ScalarField_ArrayData)
	}
	if !ok {
		return merr.WrapErrParameterInvalidMsg("scalar payload does not represent %s", expected)
	}
	return nil
}
