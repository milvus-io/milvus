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
	"bytes"
	"context"
	"fmt"
	"io"
	"strconv"
	"unicode/utf8"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// queryResponseRows owns column cursors, never the maps returned by Next.
// Setup that scans response-wide data is delayed until Next has a context.
type queryResponseRows struct {
	rowsNum             int64
	cursor              int64
	fields              []*schemapb.FieldData
	ids                 *schemapb.IDs
	scores              []float32
	enableInt64         bool
	schema              *schemapb.CollectionSchema
	pkFieldName         string
	dynamicOutputFields []string
	nativeJSON          bool
	legacyArrayResponse bool
	initialized         bool
	accessors           []*queryResponseFieldCursor
	structAccessors     []*structArrayRowAccessor
}

func newQueryResponseRows(rowsNum int64, needFields []string, fields []*schemapb.FieldData, ids *schemapb.IDs, scores []float32, enableInt64 bool, schema *schemapb.CollectionSchema) (*queryResponseRows, error) {
	if rowsNum < 0 {
		return nil, merr.WrapErrServiceInternalMsg("negative response row count %d", rowsNum)
	}
	if rowsNum == 0 {
		if len(fields) > 0 {
			var err error
			rowsNum, err = fieldDataRowCount(fields[0])
			if err != nil {
				return nil, wrapOutputFieldErr(fields[0].GetFieldName(), err)
			}
		} else if ids != nil {
			switch ids.GetIdField().(type) {
			case *schemapb.IDs_IntId:
				rowsNum = int64(len(ids.GetIntId().GetData()))
			case *schemapb.IDs_StrId:
				rowsNum = int64(len(ids.GetStrId().GetData()))
			default:
				return nil, asSafeMessage(merr.WrapErrParameterInvalidMsg("the type of primary key(id) is not supported, use other sdk please"))
			}
		}
	}
	r := &queryResponseRows{
		rowsNum: rowsNum, fields: fields, ids: ids, scores: scores,
		enableInt64: enableInt64, schema: schema, pkFieldName: DefaultPrimaryFieldName,
		dynamicOutputFields: genDynamicFields(needFields, fields),
		nativeJSON:          paramtable.Get().HTTPCfg.NativeJSONResponse.GetAsBool(),
		legacyArrayResponse: paramtable.Get().HTTPCfg.LegacyArrayResponse.GetAsBool(),
	}
	for _, field := range schema.GetFields() {
		if field.GetIsPrimaryKey() {
			r.pkFieldName = field.GetName()
			break
		}
	}
	return r, nil
}

// Next returns EOF only after the last batch. Batches own their slices and maps.
func (r *queryResponseRows) Next(ctx context.Context, maxRows int) ([]map[string]interface{}, error) {
	if maxRows <= 0 {
		return nil, merr.WrapErrServiceInternalMsg("response batch size must be positive")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if r.cursor >= r.rowsNum {
		return nil, io.EOF
	}
	if !r.initialized {
		if err := r.initialize(ctx); err != nil {
			return nil, err
		}
		r.initialized = true
	}
	count := min(int64(maxRows), r.rowsNum-r.cursor)
	batch := make([]map[string]interface{}, 0, int(count))
	for n := int64(0); n < count; n++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		row, err := r.projectRow(ctx, r.cursor)
		if err != nil {
			return nil, err
		}
		r.cursor++
		batch = append(batch, row)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return batch, nil
}

func (r *queryResponseRows) initialize(ctx context.Context) error {
	r.accessors = make([]*queryResponseFieldCursor, len(r.fields))
	r.structAccessors = make([]*structArrayRowAccessor, len(r.fields))
	for i, field := range r.fields {
		if err := ctx.Err(); err != nil {
			return err
		}
		accessor, err := newQueryResponseFieldCursor(ctx, field)
		if err != nil {
			return wrapOutputFieldErr(field.GetFieldName(), err)
		}
		r.accessors[i] = accessor
		if field.GetType() == schemapb.DataType_ArrayOfStruct {
			r.structAccessors[i], err = newStructArrayRowAccessor(field, r.schema)
			if err != nil {
				return wrapOutputFieldErr(field.GetFieldName(), err)
			}
		}
	}
	if !r.nativeJSON {
		return ctx.Err()
	}
	// Independent cursors inspect selected valid entries without constructing maps.
	// An invalid final row changes earlier batches before the first batch is returned.
	for _, accessor := range r.accessors {
		field := accessor.field
		data, ok := field.GetScalars().GetData().(*schemapb.ScalarField_JsonData)
		if !ok || field.GetIsDynamic() || field.GetType() != schemapb.DataType_JSON {
			continue
		}
		scan := *accessor
		for i := int64(0); i < r.rowsNum; i++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			idx, valid, err := scan.rowIndex(i)
			if err != nil {
				return wrapOutputFieldErr(field.GetFieldName(), err)
			}
			if !valid {
				continue
			}
			raw := data.JsonData.GetData()[idx]
			if !json.Valid(raw) || !utf8.Valid(raw) {
				r.nativeJSON = false
				mlog.Warn(ctx, "a JSON field holds bytes that are not a JSON document, returning JSON fields as strings for this response", mlog.Int64("rows", r.rowsNum))
				return ctx.Err()
			}
		}
	}
	return ctx.Err()
}

// A compact cursor replaces a response-sized index table for top-level columns.
type queryResponseFieldCursor struct {
	field     *schemapb.FieldData
	validData []bool
	compact   bool
	dataIndex int64
}

func newQueryResponseFieldCursor(ctx context.Context, field *schemapb.FieldData) (*queryResponseFieldCursor, error) {
	c := &queryResponseFieldCursor{field: field, validData: typeutil.GetFieldDataValidData(field)}
	if len(c.validData) == 0 {
		return c, nil
	}
	validCount := int64(0)
	for _, valid := range c.validData {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if valid {
			validCount++
		}
	}
	if typeutil.IsCompactNullableVectorFieldData(field) {
		if err := funcutil.ValidateNullableVectorFieldDataCompact(field, uint64(len(c.validData)), true); err != nil {
			return nil, err
		}
		c.compact = true
	} else if validCount == 0 {
		c.compact = true
	} else {
		valueCount, err := fieldDataValueCount(field)
		if err != nil {
			return nil, err
		}
		if valueCount != int64(len(c.validData)) {
			if valueCount != validCount {
				return nil, merr.WrapErrParameterInvalidMsg("field %s has %d valid rows, but data length is %d", field.GetFieldName(), validCount, valueCount)
			}
			c.compact = true
		}
	}
	return c, ctx.Err()
}

func (c *queryResponseFieldCursor) rowIndex(row int64) (int64, bool, error) {
	if len(c.validData) == 0 {
		return row, true, nil
	}
	if row >= int64(len(c.validData)) {
		return 0, false, merr.WrapErrParameterInvalidMsg("row index %d out of range for field %s valid data length %d", row, c.field.GetFieldName(), len(c.validData))
	}
	if !c.validData[row] {
		return 0, false, nil
	}
	if !c.compact {
		return row, true, nil
	}
	idx := c.dataIndex
	c.dataIndex++
	return idx, true, nil
}

// projectRow shares the existing REST conversion with the compatibility builder.
func (r *queryResponseRows) projectRow(ctx context.Context, i int64) (map[string]interface{}, error) {
	fieldDataList, fieldDataAccessors, structArrayAccessors := r.fields, r.accessors, r.structAccessors
	columnNum := len(fieldDataList)
	enableInt64, nativeJSON, legacyArrayResponse := r.enableInt64, r.nativeJSON, r.legacyArrayResponse
	dynamicOutputFields, pkFieldName, ids, scores := r.dynamicOutputFields, r.pkFieldName, r.ids, r.scores
	row := map[string]interface{}{}
	if columnNum > 0 {
		for j := 0; j < columnNum; j++ {
			fieldData := fieldDataList[j]
			dataIdx, valid, err := fieldDataAccessors[j].rowIndex(i)
			if err != nil {
				return nil, wrapOutputFieldErr(fieldData.GetFieldName(), err)
			}
			if !valid {
				if !fieldData.GetIsDynamic() {
					row[fieldData.GetFieldName()] = nil
				}
				continue
			}
			switch fieldDataList[j].GetType() {
			case schemapb.DataType_Bool:
				row[fieldDataList[j].GetFieldName()] = fieldDataList[j].GetScalars().GetBoolData().GetData()[dataIdx]
			case schemapb.DataType_Int8:
				row[fieldDataList[j].GetFieldName()] = int8(fieldDataList[j].GetScalars().GetIntData().GetData()[dataIdx])
			case schemapb.DataType_Int16:
				row[fieldDataList[j].GetFieldName()] = int16(fieldDataList[j].GetScalars().GetIntData().GetData()[dataIdx])
			case schemapb.DataType_Int32:
				row[fieldDataList[j].GetFieldName()] = fieldDataList[j].GetScalars().GetIntData().GetData()[dataIdx]
			case schemapb.DataType_Int64:
				if enableInt64 {
					row[fieldDataList[j].GetFieldName()] = fieldDataList[j].GetScalars().GetLongData().GetData()[dataIdx]
				} else {
					row[fieldDataList[j].GetFieldName()] = strconv.FormatInt(fieldDataList[j].GetScalars().GetLongData().GetData()[dataIdx], 10)
				}
			case schemapb.DataType_Float:
				row[fieldDataList[j].GetFieldName()] = fieldDataList[j].GetScalars().GetFloatData().GetData()[dataIdx]
			case schemapb.DataType_Double:
				row[fieldDataList[j].GetFieldName()] = fieldDataList[j].GetScalars().GetDoubleData().GetData()[dataIdx]
			case schemapb.DataType_Timestamptz:
				if fieldDataList[j].GetScalars().GetTimestamptzData() != nil {
					row[fieldDataList[j].FieldName] = fieldDataList[j].GetScalars().GetTimestamptzData().GetData()[dataIdx]
				} else {
					row[fieldDataList[j].FieldName] = fieldDataList[j].GetScalars().GetStringData().GetData()[dataIdx]
				}
			case schemapb.DataType_String, schemapb.DataType_VarChar, schemapb.DataType_Text:
				row[fieldDataList[j].GetFieldName()] = fieldDataList[j].GetScalars().GetStringData().GetData()[dataIdx]
			case schemapb.DataType_BinaryVector:
				row[fieldDataList[j].GetFieldName()] = fieldDataList[j].GetVectors().GetBinaryVector()[dataIdx*(fieldDataList[j].GetVectors().GetDim()/8) : (dataIdx+1)*(fieldDataList[j].GetVectors().GetDim()/8)]
			case schemapb.DataType_FloatVector:
				row[fieldDataList[j].GetFieldName()] = fieldDataList[j].GetVectors().GetFloatVector().GetData()[dataIdx*fieldDataList[j].GetVectors().GetDim() : (dataIdx+1)*fieldDataList[j].GetVectors().GetDim()]
			case schemapb.DataType_Float16Vector:
				row[fieldDataList[j].GetFieldName()] = fieldDataList[j].GetVectors().GetFloat16Vector()[dataIdx*(fieldDataList[j].GetVectors().GetDim()*2) : (dataIdx+1)*(fieldDataList[j].GetVectors().GetDim()*2)]
			case schemapb.DataType_BFloat16Vector:
				row[fieldDataList[j].GetFieldName()] = fieldDataList[j].GetVectors().GetBfloat16Vector()[dataIdx*(fieldDataList[j].GetVectors().GetDim()*2) : (dataIdx+1)*(fieldDataList[j].GetVectors().GetDim()*2)]
			case schemapb.DataType_SparseFloatVector:
				row[fieldDataList[j].GetFieldName()] = typeutil.SparseFloatBytesToMap(fieldDataList[j].GetVectors().GetSparseFloatVector().Contents[dataIdx])
			case schemapb.DataType_Int8Vector:
				row[fieldDataList[j].GetFieldName()] = fieldDataList[j].GetVectors().GetInt8Vector()[dataIdx*fieldDataList[j].GetVectors().GetDim() : (dataIdx+1)*fieldDataList[j].GetVectors().GetDim()]
			case schemapb.DataType_Array:
				arrayRow := fieldDataList[j].GetScalars().GetArrayData().GetData()[dataIdx]
				if legacyArrayResponse {
					// Escape hatch: emit the raw ScalarField so it serializes back into
					// the protobuf wrapper shape clients may have parsed before the fix.
					// The shape is what those clients read; Accept-Type-Allow-Int64 is
					// about what their JSON parser can hold, which the shape does not
					// change, so an Int64 is still rendered the way the header asks.
					row[fieldDataList[j].GetFieldName()] = legacyArrayValue(arrayRow, enableInt64)
					continue
				}
				value, err := scalarFieldToRESTAny(arrayRow, enableInt64)
				if err != nil {
					return nil, wrapOutputFieldErr(fieldData.GetFieldName(), merr.Wrapf(err, "row %d", i))
				}
				row[fieldDataList[j].GetFieldName()] = value
			case schemapb.DataType_JSON:
				data, ok := fieldDataList[j].GetScalars().GetData().(*schemapb.ScalarField_JsonData)
				if ok && !fieldDataList[j].GetIsDynamic() {
					// A JSON field reads back as a string by default, which
					// is why the same value in a dynamic field reads back as a
					// document. proxy.http.nativeJSONResponse returns the
					// document instead. The response preflight selects strings
					// for every JSON field if any selected row is invalid.
					raw := data.JsonData.GetData()[dataIdx]
					if nativeJSON {
						row[fieldDataList[j].GetFieldName()] = json.RawMessage(raw)
					} else {
						row[fieldDataList[j].GetFieldName()] = string(raw)
					}
				} else {
					var dataMap map[string]interface{}

					// Decode with UseNumber so numeric dynamic-field values are kept as
					// json.Number instead of float64. float64 only has a 53-bit mantissa,
					// so integers larger than 2^53 (e.g. 9223372036854775807) silently lose
					// precision when round-tripped through the REST response. json.Number
					// preserves the exact digits and serializes back as the same integer.
					raw := fieldDataList[j].GetScalars().GetJsonData().Data[dataIdx]

					// A Decoder reads one value and ignores whatever follows
					// it, where the Unmarshal this replaced rejected trailing
					// content. A second Decode on the same decoder restores
					// that in the same pass: only whitespace to the end
					// answers io.EOF, anything else answers a value or an
					// error. An earlier version ran json.Valid first, which
					// read every row twice; the ways to avoid the second
					// call are all closed -- Token is not in sonic's
					// Decoder, which this build uses everywhere, More
					// answers false at a closing bracket because it exists
					// to iterate inside one, and Buffered only exposes the
					// window the decoder happened to pre-read, so a second
					// document past four kilobytes of padding sat outside
					// it and was silently dropped.
					decoder := json.NewDecoder(bytes.NewReader(raw))
					decoder.UseNumber()
					err := decoder.Decode(&dataMap)
					if err == nil {
						var trailing interface{}
						if trailingErr := decoder.Decode(&trailing); trailingErr != io.EOF {
							err = merr.WrapErrParameterInvalidMsg(
								"dynamic field does not hold a single JSON document")
						}
					}
					if err != nil {
						mlog.Error(ctx,
							fmt.Sprintf("[BuildQueryResp] Unmarshal error %s", err.Error()))
						// This branch is only entered for the dynamic field, so the
						// name here is always the internal $meta -- never one of the
						// caller's outputFields -- which makes wrapOutputFieldErr's
						// contract not hold and would report a column the caller
						// cannot drop. What it can act on is the cause: the stored
						// bytes are not one JSON document. That describes its own
						// data, not our layout, so echo it and keep the decoder's
						// text (which can quote the bytes and an offset into them)
						// in the log line above.
						return nil, asSafeMessage(merr.WrapErrParameterInvalidMsg(
							"dynamic field does not hold a single JSON document"))
					}

					if containsString(dynamicOutputFields, fieldDataList[j].GetFieldName()) {
						for key, value := range dataMap {
							row[key] = value
						}
					} else {
						for _, dynamicField := range dynamicOutputFields {
							if _, ok := dataMap[dynamicField]; ok {
								row[dynamicField] = dataMap[dynamicField]
							}
						}
					}
				}
			case schemapb.DataType_Geometry:
				if fieldDataList[j].GetScalars().GetGeometryData() != nil {
					row[fieldDataList[j].FieldName] = fieldDataList[j].GetScalars().GetGeometryData().GetData()[dataIdx]
				} else {
					row[fieldDataList[j].FieldName] = fieldDataList[j].GetScalars().GetGeometryWktData().Data[dataIdx]
				}
			case schemapb.DataType_ArrayOfStruct:
				structRow, err := structArrayAccessors[j].row(int(dataIdx), enableInt64)
				if err != nil {
					return nil, wrapOutputFieldErr(fieldData.GetFieldName(), err)
				}
				row[fieldDataList[j].GetFieldName()] = structRow
			default:
				row[fieldDataList[j].GetFieldName()] = ""
			}
		}
	}
	if ids != nil {
		switch ids.GetIdField().(type) {
		case *schemapb.IDs_IntId:
			int64Pks := ids.GetIntId().GetData()
			if enableInt64 {
				row[pkFieldName] = int64Pks[i]
			} else {
				row[pkFieldName] = strconv.FormatInt(int64Pks[i], 10)
			}
		case *schemapb.IDs_StrId:
			stringPks := ids.GetStrId().GetData()
			row[pkFieldName] = stringPks[i]
		default:
			return nil, asSafeMessage(merr.WrapErrParameterInvalidMsg("the type of primary key(id) is not supported, use other sdk please"))
		}
	}
	if scores != nil && int64(len(scores)) > i {
		row[HTTPReturnDistance] = scores[i] // only 8 decimal places
	}

	return row, nil
}
