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

package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sort"
	"strconv"

	"github.com/cockroachdb/errors"
	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

//////////////////////////////////////////////////////////////////////////////////////////////////

func checkTsField(data *InsertData) bool {
	tsData, ok := data.Data[common.TimeStampField]
	if !ok {
		return false
	}

	_, ok = tsData.(*Int64FieldData)
	return ok
}

func checkRowIDField(data *InsertData) bool {
	rowIDData, ok := data.Data[common.RowIDField]
	if !ok {
		return false
	}

	_, ok = rowIDData.(*Int64FieldData)
	return ok
}

func checkNumRows(fieldDatas ...FieldData) bool {
	if len(fieldDatas) <= 0 {
		return true
	}

	numRows := fieldDatas[0].RowNum()
	for i := 1; i < len(fieldDatas); i++ {
		if numRows != fieldDatas[i].RowNum() {
			return false
		}
	}

	return true
}

type fieldDataList struct {
	IDs   []FieldID
	datas []FieldData
}

func (ls fieldDataList) Len() int {
	return len(ls.IDs)
}

func (ls fieldDataList) Less(i, j int) bool {
	return ls.IDs[i] < ls.IDs[j]
}

func (ls fieldDataList) Swap(i, j int) {
	ls.IDs[i], ls.IDs[j] = ls.IDs[j], ls.IDs[i]
	ls.datas[i], ls.datas[j] = ls.datas[j], ls.datas[i]
}

func sortFieldDataList(ls fieldDataList) {
	sort.Sort(ls)
}

// TransferColumnBasedInsertDataToRowBased transfer column-based insert data to row-based rows.
// Note:
//   - ts column must exist in insert data;
//   - row id column must exist in insert data;
//   - the row num of all column must be equal;
//   - num_rows = len(RowData), a row will be assembled into the value of blob with field id order;
func TransferColumnBasedInsertDataToRowBased(data *InsertData) (
	Timestamps []uint64,
	RowIDs []int64,
	RowData []*commonpb.Blob,
	err error,
) {
	if !checkTsField(data) {
		return nil, nil, nil,
			errors.New("cannot get timestamps from insert data")
	}

	if !checkRowIDField(data) {
		return nil, nil, nil,
			errors.New("cannot get row ids from insert data")
	}

	tss := data.Data[common.TimeStampField].(*Int64FieldData)
	rowIds := data.Data[common.RowIDField].(*Int64FieldData)

	ls := fieldDataList{}
	for fieldID := range data.Data {
		if fieldID == common.TimeStampField || fieldID == common.RowIDField {
			continue
		}

		ls.IDs = append(ls.IDs, fieldID)
		ls.datas = append(ls.datas, data.Data[fieldID])
	}

	// checkNumRows(tss, rowIds, ls.datas...) // don't work
	all := []FieldData{tss, rowIds}
	all = append(all, ls.datas...)
	if !checkNumRows(all...) {
		return nil, nil, nil,
			errors.New("columns of insert data have different length")
	}

	sortFieldDataList(ls)

	numRows := tss.RowNum()
	rows := make([]*commonpb.Blob, numRows)
	for i := 0; i < numRows; i++ {
		blob := &commonpb.Blob{}
		var buffer bytes.Buffer

		for j := 0; j < ls.Len(); j++ {
			d := ls.datas[j].GetRow(i)
			err := binary.Write(&buffer, common.Endian, d)
			if err != nil {
				return nil, nil, nil,
					fmt.Errorf("failed to get binary row, err: %v", err)
			}
		}

		blob.Value = buffer.Bytes()
		rows[i] = blob
	}

	utss := make([]uint64, tss.RowNum())
	for i := 0; i < tss.RowNum(); i++ {
		utss[i] = uint64(tss.Data[i])
	}

	return utss, rowIds.Data, rows, nil
}

///////////////////////////////////////////////////////////////////////////////////////////

// TODO: remove these functions to proper file.

// GetDimFromParams get dim from params.
func GetDimFromParams(params []*commonpb.KeyValuePair) (int, error) {
	var dim int
	var err error
	for _, t := range params {
		if t.Key == common.DimKey {
			dim, err = strconv.Atoi(t.Value)
			if err != nil {
				return -1, err
			}
			return dim, nil
		}
	}
	return -1, errors.New("dim not found in params")
}

// ReadBinary read data in bytes and write it into receiver.
//
//	The receiver can be any type in int8, int16, int32, int64, float32, float64 and bool
//	ReadBinary uses LittleEndian ByteOrder.
func ReadBinary(reader io.Reader, receiver interface{}, dataType schemapb.DataType) {
	err := binary.Read(reader, common.Endian, receiver)
	if err != nil {
		log.Error("binary.Read failed", zap.Any("data type", dataType), zap.Error(err))
	}
}

// It will save my life if golang support generic programming.

// TODO: string type.

func readFloatVectors(blobReaders []io.Reader, dim int) []float32 {
	ret := make([]float32, 0)
	for _, r := range blobReaders {
		v := make([]float32, dim)
		ReadBinary(r, &v, schemapb.DataType_FloatVector)
		ret = append(ret, v...)
	}
	return ret
}

func readBinaryVectors(blobReaders []io.Reader, dim int) []byte {
	ret := make([]byte, 0)
	for _, r := range blobReaders {
		v := make([]byte, dim/8)
		ReadBinary(r, &v, schemapb.DataType_BinaryVector)
		ret = append(ret, v...)
	}
	return ret
}

func readFloat16Vectors(blobReaders []io.Reader, dim int) []byte {
	ret := make([]byte, 0)
	for _, r := range blobReaders {
		v := make([]byte, dim*2)
		ReadBinary(r, &v, schemapb.DataType_Float16Vector)
		ret = append(ret, v...)
	}
	return ret
}

func readBoolArray(blobReaders []io.Reader) []bool {
	ret := make([]bool, 0)
	for _, r := range blobReaders {
		var v bool
		ReadBinary(r, &v, schemapb.DataType_Bool)
		ret = append(ret, v)
	}
	return ret
}

func readInt8Array(blobReaders []io.Reader) []int8 {
	ret := make([]int8, 0)
	for _, r := range blobReaders {
		var v int8
		ReadBinary(r, &v, schemapb.DataType_Int8)
		ret = append(ret, v)
	}
	return ret
}

func readInt16Array(blobReaders []io.Reader) []int16 {
	ret := make([]int16, 0)
	for _, r := range blobReaders {
		var v int16
		ReadBinary(r, &v, schemapb.DataType_Int16)
		ret = append(ret, v)
	}
	return ret
}

func readInt32Array(blobReaders []io.Reader) []int32 {
	ret := make([]int32, 0)
	for _, r := range blobReaders {
		var v int32
		ReadBinary(r, &v, schemapb.DataType_Int32)
		ret = append(ret, v)
	}
	return ret
}

func readInt64Array(blobReaders []io.Reader) []int64 {
	ret := make([]int64, 0)
	for _, r := range blobReaders {
		var v int64
		ReadBinary(r, &v, schemapb.DataType_Int64)
		ret = append(ret, v)
	}
	return ret
}

func readFloatArray(blobReaders []io.Reader) []float32 {
	ret := make([]float32, 0)
	for _, r := range blobReaders {
		var v float32
		ReadBinary(r, &v, schemapb.DataType_Float)
		ret = append(ret, v)
	}
	return ret
}

func readDoubleArray(blobReaders []io.Reader) []float64 {
	ret := make([]float64, 0)
	for _, r := range blobReaders {
		var v float64
		ReadBinary(r, &v, schemapb.DataType_Double)
		ret = append(ret, v)
	}
	return ret
}

func RowBasedInsertMsgToInsertData(msg *msgstream.InsertMsg, collSchema *schemapb.CollectionSchema) (idata *InsertData, err error) {
	blobReaders := make([]io.Reader, 0)
	for _, blob := range msg.RowData {
		blobReaders = append(blobReaders, bytes.NewReader(blob.GetValue()))
	}

	idata = &InsertData{
		Data: make(map[FieldID]FieldData),
		// TODO: handle Infos.
		Infos: nil,
	}

	for _, field := range collSchema.Fields {
		switch field.DataType {
		case schemapb.DataType_FloatVector:
			dim, err := GetDimFromParams(field.TypeParams)
			if err != nil {
				log.Error("failed to get dim", zap.Error(err))
				return nil, err
			}

			vecs := readFloatVectors(blobReaders, dim)
			idata.Data[field.FieldID] = &FloatVectorFieldData{
				Data: vecs,
				Dim:  dim,
			}

		case schemapb.DataType_Float16Vector:
			dim, err := GetDimFromParams(field.TypeParams)
			if err != nil {
				log.Error("failed to get dim", zap.Error(err))
				return nil, err
			}

			vecs := readFloat16Vectors(blobReaders, dim)
			idata.Data[field.FieldID] = &Float16VectorFieldData{
				Data: vecs,
				Dim:  dim,
			}

		case schemapb.DataType_BinaryVector:
			var dim int
			dim, err := GetDimFromParams(field.TypeParams)
			if err != nil {
				log.Error("failed to get dim", zap.Error(err))
				return nil, err
			}

			vecs := readBinaryVectors(blobReaders, dim)
			idata.Data[field.FieldID] = &BinaryVectorFieldData{
				Data: vecs,
				Dim:  dim,
			}

		case schemapb.DataType_Bool:
			idata.Data[field.FieldID] = &BoolFieldData{
				Data: readBoolArray(blobReaders),
			}

		case schemapb.DataType_Int8:
			idata.Data[field.FieldID] = &Int8FieldData{
				Data: readInt8Array(blobReaders),
			}

		case schemapb.DataType_Int16:
			idata.Data[field.FieldID] = &Int16FieldData{
				Data: readInt16Array(blobReaders),
			}

		case schemapb.DataType_Int32:
			idata.Data[field.FieldID] = &Int32FieldData{
				Data: readInt32Array(blobReaders),
			}

		case schemapb.DataType_Int64:
			idata.Data[field.FieldID] = &Int64FieldData{
				Data: nil,
			}

			fieldData := idata.Data[field.FieldID].(*Int64FieldData)
			switch field.FieldID {
			case 0: // rowIDs
				fieldData.Data = append(fieldData.Data, msg.RowIDs...)
			case 1: // Timestamps
				for _, ts := range msg.Timestamps {
					fieldData.Data = append(fieldData.Data, int64(ts))
				}
			default:
				fieldData.Data = readInt64Array(blobReaders)
			}

		case schemapb.DataType_Float:
			idata.Data[field.FieldID] = &FloatFieldData{
				Data: readFloatArray(blobReaders),
			}

		case schemapb.DataType_Double:
			idata.Data[field.FieldID] = &DoubleFieldData{
				Data: readDoubleArray(blobReaders),
			}
		}
	}

	return idata, nil
}

func ColumnBasedInsertMsgToInsertData(msg *msgstream.InsertMsg, collSchema *schemapb.CollectionSchema) (idata *InsertData, err error) {
	srcFields := make(map[FieldID]*schemapb.FieldData)
	for _, field := range msg.FieldsData {
		srcFields[field.FieldId] = field
	}

	idata = &InsertData{
		Data: make(map[FieldID]FieldData),
		// TODO: handle Infos.
		Infos: nil,
	}

	for _, field := range collSchema.Fields {
		switch field.DataType {
		case schemapb.DataType_FloatVector:
			dim, err := GetDimFromParams(field.TypeParams)
			if err != nil {
				log.Error("failed to get dim", zap.Error(err))
				return nil, err
			}

			srcData := srcFields[field.FieldID].GetVectors().GetFloatVector().GetData()

			fieldData := &FloatVectorFieldData{
				Data: make([]float32, 0, len(srcData)),
				Dim:  dim,
			}
			fieldData.Data = append(fieldData.Data, srcData...)

			idata.Data[field.FieldID] = fieldData

		case schemapb.DataType_BinaryVector:
			dim, err := GetDimFromParams(field.TypeParams)
			if err != nil {
				log.Error("failed to get dim", zap.Error(err))
				return nil, err
			}

			srcData := srcFields[field.FieldID].GetVectors().GetBinaryVector()

			fieldData := &BinaryVectorFieldData{
				Data: make([]byte, 0, len(srcData)),
				Dim:  dim,
			}
			fieldData.Data = append(fieldData.Data, srcData...)

			idata.Data[field.FieldID] = fieldData

		case schemapb.DataType_Float16Vector:
			dim, err := GetDimFromParams(field.TypeParams)
			if err != nil {
				log.Error("failed to get dim", zap.Error(err))
				return nil, err
			}

			srcData := srcFields[field.FieldID].GetVectors().GetFloat16Vector()

			fieldData := &Float16VectorFieldData{
				Data: make([]byte, 0, len(srcData)),
				Dim:  dim,
			}
			fieldData.Data = append(fieldData.Data, srcData...)

			idata.Data[field.FieldID] = fieldData

		case schemapb.DataType_Bool:
			srcData := srcFields[field.FieldID].GetScalars().GetBoolData().GetData()

			fieldData := &BoolFieldData{
				Data: make([]bool, 0, len(srcData)),
			}
			fieldData.Data = append(fieldData.Data, srcData...)

			idata.Data[field.FieldID] = fieldData

		case schemapb.DataType_Int8:
			srcData := srcFields[field.FieldID].GetScalars().GetIntData().GetData()

			fieldData := &Int8FieldData{
				Data: make([]int8, 0, len(srcData)),
			}
			int8SrcData := make([]int8, len(srcData))
			for i := 0; i < len(srcData); i++ {
				int8SrcData[i] = int8(srcData[i])
			}
			fieldData.Data = append(fieldData.Data, int8SrcData...)

			idata.Data[field.FieldID] = fieldData

		case schemapb.DataType_Int16:
			srcData := srcFields[field.FieldID].GetScalars().GetIntData().GetData()

			fieldData := &Int16FieldData{
				Data: make([]int16, 0, len(srcData)),
			}
			int16SrcData := make([]int16, len(srcData))
			for i := 0; i < len(srcData); i++ {
				int16SrcData[i] = int16(srcData[i])
			}
			fieldData.Data = append(fieldData.Data, int16SrcData...)

			idata.Data[field.FieldID] = fieldData

		case schemapb.DataType_Int32:
			srcData := srcFields[field.FieldID].GetScalars().GetIntData().GetData()

			fieldData := &Int32FieldData{
				Data: make([]int32, 0, len(srcData)),
			}
			fieldData.Data = append(fieldData.Data, srcData...)

			idata.Data[field.FieldID] = fieldData

		case schemapb.DataType_Int64:
			fieldData := &Int64FieldData{
				Data: make([]int64, 0),
			}

			switch field.FieldID {
			case 0: // rowIDs
				fieldData.Data = make([]int64, 0, len(msg.RowIDs))
				fieldData.Data = append(fieldData.Data, msg.RowIDs...)
			case 1: // Timestamps
				fieldData.Data = make([]int64, 0, len(msg.Timestamps))
				for _, ts := range msg.Timestamps {
					fieldData.Data = append(fieldData.Data, int64(ts))
				}
			default:
				srcData := srcFields[field.FieldID].GetScalars().GetLongData().GetData()
				fieldData.Data = make([]int64, 0, len(srcData))
				fieldData.Data = append(fieldData.Data, srcData...)
			}

			idata.Data[field.FieldID] = fieldData

		case schemapb.DataType_Float:
			srcData := srcFields[field.FieldID].GetScalars().GetFloatData().GetData()

			fieldData := &FloatFieldData{
				Data: make([]float32, 0, len(srcData)),
			}
			fieldData.Data = append(fieldData.Data, srcData...)

			idata.Data[field.FieldID] = fieldData

		case schemapb.DataType_Double:
			srcData := srcFields[field.FieldID].GetScalars().GetDoubleData().GetData()

			fieldData := &DoubleFieldData{
				Data: make([]float64, 0, len(srcData)),
			}
			fieldData.Data = append(fieldData.Data, srcData...)

			idata.Data[field.FieldID] = fieldData
		case schemapb.DataType_String, schemapb.DataType_VarChar:
			srcData := srcFields[field.FieldID].GetScalars().GetStringData().GetData()

			fieldData := &StringFieldData{
				Data: make([]string, 0, len(srcData)),
			}

			fieldData.Data = append(fieldData.Data, srcData...)
			idata.Data[field.FieldID] = fieldData
		case schemapb.DataType_Array:
			srcData := srcFields[field.FieldID].GetScalars().GetArrayData().GetData()

			fieldData := &ArrayFieldData{
				ElementType: field.GetElementType(),
				Data:        make([]*schemapb.ScalarField, 0, len(srcData)),
			}

			fieldData.Data = append(fieldData.Data, srcData...)
			idata.Data[field.FieldID] = fieldData
		case schemapb.DataType_JSON:
			srcData := srcFields[field.FieldID].GetScalars().GetJsonData().GetData()

			fieldData := &JSONFieldData{
				Data: make([][]byte, 0, len(srcData)),
			}

			fieldData.Data = append(fieldData.Data, srcData...)
			idata.Data[field.FieldID] = fieldData
		}
	}

	return idata, nil
}

func InsertMsgToInsertData(msg *msgstream.InsertMsg, schema *schemapb.CollectionSchema) (idata *InsertData, err error) {
	if msg.IsRowBased() {
		return RowBasedInsertMsgToInsertData(msg, schema)
	}
	return ColumnBasedInsertMsgToInsertData(msg, schema)
}

func mergeBoolField(data *InsertData, fid FieldID, field *BoolFieldData) {
	if _, ok := data.Data[fid]; !ok {
		fieldData := &BoolFieldData{
			Data: nil,
		}
		data.Data[fid] = fieldData
	}
	fieldData := data.Data[fid].(*BoolFieldData)
	fieldData.Data = append(fieldData.Data, field.Data...)
}

func mergeInt8Field(data *InsertData, fid FieldID, field *Int8FieldData) {
	if _, ok := data.Data[fid]; !ok {
		fieldData := &Int8FieldData{
			Data: nil,
		}
		data.Data[fid] = fieldData
	}
	fieldData := data.Data[fid].(*Int8FieldData)
	fieldData.Data = append(fieldData.Data, field.Data...)
}

func mergeInt16Field(data *InsertData, fid FieldID, field *Int16FieldData) {
	if _, ok := data.Data[fid]; !ok {
		fieldData := &Int16FieldData{
			Data: nil,
		}
		data.Data[fid] = fieldData
	}
	fieldData := data.Data[fid].(*Int16FieldData)
	fieldData.Data = append(fieldData.Data, field.Data...)
}

func mergeInt32Field(data *InsertData, fid FieldID, field *Int32FieldData) {
	if _, ok := data.Data[fid]; !ok {
		fieldData := &Int32FieldData{
			Data: nil,
		}
		data.Data[fid] = fieldData
	}
	fieldData := data.Data[fid].(*Int32FieldData)
	fieldData.Data = append(fieldData.Data, field.Data...)
}

func mergeInt64Field(data *InsertData, fid FieldID, field *Int64FieldData) {
	if _, ok := data.Data[fid]; !ok {
		fieldData := &Int64FieldData{
			Data: nil,
		}
		data.Data[fid] = fieldData
	}
	fieldData := data.Data[fid].(*Int64FieldData)
	fieldData.Data = append(fieldData.Data, field.Data...)
}

func mergeFloatField(data *InsertData, fid FieldID, field *FloatFieldData) {
	if _, ok := data.Data[fid]; !ok {
		fieldData := &FloatFieldData{
			Data: nil,
		}
		data.Data[fid] = fieldData
	}
	fieldData := data.Data[fid].(*FloatFieldData)
	fieldData.Data = append(fieldData.Data, field.Data...)
}

func mergeDoubleField(data *InsertData, fid FieldID, field *DoubleFieldData) {
	if _, ok := data.Data[fid]; !ok {
		fieldData := &DoubleFieldData{
			Data: nil,
		}
		data.Data[fid] = fieldData
	}
	fieldData := data.Data[fid].(*DoubleFieldData)
	fieldData.Data = append(fieldData.Data, field.Data...)
}

func mergeStringField(data *InsertData, fid FieldID, field *StringFieldData) {
	if _, ok := data.Data[fid]; !ok {
		fieldData := &StringFieldData{
			Data: nil,
		}
		data.Data[fid] = fieldData
	}
	fieldData := data.Data[fid].(*StringFieldData)
	fieldData.Data = append(fieldData.Data, field.Data...)
}

func mergeArrayField(data *InsertData, fid FieldID, field *ArrayFieldData) {
	if _, ok := data.Data[fid]; !ok {
		fieldData := &ArrayFieldData{
			ElementType: field.ElementType,
			Data:        nil,
		}
		data.Data[fid] = fieldData
	}
	fieldData := data.Data[fid].(*ArrayFieldData)
	fieldData.Data = append(fieldData.Data, field.Data...)
}

func mergeJSONField(data *InsertData, fid FieldID, field *JSONFieldData) {
	if _, ok := data.Data[fid]; !ok {
		fieldData := &JSONFieldData{
			Data: nil,
		}
		data.Data[fid] = fieldData
	}
	fieldData := data.Data[fid].(*JSONFieldData)
	fieldData.Data = append(fieldData.Data, field.Data...)
}

func mergeBinaryVectorField(data *InsertData, fid FieldID, field *BinaryVectorFieldData) {
	if _, ok := data.Data[fid]; !ok {
		fieldData := &BinaryVectorFieldData{
			Data: nil,
			Dim:  field.Dim,
		}
		data.Data[fid] = fieldData
	}
	fieldData := data.Data[fid].(*BinaryVectorFieldData)
	fieldData.Data = append(fieldData.Data, field.Data...)
}

func mergeFloatVectorField(data *InsertData, fid FieldID, field *FloatVectorFieldData) {
	if _, ok := data.Data[fid]; !ok {
		fieldData := &FloatVectorFieldData{
			Data: nil,
			Dim:  field.Dim,
		}
		data.Data[fid] = fieldData
	}
	fieldData := data.Data[fid].(*FloatVectorFieldData)
	fieldData.Data = append(fieldData.Data, field.Data...)
}

func mergeFloat16VectorField(data *InsertData, fid FieldID, field *Float16VectorFieldData) {
	if _, ok := data.Data[fid]; !ok {
		fieldData := &Float16VectorFieldData{
			Data: nil,
			Dim:  field.Dim,
		}
		data.Data[fid] = fieldData
	}
	fieldData := data.Data[fid].(*Float16VectorFieldData)
	fieldData.Data = append(fieldData.Data, field.Data...)
}

// MergeFieldData merge field into data.
func MergeFieldData(data *InsertData, fid FieldID, field FieldData) {
	if field == nil {
		return
	}
	switch field := field.(type) {
	case *BoolFieldData:
		mergeBoolField(data, fid, field)
	case *Int8FieldData:
		mergeInt8Field(data, fid, field)
	case *Int16FieldData:
		mergeInt16Field(data, fid, field)
	case *Int32FieldData:
		mergeInt32Field(data, fid, field)
	case *Int64FieldData:
		mergeInt64Field(data, fid, field)
	case *FloatFieldData:
		mergeFloatField(data, fid, field)
	case *DoubleFieldData:
		mergeDoubleField(data, fid, field)
	case *StringFieldData:
		mergeStringField(data, fid, field)
	case *ArrayFieldData:
		mergeArrayField(data, fid, field)
	case *JSONFieldData:
		mergeJSONField(data, fid, field)
	case *BinaryVectorFieldData:
		mergeBinaryVectorField(data, fid, field)
	case *FloatVectorFieldData:
		mergeFloatVectorField(data, fid, field)
	case *Float16VectorFieldData:
		mergeFloat16VectorField(data, fid, field)
	}
}

// MergeInsertData append the insert datas to the original buffer.
func MergeInsertData(buffer *InsertData, datas ...*InsertData) {
	if buffer == nil {
		log.Warn("Attempt to merge data into a nil buffer, skip the data merge.")
		return
	}

	for _, data := range datas {
		if data != nil {
			for fid, field := range data.Data {
				MergeFieldData(buffer, fid, field)
			}

			// TODO: handle storage.InsertData.Infos
			buffer.Infos = append(buffer.Infos, data.Infos...)
		}
	}
}

// TODO: string type.
func GetPkFromInsertData(collSchema *schemapb.CollectionSchema, data *InsertData) (FieldData, error) {
	helper, err := typeutil.CreateSchemaHelper(collSchema)
	if err != nil {
		log.Error("failed to create schema helper", zap.Error(err))
		return nil, err
	}

	pf, err := helper.GetPrimaryKeyField()
	if err != nil {
		log.Warn("no primary field found", zap.Error(err))
		return nil, err
	}

	pfData, ok := data.Data[pf.FieldID]
	if !ok {
		log.Warn("no primary field found in insert msg", zap.Int64("fieldID", pf.FieldID))
		return nil, errors.New("no primary field found in insert msg")
	}

	var realPfData FieldData
	switch pf.DataType {
	case schemapb.DataType_Int64:
		realPfData, ok = pfData.(*Int64FieldData)
	case schemapb.DataType_VarChar:
		realPfData, ok = pfData.(*StringFieldData)
	default:
		// TODO
	}
	if !ok {
		log.Warn("primary field not in Int64 or VarChar format", zap.Int64("fieldID", pf.FieldID))
		return nil, errors.New("primary field not in Int64 or VarChar format")
	}

	return realPfData, nil
}

// GetTimestampFromInsertData returns the Int64FieldData for timestamp field.
func GetTimestampFromInsertData(data *InsertData) (*Int64FieldData, error) {
	if data == nil {
		return nil, errors.New("try to get timestamp from nil insert data")
	}
	fieldData, ok := data.Data[common.TimeStampField]
	if !ok {
		return nil, errors.New("no timestamp field in insert data")
	}

	ifd, ok := fieldData.(*Int64FieldData)
	if !ok {
		return nil, errors.New("timestamp field is not Int64")
	}

	return ifd, nil
}

func boolFieldDataToPbBytes(field *BoolFieldData) ([]byte, error) {
	arr := &schemapb.BoolArray{Data: field.Data}
	return proto.Marshal(arr)
}

func stringFieldDataToPbBytes(field *StringFieldData) ([]byte, error) {
	arr := &schemapb.StringArray{Data: field.Data}
	return proto.Marshal(arr)
}

func arrayFieldDataToPbBytes(field *ArrayFieldData) ([]byte, error) {
	arr := &schemapb.ArrayArray{Data: field.Data}
	return proto.Marshal(arr)
}

func jsonFieldDataToPbBytes(field *JSONFieldData) ([]byte, error) {
	arr := &schemapb.JSONArray{Data: field.Data}
	return proto.Marshal(arr)
}

func binaryWrite(endian binary.ByteOrder, data interface{}) ([]byte, error) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, endian, data)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// FieldDataToBytes encode field data to byte slice.
// For some fixed-length data, such as int32, int64, float vector, use binary.Write directly.
// For binary vector, return it directly.
// For bool data, first transfer to schemapb.BoolArray and then marshal it. (TODO: handle bool like other scalar data.)
// For variable-length data, such as string, first transfer to schemapb.StringArray and then marshal it.
// TODO: find a proper way to store variable-length data. Or we should unify to use protobuf?
func FieldDataToBytes(endian binary.ByteOrder, fieldData FieldData) ([]byte, error) {
	switch field := fieldData.(type) {
	case *BoolFieldData:
		// return binaryWrite(endian, field.Data)
		return boolFieldDataToPbBytes(field)
	case *StringFieldData:
		return stringFieldDataToPbBytes(field)
	case *ArrayFieldData:
		return arrayFieldDataToPbBytes(field)
	case *JSONFieldData:
		return jsonFieldDataToPbBytes(field)
	case *BinaryVectorFieldData:
		return field.Data, nil
	case *FloatVectorFieldData:
		return binaryWrite(endian, field.Data)
	case *Int8FieldData:
		return binaryWrite(endian, field.Data)
	case *Int16FieldData:
		return binaryWrite(endian, field.Data)
	case *Int32FieldData:
		return binaryWrite(endian, field.Data)
	case *Int64FieldData:
		return binaryWrite(endian, field.Data)
	case *FloatFieldData:
		return binaryWrite(endian, field.Data)
	case *DoubleFieldData:
		return binaryWrite(endian, field.Data)
	default:
		return nil, fmt.Errorf("unsupported field data: %s", field)
	}
}

func TransferInsertDataToInsertRecord(insertData *InsertData) (*segcorepb.InsertRecord, error) {
	insertRecord := &segcorepb.InsertRecord{}
	for fieldID, rawData := range insertData.Data {
		var fieldData *schemapb.FieldData
		switch rawData := rawData.(type) {
		case *BoolFieldData:
			fieldData = &schemapb.FieldData{
				Type:    schemapb.DataType_Bool,
				FieldId: fieldID,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_BoolData{
							BoolData: &schemapb.BoolArray{
								Data: rawData.Data,
							},
						},
					},
				},
			}
		case *Int8FieldData:
			int32Data := make([]int32, len(rawData.Data))
			for index, v := range rawData.Data {
				int32Data[index] = int32(v)
			}
			fieldData = &schemapb.FieldData{
				Type:    schemapb.DataType_Int8,
				FieldId: fieldID,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_IntData{
							IntData: &schemapb.IntArray{
								Data: int32Data,
							},
						},
					},
				},
			}
		case *Int16FieldData:
			int32Data := make([]int32, len(rawData.Data))
			for index, v := range rawData.Data {
				int32Data[index] = int32(v)
			}
			fieldData = &schemapb.FieldData{
				Type:    schemapb.DataType_Int16,
				FieldId: fieldID,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_IntData{
							IntData: &schemapb.IntArray{
								Data: int32Data,
							},
						},
					},
				},
			}
		case *Int32FieldData:
			fieldData = &schemapb.FieldData{
				Type:    schemapb.DataType_Int32,
				FieldId: fieldID,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_IntData{
							IntData: &schemapb.IntArray{
								Data: rawData.Data,
							},
						},
					},
				},
			}
		case *Int64FieldData:
			fieldData = &schemapb.FieldData{
				Type:    schemapb.DataType_Int64,
				FieldId: fieldID,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{
								Data: rawData.Data,
							},
						},
					},
				},
			}
		case *FloatFieldData:
			fieldData = &schemapb.FieldData{
				Type:    schemapb.DataType_Float,
				FieldId: fieldID,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_FloatData{
							FloatData: &schemapb.FloatArray{
								Data: rawData.Data,
							},
						},
					},
				},
			}
		case *DoubleFieldData:
			fieldData = &schemapb.FieldData{
				Type:    schemapb.DataType_Double,
				FieldId: fieldID,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_DoubleData{
							DoubleData: &schemapb.DoubleArray{
								Data: rawData.Data,
							},
						},
					},
				},
			}
		case *StringFieldData:
			fieldData = &schemapb.FieldData{
				Type:    schemapb.DataType_VarChar,
				FieldId: fieldID,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{
								Data: rawData.Data,
							},
						},
					},
				},
			}
		case *ArrayFieldData:
			fieldData = &schemapb.FieldData{
				Type:    schemapb.DataType_Array,
				FieldId: fieldID,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_ArrayData{
							ArrayData: &schemapb.ArrayArray{
								Data: rawData.Data,
							},
						},
					},
				},
			}
		case *JSONFieldData:
			fieldData = &schemapb.FieldData{
				Type:    schemapb.DataType_JSON,
				FieldId: fieldID,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_JsonData{
							JsonData: &schemapb.JSONArray{
								Data: rawData.Data,
							},
						},
					},
				},
			}
		case *FloatVectorFieldData:
			fieldData = &schemapb.FieldData{
				Type:    schemapb.DataType_FloatVector,
				FieldId: fieldID,
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Data: &schemapb.VectorField_FloatVector{
							FloatVector: &schemapb.FloatArray{
								Data: rawData.Data,
							},
						},
						Dim: int64(rawData.Dim),
					},
				},
			}
		case *BinaryVectorFieldData:
			fieldData = &schemapb.FieldData{
				Type:    schemapb.DataType_BinaryVector,
				FieldId: fieldID,
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Data: &schemapb.VectorField_BinaryVector{
							BinaryVector: rawData.Data,
						},
						Dim: int64(rawData.Dim),
					},
				},
			}
		default:
			return insertRecord, fmt.Errorf("unsupported data type when transter storage.InsertData to internalpb.InsertRecord")
		}

		insertRecord.FieldsData = append(insertRecord.FieldsData, fieldData)
		insertRecord.NumRows = int64(rawData.RowNum())
	}

	return insertRecord, nil
}

func TransferInsertMsgToInsertRecord(schema *schemapb.CollectionSchema, msg *msgstream.InsertMsg) (*segcorepb.InsertRecord, error) {
	if msg.IsRowBased() {
		insertData, err := RowBasedInsertMsgToInsertData(msg, schema)
		if err != nil {
			return nil, err
		}
		return TransferInsertDataToInsertRecord(insertData)
	}

	// column base insert msg
	insertRecord := &segcorepb.InsertRecord{
		NumRows: int64(msg.NumRows),
	}

	insertRecord.FieldsData = append(insertRecord.FieldsData, msg.FieldsData...)

	return insertRecord, nil
}
