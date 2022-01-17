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
	"errors"
	"fmt"
	"sort"
	"strconv"

	"github.com/milvus-io/milvus/internal/common"

	"github.com/milvus-io/milvus/internal/proto/commonpb"

	"github.com/milvus-io/milvus/internal/kv"
)

// GetBinlogSize get size of a binlog file.
//		normal binlog file, error = nil;
//		key not exist, size = 0, error = nil;
//		key not in binlog format, size = (a not accurate number), error != nil;
//		failed to read event reader, size = (a not accurate number), error != nil;
func GetBinlogSize(kv kv.DataKV, key string) (int64, error) {

	return kv.GetSize(key)
}

// EstimateMemorySize get approximate memory size of a binlog file.
//		1, key not exist, size = 0, error != nil;
//		2, failed to read event header, size = 0, error != nil;
//		3, invalid event length, size = 0, error != nil;
//		4, failed to read descriptor event, size = 0, error != nil;
//		5, original_size not in extra, size = 0, error != nil;
//		6, original_size not in int format, size = 0, error != nil;
//		7, normal binlog with original_size, return original_size, error = nil;
func EstimateMemorySize(kv kv.DataKV, key string) (int64, error) {
	total := int64(0)

	header := &eventHeader{}
	headerSize := binary.Size(header)

	startPos := binary.Size(MagicNumber)
	endPos := startPos + headerSize

	// get header
	headerContent, err := kv.LoadPartial(key, int64(startPos), int64(endPos))
	if err != nil {
		return total, err
	}

	buffer := bytes.NewBuffer(headerContent)

	header, err = readEventHeader(buffer)
	if err != nil {
		return total, err
	}

	if header.EventLength <= 0 {
		return total, fmt.Errorf("key %v not in binlog format", key)
	}

	var desc *descriptorEvent
	endPos = startPos + int(header.EventLength)
	descContent, err := kv.LoadPartial(key, int64(startPos), int64(endPos))
	if err != nil {
		return total, err
	}

	buffer = bytes.NewBuffer(descContent)

	desc, err = ReadDescriptorEvent(buffer)
	if err != nil {
		return total, err
	}

	sizeStr, ok := desc.Extras[originalSizeKey]
	if !ok {
		return total, fmt.Errorf("key %v not in extra information", originalSizeKey)
	}

	size, err := strconv.Atoi(fmt.Sprintf("%v", sizeStr))
	if err != nil {
		return total, fmt.Errorf("%v not in valid format, value: %v", originalSizeKey, sizeStr)
	}

	total = int64(size)

	return total, nil
}

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
//	- ts column must exist in insert data;
//	- row id column must exist in insert data;
//	- the row num of all column must be equal;
//	- num_rows = len(RowData), a row will be assembled into the value of blob with field id order;
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
