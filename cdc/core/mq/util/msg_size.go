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

package util

import (
	"encoding/binary"

	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"github.com/milvus-io/milvus/cdc/core/mq/api"
	"github.com/milvus-io/milvus/cdc/core/util"
	"go.uber.org/zap"
)

func SizeOfInsertMsg(msg *api.InsertMsg) int64 {
	var totalSize int64
	sizeFunc := func(column entity.Column) bool {
		size := SizeColumn(column)
		if size < 0 {
			return false
		}
		totalSize += SizeColumn(column)
		return true
	}

	for _, fieldData := range msg.FieldsData {
		if column, err := entity.FieldDataColumn(fieldData, 0, -1); err == nil {
			if !sizeFunc(column) {
				util.Log.Warn("insert msg, fail to get the data size", zap.String("name", column.Name()))
				return -1
			}
		} else {
			column, err := entity.FieldDataVector(fieldData)
			if err != nil {
				util.Log.Warn("fail to get the data size", zap.Any("msg", msg), zap.Error(err))
				return -1
			}
			if !sizeFunc(column) {
				util.Log.Warn("insert msg, fail to get the data size", zap.String("name", column.Name()))
				return -1
			}
		}
	}
	return totalSize
}

func SizeOfDeleteMsg(msg *api.DeleteMsg) int64 {
	var totalSize int64
	column, err := entity.IDColumns(msg.PrimaryKeys, 0, -1)
	if err != nil {
		util.Log.Warn("fail to get the id columns", zap.Any("msg", msg), zap.Error(err))
		return -1
	}
	if totalSize = SizeColumn(column); totalSize < 0 {
		util.Log.Warn("delete msg, fail to get the data size", zap.String("name", column.Name()))
		return -1
	}
	return totalSize
}

func SizeColumn(column entity.Column) int64 {
	var data any
	switch c := column.(type) {
	case *entity.ColumnBool:
		data = c.Data()
	case *entity.ColumnInt8:
		data = c.Data()
	case *entity.ColumnInt16:
		data = c.Data()
	case *entity.ColumnInt32:
		data = c.Data()
	case *entity.ColumnInt64:
		data = c.Data()
	case *entity.ColumnFloat:
		data = c.Data()
	case *entity.ColumnDouble:
		data = c.Data()
	case *entity.ColumnString:
		strArr := c.Data()
		total := 0
		for _, s := range strArr {
			total += binary.Size(util.ToBytes(s))
		}
		return int64(total)
	case *entity.ColumnVarChar:
		strArr := c.Data()
		total := 0
		for _, s := range strArr {
			total += binary.Size(util.ToBytes(s))
		}
		return int64(total)
	case *entity.ColumnBinaryVector:
		byteArr := c.Data()
		total := 0
		for _, s := range byteArr {
			total += binary.Size(s)
		}
		return int64(total)
	case *entity.ColumnFloatVector:
		floatArr := c.Data()
		total := 0
		for _, f := range floatArr {
			total += binary.Size(f)
		}
		return int64(total)
	default:
		util.Log.Warn("invalid type", zap.Any("column", column))
		return -1
	}
	return int64(binary.Size(data))
}
