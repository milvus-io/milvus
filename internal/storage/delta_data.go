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
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/samber/lo"
	"github.com/valyala/fastjson"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

// parserPool use object pooling to reduce fastjson.Parser allocation.
var parserPool = &fastjson.ParserPool{}

// DeltaData stores delta data
// currently only delete tuples are stored
type DeltaData struct {
	pkType schemapb.DataType
	// delete tuples
	delPks PrimaryKeys
	delTss []Timestamp

	// stats
	delRowCount int64
	memSize     int64
}

type DeleteLog struct {
	Pk     PrimaryKey `json:"pk"`
	Ts     uint64     `json:"ts"`
	PkType int64      `json:"pkType"`
}

func NewDeleteLog(pk PrimaryKey, ts Timestamp) *DeleteLog {
	pkType := pk.Type()

	return &DeleteLog{
		Pk:     pk,
		Ts:     ts,
		PkType: int64(pkType),
	}
}

// Parse tries to parse string format delete log
// it try json first then use "," split int,ts format
func (dl *DeleteLog) Parse(val string) error {
	p := parserPool.Get()
	defer parserPool.Put(p)
	v, err := p.Parse(val)
	if err != nil {
		// compatible with versions that only support int64 type primary keys
		// compatible with fmt.Sprintf("%d,%d", pk, ts)
		// compatible error info (unmarshal err invalid character ',' after top-level value)
		splits := strings.Split(val, ",")
		if len(splits) != 2 {
			return fmt.Errorf("the format of delta log is incorrect, %v can not be split", val)
		}
		pk, err := strconv.ParseInt(splits[0], 10, 64)
		if err != nil {
			return err
		}
		dl.Pk = &Int64PrimaryKey{
			Value: pk,
		}
		dl.PkType = int64(schemapb.DataType_Int64)
		dl.Ts, err = strconv.ParseUint(splits[1], 10, 64)
		if err != nil {
			return err
		}
		return nil
	}

	dl.Ts = v.GetUint64("ts")
	dl.PkType = v.GetInt64("pkType")
	switch dl.PkType {
	case int64(schemapb.DataType_Int64):
		dl.Pk = &Int64PrimaryKey{Value: v.GetInt64("pk")}
	case int64(schemapb.DataType_VarChar):
		dl.Pk = &VarCharPrimaryKey{Value: string(v.GetStringBytes("pk"))}
	}
	return nil
}

func (dl *DeleteLog) UnmarshalJSON(data []byte) error {
	var messageMap map[string]*json.RawMessage
	var err error
	if err = json.Unmarshal(data, &messageMap); err != nil {
		return err
	}

	if err = json.Unmarshal(*messageMap["pkType"], &dl.PkType); err != nil {
		return err
	}

	switch schemapb.DataType(dl.PkType) {
	case schemapb.DataType_Int64:
		dl.Pk = &Int64PrimaryKey{}
	case schemapb.DataType_VarChar:
		dl.Pk = &VarCharPrimaryKey{}
	}

	if err = json.Unmarshal(*messageMap["pk"], dl.Pk); err != nil {
		return err
	}

	if err = json.Unmarshal(*messageMap["ts"], &dl.Ts); err != nil {
		return err
	}

	return nil
}

// DeleteData saves each entity delete message represented as <primarykey,timestamp> map.
// timestamp represents the time when this instance was deleted
type DeleteData struct {
	Pks      []PrimaryKey // primary keys
	Tss      []Timestamp  // timestamps
	RowCount int64
	memSize  int64
}

func NewDeleteData(pks []PrimaryKey, tss []Timestamp) *DeleteData {
	return &DeleteData{
		Pks:      pks,
		Tss:      tss,
		RowCount: int64(len(pks)),
		memSize:  lo.SumBy(pks, func(pk PrimaryKey) int64 { return pk.Size() }) + int64(len(tss)*8),
	}
}

// Append append 1 pk&ts pair to DeleteData
func (data *DeleteData) Append(pk PrimaryKey, ts Timestamp) {
	data.Pks = append(data.Pks, pk)
	data.Tss = append(data.Tss, ts)
	data.RowCount++
	data.memSize += pk.Size() + int64(8)
}

// Append append 1 pk&ts pair to DeleteData
func (data *DeleteData) AppendBatch(pks []PrimaryKey, tss []Timestamp) {
	data.Pks = append(data.Pks, pks...)
	data.Tss = append(data.Tss, tss...)
	data.RowCount += int64(len(pks))
	data.memSize += lo.SumBy(pks, func(pk PrimaryKey) int64 { return pk.Size() }) + int64(len(tss)*8)
}

func (data *DeleteData) Merge(other *DeleteData) {
	data.Pks = append(data.Pks, other.Pks...)
	data.Tss = append(data.Tss, other.Tss...)
	data.RowCount += other.RowCount
	data.memSize += other.Size()

	other.Pks = nil
	other.Tss = nil
	other.RowCount = 0
	other.memSize = 0
}

func (data *DeleteData) Size() int64 {
	return data.memSize
}
