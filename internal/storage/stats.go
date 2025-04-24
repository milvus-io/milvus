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
	"maps"
	"math"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/util/bloomfilter"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// PrimaryKeyStats contains rowsWithToken data for pk column
type PrimaryKeyStats struct {
	FieldID int64                            `json:"fieldID"`
	Max     int64                            `json:"max"` // useless, will delete
	Min     int64                            `json:"min"` // useless, will delete
	BFType  bloomfilter.BFType               `json:"bfType"`
	BF      bloomfilter.BloomFilterInterface `json:"bf"`
	PkType  int64                            `json:"pkType"`
	MaxPk   PrimaryKey                       `json:"maxPk"`
	MinPk   PrimaryKey                       `json:"minPk"`
}

// UnmarshalJSON unmarshal bytes to PrimaryKeyStats
func (stats *PrimaryKeyStats) UnmarshalJSON(data []byte) error {
	var messageMap map[string]*json.RawMessage
	err := json.Unmarshal(data, &messageMap)
	if err != nil {
		return err
	}

	err = json.Unmarshal(*messageMap["fieldID"], &stats.FieldID)
	if err != nil {
		return err
	}

	stats.PkType = int64(schemapb.DataType_Int64)
	if value, ok := messageMap["pkType"]; ok && value != nil {
		var typeValue int64
		err = json.Unmarshal(*value, &typeValue)
		if err != nil {
			return err
		}
		// valid pkType
		if typeValue > 0 {
			stats.PkType = typeValue
		}
	}

	switch schemapb.DataType(stats.PkType) {
	case schemapb.DataType_Int64:
		stats.MaxPk = &Int64PrimaryKey{}
		stats.MinPk = &Int64PrimaryKey{}

		// Compatible with versions that only support int64 type primary keys
		err = json.Unmarshal(*messageMap["max"], &stats.Max)
		if err != nil {
			return err
		}
		err = stats.MaxPk.SetValue(stats.Max)
		if err != nil {
			return err
		}

		err = json.Unmarshal(*messageMap["min"], &stats.Min)
		if err != nil {
			return err
		}
		err = stats.MinPk.SetValue(stats.Min)
		if err != nil {
			return err
		}
	case schemapb.DataType_VarChar:
		stats.MaxPk = &VarCharPrimaryKey{}
		stats.MinPk = &VarCharPrimaryKey{}
	default:
		return errors.New("Invalid PK Data Type")
	}

	if maxPkMessage, ok := messageMap["maxPk"]; ok && maxPkMessage != nil {
		err = json.Unmarshal(*maxPkMessage, stats.MaxPk)
		if err != nil {
			return err
		}
	}

	if minPkMessage, ok := messageMap["minPk"]; ok && minPkMessage != nil {
		err = json.Unmarshal(*minPkMessage, stats.MinPk)
		if err != nil {
			return err
		}
	}

	bfType := bloomfilter.BasicBF
	if bfTypeMessage, ok := messageMap["bfType"]; ok && bfTypeMessage != nil {
		err := json.Unmarshal(*bfTypeMessage, &bfType)
		if err != nil {
			return err
		}
		stats.BFType = bfType
	}

	if bfMessage, ok := messageMap["bf"]; ok && bfMessage != nil {
		bf, err := bloomfilter.UnmarshalJSON(*bfMessage, bfType)
		if err != nil {
			log.Warn("Failed to unmarshal bloom filter, use AlwaysTrueBloomFilter instead of return err", zap.Error(err))
			bf = bloomfilter.AlwaysTrueBloomFilter
		}
		stats.BF = bf
	}

	return nil
}

func (stats *PrimaryKeyStats) UpdateByMsgs(msgs FieldData) {
	switch schemapb.DataType(stats.PkType) {
	case schemapb.DataType_Int64:
		data := msgs.(*Int64FieldData).Data
		if len(data) < 1 {
			// return error: msgs must has one element at least
			return
		}

		b := make([]byte, 8)
		for _, int64Value := range data {
			pk := NewInt64PrimaryKey(int64Value)
			stats.UpdateMinMax(pk)
			common.Endian.PutUint64(b, uint64(int64Value))
			stats.BF.Add(b)
		}
	case schemapb.DataType_VarChar:
		data := msgs.(*StringFieldData).Data
		if len(data) < 1 {
			// return error: msgs must has one element at least
			return
		}

		for _, str := range data {
			pk := NewVarCharPrimaryKey(str)
			stats.UpdateMinMax(pk)
			stats.BF.AddString(str)
		}
	default:
		// TODO::
	}
}

func (stats *PrimaryKeyStats) Update(pk PrimaryKey) {
	stats.UpdateMinMax(pk)
	switch schemapb.DataType(stats.PkType) {
	case schemapb.DataType_Int64:
		data := pk.GetValue().(int64)
		b := make([]byte, 8)
		common.Endian.PutUint64(b, uint64(data))
		stats.BF.Add(b)
	case schemapb.DataType_VarChar:
		data := pk.GetValue().(string)
		stats.BF.AddString(data)
	default:
		log.Warn("Update pk stats with invalid data type")
	}
}

// updatePk update minPk and maxPk value
func (stats *PrimaryKeyStats) UpdateMinMax(pk PrimaryKey) {
	if stats.MinPk == nil {
		stats.MinPk = pk
	} else if stats.MinPk.GT(pk) {
		stats.MinPk = pk
	}

	if stats.MaxPk == nil {
		stats.MaxPk = pk
	} else if stats.MaxPk.LT(pk) {
		stats.MaxPk = pk
	}
}

func NewPrimaryKeyStats(fieldID, pkType, rowNum int64) (*PrimaryKeyStats, error) {
	if rowNum <= 0 {
		return nil, merr.WrapErrParameterInvalidMsg("zero or negative row num", rowNum)
	}

	bfType := paramtable.Get().CommonCfg.BloomFilterType.GetValue()
	return &PrimaryKeyStats{
		FieldID: fieldID,
		PkType:  pkType,
		BFType:  bloomfilter.BFTypeFromString(bfType),
		BF: bloomfilter.NewBloomFilterWithType(
			uint(rowNum),
			paramtable.Get().CommonCfg.MaxBloomFalsePositive.GetAsFloat(),
			bfType),
	}, nil
}

// StatsWriter writes stats to buffer
type StatsWriter struct {
	buffer []byte
}

// GetBuffer returns buffer
func (sw *StatsWriter) GetBuffer() []byte {
	return sw.buffer
}

// GenerateList writes Stats slice to buffer
func (sw *StatsWriter) GenerateList(stats []*PrimaryKeyStats) error {
	b, err := json.Marshal(stats)
	if err != nil {
		return err
	}
	sw.buffer = b
	return nil
}

// Generate writes Stats to buffer
func (sw *StatsWriter) Generate(stats *PrimaryKeyStats) error {
	b, err := json.Marshal(stats)
	if err != nil {
		return err
	}
	sw.buffer = b
	return nil
}

// GenerateByData writes Int64Stats or StringStats from @msgs with @fieldID to @buffer
func (sw *StatsWriter) GenerateByData(fieldID int64, pkType schemapb.DataType, msgs FieldData) error {
	bfType := paramtable.Get().CommonCfg.BloomFilterType.GetValue()
	stats := &PrimaryKeyStats{
		FieldID: fieldID,
		PkType:  int64(pkType),
		BFType:  bloomfilter.BFTypeFromString(bfType),
		BF: bloomfilter.NewBloomFilterWithType(
			uint(msgs.RowNum()),
			paramtable.Get().CommonCfg.MaxBloomFalsePositive.GetAsFloat(),
			bfType),
	}

	stats.UpdateByMsgs(msgs)
	return sw.Generate(stats)
}

// StatsReader reads stats
type StatsReader struct {
	buffer []byte
}

// SetBuffer sets buffer
func (sr *StatsReader) SetBuffer(buffer []byte) {
	sr.buffer = buffer
}

// GetInt64Stats returns buffer as PrimaryKeyStats
func (sr *StatsReader) GetPrimaryKeyStats() (*PrimaryKeyStats, error) {
	stats := &PrimaryKeyStats{}
	err := json.Unmarshal(sr.buffer, &stats)
	if err != nil {
		return nil, merr.WrapErrParameterInvalid(
			"valid JSON",
			string(sr.buffer),
			err.Error())
	}

	return stats, nil
}

// GetInt64Stats returns buffer as PrimaryKeyStats
func (sr *StatsReader) GetPrimaryKeyStatsList() ([]*PrimaryKeyStats, error) {
	stats := []*PrimaryKeyStats{}
	err := json.Unmarshal(sr.buffer, &stats)
	if err != nil {
		return nil, merr.WrapErrParameterInvalid(
			"valid JSON",
			string(sr.buffer),
			err.Error())
	}

	return stats, nil
}

type BM25Stats struct {
	rowsWithToken map[uint32]int32 // mapping token => row num include token
	numRow        int64            // total row num
	numToken      int64            // total token num
}

const BM25VERSION int32 = 0

func NewBM25Stats() *BM25Stats {
	return &BM25Stats{
		rowsWithToken: map[uint32]int32{},
	}
}

func NewBM25StatsWithBytes(bytes []byte) (*BM25Stats, error) {
	stats := NewBM25Stats()
	err := stats.Deserialize(bytes)
	if err != nil {
		return nil, err
	}
	return stats, nil
}

func (m *BM25Stats) Append(rows ...map[uint32]float32) {
	for _, row := range rows {
		for key, value := range row {
			m.rowsWithToken[key] += 1
			m.numToken += int64(value)
		}

		m.numRow += 1
	}
}

func (m *BM25Stats) AppendFieldData(datas ...*SparseFloatVectorFieldData) {
	for _, data := range datas {
		m.AppendBytes(data.GetContents()...)
	}
}

// Update BM25Stats by sparse vector bytes
func (m *BM25Stats) AppendBytes(datas ...[]byte) {
	for _, data := range datas {
		dim := typeutil.SparseFloatRowElementCount(data)
		for i := 0; i < dim; i++ {
			index := typeutil.SparseFloatRowIndexAt(data, i)
			value := typeutil.SparseFloatRowValueAt(data, i)
			m.rowsWithToken[index] += 1
			m.numToken += int64(value)
		}
		m.numRow += 1
	}
}

func (m *BM25Stats) NumRow() int64 {
	return m.numRow
}

func (m *BM25Stats) NumToken() int64 {
	return m.numToken
}

func (m *BM25Stats) Merge(meta *BM25Stats) {
	for key, value := range meta.rowsWithToken {
		m.rowsWithToken[key] += value
	}
	m.numRow += meta.NumRow()
	m.numToken += meta.numToken
}

func (m *BM25Stats) Minus(meta *BM25Stats) {
	for key, value := range meta.rowsWithToken {
		m.rowsWithToken[key] -= value
	}
	m.numRow -= meta.numRow
	m.numToken -= meta.numToken
}

func (m *BM25Stats) Clone() *BM25Stats {
	return &BM25Stats{
		rowsWithToken: maps.Clone(m.rowsWithToken),
		numRow:        m.numRow,
		numToken:      m.numToken,
	}
}

func (m *BM25Stats) Serialize() ([]byte, error) {
	buffer := bytes.NewBuffer(make([]byte, 0, len(m.rowsWithToken)*8+20))

	if err := binary.Write(buffer, common.Endian, BM25VERSION); err != nil {
		return nil, err
	}

	if err := binary.Write(buffer, common.Endian, m.numRow); err != nil {
		return nil, err
	}

	if err := binary.Write(buffer, common.Endian, m.numToken); err != nil {
		return nil, err
	}

	for key, value := range m.rowsWithToken {
		if err := binary.Write(buffer, common.Endian, key); err != nil {
			return nil, err
		}

		if err := binary.Write(buffer, common.Endian, value); err != nil {
			return nil, err
		}
	}

	// TODO ADD Serialize Time Metric
	return buffer.Bytes(), nil
}

func (m *BM25Stats) Deserialize(bs []byte) error {
	buffer := bytes.NewBuffer(bs)
	dim := (len(bs) - 20) / 8
	var numRow, tokenNum int64
	var version int32
	if err := binary.Read(buffer, common.Endian, &version); err != nil {
		return err
	}

	if err := binary.Read(buffer, common.Endian, &numRow); err != nil {
		return err
	}

	if err := binary.Read(buffer, common.Endian, &tokenNum); err != nil {
		return err
	}

	var keys []uint32 = make([]uint32, dim)
	var values []int32 = make([]int32, dim)
	for i := 0; i < dim; i++ {
		if err := binary.Read(buffer, common.Endian, &keys[i]); err != nil {
			return err
		}

		if err := binary.Read(buffer, common.Endian, &values[i]); err != nil {
			return err
		}
	}

	m.numRow += numRow
	m.numToken += tokenNum
	for i := 0; i < dim; i++ {
		m.rowsWithToken[keys[i]] += values[i]
	}

	return nil
}

func (m *BM25Stats) BuildIDF(tf []byte) (idf []byte) {
	numElements := typeutil.SparseFloatRowElementCount(tf)
	idf = make([]byte, len(tf))
	for idx := 0; idx < numElements; idx++ {
		key := typeutil.SparseFloatRowIndexAt(tf, idx)
		value := typeutil.SparseFloatRowValueAt(tf, idx)
		nq := m.rowsWithToken[key]
		typeutil.SparseFloatRowSetAt(idf, idx, key, value*float32(math.Log(1+(float64(m.numRow)-float64(nq)+0.5)/(float64(nq)+0.5))))
	}
	return
}

func (m *BM25Stats) GetAvgdl() float64 {
	if m.numRow == 0 || m.numToken == 0 {
		return 0
	}
	return float64(m.numToken) / float64(m.numRow)
}

// DeserializeStats deserialize @blobs as []*PrimaryKeyStats
func DeserializeStats(blobs []*Blob) ([]*PrimaryKeyStats, error) {
	results := make([]*PrimaryKeyStats, 0, len(blobs))
	for _, blob := range blobs {
		if len(blob.Value) == 0 {
			continue
		}
		sr := &StatsReader{}
		sr.SetBuffer(blob.Value)
		stats, err := sr.GetPrimaryKeyStats()
		if err != nil {
			return nil, err
		}
		results = append(results, stats)
	}
	return results, nil
}

func DeserializeStatsList(blob *Blob) ([]*PrimaryKeyStats, error) {
	if len(blob.Value) == 0 {
		return []*PrimaryKeyStats{}, nil
	}
	sr := &StatsReader{}
	sr.SetBuffer(blob.Value)
	stats, err := sr.GetPrimaryKeyStatsList()
	if err != nil {
		return nil, err
	}
	return stats, nil
}
