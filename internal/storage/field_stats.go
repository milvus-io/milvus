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

	"github.com/bits-and-blooms/bloom/v3"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

// FieldStats contains statistics data for any column
// todo: compatible to PrimaryKeyStats
type FieldStats struct {
	FieldID   int64                  `json:"fieldID"`
	Type      int64                  `json:"Type"`
	Max       ScalarFieldValue       `json:"max"`      // for scalar field
	Min       ScalarFieldValue       `json:"min"`      // for scalar field
	BF        *bloom.BloomFilter     `json:"bf"`       // for scalar field
	Centroids []schemapb.VectorField `json:"centroid"` // for vector field
}

// UnmarshalJSON unmarshal bytes to FieldStats
func (stats *FieldStats) UnmarshalJSON(data []byte) error {
	var messageMap map[string]*json.RawMessage
	err := json.Unmarshal(data, &messageMap)
	if err != nil {
		return err
	}

	err = json.Unmarshal(*messageMap["fieldID"], &stats.FieldID)
	if err != nil {
		return err
	}

	stats.Type = int64(schemapb.DataType_Int64)
	if value, ok := messageMap["Type"]; ok && value != nil {
		var typeValue int64
		err = json.Unmarshal(*value, &typeValue)
		if err != nil {
			return err
		}
		if typeValue > 0 {
			stats.Type = typeValue
		}
	}

	switch schemapb.DataType(stats.Type) {
	case schemapb.DataType_Int64:
		stats.Max = &Int64FieldValue{}
		stats.Min = &Int64FieldValue{}
		err = json.Unmarshal(*messageMap["max"], &stats.Max)
		if err != nil {
			return err
		}
		err = json.Unmarshal(*messageMap["min"], &stats.Min)
		if err != nil {
			return err
		}
	case schemapb.DataType_VarChar:
		stats.Max = &VarCharFieldValue{}
		stats.Min = &VarCharFieldValue{}
		err = json.Unmarshal(*messageMap["max"], &stats.Max)
		if err != nil {
			return err
		}
		err = json.Unmarshal(*messageMap["min"], &stats.Min)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("invalid Data Type")
	}

	// Compatible with primaryKeyStats
	if maxPkMessage, ok := messageMap["maxPk"]; ok && maxPkMessage != nil {
		err = json.Unmarshal(*maxPkMessage, stats.Max)
		if err != nil {
			return err
		}
	}

	if minPkMessage, ok := messageMap["minPk"]; ok && minPkMessage != nil {
		err = json.Unmarshal(*minPkMessage, stats.Min)
		if err != nil {
			return err
		}
	}

	if bfMessage, ok := messageMap["bf"]; ok && bfMessage != nil {
		stats.BF = &bloom.BloomFilter{}
		err = stats.BF.UnmarshalJSON(*bfMessage)
		if err != nil {
			return err
		}
	}

	return nil
}

func (stats *FieldStats) UpdateByMsgs(msgs FieldData) {
	switch schemapb.DataType(stats.Type) {
	case schemapb.DataType_Int64:
		data := msgs.(*Int64FieldData).Data
		if len(data) < 1 {
			// return error: msgs must has one element at least
			return
		}

		b := make([]byte, 8)
		for _, int64Value := range data {
			pk := NewInt64FieldValue(int64Value)
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
			pk := NewVarCharFieldValue(str)
			stats.UpdateMinMax(pk)
			stats.BF.AddString(str)
		}
	default:
		// TODO::
	}
}

func (stats *FieldStats) Update(pk ScalarFieldValue) {
	stats.UpdateMinMax(pk)
	switch schemapb.DataType(stats.Type) {
	case schemapb.DataType_Int64:
		data := pk.GetValue().(int64)
		b := make([]byte, 8)
		common.Endian.PutUint64(b, uint64(data))
		stats.BF.Add(b)
	case schemapb.DataType_VarChar:
		data := pk.GetValue().(string)
		stats.BF.AddString(data)
	default:
		log.Warn("Update stats with invalid data type")
	}
}

// UpdateMinMax update min and max value
func (stats *FieldStats) UpdateMinMax(pk ScalarFieldValue) {
	if stats.Min == nil {
		stats.Min = pk
	} else if stats.Min.GT(pk) {
		stats.Min = pk
	}

	if stats.Max == nil {
		stats.Max = pk
	} else if stats.Max.LT(pk) {
		stats.Max = pk
	}
}

func NewFieldStats(fieldID, pkType, rowNum int64) (*FieldStats, error) {
	if rowNum <= 0 {
		return nil, merr.WrapErrParameterInvalidMsg("non zero & non negative row num", rowNum)
	}
	return &FieldStats{
		FieldID: fieldID,
		Type:    pkType,
		BF:      bloom.NewWithEstimates(uint(rowNum), paramtable.Get().CommonCfg.MaxBloomFalsePositive.GetAsFloat()),
	}, nil
}

// FieldStatsWriter writes stats to buffer
type FieldStatsWriter struct {
	buffer []byte
}

// GetBuffer returns buffer
func (sw *FieldStatsWriter) GetBuffer() []byte {
	return sw.buffer
}

// GenerateList writes Stats slice to buffer
func (sw *FieldStatsWriter) GenerateList(stats []*FieldStats) error {
	b, err := json.Marshal(stats)
	if err != nil {
		return err
	}
	sw.buffer = b
	return nil
}

// Generate writes Stats to buffer
func (sw *FieldStatsWriter) Generate(stats *FieldStats) error {
	b, err := json.Marshal(stats)
	if err != nil {
		return err
	}
	sw.buffer = b
	//if len(sw.buffer) > 0 {
	//	sw.buffer = append(sw.GetBuffer(), b...)
	//} else {
	//	sw.buffer = b
	//}
	return nil
}

// GenerateByData writes Int64Stats or StringStats from @msgs with @fieldID to @buffer
func (sw *FieldStatsWriter) GenerateByData(fieldID int64, pkType schemapb.DataType, msgs ...FieldData) error {
	statsList := make([]*FieldStats, 0)
	for _, msg := range msgs {
		stats := &FieldStats{
			FieldID: fieldID,
			Type:    int64(pkType),
			BF:      bloom.NewWithEstimates(uint(msg.RowNum()), paramtable.Get().CommonCfg.MaxBloomFalsePositive.GetAsFloat()),
		}

		stats.UpdateByMsgs(msg)
		statsList = append(statsList, stats)
	}
	return sw.GenerateList(statsList)
}

// FieldStatsReader reads stats
type FieldStatsReader struct {
	buffer []byte
}

// SetBuffer sets buffer
func (sr *FieldStatsReader) SetBuffer(buffer []byte) {
	sr.buffer = buffer
}

// GetFieldStats returns buffer as FieldStats
func (sr *FieldStatsReader) GetFieldStats() (*FieldStats, error) {
	stats := &FieldStats{}
	err := json.Unmarshal(sr.buffer, &stats)
	if err != nil {
		return nil, merr.WrapErrParameterInvalid(
			"valid JSON",
			string(sr.buffer),
			err.Error())
	}

	return stats, nil
}

// GetFieldStatsList returns buffer as FieldStats
func (sr *FieldStatsReader) GetFieldStatsList() ([]*FieldStats, error) {
	var stats []*FieldStats
	err := json.Unmarshal(sr.buffer, &stats)
	if err != nil {
		return nil, merr.WrapErrParameterInvalid(
			"valid JSON",
			string(sr.buffer),
			err.Error())
	}

	return stats, nil
}

// DeserializeFieldStats deserialize @blobs as []*FieldStats
func DeserializeFieldStats(blobs []*Blob) ([]*FieldStats, error) {
	results := make([]*FieldStats, 0, len(blobs))
	for _, blob := range blobs {
		if len(blob.Value) == 0 {
			continue
		}
		sr := &FieldStatsReader{}
		sr.SetBuffer(blob.Value)
		stats, err := sr.GetFieldStats()
		if err != nil {
			return nil, err
		}
		results = append(results, stats)
	}
	return results, nil
}

func DeserializeFieldStatsList(blob *Blob) ([]*FieldStats, error) {
	if len(blob.Value) == 0 {
		return []*FieldStats{}, nil
	}
	sr := &FieldStatsReader{}
	sr.SetBuffer(blob.Value)
	stats, err := sr.GetFieldStatsList()
	if err != nil {
		return nil, err
	}
	return stats, nil
}

type segmentID int64

type PartitionStats struct {
	// todo move primaryKeyStats into segmentStats
	primaryKeyStats map[segmentID]PrimaryKeyStats
	segmentStats    map[segmentID][]FieldStats
}

// GetPrimaryKeyStats return PrimaryKeyStats of the partition
func (ps PartitionStats) GetPrimaryKeyStats() map[segmentID]PrimaryKeyStats {
	// todo: get from segmentStats after FieldStats is compatible to primaryKeyStats
	return ps.primaryKeyStats
}
