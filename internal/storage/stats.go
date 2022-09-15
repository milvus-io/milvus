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

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/milvus-io/milvus/api/schemapb"
	"github.com/milvus-io/milvus/internal/common"
)

const (
	// TODO silverxia maybe need set from config
	bloomFilterSize       uint    = 100000
	maxBloomFalsePositive float64 = 0.005
)

// PrimaryKeyStats contains statistics data for pk column
type PrimaryKeyStats struct {
	FieldID int64              `json:"fieldID"`
	Max     int64              `json:"max"` // useless, will delete
	Min     int64              `json:"min"` //useless, will delete
	BF      *bloom.BloomFilter `json:"bf"`
	PkType  int64              `json:"pkType"`
	MaxPk   PrimaryKey         `json:"maxPk"`
	MinPk   PrimaryKey         `json:"minPk"`
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

	stats.BF = bloom.NewWithEstimates(bloomFilterSize, maxBloomFalsePositive)
	if bfMessage, ok := messageMap["bf"]; ok && bfMessage != nil {
		err = stats.BF.UnmarshalJSON(*bfMessage)
		if err != nil {
			return err
		}
	}

	return nil
}

// updatePk update minPk and maxPk value
func (stats *PrimaryKeyStats) updatePk(pk PrimaryKey) {
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

// StatsWriter writes stats to buffer
type StatsWriter struct {
	buffer []byte
}

// GetBuffer returns buffer
func (sw *StatsWriter) GetBuffer() []byte {
	return sw.buffer
}

// generatePrimaryKeyStats writes Int64Stats from @msgs with @fieldID to @buffer
func (sw *StatsWriter) generatePrimaryKeyStats(fieldID int64, pkType schemapb.DataType, msgs FieldData) error {
	stats := &PrimaryKeyStats{
		FieldID: fieldID,
		PkType:  int64(pkType),
	}

	stats.BF = bloom.NewWithEstimates(bloomFilterSize, maxBloomFalsePositive)
	switch pkType {
	case schemapb.DataType_Int64:
		data := msgs.(*Int64FieldData).Data
		if len(data) < 1 {
			// return error: msgs must has one element at least
			return nil
		}

		b := make([]byte, 8)
		for _, int64Value := range data {
			pk := NewInt64PrimaryKey(int64Value)
			stats.updatePk(pk)
			common.Endian.PutUint64(b, uint64(int64Value))
			stats.BF.Add(b)
		}
	case schemapb.DataType_VarChar:
		data := msgs.(*StringFieldData).Data
		if len(data) < 1 {
			// return error: msgs must has one element at least
			return nil
		}

		for _, str := range data {
			pk := NewVarCharPrimaryKey(str)
			stats.updatePk(pk)
			stats.BF.AddString(str)
		}
	default:
		//TODO::
	}

	b, err := json.Marshal(stats)
	if err != nil {
		return err
	}
	sw.buffer = b

	return nil
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
		return nil, err
	}

	return stats, nil
}

// DeserializeStats deserialize @blobs as []*PrimaryKeyStats
func DeserializeStats(blobs []*Blob) ([]*PrimaryKeyStats, error) {
	results := make([]*PrimaryKeyStats, 0, len(blobs))
	for _, blob := range blobs {
		if blob.Value == nil {
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
