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
	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/util/bloomfilter"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// FieldStats contains statistics data for any column
// todo: compatible to PrimaryKeyStats
type FieldStats struct {
	FieldID   int64                            `json:"fieldID"`
	Type      schemapb.DataType                `json:"type"`
	Max       ScalarFieldValue                 `json:"max"`       // for scalar field
	Min       ScalarFieldValue                 `json:"min"`       // for scalar field
	BFType    bloomfilter.BFType               `json:"bfType"`    // for scalar field
	BF        bloomfilter.BloomFilterInterface `json:"bf"`        // for scalar field
	Centroids []VectorFieldValue               `json:"centroids"` // for vector field
}

func (stats *FieldStats) Clone() FieldStats {
	return FieldStats{
		FieldID:   stats.FieldID,
		Type:      stats.Type,
		Max:       stats.Max,
		Min:       stats.Min,
		BFType:    stats.BFType,
		BF:        stats.BF,
		Centroids: stats.Centroids,
	}
}

// UnmarshalJSON unmarshal bytes to FieldStats
func (stats *FieldStats) UnmarshalJSON(data []byte) error {
	var messageMap map[string]*json.RawMessage
	err := json.Unmarshal(data, &messageMap)
	if err != nil {
		return err
	}

	if value, ok := messageMap["fieldID"]; ok && value != nil {
		err = json.Unmarshal(*messageMap["fieldID"], &stats.FieldID)
		if err != nil {
			return err
		}
	} else {
		return errors.New("invalid fieldStats, no fieldID")
	}

	stats.Type = schemapb.DataType_Int64
	value, ok := messageMap["type"]
	if !ok {
		value, ok = messageMap["pkType"]
	}
	if ok && value != nil {
		var typeValue int32
		err = json.Unmarshal(*value, &typeValue)
		if err != nil {
			return err
		}
		if typeValue > 0 {
			stats.Type = schemapb.DataType(typeValue)
		}
	}

	isScalarField := false
	switch stats.Type {
	case schemapb.DataType_Int8:
		stats.Max = &Int8FieldValue{}
		stats.Min = &Int8FieldValue{}
		isScalarField = true
	case schemapb.DataType_Int16:
		stats.Max = &Int16FieldValue{}
		stats.Min = &Int16FieldValue{}
		isScalarField = true
	case schemapb.DataType_Int32:
		stats.Max = &Int32FieldValue{}
		stats.Min = &Int32FieldValue{}
		isScalarField = true
	case schemapb.DataType_Int64:
		stats.Max = &Int64FieldValue{}
		stats.Min = &Int64FieldValue{}
		isScalarField = true
	case schemapb.DataType_Float:
		stats.Max = &FloatFieldValue{}
		stats.Min = &FloatFieldValue{}
		isScalarField = true
	case schemapb.DataType_Double:
		stats.Max = &DoubleFieldValue{}
		stats.Min = &DoubleFieldValue{}
		isScalarField = true
	case schemapb.DataType_String:
		stats.Max = &StringFieldValue{}
		stats.Min = &StringFieldValue{}
		isScalarField = true
	case schemapb.DataType_VarChar:
		stats.Max = &VarCharFieldValue{}
		stats.Min = &VarCharFieldValue{}
		isScalarField = true
	case schemapb.DataType_FloatVector:
		stats.Centroids = []VectorFieldValue{}
		isScalarField = false
	default:
		// unsupported data type
	}

	if isScalarField {
		if value, ok := messageMap["max"]; ok && value != nil {
			err = json.Unmarshal(*messageMap["max"], &stats.Max)
			if err != nil {
				return err
			}
		}
		if value, ok := messageMap["min"]; ok && value != nil {
			err = json.Unmarshal(*messageMap["min"], &stats.Min)
			if err != nil {
				return err
			}
		}
		// compatible with primaryKeyStats
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
	} else {
		stats.initCentroids(data, stats.Type)
		err = json.Unmarshal(*messageMap["centroids"], &stats.Centroids)
		if err != nil {
			return err
		}
	}

	return nil
}

func (stats *FieldStats) initCentroids(data []byte, dataType schemapb.DataType) {
	type FieldStatsAux struct {
		FieldID   int64                            `json:"fieldID"`
		Type      schemapb.DataType                `json:"type"`
		Max       json.RawMessage                  `json:"max"`
		Min       json.RawMessage                  `json:"min"`
		BF        bloomfilter.BloomFilterInterface `json:"bf"`
		Centroids []json.RawMessage                `json:"centroids"`
	}
	// Unmarshal JSON into the auxiliary struct
	var aux FieldStatsAux
	if err := json.Unmarshal(data, &aux); err != nil {
		return
	}
	for i := 0; i < len(aux.Centroids); i++ {
		switch dataType {
		case schemapb.DataType_FloatVector:
			stats.Centroids = append(stats.Centroids, &FloatVectorFieldValue{})
		default:
			// other vector datatype
		}
	}
}

func (stats *FieldStats) UpdateByMsgs(msgs FieldData) {
	switch stats.Type {
	case schemapb.DataType_Int8:
		data := msgs.(*Int8FieldData).Data
		// return error: msgs must has one element at least
		if len(data) < 1 {
			return
		}
		b := make([]byte, 8)
		for _, int8Value := range data {
			pk := NewInt8FieldValue(int8Value)
			stats.UpdateMinMax(pk)
			common.Endian.PutUint64(b, uint64(int8Value))
			stats.BF.Add(b)
		}
	case schemapb.DataType_Int16:
		data := msgs.(*Int16FieldData).Data
		// return error: msgs must has one element at least
		if len(data) < 1 {
			return
		}
		b := make([]byte, 8)
		for _, int16Value := range data {
			pk := NewInt16FieldValue(int16Value)
			stats.UpdateMinMax(pk)
			common.Endian.PutUint64(b, uint64(int16Value))
			stats.BF.Add(b)
		}
	case schemapb.DataType_Int32:
		data := msgs.(*Int32FieldData).Data
		// return error: msgs must has one element at least
		if len(data) < 1 {
			return
		}
		b := make([]byte, 8)
		for _, int32Value := range data {
			pk := NewInt32FieldValue(int32Value)
			stats.UpdateMinMax(pk)
			common.Endian.PutUint64(b, uint64(int32Value))
			stats.BF.Add(b)
		}
	case schemapb.DataType_Int64:
		data := msgs.(*Int64FieldData).Data
		// return error: msgs must has one element at least
		if len(data) < 1 {
			return
		}
		b := make([]byte, 8)
		for _, int64Value := range data {
			pk := NewInt64FieldValue(int64Value)
			stats.UpdateMinMax(pk)
			common.Endian.PutUint64(b, uint64(int64Value))
			stats.BF.Add(b)
		}
	case schemapb.DataType_Float:
		data := msgs.(*FloatFieldData).Data
		// return error: msgs must has one element at least
		if len(data) < 1 {
			return
		}
		b := make([]byte, 8)
		for _, floatValue := range data {
			pk := NewFloatFieldValue(floatValue)
			stats.UpdateMinMax(pk)
			common.Endian.PutUint64(b, uint64(floatValue))
			stats.BF.Add(b)
		}
	case schemapb.DataType_Double:
		data := msgs.(*DoubleFieldData).Data
		// return error: msgs must has one element at least
		if len(data) < 1 {
			return
		}
		b := make([]byte, 8)
		for _, doubleValue := range data {
			pk := NewDoubleFieldValue(doubleValue)
			stats.UpdateMinMax(pk)
			common.Endian.PutUint64(b, uint64(doubleValue))
			stats.BF.Add(b)
		}
	case schemapb.DataType_String:
		data := msgs.(*StringFieldData).Data
		// return error: msgs must has one element at least
		if len(data) < 1 {
			return
		}
		for _, str := range data {
			pk := NewStringFieldValue(str)
			stats.UpdateMinMax(pk)
			stats.BF.AddString(str)
		}
	case schemapb.DataType_VarChar:
		data := msgs.(*StringFieldData).Data
		// return error: msgs must has one element at least
		if len(data) < 1 {
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
	switch stats.Type {
	case schemapb.DataType_Int8:
		data := pk.GetValue().(int8)
		b := make([]byte, 8)
		common.Endian.PutUint64(b, uint64(data))
		stats.BF.Add(b)
	case schemapb.DataType_Int16:
		data := pk.GetValue().(int16)
		b := make([]byte, 8)
		common.Endian.PutUint64(b, uint64(data))
		stats.BF.Add(b)
	case schemapb.DataType_Int32:
		data := pk.GetValue().(int32)
		b := make([]byte, 8)
		common.Endian.PutUint64(b, uint64(data))
		stats.BF.Add(b)
	case schemapb.DataType_Int64:
		data := pk.GetValue().(int64)
		b := make([]byte, 8)
		common.Endian.PutUint64(b, uint64(data))
		stats.BF.Add(b)
	case schemapb.DataType_Float:
		data := pk.GetValue().(float32)
		b := make([]byte, 8)
		common.Endian.PutUint64(b, uint64(data))
		stats.BF.Add(b)
	case schemapb.DataType_Double:
		data := pk.GetValue().(float64)
		b := make([]byte, 8)
		common.Endian.PutUint64(b, uint64(data))
		stats.BF.Add(b)
	case schemapb.DataType_String:
		data := pk.GetValue().(string)
		stats.BF.AddString(data)
	case schemapb.DataType_VarChar:
		data := pk.GetValue().(string)
		stats.BF.AddString(data)
	default:
		// todo support vector field
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

// SetVectorCentroids update centroids value
func (stats *FieldStats) SetVectorCentroids(centroids ...VectorFieldValue) {
	stats.Centroids = centroids
}

func NewFieldStats(fieldID int64, pkType schemapb.DataType, rowNum int64) (*FieldStats, error) {
	if pkType == schemapb.DataType_FloatVector {
		return &FieldStats{
			FieldID: fieldID,
			Type:    pkType,
		}, nil
	}
	bfType := paramtable.Get().CommonCfg.BloomFilterType.GetValue()
	return &FieldStats{
		FieldID: fieldID,
		Type:    pkType,
		BFType:  bloomfilter.BFTypeFromString(bfType),
		BF: bloomfilter.NewBloomFilterWithType(
			uint(rowNum),
			paramtable.Get().CommonCfg.MaxBloomFalsePositive.GetAsFloat(),
			bfType),
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

// GenerateByData writes data from @msgs with @fieldID to @buffer
func (sw *FieldStatsWriter) GenerateByData(fieldID int64, pkType schemapb.DataType, msgs ...FieldData) error {
	statsList := make([]*FieldStats, 0)

	bfType := paramtable.Get().CommonCfg.BloomFilterType.GetValue()
	for _, msg := range msgs {
		stats := &FieldStats{
			FieldID: fieldID,
			Type:    pkType,
			BFType:  bloomfilter.BFTypeFromString(bfType),
			BF: bloomfilter.NewBloomFilterWithType(
				uint(msg.RowNum()),
				paramtable.Get().CommonCfg.MaxBloomFalsePositive.GetAsFloat(),
				bfType),
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

// GetFieldStatsList returns buffer as FieldStats
func (sr *FieldStatsReader) GetFieldStatsList() ([]*FieldStats, error) {
	var statsList []*FieldStats
	err := json.Unmarshal(sr.buffer, &statsList)
	if err != nil {
		// Compatible to PrimaryKey Stats
		stats := &FieldStats{}
		errNew := json.Unmarshal(sr.buffer, &stats)
		if errNew != nil {
			return nil, merr.WrapErrParameterInvalid("valid JSON", string(sr.buffer), err.Error())
		}
		return []*FieldStats{stats}, nil
	}

	return statsList, nil
}

func DeserializeFieldStats(blob *Blob) ([]*FieldStats, error) {
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
