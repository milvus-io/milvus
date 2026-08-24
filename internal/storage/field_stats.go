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
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/util/bloomfilter"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
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
		return merr.WrapErrServiceInternalMsg("invalid fieldStats, no fieldID")
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
	case schemapb.DataType_Int64, schemapb.DataType_Timestamptz:
		stats.Max = &Int64FieldValue{}
		stats.Min = &Int64FieldValue{}
		isScalarField = true
	case schemapb.DataType_Date:
		stats.Max = &Int32FieldValue{}
		stats.Min = &Int32FieldValue{}
		isScalarField = true
	case schemapb.DataType_Time:
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
				mlog.Warn(context.TODO(), "Failed to unmarshal bloom filter, use AlwaysTrueBloomFilter instead of return err", mlog.Err(err))
				bf = bloomfilter.AlwaysTrueBloomFilter
			}
			stats.BF = bf
		}
	} else {
		// "centroids" carries no omitempty, so a snapshot Milvus wrote always has the
		// key, null when there is nothing to store. Types without centroid support also
		// reach this branch, so only a float vector may read a missing key as corruption.
		value, ok := messageMap["centroids"]
		switch {
		case value != nil:
			if err := stats.unmarshalCentroids(*value, stats.Type); err != nil {
				return err
			}
		case !ok && stats.Type == schemapb.DataType_FloatVector:
			// Accepting this silently hands segment pruning an empty centroid set,
			// which degrades to a full scan with no signal.
			return merr.WrapErrDataIntegrityMsg("field stats of field %d has no centroids key", stats.FieldID)
		}
	}

	return nil
}

// unmarshalCentroids decodes the centroid array into the concrete VectorFieldValue
// implementation chosen by dataType.
//
// Each centroid is decoded explicitly rather than pre-allocating concrete values into
// the interface slice and letting the decoder fill them in place. That older trick
// needed a pre-pass over the whole blob, which sonic's arm64 decoder aborts: it rejects
// null into an interface whose method set carries UnmarshalJSON, and a vector field
// always serializes "bf" as null. Nothing got pre-allocated, so partition stats with
// centroids failed to load on arm64 and pruning silently fell back to a full scan
// (#51869).
func (stats *FieldStats) unmarshalCentroids(data json.RawMessage, dataType schemapb.DataType) error {
	var rawCentroids []json.RawMessage
	if err := json.Unmarshal(data, &rawCentroids); err != nil {
		return merr.WrapErrDataIntegrity(err, "field stats of field %d has a malformed centroids array", stats.FieldID)
	}

	centroids := make([]VectorFieldValue, 0, len(rawCentroids))
	for i, rawCentroid := range rawCentroids {
		switch dataType {
		case schemapb.DataType_FloatVector:
			centroid := &FloatVectorFieldValue{}
			if err := json.Unmarshal(rawCentroid, centroid); err != nil {
				return merr.WrapErrDataIntegrity(err, "field stats of field %d has a malformed centroid at index %d", stats.FieldID, i)
			}
			centroids = append(centroids, centroid)
		default:
			// Fail loudly rather than dropping the centroids: a silently empty
			// snapshot degrades segment pruning to a full scan with no signal.
			return merr.WrapErrDataIntegrityMsg("field stats of field %d has centroids for unsupported data type %s",
				stats.FieldID, dataType.String())
		}
	}

	stats.Centroids = centroids
	return nil
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
	case schemapb.DataType_Timestamptz:
		data := msgs.(*TimestamptzFieldData).Data
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
	case schemapb.DataType_Date:
		data := msgs.(*DateFieldData).Data
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
	case schemapb.DataType_Time:
		data := msgs.(*TimeFieldData).Data
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
	case schemapb.DataType_Int64, schemapb.DataType_Timestamptz:
		data := pk.GetValue().(int64)
		b := make([]byte, 8)
		common.Endian.PutUint64(b, uint64(data))
		stats.BF.Add(b)
	case schemapb.DataType_Date:
		data := pk.GetValue().(int32)
		b := make([]byte, 8)
		common.Endian.PutUint64(b, uint64(data))
		stats.BF.Add(b)
	case schemapb.DataType_Time:
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
			return nil, merr.WrapErrDataIntegrity(err, "FieldStats list unmarshal failed")
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
