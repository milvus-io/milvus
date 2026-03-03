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
	"maps"
	"math"
	"path"

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
		return nil, merr.WrapErrParameterInvalidMsg("zero or negative row num %d", rowNum)
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

func (m *BM25Stats) SerializeToWriter(w io.Writer) error {
	if err := binary.Write(w, common.Endian, BM25VERSION); err != nil {
		return err
	}

	if err := binary.Write(w, common.Endian, m.numRow); err != nil {
		return err
	}

	if err := binary.Write(w, common.Endian, m.numToken); err != nil {
		return err
	}

	for key, value := range m.rowsWithToken {
		if err := binary.Write(w, common.Endian, key); err != nil {
			return err
		}

		if err := binary.Write(w, common.Endian, value); err != nil {
			return err
		}
	}

	return nil
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

	var key uint32
	var value int32
	for i := 0; i < dim; i++ {
		if err := binary.Read(buffer, common.Endian, &key); err != nil {
			return err
		}

		if err := binary.Read(buffer, common.Endian, &value); err != nil {
			return err
		}
		m.rowsWithToken[key] += value
	}

	m.numRow += numRow
	m.numToken += tokenNum
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

// DeserializeBloomFilterStats auto-detects compound vs default stats format
// from the paths and deserializes accordingly. Compound format is detected
// when any path has a basename matching CompoundStatsType.LogIdx().
func DeserializeBloomFilterStats(paths []string, blobs []*Blob) ([]*PrimaryKeyStats, error) {
	for i, p := range paths {
		_, logidx := path.Split(p)
		if logidx == CompoundStatsType.LogIdx() {
			return DeserializeStatsList(blobs[i])
		}
	}
	return DeserializeStats(blobs)
}

// DeserializeStats deserializes @blobs as []*PrimaryKeyStats
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
	if IsBinaryStatsFormat(blob.Value) {
		return DeserializeBinaryStats(blob.Value)
	}
	sr := &StatsReader{}
	sr.SetBuffer(blob.Value)
	stats, err := sr.GetPrimaryKeyStatsList()
	if err != nil {
		return nil, err
	}
	return stats, nil
}

// Binary stats format constants
const (
	BinaryStatsMagic      = "mpkstats"
	BinaryStatsVersion    = uint32(1)
	BinaryFileHeaderSize  = 64
	BinaryEntryHeaderSize = 64
	BinaryBlockSize       = 64 // 16 uint32 × 4 bytes
)

// IsBinaryStatsFormat checks if the data starts with the binary stats magic number.
func IsBinaryStatsFormat(data []byte) bool {
	return len(data) >= 8 && string(data[:8]) == BinaryStatsMagic
}

// SerializeBinaryStats serializes a list of PrimaryKeyStats into the binary format.
//
// File Header (64 bytes):
//
//	[0:8]    magic    "mpkstats"
//	[8:12]   version  uint32 LE (= 1)
//	[12:16]  numEntries  uint32 LE
//	[16:20]  pkType   uint32 LE
//	[20:24]  fieldID  int32 LE
//	[24:64]  reserved
//
// For each entry:
//
//	Entry Header (64 bytes):
//	  [0:4]    numBlocks  uint32 LE
//	  [4:8]    k          uint32 LE (hash function count)
//	  For Int64:
//	    [8:16]   minPk  int64 LE
//	    [16:24]  maxPk  int64 LE
//	    [24:64]  reserved
//	  For VarChar:
//	    [8:12]   pkDataSize uint32 LE (total bytes of PK data after header)
//	    [12:64]  reserved
//	VarChar PK Data (variable length, only present for VarChar):
//	  [4-byte minLen][minData...][4-byte maxLen][maxData...]
//	BF Data:
//	  numBlocks × 64 bytes
func SerializeBinaryStats(stats []*PrimaryKeyStats) ([]byte, error) {
	if len(stats) == 0 {
		return nil, errors.New("cannot serialize empty stats list")
	}

	// Calculate total size
	totalSize := BinaryFileHeaderSize
	for _, s := range stats {
		numBlocks := uint32(s.BF.Cap() / 512)
		entrySize := BinaryEntryHeaderSize + int(numBlocks)*BinaryBlockSize
		if schemapb.DataType(s.PkType) == schemapb.DataType_VarChar {
			entrySize += varCharPKDataSize(s.MinPk, s.MaxPk)
		}
		totalSize += entrySize
	}

	buf := make([]byte, totalSize)

	// Write file header
	copy(buf[0:8], BinaryStatsMagic)
	binary.LittleEndian.PutUint32(buf[8:12], BinaryStatsVersion)
	binary.LittleEndian.PutUint32(buf[12:16], uint32(len(stats)))
	binary.LittleEndian.PutUint32(buf[16:20], uint32(stats[0].PkType))
	binary.LittleEndian.PutUint32(buf[20:24], uint32(stats[0].FieldID))

	offset := BinaryFileHeaderSize
	for _, s := range stats {
		numBlocks := uint32(s.BF.Cap() / 512)
		k := uint32(s.BF.K())

		// Write entry header
		binary.LittleEndian.PutUint32(buf[offset:offset+4], numBlocks)
		binary.LittleEndian.PutUint32(buf[offset+4:offset+8], k)

		// Write min/max PK
		switch schemapb.DataType(s.PkType) {
		case schemapb.DataType_Int64:
			pkOff := offset + 8
			if s.MinPk != nil {
				binary.LittleEndian.PutUint64(buf[pkOff:pkOff+8], uint64(s.MinPk.(*Int64PrimaryKey).Value))
			}
			if s.MaxPk != nil {
				binary.LittleEndian.PutUint64(buf[pkOff+8:pkOff+16], uint64(s.MaxPk.(*Int64PrimaryKey).Value))
			}
			offset += BinaryEntryHeaderSize
		case schemapb.DataType_VarChar:
			pkSize := varCharPKDataSize(s.MinPk, s.MaxPk)
			binary.LittleEndian.PutUint32(buf[offset+8:offset+12], uint32(pkSize))
			offset += BinaryEntryHeaderSize
			writeVarCharPK(buf[offset:offset+pkSize], s.MinPk, s.MaxPk)
			offset += pkSize
		default:
			offset += BinaryEntryHeaderSize
		}

		// Write BF block data - marshal from the bloom filter
		if err := marshalBFBlocks(buf[offset:offset+int(numBlocks)*BinaryBlockSize], s.BF); err != nil {
			return nil, fmt.Errorf("failed to marshal BF blocks for entry: %w", err)
		}
		offset += int(numBlocks) * BinaryBlockSize
	}

	return buf, nil
}

// varCharPKDataSize returns the total bytes needed to encode min and max VarChar PKs.
func varCharPKDataSize(minPk, maxPk PrimaryKey) int {
	size := 8 // two 4-byte length headers
	if minPk != nil {
		size += len(minPk.(*VarCharPrimaryKey).Value)
	}
	if maxPk != nil {
		size += len(maxPk.(*VarCharPrimaryKey).Value)
	}
	return size
}

// writeVarCharPK writes min and max VarChar PKs into the provided buffer.
// Layout: [4-byte minLen][minData...][4-byte maxLen][maxData...]
// The buffer must be large enough to hold all PK data.
func writeVarCharPK(buf []byte, minPk, maxPk PrimaryKey) {
	off := 0
	if minPk != nil {
		s := minPk.(*VarCharPrimaryKey).Value
		binary.LittleEndian.PutUint32(buf[off:off+4], uint32(len(s)))
		off += 4
		copy(buf[off:off+len(s)], s)
		off += len(s)
	} else {
		binary.LittleEndian.PutUint32(buf[off:off+4], 0)
		off += 4
	}
	if maxPk != nil {
		s := maxPk.(*VarCharPrimaryKey).Value
		binary.LittleEndian.PutUint32(buf[off:off+4], uint32(len(s)))
		off += 4
		copy(buf[off:off+len(s)], s)
	} else {
		binary.LittleEndian.PutUint32(buf[off:off+4], 0)
	}
}

// marshalBFBlocks serializes a bloom filter's block data into the provided buffer.
// Uses blobloom's Dump format to extract block data.
func marshalBFBlocks(dst []byte, bf bloomfilter.BloomFilterInterface) error {
	if bf.Type() != bloomfilter.BlockedBF {
		return fmt.Errorf("binary stats format only supports BlockedBF, got %v", bf.Type())
	}

	blockData, err := bloomfilter.DumpBlockData(bf)
	if err != nil {
		return err
	}
	copy(dst, blockData)
	return nil
}

// DeserializeBinaryStats parses binary stats format and creates MmapBloomFilter instances.
// The data can be mmap'd memory, and MmapBloomFilter will reference it directly (zero-copy).
func DeserializeBinaryStats(data []byte) ([]*PrimaryKeyStats, error) {
	if len(data) < BinaryFileHeaderSize {
		return nil, errors.New("binary stats data too short for header")
	}

	if string(data[:8]) != BinaryStatsMagic {
		return nil, errors.New("invalid binary stats magic")
	}

	version := binary.LittleEndian.Uint32(data[8:12])
	if version != BinaryStatsVersion {
		return nil, fmt.Errorf("unsupported binary stats version: %d", version)
	}

	numEntries := binary.LittleEndian.Uint32(data[12:16])
	pkType := int64(binary.LittleEndian.Uint32(data[16:20]))
	fieldID := int64(int32(binary.LittleEndian.Uint32(data[20:24])))

	results := make([]*PrimaryKeyStats, 0, numEntries)
	offset := BinaryFileHeaderSize

	for i := uint32(0); i < numEntries; i++ {
		if offset+BinaryEntryHeaderSize > len(data) {
			return nil, fmt.Errorf("binary stats data truncated at entry %d", i)
		}

		numBlocks := binary.LittleEndian.Uint32(data[offset : offset+4])
		k := int(binary.LittleEndian.Uint32(data[offset+4 : offset+8]))

		// Parse min/max PK
		var minPk, maxPk PrimaryKey
		var pkDataSize int

		switch schemapb.DataType(pkType) {
		case schemapb.DataType_Int64:
			pkOff := offset + 8
			minVal := int64(binary.LittleEndian.Uint64(data[pkOff : pkOff+8]))
			maxVal := int64(binary.LittleEndian.Uint64(data[pkOff+8 : pkOff+16]))
			minPk = NewInt64PrimaryKey(minVal)
			maxPk = NewInt64PrimaryKey(maxVal)
		case schemapb.DataType_VarChar:
			pkDataSize = int(binary.LittleEndian.Uint32(data[offset+8 : offset+12]))
			pkStart := offset + BinaryEntryHeaderSize
			if pkStart+pkDataSize > len(data) {
				return nil, fmt.Errorf("binary stats data truncated at entry %d VarChar PK data", i)
			}
			var err error
			minPk, maxPk, err = readVarCharPKs(data[pkStart : pkStart+pkDataSize])
			if err != nil {
				return nil, fmt.Errorf("failed to read VarChar PKs at entry %d: %w", i, err)
			}
		}

		dataOffset := offset + BinaryEntryHeaderSize + pkDataSize
		dataEnd := dataOffset + int(numBlocks)*BinaryBlockSize
		if dataEnd > len(data) {
			return nil, fmt.Errorf("binary stats data truncated at entry %d block data", i)
		}

		// Create MmapBloomFilter directly referencing the data slice
		bf := bloomfilter.NewMmapBloomFilter(data, dataOffset, numBlocks, k)

		results = append(results, &PrimaryKeyStats{
			FieldID: fieldID,
			PkType:  pkType,
			BFType:  bloomfilter.BlockedBF,
			BF:      bf,
			MinPk:   minPk,
			MaxPk:   maxPk,
		})

		offset = dataEnd
	}

	return results, nil
}

// readVarCharPKs reads min and max VarChar primary keys from the buffer.
func readVarCharPKs(buf []byte) (PrimaryKey, PrimaryKey, error) {
	off := 0
	if off+4 > len(buf) {
		return nil, nil, errors.New("buffer too short for minPk length")
	}
	minLen := int(binary.LittleEndian.Uint32(buf[off : off+4]))
	off += 4
	if off+minLen > len(buf) {
		return nil, nil, errors.New("buffer too short for minPk data")
	}
	minPk := NewVarCharPrimaryKey(string(buf[off : off+minLen]))
	off += minLen

	if off+4 > len(buf) {
		return nil, nil, errors.New("buffer too short for maxPk length")
	}
	maxLen := int(binary.LittleEndian.Uint32(buf[off : off+4]))
	off += 4
	if off+maxLen > len(buf) {
		return nil, nil, errors.New("buffer too short for maxPk data")
	}
	maxPk := NewVarCharPrimaryKey(string(buf[off : off+maxLen]))

	return minPk, maxPk, nil
}
