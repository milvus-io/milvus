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

package entity

import (
	"encoding/binary"
	"math"
	"sort"

	"github.com/cockroachdb/errors"
)

type SparseEmbedding interface {
	Dim() int // the dimension
	Len() int // the actual items in this vector
	Get(idx int) (pos uint32, value float32, ok bool)
	Serialize() []byte
	FieldType() FieldType
}

var (
	_ SparseEmbedding = sliceSparseEmbedding{}
	_ Vector          = sliceSparseEmbedding{}
)

type sliceSparseEmbedding struct {
	positions []uint32
	values    []float32
	dim       int
	len       int
}

func (e sliceSparseEmbedding) Dim() int {
	return e.dim
}

func (e sliceSparseEmbedding) Len() int {
	return e.len
}

func (e sliceSparseEmbedding) FieldType() FieldType {
	return FieldTypeSparseVector
}

func (e sliceSparseEmbedding) Get(idx int) (uint32, float32, bool) {
	if idx < 0 || idx >= e.len {
		return 0, 0, false
	}
	return e.positions[idx], e.values[idx], true
}

func (e sliceSparseEmbedding) Serialize() []byte {
	row := make([]byte, 8*e.Len())
	for idx := 0; idx < e.Len(); idx++ {
		pos, value, _ := e.Get(idx)
		binary.LittleEndian.PutUint32(row[idx*8:], pos)
		binary.LittleEndian.PutUint32(row[idx*8+4:], math.Float32bits(value))
	}
	return row
}

// Less implements sort.Interce
func (e sliceSparseEmbedding) Less(i, j int) bool {
	return e.positions[i] < e.positions[j]
}

func (e sliceSparseEmbedding) Swap(i, j int) {
	e.positions[i], e.positions[j] = e.positions[j], e.positions[i]
	e.values[i], e.values[j] = e.values[j], e.values[i]
}

func DeserializeSliceSparseEmbedding(bs []byte) (sliceSparseEmbedding, error) {
	length := len(bs)
	if length%8 != 0 {
		return sliceSparseEmbedding{}, errors.New("not valid sparse embedding bytes")
	}

	length /= 8

	result := sliceSparseEmbedding{
		positions: make([]uint32, length),
		values:    make([]float32, length),
		len:       length,
	}

	for i := 0; i < length; i++ {
		result.positions[i] = binary.LittleEndian.Uint32(bs[i*8 : i*8+4])
		result.values[i] = math.Float32frombits(binary.LittleEndian.Uint32(bs[i*8+4 : i*8+8]))
	}
	return result, nil
}

func NewSliceSparseEmbedding(positions []uint32, values []float32) (SparseEmbedding, error) {
	if len(positions) != len(values) {
		return nil, errors.New("invalid sparse embedding input, positions shall have same number of values")
	}

	se := sliceSparseEmbedding{
		positions: positions,
		values:    values,
		len:       len(positions),
	}

	sort.Sort(se)

	if se.len > 0 {
		se.dim = int(se.positions[se.len-1]) + 1
	}

	return se, nil
}
