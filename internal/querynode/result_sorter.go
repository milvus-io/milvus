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

package querynode

import (
	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

// ----------------------------------------------------------------------
// byPK

type byPK struct {
	r *segcorepb.RetrieveResults
}

func (s *byPK) Len() int {
	if s.r == nil {
		return 0
	}

	switch id := s.r.GetIds().GetIdField().(type) {
	case *schemapb.IDs_IntId:
		return len(id.IntId.GetData())
	case *schemapb.IDs_StrId:
		return len(id.StrId.GetData())
	}
	return 0
}

func (s *byPK) Swap(i, j int) {
	s.r.Offset[i], s.r.Offset[j] = s.r.Offset[j], s.r.Offset[i]

	typeutil.SwapPK(s.r.GetIds(), i, j)

	for _, field := range s.r.GetFieldsData() {
		swapFieldData(field, i, j)
	}
}

func (s *byPK) Less(i, j int) bool {
	return typeutil.ComparePKInSlice(s.r.GetIds(), i, j)
}

// ----------------------------------------------------------------------
// byTS

type byTS struct {
	r  *segcorepb.RetrieveResults
	ts []Timestamp
}

func GetTimeStampFieldsData(s *segcorepb.RetrieveResults) []Timestamp {
	if s.FieldsData == nil {
		return nil
	}
	for _, fieldData := range s.FieldsData {
		fieldID := fieldData.FieldId
		if fieldID == common.TimeStampField {
			res := fieldData.GetScalars().GetLongData().Data
			timeStamp := make([]uint64, len(res))
			for i, v := range res {
				timeStamp[i] = uint64(v)
			}
			return timeStamp
		}
	}
	return nil
}

func swapFieldData(field *schemapb.FieldData, i int, j int) {
	switch field.GetField().(type) {
	case *schemapb.FieldData_Scalars:
		switch sd := field.GetScalars().GetData().(type) {
		case *schemapb.ScalarField_BoolData:
			data := sd.BoolData.Data
			data[i], data[j] = data[j], data[i]
		case *schemapb.ScalarField_IntData:
			data := sd.IntData.Data
			data[i], data[j] = data[j], data[i]
		case *schemapb.ScalarField_LongData:
			data := sd.LongData.Data
			data[i], data[j] = data[j], data[i]
		case *schemapb.ScalarField_FloatData:
			data := sd.FloatData.Data
			data[i], data[j] = data[j], data[i]
		case *schemapb.ScalarField_DoubleData:
			data := sd.DoubleData.Data
			data[i], data[j] = data[j], data[i]
		case *schemapb.ScalarField_StringData:
			data := sd.StringData.Data
			data[i], data[j] = data[j], data[i]
		}
	case *schemapb.FieldData_Vectors:
		dim := int(field.GetVectors().GetDim())
		switch vd := field.GetVectors().GetData().(type) {
		case *schemapb.VectorField_BinaryVector:
			steps := dim / 8 // dim for binary vector must be multiplier of 8
			srcToSwap := vd.BinaryVector[i*steps : (i+1)*steps]
			dstToSwap := vd.BinaryVector[j*steps : (j+1)*steps]

			for k := range srcToSwap {
				srcToSwap[k], dstToSwap[k] = dstToSwap[k], srcToSwap[k]
			}
		case *schemapb.VectorField_FloatVector:
			srcToSwap := vd.FloatVector.Data[i*dim : (i+1)*dim]
			dstToSwap := vd.FloatVector.Data[j*dim : (j+1)*dim]

			for k := range srcToSwap {
				srcToSwap[k], dstToSwap[k] = dstToSwap[k], srcToSwap[k]
			}
		}
	default:
		errMsg := "undefined data type " + field.Type.String()
		panic(errMsg)
	}
}

func (s *byTS) Len() int {
	if s.r == nil {
		return 0
	}
	return len(s.ts)
}

func (s *byTS) Swap(i, j int) {
	s.r.Offset[i], s.r.Offset[j] = s.r.Offset[j], s.r.Offset[i]

	typeutil.SwapPK(s.r.GetIds(), i, j)
	typeutil.SwapTS(s.ts, i, j)

	for _, field := range s.r.GetFieldsData() {
		swapFieldData(field, i, j)
	}
}

func (s *byTS) Less(i, j int) bool {
	return typeutil.CompareTimeStampInSlice(s.ts, i, j)
}
