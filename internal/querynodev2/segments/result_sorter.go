package segments

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type byPK struct {
	r *segcorepb.RetrieveResults
}

func (s *byPK) Len() int {
	if s.r == nil {
		return 0
	}

	s.r.GetIds().GetIdField()
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
		case *schemapb.ScalarField_JsonData:
			data := sd.JsonData.Data
			data[i], data[j] = data[j], data[i]
		case *schemapb.ScalarField_ArrayData:
			data := sd.ArrayData.Data
			data[i], data[j] = data[j], data[i]
		default:
			errMsg := "undefined data type " + field.Type.String()
			panic(errMsg)
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
