package funcutil

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

func FieldDataToPlaceholderGroupBytes(fieldData *schemapb.FieldData) ([]byte, error) {
	placeholderValue, err := fieldDataToPlaceholderValue(fieldData)
	if err != nil {
		return nil, err
	}

	placeholderGroup := &commonpb.PlaceholderGroup{
		Placeholders: []*commonpb.PlaceholderValue{placeholderValue},
	}

	bytes, _ := proto.Marshal(placeholderGroup)
	return bytes, nil
}

func fieldDataToPlaceholderValue(fieldData *schemapb.FieldData) (*commonpb.PlaceholderValue, error) {
	switch fieldData.Type {
	case schemapb.DataType_FloatVector:
		vectors := fieldData.GetVectors()
		x, ok := vectors.GetData().(*schemapb.VectorField_FloatVector)
		if !ok {
			return nil, errors.New("vector data is not schemapb.VectorField_FloatVector")
		}

		placeholderValue := &commonpb.PlaceholderValue{
			Tag:    "$0",
			Type:   commonpb.PlaceholderType_FloatVector,
			Values: flattenedFloatVectorsToByteVectors(x.FloatVector.Data, int(vectors.Dim)),
		}
		return placeholderValue, nil
	case schemapb.DataType_BinaryVector:
		vectors := fieldData.GetVectors()
		x, ok := vectors.GetData().(*schemapb.VectorField_BinaryVector)
		if !ok {
			return nil, errors.New("vector data is not schemapb.VectorField_BinaryVector")
		}
		placeholderValue := &commonpb.PlaceholderValue{
			Tag:    "$0",
			Type:   commonpb.PlaceholderType_BinaryVector,
			Values: flattenedByteVectorsToByteVectors(x.BinaryVector, int(vectors.Dim)),
		}
		return placeholderValue, nil
	case schemapb.DataType_Float16Vector:
		vectors := fieldData.GetVectors()
		x, ok := vectors.GetData().(*schemapb.VectorField_Float16Vector)
		if !ok {
			return nil, errors.New("vector data is not schemapb.VectorField_Float16Vector")
		}
		placeholderValue := &commonpb.PlaceholderValue{
			Tag:    "$0",
			Type:   commonpb.PlaceholderType_Float16Vector,
			Values: flattenedFloat16VectorsToByteVectors(x.Float16Vector, int(vectors.Dim)),
		}
		return placeholderValue, nil
	case schemapb.DataType_BFloat16Vector:
		vectors := fieldData.GetVectors()
		x, ok := vectors.GetData().(*schemapb.VectorField_Bfloat16Vector)
		if !ok {
			return nil, errors.New("vector data is not schemapb.VectorField_BFloat16Vector")
		}
		placeholderValue := &commonpb.PlaceholderValue{
			Tag:    "$0",
			Type:   commonpb.PlaceholderType_BFloat16Vector,
			Values: flattenedFloat16VectorsToByteVectors(x.Bfloat16Vector, int(vectors.Dim)),
		}
		return placeholderValue, nil
	case schemapb.DataType_SparseFloatVector:
		vectors, ok := fieldData.GetVectors().GetData().(*schemapb.VectorField_SparseFloatVector)
		if !ok {
			return nil, errors.New("vector data is not schemapb.VectorField_SparseFloatVector")
		}
		vec := vectors.SparseFloatVector
		bytes, err := proto.Marshal(vec)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal schemapb.SparseFloatArray to bytes: %w", err)
		}
		placeholderValue := &commonpb.PlaceholderValue{
			Tag:    "$0",
			Type:   commonpb.PlaceholderType_SparseFloatVector,
			Values: [][]byte{bytes},
		}
		return placeholderValue, nil
	default:
		return nil, errors.New("field is not a vector field")
	}
}

func flattenedFloatVectorsToByteVectors(flattenedVectors []float32, dimension int) [][]byte {
	floatVectors := flattenedFloatVectorsToFloatVectors(flattenedVectors, dimension)
	result := make([][]byte, 0)
	for _, floatVector := range floatVectors {
		result = append(result, floatVectorToByteVector(floatVector))
	}

	return result
}

func flattenedFloatVectorsToFloatVectors(flattenedVectors []float32, dimension int) [][]float32 {
	result := make([][]float32, 0)
	for i := 0; i < len(flattenedVectors); i += dimension {
		result = append(result, flattenedVectors[i:i+dimension])
	}
	return result
}

func floatVectorToByteVector(vector []float32) []byte {
	data := make([]byte, 0, 4*len(vector)) // float32 occupies 4 bytes
	buf := make([]byte, 4)
	for _, f := range vector {
		binary.LittleEndian.PutUint32(buf, math.Float32bits(f))
		data = append(data, buf...)
	}
	return data
}

func flattenedByteVectorsToByteVectors(flattenedVectors []byte, dimension int) [][]byte {
	result := make([][]byte, 0)
	for i := 0; i < len(flattenedVectors); i += dimension {
		result = append(result, flattenedVectors[i:i+dimension])
	}
	return result
}

func flattenedFloat16VectorsToByteVectors(flattenedVectors []byte, dimension int) [][]byte {
	result := make([][]byte, 0)

	vectorBytes := 2 * dimension

	for i := 0; i < len(flattenedVectors); i += vectorBytes {
		result = append(result, flattenedVectors[i:i+vectorBytes])
	}

	return result
}

func flattenedBFloat16VectorsToByteVectors(flattenedVectors []byte, dimension int) [][]byte {
	result := make([][]byte, 0)

	vectorBytes := 2 * dimension

	for i := 0; i < len(flattenedVectors); i += vectorBytes {
		result = append(result, flattenedVectors[i:i+vectorBytes])
	}

	return result
}
