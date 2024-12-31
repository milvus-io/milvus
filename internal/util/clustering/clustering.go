package clustering

import (
	"encoding/binary"
	"math"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/util/distance"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

func CalcVectorDistance(dim int64, dataType schemapb.DataType, left []byte, right []float32, metric string) ([]float32, error) {
	switch dataType {
	case schemapb.DataType_FloatVector:
		distance, err := distance.CalcFloatDistance(dim, DeserializeFloatVector(left), right, metric)
		if err != nil {
			return nil, err
		}
		return distance, nil
	// todo support other vector type
	case schemapb.DataType_BinaryVector:
	case schemapb.DataType_Float16Vector:
	case schemapb.DataType_BFloat16Vector:
	case schemapb.DataType_Int8Vector:
	default:
		return nil, merr.ErrParameterInvalid
	}
	return nil, nil
}

func DeserializeFloatVector(data []byte) []float32 {
	vectorLen := len(data) / 4 // Each float32 occupies 4 bytes
	fv := make([]float32, vectorLen)

	for i := 0; i < vectorLen; i++ {
		bits := binary.LittleEndian.Uint32(data[i*4 : (i+1)*4])
		fv[i] = math.Float32frombits(bits)
	}

	return fv
}

func SerializeFloatVector(fv []float32) []byte {
	data := make([]byte, 0, 4*len(fv)) // float32 occupies 4 bytes
	buf := make([]byte, 4)
	for _, f := range fv {
		binary.LittleEndian.PutUint32(buf, math.Float32bits(f))
		data = append(data, buf...)
	}
	return data
}

func GetClusteringKeyField(collectionSchema *schemapb.CollectionSchema) *schemapb.FieldSchema {
	var clusteringKeyField *schemapb.FieldSchema
	var partitionKeyField *schemapb.FieldSchema
	vectorFields := make([]*schemapb.FieldSchema, 0)
	for _, field := range collectionSchema.GetFields() {
		if field.IsClusteringKey {
			clusteringKeyField = field
		}
		if field.IsPartitionKey {
			partitionKeyField = field
		}
		// todo support other vector type
		// if typeutil.IsVectorType(field.GetDataType()) {
		if field.DataType == schemapb.DataType_FloatVector {
			vectorFields = append(vectorFields, field)
		}
	}
	// in some server mode, we regard partition key field or vector field as clustering key by default.
	// here is the priority: clusteringKey > partitionKey > vector field(only single vector)
	if clusteringKeyField != nil {
		if typeutil.IsVectorType(clusteringKeyField.GetDataType()) &&
			!paramtable.Get().CommonCfg.EnableVectorClusteringKey.GetAsBool() {
			return nil
		}
		return clusteringKeyField
	} else if paramtable.Get().CommonCfg.UsePartitionKeyAsClusteringKey.GetAsBool() && partitionKeyField != nil {
		return partitionKeyField
	} else if paramtable.Get().CommonCfg.EnableVectorClusteringKey.GetAsBool() &&
		paramtable.Get().CommonCfg.UseVectorAsClusteringKey.GetAsBool() &&
		len(vectorFields) == 1 {
		return vectorFields[0]
	}
	return nil
}
