package clustering

import (
	"encoding/binary"
	"math"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/distance"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func CalcVectorDistance(dim int64, dataType schemapb.DataType, left []byte, right interface{}, metric string) ([]float32, error) {
	var leftVector []float32
	var rightVector []float32
	switch dataType {
	case schemapb.DataType_FloatVector:
		value, ok := right.([]float32)
		if !ok || int64(len(left)) != dim*4 || int64(len(value)) != dim {
			return nil, merr.WrapErrParameterInvalidMsg("invalid FloatVector operands for distance calculation")
		}
		leftVector = DeserializeFloatVector(left)
		rightVector = value
	case schemapb.DataType_Float16Vector:
		value, ok := right.([]byte)
		if !ok || int64(len(left)) != dim*2 || int64(len(value)) != dim*2 {
			return nil, merr.WrapErrParameterInvalidMsg("invalid Float16Vector operands for distance calculation")
		}
		leftVector = typeutil.Float16BytesToFloat32Vector(left)
		rightVector = typeutil.Float16BytesToFloat32Vector(value)
	case schemapb.DataType_BFloat16Vector:
		value, ok := right.([]byte)
		if !ok || int64(len(left)) != dim*2 || int64(len(value)) != dim*2 {
			return nil, merr.WrapErrParameterInvalidMsg("invalid BFloat16Vector operands for distance calculation")
		}
		leftVector = typeutil.BFloat16BytesToFloat32Vector(left)
		rightVector = typeutil.BFloat16BytesToFloat32Vector(value)
	default:
		return nil, merr.WrapErrParameterInvalidMsg("unsupported vector type %s for distance calculation", dataType.String())
	}
	return distance.CalcFloatDistance(dim, leftVector, rightVector, metric)
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
		if typeutil.IsDenseFloatVectorType(field.GetDataType()) {
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
