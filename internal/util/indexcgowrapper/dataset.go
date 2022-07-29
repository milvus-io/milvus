package indexcgowrapper

import (
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
)

const (
	keyRawArr = "key_raw_arr"
)

type Dataset struct {
	DType schemapb.DataType
	Data  map[string]interface{}
}

func GenFloatVecDataset(vectors []float32) *Dataset {
	return &Dataset{
		DType: schemapb.DataType_FloatVector,
		Data: map[string]interface{}{
			keyRawArr: vectors,
		},
	}
}

func GenBinaryVecDataset(vectors []byte) *Dataset {
	return &Dataset{
		DType: schemapb.DataType_BinaryVector,
		Data: map[string]interface{}{
			keyRawArr: vectors,
		},
	}
}

func GenDataset(data storage.FieldData) *Dataset {
	switch f := data.(type) {
	case *storage.BoolFieldData:
		return &Dataset{
			DType: schemapb.DataType_Bool,
			Data: map[string]interface{}{
				keyRawArr: f.Data,
			},
		}
	case *storage.Int8FieldData:
		return &Dataset{
			DType: schemapb.DataType_Int8,
			Data: map[string]interface{}{
				keyRawArr: f.Data,
			},
		}
	case *storage.Int16FieldData:
		return &Dataset{
			DType: schemapb.DataType_Int16,
			Data: map[string]interface{}{
				keyRawArr: f.Data,
			},
		}
	case *storage.Int32FieldData:
		return &Dataset{
			DType: schemapb.DataType_Int32,
			Data: map[string]interface{}{
				keyRawArr: f.Data,
			},
		}
	case *storage.Int64FieldData:
		return &Dataset{
			DType: schemapb.DataType_Int64,
			Data: map[string]interface{}{
				keyRawArr: f.Data,
			},
		}
	case *storage.UInt8FieldData:
		return &Dataset{
			DType: schemapb.DataType_UInt8,
			Data: map[string]interface{}{
				keyRawArr: f.Data,
			},
		}
	case *storage.UInt16FieldData:
		return &Dataset{
			DType: schemapb.DataType_UInt16,
			Data: map[string]interface{}{
				keyRawArr: f.Data,
			},
		}
	case *storage.UInt32FieldData:
		return &Dataset{
			DType: schemapb.DataType_UInt32,
			Data: map[string]interface{}{
				keyRawArr: f.Data,
			},
		}
	case *storage.UInt64FieldData:
		return &Dataset{
			DType: schemapb.DataType_UInt64,
			Data: map[string]interface{}{
				keyRawArr: f.Data,
			},
		}

	case *storage.FloatFieldData:
		return &Dataset{
			DType: schemapb.DataType_Float,
			Data: map[string]interface{}{
				keyRawArr: f.Data,
			},
		}
	case *storage.DoubleFieldData:
		return &Dataset{
			DType: schemapb.DataType_Double,
			Data: map[string]interface{}{
				keyRawArr: f.Data,
			},
		}
	case *storage.StringFieldData:
		return &Dataset{
			DType: schemapb.DataType_String,
			Data: map[string]interface{}{
				keyRawArr: f.Data,
			},
		}
	case *storage.BinaryVectorFieldData:
		return GenBinaryVecDataset(f.Data)
	case *storage.FloatVectorFieldData:
		return GenFloatVecDataset(f.Data)
	default:
		return &Dataset{
			DType: schemapb.DataType_None,
			Data:  nil,
		}
	}
}
