package indexcgowrapper

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
)

const (
	keyRawArr   = "key_raw_arr"
	keyValidArr = "key_valid_arr"
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

func GenFloat16VecDataset(vectors []byte) *Dataset {
	return &Dataset{
		DType: schemapb.DataType_Float16Vector,
		Data: map[string]interface{}{
			keyRawArr: vectors,
		},
	}
}

func GenBFloat16VecDataset(vectors []byte) *Dataset {
	return &Dataset{
		DType: schemapb.DataType_BFloat16Vector,
		Data: map[string]interface{}{
			keyRawArr: vectors,
		},
	}
}

func GenSparseFloatVecDataset(data *storage.SparseFloatVectorFieldData) *Dataset {
	// TODO(SPARSE): This is used only for testing. In order to make any golang
	// tests that uses this method work, we'll need to expose
	// knowhere::sparse::SparseRow to Go, which is the accepted format in cgo
	// wrapper. Such tests are skipping sparse vector for now.
	return &Dataset{
		DType: schemapb.DataType_SparseFloatVector,
		Data:  make(map[string]interface{}),
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

func GenInt8VecDataset(vectors []int8) *Dataset {
	return &Dataset{
		DType: schemapb.DataType_Int8Vector,
		Data: map[string]interface{}{
			keyRawArr: vectors,
		},
	}
}

func GenDataset(data storage.FieldData) *Dataset {
	switch f := data.(type) {
	case *storage.BoolFieldData:
		ds := &Dataset{
			DType: schemapb.DataType_Bool,
			Data: map[string]interface{}{
				keyRawArr: f.Data,
			},
		}
		if f.Nullable && len(f.ValidData) > 0 {
			ds.Data[keyValidArr] = f.ValidData
		}
		return ds
	case *storage.Int8FieldData:
		ds := &Dataset{
			DType: schemapb.DataType_Int8,
			Data: map[string]interface{}{
				keyRawArr: f.Data,
			},
		}
		if f.Nullable && len(f.ValidData) > 0 {
			ds.Data[keyValidArr] = f.ValidData
		}
		return ds
	case *storage.Int16FieldData:
		ds := &Dataset{
			DType: schemapb.DataType_Int16,
			Data: map[string]interface{}{
				keyRawArr: f.Data,
			},
		}
		if f.Nullable && len(f.ValidData) > 0 {
			ds.Data[keyValidArr] = f.ValidData
		}
		return ds
	case *storage.Int32FieldData:
		ds := &Dataset{
			DType: schemapb.DataType_Int32,
			Data: map[string]interface{}{
				keyRawArr: f.Data,
			},
		}
		if f.Nullable && len(f.ValidData) > 0 {
			ds.Data[keyValidArr] = f.ValidData
		}
		return ds
	case *storage.Int64FieldData:
		ds := &Dataset{
			DType: schemapb.DataType_Int64,
			Data: map[string]interface{}{
				keyRawArr: f.Data,
			},
		}
		if f.Nullable && len(f.ValidData) > 0 {
			ds.Data[keyValidArr] = f.ValidData
		}
		return ds
	case *storage.FloatFieldData:
		ds := &Dataset{
			DType: schemapb.DataType_Float,
			Data: map[string]interface{}{
				keyRawArr: f.Data,
			},
		}
		if f.Nullable && len(f.ValidData) > 0 {
			ds.Data[keyValidArr] = f.ValidData
		}
		return ds
	case *storage.DoubleFieldData:
		ds := &Dataset{
			DType: schemapb.DataType_Double,
			Data: map[string]interface{}{
				keyRawArr: f.Data,
			},
		}
		if f.Nullable && len(f.ValidData) > 0 {
			ds.Data[keyValidArr] = f.ValidData
		}
		return ds
	case *storage.StringFieldData:
		ds := &Dataset{
			DType: schemapb.DataType_VarChar,
			Data: map[string]interface{}{
				keyRawArr: f.Data,
			},
		}
		if f.Nullable && len(f.ValidData) > 0 {
			ds.Data[keyValidArr] = f.ValidData
		}
		return ds
	case *storage.BinaryVectorFieldData:
		ds := &Dataset{
			DType: schemapb.DataType_BinaryVector,
			Data: map[string]interface{}{
				keyRawArr: f.Data,
			},
		}
		if f.Nullable && len(f.ValidData) > 0 {
			ds.Data[keyValidArr] = f.ValidData
		}
		return ds
	case *storage.FloatVectorFieldData:
		ds := &Dataset{
			DType: schemapb.DataType_FloatVector,
			Data: map[string]interface{}{
				keyRawArr: f.Data,
			},
		}
		if f.Nullable && len(f.ValidData) > 0 {
			ds.Data[keyValidArr] = f.ValidData
		}
		return ds
	case *storage.Float16VectorFieldData:
		ds := &Dataset{
			DType: schemapb.DataType_Float16Vector,
			Data: map[string]interface{}{
				keyRawArr: f.Data,
			},
		}
		if f.Nullable && len(f.ValidData) > 0 {
			ds.Data[keyValidArr] = f.ValidData
		}
		return ds
	case *storage.BFloat16VectorFieldData:
		ds := &Dataset{
			DType: schemapb.DataType_BFloat16Vector,
			Data: map[string]interface{}{
				keyRawArr: f.Data,
			},
		}
		if f.Nullable && len(f.ValidData) > 0 {
			ds.Data[keyValidArr] = f.ValidData
		}
		return ds
	case *storage.SparseFloatVectorFieldData:
		ds := GenSparseFloatVecDataset(f)
		if f.Nullable && len(f.ValidData) > 0 {
			ds.Data[keyValidArr] = f.ValidData
		}
		return ds
	case *storage.Int8VectorFieldData:
		ds := &Dataset{
			DType: schemapb.DataType_Int8Vector,
			Data: map[string]interface{}{
				keyRawArr: f.Data,
			},
		}
		if f.Nullable && len(f.ValidData) > 0 {
			ds.Data[keyValidArr] = f.ValidData
		}
		return ds
	default:
		return &Dataset{
			DType: schemapb.DataType_None,
			Data:  nil,
		}
	}
}
