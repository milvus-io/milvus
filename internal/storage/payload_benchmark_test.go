package storage

import (
	"math/rand"
	"testing"

	"github.com/milvus-io/milvus/internal/proto/schemapb"
)

// workload setting for benchmark
const (
	numElements = 1000
	vectorDim   = 8
)

func BenchmarkPayloadReader_Bool(b *testing.B) {
	w, _ := NewPayloadWriter(schemapb.DataType_Bool)
	defer w.ReleasePayloadWriter()
	data := make([]bool, 0, numElements)
	for i := 0; i < numElements; i++ {
		data = append(data, rand.Intn(2) != 0)
	}
	w.AddBoolToPayload(data)
	w.FinishPayloadWriter()
	buffer, _ := w.GetPayloadBufferFromWriter()

	b.Run("cgo reader", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			r, _ := NewPayloadReaderCgo(schemapb.DataType_Bool, buffer)
			r.GetBoolFromPayload()
			r.ReleasePayloadReader()
		}
	})

	b.Run("go reader", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			r, _ := NewPayloadReader(schemapb.DataType_Bool, buffer)
			r.GetBoolFromPayload()
			r.ReleasePayloadReader()
		}
	})
}

func BenchmarkPayloadReader_Int32(b *testing.B) {
	w, _ := NewPayloadWriter(schemapb.DataType_Int32)
	defer w.ReleasePayloadWriter()
	data := make([]int32, 0, numElements)
	for i := 0; i < numElements; i++ {
		data = append(data, rand.Int31n(1000))
	}
	w.AddInt32ToPayload(data)
	w.FinishPayloadWriter()
	buffer, _ := w.GetPayloadBufferFromWriter()

	b.Run("cgo reader", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			r, _ := NewPayloadReaderCgo(schemapb.DataType_Int32, buffer)
			r.GetInt32FromPayload()
			r.ReleasePayloadReader()
		}
	})

	b.Run("go reader", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			r, _ := NewPayloadReader(schemapb.DataType_Int32, buffer)
			r.GetInt32FromPayload()
			r.ReleasePayloadReader()
		}
	})
}

func BenchmarkPayloadReader_Int64(b *testing.B) {
	w, _ := NewPayloadWriter(schemapb.DataType_Int64)
	defer w.ReleasePayloadWriter()
	data := make([]int64, 0, numElements)
	for i := 0; i < numElements; i++ {
		data = append(data, rand.Int63n(1000))
	}
	w.AddInt64ToPayload(data)
	w.FinishPayloadWriter()
	buffer, _ := w.GetPayloadBufferFromWriter()

	b.Run("cgo reader", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			r, _ := NewPayloadReaderCgo(schemapb.DataType_Int64, buffer)
			r.GetInt64FromPayload()
			r.ReleasePayloadReader()
		}
	})

	b.Run("go reader", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			r, _ := NewPayloadReader(schemapb.DataType_Int64, buffer)
			r.GetInt64FromPayload()
			r.ReleasePayloadReader()
		}
	})
}

func BenchmarkPayloadReader_Float32(b *testing.B) {
	w, _ := NewPayloadWriter(schemapb.DataType_Float)
	defer w.ReleasePayloadWriter()
	data := make([]float32, 0, numElements)
	for i := 0; i < numElements; i++ {
		data = append(data, rand.Float32())
	}
	w.AddFloatToPayload(data)
	w.FinishPayloadWriter()
	buffer, _ := w.GetPayloadBufferFromWriter()

	b.Run("cgo reader", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			r, _ := NewPayloadReaderCgo(schemapb.DataType_Float, buffer)
			r.GetFloatFromPayload()
			r.ReleasePayloadReader()
		}
	})

	b.Run("go reader", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			r, _ := NewPayloadReader(schemapb.DataType_Float, buffer)
			r.GetFloatFromPayload()
			r.ReleasePayloadReader()
		}
	})
}

func BenchmarkPayloadReader_Float64(b *testing.B) {
	w, _ := NewPayloadWriter(schemapb.DataType_Double)
	defer w.ReleasePayloadWriter()
	data := make([]float64, 0, numElements)
	for i := 0; i < numElements; i++ {
		data = append(data, rand.Float64())
	}
	w.AddDoubleToPayload(data)
	w.FinishPayloadWriter()
	buffer, _ := w.GetPayloadBufferFromWriter()

	b.Run("cgo reader", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			r, _ := NewPayloadReaderCgo(schemapb.DataType_Double, buffer)
			r.GetDoubleFromPayload()
			r.ReleasePayloadReader()
		}
	})

	b.Run("go reader", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			r, _ := NewPayloadReader(schemapb.DataType_Double, buffer)
			r.GetDoubleFromPayload()
			r.ReleasePayloadReader()
		}
	})
}

func BenchmarkPayloadReader_FloatVector(b *testing.B) {
	w, _ := NewPayloadWriter(schemapb.DataType_FloatVector)
	defer w.ReleasePayloadWriter()
	data := make([]float32, 0, numElements*vectorDim)
	for i := 0; i < numElements; i++ {
		data = append(data, rand.Float32())
	}
	w.AddFloatVectorToPayload(data, vectorDim)
	w.FinishPayloadWriter()
	buffer, _ := w.GetPayloadBufferFromWriter()

	b.Run("cgo reader", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			r, _ := NewPayloadReaderCgo(schemapb.DataType_FloatVector, buffer)
			r.GetFloatVectorFromPayload()
			r.ReleasePayloadReader()
		}
	})

	b.Run("go reader", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			r, _ := NewPayloadReader(schemapb.DataType_FloatVector, buffer)
			r.GetFloatVectorFromPayload()
			r.ReleasePayloadReader()
		}
	})
}

func BenchmarkPayloadReader_BinaryVector(b *testing.B) {
	w, _ := NewPayloadWriter(schemapb.DataType_BinaryVector)
	defer w.ReleasePayloadWriter()
	data := make([]byte, numElements*vectorDim/8)
	rand.Read(data)

	err := w.AddBinaryVectorToPayload(data, vectorDim)
	if err != nil {
		panic(err)
	}
	w.FinishPayloadWriter()
	buffer, _ := w.GetPayloadBufferFromWriter()

	b.Run("cgo reader", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			r, _ := NewPayloadReaderCgo(schemapb.DataType_BinaryVector, buffer)
			r.GetBinaryVectorFromPayload()
			r.ReleasePayloadReader()
		}
	})

	b.Run("go reader", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			r, _ := NewPayloadReader(schemapb.DataType_BinaryVector, buffer)
			r.GetBinaryVectorFromPayload()
			r.ReleasePayloadReader()
		}
	})
}
