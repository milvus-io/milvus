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

// Package onnx contains the execution contract shared by CPU embedding and
// reranking functions. A Backend supplies the concrete ONNX Runtime binding;
// resource and validation policy stay independent of its C ABI.
package onnx

import (
	"context"
	"encoding/binary"
	"math"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// DataType is the ONNX tensor element type understood by the runner.
type DataType uint8

const (
	DataTypeInvalid DataType = iota
	DataTypeFloat32
	DataTypeFloat64
	DataTypeInt8
	DataTypeInt16
	DataTypeInt32
	DataTypeInt64
	DataTypeUint8
	DataTypeUint16
	DataTypeUint32
	DataTypeUint64
	DataTypeBool
)

func (d DataType) String() string {
	switch d {
	case DataTypeFloat32:
		return "float32"
	case DataTypeFloat64:
		return "float64"
	case DataTypeInt8:
		return "int8"
	case DataTypeInt16:
		return "int16"
	case DataTypeInt32:
		return "int32"
	case DataTypeInt64:
		return "int64"
	case DataTypeUint8:
		return "uint8"
	case DataTypeUint16:
		return "uint16"
	case DataTypeUint32:
		return "uint32"
	case DataTypeUint64:
		return "uint64"
	case DataTypeBool:
		return "bool"
	default:
		return "invalid"
	}
}

func (d DataType) elementSize() int {
	switch d {
	case DataTypeFloat32, DataTypeInt32, DataTypeUint32:
		return 4
	case DataTypeFloat64, DataTypeInt64, DataTypeUint64:
		return 8
	case DataTypeInt16, DataTypeUint16:
		return 2
	case DataTypeInt8, DataTypeUint8, DataTypeBool:
		return 1
	default:
		return 0
	}
}

// TensorSpec is the immutable signature of one model input or output. A
// negative dimension is dynamic and accepts any non-negative input dimension.
type TensorSpec struct {
	Name     string
	DataType DataType
	Shape    []int64
}

// Tensor is a dense tensor encoded in native little-endian element order.
// Keeping the payload opaque lets a runtime binding avoid a second copy.
type Tensor struct {
	Name     string
	DataType DataType
	Shape    []int64
	Data     []byte
}

// NewFloat32Tensor creates a tensor from float32 values in row-major order.
func NewFloat32Tensor(name string, shape []int64, values []float32) (Tensor, error) {
	if err := validateShape(shape); err != nil {
		return Tensor{}, err
	}
	count, err := shapeSize(shape)
	if err != nil {
		return Tensor{}, err
	}
	if count != int64(len(values)) {
		return Tensor{}, merr.WrapErrParameterInvalidMsg("onnx: tensor %q has %d values, expected %d", name, len(values), count)
	}
	data := make([]byte, len(values)*4)
	for i, value := range values {
		binary.LittleEndian.PutUint32(data[i*4:], math.Float32bits(value))
	}
	return Tensor{Name: name, DataType: DataTypeFloat32, Shape: append([]int64(nil), shape...), Data: data}, nil
}

func validateShape(shape []int64) error {
	for i, dim := range shape {
		if dim < 0 {
			return merr.WrapErrParameterInvalidMsg("onnx: tensor shape dimension %d is negative: %d", i, dim)
		}
	}
	return nil
}

func shapeSize(shape []int64) (int64, error) {
	if err := validateShape(shape); err != nil {
		return 0, err
	}
	size := int64(1)
	for _, dim := range shape {
		if dim != 0 && size > math.MaxInt64/dim {
			return 0, merr.WrapErrParameterInvalidMsg("onnx: tensor shape is too large")
		}
		size *= dim
	}
	return size, nil
}

// Session is one loaded CPU model. Implementations must make Run safe for
// concurrent callers; the manager bounds the number of calls in flight.
type Session interface {
	// Inputs and Outputs return immutable signatures for the session lifetime.
	Inputs() []TensorSpec
	Outputs() []TensorSpec
	Run(ctx context.Context, inputs []Tensor) ([]Tensor, error)
	// MemoryBytes reports the session's resident native allocation at load time.
	// The value must remain stable until Close returns.
	MemoryBytes() uint64
	Close() error
}

// Backend is the narrow adapter implemented by an ONNX Runtime binding.
// Version must identify the runtime build and execution-provider settings.
type Backend interface {
	Name() string
	Version() string
	Load(ctx context.Context, modelPath string) (Session, error)
}
