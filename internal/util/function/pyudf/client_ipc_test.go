// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pyudf

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/ipc"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/pyudfpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestExecuteRequestParameterConversion(t *testing.T) {
	params := &schemapb.FunctionParamObject{Fields: map[string]*schemapb.FunctionParamValue{
		"bool":   {Value: &schemapb.FunctionParamValue_BoolValue{BoolValue: false}},
		"min":    {Value: &schemapb.FunctionParamValue_Int64Value{Int64Value: math.MinInt64}},
		"max":    {Value: &schemapb.FunctionParamValue_Int64Value{Int64Value: math.MaxInt64}},
		"double": {Value: &schemapb.FunctionParamValue_DoubleValue{DoubleValue: 0.5}},
		"string": {Value: &schemapb.FunctionParamValue_StringValue{StringValue: "中文"}},
		"bytes":  {Value: &schemapb.FunctionParamValue_BytesValue{BytesValue: []byte{0, 255}}},
		"array": {Value: &schemapb.FunctionParamValue_ArrayValue{ArrayValue: &schemapb.FunctionParamArray{Values: []*schemapb.FunctionParamValue{
			{Value: &schemapb.FunctionParamValue_StringValue{StringValue: ""}},
			{Value: &schemapb.FunctionParamValue_ObjectValue{ObjectValue: &schemapb.FunctionParamObject{Fields: map[string]*schemapb.FunctionParamValue{
				"zero": {Value: &schemapb.FunctionParamValue_Int64Value{}},
			}}}},
			nil,
		}}}},
		"empty_object": {Value: &schemapb.FunctionParamValue_ObjectValue{}},
		"empty_array":  {Value: &schemapb.FunctionParamValue_ArrayValue{}},
		"unset":        {},
		"nil":          nil,
	}}
	for _, input := range []*schemapb.FunctionParamObject{nil, {}, params} {
		request := clientRequest(t)
		request.Params = input
		wire, err := encodeExecuteRequest(context.Background(), request)
		require.NoError(t, err)
		if input == nil {
			require.Nil(t, wire.Params)
			continue
		}
		// The independent messages retain the original field numbers/types, so
		// this also checks oneof presence, empty containers and nested values.
		options := proto.MarshalOptions{Deterministic: true}
		want, err := options.Marshal(input)
		require.NoError(t, err)
		got, err := options.Marshal(wire.Params)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
	require.Zero(t, pyudfpb.File_pyudf_proto.Imports().Len())
}

func TestClientIPCValidation(t *testing.T) {
	inputs := clientInput(t, 0, 2)
	payload, err := encodeInputs(context.Background(), inputs)
	require.NoError(t, err)
	decoded, err := decodeOutputs(context.Background(), payload)
	require.NoError(t, err)
	require.Len(t, decoded, 1)
	assert.Equal(t, 0, decoded[0].Chunk(0).Len())
	releaseColumns(decoded)
	for _, test := range []struct {
		name    string
		payload []byte
	}{
		{"empty", nil},
		{"truncated_schema", payload[:12]},
	} {
		t.Run(test.name, func(t *testing.T) {
			out, err := decodeOutputs(context.Background(), test.payload)
			require.Nil(t, out)
			require.ErrorIs(t, err, merr.ErrServiceInternal)
		})
	}
	t.Run("canceled_encoding", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := encodeInputs(ctx, inputs)
		require.ErrorIs(t, err, context.Canceled)
	})
	for _, input := range [][]*arrow.Chunked{nil, {nil}, {inputs[0], nil}} {
		_, err := encodeInputs(context.Background(), input)
		require.ErrorIs(t, err, merr.ErrServiceInternal)
	}
}

func TestClientIPCLateInputMismatch(t *testing.T) {
	first := clientInput(t, 2, 3)
	second := clientInput(t, 2, 4)
	payload, err := encodeInputs(context.Background(), []*arrow.Chunked{first[0], second[0]})
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	require.Nil(t, payload)
}

func TestExecuteRequestSharedInputReferences(t *testing.T) {
	first := clientInput(t, 0, 16384)[0]
	second := clientInput(t, 0, 16384)[0] // Equal values, but a distinct column.
	request := clientRequest(t)
	request.Inputs = []*arrow.Chunked{first, second, first, second, first}
	wire, err := encodeExecuteRequest(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, []uint32{0, 1, 0, 1, 0}, wire.InputColumnIndices)
	columns, err := decodeOutputs(context.Background(), wire.Inputs)
	require.NoError(t, err)
	defer releaseColumns(columns)
	require.Len(t, columns, 2)
	for _, column := range columns {
		require.Zero(t, column.Chunk(0).Len())
		require.True(t, array.Equal(first.Chunk(1), column.Chunk(1)))
	}

	request.Inputs = []*arrow.Chunked{first}
	single, err := encodeExecuteRequest(context.Background(), request)
	require.NoError(t, err)
	request.Inputs = make([]*arrow.Chunked, 1024)
	for i := range request.Inputs {
		request.Inputs[i] = first
	}
	repeated, err := encodeExecuteRequest(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, single.Inputs, repeated.Inputs) // Data is encoded exactly once.
	require.Equal(t, make([]uint32, 1024), repeated.InputColumnIndices)
	// A logical 128 MiB of arguments stays below the existing 1 MiB RPC limit.
	require.Less(t, proto.Size(repeated), 1<<20)
}

func TestClientGRPCMetadataLimit(t *testing.T) {
	cfg, _ := startClientTestServer(t, func(context.Context, *pyudfpb.ExecuteRequest) (*pyudfpb.ExecuteResponse, error) {
		t.Error("oversized request metadata reached worker")
		return nil, nil
	})
	cfg.MaxMessageBytes = 1 << 20
	req := clientRequest(t)
	req.ResourceName = string(bytes.Repeat([]byte("r"), cfg.MaxMessageBytes))
	out, err := newTestClient(t, cfg).Execute(context.Background(), req)
	require.Nil(t, out)
	require.ErrorIs(t, err, merr.ErrServiceResourceInsufficient)
}

func TestClientIPCOutputTypes(t *testing.T) {
	builder := array.NewStringBuilder(memory.DefaultAllocator)
	builder.Append("中文")
	builder.AppendNull()
	values := builder.NewArray()
	builder.Release()
	defer values.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "c0", Type: arrow.BinaryTypes.String, Nullable: true}}, nil)
	for _, compressed := range []bool{false, true} {
		var buffer bytes.Buffer
		options := []ipc.Option{ipc.WithSchema(schema)}
		if compressed {
			options = append(options, ipc.WithLZ4())
		}
		writer := ipc.NewWriter(&buffer, options...)
		record := array.NewRecord(schema, []arrow.Array{values}, 2)
		require.NoError(t, writer.Write(record))
		record.Release()
		require.NoError(t, writer.Close())
		out, err := decodeOutputs(context.Background(), buffer.Bytes())
		require.NoError(t, err)
		v := out[0].Chunk(0).(*array.String)
		assert.Equal(t, "中文", v.Value(0))
		assert.True(t, v.IsNull(1))
		releaseColumns(out)
	}
	t.Run("zero_columns", func(t *testing.T) {
		var buffer bytes.Buffer
		schema := arrow.NewSchema(nil, nil)
		writer := ipc.NewWriter(&buffer, ipc.WithSchema(schema))
		for _, rows := range []int64{0, 2} {
			record := array.NewRecord(schema, nil, rows)
			require.NoError(t, writer.Write(record))
			record.Release()
		}
		require.NoError(t, writer.Close())
		out, err := decodeOutputs(context.Background(), buffer.Bytes())
		require.NoError(t, err)
		require.Empty(t, out)
	})
}

func TestClientInvalidResponsesAndSize(t *testing.T) {
	for _, name := range []string{"missing_result", "oversize_error", "damaged_output", "response_size"} {
		t.Run(name, func(t *testing.T) {
			cfg, _ := startClientTestServer(t, func(ctx context.Context, req *pyudfpb.ExecuteRequest) (*pyudfpb.ExecuteResponse, error) {
				switch name {
				case "missing_result":
					return &pyudfpb.ExecuteResponse{}, nil
				case "oversize_error":
					return &pyudfpb.ExecuteResponse{Result: &pyudfpb.ExecuteResponse_Error{Error: &pyudfpb.ExecuteError{Message: string(bytes.Repeat([]byte("x"), MaxErrorMessageBytes+1))}}}, nil
				case "damaged_output":
					return &pyudfpb.ExecuteResponse{Result: &pyudfpb.ExecuteResponse_Outputs{Outputs: req.Inputs[:12]}}, nil
				default:
					return &pyudfpb.ExecuteResponse{Result: &pyudfpb.ExecuteResponse_Outputs{Outputs: make([]byte, 2<<20)}}, nil
				}
			})
			cfg.MaxMessageBytes = 1 << 20
			out, err := newTestClient(t, cfg).Execute(context.Background(), clientRequest(t))
			require.Nil(t, out)
			if name == "response_size" {
				require.ErrorIs(t, err, merr.ErrServiceResourceInsufficient)
			} else {
				require.ErrorIs(t, err, merr.ErrServiceInternal)
			}
		})
	}
	t.Run("request_size_before_rpc", func(t *testing.T) {
		cfg, _ := startClientTestServer(t, func(context.Context, *pyudfpb.ExecuteRequest) (*pyudfpb.ExecuteResponse, error) {
			t.Error("oversized request reached worker")
			return nil, nil
		})
		cfg.MaxMessageBytes = 1 << 20
		req := clientRequest(t)
		req.Inputs = clientInput(t, 200000)
		out, err := newTestClient(t, cfg).Execute(context.Background(), req)
		require.Nil(t, out)
		require.ErrorIs(t, err, merr.ErrServiceResourceInsufficient)
	})
	t.Run("deadline_before_decode", func(t *testing.T) {
		inputs := clientInput(t, 2)
		payload, err := encodeInputs(context.Background(), inputs)
		require.NoError(t, err)
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		defer cancel()
		out, err := decodeOutputs(ctx, payload)
		require.Nil(t, out)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})
}

func TestClientIndependentOutputBatches(t *testing.T) {
	for _, sizes := range [][]int{{3}, {0, 1, 3, 4}} {
		t.Run(fmt.Sprint(sizes), func(t *testing.T) {
			payload, err := encodeInputs(context.Background(), clientInput(t, sizes...))
			require.NoError(t, err)
			cfg, _ := startClientTestServer(t, func(context.Context, *pyudfpb.ExecuteRequest) (*pyudfpb.ExecuteResponse, error) {
				return &pyudfpb.ExecuteResponse{Result: &pyudfpb.ExecuteResponse_Outputs{Outputs: payload}}, nil
			})
			outputs, err := newTestClient(t, cfg).Execute(context.Background(), clientRequest(t))
			require.NoError(t, err)
			defer releaseColumns(outputs)
			require.Len(t, outputs, 1)
			require.Len(t, outputs[0].Chunks(), len(sizes))
			for i, size := range sizes {
				require.Equal(t, size, outputs[0].Chunk(i).Len())
			}
		})
	}
}

func TestClientIPCArrowStreamRules(t *testing.T) {
	payload, err := encodeInputs(context.Background(), clientInput(t, 0, 2))
	require.NoError(t, err)
	for name, data := range map[string][]byte{
		"missing_eos":  payload[:len(payload)-8],
		"trailing":     append(append([]byte(nil), payload...), []byte("tail")...),
		"concatenated": append(append([]byte(nil), payload...), payload...),
	} {
		t.Run(name, func(t *testing.T) {
			outputs, err := decodeOutputs(context.Background(), data)
			require.NoError(t, err)
			defer releaseColumns(outputs)
			require.Len(t, outputs, 1)
			require.Len(t, outputs[0].Chunks(), 2)
			require.Equal(t, 2, outputs[0].Chunk(1).Len())
		})
	}
}

func TestClientIPCArrowSchema(t *testing.T) {
	builder := array.NewUint32Builder(memory.DefaultAllocator)
	builder.AppendValues([]uint32{1, 2}, nil)
	values := builder.NewArray()
	builder.Release()
	defer values.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "worker_column", Type: arrow.PrimitiveTypes.Uint32}}, nil)
	var buffer bytes.Buffer
	writer := ipc.NewWriter(&buffer, ipc.WithSchema(schema), ipc.WithZstd())
	record := array.NewRecord(schema, []arrow.Array{values}, 2)
	require.NoError(t, writer.Write(record))
	record.Release()
	require.NoError(t, writer.Close())
	outputs, err := decodeOutputs(context.Background(), buffer.Bytes())
	require.NoError(t, err)
	defer releaseColumns(outputs)
	require.Len(t, outputs, 1)
	assert.True(t, array.Equal(values, outputs[0].Chunk(0)))
}
