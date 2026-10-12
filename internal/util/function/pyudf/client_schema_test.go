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

package pyudf

import (
	"context"
	"strconv"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/proto/pyudfpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func schemaClientRequest(t *testing.T) ExecuteRequest {
	t.Helper()
	field, err := types.ArrowFieldFromMilvusSchema(&schemapb.FieldSchema{Name: "embedding", DataType: schemapb.DataType_BFloat16Vector, Nullable: true, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "2"}}})
	require.NoError(t, err)
	b := array.NewFixedSizeBinaryBuilder(memory.DefaultAllocator, field.Type.(*arrow.FixedSizeBinaryType))
	b.Append([]byte{1, 2, 3, 4})
	values := b.NewArray()
	b.Release()
	t.Cleanup(values.Release)
	column := arrow.NewChunked(field.Type, []arrow.Array{values})
	t.Cleanup(column.Release)
	metadata := arrow.MetadataFrom(map[string]string{"scope": "complete-schema"})
	return ExecuteRequest{ResourceName: "model", UDFPath: "/tmp/model.whl", Stage: types.StagePostProcess, TableResult: true, Inputs: []*arrow.Chunked{column, column}, InputSchema: arrow.NewSchema([]arrow.Field{field, field}, &metadata)}
}

func TestSchemaAwareClientPreservesFieldsAndLogicalIdentity(t *testing.T) {
	request := schemaClientRequest(t)
	cfg, _ := startClientTestServer(t, func(ctx context.Context, wire *pyudfpb.ExecuteRequest) (*pyudfpb.ExecuteResponse, error) {
		require.Equal(t, schemaExecutionVersion, wire.GetExecutionVersion())
		require.Equal(t, []uint32{0, 0}, wire.GetInputColumnIndices())
		schema, columns, err := decodeOutputsWithSchema(ctx, wire.GetInputs())
		require.NoError(t, err)
		defer releaseColumns(columns)
		require.Equal(t, 1, schema.NumFields())
		require.True(t, request.InputSchema.Field(0).Equal(schema.Field(0)))
		require.True(t, request.InputSchema.Metadata().Equal(schema.Metadata()))
		return &pyudfpb.ExecuteResponse{ExecutionVersion: schemaExecutionVersion, Result: &pyudfpb.ExecuteResponse_Outputs{Outputs: wire.GetInputs()}}, nil
	})
	client := newTestClient(t, cfg)
	schema, columns, err := client.ExecuteWithSchema(context.Background(), request)
	require.NoError(t, err)
	defer releaseColumns(columns)
	require.True(t, request.InputSchema.Field(0).Equal(schema.Field(0)))
	require.Equal(t, []byte{1, 2, 3, 4}, columns[0].Chunk(0).(*array.FixedSizeBinary).Value(0))
	logical, err := types.MilvusSchemaFromArrowField(schema.Field(0))
	require.NoError(t, err)
	require.Equal(t, schemapb.DataType_BFloat16Vector, logical.GetDataType())
}

func TestSchemaAwareClientPreservesAliasedSharedColumns(t *testing.T) {
	for _, secondType := range []schemapb.DataType{schemapb.DataType_BFloat16Vector, schemapb.DataType_Float16Vector} {
		t.Run(secondType.String(), func(t *testing.T) {
			request := schemaClientRequest(t)
			first := request.InputSchema.Field(0)
			second, err := types.ArrowFieldFromMilvusSchema(&schemapb.FieldSchema{Name: "alias", DataType: secondType, Nullable: true, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "2"}}})
			require.NoError(t, err)
			column := request.Inputs[0]
			request.Inputs = []*arrow.Chunked{column, column, column}
			metadata := request.InputSchema.Metadata()
			request.InputSchema = arrow.NewSchema([]arrow.Field{first, second, first}, &metadata)
			cfg, _ := startClientTestServer(t, func(ctx context.Context, wire *pyudfpb.ExecuteRequest) (*pyudfpb.ExecuteResponse, error) {
				require.Equal(t, schemaExecutionVersion, wire.GetExecutionVersion())
				require.Equal(t, []uint32{0, 1, 0}, wire.GetInputColumnIndices())
				schema, columns, err := decodeOutputsWithSchema(ctx, wire.GetInputs())
				require.NoError(t, err)
				defer releaseColumns(columns)
				require.Equal(t, 2, schema.NumFields())
				require.True(t, first.Equal(schema.Field(0)))
				require.True(t, second.Equal(schema.Field(1)))
				require.True(t, metadata.Equal(schema.Metadata()))
				return &pyudfpb.ExecuteResponse{ExecutionVersion: schemaExecutionVersion, Result: &pyudfpb.ExecuteResponse_Outputs{Outputs: wire.GetInputs()}}, nil
			})
			client := newTestClient(t, cfg)
			schema, columns, err := client.ExecuteWithSchema(context.Background(), request)
			require.NoError(t, err)
			defer releaseColumns(columns)
			require.Equal(t, 2, schema.NumFields())
			require.True(t, first.Equal(schema.Field(0)))
			require.True(t, second.Equal(schema.Field(1)))
			for _, output := range columns {
				require.Equal(t, []byte{1, 2, 3, 4}, output.Chunk(0).(*array.FixedSizeBinary).Value(0))
			}
		})
	}
}

func TestSchemaAwareClientRejectsOldVersionSuccess(t *testing.T) {
	for _, ack := range []uint32{0, executionVersion, tableExecutionVersion} {
		t.Run(strconv.Itoa(int(ack)), func(t *testing.T) {
			cfg, _ := startClientTestServer(t, func(ctx context.Context, wire *pyudfpb.ExecuteRequest) (*pyudfpb.ExecuteResponse, error) {
				return &pyudfpb.ExecuteResponse{ExecutionVersion: ack, Result: &pyudfpb.ExecuteResponse_Outputs{Outputs: wire.GetInputs()}}, nil
			})
			client := newTestClient(t, cfg)
			schema, columns, err := client.ExecuteWithSchema(context.Background(), schemaClientRequest(t))
			require.ErrorIs(t, err, merr.ErrServiceUnimplemented)
			require.Nil(t, schema)
			require.Nil(t, columns)
		})
	}
}

func TestSchemaAwareClientRejectsContradictoryEmptySchema(t *testing.T) {
	field := arrow.Field{Name: "v", Type: &arrow.FixedSizeBinaryType{ByteWidth: 4}, Nullable: true, Metadata: arrow.MetadataFrom(map[string]string{types.MetadataKeyMilvusDataType: "102", "dim": "3"})}
	values := array.MakeArrayOfNull(memory.DefaultAllocator, field.Type, 0)
	defer values.Release()
	column := arrow.NewChunked(field.Type, []arrow.Array{values})
	defer column.Release()
	_, err := encodeInputsWithSchema(context.Background(), []*arrow.Chunked{column}, arrow.NewSchema([]arrow.Field{field}, nil))
	require.ErrorIs(t, err, merr.ErrFunctionFailed)
}
