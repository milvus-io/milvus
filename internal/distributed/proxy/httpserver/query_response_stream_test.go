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

package httpserver

import (
	"bytes"
	"context"
	gojson "encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func streamTestRows(t *testing.T, values ...float64) *queryResponseRows {
	t.Helper()
	paramtable.Init()
	field := &schemapb.FieldData{FieldName: "value", Type: schemapb.DataType_Double, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: values}}}}}
	rows, err := newQueryResponseRows(0, nil, []*schemapb.FieldData{field}, nil, nil, true, nil)
	require.NoError(t, err)
	return rows
}

func numberPreservingJSON(t *testing.T, body []byte) any {
	t.Helper()
	decoder := gojson.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	var value any
	require.NoError(t, decoder.Decode(&value))
	var extra any
	require.ErrorIs(t, decoder.Decode(&extra), io.EOF)
	return value
}

func TestWriteQueryResponseEquality(t *testing.T) {
	for _, count := range []int{0, 1, 5} {
		for _, size := range []int{1, 2, 10} {
			t.Run(fmt.Sprintf("rows=%d/batch=%d", count, size), func(t *testing.T) {
				values := []float64{1, 2, 3, 4, 5}[:count]
				rows := streamTestRows(t, values...)
				oldRows, err := buildQueryResp(0, nil, rows.fields, nil, nil, true, nil)
				require.NoError(t, err)
				old := httptest.NewRecorder()
				require.NoError(t, (jsonRender{Data: gin.H{"code": 0, "cost": int64(9007199254740993), "data": oldRows}}).Render(old))
				var got bytes.Buffer
				require.NoError(t, writeQueryResponse(context.Background(), &got, gin.H{"code": 0, "cost": int64(9007199254740993)}, rows, size))
				require.Equal(t, numberPreservingJSON(t, old.Body.Bytes()), numberPreservingJSON(t, got.Bytes()))
			})
		}
	}
}

func TestWriteQueryResponseEncoderErrors(t *testing.T) {
	for _, bad := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		for _, first := range []bool{true, false} {
			t.Run(fmt.Sprintf("bad=%v/first=%v", bad, first), func(t *testing.T) {
				values := []float64{1, bad, 3}
				if first {
					values[0] = bad
				}
				rows := streamTestRows(t, values...)
				var got bytes.Buffer
				require.Error(t, writeQueryResponse(context.Background(), &got, gin.H{"code": 0}, rows, 1))
				if first {
					require.Empty(t, got.String())
				} else {
					require.NotEmpty(t, got.String())
					require.False(t, gojson.Valid(got.Bytes()))
					require.NotContains(t, got.String(), "]}")
				}
				require.Less(t, rows.cursor, int64(3))
			})
		}
	}
}

type streamTestWriter struct {
	write func([]byte) (int, error)
}

func (w streamTestWriter) Write(p []byte) (int, error) { return w.write(p) }

func TestWriteQueryResponseWriteFailure(t *testing.T) {
	failure := errors.New("writer failed")
	for _, failAt := range []int{1, 2, 3, 4, 5, 6, 7} {
		for _, short := range []bool{false, true} {
			t.Run(fmt.Sprintf("write=%d/short=%v", failAt, short), func(t *testing.T) {
				rows := streamTestRows(t, 1, 2, 3)
				calls := 0
				cursorAtFailure := int64(0)
				writer := streamTestWriter{write: func(p []byte) (int, error) {
					calls++
					if calls == failAt {
						cursorAtFailure = rows.cursor
						if short {
							return len(p) - 1, nil
						}
						return 0, failure
					}
					return len(p), nil
				}}
				err := writeQueryResponse(context.Background(), writer, gin.H{"code": 0}, rows, 1)
				if short {
					require.ErrorIs(t, err, io.ErrShortWrite)
				} else {
					require.ErrorIs(t, err, failure)
				}
				require.Equal(t, failAt, calls)
				require.Equal(t, cursorAtFailure, rows.cursor)
				if failAt <= 2 {
					require.Equal(t, int64(1), rows.cursor)
				}
			})
		}
	}
}

type streamCancelMarshaler struct{ cancel context.CancelFunc }

func (m streamCancelMarshaler) MarshalJSON() ([]byte, error) {
	m.cancel()
	return []byte(`0`), nil
}

func TestWriteQueryResponseCancellation(t *testing.T) {
	t.Run("before metadata", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		rows := streamTestRows(t, 1, 2, 3)
		var got bytes.Buffer
		require.ErrorIs(t, writeQueryResponse(ctx, &got, gin.H{"code": 0}, rows, 1), context.Canceled)
		require.Empty(t, got.String())
		require.Zero(t, rows.cursor)
	})
	t.Run("during metadata encoding", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		rows := streamTestRows(t, 1, 2, 3)
		var got bytes.Buffer
		require.ErrorIs(t, writeQueryResponse(ctx, &got, gin.H{"code": streamCancelMarshaler{cancel}}, rows, 1), context.Canceled)
		require.Empty(t, got.String())
		require.Zero(t, rows.cursor)
	})
	for _, cancelAt := range []int{1, 2, 3, 4, 5} {
		t.Run(fmt.Sprintf("after write %d", cancelAt), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			rows := streamTestRows(t, 1, 2, 3)
			calls := 0
			var got bytes.Buffer
			writer := streamTestWriter{write: func(p []byte) (int, error) {
				calls++
				n, err := got.Write(p)
				if calls == cancelAt {
					cancel()
				}
				return n, err
			}}
			require.ErrorIs(t, writeQueryResponse(ctx, writer, gin.H{"code": 0}, rows, 1), context.Canceled)
			require.Equal(t, cancelAt, calls)
			require.False(t, gojson.Valid(got.Bytes()))
		})
	}
}

func TestWriteQueryResponseEscaping(t *testing.T) {
	paramtable.Init()
	field := &schemapb.FieldData{FieldName: "text", Type: schemapb.DataType_VarChar, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"<>&\u2028\u2029", "comma, brackets[] quote\""}}}}}}
	rows, err := newQueryResponseRows(0, nil, []*schemapb.FieldData{field}, nil, nil, true, nil)
	require.NoError(t, err)
	var got bytes.Buffer
	require.NoError(t, writeQueryResponse(context.Background(), &got, gin.H{"message": "<>&\u2028\u2029"}, rows, 1))
	for _, escaped := range []string{`\u003c`, `\u003e`, `\u0026`, `\u2028`, `\u2029`} {
		require.Contains(t, got.String(), escaped)
	}
	for _, raw := range []string{"<", ">", "&", "\u2028", "\u2029"} {
		require.NotContains(t, got.String(), raw)
	}
	require.Equal(t, map[string]any{"message": "<>&\u2028\u2029", "data": []any{map[string]any{"text": "<>&\u2028\u2029"}, map[string]any{"text": "comma, brackets[] quote\""}}}, numberPreservingJSON(t, got.Bytes()))
}

func TestWriteQueryResponseMetadataError(t *testing.T) {
	rows := streamTestRows(t, 1, 2, 3)
	var got bytes.Buffer
	require.Error(t, writeQueryResponse(context.Background(), &got, gin.H{"cost": math.NaN()}, rows, 1))
	require.Empty(t, got.String())
	require.Zero(t, rows.cursor)
}

func TestWriteQueryResponseBatchProduction(t *testing.T) {
	rows := streamTestRows(t, 1, 2, 3, 4, 5)
	var got bytes.Buffer
	produced := []int64{}
	writer := streamTestWriter{write: func(p []byte) (int, error) {
		if got.Len() > 0 && len(p) > 0 && p[0] == '{' {
			produced = append(produced, rows.cursor)
		}
		return got.Write(p)
	}}
	require.NoError(t, writeQueryResponse(context.Background(), writer, gin.H{"code": 0}, rows, 2))
	require.Equal(t, []int64{2, 4, 5}, produced)
	require.True(t, gojson.Valid(got.Bytes()))
}

func TestWriteQueryResponseEmptyMetadataAndInt64(t *testing.T) {
	paramtable.Init()
	ids := &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{9007199254740993, 9223372036854775807}}}}
	for _, numeric := range []bool{false, true} {
		rows, err := newQueryResponseRows(0, nil, nil, ids, nil, numeric, nil)
		require.NoError(t, err)
		var got bytes.Buffer
		require.NoError(t, writeQueryResponse(context.Background(), &got, nil, rows, 1))
		var first, second any = "9007199254740993", "9223372036854775807"
		if numeric {
			first, second = gojson.Number("9007199254740993"), gojson.Number("9223372036854775807")
		}
		require.Equal(t, map[string]any{"data": []any{map[string]any{DefaultPrimaryFieldName: first}, map[string]any{DefaultPrimaryFieldName: second}}}, numberPreservingJSON(t, got.Bytes()))
	}
}

func TestWriteQueryResponseProjectionError(t *testing.T) {
	paramtable.Init()
	for _, first := range []bool{false, true} {
		field := jsonRespFieldData(`{"value":1}`, `{broken`, `{"value":3}`)
		field.IsDynamic = true
		field.FieldName = "$meta"
		if first {
			field.GetScalars().GetJsonData().Data[0] = []byte(`{broken`)
		}
		rows, err := newQueryResponseRows(0, []string{"value"}, []*schemapb.FieldData{field}, nil, nil, true, nil)
		require.NoError(t, err)
		var got bytes.Buffer
		require.Error(t, writeQueryResponse(context.Background(), &got, gin.H{"code": 0}, rows, 1))
		if first {
			require.Empty(t, got.String())
		} else {
			require.NotEmpty(t, got.String())
			require.False(t, gojson.Valid(got.Bytes()))
			require.Equal(t, int64(1), rows.cursor)
		}
	}
}
