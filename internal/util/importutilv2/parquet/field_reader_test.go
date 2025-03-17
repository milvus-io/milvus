package parquet

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/parquet"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/testutil"
	"github.com/milvus-io/milvus/pkg/v2/objectstorage"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestInvalidUTF8(t *testing.T) {
	const (
		fieldID = int64(100)
		numRows = 100
	)

	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:    fieldID,
				Name:       "str",
				DataType:   schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{{Key: "max_length", Value: "256"}},
			},
		},
	}

	data := make([]string, numRows)
	for i := 0; i < numRows-1; i++ {
		data[i] = randomString(16)
	}
	data[numRows-1] = "\xc3\x28" // invalid utf-8

	filePath := fmt.Sprintf("test_%d_reader.parquet", rand.Int())
	defer os.Remove(filePath)
	wf, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0o666)
	assert.NoError(t, err)

	pqSchema, err := ConvertToArrowSchema(schema, false)
	assert.NoError(t, err)
	fw, err := pqarrow.NewFileWriter(pqSchema, wf,
		parquet.NewWriterProperties(parquet.WithMaxRowGroupLength(numRows)), pqarrow.DefaultWriterProps())
	assert.NoError(t, err)

	insertData, err := storage.NewInsertData(schema)
	assert.NoError(t, err)
	err = insertData.Data[fieldID].AppendDataRows(data)
	assert.NoError(t, err)

	columns, err := testutil.BuildArrayData(schema, insertData, false)
	assert.NoError(t, err)

	recordBatch := array.NewRecord(pqSchema, columns, numRows)
	err = fw.Write(recordBatch)
	assert.NoError(t, err)
	fw.Close()

	ctx := context.Background()
	f := storage.NewChunkManagerFactory("local", objectstorage.RootPath("/tmp/milvus_test/test_parquet_reader/"))
	cm, err := f.NewPersistentStorageChunkManager(ctx)
	assert.NoError(t, err)
	reader, err := NewReader(ctx, cm, schema, filePath, 64*1024*1024)
	assert.NoError(t, err)

	_, err = reader.Read()
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "contains invalid UTF-8 data"))
}

// TestParseSparseFloatRowVector tests the parseSparseFloatRowVector function
func TestParseSparseFloatRowVector(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		wantMaxIdx uint32
		wantErrMsg string
	}{
		{
			name:       "empty sparse vector",
			input:      "{}",
			wantMaxIdx: 0,
		},
		{
			name:       "key-value format",
			input:      "{\"275574541\":1.5383775}",
			wantMaxIdx: 275574542, // max index 275574541 + 1
		},
		{
			name:       "multiple key-value pairs",
			input:      "{\"1\":0.5,\"10\":1.5,\"100\":2.5}",
			wantMaxIdx: 101, // max index 100 + 1
		},
		{
			name:       "invalid format - missing braces",
			input:      "\"275574541\":1.5383775",
			wantErrMsg: "Invalid JSON string for SparseFloatVector",
		},
		{
			name:       "invalid JSON format",
			input:      "{275574541:1.5383775}",
			wantErrMsg: "Invalid JSON string for SparseFloatVector",
		},
		{
			name:       "malformed JSON",
			input:      "{\"key\": value}",
			wantErrMsg: "Invalid JSON string for SparseFloatVector",
		},
		{
			name:       "non-numeric index",
			input:      "{\"abc\":1.5}",
			wantErrMsg: "Invalid JSON string for SparseFloatVector",
		},
		{
			name:       "non-numeric value",
			input:      "{\"123\":\"abc\"}",
			wantErrMsg: "Invalid JSON string for SparseFloatVector",
		},
		{
			name:       "negative index",
			input:      "{\"-1\":1.5}",
			wantErrMsg: "Invalid JSON string for SparseFloatVector",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rowVec, maxIdx, err := parseSparseFloatRowVector(tt.input)

			if tt.wantErrMsg != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrMsg)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.wantMaxIdx, maxIdx)

			// Verify the rowVec is properly formatted
			if maxIdx > 0 {
				elemCount := len(rowVec) / 8
				assert.Greater(t, elemCount, 0)

				// Check the last index matches our expectation
				lastIdx := typeutil.SparseFloatRowIndexAt(rowVec, elemCount-1)
				assert.Equal(t, tt.wantMaxIdx-1, lastIdx)
			} else {
				assert.Empty(t, rowVec)
			}
		})
	}
}
