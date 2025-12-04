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

package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestDeltalogReaderWriter(t *testing.T) {
	const (
		testCollectionID = int64(1)
		testPartitionID  = int64(2)
		testSegmentID    = int64(3)
		testBatchSize    = 1024
		testNumLogs      = 100
	)

	type deleteLogGenerator func(i int) *DeleteLog

	tests := []struct {
		name         string
		format       string
		pkType       schemapb.DataType
		logGenerator deleteLogGenerator
		wantErr      bool
	}{
		{
			name:   "Int64 PK - JSON format",
			format: "json",
			pkType: schemapb.DataType_Int64,
			logGenerator: func(i int) *DeleteLog {
				return NewDeleteLog(NewInt64PrimaryKey(int64(i)), uint64(100+i))
			},
			wantErr: false,
		},
		{
			name:   "VarChar PK - JSON format",
			format: "json",
			pkType: schemapb.DataType_VarChar,
			logGenerator: func(i int) *DeleteLog {
				return NewDeleteLog(NewVarCharPrimaryKey("key_"+string(rune(i))), uint64(100+i))
			},
			wantErr: false,
		},
		{
			name:   "Int64 PK - Parquet format",
			format: "parquet",
			pkType: schemapb.DataType_Int64,
			logGenerator: func(i int) *DeleteLog {
				return NewDeleteLog(NewInt64PrimaryKey(int64(i)), uint64(100+i))
			},
			wantErr: false,
		},
		{
			name:   "VarChar PK - Parquet format",
			format: "parquet",
			pkType: schemapb.DataType_VarChar,
			logGenerator: func(i int) *DeleteLog {
				return NewDeleteLog(NewVarCharPrimaryKey("key_"+string(rune(i))), uint64(100+i))
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set deltalog format
			originalFormat := paramtable.Get().DataNodeCfg.DeltalogFormat.GetValue()
			paramtable.Get().Save(paramtable.Get().DataNodeCfg.DeltalogFormat.Key, tt.format)
			defer paramtable.Get().Save(paramtable.Get().DataNodeCfg.DeltalogFormat.Key, originalFormat)

			writer, finalizer, err := createDeltalogWriter(testCollectionID, testPartitionID, testSegmentID, tt.pkType, testBatchSize)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.NotNil(t, writer)
			assert.NotNil(t, finalizer)

			// Write delete logs
			expectedLogs := make([]*DeleteLog, 0, testNumLogs)
			for i := 0; i < testNumLogs; i++ {
				deleteLog := tt.logGenerator(i)
				expectedLogs = append(expectedLogs, deleteLog)
				err = writer.WriteValue(deleteLog)
				require.NoError(t, err)
			}

			err = writer.Close()
			require.NoError(t, err)

			blob, err := finalizer()
			require.NoError(t, err)
			assert.NotNil(t, blob)
			assert.Greater(t, len(blob.Value), 0)

			// Test round trip
			reader, err := CreateDeltalogReader([]*Blob{blob})
			require.NoError(t, err)
			require.NotNil(t, reader)

			// Read and verify contents
			readLogs := make([]*DeleteLog, 0)
			for {
				log, err := reader.NextValue()
				if err != nil {
					break
				}
				if log != nil {
					readLogs = append(readLogs, *log)
				}
			}

			assert.Equal(t, len(expectedLogs), len(readLogs))
			for i := 0; i < len(expectedLogs); i++ {
				assert.Equal(t, expectedLogs[i].Ts, readLogs[i].Ts)
				assert.Equal(t, expectedLogs[i].Pk.GetValue(), readLogs[i].Pk.GetValue())
			}

			err = reader.Close()
			assert.NoError(t, err)
		})
	}
}

func TestDeltalogStreamWriter_NoRecordWriter(t *testing.T) {
	writer := newDeltalogStreamWriter(1, 2, 3)
	assert.NotNil(t, writer)

	// Finalize without getting record writer should return error
	blob, err := writer.Finalize()
	assert.Error(t, err)
	assert.Nil(t, blob)
}
