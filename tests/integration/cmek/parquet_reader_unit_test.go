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

package cmek

import (
	"bytes"
	"context"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet"
	"github.com/apache/arrow/go/v17/parquet/file"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	"github.com/apache/arrow/go/v17/parquet/schema"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// Each mode gets fresh properties and readers. A missing key means no
// decryption configuration, rather than an invalid empty configured key.
func readParquetWithFooterKey(raw, key []byte) (arrow.Table, error) {
	props := parquet.NewReaderProperties(memory.DefaultAllocator)
	if key != nil {
		if len(key) != 16 && len(key) != 24 && len(key) != 32 {
			return nil, errors.New("invalid AES key length")
		}
		props.FileDecryptProps = parquet.NewFileDecryptionProperties(parquet.WithFooterKey(string(key)))
	}
	reader, err := openParquetFooter(raw, props)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	arrowReader, err := pqarrow.NewFileReader(reader, pqarrow.ArrowReadProperties{Parallel: false}, memory.DefaultAllocator)
	if err != nil {
		return nil, err
	}
	return arrowReader.ReadTable(context.Background())
}

// Arrow Go panics on GCM footer authentication failures. Recover this exact
// error only while opening the footer; payload reads have no panic recovery.
func openParquetFooter(raw []byte, props *parquet.ReaderProperties) (reader *file.Reader, err error) {
	defer func() {
		if failure := recover(); failure != nil {
			authError, ok := failure.(error)
			if !ok || authError.Error() != "cipher: message authentication failed" {
				panic(failure)
			}
			err = authError
		}
	}()
	return file.NewParquetReader(bytes.NewReader(raw), file.WithReadProps(props))
}

func TestParquetReaderKeyModes(t *testing.T) {
	key := []byte("0123456789abcdef0123456789abcdef")
	raw := parquetKeyModeFixture(t, key)
	t.Run("correct key reads every row group", func(t *testing.T) {
		table, err := readParquetWithFooterKey(raw, key)
		require.NoError(t, err)
		defer table.Release()
		require.EqualValues(t, 4, table.NumRows())
		var values []int64
		for _, chunk := range table.Column(0).Data().Chunks() {
			values = append(values, chunk.(*array.Int64).Int64Values()...)
		}
		require.Equal(t, []int64{7, 19, 31, 41}, values)
	})
	t.Run("missing key has no decryption configuration", func(t *testing.T) {
		table, err := readParquetWithFooterKey(raw, nil)
		require.Nil(t, table)
		require.ErrorContains(t, err, "could not read encrypted metadata, no decryption found in reader's properties")
	})
	t.Run("wrong legal length key fails authentication", func(t *testing.T) {
		wrong := append([]byte(nil), key...)
		wrong[0] ^= 1
		table, err := readParquetWithFooterKey(raw, wrong)
		require.Nil(t, table)
		require.EqualError(t, err, "cipher: message authentication failed")
	})
	t.Run("invalid key length is not authentication evidence", func(t *testing.T) {
		table, err := readParquetWithFooterKey(raw, key[:15])
		require.Nil(t, table)
		require.ErrorContains(t, err, "invalid AES key length")
	})
	t.Run("unsupported bytes are not missing key evidence", func(t *testing.T) {
		table, err := readParquetWithFooterKey([]byte("not a parquet object"), nil)
		require.Nil(t, table)
		require.Error(t, err)
		require.NotContains(t, err.Error(), "no decryption found")
		require.NotContains(t, err.Error(), "message authentication failed")
	})
	t.Run("correct key failure is retained", func(t *testing.T) {
		damaged := append([]byte(nil), raw...)
		damaged[len(damaged)-9] ^= 1
		table, err := readParquetWithFooterKey(damaged, key)
		require.Nil(t, table)
		require.Error(t, err)
	})
	t.Run("unrelated footer panic is retained", func(t *testing.T) {
		props := parquet.NewReaderProperties(memory.DefaultAllocator)
		props.FileDecryptProps = parquet.NewFileDecryptionProperties(parquet.WithFooterKey(""))
		require.PanicsWithValue(t, "no footer key or key retriever", func() {
			_, _ = openParquetFooter(raw, props)
		})
	})
}

func parquetKeyModeFixture(t *testing.T, key []byte) []byte {
	t.Helper()
	root, err := schema.NewGroupNode("schema", parquet.Repetitions.Required, schema.FieldList{
		schema.NewInt64Node("100", parquet.Repetitions.Required, -1),
	}, -1)
	require.NoError(t, err)
	props := parquet.NewWriterProperties(parquet.WithEncryptionProperties(
		parquet.NewFileEncryptionProperties(string(key), parquet.WithFooterKeyMetadata("fixture")),
	))
	var output bytes.Buffer
	writer := file.NewParquetWriter(&output, root, file.WithWriterProps(props))
	for _, values := range [][]int64{{7, 19}, {31, 41}} {
		group := writer.AppendRowGroup()
		column, err := group.NextColumn()
		require.NoError(t, err)
		_, err = column.(*file.Int64ColumnChunkWriter).WriteBatch(values, nil, nil)
		require.NoError(t, err)
		require.NoError(t, column.Close())
		require.NoError(t, group.Close())
	}
	require.NoError(t, writer.Close())
	return output.Bytes()
}
