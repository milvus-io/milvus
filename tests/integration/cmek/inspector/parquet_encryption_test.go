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

package inspector

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet"
	"github.com/apache/arrow/go/v17/parquet/file"
	"github.com/apache/arrow/go/v17/parquet/schema"
	"github.com/stretchr/testify/require"
)

func TestInspectEncryptedParquetValidatesEncryptedParquetEnvelope(t *testing.T) {
	raw := encryptedParquetFixture(t, "17_23_fixture-edek", parquet.AesGcm)
	edek, err := InspectEncryptedParquet(raw, 17, 23)
	require.NoError(t, err)
	require.Equal(t, "fixture-edek", edek)
}

func TestInspectEncryptedParquetRejectsWrongIdentityAndCipher(t *testing.T) {
	raw := encryptedParquetFixture(t, "17_23_fixture-edek", parquet.AesGcm)
	_, err := InspectEncryptedParquet(raw, 18, 23)
	require.ErrorContains(t, err, "EZ id 17")
	_, err = InspectEncryptedParquet(raw, 17, 24)
	require.ErrorContains(t, err, "collection id 23")

	raw = encryptedParquetFixture(t, "17_23_fixture-edek", parquet.AesCtr)
	_, err = InspectEncryptedParquet(raw, 17, 23)
	require.ErrorContains(t, err, "AES_GCM_V1")
}

func TestInspectEncryptedParquetRejectsPlaintextParquet(t *testing.T) {
	root, err := schema.NewGroupNode("schema", parquet.Repetitions.Required, schema.FieldList{
		schema.NewInt64Node("value", parquet.Repetitions.Required, -1),
	}, -1)
	require.NoError(t, err)
	var sink bytes.Buffer
	writer := file.NewParquetWriter(&sink, root)
	require.NoError(t, writer.Close())

	_, err = InspectEncryptedParquet(sink.Bytes(), 17, 23)
	require.ErrorContains(t, err, "encrypted footer")
}

func encryptedParquetFixture(t *testing.T, keyMetadata string, cipher parquet.Cipher) []byte {
	t.Helper()
	root, err := schema.NewGroupNode("schema", parquet.Repetitions.Required, schema.FieldList{
		schema.NewInt64Node("value", parquet.Repetitions.Required, -1),
	}, -1)
	require.NoError(t, err)
	properties := parquet.NewWriterProperties(parquet.WithEncryptionProperties(
		parquet.NewFileEncryptionProperties("0123456789abcdef", parquet.WithFooterKeyMetadata(keyMetadata), parquet.WithAlg(cipher)),
	))
	var sink bytes.Buffer
	writer := file.NewParquetWriter(&sink, root, file.WithWriterProps(properties))
	require.NoError(t, writer.Close())
	return append([]byte(nil), sink.Bytes()...)
}

func TestInspectEncryptedParquetRejectsMalformedKeyMetadata(t *testing.T) {
	for _, test := range []struct {
		name     string
		metadata string
		want     string
	}{
		{name: "missing parts", metadata: "17_23", want: "must be <ezID>_<collectionID>_<EDEK>"},
		{name: "empty EDEK", metadata: "17_23_", want: "must be <ezID>_<collectionID>_<EDEK>"},
		{name: "invalid EZ", metadata: "bad_23_edek", want: "invalid EZ id"},
		{name: "invalid collection", metadata: "17_bad_edek", want: "invalid collection id"},
	} {
		t.Run(test.name, func(t *testing.T) {
			raw := encryptedParquetFixture(t, test.metadata, parquet.AesGcm)
			_, err := InspectEncryptedParquet(raw, 17, 23)
			require.ErrorContains(t, err, test.want)
		})
	}
}

func TestInspectEncryptedParquetRejectsDamagedFooter(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func([]byte) []byte
		want   string
	}{
		{name: "truncated header", mutate: func(raw []byte) []byte { return raw[:4] }, want: "encrypted footer"},
		{name: "zero footer size", mutate: func(raw []byte) []byte {
			binary.LittleEndian.PutUint32(raw[len(raw)-8:], 0)
			return raw
		}, want: "invalid size"},
		{name: "footer extends before header", mutate: func(raw []byte) []byte {
			binary.LittleEndian.PutUint32(raw[len(raw)-8:], uint32(len(raw)))
			return raw
		}, want: "invalid size"},
		{name: "truncated crypto metadata", mutate: func(raw []byte) []byte {
			// A compact-protocol field header without its required value.
			raw = append(append([]byte(nil), raw[:4]...), 0x18, 1, 0, 0, 0, 'P', 'A', 'R', 'E')
			return raw
		}, want: "parse encrypted Parquet crypto metadata"},
	} {
		t.Run(test.name, func(t *testing.T) {
			raw := encryptedParquetFixture(t, "17_23_edek", parquet.AesGcm)
			_, err := InspectEncryptedParquet(test.mutate(raw), 17, 23)
			require.ErrorContains(t, err, test.want)
		})
	}
}

func TestReadEncryptedParquetKeyModes(t *testing.T) {
	key := []byte("0123456789abcdef0123456789abcdef")
	raw := parquetKeyModeFixture(t, key)
	t.Run("correct key reads every row group", func(t *testing.T) {
		table, err := ReadEncryptedParquet(raw, key)
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
		table, err := ReadEncryptedParquet(raw, nil)
		require.Nil(t, table)
		require.ErrorContains(t, err, "could not read encrypted metadata, no decryption found in reader's properties")
	})
	t.Run("wrong legal length key fails authentication", func(t *testing.T) {
		wrong := append([]byte(nil), key...)
		wrong[0] ^= 1
		table, err := ReadEncryptedParquet(raw, wrong)
		require.Nil(t, table)
		require.EqualError(t, err, "cipher: message authentication failed")
	})
	t.Run("invalid key length is not authentication evidence", func(t *testing.T) {
		table, err := ReadEncryptedParquet(raw, key[:15])
		require.Nil(t, table)
		require.ErrorContains(t, err, "invalid AES key length")
	})
	t.Run("unsupported bytes are not missing key evidence", func(t *testing.T) {
		table, err := ReadEncryptedParquet([]byte("not a parquet object"), nil)
		require.Nil(t, table)
		require.Error(t, err)
		require.NotContains(t, err.Error(), "no decryption found")
		require.NotContains(t, err.Error(), "message authentication failed")
	})
	t.Run("correct key failure is retained", func(t *testing.T) {
		damaged := append([]byte(nil), raw...)
		damaged[len(damaged)-9] ^= 1
		table, err := ReadEncryptedParquet(damaged, key)
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
