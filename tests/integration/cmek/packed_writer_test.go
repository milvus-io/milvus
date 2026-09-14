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
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// Exercise the real Go -> C++ -> Loon writer and reader boundaries. A NUL at
// byte 16 or 24 is especially dangerous: the truncated prefix is still a valid AES key.
func TestFFIPackedWriterPreservesBinaryKey(t *testing.T) {
	paramtable.Init()
	cipherParams := paramtable.GetCipherParams()
	oldPath := cipherParams.SoPathGo.GetValue()
	oldCppPath := cipherParams.SoPathCpp.GetValue()
	require.NoError(t, cipherParams.Save(cipherParams.SoPathGo.Key, testGoPluginPath()))
	require.NoError(t, cipherParams.Save(cipherParams.SoPathCpp.Key, fixtureCppPluginPath))
	t.Cleanup(func() {
		require.NoError(t, cipherParams.Save(cipherParams.SoPathGo.Key, oldPath))
		require.NoError(t, cipherParams.Save(cipherParams.SoPathCpp.Key, oldCppPath))
	})
	require.NoError(t, initcore.InitPluginLoader())
	t.Cleanup(initcore.CleanPluginLoader)
	const ezID, collectionID = int64(17), int64(23)
	ezKey := fixtureHMAC([]byte("milvus-cmek-fixture-master-v1"), "ezk-v1\x00", nil, ezID)
	pluginContext := &indexcgopb.StoragePluginContext{
		EncryptionZoneId: ezID, CollectionId: collectionID,
		EncryptionKey: base64.StdEncoding.EncodeToString(ezKey),
	}
	schema := arrow.NewSchema([]arrow.Field{{Name: "100", Type: arrow.PrimitiveTypes.Int64}}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	values := []int64{0, 17, 511}
	builder.Field(0).(*array.Int64Builder).AppendValues(values, nil)
	record := builder.NewRecord()
	defer record.Release()

	for _, offset := range []int{0, 16, 24, 31} {
		t.Run(fmt.Sprintf("nul_at_%d", offset), func(t *testing.T) {
			t.Setenv("MILVUS_CMEK_FIXTURE_DEK_NUL_AT", strconv.Itoa(offset))
			root := t.TempDir()
			cfg := &indexpb.StorageConfig{StorageType: "local", RootPath: root}
			writer, err := packed.NewFFIPackedWriter("binary-key", schema,
				[]storagecommon.ColumnGroup{{Columns: []int{0}, GroupID: storagecommon.DefaultShortColumnGroupID}},
				cfg, pluginContext, map[string]string{packed.PropertyWriterFormat: "parquet"})
			require.NoError(t, err)
			defer writer.Destroy()
			require.NoError(t, writer.WriteRecordBatch(record))
			output, err := writer.Close()
			require.NoError(t, err)
			defer output.Destroy()
			paths, err := filepath.Glob(filepath.Join(root, "binary-key", "_data", "*.parquet"))
			require.NoError(t, err)
			require.Len(t, paths, 1)
			raw, err := os.ReadFile(paths[0])
			require.NoError(t, err)
			require.Equal(t, "PARE", string(raw[:4]))
			require.Equal(t, "PARE", string(raw[len(raw)-4:]))
			key := fixtureParquetKey(t, raw, ezID, collectionID)
			require.Len(t, key, 32)
			require.Equal(t, offset, bytes.IndexByte(key, 0))
			table, err := readParquetWithFooterKey(raw, key)
			require.NoError(t, err)
			defer table.Release()
			require.EqualValues(t, len(values), table.NumRows())
			require.EqualValues(t, 1, table.NumCols())
			actual := make([]int64, 0, len(values))
			for _, chunk := range table.Column(0).Data().Chunks() {
				actual = append(actual, chunk.(*array.Int64).Int64Values()...)
			}
			require.Equal(t, values, actual)
			if offset == 16 || offset == 24 {
				wrongTable, err := readParquetWithFooterKey(raw, key[:offset])
				if wrongTable != nil {
					wrongTable.Release()
				}
				require.ErrorContains(t, err, "cipher: message authentication failed")
			}

			// Read the same file through Milvus's native C++ key retriever too.
			manifest, err := packed.CommitManifestUpdates("binary-key", packed.ManifestEarliest, cfg,
				&packed.ManifestUpdates{NewFiles: output})
			require.NoError(t, err)
			reader, err := packed.NewFFIPackedReader(manifest, schema, []string{"100"}, 8192,
				cfg, pluginContext, packed.ExternalReaderContext{})
			require.NoError(t, err)
			defer func() { require.NoError(t, reader.Close()) }()
			actual = nil
			for {
				batch, err := reader.ReadNext()
				if err == io.EOF {
					break
				}
				require.NoError(t, err)
				require.NotNil(t, batch)
				require.EqualValues(t, 1, batch.NumCols())
				actual = append(actual, batch.Column(0).(*array.Int64).Int64Values()...)
			}
			require.Equal(t, values, actual)
		})
	}

	t.Run("key_allocation_failure", func(t *testing.T) {
		t.Setenv("MILVUS_CMEK_FIXTURE_GET_KEY_BAD_ALLOC", "1")
		cfg := &indexpb.StorageConfig{StorageType: "local", RootPath: t.TempDir()}
		writer, err := packed.NewFFIPackedWriter("allocation-failure", schema,
			[]storagecommon.ColumnGroup{{Columns: []int{0}, GroupID: storagecommon.DefaultShortColumnGroupID}},
			cfg, pluginContext, map[string]string{packed.PropertyWriterFormat: "parquet"})
		if writer != nil {
			defer writer.Destroy()
		}
		require.Error(t, err)
		require.Nil(t, writer)
		require.EqualValues(t, merr.CodeMemAllocateFailed, merr.Code(err))
		require.Equal(t, merr.SystemError, merr.GetErrorType(err))
		require.True(t, merr.Status(err).GetRetriable())
	})
}

func fixtureHMAC(key []byte, domain string, nonce []byte, ids ...int64) []byte {
	h := hmac.New(sha256.New, key)
	_, _ = h.Write([]byte(domain))
	_, _ = h.Write(nonce)
	for _, id := range ids {
		var encoded [8]byte
		binary.BigEndian.PutUint64(encoded[:], uint64(id))
		_, _ = h.Write(encoded[:])
	}
	return h.Sum(nil)
}
