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

package binlog

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"math"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/hook"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type importReaderCipher struct {
	hook.Cipher
	lastZone int64
}

func (c *importReaderCipher) GetDecryptor(zone, collection int64, edek []byte) (hook.Decryptor, error) {
	c.lastZone = zone
	if (zone != 7 && zone != 1<<53+1) || collection != 1 || string(edek) != "file-DEK" {
		return nil, errors.New("incorrect source decryption context")
	}
	return importTestCrypt{}, nil
}

type importTestCrypt struct{}

func (importTestCrypt) Encrypt(plain []byte) ([]byte, error) {
	return append([]byte("encrypted:"), plain...), nil
}

func (importTestCrypt) Decrypt(ciphertext []byte) ([]byte, error) {
	plain, ok := bytes.CutPrefix(ciphertext, []byte("encrypted:"))
	if !ok {
		return nil, errors.New("missing test encryption prefix")
	}
	return plain, nil
}

func writeImportTestDeltalog(t *testing.T, cm storage.ChunkManager, path string, zone int64) {
	t.Helper()
	writer := storage.NewDeleteBinlogWriter(schemapb.DataType_String, 1, 1, 2,
		storage.WithWriterEncryptionContext(zone, []byte("file-DEK"), importTestCrypt{}))
	writer.SetEventTimeStamp(100, 200)
	event, err := writer.NextDeleteEventWriter()
	require.NoError(t, err)
	require.NoError(t, event.AddOneStringToPayload(`{"pk":42,"ts":150,"pkType":5}`, true))
	event.SetEventTimestamp(100, 200)
	writer.AddExtra("original_size", "40")
	require.NoError(t, writer.Finish())
	data, err := writer.GetBuffer()
	require.NoError(t, err)
	defer writer.Close()

	require.NoError(t, cm.Write(context.Background(), path+"/deletes", data))
}

func testImportSource() *indexcgopb.StoragePluginContext {
	return &indexcgopb.StoragePluginContext{
		EncryptionZoneId: 7, EncryptionKey: base64.StdEncoding.EncodeToString([]byte("historical")),
	}
}

func TestL0ReaderUsesDispatchedSourceForEncryptedDeltalog(t *testing.T) {
	t.Cleanup(hookutil.SetTestCipher(&importReaderCipher{}))
	ctx := context.Background()
	cm := storage.NewLocalChunkManager()
	path := t.TempDir()
	writeImportTestDeltalog(t, cm, path, 7)
	r, err := NewL0Reader(ctx, cm, nil, &internalpb.ImportFile{Paths: []string{path}}, 1024, 0, math.MaxUint64)
	require.NoError(t, err)
	deleted, err := r.Read()
	require.NoError(t, err)
	require.Equal(t, int64(42), deleted.Pks[0].GetValue())
	require.EqualValues(t, 150, deleted.Tss[0])
}

func TestImportBinlogValidatesStoredZoneWithoutRounding(t *testing.T) {
	const fileZone int64 = 1<<53 + 1
	ctx := context.Background()
	cm := storage.NewLocalChunkManager()
	path := t.TempDir()
	writeImportTestDeltalog(t, cm, path, fileZone)
	data, err := cm.Read(ctx, path+"/deletes")
	require.NoError(t, err)
	reader, err := storage.NewBinlogReader(data)
	require.NoError(t, err)
	stored, ok := reader.GetEzID()
	require.True(t, ok)
	require.Equal(t, fileZone, stored)
	reader.Close()

	cipher := &importReaderCipher{}
	t.Cleanup(hookutil.SetTestCipher(cipher))
	_, err = storage.NewBinlogReader(data, storage.DecryptBinlogFromHeader)
	require.NoError(t, err)
	require.Equal(t, fileZone, cipher.lastZone)
}

func TestBackupReaderUsesDispatchedSourceForInsertAndDelta(t *testing.T) {
	ctx := context.Background()
	cm := storage.NewLocalChunkManager()
	schema := typeutil.AppendSystemFields(&schemapb.CollectionSchema{
		Fields:     []*schemapb.FieldSchema{{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true}},
		Properties: []*commonpb.KeyValuePair{{Key: common.EncryptionEzIDKey, Value: "7"}},
	})
	for _, tc := range []struct {
		name     string
		source   *indexcgopb.StoragePluginContext
		expected []int64
	}{
		{"encrypted backup with a later delete", testImportSource(), []int64{41}},
		{"plaintext backup into encrypted collection", nil, []int64{41, 42}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Cleanup(hookutil.SetTestCipher(&importReaderCipher{}))
			path := t.TempDir()
			paths := []string{path + "/insert/"}
			var opts []storage.BinlogWriterOptions
			if tc.source != nil {
				opts = append(opts, storage.WithWriterEncryptionContext(7, []byte("file-DEK"), importTestCrypt{}))
				writeImportTestDeltalog(t, cm, path+"/delta", 7)
				paths = append(paths, path+"/delta/")
			}
			for _, field := range schema.GetFields() {
				values := []int64{41, 42}
				if field.FieldID == common.TimeStampField {
					values = []int64{100, 100}
				}
				data := createBinlogBuf(t, field, &storage.Int64FieldData{Data: values}, opts...)
				require.NoError(t, cm.Write(ctx, fmt.Sprintf("%s/insert/%d/1", path, field.FieldID), data))
			}
			r, err := NewReader(ctx, cm, schema, &indexpb.StorageConfig{}, storage.StorageV1,
				paths, 0, math.MaxUint64, 1024, tc.source)
			require.NoError(t, err)
			defer r.Close()
			data, err := r.Read()
			require.NoError(t, err)
			require.Equal(t, tc.expected, data.Data[100].(*storage.Int64FieldData).Data)
		})
	}
}

func TestPackedBackupReaderForwardsDispatchedSource(t *testing.T) {
	ctx := context.Background()
	cm := storage.NewLocalChunkManager()
	dir := t.TempDir()
	require.NoError(t, cm.Write(ctx, dir+"/0/1", nil))
	config := &indexpb.StorageConfig{StorageType: "local", RootPath: dir}
	schema := &schemapb.CollectionSchema{
		Fields:     []*schemapb.FieldSchema{{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true}},
		Properties: []*commonpb.KeyValuePair{{Key: common.EncryptionEzIDKey, Value: "7"}},
	}
	for _, tc := range []struct {
		name   string
		source *indexcgopb.StoragePluginContext
	}{{"same source and target zone", testImportSource()}, {"plaintext source", nil}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Cleanup(hookutil.SetTestCipher(&importReaderCipher{}))
			openErr := errors.New("packed reader open error")
			open := mockey.Mock(packed.NewPackedReader).To(func(paths []string, _ *arrow.Schema, _ int64, _ *indexpb.StorageConfig, source *indexcgopb.StoragePluginContext) (*packed.PackedReader, error) {
				require.Equal(t, []string{dir + "/0/1"}, paths)
				require.Equal(t, tc.source, source)
				return nil, openErr
			}).Build()
			defer open.UnPatch()
			r, err := NewReader(ctx, cm, schema, config, storage.StorageV2, []string{dir + "/"}, 0, math.MaxUint64, 1024, tc.source)
			require.NoError(t, err)
			defer r.Close()
			_, err = r.Read()
			require.ErrorIs(t, err, openErr)
		})
	}
}
