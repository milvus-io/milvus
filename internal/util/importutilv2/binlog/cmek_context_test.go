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
	"context"
	"math"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/hook"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type targetKeyCipher struct {
	hook.Cipher
	lookups int
}

func (c *targetKeyCipher) GetUnsafeKey(int64, int64) []byte {
	c.lookups++
	return []byte("target-key")
}

func TestBackupPlaintextDoesNotBorrowTargetKey(t *testing.T) {
	paramtable.Init()
	cipher := &targetKeyCipher{}
	enabled := mockey.Mock(hookutil.IsClusterEncryptionEnabled).Return(true).Build()
	t.Cleanup(func() { enabled.UnPatch() })
	plugin := mockey.Mock(hookutil.GetCipher).Return(cipher).Build()
	t.Cleanup(func() { plugin.UnPatch() })
	list := mockey.Mock(listInsertLogs).Return(map[int64][]string{0: {genBinlogPath(0)}}, nil).Build()
	t.Cleanup(func() { list.UnPatch() })
	footer := mockey.Mock(packed.GetPackedFileFieldIDs).To(
		func(_ []string, _ *indexpb.StorageConfig, pluginContext *indexcgopb.StoragePluginContext) ([][]int64, error) {
			require.Nil(t, pluginContext)
			return [][]int64{{100}}, nil
		}).Build()
	t.Cleanup(func() { footer.UnPatch() })
	target := &schemapb.CollectionSchema{
		Properties: []*commonpb.KeyValuePair{{Key: common.EncryptionEzIDKey, Value: "37"}},
		Fields:     []*schemapb.FieldSchema{{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true}},
	}
	// Legacy backup and Import V3 reshard both enter this reader with the
	// destination schema. Neither a source EZK nor a source context is given.
	reader, err := NewReader(context.Background(), nil, target,
		&indexpb.StorageConfig{StorageType: "local", RootPath: t.TempDir()},
		storage.StorageV2, []string{"backup/segment"}, 0, math.MaxUint64, 1024, "", nil)
	require.NoError(t, err)
	defer reader.Close()
	require.Equal(t, 1, footer.Times())
	require.Zero(t, cipher.lookups, "a plaintext source must not resolve the destination's key")
}

func TestL0ReaderPreservesEncryptedLegacyError(t *testing.T) {
	ctx := context.Background()
	cm := storage.NewLocalChunkManager()
	root := t.TempDir()
	cfg := &indexpb.StorageConfig{StorageType: "local", RootPath: root}
	path := root + "/delta/1"
	// A valid encrypted binlog descriptor is sufficient. Registration fails
	// before the reader opens the first event.
	writer := storage.NewDeleteBinlogWriter(schemapb.DataType_String, 31, 1, 2, storage.WithWriterEncryptionContext(17, []byte("EDEK"), nil))
	defer writer.Close()
	writer.AddExtra("original_size", "0")
	writer.SetEventTimeStamp(1, 2)
	require.NoError(t, writer.Finish())
	data, err := writer.GetBuffer()
	require.NoError(t, err)
	require.NoError(t, cm.Write(ctx, path, data))
	register := mockey.Mock(hookutil.RegisterImportReadKeys).Return(merr.ErrServiceUnavailable).Build()
	defer register.UnPatch()
	packedOpen := mockey.Mock(packed.NewPackedReaderWithExtfs).Return(nil, merr.ErrDataIntegrity).Build()
	defer packedOpen.UnPatch()
	for _, tc := range []struct {
		name   string
		source *indexcgopb.StoragePluginContext
		want   error
	}{
		{"plaintext task", nil, merr.ErrDataIntegrity},
		{"other zone", &indexcgopb.StoragePluginContext{EncryptionZoneId: 9, EncryptionKey: "c291cmNl"}, merr.ErrDataIntegrity},
		{"registration failed", &indexcgopb.StoragePluginContext{EncryptionZoneId: 17, EncryptionKey: "c291cmNl"}, merr.ErrServiceUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opt := storage.WithImportReadContext(tc.source)
			r, err := NewL0Reader(ctx, cm, cfg, &schemapb.FieldSchema{IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
				&internalpb.ImportFile{Paths: []string{root + "/delta"}}, 1024, 0, math.MaxUint64, opt)
			require.NoError(t, err)
			_, err = r.Read()
			require.ErrorIs(t, err, tc.want)
			merger, err := NewDeleteMerger(cm, cfg, schemapb.DataType_Int64, 1, 1024, opt)
			require.NoError(t, err)
			_, err = merger.Merge(ctx, []string{path}, 0, math.MaxUint64, true)
			require.ErrorIs(t, err, tc.want)
			require.Zero(t, packedOpen.Times())
		})
	}
	require.EqualValues(t, 2, register.Times())
}
