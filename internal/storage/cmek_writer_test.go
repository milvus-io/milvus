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
	"context"
	"fmt"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/hook"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type unavailableWriterCipher struct {
	hook.Cipher
	lookups int
}

func (c *unavailableWriterCipher) GetUnsafeKey(int64, int64) []byte { c.lookups++; return nil }
func (*unavailableWriterCipher) GetEncryptor(int64, int64) (hook.Encryptor, []byte, error) {
	return nil, nil, nil
}

func TestBinlogWriterRequiresTargetKeyBeforeOutput(t *testing.T) {
	paramtable.Init()
	for _, version := range []int64{StorageV2, StorageV3} {
		t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
			cipher := &unavailableWriterCipher{}
			patch := mockey.Mock(hookutil.GetCipher).Return(cipher).Build()
			defer patch.UnPatch()
			schema := &schemapb.CollectionSchema{Name: "encrypted", Fields: []*schemapb.FieldSchema{
				{FieldID: common.RowIDField, DataType: schemapb.DataType_Int64},
				{FieldID: common.TimeStampField, DataType: schemapb.DataType_Int64},
				{FieldID: 100, Name: "pk", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
			}, Properties: []*commonpb.KeyValuePair{{Key: common.EncryptionEzIDKey, Value: "17"}}}
			options := []RwOption{WithVersion(version), WithStorageConfig(&indexpb.StorageConfig{StorageType: "local", RootPath: t.TempDir()})}
			// A nil allocator also makes any attempted output allocation fail
			// loudly. Missing and empty contexts must fail before that point.
			for _, supplied := range []*indexcgopb.StoragePluginContext{nil, {EncryptionZoneId: 17, CollectionId: 23}} {
				writer, err := NewBinlogRecordWriter(context.Background(), 23, 29, 31, schema, nil, 1<<20, 128, append(options, WithPluginContext(supplied))...)
				require.Nil(t, writer)
				require.ErrorIs(t, err, merr.ErrServiceUnavailable)
			}
			cipher.lookups = 0
			acquired := &indexcgopb.StoragePluginContext{EncryptionZoneId: 17, CollectionId: 23, EncryptionKey: "previously-acquired-key"}
			writer, err := NewBinlogRecordWriter(context.Background(), 23, 29, 31, schema, nil, 1<<20, 128, append(options, WithPluginContext(acquired))...)
			require.NoError(t, err)
			require.Zero(t, cipher.lookups, "valid in-flight context must not require another key lookup")
			switch actual := writer.(type) {
			case *PackedBinlogRecordWriter:
				require.Same(t, acquired, actual.storagePluginContext)
			case *PackedManifestRecordWriter:
				require.Same(t, acquired, actual.storagePluginContext)
			default:
				t.Fatalf("unexpected writer %T", writer)
			}
			schema.Properties = nil
			writer, err = NewBinlogRecordWriter(context.Background(), 23, 29, 31, schema, nil, 1<<20, 128, options...)
			require.NoError(t, err)
			switch actual := writer.(type) {
			case *PackedBinlogRecordWriter:
				require.Nil(t, actual.storagePluginContext)
			case *PackedManifestRecordWriter:
				require.Nil(t, actual.storagePluginContext)
			}
		})
	}
}
