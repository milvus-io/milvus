// Copyright 2023 Zilliz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package packed

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestPackedWriterRejectsMissingCipherPlugin(t *testing.T) {
	paramtable.Init()
	// This package has no native cipher plugin loaded. A supplied encryption
	// context must therefore fail, rather than selecting plaintext Parquet.
	initcore.InitLocalArrowFileSystem("/tmp")
	schema := arrow.NewSchema([]arrow.Field{{Name: "pk", Type: arrow.PrimitiveTypes.Int64, Metadata: arrow.NewMetadata([]string{ArrowFieldIdMetadataKey}, []string{"100"})}}, nil)
	for _, explicitStorage := range []bool{false, true} {
		t.Run(fmt.Sprintf("storage_config=%t", explicitStorage), func(t *testing.T) {
			root := t.TempDir()
			output := filepath.Join(root, "data.parquet")
			var cfg *indexpb.StorageConfig
			if explicitStorage {
				cfg = &indexpb.StorageConfig{StorageType: "local", RootPath: root}
			}
			writer, err := NewPackedWriter([]string{output}, schema, 1<<20, 0, []storagecommon.ColumnGroup{{GroupID: 0, Columns: []int{0}, Fields: []int64{100}}}, cfg, &indexcgopb.StoragePluginContext{EncryptionZoneId: 17, CollectionId: 23, EncryptionKey: "required-key"})
			if writer != nil {
				_ = writer.Close()
			}
			require.ErrorContains(t, err, "cipher plugin")
			require.NotEqual(t, "true", merr.Status(err).GetExtraInfo()["is_input_error"])
			require.Nil(t, writer)
			_, err = os.Stat(output)
			require.True(t, os.IsNotExist(err), "missing plugin created an output file: %v", err)
		})
	}
}
