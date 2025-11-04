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

package compaction

import (
	"context"
	sio "io"
	"strings"

	"go.uber.org/zap"

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func isStorageV2(path string) bool {
	return strings.HasSuffix(path, ".parquet")
}

func ComposeDeleteFromDeltalogs(
	ctx context.Context,
	pkField *schemapb.FieldSchema,
	paths []string,
	option ...storage.RwOption,
) (map[any]typeutil.Timestamp, error) {
	pk2Ts := make(map[any]typeutil.Timestamp)
	log := log.Ctx(ctx)

	for _, path := range paths {
		opts := option
		if isStorageV2(path) {
			opts = append(option, storage.WithVersion(storage.StorageV2))
		}
		reader, err := storage.NewDeltalogReader(pkField, []string{path}, opts...)
		if err != nil {
			log.Error("compose delete wrong, malformed delta file", zap.Error(err))
			return nil, err
		}
		defer reader.Close()
		for {
			rec, err := reader.Next()
			if err != nil {
				if err == sio.EOF {
					break
				}
				log.Error("compose delete wrong, failed to read deltalogs", zap.Error(err))
				return nil, err
			}

			for i := 0; i < rec.Len(); i++ {
				var pk any
				switch pkField.DataType {
				case schemapb.DataType_Int64:
					pk = rec.Column(pkField.FieldID).(*array.Int64).Value(i)
				case schemapb.DataType_VarChar:
					pk = rec.Column(pkField.FieldID).(*array.String).Value(i)
				}

				ts := typeutil.Timestamp(rec.Column(common.TimeStampField).(*array.Int64).Value(i))
				if tsExisting, ok := pk2Ts[pk]; ok && tsExisting > ts {
					// skip if existing entry is newer
					continue
				}
				pk2Ts[pk] = ts
			}
		}
	}

	log.Info("compose delete end", zap.Int("delete entries counts", len(pk2Ts)))
	return pk2Ts, nil
}
