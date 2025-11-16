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

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func isStorageV2(deltalog *datapb.FieldBinlog) bool {
	return len(deltalog.ChildFields) > 0
}

// readDeltalogsInternal is the core function that reads deltalogs and processes delete records.
// It returns both the ordered slices of primary keys and timestamps.
func readDeltalogsInternal(
	ctx context.Context,
	pkType schemapb.DataType,
	deltalogs []*datapb.FieldBinlog,
	option ...storage.RwOption,
) ([]storage.PrimaryKey, []typeutil.Timestamp, error) {
	orderedPks := []storage.PrimaryKey{}
	orderedTss := []typeutil.Timestamp{}
	log := log.Ctx(ctx)

	for _, deltalog := range deltalogs {
		opts := option
		if isStorageV2(deltalog) {
			opts = append(option, storage.WithVersion(storage.StorageV2))
		}
		for _, binlog := range deltalog.Binlogs {
			path := binlog.GetLogPath()
			reader, err := storage.NewDeltalogReader(pkType, []string{path}, opts...)
			if err != nil {
				log.Error("compose delete wrong, malformed delta file", zap.Error(err))
				return nil, nil, err
			}
			defer reader.Close()
			for {
				rec, err := reader.Next()
				if err != nil {
					if err == sio.EOF {
						break
					}
					log.Error("compose delete wrong, failed to read deltalogs", zap.Error(err))
					return nil, nil, err
				}

				for i := 0; i < rec.Len(); i++ {
					var primaryKey storage.PrimaryKey
					switch pkType {
					case schemapb.DataType_Int64:
						pkVal := rec.Column(0).(*array.Int64).Value(i)
						primaryKey = storage.NewInt64PrimaryKey(pkVal)
					case schemapb.DataType_VarChar:
						pkVal := rec.Column(0).(*array.String).Value(i)
						primaryKey = storage.NewVarCharPrimaryKey(pkVal)
					}

					ts := typeutil.Timestamp(rec.Column(common.TimeStampField).(*array.Int64).Value(i))

					orderedPks = append(orderedPks, primaryKey)
					orderedTss = append(orderedTss, ts)
				}
			}
			log.Debug("finished reading deltalog file", zap.String("path", path))
		}
	}

	log.Info("compose delete end", zap.Int("delete entries counts", len(orderedPks)))

	return orderedPks, orderedTss, nil
}

// ComposeDeleteFromDeltalogs reads deltalogs and returns a map of primary key to timestamp.
// For duplicate PKs, only the latest timestamp is kept.
func ComposeDeleteFromDeltalogs(
	ctx context.Context,
	pkType schemapb.DataType,
	deltalogs []*datapb.FieldBinlog,
	option ...storage.RwOption,
) (map[any]typeutil.Timestamp, error) {
	orderedPks, orderedTss, err := readDeltalogsInternal(ctx, pkType, deltalogs, option...)
	if err != nil {
		return nil, err
	}
	switch pkType {
	case schemapb.DataType_Int64:
		pk2Ts := make(map[any]typeutil.Timestamp)
		for i, pk := range orderedPks {
			// if pk already exists, skip if the existing is newer
			if existing, ok := pk2Ts[pk.(*storage.Int64PrimaryKey).Value]; ok && existing > orderedTss[i] {
				continue
			}
			pk2Ts[pk.(*storage.Int64PrimaryKey).Value] = orderedTss[i]
		}
		return pk2Ts, nil
	case schemapb.DataType_VarChar:
		pk2Ts := make(map[any]typeutil.Timestamp)
		for i, pk := range orderedPks {
			// if pk already exists, skip if the existing is newer
			if existing, ok := pk2Ts[pk.(*storage.VarCharPrimaryKey).Value]; ok && existing > orderedTss[i] {
				continue
			}
			pk2Ts[pk.(*storage.VarCharPrimaryKey).Value] = orderedTss[i]
		}
		return pk2Ts, nil
	default:
		return nil, errors.Newf("unsupported pk type %s", pkType.String())
	}
}

// ComposeDeleteDataFromDeltalogs reads deltalogs and returns a DeleteData structure.
// For duplicate PKs, only the latest timestamp is kept.
// The order of delete records is preserved as they appear in the deltalogs (first occurrence).
func ComposeDeleteDataFromDeltalogs(
	ctx context.Context,
	pkType schemapb.DataType,
	deltalogs []*datapb.FieldBinlog,
	option ...storage.RwOption,
) (*storage.DeleteData, error) {
	orderedPks, orderedTss, err := readDeltalogsInternal(ctx, pkType, deltalogs, option...)
	if err != nil {
		return nil, err
	}

	return storage.NewDeleteData(orderedPks, orderedTss), nil
}
