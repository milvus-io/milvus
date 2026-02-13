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
	"fmt"
	"io"
	"math"

	"github.com/apache/arrow/go/v17/arrow/array"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type L0Reader interface {
	Read() (*storage.DeleteData, error)
}

type l0Reader struct {
	ctx           context.Context
	cm            storage.ChunkManager
	storageConfig *indexpb.StorageConfig
	pkField       *schemapb.FieldSchema

	bufferSize int
	deltaLogs  []string
	readIdx    int

	// new filter-based approach
	filters []L0Filter
}

func NewL0Reader(ctx context.Context,
	cm storage.ChunkManager,
	storageConfig *indexpb.StorageConfig,
	pkField *schemapb.FieldSchema,
	importFile *internalpb.ImportFile,
	bufferSize int,
	tsStart,
	tsEnd uint64,
) (*l0Reader, error) {
	r := &l0Reader{
		ctx:           ctx,
		cm:            cm,
		storageConfig: storageConfig,
		pkField:       pkField,
		bufferSize:    bufferSize,
	}

	// Initialize filters
	r.initFilters(tsStart, tsEnd)

	if len(importFile.GetPaths()) != 1 {
		return nil, merr.WrapErrImportFailed(
			fmt.Sprintf("there should be one prefix, but got %s", importFile.GetPaths()))
	}
	path := importFile.GetPaths()[0]
	deltaLogs, _, err := storage.ListAllChunkWithPrefix(context.Background(), r.cm, path, true)
	if err != nil {
		return nil, err
	}
	if len(deltaLogs) == 0 {
		log.Info("no delta logs for l0 segments", zap.String("prefix", path))
	}
	r.deltaLogs = deltaLogs
	return r, nil
}

// initFilters initializes the filter chain for L0 reader
func (r *l0Reader) initFilters(tsStart, tsEnd uint64) {
	// Add time range filter if specified
	if tsStart != 0 || tsEnd != math.MaxUint64 {
		r.filters = append(r.filters, FilterDeleteWithTimeRange(tsStart, tsEnd))
	}
}

// filter applies all filters to a delete log record
func (r *l0Reader) filter(dl *storage.DeleteLog) bool {
	for _, f := range r.filters {
		if !f(dl) {
			return false
		}
	}
	return true
}

func (r *l0Reader) Read() (*storage.DeleteData, error) {
	deleteData := storage.NewDeleteData(nil, nil)
	readInternal := func(path string, opts []storage.RwOption) (*storage.DeleteData, error) {
		tempData := storage.NewDeleteData(nil, nil)
		reader, err := storage.NewDeltalogReader(r.pkField.DataType, []string{path}, opts...)
		if err != nil {
			return nil, err
		}
		defer reader.Close()

		for {
			rec, err := reader.Next()
			if err != nil {
				if err == io.EOF {
					break
				}
				log.Error("error on importing L0 segment, fail to read deltalogs", zap.Error(err))
				return nil, err
			}

			for i := 0; i < rec.Len(); i++ {
				var pk storage.PrimaryKey
				switch r.pkField.DataType {
				case schemapb.DataType_Int64:
					pk = storage.NewInt64PrimaryKey(rec.Column(0).(*array.Int64).Value(i))
				case schemapb.DataType_VarChar:
					pk = storage.NewVarCharPrimaryKey(rec.Column(0).(*array.String).Value(i))
				}
				ts := typeutil.Timestamp(rec.Column(1).(*array.Int64).Value(i))
				dl := storage.NewDeleteLog(pk, ts)

				// Apply filters
				if !r.filter(dl) {
					continue
				}

				tempData.Append(pk, ts)
			}
		}
		return tempData, nil
	}

	for {
		if r.readIdx == len(r.deltaLogs) {
			if deleteData.RowCount != 0 {
				return deleteData, nil
			}
			return nil, io.EOF
		}
		path := r.deltaLogs[r.readIdx]

		v1opts := []storage.RwOption{
			storage.WithVersion(storage.StorageV1),
			storage.WithDownloader(func(ctx context.Context, paths []string) ([][]byte, error) {
				return r.cm.MultiRead(ctx, paths)
			}),
		}
		v2opts := []storage.RwOption{
			storage.WithVersion(storage.StorageV2),
			storage.WithStorageConfig(r.storageConfig),
		}

		// try v1 first
		tempData, errv1 := readInternal(path, v1opts)
		if errv1 != nil {
			// try v2 if v1 failed
			tempData, errv2 := readInternal(path, v2opts)
			if errv2 != nil {
				// return both the error from v1 and v2
				return nil, merr.WrapErrImportFailed(fmt.Sprintf("failed to read deltalogs from v1 and v2: %v, %v", errv1, errv2))
			}
			// Merge v2 results into deleteData
			for i := int64(0); i < tempData.RowCount; i++ {
				deleteData.Append(tempData.Pks[i], tempData.Tss[i])
			}
		} else {
			// Merge v1 results into deleteData
			for i := int64(0); i < tempData.RowCount; i++ {
				deleteData.Append(tempData.Pks[i], tempData.Tss[i])
			}
		}
		r.readIdx++
		if deleteData.Size() >= int64(r.bufferSize) {
			break
		}
	}
	return deleteData, nil
}
