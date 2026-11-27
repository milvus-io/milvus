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
	"slices"

	"github.com/apache/arrow/go/v17/arrow"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/storage/deltacodec"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/messageutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// MutationDelta holds rows for one schema version and partition scope.
type MutationDelta struct {
	Layout deltacodec.Layout
	Rows   []deltacodec.Row
}

const deltaWriteBufferSize = 32 << 20

func WriteMutationDelta(ctx context.Context, path string, d MutationDelta, cfg *indexpb.StorageConfig, plugin *indexcgopb.StoragePluginContext) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if cfg == nil {
		return 0, merr.WrapErrServiceInternalMsg("storage config is required for a typed delta")
	}
	if len(d.Rows) == 0 {
		return 0, merr.WrapErrServiceInternalMsg("empty delta file")
	}
	if err := d.Layout.ValidateIdentity(); err != nil {
		return 0, err
	}
	used := make(map[int64]bool)
	for _, r := range d.Rows {
		for _, a := range r.Assignments {
			used[a.GetFieldId()] = true
		}
	}
	d.Layout.Fields = slices.DeleteFunc(slices.Clone(d.Layout.Fields), func(f deltacodec.Field) bool { return !used[f.ID] })
	schema, err := d.Layout.Schema()
	if err != nil {
		return 0, err
	}
	metadata := arrow.MetadataFrom(map[string]string{"delta_format_version": "1"})
	schema = arrow.NewSchema(schema.Fields(), &metadata)
	columns := make([]int, schema.NumFields())
	for i := range columns {
		columns[i] = i
	}
	w, err := packed.NewPackedWriter([]string{path}, schema, deltaWriteBufferSize, packed.DefaultMultiPartUploadSize, []storagecommon.ColumnGroup{{Columns: columns}}, cfg, plugin)
	if err != nil {
		return 0, err
	}
	defer w.Close()
	for begin := 0; begin < len(d.Rows); {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		end := deltaBatchEnd(d.Rows, begin, len(d.Layout.Fields), deltaWriteBufferSize/4)
		rec, err := deltacodec.Encode(d.Layout, d.Rows[begin:end])
		if err != nil {
			return 0, err
		}
		err = w.WriteRecordBatch(rec)
		rec.Release()
		if err != nil {
			return 0, err
		}
		begin = end
	}
	sizes, err := w.CloseAndTell(1)
	if err != nil {
		return 0, err
	}
	return sizes[0], nil
}

// deltaBatchEnd accounts for shared variable-length payloads plus dense Arrow
// slots for sparse fields. One oversized WAL fragment stays indivisible.
func deltaBatchEnd(rows []deltacodec.Row, begin, fieldCount, limit int) int {
	end := begin
	var size int
	for end < len(rows) && end-begin < 256 {
		r := rows[end]
		estimate := 32 + fieldCount*12 + proto.Size(r.PrimaryKeys) + 8*messageutil.PrimaryKeyCount(r.PrimaryKeys)
		for _, a := range r.Assignments {
			estimate += proto.Size(a)
		}
		if end > begin && size+estimate > limit {
			break
		}
		size += estimate
		end++
	}
	return end
}
