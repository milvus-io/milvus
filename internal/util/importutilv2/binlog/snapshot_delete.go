// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package binlog

import (
	"context"
	"io"
	"strings"

	"github.com/apache/arrow/go/v17/arrow/array"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// readSnapshotDeletes shares the existing delta decoders but merges directly
// into the one budgeted map for segment-local and L0 deletes. No per-file copy
// or unbounded intermediate PK map is retained.
func (r *reader) readSnapshotDeletes(paths []string, start, end uint64, legacy bool) (map[any]typeutil.Timestamp, error) {
	if r.deleteData == nil {
		r.deleteData = make(map[any]typeutil.Timestamp)
	}
	pkField, err := typeutil.GetPrimaryFieldSchema(r.schema)
	if err != nil {
		return nil, err
	}
	for _, path := range paths {
		if err := r.ctx.Err(); err != nil {
			return nil, err
		}
		version := storage.StorageV3
		if legacy {
			version = storage.StorageV1
		}
		consumed, err := r.mergeSnapshotDeleteFile(path, pkField.GetDataType(), start, end, version)
		if err != nil && legacy && !consumed && r.ctx.Err() == nil {
			// Preserve legacy V1/V2 probing only before a record was accepted.
			// Once decoding began, a terminal read/merge error must fail the job;
			// retrying another decoder could retain deletes from a corrupt prefix.
			_, err = r.mergeSnapshotDeleteFile(path, pkField.GetDataType(), start, end, storage.StorageV2)
		}
		if err != nil {
			return nil, err
		}
	}
	return r.deleteData, nil
}

func (r *reader) mergeSnapshotDeleteFile(path string, pkType schemapb.DataType, start, end uint64, version int64) (bool, error) {
	options := []storage.RwOption{
		storage.WithVersion(version), storage.WithStorageConfig(r.storageConfig),
		storage.WithDownloader(func(ctx context.Context, paths []string) ([][]byte, error) {
			return r.multiReadWithRetry(ctx, paths)
		}),
	}
	delta, err := storage.NewDeltalogReader(r.ctx, pkType, []string{path}, options...)
	if err != nil {
		return false, err
	}
	defer delta.Close()
	consumed := false
	for {
		if err := r.ctx.Err(); err != nil {
			return consumed, err
		}
		record, err := delta.Next()
		if err == io.EOF {
			return consumed, nil
		}
		if err != nil {
			return consumed, err
		}
		consumed = true
		// Arrow buffers belong to delta until Next/Close. Every retained
		// string key must be owned; timestamps and integer keys are values.
		timestamps, ok := record.Column(common.TimeStampField).(*array.Int64)
		if !ok || timestamps.Len() != record.Len() || timestamps.NullN() != 0 {
			return true, merr.WrapErrDataIntegrityMsg("invalid snapshot deltalog timestamp column")
		}
		ints, intPK := record.Column(0).(*array.Int64)
		strs, strPK := record.Column(0).(*array.String)
		if (pkType != schemapb.DataType_Int64 || !intPK || ints.Len() != record.Len() || ints.NullN() != 0) &&
			(pkType != schemapb.DataType_VarChar || !strPK || strs.Len() != record.Len() || strs.NullN() != 0) {
			return true, merr.WrapErrDataIntegrityMsg("invalid snapshot deltalog primary key column")
		}
		for i := 0; i < record.Len(); i++ {
			if err := r.ctx.Err(); err != nil {
				return true, err
			}
			ts := uint64(timestamps.Value(i))
			if ts < start || ts > end {
				continue
			}
			var pk any
			if pkType == schemapb.DataType_Int64 {
				pk = ints.Value(i)
			} else {
				pk = strs.Value(i)
			}
			if err := r.mergeSnapshotDelete(pk, ts); err != nil {
				return true, err
			}
		}
	}
}

func (r *reader) mergeSnapshotDelete(pk any, ts uint64) error {
	if previous, ok := r.deleteData[pk]; ok {
		if ts > previous {
			// Go map assignment can replace an equal string key as well as
			// its value. Do not replace our owned key with an Arrow view.
			if str, ok := pk.(string); ok {
				pk = strings.Clone(str)
			}
			r.deleteData[pk] = ts
		}
		return nil
	}
	// Conservative per-entry accounting includes key/interface, timestamp,
	// bucket slack and map growth. This is an accounting bound, not an RSS cap
	// for the Arrow reader or the Go allocator.
	charge := int64(128)
	if str, ok := pk.(string); ok {
		charge += int64(len(str))
	}
	if charge > r.deleteBudget-r.deleteBytes {
		return merr.Wrapf(merr.ErrServiceResourceInsufficient, "snapshot delete map exceeds its %d byte budget", r.deleteBudget)
	}
	if str, ok := pk.(string); ok {
		pk = strings.Clone(str)
	}
	r.deleteBytes += charge
	r.deleteData[pk] = ts
	return nil
}
