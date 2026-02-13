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
	"io"

	"github.com/apache/arrow/go/v17/arrow/array"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// readFromReader reads all records from a deltalog reader and extracts pk/ts pairs.
func readFromReader(reader storage.RecordReader, pkType schemapb.DataType) ([]storage.PrimaryKey, []typeutil.Timestamp, error) {
	var pks []storage.PrimaryKey
	var tss []typeutil.Timestamp

	for {
		rec, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, err
		}

		for i := 0; i < rec.Len(); i++ {
			var pk storage.PrimaryKey
			if pkType == schemapb.DataType_Int64 {
				pk = storage.NewInt64PrimaryKey(rec.Column(0).(*array.Int64).Value(i))
			} else {
				pk = storage.NewVarCharPrimaryKey(rec.Column(0).(*array.String).Value(i))
			}
			ts := typeutil.Timestamp(rec.Column(1).(*array.Int64).Value(i))
			pks = append(pks, pk)
			tss = append(tss, ts)
		}
	}
	return pks, tss, nil
}

// readFromSegment reads deltalogs from a segment, auto-detecting V1/V2 format.
func readFromSegment(
	ctx context.Context,
	pkType schemapb.DataType,
	segment *datapb.CompactionSegmentBinlogs,
	option ...storage.RwOption,
) ([]storage.PrimaryKey, []typeutil.Timestamp, error) {
	if segment.GetManifest() != "" {
		return readDeltalogsV2(ctx, pkType, segment.GetManifest(), option...)
	}
	return readDeltalogsV1(ctx, pkType, segment.GetDeltalogs(), option...)
}

// readDeltalogsV1 reads deltalogs from V1 format (individual binlog files).
func readDeltalogsV1(
	ctx context.Context,
	pkType schemapb.DataType,
	deltalogs []*datapb.FieldBinlog,
	option ...storage.RwOption,
) ([]storage.PrimaryKey, []typeutil.Timestamp, error) {
	var allPks []storage.PrimaryKey
	var allTss []typeutil.Timestamp

	for _, deltalog := range deltalogs {
		for _, binlog := range deltalog.Binlogs {
			reader, err := storage.NewDeltalogReader(pkType, []string{binlog.GetLogPath()}, option...)
			if err != nil {
				return nil, nil, err
			}
			pks, tss, err := readFromReader(reader, pkType)
			reader.Close()
			if err != nil {
				return nil, nil, err
			}
			allPks = append(allPks, pks...)
			allTss = append(allTss, tss...)
		}
	}

	log.Ctx(ctx).Info("read V1 deltalogs", zap.Int("entries", len(allPks)))
	return allPks, allTss, nil
}

// readDeltalogsV2 reads deltalogs from V2 format (manifest-based).
func readDeltalogsV2(
	ctx context.Context,
	pkType schemapb.DataType,
	manifestPath string,
	option ...storage.RwOption,
) ([]storage.PrimaryKey, []typeutil.Timestamp, error) {
	reader, err := storage.NewDeltalogReaderFromManifest(pkType, manifestPath, option...)
	if err != nil {
		return nil, nil, err
	}
	defer reader.Close()

	pks, tss, err := readFromReader(reader, pkType)
	if err != nil {
		return nil, nil, err
	}

	log.Ctx(ctx).Info("read V2 deltalogs from manifest", zap.Int("entries", len(pks)))
	return pks, tss, nil
}

// ComposeDeleteFromDeltalogs reads deltalogs from segment and returns a map of pk to timestamp.
// Auto-detects V1/V2 based on segment.GetManifest().
func ComposeDeleteFromDeltalogs(
	ctx context.Context,
	pkType schemapb.DataType,
	segment *datapb.CompactionSegmentBinlogs,
	option ...storage.RwOption,
) (map[any]typeutil.Timestamp, error) {
	pks, tss, err := readFromSegment(ctx, pkType, segment, option...)
	if err != nil {
		return nil, err
	}
	return buildPk2TsMap(pks, tss), nil
}

// ComposeDeleteDataFromDeltalogs reads deltalogs from segment and returns DeleteData.
// Auto-detects V1/V2 based on segment.GetManifest().
func ComposeDeleteDataFromDeltalogs(
	ctx context.Context,
	pkType schemapb.DataType,
	segment *datapb.CompactionSegmentBinlogs,
	option ...storage.RwOption,
) (*storage.DeleteData, error) {
	pks, tss, err := readFromSegment(ctx, pkType, segment, option...)
	if err != nil {
		return nil, err
	}
	return storage.NewDeleteData(pks, tss), nil
}

// ComposeDeleteDataFromSegments reads deltalogs from multiple segments and returns aggregated DeleteData.
// Used for L0 compaction where deltalogs are collected from multiple L0 segments.
func ComposeDeleteDataFromSegments(
	ctx context.Context,
	pkType schemapb.DataType,
	segments []*datapb.CompactionSegmentBinlogs,
	option ...storage.RwOption,
) (*storage.DeleteData, error) {
	var allPks []storage.PrimaryKey
	var allTss []typeutil.Timestamp

	for _, segment := range segments {
		pks, tss, err := readFromSegment(ctx, pkType, segment, option...)
		if err != nil {
			return nil, err
		}
		allPks = append(allPks, pks...)
		allTss = append(allTss, tss...)
	}

	return storage.NewDeleteData(allPks, allTss), nil
}

// ComposeDeleteFromDeltalogsV1 reads V1 deltalogs and returns a map of pk to timestamp.
// For legacy code without segment info.
func ComposeDeleteFromDeltalogsV1(
	ctx context.Context,
	pkType schemapb.DataType,
	deltalogs []*datapb.FieldBinlog,
	option ...storage.RwOption,
) (map[any]typeutil.Timestamp, error) {
	pks, tss, err := readDeltalogsV1(ctx, pkType, deltalogs, option...)
	if err != nil {
		return nil, err
	}
	return buildPk2TsMap(pks, tss), nil
}

// buildPk2TsMap builds a map from pk value to timestamp.
// For duplicate PKs, keeps the latest timestamp.
func buildPk2TsMap(pks []storage.PrimaryKey, tss []typeutil.Timestamp) map[any]typeutil.Timestamp {
	pk2Ts := make(map[any]typeutil.Timestamp, len(pks))
	for i, pk := range pks {
		key := pk.GetValue()
		if existing, ok := pk2Ts[key]; ok && existing > tss[i] {
			continue
		}
		pk2Ts[key] = tss[i]
	}
	return pk2Ts
}
