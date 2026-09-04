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

package function

import (
	"context"
	"io"
	"path"
	"strconv"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	metabinlog "github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// SegmentTextTermSource describes a newly materialized segment whose final
// visible rows should be analyzed into one complete FST generation.
type SegmentTextTermSource struct {
	CollectionID   int64
	PartitionID    int64
	SegmentID      int64
	StorageVersion int64
	InsertLogs     []*datapb.FieldBinlog
	ManifestPath   string
	StorageConfig  *indexpb.StorageConfig
	Downloader     func(context.Context, []string) ([][]byte, error)
}

// SegmentTextTerms binds the vocabulary produced from a segment row set to the
// maximum timestamp observed in that same row set. Writers persist the
// timestamp as the Text Log V2 coverage fence.
type SegmentTextTerms struct {
	Fields            map[int64][][]byte
	CoverageTimestamp uint64
}

// CollectSegmentTextTerms scans the final segment row set. StorageV3 uses the
// LOB-aware SegmentReader so TEXT input columns are returned as UTF-8.
func CollectSegmentTextTerms(
	ctx context.Context,
	schema *schemapb.CollectionSchema,
	source SegmentTextTermSource,
) (SegmentTextTerms, error) {
	collector, err := NewTextTermCollector(schema)
	if err != nil {
		return SegmentTextTerms{}, err
	}
	defer collector.Close()
	if !collector.Enabled() {
		return SegmentTextTerms{}, nil
	}

	var coverageTimestamp uint64
	switch source.StorageVersion {
	case storage.StorageV1, storage.StorageV2:
		coverageTimestamp, err = collectSegmentBinlogTerms(ctx, collector, schema, source)
	case storage.StorageV3:
		coverageTimestamp, err = collectSegmentManifestTerms(ctx, collector, schema, source)
	default:
		err = merr.WrapErrServiceInternalMsg("unsupported text term storage version %d", source.StorageVersion)
	}
	if err != nil {
		return SegmentTextTerms{}, err
	}
	return SegmentTextTerms{
		Fields:            collector.Drain(),
		CoverageTimestamp: coverageTimestamp,
	}, nil
}

func collectSegmentBinlogTerms(
	ctx context.Context,
	collector *TextTermCollector,
	schema *schemapb.CollectionSchema,
	source SegmentTextTermSource,
) (uint64, error) {
	insertLogs := make([]*datapb.FieldBinlog, len(source.InsertLogs))
	for i, fieldLog := range source.InsertLogs {
		insertLogs[i] = typeutil.Clone(fieldLog)
	}
	if err := metabinlog.DecompressBinLogWithRootPath(source.StorageConfig.GetRootPath(),
		storage.InsertBinlog, source.CollectionID, source.PartitionID, source.SegmentID, insertLogs); err != nil {
		return 0, err
	}
	reader, err := storage.NewBinlogRecordReader(ctx, insertLogs, schema,
		storage.WithCollectionID(source.CollectionID),
		storage.WithVersion(source.StorageVersion),
		storage.WithDownloader(source.Downloader),
		storage.WithStorageConfig(source.StorageConfig),
	)
	if err != nil {
		return 0, err
	}
	defer reader.Close()
	var coverageTimestamp uint64
	for {
		record, err := reader.Next()
		if err == io.EOF {
			return coverageTimestamp, nil
		}
		if err != nil {
			return 0, merr.Wrap(err, "read segment output for text term FST")
		}
		if record == nil {
			return coverageTimestamp, nil
		}
		coverageTimestamp, err = updateTextTermCoverage(record.Column(common.TimeStampField), coverageTimestamp)
		if err != nil {
			return 0, err
		}
		if err := collector.CollectRecord(record); err != nil {
			return 0, err
		}
	}
}

func collectSegmentManifestTerms(
	ctx context.Context,
	collector *TextTermCollector,
	schema *schemapb.CollectionSchema,
	source SegmentTextTermSource,
) (uint64, error) {
	arrowSchema, err := storage.ConvertToArrowSchema(schema, true)
	if err != nil {
		return 0, err
	}
	inputFieldIDs := typeutil.NewSet(collector.InputFieldIDs()...)
	neededColumns := make([]string, 0, len(inputFieldIDs)+1)
	neededColumns = append(neededColumns, strconv.FormatInt(common.TimeStampField, 10))
	textColumns := make([]packed.TextColumnConfig, 0)
	partitionBasePath := path.Join(source.StorageConfig.GetRootPath(), common.SegmentInsertLogPath,
		metautil.JoinIDPath(source.CollectionID, source.PartitionID))
	for _, field := range typeutil.GetAllFieldSchemas(schema) {
		if !inputFieldIDs.Contain(field.GetFieldID()) {
			continue
		}
		neededColumns = append(neededColumns, strconv.FormatInt(field.GetFieldID(), 10))
		if field.GetDataType() == schemapb.DataType_Text {
			textColumns = append(textColumns, packed.TextColumnConfig{
				FieldID:     field.GetFieldID(),
				LobBasePath: path.Join(partitionBasePath, "lobs", strconv.FormatInt(field.GetFieldID(), 10)),
			})
		}
	}
	reader, err := packed.NewFFISegmentReader(source.ManifestPath, arrowSchema, neededColumns,
		textColumns, packed.DefaultReadBufferSize, source.StorageConfig)
	if err != nil {
		return 0, err
	}
	defer reader.Close()

	fieldColumns := make(map[int64]int, reader.Schema().NumFields())
	for i, field := range reader.Schema().Fields() {
		fieldID, err := strconv.ParseInt(field.Name, 10, 64)
		if err != nil {
			return 0, merr.WrapErrServiceInternalMsg("LOB-aware segment reader returned unexpected field name %q", field.Name)
		}
		fieldColumns[fieldID] = i
	}
	timestampColumn, ok := fieldColumns[common.TimeStampField]
	if !ok {
		return 0, merr.WrapErrDataIntegrityMsg("segment text term source is missing the timestamp column")
	}
	var coverageTimestamp uint64
	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		record, err := reader.ReadNext()
		if err == io.EOF {
			return coverageTimestamp, nil
		}
		if err != nil {
			return 0, err
		}
		if record == nil {
			return coverageTimestamp, nil
		}
		coverageTimestamp, err = updateTextTermCoverage(record.Column(timestampColumn), coverageTimestamp)
		if err == nil {
			err = collector.CollectArrowRecord(record, fieldColumns)
		}
		if err != nil {
			return 0, err
		}
	}
}

func updateTextTermCoverage(column arrow.Array, current uint64) (uint64, error) {
	timestamps, ok := column.(*array.Int64)
	if !ok {
		return 0, merr.WrapErrDataIntegrityMsg(
			"segment text term timestamp column has unexpected type %T", column)
	}
	for i := 0; i < timestamps.Len(); i++ {
		if timestamps.IsNull(i) {
			return 0, merr.WrapErrDataIntegrityMsg(
				"segment text term timestamp column contains a null value at row %d", i)
		}
		timestamp := uint64(timestamps.Value(i))
		if timestamp > current {
			current = timestamp
		}
	}
	return current, nil
}
