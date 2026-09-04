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

package segments

import (
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/pathutil"
	"github.com/milvus-io/milvus/internal/util/textindex"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type textTermFieldLoadMetadata struct {
	fieldLog           *datapb.FieldBinlog
	aggregateSize      bool
	expectedLogSize    int64
	expectedMemorySize int64
}

func (loader *segmentLoader) buildLoadedTextTermDictionary(
	ctx context.Context,
	schema *schemapb.CollectionSchema,
	loadInfo *querypb.SegmentLoadInfo,
) (_ *loadedTextTermDictionary, err error) {
	enabled := fuzzyBM25FieldIDs(schema)
	if len(enabled) == 0 {
		return nil, nil
	}

	fieldMetadata, err := validateTextTermLoadMetadata(enabled, loadInfo)
	if err != nil {
		return nil, err
	}

	result := &loadedTextTermDictionary{
		readers: make(map[int64][]*textindex.FstReader, len(enabled)),
	}
	defer func() {
		if err != nil {
			result.close()
		}
	}()

	memoryMapped := paramtable.Get().QueryNodeCfg.MmapTextLogV2.GetAsBool()
	if memoryMapped {
		basePath := pathutil.GetPath(pathutil.TextLogV2Path, paramtable.GetNodeID())
		if err = os.MkdirAll(basePath, 0o700); err != nil {
			return nil, merr.Wrap(storage.ToMilvusIoError(basePath, err), "create text-log-v2 cache root")
		}
		result.cacheDir, err = os.MkdirTemp(basePath, fmt.Sprintf("segment-%d-", loadInfo.GetSegmentID()))
		if err != nil {
			return nil, merr.Wrap(storage.ToMilvusIoError(basePath, err), "create text-log-v2 segment cache")
		}
	}

	for fieldID, metadata := range fieldMetadata {
		fieldLog := metadata.fieldLog
		var actualSize int64
		for index, binlog := range fieldLog.GetBinlogs() {
			var reader *textindex.FstReader
			if memoryMapped {
				localPath := filepath.Join(result.cacheDir, fmt.Sprintf("%d-%d.fst", fieldID, index))
				if err = loader.downloadTextTermFile(ctx, binlog.GetLogPath(), localPath); err != nil {
					return nil, err
				}
				reader, err = textindex.LoadTextFstFile(localPath, true)
			} else {
				var data []byte
				data, err = loader.cm.Read(ctx, binlog.GetLogPath())
				if err != nil {
					return nil, merr.Wrap(storage.ToMilvusIoError(binlog.GetLogPath(), err), "read text-log-v2 object")
				}
				reader, err = textindex.LoadTextFstBytes(data)
			}
			if err != nil {
				if reader != nil {
					reader.Close()
				}
				return nil, merr.Wrapf(err,
					"load text-log-v2 FST for segment %d field %d path %s",
					loadInfo.GetSegmentID(), fieldID, binlog.GetLogPath())
			}
			if memoryMapped != reader.IsMemoryMapped() {
				reader.Close()
				return nil, merr.WrapErrServiceInternalMsg(
					"text-log-v2 FST load mode mismatch for segment %d field %d",
					loadInfo.GetSegmentID(), fieldID)
			}
			if !metadata.aggregateSize &&
				(reader.DataSize() != binlog.GetLogSize() || reader.DataSize() != binlog.GetMemorySize()) {
				reader.Close()
				return nil, merr.WrapErrDataIntegrityMsg(
					"text-log-v2 FST size mismatch for segment %d field %d: log=%d memory=%d actual=%d",
					loadInfo.GetSegmentID(), fieldID, binlog.GetLogSize(), binlog.GetMemorySize(), reader.DataSize())
			}
			if binlog.GetEntriesNum() > 0 && reader.TermCount() != binlog.GetEntriesNum() {
				reader.Close()
				return nil, merr.WrapErrDataIntegrityMsg(
					"text-log-v2 FST term count mismatch for segment %d field %d: metadata=%d actual=%d",
					loadInfo.GetSegmentID(), fieldID, binlog.GetEntriesNum(), reader.TermCount())
			}
			if reader.DataSize() > math.MaxInt64-actualSize {
				reader.Close()
				return nil, merr.WrapErrDataIntegrityMsg(
					"text-log-v2 FST size overflows for segment %d field %d",
					loadInfo.GetSegmentID(), fieldID)
			}
			actualSize += reader.DataSize()
			result.readers[fieldID] = append(result.readers[fieldID], reader)
			if !memoryMapped {
				result.heapBytes += reader.DataSize()
			}
		}
		if metadata.aggregateSize &&
			(actualSize != metadata.expectedLogSize || actualSize != metadata.expectedMemorySize) {
			return nil, merr.WrapErrDataIntegrityMsg(
				"text-log-v2 FST aggregate size mismatch for segment %d field %d: log=%d memory=%d actual=%d",
				loadInfo.GetSegmentID(), fieldID, metadata.expectedLogSize, metadata.expectedMemorySize, actualSize)
		}
	}
	return result, nil
}

func validateTextTermLoadMetadata(
	enabled map[int64]struct{},
	loadInfo *querypb.SegmentLoadInfo,
) (map[int64]*textTermFieldLoadMetadata, error) {
	fieldLogs := make(map[int64]*datapb.FieldBinlog)
	for _, fieldLog := range loadInfo.GetTextLogV2() {
		if _, ok := enabled[fieldLog.GetFieldID()]; !ok {
			continue
		}
		current := fieldLogs[fieldLog.GetFieldID()]
		if current == nil {
			current = &datapb.FieldBinlog{
				FieldID: fieldLog.GetFieldID(),
				Format:  fieldLog.GetFormat(),
			}
			fieldLogs[fieldLog.GetFieldID()] = current
		} else if current.GetFormat() != fieldLog.GetFormat() {
			return nil, merr.WrapErrDataIntegrityMsg(
				"segment %d field %d has mixed text-log-v2 formats",
				loadInfo.GetSegmentID(), fieldLog.GetFieldID())
		}
		current.Binlogs = append(current.Binlogs, fieldLog.GetBinlogs()...)
	}

	dataCoverage := maxBinlogTimestamp(loadInfo.GetBinlogPaths())
	result := make(map[int64]*textTermFieldLoadMetadata, len(enabled))
	for fieldID := range enabled {
		fieldLog := fieldLogs[fieldID]
		if fieldLog == nil || len(fieldLog.GetBinlogs()) == 0 {
			return nil, merr.WrapErrDataIntegrityMsg(
				"segment %d is missing text-log-v2 coverage for fuzzy BM25 field %d",
				loadInfo.GetSegmentID(), fieldID)
		}
		if fieldLog.GetFormat() != textindex.MilvusTextFstFormat {
			return nil, merr.WrapErrDataIntegrityMsg(
				"segment %d field %d has unsupported text-log-v2 format %q",
				loadInfo.GetSegmentID(), fieldID, fieldLog.GetFormat())
		}
		for _, binlog := range fieldLog.GetBinlogs() {
			if binlog == nil || binlog.GetLogPath() == "" {
				return nil, merr.WrapErrDataIntegrityMsg(
					"segment %d field %d text-log-v2 path is missing",
					loadInfo.GetSegmentID(), fieldID)
			}
			if binlog.GetTimestampTo() == 0 {
				return nil, merr.WrapErrDataIntegrityMsg(
					"segment %d field %d text-log-v2 fragment coverage is missing for path %s",
					loadInfo.GetSegmentID(), fieldID, binlog.GetLogPath())
			}
		}
		coverage := maxBinlogTimestamp([]*datapb.FieldBinlog{fieldLog})
		if dataCoverage != 0 && coverage < dataCoverage {
			return nil, merr.WrapErrDataIntegrityMsg(
				"segment %d field %d text-log-v2 coverage %d is behind data coverage %d",
				loadInfo.GetSegmentID(), fieldID, coverage, dataCoverage)
		}
		aggregateSize := hasAggregateTextTermSize(fieldLog.GetBinlogs())
		var expectedLogSize, expectedMemorySize int64
		for index, binlog := range fieldLog.GetBinlogs() {
			if aggregateSize && index > 0 {
				continue
			}
			if binlog.GetLogSize() <= 0 || binlog.GetMemorySize() <= 0 {
				return nil, merr.WrapErrDataIntegrityMsg(
					"segment %d field %d text-log-v2 size metadata is missing for path %s",
					loadInfo.GetSegmentID(), fieldID, binlog.GetLogPath())
			}
			if binlog.GetLogSize() > math.MaxInt64-expectedLogSize ||
				binlog.GetMemorySize() > math.MaxInt64-expectedMemorySize {
				return nil, merr.WrapErrDataIntegrityMsg(
					"segment %d field %d text-log-v2 size metadata overflows",
					loadInfo.GetSegmentID(), fieldID)
			}
			expectedLogSize += binlog.GetLogSize()
			expectedMemorySize += binlog.GetMemorySize()
		}
		result[fieldID] = &textTermFieldLoadMetadata{
			fieldLog:           fieldLog,
			aggregateSize:      aggregateSize,
			expectedLogSize:    expectedLogSize,
			expectedMemorySize: expectedMemorySize,
		}
	}
	return result, nil
}

func (loader *segmentLoader) downloadTextTermFile(ctx context.Context, remotePath, localPath string) error {
	reader, err := loader.cm.Reader(ctx, remotePath)
	if err != nil {
		return merr.Wrap(storage.ToMilvusIoError(remotePath, err), "open text-log-v2 object")
	}
	defer reader.Close()

	file, err := os.OpenFile(localPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return merr.Wrap(storage.ToMilvusIoError(localPath, err), "create local text-log-v2 file")
	}
	remove := true
	defer func() {
		_ = file.Close()
		if remove {
			_ = os.Remove(localPath)
		}
	}()
	if _, err = io.Copy(file, reader); err != nil {
		return merr.Wrap(storage.ToMilvusIoError(remotePath, err), "download text-log-v2 file")
	}
	if err = file.Sync(); err != nil {
		return merr.Wrap(storage.ToMilvusIoError(localPath, err), "sync local text-log-v2 file")
	}
	if err = file.Close(); err != nil {
		return merr.Wrap(storage.ToMilvusIoError(localPath, err), "close local text-log-v2 file")
	}
	remove = false
	return nil
}

func maxBinlogTimestamp(fieldLogs []*datapb.FieldBinlog) uint64 {
	var result uint64
	for _, fieldLog := range fieldLogs {
		for _, binlog := range fieldLog.GetBinlogs() {
			if binlog.GetTimestampTo() > result {
				result = binlog.GetTimestampTo()
			}
		}
	}
	return result
}

func hasAggregateTextTermSize(binlogs []*datapb.Binlog) bool {
	if len(binlogs) < 2 || binlogs[0].GetLogSize() <= 0 || binlogs[0].GetMemorySize() <= 0 {
		return false
	}
	for _, binlog := range binlogs[1:] {
		if binlog.GetLogSize() != 0 || binlog.GetMemorySize() != 0 {
			return false
		}
	}
	return true
}
