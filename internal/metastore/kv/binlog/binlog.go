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
	"fmt"
	"strconv"
	"strings"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metautil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

func CompressSaveBinlogPaths(req *datapb.SaveBinlogPathsRequest) error {
	err := CompressFieldBinlogs(req.GetDeltalogs())
	if err != nil {
		return err
	}
	err = CompressFieldBinlogs(req.GetField2BinlogPaths())
	if err != nil {
		return err
	}
	err = CompressFieldBinlogs(req.GetField2StatslogPaths())
	if err != nil {
		return err
	}
	err = CompressFieldBinlogs(req.GetField2Bm25LogPaths())
	if err != nil {
		return err
	}
	return nil
}

func CompressCompactionBinlogs(binlogs []*datapb.CompactionSegment) error {
	for _, binlog := range binlogs {
		err := CompressFieldBinlogs(binlog.GetInsertLogs())
		if err != nil {
			return err
		}
		err = CompressFieldBinlogs(binlog.GetDeltalogs())
		if err != nil {
			return err
		}
		err = CompressFieldBinlogs(binlog.GetField2StatslogPaths())
		if err != nil {
			return err
		}
		err = CompressFieldBinlogs(binlog.GetBm25Logs())
		if err != nil {
			return err
		}
	}
	return nil
}

func CompressBinLogs(binlogs ...[]*datapb.FieldBinlog) error {
	for _, l := range binlogs {
		err := CompressFieldBinlogs(l)
		if err != nil {
			return err
		}
	}
	return nil
}

func CompressFieldBinlogs(fieldBinlogs []*datapb.FieldBinlog) error {
	for _, fieldBinlog := range fieldBinlogs {
		for _, binlog := range fieldBinlog.Binlogs {
			logPath := binlog.GetLogPath()
			if len(logPath) != 0 {
				logID, err := GetLogIDFromBingLogPath(logPath)
				if err != nil {
					return err
				}
				binlog.LogID = logID
				binlog.LogPath = ""
			}
		}
	}
	return nil
}

func DecompressMultiBinLogs(infos []*datapb.SegmentInfo) error {
	for _, info := range infos {
		err := DecompressBinLogs(info)
		if err != nil {
			return err
		}
	}
	return nil
}

func DecompressCompactionBinlogs(binlogs []*datapb.CompactionSegmentBinlogs) error {
	for _, binlog := range binlogs {
		collectionID, partitionID, segmentID := binlog.GetCollectionID(), binlog.GetPartitionID(), binlog.GetSegmentID()
		err := DecompressBinLog(storage.InsertBinlog, collectionID, partitionID, segmentID, binlog.GetFieldBinlogs())
		if err != nil {
			return err
		}
		err = DecompressBinLog(storage.DeleteBinlog, collectionID, partitionID, segmentID, binlog.GetDeltalogs())
		if err != nil {
			return err
		}
		err = DecompressBinLog(storage.StatsBinlog, collectionID, partitionID, segmentID, binlog.GetField2StatslogPaths())
		if err != nil {
			return err
		}
	}
	return nil
}

func DecompressBinLogs(s *datapb.SegmentInfo) error {
	collectionID, partitionID, segmentID := s.GetCollectionID(), s.GetPartitionID(), s.ID
	err := DecompressBinLog(storage.InsertBinlog, collectionID, partitionID, segmentID, s.GetBinlogs())
	if err != nil {
		return err
	}
	err = DecompressBinLog(storage.DeleteBinlog, collectionID, partitionID, segmentID, s.GetDeltalogs())
	if err != nil {
		return err
	}
	err = DecompressBinLog(storage.StatsBinlog, collectionID, partitionID, segmentID, s.GetStatslogs())
	if err != nil {
		return err
	}

	err = DecompressBinLog(storage.BM25Binlog, collectionID, partitionID, segmentID, s.GetBm25Statslogs())
	if err != nil {
		return err
	}
	return nil
}

func DecompressBinLog(binlogType storage.BinlogType, collectionID, partitionID,
	segmentID typeutil.UniqueID, fieldBinlogs []*datapb.FieldBinlog,
) error {
	for _, fieldBinlog := range fieldBinlogs {
		for _, binlog := range fieldBinlog.Binlogs {
			if binlog.GetLogPath() == "" {
				path, err := BuildLogPath(binlogType, collectionID, partitionID,
					segmentID, fieldBinlog.GetFieldID(), binlog.GetLogID())
				if err != nil {
					return err
				}
				binlog.LogPath = path
			}
		}
	}
	return nil
}

func DecompressBinLogWithRootPath(rootPath string, binlogType storage.BinlogType, collectionID, partitionID,
	segmentID typeutil.UniqueID, fieldBinlogs []*datapb.FieldBinlog,
) error {
	for _, fieldBinlog := range fieldBinlogs {
		for _, binlog := range fieldBinlog.Binlogs {
			if binlog.GetLogPath() == "" {
				path, err := BuildLogPathWithRootPath(rootPath, binlogType, collectionID, partitionID,
					segmentID, fieldBinlog.GetFieldID(), binlog.GetLogID())
				if err != nil {
					return err
				}
				binlog.LogPath = path
			}
		}
	}
	return nil
}

// build a binlog path on the storage by metadata
func BuildLogPath(binlogType storage.BinlogType, collectionID, partitionID, segmentID, fieldID, logID typeutil.UniqueID) (string, error) {
	chunkManagerRootPath := paramtable.Get().MinioCfg.RootPath.GetValue()
	if paramtable.Get().CommonCfg.StorageType.GetValue() == "local" {
		chunkManagerRootPath = paramtable.Get().LocalStorageCfg.Path.GetValue()
	}
	return BuildLogPathWithRootPath(chunkManagerRootPath, binlogType, collectionID, partitionID, segmentID, fieldID, logID)
}

func BuildLogPathWithRootPath(chunkManagerRootPath string, binlogType storage.BinlogType, collectionID, partitionID, segmentID, fieldID, logID typeutil.UniqueID) (string, error) {
	switch binlogType {
	case storage.InsertBinlog:
		return metautil.BuildInsertLogPath(chunkManagerRootPath, collectionID, partitionID, segmentID, fieldID, logID), nil
	case storage.DeleteBinlog:
		return metautil.BuildDeltaLogPath(chunkManagerRootPath, collectionID, partitionID, segmentID, logID), nil
	case storage.StatsBinlog:
		return metautil.BuildStatsLogPath(chunkManagerRootPath, collectionID, partitionID, segmentID, fieldID, logID), nil
	case storage.BM25Binlog:
		return metautil.BuildBm25LogPath(chunkManagerRootPath, collectionID, partitionID, segmentID, fieldID, logID), nil
	}
	// should not happen
	return "", merr.WrapErrParameterInvalidMsg("invalid binlog type")
}

// GetLogIDFromBingLogPath get log id from binlog path
func GetLogIDFromBingLogPath(logPath string) (int64, error) {
	var logID int64
	idx := strings.LastIndex(logPath, "/")
	if idx == -1 {
		return 0, merr.WrapErrParameterInvalidMsg(fmt.Sprintf("invalid binlog path: %s", logPath))
	}
	var err error
	logPathStr := logPath[(idx + 1):]
	logID, err = strconv.ParseInt(logPathStr, 10, 64)
	if err != nil {
		return 0, err
	}
	return logID, nil
}
