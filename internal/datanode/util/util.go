package util

import (
	"path"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/util/metautil"
)

func ParseStorageConfig(s *indexpb.StorageConfig) (*indexcgopb.StorageConfig, error) {
	bs, err := proto.Marshal(s)
	if err != nil {
		return nil, err
	}
	res := &indexcgopb.StorageConfig{}
	err = proto.Unmarshal(bs, res)
	return res, err
}

func GetSegmentInsertFiles(fieldBinlogs []*datapb.FieldBinlog, storageConfig *indexpb.StorageConfig, collectionID int64, partitionID int64, segmentID int64) *indexcgopb.SegmentInsertFiles {
	insertLogs := make([]*indexcgopb.FieldInsertFiles, 0)
	for _, insertLog := range fieldBinlogs {
		filePaths := make([]string, 0)
		columnGroupID := insertLog.GetFieldID()
		for _, binlog := range insertLog.GetBinlogs() {
			filePath := metautil.BuildInsertLogPath(storageConfig.GetRootPath(), collectionID, partitionID, segmentID, columnGroupID, binlog.GetLogID())
			if storageConfig.StorageType != "local" {
				filePath = path.Join(storageConfig.GetBucketName(), filePath)
			}
			filePaths = append(filePaths, filePath)
		}
		insertLogs = append(insertLogs, &indexcgopb.FieldInsertFiles{
			FilePaths: filePaths,
		})
	}
	return &indexcgopb.SegmentInsertFiles{
		FieldInsertFiles: insertLogs,
	}
}
