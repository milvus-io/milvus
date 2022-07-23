package table

import (
	"encoding/json"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
)

type SegmentIndex struct {
	ID       int64   `db:"id"`
	TenantID *string `db:"tenant_id"`
	// SegmentIndexInfo (CollectionID & PartitionID & SegmentID & FieldID & IndexID & BuildID & EnableIndex)
	CollectionID int64 `db:"collection_id"`
	PartitionID  int64 `db:"partition_id"`
	SegmentID    int64 `db:"segment_id"`
	// FieldIndexInfo (FieldID & IndexID)
	FieldID int64 `db:"field_id"`
	// IndexInfo (IndexID & IndexName & IndexParams)
	IndexID        int64              `db:"index_id"`
	BuildID        int64              `db:"build_id"`
	EnableIndex    bool               `db:"enable_index"`
	IndexFilePaths string             `db:"index_file_paths"`
	IndexSize      uint64             `db:"index_size"`
	Ts             typeutil.Timestamp `db:"ts"`
	IsDeleted      bool               `db:"is_deleted"`
	CreatedAt      time.Time          `db:"created_at"`
	UpdatedAt      time.Time          `db:"updated_at"`
}

// model <---> db

func ConvertSegmentIndexDBToModel(segmentIndex *SegmentIndex) *model.SegmentIndex {
	var indexFilePaths []string
	err := json.Unmarshal([]byte(segmentIndex.IndexFilePaths), &indexFilePaths)
	if err != nil {
		log.Error("unmarshal IndexFilePaths of segment index failed", zap.Error(err))
	}
	return &model.SegmentIndex{
		Segment: model.Segment{
			SegmentID:   segmentIndex.SegmentID,
			PartitionID: segmentIndex.PartitionID,
		},
		EnableIndex:    segmentIndex.EnableIndex,
		BuildID:        segmentIndex.BuildID,
		IndexSize:      segmentIndex.IndexSize,
		IndexFilePaths: indexFilePaths,
	}
}
