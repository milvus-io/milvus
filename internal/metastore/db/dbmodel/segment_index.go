package dbmodel

import (
	"time"

	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type SegmentIndex struct {
	ID       int64   `gorm:"id"`
	TenantID *string `gorm:"tenant_id"`
	// SegmentIndexInfo (CollectionID & PartitionID & SegmentID & FieldID & IndexID & BuildID & EnableIndex)
	CollectionID int64 `gorm:"collection_id"`
	PartitionID  int64 `gorm:"partition_id"`
	SegmentID    int64 `gorm:"segment_id"`
	// FieldIndexInfo (FieldID & IndexID)
	FieldID int64 `gorm:"field_id"`
	// IndexInfo (IndexID & IndexName & IndexParams)
	IndexID        int64     `gorm:"index_id"`
	IndexBuildID   int64     `gorm:"index_build_id"`
	EnableIndex    bool      `gorm:"enable_index"`
	IndexFilePaths string    `gorm:"index_file_paths"`
	IndexSize      uint64    `gorm:"index_size"`
	IsDeleted      bool      `gorm:"is_deleted"`
	CreatedAt      time.Time `gorm:"created_at"`
	UpdatedAt      time.Time `gorm:"updated_at"`
}

func (v SegmentIndex) TableName() string {
	return "segment_indexes"
}

type ISegmentIndexDb interface {
	Insert(in []*SegmentIndex) error
	MarkDeleted(tenantID string, in []*SegmentIndex) error
	MarkDeletedByCollID(tenantID string, collID typeutil.UniqueID) error
	MarkDeletedByIdxID(tenantID string, idxID typeutil.UniqueID) error
}
