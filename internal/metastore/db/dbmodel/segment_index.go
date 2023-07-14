package dbmodel

import (
	"time"

	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type SegmentIndex struct {
	ID       int64  `gorm:"id"`
	TenantID string `gorm:"tenant_id"`
	// SegmentIndexInfo (CollectionID & PartitionID & SegmentID & FieldID & IndexID & BuildID & EnableIndex)
	CollectionID int64 `gorm:"collection_id"`
	PartitionID  int64 `gorm:"partition_id"`
	SegmentID    int64 `gorm:"segment_id"`
	NumRows      int64 `gorm:"num_rows"`
	// IndexInfo (IndexID & IndexName & IndexParams)
	IndexID       int64     `gorm:"index_id"`
	BuildID       int64     `gorm:"build_id"`
	NodeID        int64     `gorm:"node_id"`
	IndexVersion  int64     `gorm:"index_version"`
	IndexState    int32     `gorm:"index_state"`
	FailReason    string    `gorm:"fail_reason"`
	CreateTime    uint64    `gorm:"create_time"`
	IndexFileKeys string    `gorm:"index_file_keys"`
	IndexSize     uint64    `gorm:"index_size"`
	IsDeleted     bool      `gorm:"is_deleted"`
	CreatedAt     time.Time `gorm:"created_at"`
	UpdatedAt     time.Time `gorm:"updated_at"`
}

func (v SegmentIndex) TableName() string {
	return "segment_indexes"
}

type SegmentIndexResult struct {
	CollectionID  int64
	PartitionID   int64
	SegmentID     int64
	NumRows       int64
	IndexID       int64
	BuildID       int64
	NodeID        int64
	IndexVersion  int64
	IndexState    int32
	FailReason    string
	IsDeleted     bool
	CreateTime    uint64
	IndexFileKeys string
	IndexSize     uint64
}

//go:generate mockery --name=ISegmentIndexDb
type ISegmentIndexDb interface {
	Get(tenantID string, collectionID, buildID typeutil.UniqueID) ([]*SegmentIndexResult, error)
	List(tenantID string) ([]*SegmentIndexResult, error)
	Insert(in []*SegmentIndex) error
	Update(in *SegmentIndex) error
	MarkDeleted(tenantID string, in []*SegmentIndex) error
	MarkDeletedByCollectionID(tenantID string, collID typeutil.UniqueID) error
	MarkDeletedByBuildID(tenantID string, idxID typeutil.UniqueID) error
}

//func UnmarshalSegmentIndexModel(inputs []*SegmentIndexResult) ([]*model.SegmentIndex, error) {
//	result := make([]*model.SegmentIndex, 0, len(inputs))
//	for _, ir := range inputs {
//
//		var IndexFileKeys []string
//		if ir.IndexFileKeys != "" {
//			err := json.Unmarshal([]byte(ir.IndexFileKeys), &IndexFileKeys)
//			if err != nil {
//				log.Error("unmarshal index file paths of segment index failed", zap.Int64("collectionID", ir.CollectionID),
//					zap.Int64("indexID", ir.IndexID), zap.Int64("segmentID", ir.SegmentID),
//					zap.Int64("buildID", ir.BuildID), zap.Error(err))
//				return nil, err
//			}
//		}
//
//		idx := &model.SegmentIndex{
//			SegmentID:     ir.SegmentID,
//			CollectionID:  ir.CollectionID,
//			PartitionID:   ir.PartitionID,
//			NumRows:       ir.NumRows,
//			IndexID:       ir.IndexID,
//			BuildID:       ir.BuildID,
//			NodeID:        ir.NodeID,
//			IndexVersion:  ir.IndexVersion,
//			IndexState:    commonpb.IndexState(ir.IndexState),
//			FailReason:    ir.FailReason,
//			IsDeleted:     ir.IsDeleted,
//			CreateTime:    ir.CreateTime,
//			IndexFileKeys: IndexFileKeys,
//			IndexSize:     ir.IndexSize,
//		}
//		result = append(result, idx)
//	}
//
//	return result, nil
//}
