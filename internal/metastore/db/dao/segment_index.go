package dao

import (
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metastore/db/dbmodel"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type segmentIndexDb struct {
	db *gorm.DB
}

func (s *segmentIndexDb) Insert(in []*dbmodel.SegmentIndex) error {
	err := s.db.CreateInBatches(in, 100).Error
	if err != nil {
		log.Error("insert segment_indexes failed", zap.Error(err))
		return err
	}

	return nil
}

// MarkDeleted param in as multiple or condition
func (s *segmentIndexDb) MarkDeleted(tenantID string, segIndexes []*dbmodel.SegmentIndex) error {
	tx := s.db.Model(&dbmodel.SegmentIndex{})

	if tenantID != "" {
		tx = tx.Where("tenant_id = ?", tenantID)
	}

	inValues := make([][]interface{}, 0, len(segIndexes))
	for _, segIdx := range segIndexes {
		in := []interface{}{segIdx.CollectionID, segIdx.PartitionID, segIdx.SegmentID, segIdx.IndexID}
		inValues = append(inValues, in)
	}

	err := tx.Where("(collection_id, partition_id, segment_id, index_id) IN ?", inValues).Updates(dbmodel.SegmentIndex{
		IsDeleted: true,
		UpdatedAt: time.Now(),
	}).Error

	if err != nil {
		log.Error("update segment_indexes deleted failed", zap.Error(err))
		return err
	}

	return nil
}

func (s *segmentIndexDb) MarkDeletedByCollID(tenantID string, collID typeutil.UniqueID) error {
	tx := s.db.Model(&dbmodel.SegmentIndex{})

	if tenantID != "" {
		tx = tx.Where("tenant_id = ?", tenantID)
	}

	err := tx.Where("collection_id = ?", collID).Updates(dbmodel.SegmentIndex{
		IsDeleted: true,
		UpdatedAt: time.Now(),
	}).Error

	if err != nil {
		log.Error("update segment_indexes deleted by collection id failed", zap.Error(err))
		return err
	}

	return nil
}

func (s *segmentIndexDb) MarkDeletedByIdxID(tenantID string, idxID typeutil.UniqueID) error {
	tx := s.db.Model(&dbmodel.SegmentIndex{})

	if tenantID != "" {
		tx = tx.Where("tenant_id = ?", tenantID)
	}

	err := tx.Where("index_id = ?", idxID).Updates(dbmodel.SegmentIndex{
		IsDeleted: true,
		UpdatedAt: time.Now(),
	}).Error
	
	if err != nil {
		log.Error("update segment_indexes deleted by index id failed", zap.Error(err))
		return err
	}

	return nil
}
