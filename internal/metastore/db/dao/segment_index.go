package dao

import (
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metastore/db/dbmodel"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
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

func (s *segmentIndexDb) Upsert(in []*dbmodel.SegmentIndex) error {
	err := s.db.Clauses(clause.OnConflict{
		// constraint UNIQUE (tenant_id, segment_id, index_id)
		DoUpdates: clause.AssignmentColumns([]string{"index_build_id", "enable_index", "create_time"}),
	}).CreateInBatches(in, 100).Error

	if err != nil {
		log.Error("upsert segment_indexes failed", zap.Error(err))
		return err
	}

	return nil
}

func (s *segmentIndexDb) MarkDeleted(tenantID string, segIndexes []*dbmodel.SegmentIndex) error {
	inValues := make([][]interface{}, 0, len(segIndexes))
	for _, segIdx := range segIndexes {
		in := []interface{}{segIdx.SegmentID, segIdx.IndexID}
		inValues = append(inValues, in)
	}

	err := s.db.Model(&dbmodel.SegmentIndex{}).Where("tenant_id = ? AND (segment_id, index_id) IN ?", tenantID, inValues).Updates(dbmodel.SegmentIndex{
		IsDeleted: true,
	}).Error

	if err != nil {
		log.Error("update segment_indexes deleted failed", zap.String("tenant", tenantID), zap.Any("segmentIDIndexID", inValues), zap.Error(err))
		return err
	}

	return nil
}

func (s *segmentIndexDb) MarkDeletedByCollectionID(tenantID string, collID typeutil.UniqueID) error {
	err := s.db.Model(&dbmodel.SegmentIndex{}).Where("tenant_id = ? AND collection_id = ?", tenantID, collID).Updates(dbmodel.SegmentIndex{
		IsDeleted: true,
	}).Error

	if err != nil {
		log.Error("update segment_indexes deleted by collection id failed", zap.String("tenant", tenantID), zap.Int64("collID", collID), zap.Error(err))
		return err
	}

	return nil
}

func (s *segmentIndexDb) MarkDeletedByIndexID(tenantID string, indexID typeutil.UniqueID) error {
	err := s.db.Model(&dbmodel.SegmentIndex{}).Where("tenant_id = ? AND index_id = ?", tenantID, indexID).Updates(dbmodel.SegmentIndex{
		IsDeleted: true,
	}).Error

	if err != nil {
		log.Error("update segment_indexes deleted by index id failed", zap.String("tenant", tenantID), zap.Int64("indexID", indexID), zap.Error(err))
		return err
	}

	return nil
}
