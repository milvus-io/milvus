package dao

import (
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/milvus-io/milvus/internal/metastore/db/dbmodel"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type segmentIndexDb struct {
	db *gorm.DB
}

func (s *segmentIndexDb) Get(tenantID string, collectionID, buildID typeutil.UniqueID) ([]*dbmodel.SegmentIndexResult, error) {
	var r []*dbmodel.SegmentIndexResult

	err := s.db.Model(&dbmodel.SegmentIndex{}).Where("tenant_id = ? AND collection_id = ? AND build_id = ?", tenantID, collectionID, buildID).Find(&r).Error
	if err != nil {
		log.Error("get indexes by collection_id failed", zap.String("tenant", tenantID), zap.Int64("collectionID", collectionID), zap.Error(err))
		return nil, err
	}

	return r, nil
}

func (s *segmentIndexDb) List(tenantID string) ([]*dbmodel.SegmentIndexResult, error) {
	tx := s.db.Table("segment_indexes").
		Select("segment_indexes.collection_id AS collection_id, segment_indexes.partition_id AS partition_id, "+
			"segment_indexes.segment_id AS segment_id, segment_indexes.num_rows AS num_rows, segment_indexes.index_id AS index_id, "+
			"segment_indexes.build_id AS build_id, segment_indexes.node_id AS node_id, segment_indexes.index_version AS index_version, "+
			"segment_indexes.index_state AS index_state,segment_indexes.fail_reason AS fail_reason, segment_indexes.create_time AS create_time,"+
			"segment_indexes.index_file_keys AS index_file_keys, segment_indexes.index_size AS index_size, segment_indexes.is_deleted AS is_deleted").
		Where("indexes.tenant_id = ?", tenantID)

	var rs []*dbmodel.SegmentIndexResult
	err := tx.Scan(&rs).Error
	if err != nil {
		log.Error("list indexes by join failed", zap.String("tenant", tenantID), zap.Error(err))
		return nil, err
	}

	return rs, nil
}

func (s *segmentIndexDb) Insert(in []*dbmodel.SegmentIndex) error {
	err := s.db.CreateInBatches(in, 100).Error
	if err != nil {
		log.Error("insert segment_indexes failed", zap.Error(err))
		return err
	}

	return nil
}

func (s *segmentIndexDb) Update(in *dbmodel.SegmentIndex) error {
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
		log.Error("update segment_indexes deleted by collection id failed", zap.String("tenant", tenantID), zap.Int64("collectionID", collID), zap.Error(err))
		return err
	}

	return nil
}

func (s *segmentIndexDb) MarkDeletedByBuildID(tenantID string, buildID typeutil.UniqueID) error {
	err := s.db.Model(&dbmodel.SegmentIndex{}).Where("tenant_id = ? AND build_id = ?", tenantID, buildID).Updates(dbmodel.SegmentIndex{
		IsDeleted: true,
	}).Error

	if err != nil {
		log.Error("update segment_indexes deleted by index id failed", zap.String("tenant", tenantID), zap.Int64("buildID", buildID), zap.Error(err))
		return err
	}

	return nil
}
