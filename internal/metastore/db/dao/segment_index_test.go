package dao

import (
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/milvus-io/milvus/internal/metastore/db/dbmodel"
	"github.com/stretchr/testify/assert"
)

func TestSegmentIndex_Insert(t *testing.T) {
	var segIndexes = []*dbmodel.SegmentIndex{
		{
			TenantID:       tenantID,
			CollectionID:   collID1,
			PartitionID:    partitionID1,
			SegmentID:      segmentID1,
			FieldID:        fieldID1,
			IndexID:        indexID1,
			IndexBuildID:   indexBuildID1,
			EnableIndex:    false,
			CreateTime:     uint64(1011),
			IndexFilePaths: "",
			IndexSize:      1024,
			IsDeleted:      false,
			CreatedAt:      time.Now(),
			UpdatedAt:      time.Now(),
		},
	}

	// expectation
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO `segment_indexes` (`tenant_id`,`collection_id`,`partition_id`,`segment_id`,`field_id`,`index_id`,`index_build_id`,`enable_index`,`create_time`,`index_file_paths`,`index_size`,`is_deleted`,`created_at`,`updated_at`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)").
		WithArgs(segIndexes[0].TenantID, segIndexes[0].CollectionID, segIndexes[0].PartitionID, segIndexes[0].SegmentID, segIndexes[0].FieldID, segIndexes[0].IndexID, segIndexes[0].IndexBuildID, segIndexes[0].EnableIndex, segIndexes[0].CreateTime, segIndexes[0].IndexFilePaths, segIndexes[0].IndexSize, segIndexes[0].IsDeleted, segIndexes[0].CreatedAt, segIndexes[0].UpdatedAt).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// actual
	err := segIndexTestDb.Insert(segIndexes)
	assert.Nil(t, err)
}

func TestSegmentIndex_Insert_Error(t *testing.T) {
	var segIndexes = []*dbmodel.SegmentIndex{
		{
			TenantID:       tenantID,
			CollectionID:   collID1,
			PartitionID:    partitionID1,
			SegmentID:      segmentID1,
			FieldID:        fieldID1,
			IndexID:        indexID1,
			IndexBuildID:   indexBuildID1,
			EnableIndex:    false,
			CreateTime:     uint64(1011),
			IndexFilePaths: "",
			IndexSize:      1024,
			IsDeleted:      false,
			CreatedAt:      time.Now(),
			UpdatedAt:      time.Now(),
		},
	}

	// expectation
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO `segment_indexes` (`tenant_id`,`collection_id`,`partition_id`,`segment_id`,`field_id`,`index_id`,`index_build_id`,`enable_index`,`create_time`,`index_file_paths`,`index_size`,`is_deleted`,`created_at`,`updated_at`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)").
		WithArgs(segIndexes[0].TenantID, segIndexes[0].CollectionID, segIndexes[0].PartitionID, segIndexes[0].SegmentID, segIndexes[0].FieldID, segIndexes[0].IndexID, segIndexes[0].IndexBuildID, segIndexes[0].EnableIndex, segIndexes[0].CreateTime, segIndexes[0].IndexFilePaths, segIndexes[0].IndexSize, segIndexes[0].IsDeleted, segIndexes[0].CreatedAt, segIndexes[0].UpdatedAt).
		WillReturnError(errors.New("test error"))
	mock.ExpectRollback()

	// actual
	err := segIndexTestDb.Insert(segIndexes)
	assert.Error(t, err)
}

func TestSegmentIndex_Upsert(t *testing.T) {
	var segIndexes = []*dbmodel.SegmentIndex{
		{
			TenantID:     tenantID,
			CollectionID: collID1,
			PartitionID:  partitionID1,
			SegmentID:    segmentID1,
			FieldID:      fieldID1,
			IndexID:      indexID1,
			IndexBuildID: indexBuildID1,
			EnableIndex:  false,
			CreateTime:   uint64(1011),
		},
	}

	// expectation
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO `segment_indexes` (`tenant_id`,`collection_id`,`partition_id`,`segment_id`,`field_id`,`index_id`,`index_build_id`,`enable_index`,`create_time`,`index_file_paths`,`index_size`,`is_deleted`,`created_at`,`updated_at`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE `index_build_id`=VALUES(`index_build_id`),`enable_index`=VALUES(`enable_index`),`create_time`=VALUES(`create_time`)").
		WithArgs(segIndexes[0].TenantID, segIndexes[0].CollectionID, segIndexes[0].PartitionID, segIndexes[0].SegmentID, segIndexes[0].FieldID, segIndexes[0].IndexID, segIndexes[0].IndexBuildID, segIndexes[0].EnableIndex, segIndexes[0].CreateTime, "", uint64(0), false, AnyTime{}, AnyTime{}).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// actual
	err := segIndexTestDb.Upsert(segIndexes)
	assert.Nil(t, err)
}

func TestSegmentIndex_Upsert_Error(t *testing.T) {
	var segIndexes = []*dbmodel.SegmentIndex{
		{
			TenantID:     tenantID,
			CollectionID: collID1,
			PartitionID:  partitionID1,
			SegmentID:    segmentID1,
			FieldID:      fieldID1,
			IndexID:      indexID1,
			IndexBuildID: indexBuildID1,
			EnableIndex:  false,
			CreateTime:   uint64(1011),
		},
	}

	// expectation
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO `segment_indexes` (`tenant_id`,`collection_id`,`partition_id`,`segment_id`,`field_id`,`index_id`,`index_build_id`,`enable_index`,`create_time`,`index_file_paths`,`index_size`,`is_deleted`,`created_at`,`updated_at`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE `index_build_id`=VALUES(`index_build_id`),`enable_index`=VALUES(`enable_index`),`create_time`=VALUES(`create_time`)").
		WithArgs(segIndexes[0].TenantID, segIndexes[0].CollectionID, segIndexes[0].PartitionID, segIndexes[0].SegmentID, segIndexes[0].FieldID, segIndexes[0].IndexID, segIndexes[0].IndexBuildID, segIndexes[0].EnableIndex, segIndexes[0].CreateTime, "", uint64(0), false, AnyTime{}, AnyTime{}).
		WillReturnError(errors.New("test error"))
	mock.ExpectRollback()

	// actual
	err := segIndexTestDb.Upsert(segIndexes)
	assert.Error(t, err)
}

func TestSegmentIndex_MarkDeleted(t *testing.T) {
	var segIndexes = []*dbmodel.SegmentIndex{
		{
			SegmentID: segmentID1,
			IndexID:   indexID1,
		},
		{
			SegmentID: segmentID2,
			IndexID:   indexID2,
		},
	}

	// expectation
	mock.ExpectBegin()
	mock.ExpectExec("UPDATE `segment_indexes` SET `is_deleted`=?,`updated_at`=? WHERE tenant_id = ? AND (segment_id, index_id) IN ((?,?),(?,?))").
		WithArgs(true, AnyTime{}, tenantID, segIndexes[0].SegmentID, segIndexes[0].IndexID, segIndexes[1].SegmentID, segIndexes[1].IndexID).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// actual
	err := segIndexTestDb.MarkDeleted(tenantID, segIndexes)
	assert.Nil(t, err)
}

func TestSegmentIndex_MarkDeleted_Error(t *testing.T) {
	var segIndexes = []*dbmodel.SegmentIndex{
		{
			SegmentID: segmentID1,
			IndexID:   indexID1,
		},
	}

	// expectation
	mock.ExpectBegin()
	mock.ExpectExec("UPDATE `segment_indexes` SET `is_deleted`=?,`updated_at`=? WHERE tenant_id = ? AND (segment_id, index_id) IN ((?,?))").
		WithArgs(true, AnyTime{}, tenantID, segIndexes[0].SegmentID, segIndexes[0].IndexID).
		WillReturnError(errors.New("test error"))
	mock.ExpectRollback()

	// actual
	err := segIndexTestDb.MarkDeleted(tenantID, segIndexes)
	assert.Error(t, err)
}

func TestSegmentIndex_MarkDeletedByCollID(t *testing.T) {
	// expectation
	mock.ExpectBegin()
	mock.ExpectExec("UPDATE `segment_indexes` SET `is_deleted`=?,`updated_at`=? WHERE tenant_id = ? AND collection_id = ?").
		WithArgs(true, AnyTime{}, tenantID, collID1).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// actual
	err := segIndexTestDb.MarkDeletedByCollectionID(tenantID, collID1)
	assert.Nil(t, err)
}

func TestSegmentIndex_MarkDeletedByCollID_Error(t *testing.T) {
	// expectation
	mock.ExpectBegin()
	mock.ExpectExec("UPDATE `segment_indexes` SET `is_deleted`=?,`updated_at`=? WHERE tenant_id = ? AND collection_id = ?").
		WithArgs(true, AnyTime{}, tenantID, collID1).
		WillReturnError(errors.New("test error"))
	mock.ExpectRollback()

	// actual
	err := segIndexTestDb.MarkDeletedByCollectionID(tenantID, collID1)
	assert.Error(t, err)
}

func TestSegmentIndex_MarkDeletedByIdxID(t *testing.T) {
	// expectation
	mock.ExpectBegin()
	mock.ExpectExec("UPDATE `segment_indexes` SET `is_deleted`=?,`updated_at`=? WHERE tenant_id = ? AND index_id = ?").
		WithArgs(true, AnyTime{}, tenantID, indexID1).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// actual
	err := segIndexTestDb.MarkDeletedByIndexID(tenantID, indexID1)
	assert.Nil(t, err)
}

func TestSegmentIndex_MarkDeletedByIdxID_Error(t *testing.T) {
	// expectation
	mock.ExpectBegin()
	mock.ExpectExec("UPDATE `segment_indexes` SET `is_deleted`=?,`updated_at`=? WHERE tenant_id = ? AND index_id = ?").
		WithArgs(true, AnyTime{}, tenantID, indexID1).
		WillReturnError(errors.New("test error"))
	mock.ExpectRollback()

	// actual
	err := segIndexTestDb.MarkDeletedByIndexID(tenantID, indexID1)
	assert.Error(t, err)
}
