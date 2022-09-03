package dao

import (
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/milvus-io/milvus/internal/metastore/db/dbmodel"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/stretchr/testify/assert"
)

func TestPartition_GetByCollID(t *testing.T) {
	var partitions = []*dbmodel.Partition{
		{
			TenantID:                  tenantID,
			PartitionID:               fieldID1,
			PartitionName:             "test_field_1",
			PartitionCreatedTimestamp: typeutil.Timestamp(1000),
			CollectionID:              collID1,
			Ts:                        ts,
		},
	}

	// expectation
	mock.ExpectQuery("SELECT * FROM `partitions` WHERE tenant_id = ? AND collection_id = ? AND ts = ? AND is_deleted = false").
		WithArgs(tenantID, collID1, ts).
		WillReturnRows(
			sqlmock.NewRows([]string{"tenant_id", "partition_id", "partition_name", "partition_created_timestamp", "collection_id", "ts"}).
				AddRow(partitions[0].TenantID, partitions[0].PartitionID, partitions[0].PartitionName, partitions[0].PartitionCreatedTimestamp, partitions[0].CollectionID, partitions[0].Ts))

	// actual
	res, err := partitionTestDb.GetByCollectionID(tenantID, collID1, ts)
	assert.Nil(t, err)
	assert.Equal(t, partitions, res)
}

func TestPartition_GetByCollID_Error(t *testing.T) {
	// expectation
	mock.ExpectQuery("SELECT * FROM `partitions` WHERE tenant_id = ? AND collection_id = ? AND ts = ? AND is_deleted = false").
		WithArgs(tenantID, collID1, ts).
		WillReturnError(errors.New("test error"))

	// actual
	res, err := partitionTestDb.GetByCollectionID(tenantID, collID1, ts)
	assert.Nil(t, res)
	assert.Error(t, err)
}

func TestPartition_Insert(t *testing.T) {
	var partitions = []*dbmodel.Partition{
		{
			TenantID:                  tenantID,
			PartitionID:               fieldID1,
			PartitionName:             "test_field_1",
			PartitionCreatedTimestamp: typeutil.Timestamp(1000),
			CollectionID:              collID1,
			Ts:                        ts,
			IsDeleted:                 false,
			CreatedAt:                 time.Now(),
			UpdatedAt:                 time.Now(),
		},
	}

	// expectation
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO `partitions` (`tenant_id`,`partition_id`,`partition_name`,`partition_created_timestamp`,`collection_id`,`status`,`ts`,`is_deleted`,`created_at`,`updated_at`) VALUES (?,?,?,?,?,?,?,?,?,?)").
		WithArgs(partitions[0].TenantID, partitions[0].PartitionID, partitions[0].PartitionName, partitions[0].PartitionCreatedTimestamp, partitions[0].CollectionID, partitions[0].Status, partitions[0].Ts, partitions[0].IsDeleted, partitions[0].CreatedAt, partitions[0].UpdatedAt).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// actual
	err := partitionTestDb.Insert(partitions)
	assert.Nil(t, err)
}

func TestPartition_Insert_Error(t *testing.T) {
	var partitions = []*dbmodel.Partition{
		{
			TenantID:                  tenantID,
			PartitionID:               fieldID1,
			PartitionName:             "test_field_1",
			PartitionCreatedTimestamp: typeutil.Timestamp(1000),
			CollectionID:              collID1,
			Ts:                        ts,
			IsDeleted:                 false,
			CreatedAt:                 time.Now(),
			UpdatedAt:                 time.Now(),
		},
	}

	// expectation
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO `partitions` (`tenant_id`,`partition_id`,`partition_name`,`partition_created_timestamp`,`collection_id`,`ts`,`is_deleted`,`created_at`,`updated_at`) VALUES (?,?,?,?,?,?,?,?,?)").
		WithArgs(partitions[0].TenantID, partitions[0].PartitionID, partitions[0].PartitionName, partitions[0].PartitionCreatedTimestamp, partitions[0].CollectionID, partitions[0].Ts, partitions[0].IsDeleted, partitions[0].CreatedAt, partitions[0].UpdatedAt).
		WillReturnError(errors.New("test error"))
	mock.ExpectRollback()

	// actual
	err := partitionTestDb.Insert(partitions)
	assert.Error(t, err)
}

func Test_partitionDb_Update(t *testing.T) {
	t.Run("normal case", func(t *testing.T) {
		partition := &dbmodel.Partition{
			ID:                        100,
			TenantID:                  tenantID,
			PartitionID:               fieldID1,
			PartitionName:             "test_field_1",
			PartitionCreatedTimestamp: typeutil.Timestamp(1000),
			CollectionID:              collID1,
			Ts:                        ts,
			IsDeleted:                 false,
			CreatedAt:                 time.Now(),
			UpdatedAt:                 time.Now(),
		}

		mock.ExpectBegin()
		mock.ExpectExec("UPDATE `partitions` SET `collection_id`=?,`created_at`=?,`is_deleted`=?,`partition_created_timestamp`=?,`partition_id`=?,`partition_name`=?,`status`=?,`tenant_id`=?,`ts`=?,`updated_at`=? WHERE id = ?").
			WithArgs(partition.CollectionID, partition.CreatedAt, partition.IsDeleted, partition.PartitionCreatedTimestamp, partition.PartitionID, partition.PartitionName, partition.Status, partition.TenantID, partition.Ts, partition.UpdatedAt, partition.ID).
			WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectCommit()

		err := partitionTestDb.Update(partition)
		assert.NoError(t, err)
	})

	t.Run("error case", func(t *testing.T) {
		partition := &dbmodel.Partition{
			ID:                        100,
			TenantID:                  tenantID,
			PartitionID:               fieldID1,
			PartitionName:             "test_field_1",
			PartitionCreatedTimestamp: typeutil.Timestamp(1000),
			CollectionID:              collID1,
			Ts:                        ts,
			IsDeleted:                 false,
			CreatedAt:                 time.Now(),
			UpdatedAt:                 time.Now(),
		}

		mock.ExpectBegin()
		mock.ExpectExec("UPDATE `partitions` SET `collection_id`=?,`created_at`=?,`is_deleted`=?,`partition_created_timestamp`=?,`partition_id`=?,`partition_name`=?,`status`=?,`tenant_id`=?,`ts`=?,`updated_at`=? WHERE id = ?").
			WithArgs(partition.CollectionID, partition.CreatedAt, partition.IsDeleted, partition.PartitionCreatedTimestamp, partition.PartitionID, partition.PartitionName, partition.Status, partition.TenantID, partition.Ts, partition.UpdatedAt, partition.ID).
			WillReturnError(errors.New("error mock Update Partition"))
		mock.ExpectRollback()

		err := partitionTestDb.Update(partition)
		assert.Error(t, err)
	})

}
