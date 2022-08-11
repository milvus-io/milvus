package dao

import (
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/milvus-io/milvus/internal/metastore/db/dbmodel"
	"github.com/stretchr/testify/assert"
)

func TestCollectionChannel_GetByCollID(t *testing.T) {
	var collChannels = []*dbmodel.CollectionChannel{
		{
			TenantID:            tenantID,
			CollectionID:        collID1,
			VirtualChannelName:  "test_virtual_channel_1",
			PhysicalChannelName: "test_physical_channel_1",
			Removed:             false,
			Ts:                  ts,
			IsDeleted:           false,
			CreatedAt:           time.Now(),
			UpdatedAt:           time.Now(),
		},
	}

	// expectation
	mock.ExpectQuery("SELECT * FROM `collection_channels` WHERE tenant_id = ? AND collection_id = ? AND ts = ? AND is_deleted = false").
		WithArgs(tenantID, collID1, ts).
		WillReturnRows(
			sqlmock.NewRows([]string{"tenant_id", "collection_id", "virtual_channel_name", "physical_channel_name", "removed", "ts", "is_deleted", "created_at", "updated_at"}).
				AddRow(collChannels[0].TenantID, collChannels[0].CollectionID, collChannels[0].VirtualChannelName, collChannels[0].PhysicalChannelName, collChannels[0].Removed, collChannels[0].Ts, collChannels[0].IsDeleted, collChannels[0].CreatedAt, collChannels[0].UpdatedAt))

	// actual
	res, err := channelTestDb.GetByCollectionID(tenantID, collID1, ts)
	assert.Nil(t, err)
	assert.Equal(t, collChannels, res)
}

func TestCollectionChannel_GetByCollID_Error(t *testing.T) {
	// expectation
	mock.ExpectQuery("SELECT * FROM `collection_channels` WHERE tenant_id = ? AND collection_id = ? AND ts = ? AND is_deleted = false").
		WithArgs(tenantID, collID1, ts).
		WillReturnError(errors.New("test error"))

	// actual
	res, err := channelTestDb.GetByCollectionID(tenantID, collID1, ts)
	assert.Nil(t, res)
	assert.Error(t, err)
}

func TestCollectionChannel_Insert(t *testing.T) {
	var collChannels = []*dbmodel.CollectionChannel{
		{
			TenantID:            "",
			CollectionID:        collID1,
			VirtualChannelName:  "test_virtual_channel_1",
			PhysicalChannelName: "test_physical_channel_1",
			Removed:             false,
			Ts:                  ts,
			IsDeleted:           false,
			CreatedAt:           time.Now(),
			UpdatedAt:           time.Now(),
		},
	}

	// expectation
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO `collection_channels` (`tenant_id`,`collection_id`,`virtual_channel_name`,`physical_channel_name`,`removed`,`ts`,`is_deleted`,`created_at`,`updated_at`) VALUES (?,?,?,?,?,?,?,?,?)").
		WithArgs(collChannels[0].TenantID, collChannels[0].CollectionID, collChannels[0].VirtualChannelName, collChannels[0].PhysicalChannelName, collChannels[0].Removed, collChannels[0].Ts, collChannels[0].IsDeleted, collChannels[0].CreatedAt, collChannels[0].UpdatedAt).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// actual
	err := channelTestDb.Insert(collChannels)
	assert.Nil(t, err)
}

func TestCollectionChannel_Insert_Error(t *testing.T) {
	var collChannels = []*dbmodel.CollectionChannel{
		{
			TenantID:            "",
			CollectionID:        collID1,
			VirtualChannelName:  "test_virtual_channel_1",
			PhysicalChannelName: "test_physical_channel_1",
			Removed:             false,
			Ts:                  ts,
			IsDeleted:           false,
			CreatedAt:           time.Now(),
			UpdatedAt:           time.Now(),
		},
	}

	// expectation
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO `collection_channels` (`tenant_id`,`collection_id`,`virtual_channel_name`,`physical_channel_name`,`removed`,`ts`,`is_deleted`,`created_at`,`updated_at`) VALUES (?,?,?,?,?,?,?,?,?)").
		WithArgs(collChannels[0].TenantID, collChannels[0].CollectionID, collChannels[0].VirtualChannelName, collChannels[0].PhysicalChannelName, collChannels[0].Removed, collChannels[0].Ts, collChannels[0].IsDeleted, collChannels[0].CreatedAt, collChannels[0].UpdatedAt).
		WillReturnError(errors.New("test error"))
	mock.ExpectRollback()

	// actual
	err := channelTestDb.Insert(collChannels)
	assert.Error(t, err)
}
