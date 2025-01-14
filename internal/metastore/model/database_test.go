package model

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/proto/etcdpb"
)

var (
	properties = []*commonpb.KeyValuePair{
		{
			Key:   "key1",
			Value: "value1",
		},
		{
			Key:   "key2",
			Value: "value2",
		},
	}
	dbPB = &etcdpb.DatabaseInfo{
		TenantId:    "1",
		Name:        "test",
		Id:          1,
		CreatedTime: 1,
		State:       etcdpb.DatabaseState_DatabaseCreated,
		Properties:  properties,
	}

	dbModel = &Database{
		TenantID:    "1",
		Name:        "test",
		ID:          1,
		CreatedTime: 1,
		State:       etcdpb.DatabaseState_DatabaseCreated,
		Properties:  properties,
	}
)

func TestMarshalDatabaseModel(t *testing.T) {
	ret := MarshalDatabaseModel(dbModel)
	assert.Equal(t, dbPB, ret)
	assert.Nil(t, MarshalDatabaseModel(nil))
}

func TestUnmarshalDatabaseModel(t *testing.T) {
	ret := UnmarshalDatabaseModel(dbPB)
	assert.Equal(t, dbModel, ret)
	assert.Nil(t, UnmarshalDatabaseModel(nil))
}

func TestDatabaseCloneAndEqual(t *testing.T) {
	clone := dbModel.Clone()
	assert.Equal(t, dbModel, clone)
	assert.True(t, dbModel.Equal(*clone))
}

func TestDatabaseAvailable(t *testing.T) {
	assert.True(t, dbModel.Available())
	assert.True(t, NewDefaultDatabase(nil).Available())
}
