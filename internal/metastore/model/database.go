package model

import (
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/common"
	pb "github.com/milvus-io/milvus/pkg/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/util"
)

type Database struct {
	TenantID    string
	ID          int64
	Name        string
	State       pb.DatabaseState
	CreatedTime uint64
	Properties  []*commonpb.KeyValuePair
}

func NewDatabase(id int64, name string, state pb.DatabaseState, properties []*commonpb.KeyValuePair) *Database {
	if properties == nil {
		properties = make([]*commonpb.KeyValuePair, 0)
	}
	return &Database{
		ID:          id,
		Name:        name,
		State:       state,
		CreatedTime: uint64(time.Now().UnixNano()),
		Properties:  properties,
	}
}

func NewDefaultDatabase(prop []*commonpb.KeyValuePair) *Database {
	return NewDatabase(util.DefaultDBID, util.DefaultDBName, pb.DatabaseState_DatabaseCreated, prop)
}

func (c *Database) Available() bool {
	return c.State == pb.DatabaseState_DatabaseCreated
}

func (c *Database) Clone() *Database {
	return &Database{
		TenantID:    c.TenantID,
		ID:          c.ID,
		Name:        c.Name,
		State:       c.State,
		CreatedTime: c.CreatedTime,
		Properties:  common.CloneKeyValuePairs(c.Properties),
	}
}

func (c *Database) Equal(other Database) bool {
	return c.TenantID == other.TenantID &&
		c.Name == other.Name &&
		c.ID == other.ID &&
		c.State == other.State &&
		c.CreatedTime == other.CreatedTime &&
		checkParamsEqual(c.Properties, other.Properties)
}

func (c *Database) GetProperty(key string) string {
	for _, e := range c.Properties {
		if e.GetKey() == key {
			return e.GetValue()
		}
	}
	return ""
}

func MarshalDatabaseModel(db *Database) *pb.DatabaseInfo {
	if db == nil {
		return nil
	}

	return &pb.DatabaseInfo{
		TenantId:    db.TenantID,
		Id:          db.ID,
		Name:        db.Name,
		State:       db.State,
		CreatedTime: db.CreatedTime,
		Properties:  db.Properties,
	}
}

func UnmarshalDatabaseModel(info *pb.DatabaseInfo) *Database {
	if info == nil {
		return nil
	}

	return &Database{
		Name:        info.GetName(),
		ID:          info.GetId(),
		CreatedTime: info.GetCreatedTime(),
		State:       info.GetState(),
		TenantID:    info.GetTenantId(),
		Properties:  info.GetProperties(),
	}
}
