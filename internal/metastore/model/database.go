package model

import (
	"time"

	pb "github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/util"
)

type Database struct {
	TenantID    string
	ID          int64
	Name        string
	State       pb.DatabaseState
	CreatedTime uint64
}

func NewDatabase(id int64, name string, sate pb.DatabaseState) *Database {
	return &Database{
		ID:          id,
		Name:        name,
		State:       sate,
		CreatedTime: uint64(time.Now().UnixNano()),
	}
}

func NewDefaultDatabase() *Database {
	return NewDatabase(util.DefaultDBID, util.DefaultDBName, pb.DatabaseState_DatabaseCreated)
}

func (c Database) Available() bool {
	return c.State == pb.DatabaseState_DatabaseCreated
}

func (c Database) Clone() *Database {
	return &Database{
		TenantID:    c.TenantID,
		ID:          c.ID,
		Name:        c.Name,
		State:       c.State,
		CreatedTime: c.CreatedTime,
	}
}

func (c Database) Equal(other Database) bool {
	return c.TenantID == other.TenantID &&
		c.Name == other.Name &&
		c.ID == other.ID &&
		c.State == other.State &&
		c.CreatedTime == other.CreatedTime
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
	}
}
