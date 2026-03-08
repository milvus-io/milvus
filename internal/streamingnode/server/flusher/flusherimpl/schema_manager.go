package flusherimpl

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
)

// newVersionedSchemaManager creates a new versioned schema manager.
func newVersionedSchemaManager(vchannel string, rs recovery.RecoveryStorage) *versionedSchemaManager {
	return &versionedSchemaManager{
		vchannel: vchannel,
		rs:       rs,
	}
}

// versionedSchemaManager is a schema manager that gets the schema from the recovery storage.
// It is used to get the schema of the vchannel at the given timetick.
type versionedSchemaManager struct {
	vchannel string
	rs       recovery.RecoveryStorage
}

func (m *versionedSchemaManager) GetSchema(timetick uint64) *schemapb.CollectionSchema {
	schema, err := m.rs.GetSchema(context.Background(), m.vchannel, timetick)
	if err != nil {
		panic(err)
	}
	return schema
}
