package segments

import (
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// TODO(#40451): remove this borrowed collection adapter when QueryView execution
// uses segcore collections directly instead of the legacy QueryNode task layer.
func NewCollectionFromCCollectionForViewQuery(ccollection *segcore.CCollection, databaseName ...string) (*Collection, error) {
	if ccollection == nil {
		return nil, merr.WrapErrServiceInternalMsg("nil collection for view query execution")
	}
	schema := ccollection.Schema()
	loadFields := collectionLoadFields(schema)
	collection := &Collection{
		ccollection: ccollection,
		id:          ccollection.ID(),
		partitions:  typeutil.NewConcurrentSet[int64](),
		loadType:    querypb.LoadType_LoadCollection,
		refCount:    atomic.NewUint32(0),
		loadFields:  loadFields,
	}
	var logicalSchemaVersion uint64
	if schema != nil {
		logicalSchemaVersion = uint64(schema.GetVersion())
	}
	collection.setSchema(schema, logicalSchemaVersion, 0, initialSegcoreSchemaVersion(logicalSchemaVersion, 0))
	if len(databaseName) > 0 {
		collection.dbName = databaseName[0]
	}
	return collection, nil
}

func collectionLoadFields(schema *schemapb.CollectionSchema) typeutil.Set[int64] {
	loadFields := typeutil.NewSet[int64]()
	if schema == nil {
		return loadFields
	}
	for _, field := range schema.GetFields() {
		loadFields.Insert(field.GetFieldID())
	}
	for _, structArrayField := range schema.GetStructArrayFields() {
		for _, field := range structArrayField.GetFields() {
			loadFields.Insert(field.GetFieldID())
		}
	}
	return loadFields
}
