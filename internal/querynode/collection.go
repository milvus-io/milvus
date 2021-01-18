package querynode

/*

#cgo CFLAGS: -I${SRCDIR}/../core/output/include

#cgo LDFLAGS: -L${SRCDIR}/../core/output/lib -lmilvus_segcore -Wl,-rpath=${SRCDIR}/../core/output/lib

#include "segcore/collection_c.h"
#include "segcore/segment_c.h"

*/
import "C"
import (
	"github.com/golang/protobuf/proto"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
)

type Collection struct {
	collectionPtr C.CCollection
	id            UniqueID
	schema        *schemapb.CollectionSchema
	partitions    []*Partition
}

func (c *Collection) Name() string {
	return c.schema.Name
}

func (c *Collection) ID() UniqueID {
	return c.id
}

func (c *Collection) Partitions() *[]*Partition {
	return &c.partitions
}

func (c *Collection) Schema() *schemapb.CollectionSchema {
	return c.schema
}

func newCollection(collectionID UniqueID, schema *schemapb.CollectionSchema) *Collection {
	/*
		CCollection
		NewCollection(const char* schema_proto_blob);

		const char*
		GetCollectionName(CCollection collection);
	*/
	schemaBlob := proto.MarshalTextString(schema)

	cSchemaBlob := C.CString(schemaBlob)
	collection := C.NewCollection(cSchemaBlob)

	var newCollection = &Collection{
		collectionPtr: collection,
		id:            collectionID,
		schema:        schema,
	}

	return newCollection
}

func deleteCollection(collection *Collection) {
	/*
		void
		deleteCollection(CCollection collection);
	*/
	cPtr := collection.collectionPtr
	C.DeleteCollection(cPtr)
}
