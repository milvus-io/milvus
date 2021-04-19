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
	"github.com/zilliztech/milvus-distributed/internal/log"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
	"go.uber.org/zap"
)

type Collection struct {
	collectionPtr C.CCollection
	id            UniqueID
	partitionIDs  []UniqueID
	schema        *schemapb.CollectionSchema
}

func (c *Collection) ID() UniqueID {
	return c.id
}

func (c *Collection) Schema() *schemapb.CollectionSchema {
	return c.schema
}

func (c *Collection) addPartitionID(partitionID UniqueID) {
	c.partitionIDs = append(c.partitionIDs, partitionID)
}

func (c *Collection) removePartitionID(partitionID UniqueID) {
	tmpIDs := make([]UniqueID, 0)
	for _, id := range c.partitionIDs {
		if id == partitionID {
			tmpIDs = append(tmpIDs, id)
		}
	}
	c.partitionIDs = tmpIDs
}

func newCollection(collectionID UniqueID, schema *schemapb.CollectionSchema) *Collection {
	/*
		CCollection
		NewCollection(const char* schema_proto_blob);
	*/
	schemaBlob := proto.MarshalTextString(schema)

	cSchemaBlob := C.CString(schemaBlob)
	collection := C.NewCollection(cSchemaBlob)

	var newCollection = &Collection{
		collectionPtr: collection,
		id:            collectionID,
		schema:        schema,
	}

	log.Debug("create collection", zap.Int64("collectionID", collectionID))

	return newCollection
}

func deleteCollection(collection *Collection) {
	/*
		void
		deleteCollection(CCollection collection);
	*/
	cPtr := collection.collectionPtr
	C.DeleteCollection(cPtr)

	collection.collectionPtr = nil

	log.Debug("delete collection", zap.Int64("collectionID", collection.ID()))

	collection = nil
}
