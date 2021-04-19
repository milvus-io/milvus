package querynodeimp

/*

#cgo CFLAGS: -I${SRCDIR}/../core/output/include

#cgo LDFLAGS: -L${SRCDIR}/../core/output/lib -lmilvus_segcore -Wl,-rpath=${SRCDIR}/../core/output/lib

#include "segcore/collection_c.h"
#include "segcore/segment_c.h"

*/
import "C"
import "unsafe"

type Collection struct {
	collectionPtr C.CCollection
	id            UniqueID
	name          string
	partitions    []*Partition
}

func (c *Collection) Name() string {
	return c.name
}

func (c *Collection) ID() UniqueID {
	return c.id
}

func (c *Collection) Partitions() *[]*Partition {
	return &c.partitions
}

func newCollection(collectionID UniqueID, schemaBlob string) *Collection {
	/*
		CCollection
		NewCollection(const char* schema_proto_blob);

		const char*
		GetCollectionName(CCollection collection);
	*/
	cSchemaBlob := C.CString(schemaBlob)
	collection := C.NewCollection(cSchemaBlob)

	name := C.GetCollectionName(collection)
	collectionName := C.GoString(name)
	defer C.free(unsafe.Pointer(name))

	var newCollection = &Collection{
		collectionPtr: collection,
		id:            collectionID,
		name:          collectionName,
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
