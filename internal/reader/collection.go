package reader

/*

#cgo CFLAGS: -I${SRCDIR}/../core/output/include

#cgo LDFLAGS: -L${SRCDIR}/../core/output/lib -lmilvus_segcore -Wl,-rpath=${SRCDIR}/../core/output/lib

#include "collection_c.h"
#include "partition_c.h"
#include "segment_c.h"

*/
import "C"
import (
	"github.com/zilliztech/milvus-distributed/internal/proto/etcdpb"
)

type Collection struct {
	collectionPtr C.CCollection
	meta          *etcdpb.CollectionMeta
	partitions    []*Partition
}

func (c *Collection) Name() string {
	return (*c.meta).Schema.Name
}

func (c *Collection) ID() UniqueID {
	return (*c.meta).Id
}

func (c *Collection) Partitions() *[]*Partition {
	return &c.partitions
}

func newCollection(collMeta *etcdpb.CollectionMeta, collMetaBlob string) *Collection {
	/*
		CCollection
		newCollection(const char* schema_conf);
	*/
	cTmp := C.CString("tmp")
	cCollMetaBlob := C.CString(collMetaBlob)
	collection := C.NewCollection(cTmp, cCollMetaBlob)

	var newCollection = &Collection{collectionPtr: collection, meta: collMeta}

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
