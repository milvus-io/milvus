// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package querynode

/*

#cgo CFLAGS: -I${SRCDIR}/../core/output/include

#cgo LDFLAGS: -L${SRCDIR}/../core/output/lib -lmilvus_segcore -Wl,-rpath=${SRCDIR}/../core/output/lib

#include "segcore/collection_c.h"
#include "segcore/segment_c.h"

*/
import "C"
import (
	"math"
	"sync"
	"unsafe"

	"go.uber.org/zap"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
)

type Collection struct {
	collectionPtr C.CCollection
	id            UniqueID
	partitionIDs  []UniqueID
	schema        *schemapb.CollectionSchema
	vChannels     []Channel
	pChannels     []Channel

	releaseMu   sync.RWMutex // guards releaseTime
	releaseTime Timestamp
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
		if id != partitionID {
			tmpIDs = append(tmpIDs, id)
		}
	}
	c.partitionIDs = tmpIDs
}

func (c *Collection) addVChannels(channels []Channel) {
	log.Debug("add vChannels to collection",
		zap.Any("channels", channels),
		zap.Any("collectionID", c.ID()))
	c.vChannels = append(c.vChannels, channels...)
}

func (c *Collection) getVChannels() []Channel {
	return c.vChannels
}

func (c *Collection) addPChannels(channels []Channel) {
	log.Debug("add pChannels to collection",
		zap.Any("channels", channels),
		zap.Any("collectionID", c.ID()))
	c.pChannels = append(c.pChannels, channels...)
}

func (c *Collection) getPChannels() []Channel {
	return c.pChannels
}

func (c *Collection) setReleaseTime(t Timestamp) {
	c.releaseMu.Lock()
	defer c.releaseMu.Unlock()
	c.releaseTime = t
}

func (c *Collection) getReleaseTime() Timestamp {
	c.releaseMu.RLock()
	defer c.releaseMu.RUnlock()
	return c.releaseTime
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
		vChannels:     make([]Channel, 0),
		pChannels:     make([]Channel, 0),
	}
	C.free(unsafe.Pointer(cSchemaBlob))

	log.Debug("create collection", zap.Int64("collectionID", collectionID))

	newCollection.setReleaseTime(Timestamp(math.MaxUint64))
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
