// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package querynode

/*

#cgo CFLAGS: -I${SRCDIR}/../core/output/include

#cgo darwin LDFLAGS: -L${SRCDIR}/../core/output/lib -lmilvus_segcore -Wl,-rpath,"${SRCDIR}/../core/output/lib"
#cgo linux LDFLAGS: -L${SRCDIR}/../core/output/lib -lmilvus_segcore -Wl,-rpath=${SRCDIR}/../core/output/lib
#cgo windows LDFLAGS: -L${SRCDIR}/../core/output/lib -lmilvus_segcore -Wl,-rpath=${SRCDIR}/../core/output/lib

#include "segcore/collection_c.h"
#include "segcore/segment_c.h"

*/
import "C"
import (
	"fmt"
	"math"
	"sync"
	"unsafe"

	"github.com/milvus-io/milvus/internal/metrics"

	"go.uber.org/zap"

	"github.com/golang/protobuf/proto"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
)

// Collection is a wrapper of the underlying C-structure C.CCollection
type Collection struct {
	collectionPtr C.CCollection
	id            UniqueID
	partitionIDs  []UniqueID
	schema        *schemapb.CollectionSchema

	channelMu      sync.RWMutex
	vChannels      []Channel
	pChannels      []Channel
	vDeltaChannels []Channel
	pDeltaChannels []Channel

	loadType loadType

	releaseMu          sync.RWMutex // guards release
	releasedPartitions map[UniqueID]struct{}
	releaseTime        Timestamp
}

// ID returns collection id
func (c *Collection) ID() UniqueID {
	return c.id
}

// Schema returns the schema of collection
func (c *Collection) Schema() *schemapb.CollectionSchema {
	return c.schema
}

// addPartitionID would add a partition id to partition id list of collection
func (c *Collection) addPartitionID(partitionID UniqueID) {
	c.releaseMu.Lock()
	defer c.releaseMu.Unlock()

	c.partitionIDs = append(c.partitionIDs, partitionID)
	log.Debug("queryNode collection info after add a partition",
		zap.Int64("partitionID", partitionID), zap.Int64("collectionID", c.id),
		zap.Int64s("partitions", c.partitionIDs))
}

// removePartitionID removes the partition id from partition id list of collection
func (c *Collection) removePartitionID(partitionID UniqueID) {
	tmpIDs := make([]UniqueID, 0, len(c.partitionIDs))
	for _, id := range c.partitionIDs {
		if id != partitionID {
			tmpIDs = append(tmpIDs, id)
		}
	}
	c.partitionIDs = tmpIDs
}

// addVChannels adds virtual channels to collection
func (c *Collection) addVChannels(channels []Channel) {
	c.channelMu.Lock()
	defer c.channelMu.Unlock()
OUTER:
	for _, dstChan := range channels {
		for _, srcChan := range c.vChannels {
			if dstChan == srcChan {
				log.Debug("vChannel has been existed in collection's vChannels",
					zap.Int64("collectionID", c.ID()),
					zap.String("vChannel", dstChan),
				)
				continue OUTER
			}
		}
		log.Debug("add vChannel to collection",
			zap.Int64("collectionID", c.ID()),
			zap.String("vChannel", dstChan),
		)
		c.vChannels = append(c.vChannels, dstChan)
	}

	metrics.QueryNodeNumDmlChannels.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.QueryNodeID)).Add(float64(len(c.vChannels)))
}

// getVChannels get virtual channels of collection
func (c *Collection) getVChannels() []Channel {
	c.channelMu.RLock()
	defer c.channelMu.RUnlock()
	tmpChannels := make([]Channel, len(c.vChannels))
	copy(tmpChannels, c.vChannels)
	return tmpChannels
}

// removeVChannel remove the virtual channel from collection
func (c *Collection) removeVChannel(channel Channel) {
	c.channelMu.Lock()
	defer c.channelMu.Unlock()
	tmpChannels := make([]Channel, 0)
	for _, vChannel := range c.vChannels {
		if channel != vChannel {
			tmpChannels = append(tmpChannels, vChannel)
		}
	}
	c.vChannels = tmpChannels
	log.Debug("remove vChannel from collection",
		zap.Int64("collectionID", c.ID()),
		zap.String("channel", channel),
	)

	metrics.QueryNodeNumDmlChannels.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.QueryNodeID)).Sub(float64(len(c.vChannels)))
}

// addPChannels add physical channels to physical channels of collection
func (c *Collection) addPChannels(channels []Channel) {
	c.channelMu.Lock()
	defer c.channelMu.Unlock()
OUTER:
	for _, dstChan := range channels {
		for _, srcChan := range c.pChannels {
			if dstChan == srcChan {
				log.Debug("pChannel has been existed in collection's pChannels",
					zap.Int64("collectionID", c.ID()),
					zap.String("pChannel", dstChan),
				)
				continue OUTER
			}
		}
		log.Debug("add pChannel to collection",
			zap.Int64("collectionID", c.ID()),
			zap.String("pChannel", dstChan),
		)
		c.pChannels = append(c.pChannels, dstChan)
	}
}

// getPChannels get physical channels of collection
func (c *Collection) getPChannels() []Channel {
	c.channelMu.RLock()
	defer c.channelMu.RUnlock()
	tmpChannels := make([]Channel, len(c.pChannels))
	copy(tmpChannels, c.pChannels)
	return tmpChannels
}

// addPChannels add physical channels to physical channels of collection
func (c *Collection) addPDeltaChannels(channels []Channel) {
	c.channelMu.Lock()
	defer c.channelMu.Unlock()
OUTER:
	for _, dstChan := range channels {
		for _, srcChan := range c.pDeltaChannels {
			if dstChan == srcChan {
				log.Debug("pChannel has been existed in collection's pChannels",
					zap.Int64("collectionID", c.ID()),
					zap.String("pChannel", dstChan),
				)
				continue OUTER
			}
		}
		log.Debug("add pChannel to collection",
			zap.Int64("collectionID", c.ID()),
			zap.String("pChannel", dstChan),
		)
		c.pDeltaChannels = append(c.pDeltaChannels, dstChan)
	}
}

// getPChannels get physical channels of collection
func (c *Collection) getPDeltaChannels() []Channel {
	c.channelMu.RLock()
	defer c.channelMu.RUnlock()
	tmpChannels := make([]Channel, len(c.pDeltaChannels))
	copy(tmpChannels, c.pDeltaChannels)
	return tmpChannels
}

func (c *Collection) getVDeltaChannels() []Channel {
	c.channelMu.RLock()
	defer c.channelMu.RUnlock()
	tmpChannels := make([]Channel, len(c.vDeltaChannels))
	copy(tmpChannels, c.vDeltaChannels)
	return tmpChannels
}

// addVChannels add virtual channels to collection
func (c *Collection) addVDeltaChannels(channels []Channel) {
	c.channelMu.Lock()
	defer c.channelMu.Unlock()
OUTER:
	for _, dstChan := range channels {
		for _, srcChan := range c.vDeltaChannels {
			if dstChan == srcChan {
				log.Debug("vDeltaChannel has been existed in collection's vDeltaChannels",
					zap.Int64("collectionID", c.ID()),
					zap.String("vChannel", dstChan),
				)
				continue OUTER
			}
		}
		log.Debug("add vDeltaChannel to collection",
			zap.Int64("collectionID", c.ID()),
			zap.String("vDeltaChannel", dstChan),
		)
		c.vDeltaChannels = append(c.vDeltaChannels, dstChan)
	}

	metrics.QueryNodeNumDeltaChannels.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.QueryNodeID)).Add(float64(len(c.vDeltaChannels)))
}

func (c *Collection) removeVDeltaChannel(channel Channel) {
	c.channelMu.Lock()
	defer c.channelMu.Unlock()
	tmpChannels := make([]Channel, 0)
	for _, vChannel := range c.vDeltaChannels {
		if channel != vChannel {
			tmpChannels = append(tmpChannels, vChannel)
		}
	}
	c.vDeltaChannels = tmpChannels
	log.Debug("remove vDeltaChannel from collection",
		zap.Int64("collectionID", c.ID()),
		zap.String("channel", channel),
	)

	metrics.QueryNodeNumDeltaChannels.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.QueryNodeID)).Sub(float64(len(c.vDeltaChannels)))
}

// setReleaseTime records when collection is released
func (c *Collection) setReleaseTime(t Timestamp) {
	c.releaseMu.Lock()
	defer c.releaseMu.Unlock()
	c.releaseTime = t
}

// getReleaseTime gets the time when collection is released
func (c *Collection) getReleaseTime() Timestamp {
	c.releaseMu.RLock()
	defer c.releaseMu.RUnlock()
	return c.releaseTime
}

// setLoadType set the loading type of collection, which is loadTypeCollection or loadTypePartition
func (c *Collection) setLoadType(l loadType) {
	c.loadType = l
}

// getLoadType get the loadType of collection, which is loadTypeCollection or loadTypePartition
func (c *Collection) getLoadType() loadType {
	return c.loadType
}

// newCollection returns a new Collection
func newCollection(collectionID UniqueID, schema *schemapb.CollectionSchema) *Collection {
	/*
		CCollection
		NewCollection(const char* schema_proto_blob);
	*/
	schemaBlob := proto.MarshalTextString(schema)

	cSchemaBlob := C.CString(schemaBlob)
	collection := C.NewCollection(cSchemaBlob)

	var newCollection = &Collection{
		collectionPtr:      collection,
		id:                 collectionID,
		schema:             schema,
		releasedPartitions: make(map[UniqueID]struct{}),
	}
	C.free(unsafe.Pointer(cSchemaBlob))

	log.Debug("create collection", zap.Int64("collectionID", collectionID))

	newCollection.setReleaseTime(Timestamp(math.MaxUint64))
	return newCollection
}

// deleteCollection delete collection and free the collection memory
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
