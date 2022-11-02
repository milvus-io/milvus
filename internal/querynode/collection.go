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
#cgo pkg-config: milvus_segcore

#include "segcore/collection_c.h"
#include "segcore/segment_c.h"
*/
import "C"
import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/milvus-io/milvus/internal/util/typeutil"

	"github.com/milvus-io/milvus/internal/metrics"

	"go.uber.org/zap"

	"github.com/golang/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/internal/log"
)

// Collection is a wrapper of the underlying C-structure C.CCollection
type Collection struct {
	mu            sync.RWMutex
	collectionPtr C.CCollection
	id            UniqueID
	partitionIDs  []UniqueID
	schema        *schemapb.CollectionSchema

	// TODO, remove delta channels
	channelMu      sync.RWMutex
	vChannels      []Channel
	pChannels      []Channel
	vDeltaChannels []Channel
	pDeltaChannels []Channel

	loadType int32

	releasedPartitions map[UniqueID]struct{}
	markDestroyed      bool
}

// ID returns collection id
func (c *Collection) ID() UniqueID {
	if c == nil {
		return 0
	}
	return c.id
}

// Schema returns the schema of collection
func (c *Collection) Schema() *schemapb.CollectionSchema {
	return c.schema
}

// getPartitionIDs return partitionIDs of collection
// it only called by meta_replica, no need to add lock
func (c *Collection) getPartitionIDs() []UniqueID {
	dst := make([]UniqueID, len(c.partitionIDs))
	copy(dst, c.partitionIDs)
	return dst
}

// addPartitionID would add a partition id to partition id list of collection
// it only called by meta_replica, no need to add lock
func (c *Collection) addPartitionID(partitionID UniqueID) {
	c.partitionIDs = append(c.partitionIDs, partitionID)
	log.Info("queryNode collection info after add a partition",
		zap.Int64("partitionID", partitionID), zap.Int64("collectionID", c.id),
		zap.Int64s("partitions", c.partitionIDs))
}

// removePartitionID removes the partition id from partition id list of collection
// it only called by meta_replica, no need to add lock
func (c *Collection) removePartitionID(partitionID UniqueID) {
	tmpIDs := make([]UniqueID, 0, len(c.partitionIDs))
	for _, id := range c.partitionIDs {
		if id != partitionID {
			tmpIDs = append(tmpIDs, id)
		}
	}
	c.partitionIDs = tmpIDs
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
	log.Info("remove vChannel from collection",
		zap.Int64("collectionID", c.ID()),
		zap.String("channel", channel),
	)

	metrics.QueryNodeNumDmlChannels.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID())).Sub(float64(len(c.vChannels)))
}

// getPChannels get physical channels of collection
func (c *Collection) getPChannels() []Channel {
	c.channelMu.RLock()
	defer c.channelMu.RUnlock()
	tmpChannels := make([]Channel, len(c.pChannels))
	copy(tmpChannels, c.pChannels)
	return tmpChannels
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

func (c *Collection) AddChannels(toLoadChannels []Channel, VPChannels map[string]string) []Channel {
	c.channelMu.Lock()
	defer c.channelMu.Unlock()

	retVChannels := []Channel{}
	for _, toLoadChannel := range toLoadChannels {
		if !c.isVChannelExist(toLoadChannel) {
			retVChannels = append(retVChannels, toLoadChannel)
			c.vChannels = append(c.vChannels, toLoadChannel)
			if !c.isPChannelExist(VPChannels[toLoadChannel]) {
				c.pChannels = append(c.pChannels, VPChannels[toLoadChannel])
			}
		}
	}
	return retVChannels
}

func (c *Collection) isVChannelExist(channel string) bool {
	for _, vChannel := range c.vChannels {
		if vChannel == channel {
			return true
		}
	}
	return false
}

func (c *Collection) isPChannelExist(channel string) bool {
	for _, vChannel := range c.pChannels {
		if vChannel == channel {
			return true
		}
	}
	return false
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
	log.Info("remove vDeltaChannel from collection",
		zap.Int64("collectionID", c.ID()),
		zap.String("channel", channel),
	)

	metrics.QueryNodeNumDeltaChannels.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID())).Sub(float64(len(c.vDeltaChannels)))
}

func (c *Collection) AddVDeltaChannels(toLoadChannels []Channel, VPChannels map[string]string) []Channel {
	c.channelMu.Lock()
	defer c.channelMu.Unlock()

	retVDeltaChannels := []Channel{}
	for _, toLoadChannel := range toLoadChannels {
		if !c.isVDeltaChannelExist(toLoadChannel) {
			retVDeltaChannels = append(retVDeltaChannels, toLoadChannel)
			c.vDeltaChannels = append(c.vDeltaChannels, toLoadChannel)
			if !c.isPDeltaChannelExist(VPChannels[toLoadChannel]) {
				c.pDeltaChannels = append(c.pDeltaChannels, VPChannels[toLoadChannel])
			}
		}
	}
	return retVDeltaChannels
}

func (c *Collection) isVDeltaChannelExist(channel string) bool {
	for _, vDeltaChanel := range c.vDeltaChannels {
		if vDeltaChanel == channel {
			return true
		}
	}
	return false
}

func (c *Collection) isPDeltaChannelExist(channel string) bool {
	for _, vChannel := range c.pDeltaChannels {
		if vChannel == channel {
			return true
		}
	}
	return false
}

// MarkDestroyed mark the collection to be/ has been destroyed
func (c *Collection) MarkDestroyed() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.markDestroyed = true
}

// healthyCheck check whether the collection is healthy
func (c *Collection) healthyCheck() bool {
	return !c.markDestroyed && c.collectionPtr != nil
}

// IsHealthy get whether the collection is healthy
func (c *Collection) IsHealthy() bool {
	if c == nil {
		return false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.healthyCheck()
}

// setLoadType set the loading type of collection, which is loadTypeCollection or loadTypePartition
func (c *Collection) setLoadType(l loadType) {
	atomic.StoreInt32(&c.loadType, int32(l))
}

// getLoadType get the loadType of collection, which is loadTypeCollection or loadTypePartition
func (c *Collection) getLoadType() loadType {
	l := atomic.LoadInt32(&c.loadType)
	return loadType(l)
}

// getFieldType get the field type according to the field id.
func (c *Collection) getFieldType(fieldID FieldID) (schemapb.DataType, error) {
	helper, err := typeutil.CreateSchemaHelper(c.schema)
	if err != nil {
		return schemapb.DataType_None, err
	}
	field, err := helper.GetFieldFromID(fieldID)
	if err != nil {
		return schemapb.DataType_None, err
	}
	return field.GetDataType(), nil
}

// Clear free the collection memory
func (c *Collection) Clear() {
	defer func() {
		log.Info("delete collection", zap.Int64("collectionID", c.ID()))
	}()

	if c == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.markDestroyed = true

	/*
		void
		deleteCollection(CCollection collection);
	*/
	if c.collectionPtr == nil {
		return
	}

	C.DeleteCollection(c.collectionPtr)
	c.collectionPtr = nil
}

// createSearchPlan returns a new SearchPlan and error
func (c *Collection) createSearchPlan(dsl string) (*SearchPlan, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if !c.healthyCheck() {
		return nil, errors.New("unhealthy collection, collectionID = " + fmt.Sprintln(c.ID()))
	}

	cDsl := C.CString(dsl)
	defer C.free(unsafe.Pointer(cDsl))
	var cPlan C.CSearchPlan
	status := C.CreateSearchPlan(c.collectionPtr, cDsl, &cPlan)

	err1 := HandleCStatus(&status, "Create Plan failed")
	if err1 != nil {
		return nil, err1
	}

	var newPlan = &SearchPlan{cSearchPlan: cPlan}
	return newPlan, nil
}

func (c *Collection) createSearchPlanByExpr(expr []byte) (*SearchPlan, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if !c.healthyCheck() {
		return nil, errors.New("unhealthy collection, collectionID = " + fmt.Sprintln(c.ID()))
	}
	var cPlan C.CSearchPlan
	status := C.CreateSearchPlanByExpr(c.collectionPtr, unsafe.Pointer(&expr[0]), (C.int64_t)(len(expr)), &cPlan)

	err1 := HandleCStatus(&status, "Create Plan by expr failed")
	if err1 != nil {
		return nil, err1
	}

	var newPlan = &SearchPlan{cSearchPlan: cPlan}
	return newPlan, nil
}

func (c *Collection) createRetrievePlanByExpr(expr []byte, timestamp Timestamp, msgID UniqueID) (*RetrievePlan, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if !c.healthyCheck() {
		return nil, errors.New("unhealthy collection, collectionID = " + fmt.Sprintln(c.ID()))
	}

	var cPlan C.CRetrievePlan
	status := C.CreateRetrievePlanByExpr(c.collectionPtr, unsafe.Pointer(&expr[0]), (C.int64_t)(len(expr)), &cPlan)

	err := HandleCStatus(&status, "Create retrieve plan by expr failed")
	if err != nil {
		return nil, err
	}

	var newPlan = &RetrievePlan{
		cRetrievePlan: cPlan,
		Timestamp:     timestamp,
		msgID:         msgID,
	}
	return newPlan, nil
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

	log.Info("create collection", zap.Int64("collectionID", collectionID))
	return newCollection
}
