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

import (
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
	"github.com/milvus-io/milvus/internal/storage"
)

// retrieveOnSegments performs retrieve on listed segments
// all segment ids are validated before calling this function
func retrieveOnSegments(replica ReplicaInterface, collID UniqueID, plan *RetrievePlan, segIDs []UniqueID, vcm storage.ChunkManager) ([]*segcorepb.RetrieveResults, error) {
	var retrieveResults []*segcorepb.RetrieveResults

	for _, segID := range segIDs {
		seg, err := replica.getSegmentByID(segID)
		if err != nil {
			return nil, err
		}
		result, err := seg.retrieve(plan)
		if err != nil {
			return nil, err
		}
		if err := seg.fillIndexedFieldsData(collID, vcm, result); err != nil {
			return nil, err
		}
		retrieveResults = append(retrieveResults, result)
	}
	return retrieveResults, nil
}

// retrieveHistorical will retrieve all the target segments in historical
func retrieveHistorical(replica ReplicaInterface, plan *RetrievePlan, collID UniqueID, partIDs []UniqueID, segIDs []UniqueID, vcm storage.ChunkManager) ([]*segcorepb.RetrieveResults, []UniqueID, []UniqueID, error) {
	var err error
	var retrieveResults []*segcorepb.RetrieveResults
	var retrieveSegmentIDs []UniqueID
	var retrievePartIDs []UniqueID
	retrievePartIDs, retrieveSegmentIDs, err = validateOnHistoricalReplica(replica, collID, partIDs, segIDs)
	if err != nil {
		return retrieveResults, retrieveSegmentIDs, retrievePartIDs, err
	}

	retrieveResults, err = retrieveOnSegments(replica, collID, plan, retrieveSegmentIDs, vcm)
	return retrieveResults, retrievePartIDs, retrieveSegmentIDs, err
}

// retrieveStreaming will retrieve all the target segments in streaming
func retrieveStreaming(replica ReplicaInterface, plan *RetrievePlan, collID UniqueID, partIDs []UniqueID, vChannel Channel, vcm storage.ChunkManager) ([]*segcorepb.RetrieveResults, []UniqueID, []UniqueID, error) {
	var err error
	var retrieveResults []*segcorepb.RetrieveResults
	var retrievePartIDs []UniqueID
	var retrieveSegmentIDs []UniqueID

	retrievePartIDs, retrieveSegmentIDs, err = validateOnStreamReplica(replica, collID, partIDs, vChannel)
	if err != nil {
		return retrieveResults, retrieveSegmentIDs, retrievePartIDs, err
	}
	retrieveResults, err = retrieveOnSegments(replica, collID, plan, retrieveSegmentIDs, vcm)
	return retrieveResults, retrievePartIDs, retrieveSegmentIDs, err
}
