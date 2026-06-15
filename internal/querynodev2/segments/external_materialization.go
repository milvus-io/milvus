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

package segments

import (
	"strconv"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/externalmaterialization"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func ValidateExternalMaterializedFields(schema *schemapb.CollectionSchema, loadInfos []*querypb.SegmentLoadInfo, fieldIDs []int64) error {
	if schema == nil || len(fieldIDs) == 0 || !typeutil.IsExternalCollection(schema) {
		return nil
	}
	fieldIDs = externalFieldIDs(schema, fieldIDs)
	if len(fieldIDs) == 0 {
		return nil
	}

	for _, loadInfo := range loadInfos {
		missingFields := externalmaterialization.MissingFieldsForSegment(
			schema.GetFields(),
			segmentInfoFromLoadInfo(loadInfo),
			fieldIDs,
		)
		if len(missingFields) == 0 {
			continue
		}

		fieldName := missingFields[0].FieldName
		if fieldName == "" {
			fieldName = strconv.FormatInt(missingFields[0].FieldID, 10)
		}
		return merr.WrapErrParameterInvalidMsg("external field %s is not materialized; run RefreshExternalCollection", fieldName)
	}

	return nil
}

func LoadInfosFromSegments(segmentList []Segment) []*querypb.SegmentLoadInfo {
	loadInfos := make([]*querypb.SegmentLoadInfo, 0, len(segmentList))
	for _, segment := range segmentList {
		if segment == nil {
			continue
		}
		loadInfos = append(loadInfos, segment.LoadInfo())
	}
	return loadInfos
}

func RetrieveRequiredFieldIDs(req *querypb.QueryRequest) []int64 {
	if req == nil || req.GetReq() == nil {
		return nil
	}

	fields := typeutil.NewSet[int64]()
	insertFieldIDs := func(fieldIDs ...int64) {
		for _, fieldID := range fieldIDs {
			if fieldID <= 0 {
				continue
			}
			fields.Insert(fieldID)
		}
	}

	retrieveReq := req.GetReq()
	insertFieldIDs(retrieveReq.GetOutputFieldsId()...)
	insertFieldIDs(retrieveReq.GetGroupByFieldIds()...)
	for _, aggregate := range retrieveReq.GetAggregates() {
		insertFieldIDs(aggregate.GetFieldId())
	}
	for _, orderBy := range retrieveReq.GetOrderByFields() {
		insertFieldIDs(orderBy.GetFieldId())
	}

	return fields.Collect()
}

func SearchRequiredFieldIDs(searchReq *SearchRequest, req *querypb.SearchRequest) []int64 {
	fields := typeutil.NewSet[int64]()
	insertFieldIDs := func(fieldIDs ...int64) {
		for _, fieldID := range fieldIDs {
			if fieldID <= 0 {
				continue
			}
			fields.Insert(fieldID)
		}
	}

	if searchReq != nil {
		insertFieldIDs(searchReq.SearchFieldID())
	}

	searchReqPB := req.GetReq()
	if searchReqPB != nil {
		insertFieldIDs(searchReqPB.GetOutputFieldsId()...)
		insertFieldIDs(searchReqPB.GetGroupByFieldId(), searchReqPB.GetFieldId())
		insertFieldIDs(searchReqPB.GetGroupByFieldIds()...)
		for _, subReq := range searchReqPB.GetSubReqs() {
			insertFieldIDs(subReq.GetGroupByFieldId(), subReq.GetFieldId())
		}
	}

	return fields.Collect()
}

func validateExternalMaterializedFieldsForCollection(mgr *Manager, collectionID int64, segmentList []Segment, fieldIDs []int64) error {
	if len(fieldIDs) == 0 {
		return nil
	}
	if AllowUnmaterializedExternalFieldAccess() {
		return nil
	}
	if mgr == nil || mgr.Collection == nil {
		return merr.WrapErrCollectionNotFound(collectionID)
	}

	collection := mgr.Collection.Get(collectionID)
	if collection == nil {
		return merr.WrapErrCollectionNotFound(collectionID)
	}

	return ValidateExternalMaterializedFields(collection.Schema(), LoadInfosFromSegments(segmentList), fieldIDs)
}

func AllowUnmaterializedExternalFieldAccess() bool {
	return paramtable.Get().QueryNodeCfg.ExternalCollectionAllowUnmaterializedFieldAccess.GetAsBool()
}

func segmentInfoFromLoadInfo(loadInfo *querypb.SegmentLoadInfo) *datapb.SegmentInfo {
	return &datapb.SegmentInfo{
		ID:              loadInfo.GetSegmentID(),
		CollectionID:    loadInfo.GetCollectionID(),
		PartitionID:     loadInfo.GetPartitionID(),
		InsertChannel:   loadInfo.GetInsertChannel(),
		NumOfRows:       loadInfo.GetNumOfRows(),
		Binlogs:         loadInfo.GetBinlogPaths(),
		Statslogs:       loadInfo.GetStatslogs(),
		Deltalogs:       loadInfo.GetDeltalogs(),
		CompactionFrom:  loadInfo.GetCompactionFrom(),
		Level:           loadInfo.GetLevel(),
		StorageVersion:  loadInfo.GetStorageVersion(),
		IsSorted:        loadInfo.GetIsSorted(),
		TextStatsLogs:   loadInfo.GetTextStatsLogs(),
		Bm25Statslogs:   loadInfo.GetBm25Logs(),
		JsonKeyStats:    loadInfo.GetJsonKeyStatsLogs(),
		ManifestPath:    loadInfo.GetManifestPath(),
		DataVersion:     loadInfo.GetDataVersion(),
		CommitTimestamp: loadInfo.GetCommitTimestamp(),
	}
}

func externalFieldIDs(schema *schemapb.CollectionSchema, fieldIDs []int64) []int64 {
	externalFields := typeutil.NewSet[int64]()
	knownFields := typeutil.NewSet[int64]()
	for _, field := range schema.GetFields() {
		knownFields.Insert(field.GetFieldID())
		if field.GetExternalField() == "" && !field.GetIsFunctionOutput() && field.GetDataType() != schemapb.DataType_SparseFloatVector {
			continue
		}
		externalFields.Insert(field.GetFieldID())
	}

	filtered := typeutil.NewSet[int64]()
	for _, fieldID := range fieldIDs {
		if externalFields.Contain(fieldID) || (!knownFields.Contain(fieldID) && fieldID >= common.StartOfUserFieldID) {
			filtered.Insert(fieldID)
		}
	}
	return filtered.Collect()
}
