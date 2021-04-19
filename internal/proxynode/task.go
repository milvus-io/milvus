package proxynode

import (
	"context"
	"errors"
	"log"
	"math"
	"strconv"

	"github.com/opentracing/opentracing-go"
	oplog "github.com/opentracing/opentracing-go/log"

	"github.com/golang/protobuf/proto"
	"github.com/zilliztech/milvus-distributed/internal/allocator"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/proto/masterpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
	"github.com/zilliztech/milvus-distributed/internal/proto/servicepb"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

type task interface {
	ID() UniqueID       // return ReqID
	SetID(uid UniqueID) // set ReqID
	Type() commonpb.MsgType
	BeginTs() Timestamp
	EndTs() Timestamp
	SetTs(ts Timestamp)
	PreExecute() error
	Execute() error
	PostExecute() error
	WaitToFinish() error
	Notify(err error)
}

type BaseInsertTask = msgstream.InsertMsg

type InsertTask struct {
	BaseInsertTask
	Condition
	result                *servicepb.IntegerRangeResponse
	manipulationMsgStream *msgstream.PulsarMsgStream
	ctx                   context.Context
	rowIDAllocator        *allocator.IDAllocator
}

func (it *InsertTask) SetID(uid UniqueID) {
	it.Base.MsgID = uid
}

func (it *InsertTask) SetTs(ts Timestamp) {
	rowNum := len(it.RowData)
	it.Timestamps = make([]uint64, rowNum)
	for index := range it.Timestamps {
		it.Timestamps[index] = ts
	}
	it.BeginTimestamp = ts
	it.EndTimestamp = ts
}

func (it *InsertTask) BeginTs() Timestamp {
	return it.BeginTimestamp
}

func (it *InsertTask) EndTs() Timestamp {
	return it.EndTimestamp
}

func (it *InsertTask) ID() UniqueID {
	return it.Base.MsgID
}

func (it *InsertTask) Type() commonpb.MsgType {
	return it.Base.MsgType
}

func (it *InsertTask) PreExecute() error {
	span, ctx := opentracing.StartSpanFromContext(it.ctx, "InsertTask preExecute")
	defer span.Finish()
	it.ctx = ctx
	span.SetTag("hash keys", it.Base.MsgID)
	span.SetTag("start time", it.BeginTs())
	collectionName := it.BaseInsertTask.CollectionName
	if err := ValidateCollectionName(collectionName); err != nil {
		span.LogFields(oplog.Error(err))
		span.Finish()
		return err
	}
	partitionTag := it.BaseInsertTask.PartitionName
	if err := ValidatePartitionTag(partitionTag, true); err != nil {
		span.LogFields(oplog.Error(err))
		span.Finish()
		return err
	}

	return nil
}

func (it *InsertTask) Execute() error {
	span, ctx := opentracing.StartSpanFromContext(it.ctx, "InsertTask Execute")
	defer span.Finish()
	it.ctx = ctx
	span.SetTag("hash keys", it.Base.MsgID)
	span.SetTag("start time", it.BeginTs())
	collectionName := it.BaseInsertTask.CollectionName
	span.LogFields(oplog.String("collection_name", collectionName))
	if !globalMetaCache.Hit(collectionName) {
		err := globalMetaCache.Sync(collectionName)
		if err != nil {
			span.LogFields(oplog.Error(err))
			span.Finish()
			return err
		}
	}
	description, err := globalMetaCache.Get(collectionName)
	if err != nil || description == nil {
		span.LogFields(oplog.Error(err))
		span.Finish()
		return err
	}
	autoID := description.Schema.AutoID
	span.LogFields(oplog.Bool("auto_id", autoID))
	var rowIDBegin UniqueID
	var rowIDEnd UniqueID
	rowNums := len(it.BaseInsertTask.RowData)
	rowIDBegin, rowIDEnd, _ = it.rowIDAllocator.Alloc(uint32(rowNums))
	span.LogFields(oplog.Int("rowNums", rowNums),
		oplog.Int("rowIDBegin", int(rowIDBegin)),
		oplog.Int("rowIDEnd", int(rowIDEnd)))
	it.BaseInsertTask.RowIDs = make([]UniqueID, rowNums)
	for i := rowIDBegin; i < rowIDEnd; i++ {
		offset := i - rowIDBegin
		it.BaseInsertTask.RowIDs[offset] = i
	}

	if autoID {
		if it.HashValues == nil || len(it.HashValues) == 0 {
			it.HashValues = make([]uint32, 0)
		}
		for _, rowID := range it.RowIDs {
			hashValue, _ := typeutil.Hash32Int64(rowID)
			it.HashValues = append(it.HashValues, hashValue)
		}
	}

	var tsMsg msgstream.TsMsg = &it.BaseInsertTask
	msgPack := &msgstream.MsgPack{
		BeginTs: it.BeginTs(),
		EndTs:   it.EndTs(),
		Msgs:    make([]msgstream.TsMsg, 1),
	}
	tsMsg.SetMsgContext(ctx)
	span.LogFields(oplog.String("send msg", "send msg"))
	msgPack.Msgs[0] = tsMsg
	err = it.manipulationMsgStream.Produce(msgPack)

	it.result = &servicepb.IntegerRangeResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
		},
		Begin: rowIDBegin,
		End:   rowIDEnd,
	}
	if err != nil {
		it.result.Status.ErrorCode = commonpb.ErrorCode_UNEXPECTED_ERROR
		it.result.Status.Reason = err.Error()
		span.LogFields(oplog.Error(err))
	}
	return nil
}

func (it *InsertTask) PostExecute() error {
	span, _ := opentracing.StartSpanFromContext(it.ctx, "InsertTask postExecute")
	defer span.Finish()
	return nil
}

type CreateCollectionTask struct {
	Condition
	milvuspb.CreateCollectionRequest
	masterClient masterpb.MasterServiceClient
	result       *commonpb.Status
	ctx          context.Context
	schema       *schemapb.CollectionSchema
}

func (cct *CreateCollectionTask) ID() UniqueID {
	return cct.Base.MsgID
}

func (cct *CreateCollectionTask) SetID(uid UniqueID) {
	cct.Base.MsgID = uid
}

func (cct *CreateCollectionTask) Type() commonpb.MsgType {
	return cct.Base.MsgType
}

func (cct *CreateCollectionTask) BeginTs() Timestamp {
	return cct.Base.Timestamp
}

func (cct *CreateCollectionTask) EndTs() Timestamp {
	return cct.Base.Timestamp
}

func (cct *CreateCollectionTask) SetTs(ts Timestamp) {
	cct.Base.Timestamp = ts
}

func (cct *CreateCollectionTask) PreExecute() error {
	if int64(len(cct.schema.Fields)) > Params.MaxFieldNum() {
		return errors.New("maximum field's number should be limited to " + strconv.FormatInt(Params.MaxFieldNum(), 10))
	}

	// validate collection name
	if err := ValidateCollectionName(cct.schema.Name); err != nil {
		return err
	}

	if err := ValidateDuplicatedFieldName(cct.schema.Fields); err != nil {
		return err
	}

	if err := ValidatePrimaryKey(cct.schema); err != nil {
		return err
	}

	// validate field name
	for _, field := range cct.schema.Fields {
		if err := ValidateFieldName(field.Name); err != nil {
			return err
		}
		if field.DataType == schemapb.DataType_VECTOR_FLOAT || field.DataType == schemapb.DataType_VECTOR_BINARY {
			exist := false
			var dim int64 = 0
			for _, param := range field.TypeParams {
				if param.Key == "dim" {
					exist = true
					tmp, err := strconv.ParseInt(param.Value, 10, 64)
					if err != nil {
						return err
					}
					dim = tmp
					break
				}
			}
			if !exist {
				return errors.New("dimension is not defined in field type params")
			}
			if field.DataType == schemapb.DataType_VECTOR_FLOAT {
				if err := ValidateDimension(dim, false); err != nil {
					return err
				}
			} else {
				if err := ValidateDimension(dim, true); err != nil {
					return err
				}
			}
		}
		if err := ValidateVectorFieldMetricType(field); err != nil {
			return err
		}
	}

	return nil
}

func (cct *CreateCollectionTask) Execute() error {
	schemaBytes, _ := proto.Marshal(cct.schema)
	cct.CreateCollectionRequest.Schema = schemaBytes
	resp, err := cct.masterClient.CreateCollection(cct.ctx, &cct.CreateCollectionRequest)
	if err != nil {
		log.Printf("create collection failed, error= %v", err)
		cct.result = &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    err.Error(),
		}
	} else {
		cct.result = resp
	}
	return err
}

func (cct *CreateCollectionTask) PostExecute() error {
	return nil
}

type DropCollectionTask struct {
	Condition
	milvuspb.DropCollectionRequest
	masterClient masterpb.MasterServiceClient
	result       *commonpb.Status
	ctx          context.Context
}

func (dct *DropCollectionTask) ID() UniqueID {
	return dct.Base.MsgID
}

func (dct *DropCollectionTask) SetID(uid UniqueID) {
	dct.Base.MsgID = uid
}

func (dct *DropCollectionTask) Type() commonpb.MsgType {
	return dct.Base.MsgType
}

func (dct *DropCollectionTask) BeginTs() Timestamp {
	return dct.Base.Timestamp
}

func (dct *DropCollectionTask) EndTs() Timestamp {
	return dct.Base.Timestamp
}

func (dct *DropCollectionTask) SetTs(ts Timestamp) {
	dct.Base.Timestamp = ts
}

func (dct *DropCollectionTask) PreExecute() error {
	if err := ValidateCollectionName(dct.CollectionName); err != nil {
		return err
	}
	return nil
}

func (dct *DropCollectionTask) Execute() error {
	resp, err := dct.masterClient.DropCollection(dct.ctx, &dct.DropCollectionRequest)
	if err != nil {
		log.Printf("drop collection failed, error= %v", err)
		dct.result = &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    err.Error(),
		}
	} else {
		dct.result = resp
	}
	return err
}

func (dct *DropCollectionTask) PostExecute() error {
	if globalMetaCache.Hit(dct.CollectionName) {
		return globalMetaCache.Remove(dct.CollectionName)
	}
	return nil
}

type SearchTask struct {
	Condition
	internalpb2.SearchRequest
	queryMsgStream *msgstream.PulsarMsgStream
	resultBuf      chan []*internalpb2.SearchResults
	result         *servicepb.QueryResult
	ctx            context.Context
	query          *servicepb.Query
}

func (st *SearchTask) ID() UniqueID {
	return st.Base.MsgID
}

func (st *SearchTask) SetID(uid UniqueID) {
	st.Base.MsgID = uid
}

func (st *SearchTask) Type() commonpb.MsgType {
	return st.Base.MsgType
}

func (st *SearchTask) BeginTs() Timestamp {
	return st.Base.Timestamp
}

func (st *SearchTask) EndTs() Timestamp {
	return st.Base.Timestamp
}

func (st *SearchTask) SetTs(ts Timestamp) {
	st.Base.Timestamp = ts
}

func (st *SearchTask) PreExecute() error {
	span, ctx := opentracing.StartSpanFromContext(st.ctx, "SearchTask preExecute")
	defer span.Finish()
	st.ctx = ctx
	span.SetTag("hash keys", st.Base.MsgID)
	span.SetTag("start time", st.BeginTs())

	collectionName := st.query.CollectionName
	if !globalMetaCache.Hit(collectionName) {
		err := globalMetaCache.Sync(collectionName)
		if err != nil {
			span.LogFields(oplog.Error(err))
			span.Finish()
			return err
		}
	}
	_, err := globalMetaCache.Get(collectionName)
	if err != nil { // err is not nil if collection not exists
		span.LogFields(oplog.Error(err))
		span.Finish()
		return err
	}

	if err := ValidateCollectionName(st.query.CollectionName); err != nil {
		span.LogFields(oplog.Error(err))
		span.Finish()
		return err
	}

	for _, tag := range st.query.PartitionTags {
		if err := ValidatePartitionTag(tag, false); err != nil {
			span.LogFields(oplog.Error(err))
			span.Finish()
			return err
		}
	}
	st.Base.MsgType = commonpb.MsgType_kSearch
	queryBytes, err := proto.Marshal(st.query)
	if err != nil {
		span.LogFields(oplog.Error(err))
		span.Finish()
		return err
	}
	st.Query = &commonpb.Blob{
		Value: queryBytes,
	}
	return nil
}

func (st *SearchTask) Execute() error {
	span, ctx := opentracing.StartSpanFromContext(st.ctx, "SearchTask Execute")
	defer span.Finish()
	st.ctx = ctx
	span.SetTag("hash keys", st.Base.MsgID)
	span.SetTag("start time", st.BeginTs())
	var tsMsg msgstream.TsMsg = &msgstream.SearchMsg{
		SearchRequest: st.SearchRequest,
		BaseMsg: msgstream.BaseMsg{
			HashValues:     []uint32{uint32(Params.ProxyID())},
			BeginTimestamp: st.Base.Timestamp,
			EndTimestamp:   st.Base.Timestamp,
		},
	}
	msgPack := &msgstream.MsgPack{
		BeginTs: st.Base.Timestamp,
		EndTs:   st.Base.Timestamp,
		Msgs:    make([]msgstream.TsMsg, 1),
	}
	tsMsg.SetMsgContext(ctx)
	msgPack.Msgs[0] = tsMsg
	err := st.queryMsgStream.Produce(msgPack)
	log.Printf("[Proxy] length of searchMsg: %v", len(msgPack.Msgs))
	if err != nil {
		span.LogFields(oplog.Error(err))
		span.Finish()
		log.Printf("[Proxy] send search request failed: %v", err)
	}
	return err
}

func (st *SearchTask) PostExecute() error {
	span, _ := opentracing.StartSpanFromContext(st.ctx, "SearchTask postExecute")
	defer span.Finish()
	span.SetTag("hash keys", st.Base.MsgID)
	span.SetTag("start time", st.BeginTs())
	for {
		select {
		case <-st.ctx.Done():
			log.Print("wait to finish failed, timeout!")
			span.LogFields(oplog.String("wait to finish failed, timeout", "wait to finish failed, timeout"))
			return errors.New("wait to finish failed, timeout")
		case searchResults := <-st.resultBuf:
			span.LogFields(oplog.String("receive result", "receive result"))
			filterSearchResult := make([]*internalpb2.SearchResults, 0)
			var filterReason string
			for _, partialSearchResult := range searchResults {
				if partialSearchResult.Status.ErrorCode == commonpb.ErrorCode_SUCCESS {
					filterSearchResult = append(filterSearchResult, partialSearchResult)
					// For debugging, please don't delete.
					//for i := 0; i < len(partialSearchResult.Hits); i++ {
					//	testHits := milvuspb.Hits{}
					//	err := proto.Unmarshal(partialSearchResult.Hits[i], &testHits)
					//	if err != nil {
					//		panic(err)
					//	}
					//	fmt.Println(testHits.IDs)
					//	fmt.Println(testHits.Scores)
					//}
				} else {
					filterReason += partialSearchResult.Status.Reason + "\n"
				}
			}

			availableQueryNodeNum := len(filterSearchResult)
			if availableQueryNodeNum <= 0 {
				st.result = &servicepb.QueryResult{
					Status: &commonpb.Status{
						ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
						Reason:    filterReason,
					},
				}
				span.LogFields(oplog.Error(errors.New(filterReason)))
				return errors.New(filterReason)
			}

			hits := make([][]*servicepb.Hits, 0)
			for _, partialSearchResult := range filterSearchResult {
				if partialSearchResult.Hits == nil || len(partialSearchResult.Hits) <= 0 {
					filterReason += "nq is zero\n"
					continue
				}
				partialHits := make([]*servicepb.Hits, 0)
				for _, bs := range partialSearchResult.Hits {
					partialHit := &servicepb.Hits{}
					err := proto.Unmarshal(bs, partialHit)
					if err != nil {
						log.Println("unmarshal error")
						return err
					}
					partialHits = append(partialHits, partialHit)
				}
				hits = append(hits, partialHits)
			}

			availableQueryNodeNum = len(hits)
			if availableQueryNodeNum <= 0 {
				st.result = &servicepb.QueryResult{
					Status: &commonpb.Status{
						ErrorCode: commonpb.ErrorCode_SUCCESS,
						Reason:    filterReason,
					},
				}
				return nil
			}

			nq := len(hits[0])
			if nq <= 0 {
				st.result = &servicepb.QueryResult{
					Status: &commonpb.Status{
						ErrorCode: commonpb.ErrorCode_SUCCESS,
						Reason:    filterReason,
					},
				}
				return nil
			}

			topk := 0
			getMax := func(a, b int) int {
				if a > b {
					return a
				}
				return b
			}
			for _, hit := range hits {
				topk = getMax(topk, len(hit[0].IDs))
			}
			st.result = &servicepb.QueryResult{
				Status: &commonpb.Status{
					ErrorCode: 0,
				},
				Hits: make([][]byte, 0),
			}

			const minFloat32 = -1 * float32(math.MaxFloat32)
			for i := 0; i < nq; i++ {
				locs := make([]int, availableQueryNodeNum)
				reducedHits := &servicepb.Hits{
					IDs:     make([]int64, 0),
					RowData: make([][]byte, 0),
					Scores:  make([]float32, 0),
				}

				for j := 0; j < topk; j++ {
					valid := false
					choice, maxDistance := 0, minFloat32
					for q, loc := range locs { // query num, the number of ways to merge
						if loc >= len(hits[q][i].IDs) {
							continue
						}
						distance := hits[q][i].Scores[loc]
						if distance > maxDistance || (math.Abs(float64(distance-maxDistance)) < math.SmallestNonzeroFloat32 && choice != q) {
							choice = q
							maxDistance = distance
							valid = true
						}
					}
					if !valid {
						break
					}
					choiceOffset := locs[choice]
					// check if distance is valid, `invalid` here means very very big,
					// in this process, distance here is the smallest, so the rest of distance are all invalid
					if hits[choice][i].Scores[choiceOffset] <= minFloat32 {
						break
					}
					reducedHits.IDs = append(reducedHits.IDs, hits[choice][i].IDs[choiceOffset])
					if hits[choice][i].RowData != nil && len(hits[choice][i].RowData) > 0 {
						reducedHits.RowData = append(reducedHits.RowData, hits[choice][i].RowData[choiceOffset])
					}
					reducedHits.Scores = append(reducedHits.Scores, hits[choice][i].Scores[choiceOffset])
					locs[choice]++
				}
				if searchResults[0].MetricType != "IP" {
					for k := range reducedHits.Scores {
						reducedHits.Scores[k] *= -1
					}
				}
				reducedHitsBs, err := proto.Marshal(reducedHits)
				if err != nil {
					log.Println("marshal error")
					span.LogFields(oplog.Error(err))
					return err
				}
				st.result.Hits = append(st.result.Hits, reducedHitsBs)
			}
			return nil
		}
	}
}

type HasCollectionTask struct {
	Condition
	milvuspb.HasCollectionRequest
	masterClient masterpb.MasterServiceClient
	result       *servicepb.BoolResponse
	ctx          context.Context
}

func (hct *HasCollectionTask) ID() UniqueID {
	return hct.Base.MsgID
}

func (hct *HasCollectionTask) SetID(uid UniqueID) {
	hct.Base.MsgID = uid
}

func (hct *HasCollectionTask) Type() commonpb.MsgType {
	return hct.Base.MsgType
}

func (hct *HasCollectionTask) BeginTs() Timestamp {
	return hct.Base.Timestamp
}

func (hct *HasCollectionTask) EndTs() Timestamp {
	return hct.Base.Timestamp
}

func (hct *HasCollectionTask) SetTs(ts Timestamp) {
	hct.Base.Timestamp = ts
}

func (hct *HasCollectionTask) PreExecute() error {
	if err := ValidateCollectionName(hct.CollectionName); err != nil {
		return err
	}
	return nil
}

func (hct *HasCollectionTask) Execute() error {
	resp, err := hct.masterClient.HasCollection(hct.ctx, &hct.HasCollectionRequest)
	if err != nil {
		log.Printf("has collection failed, error= %v", err)
		hct.result = &servicepb.BoolResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    "internal error",
			},
			Value: false,
		}
	} else {
		hct.result = &servicepb.BoolResponse{
			Status: resp.Status,
			Value:  resp.Value,
		}
	}
	return err
}

func (hct *HasCollectionTask) PostExecute() error {
	return nil
}

type DescribeCollectionTask struct {
	Condition
	milvuspb.DescribeCollectionRequest
	masterClient masterpb.MasterServiceClient
	result       *servicepb.CollectionDescription
	ctx          context.Context
}

func (dct *DescribeCollectionTask) ID() UniqueID {
	return dct.Base.MsgID
}

func (dct *DescribeCollectionTask) SetID(uid UniqueID) {
	dct.Base.MsgID = uid
}

func (dct *DescribeCollectionTask) Type() commonpb.MsgType {
	return dct.Base.MsgType
}

func (dct *DescribeCollectionTask) BeginTs() Timestamp {
	return dct.Base.Timestamp
}

func (dct *DescribeCollectionTask) EndTs() Timestamp {
	return dct.Base.Timestamp
}

func (dct *DescribeCollectionTask) SetTs(ts Timestamp) {
	dct.Base.Timestamp = ts
}

func (dct *DescribeCollectionTask) PreExecute() error {
	if err := ValidateCollectionName(dct.CollectionName); err != nil {
		return err
	}
	return nil
}

func (dct *DescribeCollectionTask) Execute() error {
	result, err := dct.masterClient.DescribeCollection(dct.ctx, &dct.DescribeCollectionRequest)
	if err != nil {
		return err
	}
	dct.result = &servicepb.CollectionDescription{
		Status: result.Status,
		Schema: result.Schema,
	}
	err = globalMetaCache.Update(dct.CollectionName, dct.result)
	return err
}

func (dct *DescribeCollectionTask) PostExecute() error {
	return nil
}

type ShowCollectionsTask struct {
	Condition
	milvuspb.ShowCollectionRequest
	masterClient masterpb.MasterServiceClient
	result       *servicepb.StringListResponse
	ctx          context.Context
}

func (sct *ShowCollectionsTask) ID() UniqueID {
	return sct.Base.MsgID
}

func (sct *ShowCollectionsTask) SetID(uid UniqueID) {
	sct.Base.MsgID = uid
}

func (sct *ShowCollectionsTask) Type() commonpb.MsgType {
	return sct.Base.MsgType
}

func (sct *ShowCollectionsTask) BeginTs() Timestamp {
	return sct.Base.Timestamp
}

func (sct *ShowCollectionsTask) EndTs() Timestamp {
	return sct.Base.Timestamp
}

func (sct *ShowCollectionsTask) SetTs(ts Timestamp) {
	sct.Base.Timestamp = ts
}

func (sct *ShowCollectionsTask) PreExecute() error {
	return nil
}

func (sct *ShowCollectionsTask) Execute() error {
	resp, err := sct.masterClient.ShowCollections(sct.ctx, &sct.ShowCollectionRequest)
	if err != nil {
		log.Printf("show collections failed, error= %v", err)
		sct.result = &servicepb.StringListResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    "internal error",
			},
		}
	} else {
		sct.result = &servicepb.StringListResponse{
			Status: resp.Status,
			Values: resp.CollectionNames,
		}
	}
	return err
}

func (sct *ShowCollectionsTask) PostExecute() error {
	return nil
}

type CreatePartitionTask struct {
	Condition
	milvuspb.CreatePartitionRequest
	masterClient masterpb.MasterServiceClient
	result       *commonpb.Status
	ctx          context.Context
}

func (cpt *CreatePartitionTask) ID() UniqueID {
	return cpt.Base.MsgID
}

func (cpt *CreatePartitionTask) SetID(uid UniqueID) {
	cpt.Base.MsgID = uid
}

func (cpt *CreatePartitionTask) Type() commonpb.MsgType {
	return cpt.Base.MsgType
}

func (cpt *CreatePartitionTask) BeginTs() Timestamp {
	return cpt.Base.Timestamp
}

func (cpt *CreatePartitionTask) EndTs() Timestamp {
	return cpt.Base.Timestamp
}

func (cpt *CreatePartitionTask) SetTs(ts Timestamp) {
	cpt.Base.Timestamp = ts
}

func (cpt *CreatePartitionTask) PreExecute() error {
	collName, partitionTag := cpt.CollectionName, cpt.PartitionName

	if err := ValidateCollectionName(collName); err != nil {
		return err
	}

	if err := ValidatePartitionTag(partitionTag, true); err != nil {
		return err
	}

	return nil
}

func (cpt *CreatePartitionTask) Execute() (err error) {
	cpt.result, err = cpt.masterClient.CreatePartition(cpt.ctx, &cpt.CreatePartitionRequest)
	return err
}

func (cpt *CreatePartitionTask) PostExecute() error {
	return nil
}

type DropPartitionTask struct {
	Condition
	milvuspb.DropPartitionRequest
	masterClient masterpb.MasterServiceClient
	result       *commonpb.Status
	ctx          context.Context
}

func (dpt *DropPartitionTask) ID() UniqueID {
	return dpt.Base.MsgID
}

func (dpt *DropPartitionTask) SetID(uid UniqueID) {
	dpt.Base.MsgID = uid
}

func (dpt *DropPartitionTask) Type() commonpb.MsgType {
	return dpt.Base.MsgType
}

func (dpt *DropPartitionTask) BeginTs() Timestamp {
	return dpt.Base.Timestamp
}

func (dpt *DropPartitionTask) EndTs() Timestamp {
	return dpt.Base.Timestamp
}

func (dpt *DropPartitionTask) SetTs(ts Timestamp) {
	dpt.Base.Timestamp = ts
}

func (dpt *DropPartitionTask) PreExecute() error {
	collName, partitionTag := dpt.CollectionName, dpt.PartitionName

	if err := ValidateCollectionName(collName); err != nil {
		return err
	}

	if err := ValidatePartitionTag(partitionTag, true); err != nil {
		return err
	}

	return nil
}

func (dpt *DropPartitionTask) Execute() (err error) {
	dpt.result, err = dpt.masterClient.DropPartition(dpt.ctx, &dpt.DropPartitionRequest)
	return err
}

func (dpt *DropPartitionTask) PostExecute() error {
	return nil
}

type HasPartitionTask struct {
	Condition
	milvuspb.HasPartitionRequest
	masterClient masterpb.MasterServiceClient
	result       *servicepb.BoolResponse
	ctx          context.Context
}

func (hpt *HasPartitionTask) ID() UniqueID {
	return hpt.Base.MsgID
}

func (hpt *HasPartitionTask) SetID(uid UniqueID) {
	hpt.Base.MsgID = uid
}

func (hpt *HasPartitionTask) Type() commonpb.MsgType {
	return hpt.Base.MsgType
}

func (hpt *HasPartitionTask) BeginTs() Timestamp {
	return hpt.Base.Timestamp
}

func (hpt *HasPartitionTask) EndTs() Timestamp {
	return hpt.Base.Timestamp
}

func (hpt *HasPartitionTask) SetTs(ts Timestamp) {
	hpt.Base.Timestamp = ts
}

func (hpt *HasPartitionTask) PreExecute() error {
	collName, partitionTag := hpt.CollectionName, hpt.PartitionName

	if err := ValidateCollectionName(collName); err != nil {
		return err
	}

	if err := ValidatePartitionTag(partitionTag, true); err != nil {
		return err
	}
	return nil
}

func (hpt *HasPartitionTask) Execute() (err error) {
	result, err := hpt.masterClient.HasPartition(hpt.ctx, &hpt.HasPartitionRequest)
	hpt.result = &servicepb.BoolResponse{
		Status: result.Status,
		Value:  result.Value,
	}
	return err
}

func (hpt *HasPartitionTask) PostExecute() error {
	return nil
}

//type DescribePartitionTask struct {
//	Condition
//	internalpb.DescribePartitionRequest
//	masterClient masterpb.MasterServiceClient
//	result       *servicepb.PartitionDescription
//	ctx          context.Context
//}
//
//func (dpt *DescribePartitionTask) ID() UniqueID {
//	return dpt.ReqID
//}
//
//func (dpt *DescribePartitionTask) SetID(uid UniqueID) {
//	dpt.ReqID = uid
//}
//
//func (dpt *DescribePartitionTask) Type() commonpb.MsgType {
//	return dpt.MsgType
//}
//
//func (dpt *DescribePartitionTask) BeginTs() Timestamp {
//	return dpt.Timestamp
//}
//
//func (dpt *DescribePartitionTask) EndTs() Timestamp {
//	return dpt.Timestamp
//}
//
//func (dpt *DescribePartitionTask) SetTs(ts Timestamp) {
//	dpt.Timestamp = ts
//}
//
//func (dpt *DescribePartitionTask) PreExecute() error {
//	collName, partitionTag := dpt.PartitionName.CollectionName, dpt.PartitionName.Tag
//
//	if err := ValidateCollectionName(collName); err != nil {
//		return err
//	}
//
//	if err := ValidatePartitionTag(partitionTag, true); err != nil {
//		return err
//	}
//	return nil
//}
//
//func (dpt *DescribePartitionTask) Execute() (err error) {
//	dpt.result, err = dpt.masterClient.DescribePartition(dpt.ctx, &dpt.DescribePartitionRequest)
//	return err
//}
//
//func (dpt *DescribePartitionTask) PostExecute() error {
//	return nil
//}

type ShowPartitionsTask struct {
	Condition
	milvuspb.ShowPartitionRequest
	masterClient masterpb.MasterServiceClient
	result       *servicepb.StringListResponse
	ctx          context.Context
}

func (spt *ShowPartitionsTask) ID() UniqueID {
	return spt.Base.MsgID
}

func (spt *ShowPartitionsTask) SetID(uid UniqueID) {
	spt.Base.MsgID = uid
}

func (spt *ShowPartitionsTask) Type() commonpb.MsgType {
	return spt.Base.MsgType
}

func (spt *ShowPartitionsTask) BeginTs() Timestamp {
	return spt.Base.Timestamp
}

func (spt *ShowPartitionsTask) EndTs() Timestamp {
	return spt.Base.Timestamp
}

func (spt *ShowPartitionsTask) SetTs(ts Timestamp) {
	spt.Base.Timestamp = ts
}

func (spt *ShowPartitionsTask) PreExecute() error {
	if err := ValidateCollectionName(spt.CollectionName); err != nil {
		return err
	}
	return nil
}

func (spt *ShowPartitionsTask) Execute() error {
	result, err := spt.masterClient.ShowPartitions(spt.ctx, &spt.ShowPartitionRequest)
	spt.result = &servicepb.StringListResponse{
		Status: result.Status,
		Values: result.PartitionNames,
	}
	return err
}

func (spt *ShowPartitionsTask) PostExecute() error {
	return nil
}

type CreateIndexTask struct {
	Condition
	milvuspb.CreateIndexRequest
	masterClient masterpb.MasterServiceClient
	result       *commonpb.Status
	ctx          context.Context
}

func (cit *CreateIndexTask) ID() UniqueID {
	return cit.Base.MsgID
}

func (cit *CreateIndexTask) SetID(uid UniqueID) {
	cit.Base.MsgID = uid
}

func (cit *CreateIndexTask) Type() commonpb.MsgType {
	return cit.Base.MsgType
}

func (cit *CreateIndexTask) BeginTs() Timestamp {
	return cit.Base.Timestamp
}

func (cit *CreateIndexTask) EndTs() Timestamp {
	return cit.Base.Timestamp
}

func (cit *CreateIndexTask) SetTs(ts Timestamp) {
	cit.Base.Timestamp = ts
}

func (cit *CreateIndexTask) PreExecute() error {
	collName, fieldName := cit.CollectionName, cit.FieldName

	if err := ValidateCollectionName(collName); err != nil {
		return err
	}

	if err := ValidateFieldName(fieldName); err != nil {
		return err
	}

	return nil
}

func (cit *CreateIndexTask) Execute() (err error) {
	cit.result, err = cit.masterClient.CreateIndex(cit.ctx, &cit.CreateIndexRequest)
	return err
}

func (cit *CreateIndexTask) PostExecute() error {
	return nil
}

type DescribeIndexTask struct {
	Condition
	milvuspb.DescribeIndexRequest
	masterClient masterpb.MasterServiceClient
	result       *servicepb.DescribeIndexResponse
	ctx          context.Context
}

func (dit *DescribeIndexTask) ID() UniqueID {
	return dit.Base.MsgID
}

func (dit *DescribeIndexTask) SetID(uid UniqueID) {
	dit.Base.MsgID = uid
}

func (dit *DescribeIndexTask) Type() commonpb.MsgType {
	return dit.Base.MsgType
}

func (dit *DescribeIndexTask) BeginTs() Timestamp {
	return dit.Base.Timestamp
}

func (dit *DescribeIndexTask) EndTs() Timestamp {
	return dit.Base.Timestamp
}

func (dit *DescribeIndexTask) SetTs(ts Timestamp) {
	dit.Base.Timestamp = ts
}

func (dit *DescribeIndexTask) PreExecute() error {
	collName, fieldName := dit.CollectionName, dit.FieldName

	if err := ValidateCollectionName(collName); err != nil {
		return err
	}

	if err := ValidateFieldName(fieldName); err != nil {
		return err
	}

	return nil
}

func (dit *DescribeIndexTask) Execute() error {
	result, err := dit.masterClient.DescribeIndex(dit.ctx, &dit.DescribeIndexRequest)
	dit.result = &servicepb.DescribeIndexResponse{
		Status:         result.Status,
		CollectionName: dit.CollectionName,
		FieldName:      dit.FieldName,
		ExtraParams:    result.IndexDescriptions[0].Params,
	}
	return err
}

func (dit *DescribeIndexTask) PostExecute() error {
	return nil
}

type DescribeIndexProgressTask struct {
	Condition
	milvuspb.IndexStateRequest
	masterClient masterpb.MasterServiceClient
	result       *servicepb.BoolResponse
	ctx          context.Context
}

func (dipt *DescribeIndexProgressTask) ID() UniqueID {
	return dipt.Base.MsgID
}

func (dipt *DescribeIndexProgressTask) SetID(uid UniqueID) {
	dipt.Base.MsgID = uid
}

func (dipt *DescribeIndexProgressTask) Type() commonpb.MsgType {
	return dipt.Base.MsgType
}

func (dipt *DescribeIndexProgressTask) BeginTs() Timestamp {
	return dipt.Base.Timestamp
}

func (dipt *DescribeIndexProgressTask) EndTs() Timestamp {
	return dipt.Base.Timestamp
}

func (dipt *DescribeIndexProgressTask) SetTs(ts Timestamp) {
	dipt.Base.Timestamp = ts
}

func (dipt *DescribeIndexProgressTask) PreExecute() error {
	collName, fieldName := dipt.CollectionName, dipt.FieldName

	if err := ValidateCollectionName(collName); err != nil {
		return err
	}

	if err := ValidateFieldName(fieldName); err != nil {
		return err
	}

	return nil
}

func (dipt *DescribeIndexProgressTask) Execute() error {
	result, err := dipt.masterClient.GetIndexState(dipt.ctx, &dipt.IndexStateRequest)
	dipt.result = &servicepb.BoolResponse{
		Status: result.Status,
		Value:  result.State == commonpb.IndexState_FINISHED,
	}
	return err
}

func (dipt *DescribeIndexProgressTask) PostExecute() error {
	return nil
}
