package proxy

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/types"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"

	"github.com/milvus-io/milvus/internal/util/timerecord"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type queryTask struct {
	Condition
	*internalpb.RetrieveRequest
	ctx            context.Context
	resultBuf      chan []*internalpb.RetrieveResults
	result         *milvuspb.QueryResults
	query          *milvuspb.QueryRequest
	chMgr          channelsMgr
	qc             types.QueryCoord
	ids            *schemapb.IDs
	collectionName string
	collectionID   UniqueID
}

func (qt *queryTask) PreExecute(ctx context.Context) error {
	qt.Base.MsgType = commonpb.MsgType_Retrieve
	qt.Base.SourceID = Params.ProxyCfg.ProxyID

	collectionName := qt.query.CollectionName

	if err := validateCollectionName(qt.query.CollectionName); err != nil {
		log.Warn("Invalid collection name.", zap.String("collectionName", collectionName),
			zap.Int64("requestID", qt.Base.MsgID), zap.String("requestType", "query"))
		return err
	}
	log.Info("Validate collection name.", zap.Any("collectionName", collectionName),
		zap.Any("requestID", qt.Base.MsgID), zap.Any("requestType", "query"))

	info, err := globalMetaCache.GetCollectionInfo(ctx, collectionName)
	if err != nil {
		log.Debug("Failed to get collection id.", zap.Any("collectionName", collectionName),
			zap.Any("requestID", qt.Base.MsgID), zap.Any("requestType", "query"))
		return err
	}
	qt.collectionName = info.schema.Name
	log.Info("Get collection id by name.", zap.Any("collectionName", collectionName),
		zap.Any("requestID", qt.Base.MsgID), zap.Any("requestType", "query"))

	for _, tag := range qt.query.PartitionNames {
		if err := validatePartitionTag(tag, false); err != nil {
			log.Debug("Invalid partition name.", zap.Any("partitionName", tag),
				zap.Any("requestID", qt.Base.MsgID), zap.Any("requestType", "query"))
			return err
		}
	}
	log.Info("Validate partition names.",
		zap.Any("requestID", qt.Base.MsgID), zap.Any("requestType", "query"))

	// check if collection was already loaded into query node
	showResp, err := qt.qc.ShowCollections(qt.ctx, &querypb.ShowCollectionsRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_ShowCollections,
			MsgID:     qt.Base.MsgID,
			Timestamp: qt.Base.Timestamp,
			SourceID:  Params.ProxyCfg.ProxyID,
		},
		DbID: 0, // TODO(dragondriver)
	})
	if err != nil {
		return err
	}
	if showResp.Status.ErrorCode != commonpb.ErrorCode_Success {
		return errors.New(showResp.Status.Reason)
	}
	log.Debug("QueryCoord show collections",
		zap.Any("collections", showResp.CollectionIDs),
		zap.Any("collID", info.collID))

	collectionLoaded := false
	for _, collID := range showResp.CollectionIDs {
		if info.collID == collID {
			collectionLoaded = true
			break
		}
	}
	if !collectionLoaded {
		return fmt.Errorf("collection %v was not loaded into memory", collectionName)
	}

	schema, _ := globalMetaCache.GetCollectionSchema(ctx, qt.query.CollectionName)

	if qt.ids != nil {
		pkField := ""
		for _, field := range schema.Fields {
			if field.IsPrimaryKey {
				pkField = field.Name
			}
		}
		qt.query.Expr = IDs2Expr(pkField, qt.ids.GetIntId().Data)
	}

	if qt.query.Expr == "" {
		errMsg := "Query expression is empty"
		return fmt.Errorf(errMsg)
	}

	plan, err := createExprPlan(schema, qt.query.Expr)
	if err != nil {
		return err
	}
	qt.query.OutputFields, err = translateOutputFields(qt.query.OutputFields, schema, true)
	if err != nil {
		return err
	}
	log.Debug("translate output fields", zap.Any("OutputFields", qt.query.OutputFields))
	if len(qt.query.OutputFields) == 0 {
		for _, field := range schema.Fields {
			if field.FieldID >= 100 && field.DataType != schemapb.DataType_FloatVector && field.DataType != schemapb.DataType_BinaryVector {
				qt.OutputFieldsId = append(qt.OutputFieldsId, field.FieldID)
			}
		}
	} else {
		addPrimaryKey := false
		for _, reqField := range qt.query.OutputFields {
			findField := false
			for _, field := range schema.Fields {
				if reqField == field.Name {
					if field.IsPrimaryKey {
						addPrimaryKey = true
					}
					findField = true
					qt.OutputFieldsId = append(qt.OutputFieldsId, field.FieldID)
					plan.OutputFieldIds = append(plan.OutputFieldIds, field.FieldID)
				} else {
					if field.IsPrimaryKey && !addPrimaryKey {
						qt.OutputFieldsId = append(qt.OutputFieldsId, field.FieldID)
						plan.OutputFieldIds = append(plan.OutputFieldIds, field.FieldID)
						addPrimaryKey = true
					}
				}
			}
			if !findField {
				errMsg := "Field " + reqField + " not exist"
				return errors.New(errMsg)
			}
		}
	}
	log.Debug("translate output fields to field ids", zap.Any("OutputFieldsID", qt.OutputFieldsId))

	qt.RetrieveRequest.SerializedExprPlan, err = proto.Marshal(plan)
	if err != nil {
		return err
	}
	travelTimestamp := qt.query.TravelTimestamp
	if travelTimestamp == 0 {
		travelTimestamp = qt.BeginTs()
	} else {
		durationSeconds := tsoutil.CalculateDuration(qt.BeginTs(), travelTimestamp) / 1000
		if durationSeconds > Params.CommonCfg.RetentionDuration {
			duration := time.Second * time.Duration(durationSeconds)
			return fmt.Errorf("only support to travel back to %s so far", duration.String())
		}
	}
	guaranteeTimestamp := qt.query.GuaranteeTimestamp
	if guaranteeTimestamp == 0 {
		guaranteeTimestamp = qt.BeginTs()
	}
	qt.TravelTimestamp = travelTimestamp
	qt.GuaranteeTimestamp = guaranteeTimestamp
	deadline, ok := qt.TraceCtx().Deadline()
	if ok {
		qt.RetrieveRequest.TimeoutTimestamp = tsoutil.ComposeTSByTime(deadline, 0)
	}

	qt.ResultChannelID = Params.ProxyCfg.RetrieveResultChannelNames[0]
	qt.DbID = 0 // todo(yukun)

	qt.CollectionID = info.collID
	qt.PartitionIDs = make([]UniqueID, 0)

	partitionsMap, err := globalMetaCache.GetPartitions(ctx, collectionName)
	if err != nil {
		log.Debug("Failed to get partitions in collection.", zap.Any("collectionName", collectionName),
			zap.Any("requestID", qt.Base.MsgID), zap.Any("requestType", "query"))
		return err
	}
	log.Info("Get partitions in collection.", zap.Any("collectionName", collectionName),
		zap.Any("requestID", qt.Base.MsgID), zap.Any("requestType", "query"))

	partitionsRecord := make(map[UniqueID]bool)
	for _, partitionName := range qt.query.PartitionNames {
		pattern := fmt.Sprintf("^%s$", partitionName)
		re, err := regexp.Compile(pattern)
		if err != nil {
			log.Debug("Failed to compile partition name regex expression.", zap.Any("partitionName", partitionName),
				zap.Any("requestID", qt.Base.MsgID), zap.Any("requestType", "query"))
			return errors.New("invalid partition names")
		}
		found := false
		for name, pID := range partitionsMap {
			if re.MatchString(name) {
				if _, exist := partitionsRecord[pID]; !exist {
					qt.PartitionIDs = append(qt.PartitionIDs, pID)
					partitionsRecord[pID] = true
				}
				found = true
			}
		}
		if !found {
			// FIXME(wxyu): undefined behavior
			errMsg := fmt.Sprintf("PartitonName: %s not found", partitionName)
			return errors.New(errMsg)
		}
	}

	log.Info("Query PreExecute done.",
		zap.Any("requestID", qt.Base.MsgID), zap.Any("requestType", "query"))
	return nil
}

func (qt *queryTask) Execute(ctx context.Context) error {
	tr := timerecord.NewTimeRecorder(fmt.Sprintf("proxy execute query %d", qt.ID()))
	defer tr.Elapse("done")

	var tsMsg msgstream.TsMsg = &msgstream.RetrieveMsg{
		RetrieveRequest: *qt.RetrieveRequest,
		BaseMsg: msgstream.BaseMsg{
			Ctx:            ctx,
			HashValues:     []uint32{uint32(Params.ProxyCfg.ProxyID)},
			BeginTimestamp: qt.Base.Timestamp,
			EndTimestamp:   qt.Base.Timestamp,
		},
	}
	msgPack := msgstream.MsgPack{
		BeginTs: qt.Base.Timestamp,
		EndTs:   qt.Base.Timestamp,
		Msgs:    make([]msgstream.TsMsg, 1),
	}
	msgPack.Msgs[0] = tsMsg

	stream, err := qt.chMgr.getDQLStream(qt.CollectionID)
	if err != nil {
		err = qt.chMgr.createDQLStream(qt.CollectionID)
		if err != nil {
			qt.result.Status.ErrorCode = commonpb.ErrorCode_UnexpectedError
			qt.result.Status.Reason = err.Error()
			return err
		}
		stream, err = qt.chMgr.getDQLStream(qt.CollectionID)
		if err != nil {
			qt.result.Status.ErrorCode = commonpb.ErrorCode_UnexpectedError
			qt.result.Status.Reason = err.Error()
			return err
		}
	}
	tr.Record("get used message stream")

	err = stream.Produce(&msgPack)
	if err != nil {
		log.Debug("Failed to send retrieve request.",
			zap.Any("requestID", qt.Base.MsgID), zap.Any("requestType", "query"))
	}
	log.Debug("proxy sent one retrieveMsg",
		zap.Int64("collectionID", qt.CollectionID),
		zap.Int64("msgID", tsMsg.ID()),
		zap.Int("length of search msg", len(msgPack.Msgs)),
		zap.Uint64("timeoutTs", qt.RetrieveRequest.TimeoutTimestamp))
	tr.Record("send retrieve request to message stream")

	log.Info("Query Execute done.",
		zap.Any("requestID", qt.Base.MsgID), zap.Any("requestType", "query"))
	return err
}

func (qt *queryTask) PostExecute(ctx context.Context) error {
	tr := timerecord.NewTimeRecorder("queryTask PostExecute")
	defer func() {
		tr.Elapse("done")
	}()
	select {
	case <-qt.TraceCtx().Done():
		log.Debug("proxy", zap.Int64("Query: wait to finish failed, timeout!, taskID:", qt.ID()))
		return fmt.Errorf("queryTask:wait to finish failed, timeout : %d", qt.ID())
	case retrieveResults := <-qt.resultBuf:
		filterRetrieveResults := make([]*internalpb.RetrieveResults, 0)
		var reason string
		for _, partialRetrieveResult := range retrieveResults {
			if partialRetrieveResult.Status.ErrorCode == commonpb.ErrorCode_Success {
				filterRetrieveResults = append(filterRetrieveResults, partialRetrieveResult)
			} else {
				reason += partialRetrieveResult.Status.Reason + "\n"
			}
		}

		if len(filterRetrieveResults) == 0 {
			qt.result = &milvuspb.QueryResults{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    reason,
				},
				CollectionName: qt.collectionName,
			}
			log.Debug("Query failed on all querynodes.",
				zap.Any("requestID", qt.Base.MsgID), zap.Any("requestType", "query"))
			return errors.New(reason)
		}

		var err error
		qt.result, err = mergeRetrieveResults(filterRetrieveResults)
		if err != nil {
			return err
		}
		qt.result.CollectionName = qt.collectionName

		if len(qt.result.FieldsData) > 0 {
			qt.result.Status = &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			}
		} else {
			log.Info("Query result is nil", zap.Any("requestID", qt.Base.MsgID), zap.Any("requestType", "query"))
			qt.result.Status = &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_EmptyCollection,
				Reason:    reason,
			}
			return nil
		}

		schema, err := globalMetaCache.GetCollectionSchema(ctx, qt.query.CollectionName)
		if err != nil {
			return err
		}
		for i := 0; i < len(qt.result.FieldsData); i++ {
			for _, field := range schema.Fields {
				if field.FieldID == qt.OutputFieldsId[i] {
					qt.result.FieldsData[i].FieldName = field.Name
					qt.result.FieldsData[i].FieldId = field.FieldID
					qt.result.FieldsData[i].Type = field.DataType
				}
			}
		}
	}

	log.Info("Query PostExecute done", zap.Any("requestID", qt.Base.MsgID), zap.Any("requestType", "query"))
	return nil
}
func (qt *queryTask) getChannels() ([]pChan, error) {
	collID, err := globalMetaCache.GetCollectionID(qt.ctx, qt.query.CollectionName)
	if err != nil {
		return nil, err
	}
	var channels []pChan
	channels, err = qt.chMgr.getChannels(collID)
	if err != nil {
		err := qt.chMgr.createDMLMsgStream(collID)
		if err != nil {
			return nil, err
		}
		return qt.chMgr.getChannels(collID)
	}

	return channels, nil
}

func (qt *queryTask) getVChannels() ([]vChan, error) {
	collID, err := globalMetaCache.GetCollectionID(qt.ctx, qt.query.CollectionName)
	if err != nil {
		return nil, err
	}

	var channels []vChan
	channels, err = qt.chMgr.getVChannels(collID)
	if err != nil {
		err := qt.chMgr.createDMLMsgStream(collID)
		if err != nil {
			return nil, err
		}
		return qt.chMgr.getVChannels(collID)
	}

	return channels, nil
}

// IDs2Expr converts ids slices to bool expresion with specified field name
func IDs2Expr(fieldName string, ids []int64) string {
	idsStr := strings.Trim(strings.Join(strings.Fields(fmt.Sprint(ids)), ", "), "[]")
	return fieldName + " in [ " + idsStr + " ]"
}

func mergeRetrieveResults(retrieveResults []*internalpb.RetrieveResults) (*milvuspb.QueryResults, error) {
	var ret *milvuspb.QueryResults
	var skipDupCnt int64
	var idSet = make(map[int64]struct{})

	// merge results and remove duplicates
	for _, rr := range retrieveResults {
		// skip empty result, it will break merge result
		if rr == nil || rr.Ids == nil || rr.Ids.GetIntId() == nil || len(rr.Ids.GetIntId().Data) == 0 {
			continue
		}

		if ret == nil {
			ret = &milvuspb.QueryResults{
				FieldsData: make([]*schemapb.FieldData, len(rr.FieldsData)),
			}
		}

		if len(ret.FieldsData) != len(rr.FieldsData) {
			return nil, fmt.Errorf("mismatch FieldData in proxy RetrieveResults, expect %d get %d", len(ret.FieldsData), len(rr.FieldsData))
		}

		for i, id := range rr.Ids.GetIntId().GetData() {
			if _, ok := idSet[id]; !ok {
				typeutil.AppendFieldData(ret.FieldsData, rr.FieldsData, int64(i))
				idSet[id] = struct{}{}
			} else {
				// primary keys duplicate
				skipDupCnt++
			}
		}
	}
	log.Debug("skip duplicated query result", zap.Int64("count", skipDupCnt))

	if ret == nil {
		ret = &milvuspb.QueryResults{
			FieldsData: []*schemapb.FieldData{},
		}
	}

	return ret, nil
}

func (qt *queryTask) TraceCtx() context.Context {
	return qt.ctx
}

func (qt *queryTask) ID() UniqueID {
	return qt.Base.MsgID
}

func (qt *queryTask) SetID(uid UniqueID) {
	qt.Base.MsgID = uid
}

func (qt *queryTask) Name() string {
	return RetrieveTaskName
}

func (qt *queryTask) Type() commonpb.MsgType {
	return qt.Base.MsgType
}

func (qt *queryTask) BeginTs() Timestamp {
	return qt.Base.Timestamp
}

func (qt *queryTask) EndTs() Timestamp {
	return qt.Base.Timestamp
}

func (qt *queryTask) SetTs(ts Timestamp) {
	qt.Base.Timestamp = ts
}

func (qt *queryTask) OnEnqueue() error {
	qt.Base.MsgType = commonpb.MsgType_Retrieve
	return nil
}
