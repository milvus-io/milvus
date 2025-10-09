package proxy

import (
	"context"
	"strconv"

	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type insertTask struct {
	baseTask
	// req *milvuspb.InsertRequest
	Condition
	insertMsg *BaseInsertTask
	ctx       context.Context

	result          *milvuspb.MutationResult
	idAllocator     *allocator.IDAllocator
	chMgr           channelsMgr
	vChannels       []vChan
	pChannels       []pChan
	schema          *schemapb.CollectionSchema
	partitionKeys   *schemapb.FieldData
	schemaTimestamp uint64
	collectionID    int64
}

// TraceCtx returns insertTask context
func (it *insertTask) TraceCtx() context.Context {
	return it.ctx
}

func (it *insertTask) ID() UniqueID {
	return it.insertMsg.Base.MsgID
}

func (it *insertTask) SetID(uid UniqueID) {
	it.insertMsg.Base.MsgID = uid
}

func (it *insertTask) Name() string {
	return InsertTaskName
}

func (it *insertTask) Type() commonpb.MsgType {
	return it.insertMsg.Base.MsgType
}

func (it *insertTask) BeginTs() Timestamp {
	return it.insertMsg.BeginTimestamp
}

func (it *insertTask) SetTs(ts Timestamp) {
	it.insertMsg.BeginTimestamp = ts
	it.insertMsg.EndTimestamp = ts
}

func (it *insertTask) EndTs() Timestamp {
	return it.insertMsg.EndTimestamp
}

func (it *insertTask) setChannels() error {
	collID, err := globalMetaCache.GetCollectionID(it.ctx, it.insertMsg.GetDbName(), it.insertMsg.CollectionName)
	if err != nil {
		return err
	}
	channels, err := it.chMgr.getChannels(collID)
	if err != nil {
		return err
	}
	it.pChannels = channels
	return nil
}

func (it *insertTask) getChannels() []pChan {
	return it.pChannels
}

func (it *insertTask) OnEnqueue() error {
	if it.insertMsg.Base == nil {
		it.insertMsg.Base = commonpbutil.NewMsgBase()
	}
	it.insertMsg.Base.MsgType = commonpb.MsgType_Insert
	it.insertMsg.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (it *insertTask) PreExecute(ctx context.Context) error {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-Insert-PreExecute")
	defer sp.End()

	it.result = &milvuspb.MutationResult{
		Status: merr.Success(),
		IDs: &schemapb.IDs{
			IdField: nil,
		},
		Timestamp: it.EndTs(),
	}

	collectionName := it.insertMsg.CollectionName
	if err := validateCollectionName(collectionName); err != nil {
		log.Ctx(ctx).Warn("valid collection name failed", zap.String("collectionName", collectionName), zap.Error(err))
		return err
	}

	maxInsertSize := Params.QuotaConfig.MaxInsertSize.GetAsInt()
	if maxInsertSize != -1 && it.insertMsg.Size() > maxInsertSize {
		log.Ctx(ctx).Warn("insert request size exceeds maxInsertSize",
			zap.Int("request size", it.insertMsg.Size()), zap.Int("maxInsertSize", maxInsertSize))
		return merr.WrapErrAsInputError(merr.WrapErrParameterTooLarge("insert request size exceeds maxInsertSize"))
	}

	replicateID, err := GetReplicateID(it.ctx, it.insertMsg.GetDbName(), collectionName)
	if err != nil {
		log.Warn("get replicate id failed", zap.String("collectionName", collectionName), zap.Error(err))
		return merr.WrapErrAsInputError(err)
	}
	if replicateID != "" {
		return merr.WrapErrCollectionReplicateMode("insert")
	}

	collID, err := globalMetaCache.GetCollectionID(context.Background(), it.insertMsg.GetDbName(), collectionName)
	if err != nil {
		log.Ctx(ctx).Warn("fail to get collection id", zap.Error(err))
		return err
	}
	it.collectionID = collID

	colInfo, err := globalMetaCache.GetCollectionInfo(ctx, it.insertMsg.GetDbName(), collectionName, collID)
	if err != nil {
		log.Ctx(ctx).Warn("fail to get collection info", zap.Error(err))
		return err
	}
	if it.schemaTimestamp != 0 {
		if it.schemaTimestamp != colInfo.updateTimestamp {
			err := merr.WrapErrCollectionSchemaMisMatch(collectionName)
			log.Ctx(ctx).Info("collection schema mismatch",
				zap.String("collectionName", collectionName),
				zap.Uint64("requestSchemaTs", it.schemaTimestamp),
				zap.Uint64("collectionSchemaTs", colInfo.updateTimestamp),
				zap.Error(err))
			return err
		}
	}

	schema, err := globalMetaCache.GetCollectionSchema(ctx, it.insertMsg.GetDbName(), collectionName)
	if err != nil {
		log.Ctx(ctx).Warn("get collection schema from global meta cache failed", zap.String("collectionName", collectionName), zap.Error(err))
		return merr.WrapErrAsInputErrorWhen(err, merr.ErrCollectionNotFound, merr.ErrDatabaseNotFound)
	}
	it.schema = schema.CollectionSchema

	if err := genFunctionFields(ctx, it.insertMsg, schema, false); err != nil {
		return err
	}

	rowNums := uint32(it.insertMsg.NRows())
	// set insertTask.rowIDs
	var rowIDBegin UniqueID
	var rowIDEnd UniqueID
	tr := timerecord.NewTimeRecorder("applyPK")
	clusterID := Params.CommonCfg.ClusterID.GetAsUint64()
	rowIDBegin, rowIDEnd, _ = common.AllocAutoID(it.idAllocator.Alloc, rowNums, clusterID)
	metrics.ProxyApplyPrimaryKeyLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10)).Observe(float64(tr.ElapseSpan().Milliseconds()))

	it.insertMsg.RowIDs = make([]UniqueID, rowNums)
	for i := rowIDBegin; i < rowIDEnd; i++ {
		offset := i - rowIDBegin
		it.insertMsg.RowIDs[offset] = i
	}
	// set insertTask.timeStamps
	rowNum := it.insertMsg.NRows()
	it.insertMsg.Timestamps = make([]uint64, rowNum)
	for index := range it.insertMsg.Timestamps {
		it.insertMsg.Timestamps[index] = it.insertMsg.BeginTimestamp
	}

	// set result.SuccIndex
	sliceIndex := make([]uint32, rowNums)
	for i := uint32(0); i < rowNums; i++ {
		sliceIndex[i] = i
	}
	it.result.SuccIndex = sliceIndex

	if it.schema.EnableDynamicField {
		err = checkDynamicFieldData(it.schema, it.insertMsg)
		if err != nil {
			return err
		}
	}

	if Params.CommonCfg.EnableNamespace.GetAsBool() {
		err = addNamespaceData(it.schema, it.insertMsg)
		if err != nil {
			return err
		}
	}

	err = checkAndFlattenStructFieldData(it.schema, it.insertMsg)
	if err != nil {
		return err
	}

	allFields := typeutil.GetAllFieldSchemas(it.schema)

	// check primaryFieldData whether autoID is true or not
	// set rowIDs as primary data if autoID == true
	// TODO(dragondriver): in fact, NumRows is not trustable, we should check all input fields
	it.result.IDs, err = checkPrimaryFieldData(allFields, it.schema, it.insertMsg)
	log := log.Ctx(ctx).With(zap.String("collectionName", collectionName))
	if err != nil {
		log.Warn("check primary field data and hash primary key failed",
			zap.Error(err))
		return err
	}

	// check varchar/text with analyzer was utf-8 format
	err = checkInputUtf8Compatiable(allFields, it.insertMsg)
	if err != nil {
		log.Warn("check varchar/text format failed", zap.Error(err))
		return err
	}

	// set field ID to insert field data
	err = fillFieldPropertiesBySchema(it.insertMsg.GetFieldsData(), schema.CollectionSchema)
	if err != nil {
		log.Info("set fieldID to fieldData failed",
			zap.Error(err))
		return err
	}

	// trans timestamptz data
	_, colTimezone := getColTimezone(colInfo)
	err = timestamptzIsoStr2Utc(it.insertMsg.GetFieldsData(), colTimezone)
	if err != nil {
		return err
	}

	partitionKeyMode, err := isPartitionKeyMode(ctx, it.insertMsg.GetDbName(), collectionName)
	if err != nil {
		log.Warn("check partition key mode failed", zap.String("collectionName", collectionName), zap.Error(err))
		return err
	}
	if partitionKeyMode {
		fieldSchema, _ := typeutil.GetPartitionKeyFieldSchema(it.schema)
		it.partitionKeys, err = getPartitionKeyFieldData(fieldSchema, it.insertMsg)
		if err != nil {
			log.Warn("get partition keys from insert request failed", zap.String("collectionName", collectionName), zap.Error(err))
			return err
		}
	} else {
		// set default partition name if not use partition key
		// insert to _default partition
		partitionTag := it.insertMsg.GetPartitionName()
		if len(partitionTag) <= 0 {
			pinfo, err := globalMetaCache.GetPartitionInfo(ctx, it.insertMsg.GetDbName(), collectionName, "")
			if err != nil {
				log.Warn("get partition info failed", zap.String("collectionName", collectionName), zap.Error(err))
				return err
			}
			partitionTag = pinfo.name
			it.insertMsg.PartitionName = partitionTag
		}

		if err := validatePartitionTag(partitionTag, true); err != nil {
			log.Warn("valid partition name failed", zap.String("partition name", partitionTag), zap.Error(err))
			return err
		}
	}

	if err := newValidateUtil(withNANCheck(), withOverflowCheck(), withMaxLenCheck(), withMaxCapCheck()).
		Validate(it.insertMsg.GetFieldsData(), schema.schemaHelper, it.insertMsg.NRows()); err != nil {
		return merr.WrapErrAsInputError(err)
	}

	log.Debug("Proxy Insert PreExecute done")

	return nil
}

func (it *insertTask) PostExecute(ctx context.Context) error {
	return nil
}
