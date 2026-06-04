package proxy

import (
	"context"
	"strconv"

	"go.opentelemetry.io/otel"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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
	schemaVersion   int32

	idempotencyEnabled bool
	idempotencyKey     string
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
	log := mlog.With(mlog.String("collectionName", collectionName))
	if err := validateCollectionName(collectionName); err != nil {
		log.Warn(ctx, "valid collection name failed", mlog.String("collectionName", collectionName), mlog.Err(err))
		return err
	}

	maxInsertSize := Params.QuotaConfig.MaxInsertSize.GetAsInt()
	if maxInsertSize != -1 && it.insertMsg.Size() > maxInsertSize {
		log.Warn(ctx, "insert request size exceeds maxInsertSize",
			mlog.Int("request size", it.insertMsg.Size()), mlog.Int("maxInsertSize", maxInsertSize))
		return merr.WrapErrAsInputError(merr.WrapErrParameterTooLarge("insert request size exceeds maxInsertSize"))
	}

	collID, err := globalMetaCache.GetCollectionID(context.Background(), it.insertMsg.GetDbName(), collectionName)
	if err != nil {
		log.Warn(ctx, "fail to get collection id", mlog.Err(err))
		return err
	}
	it.collectionID = collID

	colInfo, err := globalMetaCache.GetCollectionInfo(ctx, it.insertMsg.GetDbName(), collectionName, collID)
	if err != nil {
		log.Warn(ctx, "fail to get collection info", mlog.Err(err))
		return err
	}

	if it.schemaTimestamp != 0 {
		if it.schemaTimestamp != colInfo.updateTimestamp {
			err := merr.WrapErrCollectionSchemaMisMatch(collectionName)
			log.Info(ctx, "collection schema mismatch",
				mlog.String("collectionName", collectionName),
				mlog.Uint64("requestSchemaTs", it.schemaTimestamp),
				mlog.Uint64("collectionSchemaTs", colInfo.updateTimestamp),
				mlog.Err(err))
			return err
		}
	}

	schema, err := globalMetaCache.GetCollectionSchema(ctx, it.insertMsg.GetDbName(), collectionName)
	if err != nil {
		log.Warn(ctx, "get collection schema from global meta cache failed", mlog.String("collectionName", collectionName), mlog.Err(err))
		return err
	}
	it.schema = schema.CollectionSchema
	it.schemaVersion = schema.Version
	if err := validateTextStorageV3Enabled(it.schema); err != nil {
		return err
	}

	primaryFieldSchema, err := typeutil.GetPrimaryFieldSchema(it.schema)
	if err != nil {
		log.Warn(ctx, "get primary field schema failed", mlog.Any("schema", it.schema), mlog.Err(err))
		return err
	}
	excludeAutoIDPrimary := primaryFieldSchema.GetAutoID() &&
		!typeutil.IsPrimaryFieldDataExist(it.insertMsg.GetFieldsData(), primaryFieldSchema)
	if err := it.prepareAutoIdempotencyKeyIfEnabled(ctx, colInfo.properties, excludeAutoIDPrimary); err != nil {
		return err
	}

	if err := genFunctionFields(ctx, it.insertMsg, schema, false); err != nil {
		return err
	}

	rowNums := uint32(it.insertMsg.NRows())
	// set insertTask.rowIDs
	var rowIDBegin UniqueID
	var rowIDEnd UniqueID
	tr := timerecord.NewTimeRecorder("applyPK")
	clusterID := Params.CommonCfg.ClusterID.GetAsUint64()
	rowIDBegin, rowIDEnd, AllocErr := common.AllocAutoID(it.idAllocator.Alloc, rowNums, clusterID)
	metrics.ProxyApplyPrimaryKeyLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10)).Observe(float64(tr.ElapseSpan().Microseconds()) / 1000.0)
	if AllocErr != nil {
		log.Warn(ctx, "failed to allocate auto id",
			mlog.String("collectionName", collectionName),
			mlog.Int64("collectionID", it.collectionID),
			mlog.Uint32("rowNums", rowNums),
			mlog.Err(AllocErr))
		return AllocErr
	}

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

	err = addNamespaceData(it.schema, it.insertMsg)
	if err != nil {
		return err
	}

	err = checkAndFlattenStructFieldData(it.schema, it.insertMsg)
	if err != nil {
		return err
	}

	allFields := typeutil.GetAllFieldSchemas(it.schema)

	// check primaryFieldData whether autoID is true or not
	// set rowIDs as primary data if autoID == true
	// TODO(dragondriver): in fact, NumRows is not trustable, we should check all input fields
	it.result.IDs, err = checkPrimaryFieldData(ctx, allFields, it.schema, it.insertMsg)
	if err != nil {
		log.Warn(ctx, "check primary field data and hash primary key failed",
			mlog.Err(err))
		return err
	}

	// check varchar/text with analyzer was utf-8 format
	err = checkInputUtf8Compatiable(allFields, it.insertMsg)
	if err != nil {
		log.Warn(ctx, "check varchar/text format failed", mlog.Err(err))
		return err
	}

	// Validate and set field ID to insert field data
	err = validateFieldDataColumns(it.insertMsg.GetFieldsData(), schema)
	if err != nil {
		log.Info(ctx, "validate field data columns failed", mlog.Err(err))
		return err
	}
	err = fillFieldPropertiesOnly(it.insertMsg.GetFieldsData(), schema)
	if err != nil {
		log.Info(ctx, "fill field properties failed", mlog.Err(err))
		return err
	}
	err = normalizeFP32ToFP16BF16VectorFieldData(it.insertMsg.GetFieldsData(), schema)
	if err != nil {
		log.Info(ctx, "normalize fp32 to fp16/bf16 vector field data failed", mlog.Err(err))
		return err
	}
	if err := it.reassignAutoIDForIdempotencyIfNeeded(ctx, excludeAutoIDPrimary, primaryFieldSchema); err != nil {
		return err
	}

	partitionKeyMode, err := isPartitionKeyMode(ctx, it.insertMsg.GetDbName(), collectionName)
	if err != nil {
		log.Warn(ctx, "check partition key mode failed", mlog.String("collectionName", collectionName), mlog.Err(err))
		return err
	}
	if partitionKeyMode {
		fieldSchema, _ := typeutil.GetPartitionKeyFieldSchema(it.schema)
		it.partitionKeys, err = getPartitionKeyFieldData(fieldSchema, it.insertMsg)
		if err != nil {
			log.Warn(ctx, "get partition keys from insert request failed", mlog.String("collectionName", collectionName), mlog.Err(err))
			return err
		}
	} else {
		// set default partition name if not use partition key
		// insert to _default partition
		partitionTag := it.insertMsg.GetPartitionName()
		if len(partitionTag) <= 0 {
			pinfo, err := globalMetaCache.GetPartitionInfo(ctx, it.insertMsg.GetDbName(), collectionName, "")
			if err != nil {
				log.Warn(ctx, "get partition info failed", mlog.String("collectionName", collectionName), mlog.Err(err))
				return err
			}
			partitionTag = pinfo.name
			it.insertMsg.PartitionName = partitionTag
		}

		if err := validatePartitionTag(partitionTag, true); err != nil {
			log.Warn(ctx, "valid partition name failed", mlog.String("partition name", partitionTag), mlog.Err(err))
			return err
		}
	}

	if err := newValidateUtil(withNANCheck(), withOverflowCheck(), withMaxLenCheck(), withMaxCapCheck()).
		Validate(it.insertMsg.GetFieldsData(), schema.schemaHelper, it.insertMsg.NRows()); err != nil {
		return merr.WrapErrAsInputError(err)
	}

	log.Debug(ctx, "Proxy Insert PreExecute done")

	return nil
}

func (it *insertTask) PostExecute(ctx context.Context) error {
	return nil
}
