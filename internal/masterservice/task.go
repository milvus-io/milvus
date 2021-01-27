package masterservice

import (
	"github.com/golang/protobuf/proto"
	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/etcdpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

type reqTask interface {
	Type() commonpb.MsgType
	Ts() (typeutil.Timestamp, error)
	IgnoreTimeStamp() bool
	Execute() error
	WaitToFinish() error
	Notify(err error)
}

type baseReqTask struct {
	cv   chan error
	core *Core
}

func (bt *baseReqTask) Notify(err error) {
	bt.cv <- err
}

func (bt *baseReqTask) WaitToFinish() error {
	select {
	case <-bt.core.ctx.Done():
		return errors.Errorf("context done")
	case err, ok := <-bt.cv:
		if !ok {
			return errors.Errorf("notify chan closed")
		}
		return err
	}
}

type CreateCollectionReqTask struct {
	baseReqTask
	Req *milvuspb.CreateCollectionRequest
}

func (t *CreateCollectionReqTask) Type() commonpb.MsgType {
	return t.Req.Base.MsgType
}

func (t *CreateCollectionReqTask) Ts() (typeutil.Timestamp, error) {
	return t.Req.Base.Timestamp, nil
}

func (t *CreateCollectionReqTask) IgnoreTimeStamp() bool {
	return false
}

func (t *CreateCollectionReqTask) Execute() error {
	var schema schemapb.CollectionSchema
	err := proto.Unmarshal(t.Req.Schema, &schema)
	if err != nil {
		return err
	}

	if t.Req.CollectionName != schema.Name {
		return errors.Errorf("collection name = %s, schema.Name=%s", t.Req.CollectionName, schema.Name)
	}

	for idx, field := range schema.Fields {
		field.FieldID = int64(idx + StartOfUserFieldID)
	}
	rowIDField := &schemapb.FieldSchema{
		FieldID:      int64(RowIDField),
		Name:         RowIDFieldName,
		IsPrimaryKey: false,
		Description:  "row id",
		DataType:     schemapb.DataType_INT64,
	}
	timeStampField := &schemapb.FieldSchema{
		FieldID:      int64(TimeStampField),
		Name:         TimeStampFieldName,
		IsPrimaryKey: false,
		Description:  "time stamp",
		DataType:     schemapb.DataType_INT64,
	}
	schema.Fields = append(schema.Fields, rowIDField, timeStampField)

	collID, err := t.core.idAllocator.AllocOne()
	if err != nil {
		return err
	}
	collTs, err := t.Ts()
	if err != nil {
		return err
	}
	partitionID, err := t.core.idAllocator.AllocOne()
	if err != nil {
		return err
	}
	collMeta := etcdpb.CollectionInfo{
		ID:           collID,
		Schema:       &schema,
		CreateTime:   collTs,
		PartitionIDs: make([]typeutil.UniqueID, 0, 16),
	}
	partMeta := etcdpb.PartitionInfo{
		PartitionName: Params.DefaultPartitionName,
		PartitionID:   partitionID,
		SegmentIDs:    make([]typeutil.UniqueID, 0, 16),
	}

	err = t.core.MetaTable.AddCollection(&collMeta, &partMeta)
	if err != nil {
		return err
	}
	schemaBytes, err := proto.Marshal(&schema)
	if err != nil {
		return err
	}

	ddReq := internalpb2.CreateCollectionRequest{
		Base:           t.Req.Base,
		DbName:         t.Req.DbName,
		CollectionName: t.Req.CollectionName,
		DbID:           0, //TODO,not used
		CollectionID:   collID,
		Schema:         schemaBytes,
	}

	err = t.core.DdCreateCollectionReq(&ddReq)
	if err != nil {
		return err
	}

	return nil
}

type DropCollectionReqTask struct {
	baseReqTask
	Req *milvuspb.DropCollectionRequest
}

func (t *DropCollectionReqTask) Type() commonpb.MsgType {
	return t.Req.Base.MsgType
}

func (t *DropCollectionReqTask) Ts() (typeutil.Timestamp, error) {
	return t.Req.Base.Timestamp, nil
}

func (t *DropCollectionReqTask) IgnoreTimeStamp() bool {
	return false
}

func (t *DropCollectionReqTask) Execute() error {
	collMeta, err := t.core.MetaTable.GetCollectionByName(t.Req.CollectionName)
	if err != nil {
		return err
	}
	if err = t.core.InvalidateCollectionMetaCache(t.Req.Base.Timestamp, t.Req.DbName, t.Req.CollectionName); err != nil {
		return err
	}
	err = t.core.MetaTable.DeleteCollection(collMeta.ID)
	if err != nil {
		return err
	}

	//data service should drop segments , which belong to this collection, from the segment manager

	ddReq := internalpb2.DropCollectionRequest{
		Base:           t.Req.Base,
		DbName:         t.Req.DbName,
		CollectionName: t.Req.CollectionName,
		DbID:           0, //not used
		CollectionID:   collMeta.ID,
	}

	err = t.core.DdDropCollectionReq(&ddReq)
	if err != nil {
		return err
	}
	return nil
}

type HasCollectionReqTask struct {
	baseReqTask
	Req           *milvuspb.HasCollectionRequest
	HasCollection bool
}

func (t *HasCollectionReqTask) Type() commonpb.MsgType {
	return t.Req.Base.MsgType
}

func (t *HasCollectionReqTask) Ts() (typeutil.Timestamp, error) {
	return t.Req.Base.Timestamp, nil
}

func (t *HasCollectionReqTask) IgnoreTimeStamp() bool {
	return true
}

func (t *HasCollectionReqTask) Execute() error {
	_, err := t.core.MetaTable.GetCollectionByName(t.Req.CollectionName)
	if err == nil {
		t.HasCollection = true
	} else {
		t.HasCollection = false
	}
	return nil
}

type DescribeCollectionReqTask struct {
	baseReqTask
	Req *milvuspb.DescribeCollectionRequest
	Rsp *milvuspb.DescribeCollectionResponse
}

func (t *DescribeCollectionReqTask) Type() commonpb.MsgType {
	return t.Req.Base.MsgType
}

func (t *DescribeCollectionReqTask) Ts() (typeutil.Timestamp, error) {
	return t.Req.Base.Timestamp, nil
}

func (t *DescribeCollectionReqTask) IgnoreTimeStamp() bool {
	return true
}

func (t *DescribeCollectionReqTask) Execute() error {
	coll, err := t.core.MetaTable.GetCollectionByName(t.Req.CollectionName)
	if err != nil {
		return err
	}
	t.Rsp.Schema = proto.Clone(coll.Schema).(*schemapb.CollectionSchema)
	t.Rsp.CollectionID = coll.ID
	var newField []*schemapb.FieldSchema
	for _, field := range t.Rsp.Schema.Fields {
		if field.FieldID >= StartOfUserFieldID {
			newField = append(newField, field)
		}
	}
	t.Rsp.Schema.Fields = newField
	return nil
}

type ShowCollectionReqTask struct {
	baseReqTask
	Req *milvuspb.ShowCollectionRequest
	Rsp *milvuspb.ShowCollectionResponse
}

func (t *ShowCollectionReqTask) Type() commonpb.MsgType {
	return t.Req.Base.MsgType
}

func (t *ShowCollectionReqTask) Ts() (typeutil.Timestamp, error) {
	return t.Req.Base.Timestamp, nil
}

func (t *ShowCollectionReqTask) IgnoreTimeStamp() bool {
	return true
}

func (t *ShowCollectionReqTask) Execute() error {
	coll, err := t.core.MetaTable.ListCollections()
	if err != nil {
		return err
	}
	t.Rsp.CollectionNames = coll
	return nil
}

type CreatePartitionReqTask struct {
	baseReqTask
	Req *milvuspb.CreatePartitionRequest
}

func (t *CreatePartitionReqTask) Type() commonpb.MsgType {
	return t.Req.Base.MsgType
}

func (t *CreatePartitionReqTask) Ts() (typeutil.Timestamp, error) {
	return t.Req.Base.Timestamp, nil
}

func (t *CreatePartitionReqTask) IgnoreTimeStamp() bool {
	return false
}

func (t *CreatePartitionReqTask) Execute() error {
	collMeta, err := t.core.MetaTable.GetCollectionByName(t.Req.CollectionName)
	if err != nil {
		return err
	}
	partitionID, err := t.core.idAllocator.AllocOne()
	if err != nil {
		return err
	}
	err = t.core.MetaTable.AddPartition(collMeta.ID, t.Req.PartitionName, partitionID)
	if err != nil {
		return err
	}

	ddReq := internalpb2.CreatePartitionRequest{
		Base:           t.Req.Base,
		DbName:         t.Req.DbName,
		CollectionName: t.Req.CollectionName,
		PartitionName:  t.Req.PartitionName,
		DbID:           0, // todo, not used
		CollectionID:   collMeta.ID,
		PartitionID:    partitionID,
	}

	err = t.core.DdCreatePartitionReq(&ddReq)
	if err != nil {
		return err
	}

	return nil
}

type DropPartitionReqTask struct {
	baseReqTask
	Req *milvuspb.DropPartitionRequest
}

func (t *DropPartitionReqTask) Type() commonpb.MsgType {
	return t.Req.Base.MsgType
}

func (t *DropPartitionReqTask) Ts() (typeutil.Timestamp, error) {
	return t.Req.Base.Timestamp, nil
}

func (t *DropPartitionReqTask) IgnoreTimeStamp() bool {
	return false
}

func (t *DropPartitionReqTask) Execute() error {
	coll, err := t.core.MetaTable.GetCollectionByName(t.Req.CollectionName)
	if err != nil {
		return err
	}
	partID, err := t.core.MetaTable.DeletePartition(coll.ID, t.Req.PartitionName)
	if err != nil {
		return err
	}

	ddReq := internalpb2.DropPartitionRequest{
		Base:           t.Req.Base,
		DbName:         t.Req.DbName,
		CollectionName: t.Req.CollectionName,
		PartitionName:  t.Req.PartitionName,
		DbID:           0, //todo,not used
		CollectionID:   coll.ID,
		PartitionID:    partID,
	}

	err = t.core.DdDropPartitionReq(&ddReq)
	if err != nil {
		return err
	}
	return nil
}

type HasPartitionReqTask struct {
	baseReqTask
	Req          *milvuspb.HasPartitionRequest
	HasPartition bool
}

func (t *HasPartitionReqTask) Type() commonpb.MsgType {
	return t.Req.Base.MsgType
}

func (t *HasPartitionReqTask) Ts() (typeutil.Timestamp, error) {
	return t.Req.Base.Timestamp, nil
}

func (t *HasPartitionReqTask) IgnoreTimeStamp() bool {
	return true
}

func (t *HasPartitionReqTask) Execute() error {
	coll, err := t.core.MetaTable.GetCollectionByName(t.Req.CollectionName)
	if err != nil {
		return err
	}
	t.HasPartition = t.core.MetaTable.HasPartition(coll.ID, t.Req.PartitionName)
	return nil
}

type ShowPartitionReqTask struct {
	baseReqTask
	Req *milvuspb.ShowPartitionRequest
	Rsp *milvuspb.ShowPartitionResponse
}

func (t *ShowPartitionReqTask) Type() commonpb.MsgType {
	return t.Req.Base.MsgType
}

func (t *ShowPartitionReqTask) Ts() (typeutil.Timestamp, error) {
	return t.Req.Base.Timestamp, nil
}

func (t *ShowPartitionReqTask) IgnoreTimeStamp() bool {
	return true
}

func (t *ShowPartitionReqTask) Execute() error {
	coll, err := t.core.MetaTable.GetCollectionByID(t.Req.CollectionID)
	if err != nil {
		return err
	}
	if coll.Schema.Name != t.Req.CollectionName {
		return errors.Errorf("collection %s not exist", t.Req.CollectionName)
	}
	for _, partID := range coll.PartitionIDs {
		partMeta, err := t.core.MetaTable.GetPartitionByID(partID)
		if err != nil {
			return err
		}
		t.Rsp.PartitionIDs = append(t.Rsp.PartitionIDs, partMeta.PartitionID)
		t.Rsp.PartitionNames = append(t.Rsp.PartitionNames, partMeta.PartitionName)
	}
	return nil
}

type DescribeSegmentReqTask struct {
	baseReqTask
	Req *milvuspb.DescribeSegmentRequest
	Rsp *milvuspb.DescribeSegmentResponse //TODO,return repeated segment id in the future
}

func (t *DescribeSegmentReqTask) Type() commonpb.MsgType {
	return t.Req.Base.MsgType
}

func (t *DescribeSegmentReqTask) Ts() (typeutil.Timestamp, error) {
	return t.Req.Base.Timestamp, nil
}

func (t *DescribeSegmentReqTask) IgnoreTimeStamp() bool {
	return true
}

func (t *DescribeSegmentReqTask) Execute() error {
	coll, err := t.core.MetaTable.GetCollectionByID(t.Req.CollectionID)
	if err != nil {
		return err
	}
	exist := false
	for _, partID := range coll.PartitionIDs {
		if exist {
			break
		}
		partMeta, err := t.core.MetaTable.GetPartitionByID(partID)
		if err != nil {
			return err
		}
		for _, e := range partMeta.SegmentIDs {
			if e == t.Req.SegmentID {
				exist = true
				break
			}
		}
	}
	if !exist {
		return errors.Errorf("segment id %d not belong to collection id %d", t.Req.SegmentID, t.Req.CollectionID)
	}
	//TODO, get filed_id and index_name from request
	segIdxInfo, err := t.core.MetaTable.GetSegmentIndexInfoByID(t.Req.SegmentID, -1, "")
	if err != nil {
		return err
	}
	t.Rsp.IndexID = segIdxInfo.IndexID
	indexInfo, err := t.core.MetaTable.GetIndexByID(segIdxInfo.IndexID)
	if err != nil {
		return err
	}
	t.Rsp.IndexDescription = &milvuspb.IndexDescription{
		IndexName: Params.DefaultIndexName, // TODO: set index name
		Params:    indexInfo.IndexParams,
	}
	return nil
}

type ShowSegmentReqTask struct {
	baseReqTask
	Req *milvuspb.ShowSegmentRequest
	Rsp *milvuspb.ShowSegmentResponse
}

func (t *ShowSegmentReqTask) Type() commonpb.MsgType {
	return t.Req.Base.MsgType
}

func (t *ShowSegmentReqTask) Ts() (typeutil.Timestamp, error) {
	return t.Req.Base.Timestamp, nil
}

func (t *ShowSegmentReqTask) IgnoreTimeStamp() bool {
	return true
}

func (t *ShowSegmentReqTask) Execute() error {
	coll, err := t.core.MetaTable.GetCollectionByID(t.Req.CollectionID)
	if err != nil {
		return err
	}
	for _, partID := range coll.PartitionIDs {
		partMeta, err := t.core.MetaTable.GetPartitionByID(partID)
		if err != nil {
			return err
		}
		t.Rsp.SegmentIDs = append(t.Rsp.SegmentIDs, partMeta.SegmentIDs...)
	}
	return nil
}

type CreateIndexReqTask struct {
	baseReqTask
	Req *milvuspb.CreateIndexRequest
}

func (t *CreateIndexReqTask) Type() commonpb.MsgType {
	return t.Req.Base.MsgType
}

func (t *CreateIndexReqTask) Ts() (typeutil.Timestamp, error) {
	return t.Req.Base.Timestamp, nil
}

func (t *CreateIndexReqTask) IgnoreTimeStamp() bool {
	return false
}

func (t *CreateIndexReqTask) Execute() error {
	segIDs, field, err := t.core.MetaTable.GetNotIndexedSegments(t.Req.CollectionName, t.Req.FieldName, t.Req.ExtraParams)
	if err != nil {
		return err
	}
	if field.DataType != schemapb.DataType_VECTOR_FLOAT && field.DataType != schemapb.DataType_VECTOR_BINARY {
		return errors.Errorf("field name = %s, data type = %s", t.Req.FieldName, schemapb.DataType_name[int32(field.DataType)])
	}
	for _, seg := range segIDs {
		task := CreateIndexTask{
			core:        t.core,
			segmentID:   seg,
			indexName:   Params.DefaultIndexName, //TODO, get name from request
			fieldSchema: &field,
			indexParams: t.Req.ExtraParams,
		}
		t.core.indexTaskQueue <- &task
	}
	return nil
}

type DescribeIndexReqTask struct {
	baseReqTask
	Req *milvuspb.DescribeIndexRequest
	Rsp *milvuspb.DescribeIndexResponse
}

func (t *DescribeIndexReqTask) Type() commonpb.MsgType {
	return t.Req.Base.MsgType
}

func (t *DescribeIndexReqTask) Ts() (typeutil.Timestamp, error) {
	return t.Req.Base.Timestamp, nil
}

func (t *DescribeIndexReqTask) IgnoreTimeStamp() bool {
	return true
}

func (t *DescribeIndexReqTask) Execute() error {
	idx, err := t.core.MetaTable.GetIndexByName(t.Req.CollectionName, t.Req.FieldName, t.Req.IndexName)
	if err != nil {
		return err
	}
	for _, i := range idx {
		desc := &milvuspb.IndexDescription{
			IndexName: i.IndexName,
			Params:    i.IndexParams,
		}
		t.Rsp.IndexDescriptions = append(t.Rsp.IndexDescriptions, desc)
	}
	return nil
}

type CreateIndexTask struct {
	core        *Core
	segmentID   typeutil.UniqueID
	indexName   string
	fieldSchema *schemapb.FieldSchema
	indexParams []*commonpb.KeyValuePair
}

func (t *CreateIndexTask) BuildIndex() error {
	if t.core.MetaTable.IsSegmentIndexed(t.segmentID, t.fieldSchema, t.indexParams) {
		return nil
	}
	idxID, err := t.core.idAllocator.AllocOne()
	if err != nil {
		return err
	}
	binlogs, err := t.core.GetBinlogFilePathsFromDataServiceReq(t.segmentID, t.fieldSchema.FieldID)
	if err != nil {
		return err
	}
	var bldID typeutil.UniqueID

	if len(t.indexParams) == 0 {
		t.indexParams = make([]*commonpb.KeyValuePair, 0, len(t.fieldSchema.IndexParams))
		for _, p := range t.fieldSchema.IndexParams {
			t.indexParams = append(t.indexParams, &commonpb.KeyValuePair{
				Key:   p.Key,
				Value: p.Value,
			})
		}
	}
	bldID, err = t.core.BuildIndexReq(binlogs, t.fieldSchema.TypeParams, t.indexParams)
	if err != nil {
		return err
	}
	seg := etcdpb.SegmentIndexInfo{
		SegmentID: t.segmentID,
		FieldID:   t.fieldSchema.FieldID,
		IndexID:   idxID,
		BuildID:   bldID,
	}
	idx := etcdpb.IndexInfo{
		IndexName:   t.indexName,
		IndexID:     idxID,
		IndexParams: t.indexParams,
	}
	err = t.core.MetaTable.AddIndex(&seg, &idx)
	return err
}
