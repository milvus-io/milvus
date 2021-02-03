package datanode

import (
	"bytes"
	"encoding/binary"
	"log"
	"math"
	"math/rand"
	"os"
	"strconv"
	"testing"

	"go.etcd.io/etcd/clientv3"

	etcdkv "github.com/zilliztech/milvus-distributed/internal/kv/etcd"

	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/etcdpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/proto/masterpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
)

func makeNewChannelNames(names []string, suffix string) []string {
	var ret []string
	for _, name := range names {
		ret = append(ret, name+suffix)
	}
	return ret
}

func refreshChannelNames() {
	Params.DDChannelNames = []string{"datanode-test"}
	Params.SegmentStatisticsChannelName = "segtment-statistics"
	Params.CompleteFlushChannelName = "flush-completed"
	Params.InsertChannelNames = []string{"intsert-a-1", "insert-b-1"}
	Params.TimeTickChannelName = "hard-timetick"
	suffix := "-test-data-node" + strconv.FormatInt(rand.Int63n(100), 10)
	Params.DDChannelNames = makeNewChannelNames(Params.DDChannelNames, suffix)
	Params.InsertChannelNames = makeNewChannelNames(Params.InsertChannelNames, suffix)
}

func TestMain(m *testing.M) {
	Params.Init()

	refreshChannelNames()
	exitCode := m.Run()
	os.Exit(exitCode)
}

func newMetaTable() *metaTable {
	etcdClient, _ := clientv3.New(clientv3.Config{Endpoints: []string{Params.EtcdAddress}})

	etcdKV := etcdkv.NewEtcdKV(etcdClient, Params.MetaRootPath)
	mt, _ := NewMetaTable(etcdKV)
	return mt
}

func clearEtcd(rootPath string) error {
	etcdAddr := Params.EtcdAddress
	etcdClient, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdAddr}})
	if err != nil {
		return err
	}
	etcdKV := etcdkv.NewEtcdKV(etcdClient, rootPath)

	err = etcdKV.RemoveWithPrefix("writer/segment")
	if err != nil {
		return err
	}
	_, _, err = etcdKV.LoadWithPrefix("writer/segment")
	if err != nil {
		return err
	}
	log.Println("Clear ETCD with prefix writer/segment ")

	err = etcdKV.RemoveWithPrefix("writer/ddl")
	if err != nil {
		return err
	}
	_, _, err = etcdKV.LoadWithPrefix("writer/ddl")
	if err != nil {
		return err
	}
	log.Println("Clear ETCD with prefix writer/ddl")
	return nil

}

type (
	Factory interface {
	}

	MetaFactory struct {
	}

	DataFactory struct {
		rawData []byte
	}

	AllocatorFactory struct {
		ID UniqueID
	}

	MasterServiceFactory struct {
		ID             UniqueID
		collectionName string
		collectionID   UniqueID
	}
)

func (mf *MetaFactory) CollectionMetaFactory(collectionID UniqueID, collectionName string) *etcdpb.CollectionMeta {
	sch := schemapb.CollectionSchema{
		Name:        collectionName,
		Description: "test collection by meta factory",
		AutoID:      true,
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:     0,
				Name:        "RowID",
				Description: "RowID field",
				DataType:    schemapb.DataType_INT64,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   "f0_tk1",
						Value: "f0_tv1",
					},
				},
			},
			{
				FieldID:     1,
				Name:        "Timestamp",
				Description: "Timestamp field",
				DataType:    schemapb.DataType_INT64,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   "f1_tk1",
						Value: "f1_tv1",
					},
				},
			},
			{
				FieldID:     100,
				Name:        "float_vector_field",
				Description: "field 100",
				DataType:    schemapb.DataType_VECTOR_FLOAT,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   "dim",
						Value: "2",
					},
				},
				IndexParams: []*commonpb.KeyValuePair{
					{
						Key:   "indexkey",
						Value: "indexvalue",
					},
				},
			},
			{
				FieldID:     101,
				Name:        "binary_vector_field",
				Description: "field 101",
				DataType:    schemapb.DataType_VECTOR_BINARY,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   "dim",
						Value: "32",
					},
				},
				IndexParams: []*commonpb.KeyValuePair{
					{
						Key:   "indexkey",
						Value: "indexvalue",
					},
				},
			},
			{
				FieldID:     102,
				Name:        "bool_field",
				Description: "field 102",
				DataType:    schemapb.DataType_BOOL,
				TypeParams:  []*commonpb.KeyValuePair{},
				IndexParams: []*commonpb.KeyValuePair{},
			},
			{
				FieldID:     103,
				Name:        "int8_field",
				Description: "field 103",
				DataType:    schemapb.DataType_INT8,
				TypeParams:  []*commonpb.KeyValuePair{},
				IndexParams: []*commonpb.KeyValuePair{},
			},
			{
				FieldID:     104,
				Name:        "int16_field",
				Description: "field 104",
				DataType:    schemapb.DataType_INT16,
				TypeParams:  []*commonpb.KeyValuePair{},
				IndexParams: []*commonpb.KeyValuePair{},
			},
			{
				FieldID:     105,
				Name:        "int32_field",
				Description: "field 105",
				DataType:    schemapb.DataType_INT32,
				TypeParams:  []*commonpb.KeyValuePair{},
				IndexParams: []*commonpb.KeyValuePair{},
			},
			{
				FieldID:     106,
				Name:        "int64_field",
				Description: "field 106",
				DataType:    schemapb.DataType_INT64,
				TypeParams:  []*commonpb.KeyValuePair{},
				IndexParams: []*commonpb.KeyValuePair{},
			},
			{
				FieldID:     107,
				Name:        "float32_field",
				Description: "field 107",
				DataType:    schemapb.DataType_FLOAT,
				TypeParams:  []*commonpb.KeyValuePair{},
				IndexParams: []*commonpb.KeyValuePair{},
			},
			{
				FieldID:     108,
				Name:        "float64_field",
				Description: "field 108",
				DataType:    schemapb.DataType_DOUBLE,
				TypeParams:  []*commonpb.KeyValuePair{},
				IndexParams: []*commonpb.KeyValuePair{},
			},
		},
	}

	collection := etcdpb.CollectionMeta{
		ID:            collectionID,
		Schema:        &sch,
		CreateTime:    Timestamp(1),
		SegmentIDs:    make([]UniqueID, 0),
		PartitionTags: make([]string, 0),
	}
	return &collection
}

func NewDataFactory() *DataFactory {
	return &DataFactory{rawData: GenRowData()}
}

func GenRowData() (rawData []byte) {
	const DIM = 2
	const N = 1

	// Float vector
	var fvector = [DIM]float32{1, 2}
	for _, ele := range fvector {
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, math.Float32bits(ele))
		rawData = append(rawData, buf...)
	}

	// Binary vector
	// Dimension of binary vector is 32
	// size := 4,  = 32 / 8
	var bvector = []byte{255, 255, 255, 0}
	rawData = append(rawData, bvector...)

	// Bool
	var fieldBool = true
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.LittleEndian, fieldBool); err != nil {
		panic(err)
	}

	rawData = append(rawData, buf.Bytes()...)

	// int8
	var dataInt8 int8 = 100
	bint8 := new(bytes.Buffer)
	if err := binary.Write(bint8, binary.LittleEndian, dataInt8); err != nil {
		panic(err)
	}
	rawData = append(rawData, bint8.Bytes()...)

	// int16
	var dataInt16 int16 = 200
	bint16 := new(bytes.Buffer)
	if err := binary.Write(bint16, binary.LittleEndian, dataInt16); err != nil {
		panic(err)
	}
	rawData = append(rawData, bint16.Bytes()...)

	// int32
	var dataInt32 int32 = 300
	bint32 := new(bytes.Buffer)
	if err := binary.Write(bint32, binary.LittleEndian, dataInt32); err != nil {
		panic(err)
	}
	rawData = append(rawData, bint32.Bytes()...)

	// int64
	var dataInt64 int64 = 400
	bint64 := new(bytes.Buffer)
	if err := binary.Write(bint64, binary.LittleEndian, dataInt64); err != nil {
		panic(err)
	}
	rawData = append(rawData, bint64.Bytes()...)

	// float32
	var datafloat float32 = 1.1
	bfloat32 := new(bytes.Buffer)
	if err := binary.Write(bfloat32, binary.LittleEndian, datafloat); err != nil {
		panic(err)
	}
	rawData = append(rawData, bfloat32.Bytes()...)

	// float64
	var datafloat64 float64 = 2.2
	bfloat64 := new(bytes.Buffer)
	if err := binary.Write(bfloat64, binary.LittleEndian, datafloat64); err != nil {
		panic(err)
	}
	rawData = append(rawData, bfloat64.Bytes()...)
	log.Println("Rawdata length:", len(rawData))
	return
}

// n: number of TsinsertMsgs to generate
func (df *DataFactory) GetMsgStreamTsInsertMsgs(n int) (inMsgs []msgstream.TsMsg) {
	for i := 0; i < n; i++ {
		var msg msgstream.TsMsg = &msgstream.InsertMsg{
			BaseMsg: msgstream.BaseMsg{
				HashValues: []uint32{uint32(i)},
			},
			InsertRequest: internalpb2.InsertRequest{
				Base: &commonpb.MsgBase{
					MsgType:   commonpb.MsgType_kInsert,
					MsgID:     0, // GOOSE TODO
					Timestamp: Timestamp(i + 1000),
					SourceID:  0,
				},
				CollectionName: "col1", // GOOSE TODO
				PartitionName:  "default",
				SegmentID:      1,   // GOOSE TODO
				ChannelID:      "0", // GOOSE TODO
				Timestamps:     []Timestamp{Timestamp(i + 1000)},
				RowIDs:         []UniqueID{UniqueID(i)},
				RowData:        []*commonpb.Blob{{Value: df.rawData}},
			},
		}
		inMsgs = append(inMsgs, msg)
	}
	return
}

// n: number of insertMsgs to generate
func (df *DataFactory) GetMsgStreamInsertMsgs(n int) (inMsgs []*msgstream.InsertMsg) {
	for i := 0; i < n; i++ {
		var msg = &msgstream.InsertMsg{
			BaseMsg: msgstream.BaseMsg{
				HashValues: []uint32{uint32(i)},
			},
			InsertRequest: internalpb2.InsertRequest{
				Base: &commonpb.MsgBase{
					MsgType:   commonpb.MsgType_kInsert,
					MsgID:     0, // GOOSE TODO
					Timestamp: Timestamp(i + 1000),
					SourceID:  0,
				},
				CollectionName: "col1", // GOOSE TODO
				PartitionName:  "default",
				SegmentID:      1,   // GOOSE TODO
				ChannelID:      "0", // GOOSE TODO
				Timestamps:     []Timestamp{Timestamp(i + 1000)},
				RowIDs:         []UniqueID{UniqueID(i)},
				RowData:        []*commonpb.Blob{{Value: df.rawData}},
			},
		}
		inMsgs = append(inMsgs, msg)
	}
	return
}

func NewAllocatorFactory(id ...UniqueID) *AllocatorFactory {
	f := &AllocatorFactory{}
	if len(id) == 1 {
		f.ID = id[0]
	}
	return f
}

func (alloc AllocatorFactory) setID(id UniqueID) {
	alloc.ID = id
}

func (alloc AllocatorFactory) allocID() (UniqueID, error) {
	if alloc.ID == 0 {
		return UniqueID(0), nil // GOOSE TODO: random ID generating
	}
	return alloc.ID, nil
}

func (m *MasterServiceFactory) setID(id UniqueID) {
	m.ID = id // GOOSE TODO: random ID generator
}

func (m *MasterServiceFactory) setCollectionID(id UniqueID) {
	m.collectionID = id
}

func (m *MasterServiceFactory) setCollectionName(name string) {
	m.collectionName = name
}

func (m *MasterServiceFactory) AllocID(in *masterpb.IDRequest) (*masterpb.IDResponse, error) {
	resp := &masterpb.IDResponse{
		Status: &commonpb.Status{},
		ID:     m.ID,
	}
	return resp, nil
}

func (m *MasterServiceFactory) ShowCollections(in *milvuspb.ShowCollectionRequest) (*milvuspb.ShowCollectionResponse, error) {
	resp := &milvuspb.ShowCollectionResponse{
		Status:          &commonpb.Status{},
		CollectionNames: []string{m.collectionName},
	}
	return resp, nil

}
func (m *MasterServiceFactory) DescribeCollection(in *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	f := MetaFactory{}
	meta := f.CollectionMetaFactory(m.collectionID, m.collectionName)
	resp := &milvuspb.DescribeCollectionResponse{
		Status:       &commonpb.Status{},
		CollectionID: m.collectionID,
		Schema:       meta.Schema,
	}
	return resp, nil
}

func (m *MasterServiceFactory) GetComponentStates() (*internalpb2.ComponentStates, error) {
	return &internalpb2.ComponentStates{
		State:              &internalpb2.ComponentInfo{},
		SubcomponentStates: make([]*internalpb2.ComponentInfo, 0),
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
		},
	}, nil
}
