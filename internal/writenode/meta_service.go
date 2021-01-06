package writenode

import (
	"context"
	"fmt"
	"log"
	"path"
	"reflect"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	"go.etcd.io/etcd/clientv3"

	etcdkv "github.com/zilliztech/milvus-distributed/internal/kv/etcd"
	"github.com/zilliztech/milvus-distributed/internal/proto/etcdpb"
)

type metaService struct {
	ctx     context.Context
	kvBase  *etcdkv.EtcdKV
	replica collectionReplica
}

func newMetaService(ctx context.Context, replica collectionReplica) *metaService {
	ETCDAddr := Params.EtcdAddress
	MetaRootPath := Params.MetaRootPath

	cli, _ := clientv3.New(clientv3.Config{
		Endpoints:   []string{ETCDAddr},
		DialTimeout: 5 * time.Second,
	})

	return &metaService{
		ctx:     ctx,
		kvBase:  etcdkv.NewEtcdKV(cli, MetaRootPath),
		replica: replica,
	}
}

func (mService *metaService) start() {
	// init from meta
	err := mService.loadCollections()
	if err != nil {
		log.Fatal("metaService loadCollections failed")
	}
}

func GetCollectionObjID(key string) string {
	ETCDRootPath := Params.MetaRootPath

	prefix := path.Join(ETCDRootPath, CollectionPrefix) + "/"
	return strings.TrimPrefix(key, prefix)
}

func isCollectionObj(key string) bool {
	ETCDRootPath := Params.MetaRootPath

	prefix := path.Join(ETCDRootPath, CollectionPrefix) + "/"
	prefix = strings.TrimSpace(prefix)
	index := strings.Index(key, prefix)

	return index == 0
}

func isSegmentObj(key string) bool {
	ETCDRootPath := Params.MetaRootPath

	prefix := path.Join(ETCDRootPath, SegmentPrefix) + "/"
	prefix = strings.TrimSpace(prefix)
	index := strings.Index(key, prefix)

	return index == 0
}

func printCollectionStruct(obj *etcdpb.CollectionMeta) {
	v := reflect.ValueOf(obj)
	v = reflect.Indirect(v)
	typeOfS := v.Type()

	for i := 0; i < v.NumField(); i++ {
		if typeOfS.Field(i).Name == "GrpcMarshalString" {
			continue
		}
		fmt.Printf("Field: %s\tValue: %v\n", typeOfS.Field(i).Name, v.Field(i).Interface())
	}
}

func (mService *metaService) processCollectionCreate(id string, value string) {
	//println(fmt.Sprintf("Create Collection:$%s$", id))

	col := mService.collectionUnmarshal(value)
	if col != nil {
		schema := col.Schema
		schemaBlob := proto.MarshalTextString(schema)
		err := mService.replica.addCollection(col.ID, schemaBlob)
		if err != nil {
			log.Println(err)
		}
	}
}

func (mService *metaService) loadCollections() error {
	keys, values, err := mService.kvBase.LoadWithPrefix(CollectionPrefix)
	if err != nil {
		return err
	}

	for i := range keys {
		objID := GetCollectionObjID(keys[i])
		mService.processCollectionCreate(objID, values[i])
	}

	return nil
}

//----------------------------------------------------------------------- Unmarshal and Marshal
func (mService *metaService) collectionUnmarshal(value string) *etcdpb.CollectionMeta {
	col := etcdpb.CollectionMeta{}
	err := proto.UnmarshalText(value, &col)
	if err != nil {
		log.Println(err)
		return nil
	}
	return &col
}

func (mService *metaService) collectionMarshal(col *etcdpb.CollectionMeta) string {
	value := proto.MarshalTextString(col)
	if value == "" {
		log.Println("marshal collection failed")
		return ""
	}
	return value
}
