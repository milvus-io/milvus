package datanode

import (
	"context"
	"fmt"
	"log"
	"reflect"

	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/etcdpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
)

type metaService struct {
	ctx          context.Context
	replica      collectionReplica
	masterClient MasterServiceInterface
}

func newMetaService(ctx context.Context, replica collectionReplica, m MasterServiceInterface) *metaService {
	return &metaService{
		ctx:          ctx,
		replica:      replica,
		masterClient: m,
	}
}

func (mService *metaService) init() {
	log.Println("Initing meta ...")
	err := mService.loadCollections()
	if err != nil {
		log.Fatal("metaService init failed:", err)
	}
}

func (mService *metaService) loadCollections() error {
	names, err := mService.getCollectionNames()
	if err != nil {
		return err
	}

	for _, name := range names {
		err := mService.createCollection(name)
		if err != nil {
			return err
		}
	}
	return nil
}

func (mService *metaService) getCollectionNames() ([]string, error) {
	req := &milvuspb.ShowCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_kShowCollections,
			MsgID:     0, //GOOSE TODO
			Timestamp: 0, // GOOSE TODO
			SourceID:  Params.NodeID,
		},
		DbName: "default", // GOOSE TODO
	}

	response, err := mService.masterClient.ShowCollections(req)
	if err != nil {
		return nil, errors.Errorf("Get collection names from master service wrong: %v", err)
	}
	return response.GetCollectionNames(), nil
}

func (mService *metaService) createCollection(name string) error {
	log.Println("Describing collections")
	req := &milvuspb.DescribeCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_kDescribeCollection,
			MsgID:     0, //GOOSE TODO
			Timestamp: 0, // GOOSE TODO
			SourceID:  Params.NodeID,
		},
		DbName:         "default", // GOOSE TODO
		CollectionName: name,
	}

	response, err := mService.masterClient.DescribeCollection(req)
	if err != nil {
		return errors.Errorf("Describe collection %v from master service wrong: %v", name, err)
	}

	err = mService.replica.addCollection(response.GetCollectionID(), response.GetSchema())
	if err != nil {
		return errors.Errorf("Add collection %v into collReplica wrong: %v", name, err)
	}

	return nil
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
