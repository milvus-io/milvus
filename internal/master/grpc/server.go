package grpc

import (
	"context"
	"fmt"
	"net"
	"strconv"

	"github.com/zilliztech/milvus-distributed/internal/conf"
	"github.com/zilliztech/milvus-distributed/internal/master/controller"
	masterpb "github.com/zilliztech/milvus-distributed/internal/proto/master"
	messagepb "github.com/zilliztech/milvus-distributed/internal/proto/message"
	"github.com/zilliztech/milvus-distributed/internal/master/kv"
	"google.golang.org/grpc"
)

func Server(ch chan *messagepb.Mapping, errch chan error, kvbase kv.Base) {
	defaultGRPCPort := ":"
	defaultGRPCPort += strconv.FormatInt(int64(conf.Config.Master.Port), 10)
	lis, err := net.Listen("tcp", defaultGRPCPort)
	if err != nil {
		//		log.Fatal("failed to listen: %v", err)
		errch <- err
		return
	}
	s := grpc.NewServer()
	masterpb.RegisterMasterServer(s, GRPCMasterServer{CreateRequest: ch, kvbase: kvbase})
	if err := s.Serve(lis); err != nil {
		//		log.Fatalf("failed to serve: %v", err)
		errch <- err
		return
	}
}

type GRPCMasterServer struct {
	CreateRequest chan *messagepb.Mapping
	kvbase        kv.Base
}

func (ms GRPCMasterServer) CreateCollection(ctx context.Context, in *messagepb.Mapping) (*messagepb.Status, error) {
	//	ms.CreateRequest <- in2
	fmt.Println("Handle a new create collection request")
	err := controller.WriteCollection2Datastore(in, ms.kvbase)
	if err != nil {
		return &messagepb.Status{
			ErrorCode: 100,
			Reason:    "",
		}, err
	}
	return &messagepb.Status{
		ErrorCode: 0,
		Reason:    "",
	}, nil
}

func (ms GRPCMasterServer) CreateIndex(ctx context.Context, in *messagepb.IndexParam) (*messagepb.Status, error) {
	fmt.Println("Handle a new create index request")
	err := controller.UpdateCollectionIndex(in, ms.kvbase)
	if err != nil {
		return &messagepb.Status{
			ErrorCode: 100,
			Reason:    "",
		}, err
	}
	return &messagepb.Status{
		ErrorCode: 0,
		Reason:    "",
	}, nil
}
