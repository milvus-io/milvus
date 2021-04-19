package grpc

import (
	"context"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
	"github.com/zilliztech/milvus-distributed/internal/proto/servicepb"
	"net"
	"strconv"

	"github.com/zilliztech/milvus-distributed/internal/conf"
	"github.com/zilliztech/milvus-distributed/internal/master/kv"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/masterpb"
	"google.golang.org/grpc"
)

func Server(ch chan *schemapb.CollectionSchema, errch chan error, kvbase kv.Base) {
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
	CreateRequest chan *schemapb.CollectionSchema
	kvbase        kv.Base
}

func (ms GRPCMasterServer) CreateCollection(ctx context.Context, in *internalpb.CreateCollectionRequest) (*commonpb.Status, error) {
	return &commonpb.Status{
		ErrorCode: 0,
		Reason:    "",
	}, nil
}

func (ms GRPCMasterServer) DropCollection(ctx context.Context, in *internalpb.DropCollectionRequest) (*commonpb.Status, error) {
	return &commonpb.Status{
		ErrorCode: 0,
		Reason:    "",
	}, nil
}

func (ms GRPCMasterServer) HasCollection(ctx context.Context, in *internalpb.HasCollectionRequest) (*servicepb.BoolResponse, error) {
	return &servicepb.BoolResponse{
		Status: &commonpb.Status{
			ErrorCode: 0,
			Reason:    "",
		},
		Value: true,
	},nil
}

func (ms GRPCMasterServer) DescribeCollection(ctx context.Context, in *internalpb.DescribeCollectionRequest) (*servicepb.CollectionDescription, error) {
	return &servicepb.CollectionDescription{
		Status: &commonpb.Status{
			ErrorCode: 0,
			Reason:    "",
		},
	},nil
}

func (ms GRPCMasterServer) ShowCollections(ctx context.Context, in *internalpb.ShowCollectionRequest) (*servicepb.StringListResponse, error) {
	return &servicepb.StringListResponse{
		Status: &commonpb.Status{
			ErrorCode: 0,
			Reason:    "",
		},
	},nil
}


func (ms GRPCMasterServer) CreatePartition(ctx context.Context, in *internalpb.CreatePartitionRequest) (*commonpb.Status, error) {
	return &commonpb.Status{
		ErrorCode: 0,
		Reason:    "",
	}, nil
}


func (ms GRPCMasterServer) DropPartition(ctx context.Context, in *internalpb.DropPartitionRequest) (*commonpb.Status, error) {
	return &commonpb.Status{
		ErrorCode: 0,
		Reason:    "",
	}, nil
}

func (ms GRPCMasterServer) HasPartition(ctx context.Context, in *internalpb.HasPartitionRequest) (*servicepb.BoolResponse, error) {
	return &servicepb.BoolResponse{
		Status: &commonpb.Status{
			ErrorCode: 0,
			Reason:    "",
		},
		Value: true,
	},nil
}

func (ms GRPCMasterServer) DescribePartition(ctx context.Context, in *internalpb.DescribePartitionRequest) (*servicepb.PartitionDescription, error) {
	return &servicepb.PartitionDescription{
		Status: &commonpb.Status{
			ErrorCode: 0,
			Reason:    "",
		},
	},nil
}

func (ms GRPCMasterServer) ShowPartitions(ctx context.Context, in *internalpb.ShowPartitionRequest) (*servicepb.StringListResponse, error) {
	return &servicepb.StringListResponse{
		Status: &commonpb.Status{
			ErrorCode: 0,
			Reason:    "",
		},
	},nil
}

//func (ms GRPCMasterServer) CreateCollection(ctx context.Context, in *messagepb.Mapping) (*messagepb.Status, error) {
//	//	ms.CreateRequest <- in2
//	fmt.Println("Handle a new create collection request")
//	err := controller.WriteCollection2Datastore(in, ms.kvbase)
//	if err != nil {
//		return &messagepb.Status{
//			ErrorCode: 100,
//			Reason:    "",
//		}, err
//	}
//	return &messagepb.Status{
//		ErrorCode: 0,
//		Reason:    "",
//	}, nil
//}

//func (ms GRPCMasterServer) CreateIndex(ctx context.Context, in *messagepb.IndexParam) (*messagepb.Status, error) {
//	fmt.Println("Handle a new create index request")
//	err := controller.UpdateCollectionIndex(in, ms.kvbase)
//	if err != nil {
//		return &messagepb.Status{
//			ErrorCode: 100,
//			Reason:    "",
//		}, err
//	}
//	return &messagepb.Status{
//		ErrorCode: 0,
//		Reason:    "",
//	}, nil
//}
