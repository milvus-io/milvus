package proxy

import (
	"context"
	"log"
	"net"
	"sync"

	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/servicepb"
	"google.golang.org/grpc"
)

type proxyInstance struct {
	servicepb.UnimplementedMilvusServiceServer
	grpcServer *grpc.Server
	taskSch    *taskScheduler
	taskChan   chan *task
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
}

func (ins *proxyInstance) Insert(ctx context.Context, req *servicepb.RowBatch) (*servicepb.IntegerRangeResponse, error) {
	return &servicepb.IntegerRangeResponse{}, nil
}

func (ins *proxyInstance) StartGrpcServer() error {
	// TODO: use address in config instead
	lis, err := net.Listen("tcp", "127.0.0.1")
	if err != nil {
		return err
	}
	go func() {
		ins.wg.Add(1)
		defer ins.wg.Done()
		server := grpc.NewServer()
		servicepb.RegisterMilvusServiceServer(server, ins)
		err := server.Serve(lis)
		if err != nil {
			log.Fatalf("Proxy grpc server fatal error=%v", err)
		}
	}()
	return nil
}

func (ins *proxyInstance) restartSchedulerRoutine(bufSize int) error {
	ins.taskChan = make(chan *task, bufSize)

	go func() {
		for {
			select {
			case t := <-ins.taskChan:
				switch (*t).Type() {
				case internalpb.MsgType_kInsert:
					ins.taskSch.DmQueue.Enqueue(t)
				default:
					return
				}
			default:
				return
			}
		}
	}()

	return nil
}

func (ins *proxyInstance) restartForwardRoutine() error {

	return nil
}

func startProxyInstance(ins *proxyInstance) error {
	if err := ins.restartSchedulerRoutine(1024); err != nil {
		return err
	}
	if err := ins.restartForwardRoutine(); err != nil {
		return err
	}

	return ins.StartGrpcServer()
}

func StartProxyInstance() error {
	ins := &proxyInstance{}
	err := startProxyInstance(ins)
	if err != nil {
		return nil
	}

	ins.wg.Wait()
	return nil
}
