package streamrpc

import (
	"context"
	"io"
	"sync"

	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/proto/internalpb"
)

type QueryStreamServer interface {
	Send(*internalpb.RetrieveResults) error
	Context() context.Context
}
type QueryStreamClient interface {
	Recv() (*internalpb.RetrieveResults, error)
	Context() context.Context
	CloseSend() error
}

type ConcurrentQueryStreamServer struct {
	server QueryStreamServer
	mu     sync.Mutex
}

func (s *ConcurrentQueryStreamServer) Send(result *internalpb.RetrieveResults) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.server.Send(result)
}

func (s *ConcurrentQueryStreamServer) Context() context.Context {
	return s.server.Context()
}

func NewConcurrentQueryStreamServer(srv QueryStreamServer) *ConcurrentQueryStreamServer {
	return &ConcurrentQueryStreamServer{
		server: srv,
		mu:     sync.Mutex{},
	}
}

// TODO LOCAL SERVER AND CLIENT FOR STANDALONE
// ONLY FOR TEST
type LocalQueryServer struct {
	grpc.ServerStream

	resultCh chan *internalpb.RetrieveResults
	ctx      context.Context

	finishOnce sync.Once
	errCh      chan error
	mu         sync.Mutex
}

func (s *LocalQueryServer) Send(result *internalpb.RetrieveResults) error {
	select {
	case <-s.ctx.Done():
		return s.ctx.Err()
	default:
		s.resultCh <- result
		return nil
	}
}

func (s *LocalQueryServer) FinishError() error {
	return <-s.errCh
}

func (s *LocalQueryServer) Context() context.Context {
	return s.ctx
}

func (s *LocalQueryServer) FinishSend(err error) error {
	s.finishOnce.Do(func() {
		close(s.resultCh)
		if err != nil {
			s.errCh <- err
		} else {
			s.errCh <- io.EOF
		}
	})
	return nil
}

type LocalQueryClient struct {
	grpc.ClientStream

	server   *LocalQueryServer
	resultCh chan *internalpb.RetrieveResults
	ctx      context.Context
}

func (s *LocalQueryClient) RecvMsg(m interface{}) error {
	// TODO implement me
	panic("implement me")
}

func (s *LocalQueryClient) Recv() (*internalpb.RetrieveResults, error) {
	select {
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	default:
		result, ok := <-s.resultCh
		if !ok {
			return nil, s.server.FinishError()
		}
		return result, nil
	}
}

func (s *LocalQueryClient) Context() context.Context {
	return s.ctx
}

func (s *LocalQueryClient) CloseSend() error {
	return nil
}

func (s *LocalQueryClient) CreateServer() *LocalQueryServer {
	s.server = &LocalQueryServer{
		resultCh: s.resultCh,
		ctx:      s.ctx,
		mu:       sync.Mutex{},
		errCh:    make(chan error, 1),
	}
	return s.server
}

func NewLocalQueryClient(ctx context.Context) *LocalQueryClient {
	return &LocalQueryClient{
		resultCh: make(chan *internalpb.RetrieveResults, 64),
		ctx:      ctx,
	}
}
