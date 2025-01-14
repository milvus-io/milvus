package streamrpc

import (
	"context"
	"io"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/proto/internalpb"
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

type RetrieveResultCache struct {
	result *internalpb.RetrieveResults
	size   int
	cap    int
}

func (c *RetrieveResultCache) Put(result *internalpb.RetrieveResults) {
	if c.result == nil {
		c.result = result
		c.size = proto.Size(result)
		return
	}

	c.merge(result)
}

func (c *RetrieveResultCache) Flush() *internalpb.RetrieveResults {
	result := c.result
	c.result = nil
	c.size = 0
	return result
}

func (c *RetrieveResultCache) Alloc(result *internalpb.RetrieveResults) bool {
	return proto.Size(result)+c.size <= c.cap
}

func (c *RetrieveResultCache) IsFull() bool {
	return c.size > c.cap
}

func (c *RetrieveResultCache) IsEmpty() bool {
	return c.size == 0
}

func (c *RetrieveResultCache) merge(result *internalpb.RetrieveResults) {
	switch result.GetIds().GetIdField().(type) {
	case *schemapb.IDs_IntId:
		c.result.GetIds().GetIntId().Data = append(c.result.GetIds().GetIntId().GetData(), result.GetIds().GetIntId().GetData()...)
	case *schemapb.IDs_StrId:
		c.result.GetIds().GetStrId().Data = append(c.result.GetIds().GetStrId().GetData(), result.GetIds().GetStrId().GetData()...)
	}
	c.result.AllRetrieveCount = c.result.AllRetrieveCount + result.AllRetrieveCount
	c.result.CostAggregation = mergeCostAggregation(c.result.GetCostAggregation(), result.GetCostAggregation())
	c.size = proto.Size(c.result)
}

func mergeCostAggregation(a *internalpb.CostAggregation, b *internalpb.CostAggregation) *internalpb.CostAggregation {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}

	return &internalpb.CostAggregation{
		ResponseTime:         a.GetResponseTime() + b.GetResponseTime(),
		ServiceTime:          a.GetServiceTime() + b.GetServiceTime(),
		TotalNQ:              a.GetTotalNQ() + b.GetTotalNQ(),
		TotalRelatedDataSize: a.GetTotalRelatedDataSize() + b.GetTotalRelatedDataSize(),
	}
}

// Merge result by size and time.
type ResultCacheServer struct {
	srv   QueryStreamServer
	cache *RetrieveResultCache
	mu    sync.Mutex
}

func NewResultCacheServer(srv QueryStreamServer, cap int) *ResultCacheServer {
	return &ResultCacheServer{
		srv:   srv,
		cache: &RetrieveResultCache{cap: cap},
	}
}

func (s *ResultCacheServer) Send(result *internalpb.RetrieveResults) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.cache.Alloc(result) && !s.cache.IsEmpty() {
		result := s.cache.Flush()
		if err := s.srv.Send(result); err != nil {
			return err
		}
	}

	s.cache.Put(result)
	if s.cache.IsFull() {
		result := s.cache.Flush()
		if err := s.srv.Send(result); err != nil {
			return err
		}
	}
	return nil
}

func (s *ResultCacheServer) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	result := s.cache.Flush()
	if result == nil {
		return nil
	}

	if err := s.srv.Send(result); err != nil {
		return err
	}
	return nil
}

func (s *ResultCacheServer) Context() context.Context {
	return s.srv.Context()
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
