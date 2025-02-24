package streamrpc

import (
	"context"
	"io"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
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
		TotalNQ:              a.GetTotalNQ(),
		TotalRelatedDataSize: a.GetTotalRelatedDataSize() + b.GetTotalRelatedDataSize(),
	}
}

// Merge result by size and time.
type ResultCacheServer struct {
	mu         sync.Mutex
	srv        QueryStreamServer
	cache      *RetrieveResultCache
	maxMsgSize int
}

func NewResultCacheServer(srv QueryStreamServer, cap int, maxMsgSize int) *ResultCacheServer {
	return &ResultCacheServer{
		srv:        srv,
		cache:      &RetrieveResultCache{cap: cap},
		maxMsgSize: maxMsgSize,
	}
}

func (s *ResultCacheServer) splitMsgToMaxSize(result *internalpb.RetrieveResults) []*internalpb.RetrieveResults {
	newpks := make([]*schemapb.IDs, 0)
	switch result.GetIds().GetIdField().(type) {
	case *schemapb.IDs_IntId:
		pks := result.GetIds().GetIntId().Data
		batch := s.maxMsgSize / 8
		print(batch)
		for start := 0; start < len(pks); start += batch {
			newpks = append(newpks, &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: pks[start:min(start+batch, len(pks))]}}})
		}

	case *schemapb.IDs_StrId:
		pks := result.GetIds().GetStrId().Data
		start := 0
		size := 0
		for i, pk := range pks {
			if size+len(pk) > s.maxMsgSize {
				newpks = append(newpks, &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: pks[start:i]}}})
				start = i
				size = 0
			}
			size += len(pk)
		}
		if size > 0 {
			newpks = append(newpks, &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: pks[start:]}}})
		}
	}

	results := make([]*internalpb.RetrieveResults, len(newpks))
	for i, pks := range newpks {
		results[i] = &internalpb.RetrieveResults{
			Status: merr.Status(nil),
			Ids:    pks,
		}
	}
	results[len(results)-1].AllRetrieveCount = result.AllRetrieveCount
	results[len(results)-1].CostAggregation = result.CostAggregation
	return results
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
	if s.cache.IsFull() && s.cache.size <= s.maxMsgSize {
		result := s.cache.Flush()
		if err := s.srv.Send(result); err != nil {
			return err
		}
	} else if s.cache.IsFull() && s.cache.size > s.maxMsgSize {
		results := s.splitMsgToMaxSize(s.cache.Flush())
		if proto.Size(results[len(results)-1]) < s.cache.cap {
			s.cache.Put(results[len(results)-1])
			results = results[:len(results)-1]
		}

		for _, result := range results {
			if err := s.srv.Send(result); err != nil {
				return err
			}
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
