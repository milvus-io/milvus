// Copyright 2016 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package pd

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/czs007/suvlim/errors"
	"github.com/czs007/suvlim/pkg/pdpb"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)


// Client is a PD (Placement Driver) client.
// It should not be used after calling Close().
type Client interface {
	// GetClusterID gets the cluster ID from PD.
	GetClusterID(ctx context.Context) uint64
	// GetMemberInfo gets the members Info from PD
	//GetMemberInfo(ctx context.Context) ([]*pdpb.Member, error)
	// GetLeaderAddr returns current leader's address. It returns "" before
	// syncing leader from server.
	GetLeaderAddr() string
	// GetTS gets a timestamp from PD.
	GetTS(ctx context.Context) (int64, int64, error)
	// GetTSAsync gets a timestamp from PD, without block the caller.
	GetTSAsync(ctx context.Context) TSFuture
	Close()
}

type tsoRequest struct {
	start    time.Time
	ctx      context.Context
	done     chan error
	physical int64
	logical  int64
}

const (
	defaultPDTimeout      = 3 * time.Second
	dialTimeout           = 3 * time.Second
	updateLeaderTimeout   = time.Second // Use a shorter timeout to recover faster from network isolation.
	maxMergeTSORequests   = 10000       // should be higher if client is sending requests in burst
	maxInitClusterRetries = 100
)

var (
	// errFailInitClusterID is returned when failed to load clusterID from all supplied PD addresses.
	errFailInitClusterID = errors.New("[pd] failed to get cluster id")
	// errClosing is returned when request is canceled when client is closing.
	errClosing = errors.New("[pd] closing")
	// errTSOLength is returned when the number of response timestamps is inconsistent with request.
	errTSOLength = errors.New("[pd] tso length in rpc response is incorrect")
)

type client struct {
	*baseClient
	tsoRequests chan *tsoRequest

	lastPhysical int64
	lastLogical  int64

	tsDeadlineCh chan deadline
}

// NewClient creates a PD client.
func NewClient(pdAddrs []string, security SecurityOption, opts ...ClientOption) (Client, error) {
	return NewClientWithContext(context.Background(), pdAddrs, security, opts...)
}

// NewClientWithContext creates a PD client with context.
func NewClientWithContext(ctx context.Context, pdAddrs []string, security SecurityOption, opts ...ClientOption) (Client, error) {
	log.Info("[pd] create pd client with endpoints", zap.Strings("pd-address", pdAddrs))
	base, err := newBaseClient(ctx, addrsToUrls(pdAddrs), security, opts...)
	if err != nil {
		return nil, err
	}
	c := &client{
		baseClient:   base,
		tsoRequests:  make(chan *tsoRequest, maxMergeTSORequests),
		tsDeadlineCh: make(chan deadline, 1),
	}

	c.wg.Add(2)
	go c.tsLoop()
	go c.tsCancelLoop()

	return c, nil
}

type deadline struct {
	timer  <-chan time.Time
	done   chan struct{}
	cancel context.CancelFunc
}

func (c *client) tsCancelLoop() {
	defer c.wg.Done()

	ctx, cancel := context.WithCancel(c.ctx)
	defer cancel()

	for {
		select {
		case d := <-c.tsDeadlineCh:
			select {
			case <-d.timer:
				log.Error("tso request is canceled due to timeout")
				d.cancel()
			case <-d.done:
			case <-ctx.Done():
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func (c *client) checkStreamTimeout(loopCtx context.Context, cancel context.CancelFunc, createdCh chan struct{}) {
	select {
	case <-time.After(c.timeout):
		cancel()
	case <-createdCh:
		return
	case <-loopCtx.Done():
		return
	}
}


func (c *client) tsLoop() {
	defer c.wg.Done()

	loopCtx, loopCancel := context.WithCancel(c.ctx)
	defer loopCancel()

	defaultSize := maxMergeTSORequests + 1
	requests := make([]*tsoRequest, defaultSize)
	createdCh := make(chan struct{})

	var opts []opentracing.StartSpanOption
	var stream pdpb.PD_TsoClient
	var cancel context.CancelFunc

	for {
		var err error

		if stream == nil {
			var ctx context.Context
			ctx, cancel = context.WithCancel(loopCtx)
			go c.checkStreamTimeout(loopCtx, cancel, createdCh)
			stream, err = c.leaderClient().Tso(ctx)
			if stream != nil {
				createdCh <- struct{}{}
			}
			if err != nil {
				select {
				case <-loopCtx.Done():
					cancel()
					return
				default:
				}
				log.Error("[pd] create tso stream error")
				cancel()
				c.revokeTSORequest(errors.WithStack(err))
				select {
				case <-time.After(time.Second):
				case <-loopCtx.Done():
					return
				}
				continue
			}
		}

		select {
		case first := <-c.tsoRequests:
			pendingPlus1 := len(c.tsoRequests) + 1
			requests[0] = first
			for i := 1; i < pendingPlus1; i++ {
				requests[i] = <-c.tsoRequests
			}
			done := make(chan struct{})
			dl := deadline{
				timer:  time.After(c.timeout),
				done:   done,
				cancel: cancel,
			}
			select {
			case c.tsDeadlineCh <- dl:
			case <-loopCtx.Done():
				cancel()
				return
			}
			opts = extractSpanReference(requests[:pendingPlus1], opts[:0])
			err = c.processTSORequests(stream, requests[:pendingPlus1], opts)
			close(done)
		case <-loopCtx.Done():
			cancel()
			return
		}

		if err != nil {
			select {
			case <-loopCtx.Done():
				cancel()
				return
			default:
			}
			log.Error("[pd] getTS error")
			cancel()
			stream, cancel = nil, nil
		}
	}
}

func extractSpanReference(requests []*tsoRequest, opts []opentracing.StartSpanOption) []opentracing.StartSpanOption {
	for _, req := range requests {
		if span := opentracing.SpanFromContext(req.ctx); span != nil {
			opts = append(opts, opentracing.ChildOf(span.Context()))
		}
	}
	return opts
}

func (c *client) processTSORequests(stream pdpb.PD_TsoClient, requests []*tsoRequest, opts []opentracing.StartSpanOption) error {
	if len(opts) > 0 {
		span := opentracing.StartSpan("pdclient.processTSORequests", opts...)
		defer span.Finish()
	}
	count := len(requests)
	//start := time.Now()
	req := &pdpb.TsoRequest{
		Header: c.requestHeader(),
		Count:  uint32(count),
	}

	if err := stream.Send(req); err != nil {
		err = errors.WithStack(err)
		c.finishTSORequest(requests, 0, 0, err)
		return err
	}
	resp, err := stream.Recv()
	if err != nil {
		err = errors.WithStack(err)
		c.finishTSORequest(requests, 0, 0, err)
		return err
	}

	if resp.GetCount() != uint32(len(requests)) {
		err = errors.WithStack(errTSOLength)
		c.finishTSORequest(requests, 0, 0, err)
		return err
	}

	physical, logical := resp.GetTimestamp().GetPhysical(), resp.GetTimestamp().GetLogical()
	// Server returns the highest ts.
	logical -= int64(resp.GetCount() - 1)
	if tsLessEqual(physical, logical, c.lastPhysical, c.lastLogical) {
		panic(errors.Errorf("timestamp fallback, newly acquired ts (%d,%d) is less or equal to last one (%d, %d)",
			physical, logical, c.lastLogical, c.lastLogical))
	}
	c.lastPhysical = physical
	c.lastLogical = logical + int64(len(requests)) - 1
	c.finishTSORequest(requests, physical, logical, nil)
	return nil
}

func tsLessEqual(physical, logical, thatPhysical, thatLogical int64) bool {
	if physical == thatPhysical {
		return logical <= thatLogical
	}
	return physical < thatPhysical
}

func (c *client) finishTSORequest(requests []*tsoRequest, physical, firstLogical int64, err error) {
	for i := 0; i < len(requests); i++ {
		if span := opentracing.SpanFromContext(requests[i].ctx); span != nil {
			span.Finish()
		}
		requests[i].physical, requests[i].logical = physical, firstLogical+int64(i)
		requests[i].done <- err
	}
}

func (c *client) revokeTSORequest(err error) {
	n := len(c.tsoRequests)
	for i := 0; i < n; i++ {
		req := <-c.tsoRequests
		req.done <- err
	}
}

func (c *client) Close() {
	c.cancel()
	c.wg.Wait()

	c.revokeTSORequest(errors.WithStack(errClosing))

	c.connMu.Lock()
	defer c.connMu.Unlock()
	for _, cc := range c.connMu.clientConns {
		if err := cc.Close(); err != nil {
			log.Error("[pd] failed to close gRPC clientConn")
		}
	}
}

// leaderClient gets the client of current PD leader.
func (c *client) leaderClient() pdpb.PDClient {
	c.connMu.RLock()
	defer c.connMu.RUnlock()

	return pdpb.NewPDClient(c.connMu.clientConns[c.connMu.leader])
}

var tsoReqPool = sync.Pool{
	New: func() interface{} {
		return &tsoRequest{
			done:     make(chan error, 1),
			physical: 0,
			logical:  0,
		}
	},
}

func (c *client) GetTSAsync(ctx context.Context) TSFuture {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span = opentracing.StartSpan("GetTSAsync", opentracing.ChildOf(span.Context()))
		ctx = opentracing.ContextWithSpan(ctx, span)
	}
	req := tsoReqPool.Get().(*tsoRequest)
	req.ctx = ctx
	req.start = time.Now()
	c.tsoRequests <- req

	return req
}

// TSFuture is a future which promises to return a TSO.
type TSFuture interface {
	// Wait gets the physical and logical time, it would block caller if data is not available yet.
	Wait() (int64, int64, error)
}

func (req *tsoRequest) Wait() (physical int64, logical int64, err error) {
	// If tso command duration is observed very high, the reason could be it
	// takes too long for Wait() be called.
	select {
	case err = <-req.done:
		err = errors.WithStack(err)
		defer tsoReqPool.Put(req)
		if err != nil {
			return 0, 0, err
		}
		physical, logical = req.physical, req.logical
		return
	case <-req.ctx.Done():
		return 0, 0, errors.WithStack(req.ctx.Err())
	}
}

func (c *client) GetTS(ctx context.Context) (physical int64, logical int64, err error) {
	resp := c.GetTSAsync(ctx)
	return resp.Wait()
}



func (c *client) requestHeader() *pdpb.RequestHeader {
	return &pdpb.RequestHeader{
		ClusterId: c.clusterID,
	}
}

func addrsToUrls(addrs []string) []string {
	// Add default schema "http://" to addrs.
	urls := make([]string, 0, len(addrs))
	for _, addr := range addrs {
		if strings.Contains(addr, "://") {
			urls = append(urls, addr)
		} else {
			urls = append(urls, "http://"+addr)
		}
	}
	return urls
}
