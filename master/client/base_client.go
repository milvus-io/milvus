// Copyright 2019 TiKV Project Authors.
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
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/czs007/suvlim/errors"
	"github.com/czs007/suvlim/pkg/pdpb"
	"github.com/czs007/suvlim/util/grpcutil"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// baseClient is a basic client for all other complex client.
type baseClient struct {
	urls      []string
	clusterID uint64
	connMu    struct {
		sync.RWMutex
		clientConns map[string]*grpc.ClientConn
		leader      string
	}

	checkLeaderCh chan struct{}

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	security SecurityOption

	gRPCDialOptions []grpc.DialOption
	timeout         time.Duration
}

// SecurityOption records options about tls
type SecurityOption struct {
	CAPath   string
	CertPath string
	KeyPath  string
}

// ClientOption configures client.
type ClientOption func(c *baseClient)

// WithGRPCDialOptions configures the client with gRPC dial options.
func WithGRPCDialOptions(opts ...grpc.DialOption) ClientOption {
	return func(c *baseClient) {
		c.gRPCDialOptions = append(c.gRPCDialOptions, opts...)
	}
}

// WithCustomTimeoutOption configures the client with timeout option.
func WithCustomTimeoutOption(timeout time.Duration) ClientOption {
	return func(c *baseClient) {
		c.timeout = timeout
	}
}

// newBaseClient returns a new baseClient.
func newBaseClient(ctx context.Context, urls []string, security SecurityOption, opts ...ClientOption) (*baseClient, error) {
	ctx1, cancel := context.WithCancel(ctx)
	c := &baseClient{
		urls:          urls,
		checkLeaderCh: make(chan struct{}, 1),
		ctx:           ctx1,
		cancel:        cancel,
		security:      security,
		timeout:       defaultPDTimeout,
	}
	c.connMu.clientConns = make(map[string]*grpc.ClientConn)
	for _, opt := range opts {
		opt(c)
	}

	log.Info("[pd] init cluster id", zap.Uint64("cluster-id", c.clusterID))

	c.wg.Add(1)
	go c.leaderLoop()

	return c, nil
}

func (c *baseClient) initRetry(f func() error) error {
	var err error
	for i := 0; i < maxInitClusterRetries; i++ {
		if err = f(); err == nil {
			return nil
		}
		select {
		case <-c.ctx.Done():
			return err
		case <-time.After(time.Second):
		}
	}
	return errors.WithStack(err)
}

func (c *baseClient) leaderLoop() {
	defer c.wg.Done()

	ctx, cancel := context.WithCancel(c.ctx)
	defer cancel()

	for {
		select {
		case <-c.checkLeaderCh:
		case <-time.After(time.Minute):
		case <-ctx.Done():
			return
		}
	}
}


// GetClusterID returns the ClusterID.
func (c *baseClient) GetClusterID(context.Context) uint64 {
	return c.clusterID
}

// GetLeaderAddr returns the leader address.
// For testing use.
func (c *baseClient) GetLeaderAddr() string {
	c.connMu.RLock()
	defer c.connMu.RUnlock()
	return c.connMu.leader
}

// GetURLs returns the URLs.
// For testing use. It should only be called when the client is closed.
func (c *baseClient) GetURLs() []string {
	return c.urls
}


func (c *baseClient) updateURLs(members []*pdpb.Member) {
	urls := make([]string, 0, len(members))
	for _, m := range members {
		urls = append(urls, m.GetClientUrls()...)
	}

	sort.Strings(urls)
	// the url list is same.
	if reflect.DeepEqual(c.urls, urls) {
		return
	}

	log.Info("[pd] update member urls", zap.Strings("old-urls", c.urls), zap.Strings("new-urls", urls))
	c.urls = urls
}


func (c *baseClient) getOrCreateGRPCConn(addr string) (*grpc.ClientConn, error) {
	c.connMu.RLock()
	conn, ok := c.connMu.clientConns[addr]
	c.connMu.RUnlock()
	if ok {
		return conn, nil
	}
	tlsCfg, err := grpcutil.SecurityConfig{
		CAPath:   c.security.CAPath,
		CertPath: c.security.CertPath,
		KeyPath:  c.security.KeyPath,
	}.ToTLSConfig()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	dctx, cancel := context.WithTimeout(c.ctx, dialTimeout)
	defer cancel()
	cc, err := grpcutil.GetClientConn(dctx, addr, tlsCfg, c.gRPCDialOptions...)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	c.connMu.Lock()
	defer c.connMu.Unlock()
	if old, ok := c.connMu.clientConns[addr]; ok {
		cc.Close()
		log.Debug("use old connection", zap.String("target", cc.Target()), zap.String("state", cc.GetState().String()))
		return old, nil
	}

	c.connMu.clientConns[addr] = cc
	return cc, nil
}
