// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package msgdispatcher

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

func TestClientSuite(t *testing.T) {
	suite.Run(t, new(ClientSuite))
}

type ClientSuite struct {
	suite.Suite

	client Client
}

func (s *ClientSuite) SetupTest() {
	s.client = NewClient(newMockFactory(), typeutil.ProxyRole, 1)
	s.NotNil(s.client)
}

func (s *ClientSuite) TeardownTest() {
	s.client.Close()
}

func (s *ClientSuite) TestNormalRegister() {
	ctx := context.Background()

	vchannels := []string{}
	for i := 0; i < 10; i++ {
		vchannels = append(vchannels, fmt.Sprintf("mock_vchannel_%d_%d", i, rand.Int()))
	}

	for _, vchannel := range vchannels {
		ch, err := s.client.Register(ctx, vchannel, nil, mqwrapper.SubscriptionPositionUnknown)
		s.NoError(err)
		s.NotNil(ch)
	}

	s.NotPanics(func() {
		for _, vchannel := range vchannels {
			s.client.Deregister(vchannel)
		}
	})
}

func (s *ClientSuite) TestRegisterWithTimeoutCtx() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Millisecond)
	defer cancel()
	<-time.After(2 * time.Millisecond)

	vchannels := []string{}
	for i := 0; i < 10; i++ {
		vchannels = append(vchannels, fmt.Sprintf("mock_vchannel_%d", i))
	}

	for _, vchannel := range vchannels {
		ch, err := s.client.Register(ctx, vchannel, nil, mqwrapper.SubscriptionPositionUnknown)
		s.Error(err)
		s.True(merr.IsCanceledOrTimeout(err))
		s.Nil(ch)
	}
}

func (s *ClientSuite) TestConcRegisterDeregister() {
	wg := &sync.WaitGroup{}
	const total = 100
	deregisterCount := atomic.NewInt32(0)
	wg.Add(total)
	for i := 0; i < total; i++ {
		vchannel := fmt.Sprintf("mock-vchannel-%d_%d", i, rand.Int())
		go func() {
			ch, err := s.client.Register(context.Background(), vchannel, nil, mqwrapper.SubscriptionPositionUnknown)
			s.NoError(err)
			s.NotNil(ch)
			for j := 0; j < rand.Intn(2); j++ {
				s.client.Deregister(vchannel)
				deregisterCount.Inc()
			}
			wg.Done()
		}()
	}
	wg.Wait()
	expected := int(total - deregisterCount.Load())

	c := s.client.(*client)
	c.managerMut.Lock()
	n := len(c.managers)
	c.managerMut.Unlock()
	s.Equal(expected, n)
}

func (s *ClientSuite) TestRegisterThreeTimes() {
	pchan := "registertwice_vchannel-1"
	vchan := fmt.Sprintf("%s_%d", pchan, 1)
	ctx := context.Background()
	ch, err := s.client.Register(ctx, vchan, nil, mqwrapper.SubscriptionPositionUnknown)
	s.NoError(err)
	s.NotNil(ch)

	ch, err = s.client.Register(ctx, vchan, nil, mqwrapper.SubscriptionPositionUnknown)
	s.NoError(err)
	s.NotNil(ch)

	s.Panics(func() {
		ch, err = s.client.Register(ctx, vchan, nil, mqwrapper.SubscriptionPositionUnknown)
		s.NoError(err)
		s.NotNil(ch)
	})

	c := s.client.(*client)

	c.managerMut.Lock()
	m := c.managers
	s.Equal(1, len(m))
	manager, ok := m[pchan]
	s.True(ok)
	s.NotNil(manager)
	s.Equal(2, manager.Num())

	c.managerMut.Unlock()
}

func (s *ClientSuite) TestDeregisterUnknownChannel() {
	vchan := "unknown_vchannel-1_1"
	s.NotPanics(func() { s.client.Deregister(vchan) })
}
