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

package eventlog

import (
	context "context"
	fmt "fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type GrpcLoggerSuite struct {
	suite.Suite
	l    *grpcLogger
	port int
}

type localListenerClient struct {
	conn         *grpc.ClientConn
	client       EventLogServiceClient
	listenClient EventLogService_ListenClient
	result       chan *Event
}

func (c *localListenerClient) listen(t *testing.T) {
	for {
		evt, err := c.listenClient.Recv()
		if err != nil {
			return
		}

		select {
		case c.result <- evt:
		default:
		}
	}
}

func (c *localListenerClient) close() {
	if c.conn != nil {
		c.conn.Close()
	}
	if c.result != nil {
		close(c.result)
	}
}

func (s *GrpcLoggerSuite) SetupTest() {
	port, err := getGrpcLogger()
	s.Require().NoError(err)
	s.port = port

	s.l = grpcLog.Load()
	s.Require().NotNil(s.l)
}

func (s *GrpcLoggerSuite) registerClient() *localListenerClient {
	ctx := context.Background()
	addr := fmt.Sprintf("127.0.0.1:%d", s.port)
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithTimeout(time.Second),
	}

	conn, err := grpc.DialContext(ctx, addr, opts...)

	s.Require().NoError(err)

	client := NewEventLogServiceClient(conn)

	listenClient, err := client.Listen(ctx, &ListenRequest{})
	s.Require().NoError(err)

	c := &localListenerClient{
		conn:         conn,
		client:       client,
		listenClient: listenClient,
		result:       make(chan *Event, 100),
	}

	go c.listen(s.T())

	return c
}

func (s *GrpcLoggerSuite) TestRecord() {
	s.Run("normal_case", func() {
		c := s.registerClient()

		s.Eventually(func() bool {
			return s.l.clients.Len() == 1
		}, time.Second, time.Millisecond*100)

		s.l.Record(NewRawEvt(Level_Info, "test"))

		evt := <-c.result
		s.Equal(Level_Info, evt.GetLevel())
		s.EqualValues("test", evt.GetData())

		c.close()

		s.Eventually(func() bool {
			return s.l.clients.Len() == 0
		}, time.Second, time.Millisecond*100)
	})

	s.Run("skip_level", func() {
		s.l.SetLevel(Level_Warn)
		defer s.l.SetLevel(Level_Debug)
		c := s.registerClient()

		s.Eventually(func() bool {
			return s.l.clients.Len() == 1
		}, time.Second, time.Millisecond*100)

		s.l.Record(NewRawEvt(Level_Info, "test"))

		c.close()

		s.Eventually(func() bool {
			return s.l.clients.Len() == 0
		}, time.Second, time.Millisecond*100)

		var result []*Event
		for evt := range c.result {
			result = append(result, evt)
		}
		s.Equal(0, len(result))
	})
}

func (s *GrpcLoggerSuite) TestRecordFunc() {
	s.Run("normal_case", func() {
		c := s.registerClient()

		s.Eventually(func() bool {
			return s.l.clients.Len() == 1
		}, time.Second, time.Millisecond*100)

		s.l.RecordFunc(Level_Info, func() Evt { return NewRawEvt(Level_Info, "test") })

		evt := <-c.result
		s.Equal(Level_Info, evt.GetLevel())
		s.EqualValues("test", evt.GetData())

		c.close()

		s.Eventually(func() bool {
			return s.l.clients.Len() == 0
		}, time.Second, time.Millisecond*100)
	})

	s.Run("skip_level", func() {
		s.l.SetLevel(Level_Warn)
		defer s.l.SetLevel(Level_Debug)
		c := s.registerClient()

		s.Eventually(func() bool {
			return s.l.clients.Len() == 1
		}, time.Second, time.Millisecond*100)

		s.l.RecordFunc(Level_Info, func() Evt { return NewRawEvt(Level_Info, "test") })

		c.close()

		s.Eventually(func() bool {
			return s.l.clients.Len() == 0
		}, time.Second, time.Millisecond*100)

		var result []*Event
		for evt := range c.result {
			result = append(result, evt)
		}
		s.Equal(0, len(result))
	})
}

func (s *GrpcLoggerSuite) TestFlush() {
	s.NoError(s.l.Flush())
}

func TestGrpcLogger(t *testing.T) {
	suite.Run(t, new(GrpcLoggerSuite))
}
