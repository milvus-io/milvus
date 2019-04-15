/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package tests

import (
	"context"
	"fmt"
	"net"
	"onewaytest"
	"testing"
	"thrift"
	"time"
)

func findPort() net.Addr {
	if l, err := net.Listen("tcp", "127.0.0.1:0"); err != nil {
		panic("Could not find available server port")
	} else {
		defer l.Close()
		return l.Addr()
	}
}

type impl struct{}

func (i *impl) Hi(ctx context.Context, in int64, s string) (err error)        { fmt.Println("Hi!"); return }
func (i *impl) Emptyfunc(ctx context.Context) (err error)                     { return }
func (i *impl) EchoInt(ctx context.Context, param int64) (r int64, err error) { return param, nil }

const TIMEOUT = time.Second

var addr net.Addr
var server *thrift.TSimpleServer
var client *onewaytest.OneWayClient

func TestInitOneway(t *testing.T) {
	var err error
	addr = findPort()
	serverTransport, err := thrift.NewTServerSocketTimeout(addr.String(), TIMEOUT)
	if err != nil {
		t.Fatal("Unable to create server socket", err)
	}
	processor := onewaytest.NewOneWayProcessor(&impl{})
	server = thrift.NewTSimpleServer2(processor, serverTransport)

	go server.Serve()
	time.Sleep(10 * time.Millisecond)
}

func TestInitOnewayClient(t *testing.T) {
	transport := thrift.NewTSocketFromAddrTimeout(addr, TIMEOUT)
	protocol := thrift.NewTBinaryProtocolTransport(transport)
	client = onewaytest.NewOneWayClient(thrift.NewTStandardClient(protocol, protocol))
	err := transport.Open()
	if err != nil {
		t.Fatal("Unable to open client socket", err)
	}
}

func TestCallOnewayServer(t *testing.T) {
	//call oneway function
	err := client.Hi(defaultCtx, 1, "")
	if err != nil {
		t.Fatal("Unexpected error: ", err)
	}
	//There is no way to detect protocol problems with single oneway call so we call it second time
	i, err := client.EchoInt(defaultCtx, 42)
	if err != nil {
		t.Fatal("Unexpected error: ", err)
	}
	if i != 42 {
		t.Fatal("Unexpected returned value: ", i)
	}
}
