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

package thrift

import (
	"testing"
	"time"
)

type mockServerTransport struct {
	ListenFunc    func() error
	AcceptFunc    func() (TTransport, error)
	CloseFunc     func() error
	InterruptFunc func() error
}

func (m *mockServerTransport) Listen() error {
	return m.ListenFunc()
}

func (m *mockServerTransport) Accept() (TTransport, error) {
	return m.AcceptFunc()
}

func (m *mockServerTransport) Close() error {
	return m.CloseFunc()
}

func (m *mockServerTransport) Interrupt() error {
	return m.InterruptFunc()
}

type mockTTransport struct {
	TTransport
}

func (m *mockTTransport) Close() error {
	return nil
}

func TestMultipleStop(t *testing.T) {
	proc := &mockProcessor{
		ProcessFunc: func(in, out TProtocol) (bool, TException) {
			return false, nil
		},
	}

	var interruptCalled bool
	c := make(chan struct{})
	trans := &mockServerTransport{
		ListenFunc: func() error {
			return nil
		},
		AcceptFunc: func() (TTransport, error) {
			<-c
			return nil, nil
		},
		CloseFunc: func() error {
			c <- struct{}{}
			return nil
		},
		InterruptFunc: func() error {
			interruptCalled = true
			return nil
		},
	}

	serv := NewTSimpleServer2(proc, trans)
	go serv.Serve()
	serv.Stop()
	if !interruptCalled {
		t.Error("first server transport should have been interrupted")
	}

	serv = NewTSimpleServer2(proc, trans)
	interruptCalled = false
	go serv.Serve()
	serv.Stop()
	if !interruptCalled {
		t.Error("second server transport should have been interrupted")
	}
}

func TestWaitRace(t *testing.T) {
	proc := &mockProcessor{
		ProcessFunc: func(in, out TProtocol) (bool, TException) {
			return false, nil
		},
	}

	trans := &mockServerTransport{
		ListenFunc: func() error {
			return nil
		},
		AcceptFunc: func() (TTransport, error) {
			return &mockTTransport{}, nil
		},
		CloseFunc: func() error {
			return nil
		},
		InterruptFunc: func() error {
			return nil
		},
	}

	serv := NewTSimpleServer2(proc, trans)
	go serv.Serve()
	time.Sleep(1)
	serv.Stop()
}
