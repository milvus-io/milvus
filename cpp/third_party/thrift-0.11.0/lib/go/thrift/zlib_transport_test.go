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
	"compress/zlib"
	"testing"
)

func TestZlibTransport(t *testing.T) {
	trans, err := NewTZlibTransport(NewTMemoryBuffer(), zlib.BestCompression)
	if err != nil {
		t.Fatal(err)
	}
	TransportTest(t, trans, trans)
}

type DummyTransportFactory struct{}

func (p *DummyTransportFactory) GetTransport(trans TTransport) (TTransport, error) {
	return NewTMemoryBuffer(), nil
}

func TestZlibFactoryTransportWithFactory(t *testing.T) {
	factory := NewTZlibTransportFactoryWithFactory(
		zlib.BestCompression,
		&DummyTransportFactory{},
	)
	buffer := NewTMemoryBuffer()
	trans, err := factory.GetTransport(buffer)
	if err != nil {
		t.Fatal(err)
	}
	TransportTest(t, trans, trans)
}

func TestZlibFactoryTransportWithoutFactory(t *testing.T) {
	factory := NewTZlibTransportFactoryWithFactory(zlib.BestCompression, nil)
	buffer := NewTMemoryBuffer()
	trans, err := factory.GetTransport(buffer)
	if err != nil {
		t.Fatal(err)
	}
	TransportTest(t, trans, trans)
}
