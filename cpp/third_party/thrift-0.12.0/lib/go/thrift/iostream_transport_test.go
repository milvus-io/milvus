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
	"bytes"
	"testing"
)

func TestStreamTransport(t *testing.T) {
	trans := NewStreamTransportRW(bytes.NewBuffer(make([]byte, 0, 1024)))
	TransportTest(t, trans, trans)
}

func TestStreamTransportOpenClose(t *testing.T) {
	trans := NewStreamTransportRW(bytes.NewBuffer(make([]byte, 0, 1024)))
	if !trans.IsOpen() {
		t.Fatal("StreamTransport should be already open")
	}
	if trans.Open() == nil {
		t.Fatal("StreamTransport should return error when open twice")
	}
	if trans.Close() != nil {
		t.Fatal("StreamTransport should not return error when closing open transport")
	}
	if trans.IsOpen() {
		t.Fatal("StreamTransport should not be open after close")
	}
	if trans.Close() == nil {
		t.Fatal("StreamTransport should return error when closing a non open transport")
	}
	if trans.Open() == nil {
		t.Fatal("StreamTransport should not be able to reopen")
	}
}
