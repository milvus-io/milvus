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
	"errors"
	"io"
	"reflect"
	"testing"
)

func TestEnsureTransportsAreRich(t *testing.T) {
	buf := bytes.NewBuffer(make([]byte, 0, 1024))

	transports := []TTransportFactory{
		NewTMemoryBufferTransportFactory(1024),
		NewStreamTransportFactory(buf, buf, true),
		NewTFramedTransportFactory(NewTMemoryBufferTransportFactory(1024)),
		NewTHttpPostClientTransportFactory("http://127.0.0.1"),
	}
	for _, tf := range transports {
		trans, err := tf.GetTransport(nil)
		if err != nil {
			t.Error(err)
			continue
		}
		_, ok := trans.(TRichTransport)
		if !ok {
			t.Errorf("Transport %s does not implement TRichTransport interface", reflect.ValueOf(trans))
		}
	}
}

// TestReadByte tests whether readByte handles error cases correctly.
func TestReadByte(t *testing.T) {
	for i, test := range readByteTests {
		v, err := readByte(test.r)
		if v != test.v {
			t.Fatalf("TestReadByte %d: value differs. Expected %d, got %d", i, test.v, test.r.v)
		}
		if err != test.err {
			t.Fatalf("TestReadByte %d: error differs. Expected %s, got %s", i, test.err, test.r.err)
		}
	}
}

var someError = errors.New("Some error")
var readByteTests = []struct {
	r   *mockReader
	v   byte
	err error
}{
	{&mockReader{0, 55, io.EOF}, 0, io.EOF},        // reader sends EOF w/o data
	{&mockReader{0, 55, someError}, 0, someError},  // reader sends some other error
	{&mockReader{1, 55, nil}, 55, nil},             // reader sends data w/o error
	{&mockReader{1, 55, io.EOF}, 55, nil},          // reader sends data with EOF
	{&mockReader{1, 55, someError}, 55, someError}, // reader sends data withsome error
}

type mockReader struct {
	n   int
	v   byte
	err error
}

func (r *mockReader) Read(p []byte) (n int, err error) {
	if r.n > 0 {
		p[0] = r.v
	}
	return r.n, r.err
}
