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

func TestReadWriteCompactProtocol(t *testing.T) {
	ReadWriteProtocolTest(t, NewTCompactProtocolFactory())

	transports := []TTransport{
		NewTMemoryBuffer(),
		NewStreamTransportRW(bytes.NewBuffer(make([]byte, 0, 16384))),
		NewTFramedTransport(NewTMemoryBuffer()),
	}

	zlib0, _ := NewTZlibTransport(NewTMemoryBuffer(), 0)
	zlib6, _ := NewTZlibTransport(NewTMemoryBuffer(), 6)
	zlib9, _ := NewTZlibTransport(NewTFramedTransport(NewTMemoryBuffer()), 9)
	transports = append(transports, zlib0, zlib6, zlib9)

	for _, trans := range transports {
		p := NewTCompactProtocol(trans)
		ReadWriteBool(t, p, trans)
		p = NewTCompactProtocol(trans)
		ReadWriteByte(t, p, trans)
		p = NewTCompactProtocol(trans)
		ReadWriteI16(t, p, trans)
		p = NewTCompactProtocol(trans)
		ReadWriteI32(t, p, trans)
		p = NewTCompactProtocol(trans)
		ReadWriteI64(t, p, trans)
		p = NewTCompactProtocol(trans)
		ReadWriteDouble(t, p, trans)
		p = NewTCompactProtocol(trans)
		ReadWriteString(t, p, trans)
		p = NewTCompactProtocol(trans)
		ReadWriteBinary(t, p, trans)
		trans.Close()
	}
}
