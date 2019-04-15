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
	"errors"
	"testing"
)

func TestPrependError(t *testing.T) {
	err := NewTApplicationException(INTERNAL_ERROR, "original error")
	err2, ok := PrependError("Prepend: ", err).(TApplicationException)
	if !ok {
		t.Fatal("Couldn't cast error TApplicationException")
	}
	if err2.Error() != "Prepend: original error" {
		t.Fatal("Unexpected error string")
	}
	if err2.TypeId() != INTERNAL_ERROR {
		t.Fatal("Unexpected type error")
	}

	err3 := NewTProtocolExceptionWithType(INVALID_DATA, errors.New("original error"))
	err4, ok := PrependError("Prepend: ", err3).(TProtocolException)
	if !ok {
		t.Fatal("Couldn't cast error TProtocolException")
	}
	if err4.Error() != "Prepend: original error" {
		t.Fatal("Unexpected error string")
	}
	if err4.TypeId() != INVALID_DATA {
		t.Fatal("Unexpected type error")
	}

	err5 := NewTTransportException(TIMED_OUT, "original error")
	err6, ok := PrependError("Prepend: ", err5).(TTransportException)
	if !ok {
		t.Fatal("Couldn't cast error TTransportException")
	}
	if err6.Error() != "Prepend: original error" {
		t.Fatal("Unexpected error string")
	}
	if err6.TypeId() != TIMED_OUT {
		t.Fatal("Unexpected type error")
	}

	err7 := errors.New("original error")
	err8 := PrependError("Prepend: ", err7)
	if err8.Error() != "Prepend: original error" {
		t.Fatal("Unexpected error string")
	}
}
