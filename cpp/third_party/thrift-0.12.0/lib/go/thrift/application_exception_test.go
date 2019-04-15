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
)

func TestTApplicationException(t *testing.T) {
	exc := NewTApplicationException(UNKNOWN_APPLICATION_EXCEPTION, "")
	if exc.Error() != defaultApplicationExceptionMessage[UNKNOWN_APPLICATION_EXCEPTION] {
		t.Fatalf("Expected empty string for exception but found '%s'", exc.Error())
	}
	if exc.TypeId() != UNKNOWN_APPLICATION_EXCEPTION {
		t.Fatalf("Expected type UNKNOWN for exception but found '%v'", exc.TypeId())
	}
	exc = NewTApplicationException(WRONG_METHOD_NAME, "junk_method")
	if exc.Error() != "junk_method" {
		t.Fatalf("Expected 'junk_method' for exception but found '%s'", exc.Error())
	}
	if exc.TypeId() != WRONG_METHOD_NAME {
		t.Fatalf("Expected type WRONG_METHOD_NAME for exception but found '%v'", exc.TypeId())
	}
}
