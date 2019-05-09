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

package dontexportrwtest

import (
	"fmt"
	"testing"
)

// Make sure that thrift generates non-exported read/write methods if
// read_write_private option is specified
func TestReadWriteMethodsArePrivate(t *testing.T) {
	// This will only compile if read/write methods exist
	s := NewTestStruct()
	fmt.Sprintf("%v", s.read)
	fmt.Sprintf("%v", s.write)

	is := NewInnerStruct()
	fmt.Sprintf("%v", is.read)
	fmt.Sprintf("%v", is.write)
}
