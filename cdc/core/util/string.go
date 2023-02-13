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

package util

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"unsafe"

	"go.uber.org/zap"
)

// ToBytes performs unholy acts to avoid allocations
func ToBytes(s string) []byte {
	return *(*[]byte)(unsafe.Pointer(&s))
}

// ToString like ToBytes
func ToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

// ToPhysicalChannel get physical channel name from virtual channel name
func ToPhysicalChannel(vchannel string) string {
	index := strings.LastIndex(vchannel, "_")
	if index < 0 {
		return vchannel
	}
	return vchannel[:index]
}

func Base64Encode(obj any) string {
	objByte, err := json.Marshal(obj)
	if err != nil {
		Log.Warn("fail to marshal obj", zap.Any("obj", obj))
		return ""
	}
	return base64.StdEncoding.EncodeToString(objByte)
}
