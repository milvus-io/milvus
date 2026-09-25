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

package message

import (
	"fmt"

	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
)

// CipherConfigOf returns the cipher config that msg was encrypted with,
// or nil if msg is not encrypted.
// It panics for a message implementation that cannot carry a cipher header,
// because a silent nil would send a derived message of an encrypted collection
// out as plaintext.
func CipherConfigOf(msg BasicMessage) *CipherConfig {
	if msg == nil {
		return nil
	}
	c, ok := msg.(interface {
		cipherHeader() *messagespb.CipherHeader
	})
	if !ok {
		panic(fmt.Sprintf("message implementation %T does not carry a cipher header", msg))
	}
	header := c.cipherHeader()
	if header == nil {
		return nil
	}
	return &CipherConfig{
		EzID:         header.GetEzId(),
		CollectionID: header.GetCollectionId(),
	}
}
