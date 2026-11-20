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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
)

// messageWithoutCipherHeader is a message implementation that cannot carry a
// cipher header. Embedding the interface promotes no cipherHeader method.
type messageWithoutCipherHeader struct{ BasicMessage }

func TestCipherConfigOf(t *testing.T) {
	require.Nil(t, CipherConfigOf(nil))

	plain := CreateTestEmptyInsertMesage(1, nil)
	require.Nil(t, CipherConfigOf(plain))

	header, err := EncodeProto(&messagespb.CipherHeader{EzId: 3, CollectionId: 9})
	require.NoError(t, err)
	properties := make(map[string]string, len(plain.Properties().ToRawMap())+1)
	for k, v := range plain.Properties().ToRawMap() {
		properties[k] = v
	}
	properties[messageCipherHeader] = header
	encrypted := NewMutableMessageBeforeAppend(plain.Payload(), properties)

	cfg := CipherConfigOf(encrypted)
	require.NotNil(t, cfg)
	require.Equal(t, int64(3), cfg.EzID)
	require.Equal(t, int64(9), cfg.CollectionID)
	require.Equal(t, cfg, CipherConfigOf(MustAsMutableInsertMessageV1(encrypted)))

	require.Panics(t, func() {
		CipherConfigOf(messageWithoutCipherHeader{})
	}, "a message that cannot carry a cipher header must not be reported as plaintext")
}
