// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package delegator

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBytesOffsetToRuneOffset(t *testing.T) {
	// test with chinese
	text := "你好世界" // 12 bytes, 4 runes
	spans := SpanList{{0, 6}, {6, 12}}
	err := bytesOffsetToRuneOffset(text, spans)
	assert.NoError(t, err)
	assert.Equal(t, SpanList{{0, 2}, {2, 4}}, spans)

	// test with emoji
	text = "Hello👋World" // 15 bytes, 11 runes
	spans = SpanList{{0, 5}, {5, 9}, {9, 14}}
	err = bytesOffsetToRuneOffset(text, spans)
	assert.NoError(t, err)
	assert.Equal(t, SpanList{{0, 5}, {5, 6}, {6, 11}}, spans)
}
