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

package index

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAnnParams(t *testing.T) {
	ivfAP := NewIvfAnnParam(16)
	result := ivfAP.Params()
	v, ok := result[ivfNprobeKey]
	assert.True(t, ok)
	assert.Equal(t, 16, v)

	hnswAP := NewHNSWAnnParam(16)
	result = hnswAP.Params()
	v, ok = result[hnswEfKey]
	assert.True(t, ok)
	assert.Equal(t, 16, v)

	diskAP := NewDiskAnnParam(10)
	result = diskAP.Params()
	v, ok = result[diskANNSearchListKey]
	assert.True(t, ok)
	assert.Equal(t, 10, v)

	scannAP := NewSCANNAnnParam(32, 50)
	result = scannAP.Params()
	v, ok = result[scannNProbeKey]
	assert.True(t, ok)
	assert.Equal(t, 32, v)
	v, ok = result[scannReorderKKey]
	assert.True(t, ok)
	assert.Equal(t, 50, v)
}
