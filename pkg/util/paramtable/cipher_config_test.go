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

package paramtable

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCipherConfigUpdatePeriodKey(t *testing.T) {
	Init()
	params := GetCipherParams()

	assert.Equal(t, "cipherPlugin.updatePeriodInMinutes", params.UpdatePeriodInMinutes.Key)
	assert.Equal(t, "60", params.UpdatePeriodInMinutes.GetValue())

	// The key shipped misspelled in 2.6.1; an override written under the old
	// spelling must still be read through the fallback key.
	assert.NoError(t, params.Save("cipherPlugin.updatePerieldInMinutes", "30"))
	assert.Equal(t, "30", params.UpdatePeriodInMinutes.GetValue())

	// The correctly spelled key wins once it is set.
	assert.NoError(t, params.Save("cipherPlugin.updatePeriodInMinutes", "45"))
	assert.Equal(t, "45", params.UpdatePeriodInMinutes.GetValue())

	assert.Equal(t, "cipherPlugin.enableDiskEncryption", params.EnableDiskEncryption.Key)
	assert.Equal(t, "false", params.EnableDiskEncryption.GetValue())
}
