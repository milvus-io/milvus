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

package taskcommon

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestProperties_AppendType_AllValidTypes(t *testing.T) {
	for _, tt := range TypeList {
		t.Run(tt, func(t *testing.T) {
			props := NewProperties(nil)
			props.AppendTaskID(1)
			props.AppendType(tt)
			taskType, err := props.GetTaskType()
			assert.NoError(t, err)
			assert.Equal(t, tt, taskType)
		})
	}
}

func TestProperties_AppendType_InvalidType(t *testing.T) {
	props := NewProperties(nil)
	props.AppendTaskID(1)
	props.AppendType("InvalidType")
	assert.Equal(t, TypeNone, props[TypeKey])
}

func TestProperties_GetTaskType_Missing(t *testing.T) {
	props := NewProperties(nil)
	props.AppendTaskID(1)
	_, err := props.GetTaskType()
	assert.Error(t, err)
}

func TestProperties_GetTaskType_Unrecognized(t *testing.T) {
	props := NewProperties(map[string]string{
		TypeKey:   "UnknownType",
		TaskIDKey: "1",
	})
	taskType, err := props.GetTaskType()
	assert.Error(t, err)
	assert.Equal(t, "UnknownType", taskType)
}
