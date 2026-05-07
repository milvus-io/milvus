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

package querycoordv2

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestShouldApplyLocallyOnNonPrimary(t *testing.T) {
	paramtable.Init()

	t.Run("non_ErrNotPrimary_returns_false", func(t *testing.T) {
		assert.False(t, shouldApplyLocallyOnNonPrimary(errors.New("some other error"), message.MessageTypeAlterResourceGroup))
		assert.False(t, shouldApplyLocallyOnNonPrimary(errors.New("some other error"), message.MessageTypeDropResourceGroup))
	})

	t.Run("nil_error_returns_false", func(t *testing.T) {
		assert.False(t, shouldApplyLocallyOnNonPrimary(nil, message.MessageTypeAlterResourceGroup))
	})

	t.Run("ErrNotPrimary_with_default_skip_config", func(t *testing.T) {
		// Default config: "AlterResourceGroup,DropResourceGroup"
		assert.True(t, shouldApplyLocallyOnNonPrimary(broadcast.ErrNotPrimary, message.MessageTypeAlterResourceGroup))
		assert.True(t, shouldApplyLocallyOnNonPrimary(broadcast.ErrNotPrimary, message.MessageTypeDropResourceGroup))
	})

	t.Run("ErrNotPrimary_with_msg_type_not_in_skip_config", func(t *testing.T) {
		// CreateCollection is not in the default skip list
		assert.False(t, shouldApplyLocallyOnNonPrimary(broadcast.ErrNotPrimary, message.MessageTypeCreateCollection))
	})

	t.Run("wrapped_ErrNotPrimary_returns_true", func(t *testing.T) {
		wrappedErr := fmt.Errorf("wrapped: %w", broadcast.ErrNotPrimary)
		assert.True(t, shouldApplyLocallyOnNonPrimary(wrappedErr, message.MessageTypeAlterResourceGroup))
	})

	t.Run("ErrNotPrimary_with_empty_skip_config", func(t *testing.T) {
		paramtable.Get().Save(paramtable.Get().StreamingCfg.ReplicationSkipMessageTypes.Key, "")
		defer paramtable.Get().Reset(paramtable.Get().StreamingCfg.ReplicationSkipMessageTypes.Key)

		assert.False(t, shouldApplyLocallyOnNonPrimary(broadcast.ErrNotPrimary, message.MessageTypeAlterResourceGroup))
		assert.False(t, shouldApplyLocallyOnNonPrimary(broadcast.ErrNotPrimary, message.MessageTypeDropResourceGroup))
	})

	t.Run("ErrNotPrimary_with_custom_skip_config", func(t *testing.T) {
		paramtable.Get().Save(paramtable.Get().StreamingCfg.ReplicationSkipMessageTypes.Key, "AlterResourceGroup")
		defer paramtable.Get().Reset(paramtable.Get().StreamingCfg.ReplicationSkipMessageTypes.Key)

		assert.True(t, shouldApplyLocallyOnNonPrimary(broadcast.ErrNotPrimary, message.MessageTypeAlterResourceGroup))
		assert.False(t, shouldApplyLocallyOnNonPrimary(broadcast.ErrNotPrimary, message.MessageTypeDropResourceGroup))
	})
}
