// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package retry

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMaxAttemptsFromContext(t *testing.T) {
	// No value set — returns 0, false
	ctx := context.Background()
	v, ok := MaxAttemptsFromContext(ctx)
	assert.False(t, ok)
	assert.Equal(t, uint(0), v)

	// Value set — returns it
	ctx = WithMaxAttemptsContext(ctx, 3)
	v, ok = MaxAttemptsFromContext(ctx)
	assert.True(t, ok)
	assert.Equal(t, uint(3), v)

	// Zero is a valid explicit value
	ctx = WithMaxAttemptsContext(context.Background(), 0)
	v, ok = MaxAttemptsFromContext(ctx)
	assert.True(t, ok)
	assert.Equal(t, uint(0), v)
}

func TestMaxAttemptsFromContextOrDefault(t *testing.T) {
	// No value in ctx — returns default
	ctx := context.Background()
	assert.Equal(t, uint(10), MaxAttemptsFromContextOrDefault(ctx, 10))

	// Value in ctx — returns ctx value, ignoring default
	ctx = WithMaxAttemptsContext(ctx, 3)
	assert.Equal(t, uint(3), MaxAttemptsFromContextOrDefault(ctx, 10))

	// Zero in ctx — returns 0, not default
	ctx = WithMaxAttemptsContext(context.Background(), 0)
	assert.Equal(t, uint(0), MaxAttemptsFromContextOrDefault(ctx, 10))
}
