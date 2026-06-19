/*
 * # Licensed to the LF AI & Data foundation under one
 * # or more contributor license agreements. See the NOTICE file
 * # distributed with this work for additional information
 * # regarding copyright ownership. The ASF licenses this file
 * # to you under the Apache License, Version 2.0 (the
 * # "License"); you may not use this file except in compliance
 * # with the License. You may obtain a copy of the License at
 * #
 * #     http://www.apache.org/licenses/LICENSE-2.0
 * #
 * # Unless required by applicable law or agreed to in writing, software
 * # distributed under the License is distributed on an "AS IS" BASIS,
 * # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * # See the License for the specific language governing permissions and
 * # limitations under the License.
 */

package types

import (
	"context"
	"testing"

	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestNewFuncContext(t *testing.T) {
	pool := memory.NewGoAllocator()
	ctx := NewFuncContext(pool)
	assert.NotNil(t, ctx)
	assert.Equal(t, pool, ctx.Pool())
	assert.NotNil(t, ctx.Context())
	assert.Equal(t, "", ctx.Stage())
}

func TestNewFuncContext_NilPool(t *testing.T) {
	// nil pool should use DefaultAllocator
	ctx := NewFuncContext(nil)
	assert.NotNil(t, ctx)
	assert.Equal(t, memory.DefaultAllocator, ctx.Pool())
}

func TestNewFuncContextWithContext(t *testing.T) {
	pool := memory.NewGoAllocator()
	goCtx := context.Background()
	ctx := NewFuncContextWithContext(goCtx, pool)
	assert.NotNil(t, ctx)
	assert.Equal(t, pool, ctx.Pool())
	assert.Equal(t, goCtx, ctx.Context())
}

func TestNewFuncContextWithContext_NilPool(t *testing.T) {
	// nil pool should use DefaultAllocator
	ctx := NewFuncContextWithContext(context.TODO(), nil)
	assert.NotNil(t, ctx)
	assert.Equal(t, memory.DefaultAllocator, ctx.Pool())
}

func TestNewFuncContextWithContext_NilContext(t *testing.T) {
	pool := memory.NewGoAllocator()
	ctx := NewFuncContextWithContext(context.TODO(), pool)
	assert.NotNil(t, ctx)
	assert.NotNil(t, ctx.Context())
	assert.Equal(t, pool, ctx.Pool())
}

func TestNewFuncContextWithStage(t *testing.T) {
	pool := memory.NewGoAllocator()
	ctx := NewFuncContextWithStage(pool, StageL2Rerank)
	assert.NotNil(t, ctx)
	assert.Equal(t, pool, ctx.Pool())
	assert.Equal(t, StageL2Rerank, ctx.Stage())
}

func TestNewFuncContextFull(t *testing.T) {
	pool := memory.NewGoAllocator()
	goCtx := context.Background()
	ctx := NewFuncContextFull(goCtx, pool, StageIngestion)
	assert.NotNil(t, ctx)
	assert.Equal(t, pool, ctx.Pool())
	assert.Equal(t, goCtx, ctx.Context())
	assert.Equal(t, StageIngestion, ctx.Stage())
}

func TestStageConstants(t *testing.T) {
	// Verify all stage constants are defined
	assert.Equal(t, "ingestion", StageIngestion)
	assert.Equal(t, "L2_rerank", StageL2Rerank)
	assert.Equal(t, "L1_rerank", StageL1Rerank)
	assert.Equal(t, "L0_rerank", StageL0Rerank)
	assert.Equal(t, "pre_process", StagePreProcess)
	assert.Equal(t, "post_process", StagePostProcess)
}
