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

package extension

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
)

func TestNoopRewriteRequestParamsReturnsItsArgumentsUntouched(t *testing.T) {
	params := []*commonpb.KeyValuePair{
		{Key: "metric_type", Value: "L2"},
		{Key: "x-form-reserved", Value: "in07-a"},
	}
	ctx := context.Background()

	gotCtx, cleaned := NoopProxyExtension{}.RewriteRequestParams(ctx, params)

	assert.True(t, ctx == gotCtx, "the native default must hand back the caller's own context, not a derived one")
	require.Len(t, cleaned, 2)
	assert.True(t, &params[0] == &cleaned[0],
		"the native default must hand back the caller's own slice, reserved-looking entry included: it has no protocol of its own to strip")
}
