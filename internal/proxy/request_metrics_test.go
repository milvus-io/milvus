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

package proxy

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/extension"
)

func TestRequestMetricsAttributionDoesNotMutateRouting(t *testing.T) {
	original := context.Background()
	ctx, m := WithRequestMetrics(original)
	admitted := extension.WithQueryResourceGroup(ctx, "rg-a")
	setRequestResourceGroup(ctx, admitted)
	assert.Equal(t, "rg-a", m.ResourceGroup())
	assert.Empty(t, extension.QueryResourceGroupFromContext(ctx))
	assert.Empty(t, extension.QueryResourceGroupFromContext(original))
	assert.Equal(t, "rg-a", extension.QueryResourceGroupFromContext(admitted))
	setRequestResourceGroup(ctx, ctx)
	assert.Equal(t, "rg-a", m.ResourceGroup())
	nested, child := WithRequestMetrics(ctx)
	setRequestResourceGroup(nested, extension.WithQueryResourceGroup(nested, "rg-b"))
	assert.Equal(t, "rg-b", child.ResourceGroup())
	assert.Equal(t, "rg-a", m.ResourceGroup())
	_, unscoped := WithRequestMetrics(context.Background())
	assert.Empty(t, unscoped.ResourceGroup())
}
