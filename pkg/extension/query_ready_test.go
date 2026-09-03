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
)

func TestQueryResourceGroupSurvivesOnTheContext(t *testing.T) {
	ctx := WithQueryResourceGroup(context.Background(), "rg-a")
	assert.Equal(t, "rg-a", QueryResourceGroupFromContext(ctx),
		"the routing scope must be readable by every stage that runs under the request's context")

	assert.Equal(t, "", QueryResourceGroupFromContext(context.Background()),
		"a context nothing scoped must report no scope, which is what every request in a stock binary looks like")
}

// TestQueryResourceGroupKeyIsUnreachableFromOutside proves nothing but this
// package can plant a routing scope. An extension carries its own values on the
// same context - that is what RewriteRequestParams is for - and one of them
// guessing the string "rg" or "resource_group" must not become the scope every
// shard-leader lookup honors. The scope is milvus's decision, made in exactly
// one place, and this is what keeps it that way.
func TestQueryResourceGroupKeyIsUnreachableFromOutside(t *testing.T) {
	ctx := context.Background()
	for _, guess := range []string{"rg", "resource_group", "queryResourceGroupKey"} {
		//nolint:staticcheck // planting a string key is exactly what is under test
		ctx = context.WithValue(ctx, guess, "forged-"+guess)
	}

	assert.Equal(t, "", QueryResourceGroupFromContext(ctx),
		"only WithQueryResourceGroup may set the scope routing reads back")
}

// TestQueryResourceGroupRebindShadowsTheOuterValue pins the precedence a nested
// bind has: a re-scoped context answers with the inner value while the context
// it derived from keeps answering with its own. A sub-query run under an outer
// request's context - the search-by-primary-key requery does exactly this -
// must not be able to re-route the request it runs inside.
func TestQueryResourceGroupRebindShadowsTheOuterValue(t *testing.T) {
	outer := WithQueryResourceGroup(context.Background(), "rg-outer")
	inner := WithQueryResourceGroup(outer, "rg-inner")

	assert.Equal(t, "rg-inner", QueryResourceGroupFromContext(inner))
	assert.Equal(t, "rg-outer", QueryResourceGroupFromContext(outer),
		"scoping a derived context must not rewrite the context it derived from")
}
