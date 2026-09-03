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

func TestInternalDomainMarkRoundTrips(t *testing.T) {
	ctx := context.Background()
	assert.False(t, FromInternalDomain(ctx), "an unmarked context must not read as internal")
	assert.True(t, FromInternalDomain(WithInternalDomain(ctx)))
}

func TestInternalDomainMarkCannotBeForgedWithAStringKey(t *testing.T) {
	// Middleware stuffing string keys into the same context - gin, for one -
	// must not be able to collide with the mark.
	ctx := context.WithValue(context.Background(), "milvus-internal-domain", true) //nolint:staticcheck
	assert.False(t, FromInternalDomain(ctx))
}
