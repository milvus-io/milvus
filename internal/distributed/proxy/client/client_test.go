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

package grpcproxyclient

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_NewClient(t *testing.T) {
	client, err := NewClient(context.Background(), "")
	assert.Nil(t, client)
	assert.Error(t, err)

	client, err = NewClient(context.Background(), "localhost")
	assert.Nil(t, err)
	assert.NotNil(t, client)

	err = client.Start()
	assert.Nil(t, err)

	err = client.Stop()
	assert.Nil(t, err)

	err = client.Register()
	assert.Nil(t, err)
}
