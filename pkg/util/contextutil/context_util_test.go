/*
 * Licensed to the LF AI & Data foundation under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package contextutil

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus/pkg/v2/util"
	"github.com/milvus-io/milvus/pkg/v2/util/crypto"
)

func TestAppendToIncomingContext(t *testing.T) {
	t.Run("invalid kvs", func(t *testing.T) {
		assert.Panics(t, func() {
			// nolint
			AppendToIncomingContext(context.Background(), "foo")
		})
	})

	t.Run("valid kvs", func(t *testing.T) {
		ctx := context.Background()
		ctx = AppendToIncomingContext(ctx, "foo", "bar")
		md, ok := metadata.FromIncomingContext(ctx)
		assert.True(t, ok)
		assert.Equal(t, "bar", md.Get("foo")[0])
	})
}

func TestGetCurUserFromContext(t *testing.T) {
	_, err := GetCurUserFromContext(context.Background())
	assert.Error(t, err)

	_, err = GetCurUserFromContext(metadata.NewIncomingContext(context.Background(), metadata.New(map[string]string{})))
	assert.Error(t, err)

	_, err = GetCurUserFromContext(GetContext(context.Background(), "123456"))
	assert.Error(t, err)

	root := "root"
	password := "123456"
	username, err := GetCurUserFromContext(GetContext(context.Background(), fmt.Sprintf("%s%s%s", root, util.CredentialSeperator, password)))
	assert.NoError(t, err)
	assert.Equal(t, root, username)

	{
		u, p, e := GetAuthInfoFromContext(GetContext(context.Background(), fmt.Sprintf("%s%s%s", root, util.CredentialSeperator, password)))
		assert.NoError(t, e)
		assert.Equal(t, "root", u)
		assert.Equal(t, password, p)
	}
}

func GetContext(ctx context.Context, originValue string) context.Context {
	authKey := strings.ToLower(util.HeaderAuthorize)
	authValue := crypto.Base64Encode(originValue)
	contextMap := map[string]string{
		authKey: authValue,
	}
	md := metadata.New(contextMap)
	return metadata.NewIncomingContext(ctx, md)
}
