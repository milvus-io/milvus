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

package rootcoord

import (
	"strconv"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// Params mirrors the internal/rootcoord package-level handle so the meta tests
// that moved here keep reading config through the same accessor.
var Params = paramtable.Get()

// fakeCipherHelper is a test DatabaseCipherHelper that mirrors the production
// hookutil-backed behavior without depending on internal/util/hookutil: when a
// default root key is configured it stamps the encryption properties (ezID +
// root key) onto the database, otherwise it leaves the properties untouched.
type fakeCipherHelper struct{}

func (fakeCipherHelper) TidyDBCipherProperties(ezID int64, props []*commonpb.KeyValuePair) ([]*commonpb.KeyValuePair, error) {
	defaultKey := paramtable.GetCipherParams().DefaultRootKey.GetValue()
	if defaultKey == "" {
		return props, nil
	}
	return append(props,
		&commonpb.KeyValuePair{Key: common.EncryptionEzIDKey, Value: strconv.FormatInt(ezID, 10)},
		&commonpb.KeyValuePair{Key: common.EncryptionRootKeyKey, Value: defaultKey},
	), nil
}

func (fakeCipherHelper) CreateEZByDBProperties([]*commonpb.KeyValuePair) error { return nil }

// isDBEncrypted reports whether the database properties carry an encryption root key.
func isDBEncrypted(props []*commonpb.KeyValuePair) bool {
	for _, p := range props {
		if p.GetKey() == common.EncryptionRootKeyKey {
			return true
		}
	}
	return false
}
