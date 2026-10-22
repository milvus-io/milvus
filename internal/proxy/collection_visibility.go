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

	"github.com/milvus-io/milvus/internal/proxy/privilege"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func localCollectionVisibilityEnabled() bool {
	cache := privilege.GetPrivilegeCache()
	return Params.CommonCfg.AuthorizationEnabled.GetAsBool() &&
		Params.ProxyCfg.EnableLocalCollectionVisibility.GetAsBool() &&
		cache != nil && cache.SupportsCollectionVisibility()
}

// tryLocalCollectionVisibility never treats an unsupported projection as an
// empty (deny-all) projection. Unknown means the existing RC check is required.
func tryLocalCollectionVisibility(ctx context.Context, database, collection string, id int64) (known bool, err error) {
	if !localCollectionVisibilityEnabled() {
		return false, nil
	}
	if err := checkDescribeCollectionUser(ctx); err != nil {
		return true, err
	}
	if database == "" || collection == "" || id <= 0 {
		return true, merr.WrapErrServiceUnavailable("resolved collection identity is unavailable for visibility check")
	}
	if !Params.CommonCfg.AuthorizationEnabled.GetAsBool() {
		return true, nil
	}
	user, err := GetCurUserFromContext(ctx)
	if err != nil {
		return true, err
	}
	if user == util.UserRoot && !Params.CommonCfg.RootShouldBindRole.GetAsBool() {
		return true, nil
	}
	cache := privilege.GetPrivilegeCache()
	if cache == nil {
		return false, nil
	}
	visible, supported := cache.CanSeeCollection(user, database, collection)
	if !supported {
		return false, nil
	}
	if !visible {
		return true, merr.WrapErrPrivilegeNotPermitted("not allowed to access collection")
	}
	return true, nil
}
