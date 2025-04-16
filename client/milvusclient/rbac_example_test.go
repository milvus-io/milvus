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

// nolint
package milvusclient_test

import (
	"context"

	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

func ExampleClient_GrantPrivilege() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cli, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address: milvusAddr,
	})
	if err != nil {
		// handle error
	}

	defer cli.Close(ctx)

	readOnlyPrivileges := []*entity.RoleGrants{
		{Object: "Global", ObjectName: "*", PrivilegeName: "DescribeCollection"},
		{Object: "Global", ObjectName: "*", PrivilegeName: "ShowCollections"},
		{Object: "Collection", ObjectName: "quick_setup", PrivilegeName: "Search"},
	}

	for _, grantItem := range readOnlyPrivileges {
		err := cli.GrantPrivilege(ctx, milvusclient.NewGrantPrivilegeOption("my_role", grantItem.Object, grantItem.PrivilegeName, grantItem.ObjectName))
		if err != nil {
			// handle error
		}
	}
}

func ExampleClient_GrantPrivilegeV2() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cli, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address: milvusAddr,
	})
	if err != nil {
		// handle error
	}

	defer cli.Close(ctx)

	err = cli.GrantPrivilegeV2(ctx, milvusclient.NewGrantPrivilegeV2Option("my_role", "Search", "quick_setup"))
	if err != nil {
		// handle error
	}
}
