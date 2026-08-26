// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package paramtable

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/config"
)

func TestEvaluateConfigMutation(t *testing.T) {
	manager := config.NewManager()
	manager.RegisterConfigKey("common.security.authorizationEnabled")
	manager.RegisterConfigKey("mq.type")
	manager.RegisterConfigKey("public.key")
	manager.RegisterConfigKey("sensitive.key")
	manager.RegisterSensitiveKey("sensitive.key")
	manager.RegisterConfigKey("immutable.key")
	manager.ImmutableUpdate("immutable.key")
	manager.RegisterConfigPrefix("sensitive.group.")
	manager.RegisterSensitivePrefix("sensitive.group.")

	tests := []struct {
		name      string
		key       string
		operation ConfigMutationOperation
		canonical string
		want      ConfigMutationRejection
	}{
		{name: "public set", key: "PUBLIC_KEY", operation: ConfigMutationSet, canonical: "public.key", want: ConfigMutationAllowed},
		{name: "public delete", key: "public/key", operation: ConfigMutationDelete, canonical: "public.key", want: ConfigMutationAllowed},
		{name: "security set", key: "COMMON_SECURITY_AUTHORIZATION_ENABLED", operation: ConfigMutationSet, canonical: "common.security.authorizationenabled", want: ConfigMutationSecurityGoverning},
		{name: "security delete", key: "common.security.superUsers", operation: ConfigMutationDelete, canonical: "common.security.superusers", want: ConfigMutationSecurityGoverning},
		{name: "wal set", key: "MQ_TYPE", operation: ConfigMutationSet, canonical: "mq.type", want: ConfigMutationWALType},
		{name: "wal delete", key: "mq/type", operation: ConfigMutationDelete, canonical: "mq.type", want: ConfigMutationWALType},
		{name: "immutable", key: "immutable_key", operation: ConfigMutationSet, canonical: "immutable.key", want: ConfigMutationImmutable},
		{name: "unknown set", key: "legacy.unknown", operation: ConfigMutationSet, canonical: "legacy.unknown", want: ConfigMutationUnregistered},
		{name: "unknown cleanup", key: "legacy.unknown", operation: ConfigMutationDelete, canonical: "legacy.unknown", want: ConfigMutationAllowed},
		{name: "sensitive scalar set", key: "sensitive_key", operation: ConfigMutationSet, canonical: "sensitive.key", want: ConfigMutationSensitive},
		{name: "sensitive scalar delete", key: "sensitive.key", operation: ConfigMutationDelete, canonical: "sensitive.key", want: ConfigMutationSensitive},
		{name: "sensitive group set", key: "sensitive.group.member", operation: ConfigMutationSet, canonical: "sensitive.group.member", want: ConfigMutationSensitive},
		{name: "sensitive group cleanup", key: "sensitive.group.member", operation: ConfigMutationDelete, canonical: "sensitive.group.member", want: ConfigMutationAllowed},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			decision := EvaluateConfigMutation(manager, test.key, test.operation)
			assert.Equal(t, test.canonical, decision.CanonicalKey)
			assert.Equal(t, test.want, decision.Rejection)
		})
	}
}
