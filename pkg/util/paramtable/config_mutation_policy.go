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

import "github.com/milvus-io/milvus/pkg/v3/config"

// ConfigMutationOperation identifies the external mutation a caller wants to
// perform. Deletes are distinct because they reactivate a lower-priority value.
type ConfigMutationOperation int

const (
	// ConfigMutationSet replaces the high-priority value for a key.
	ConfigMutationSet ConfigMutationOperation = iota
	// ConfigMutationDelete removes the high-priority value for a key.
	ConfigMutationDelete
)

// ConfigMutationRejection identifies the policy that rejected a mutation.
// The management transport maps it to its stable HTTP error text.
type ConfigMutationRejection int

const (
	// ConfigMutationAllowed means no mutation policy rejected the request.
	ConfigMutationAllowed ConfigMutationRejection = iota
	// ConfigMutationSecurityGoverning protects the management access boundary.
	ConfigMutationSecurityGoverning
	// ConfigMutationWALType requires the dedicated WAL transition protocol.
	ConfigMutationWALType
	// ConfigMutationImmutable protects a configuration declared immutable.
	ConfigMutationImmutable
	// ConfigMutationUnregistered refuses creation outside declared namespaces.
	ConfigMutationUnregistered
	// ConfigMutationSensitive protects credentials and topology targets.
	ConfigMutationSensitive
)

// ConfigMutationDecision is the complete result of external mutation policy.
// CanonicalKey is the single identity used by every later check and by etcd
// key formatting.
type ConfigMutationDecision struct {
	CanonicalKey string
	Rejection    ConfigMutationRejection
}

// EvaluateConfigMutation centralizes every rule an external generic config
// mutation must pass. Keeping identity resolution and policy in one module
// prevents a new transport from applying the same predicates in a different
// order or against a different spelling.
func EvaluateConfigMutation(manager *config.Manager, key string, operation ConfigMutationOperation) ConfigMutationDecision {
	canonicalKey, registeredKind := manager.ResolveRegisteredConfigKey(key)
	decision := ConfigMutationDecision{CanonicalKey: canonicalKey}

	switch {
	case IsSecurityGoverningConfig(canonicalKey):
		decision.Rejection = ConfigMutationSecurityGoverning
	case config.EtcdConfigKey(canonicalKey) == config.EtcdConfigKey("mq.type"):
		decision.Rejection = ConfigMutationWALType
	case manager.IsImmutable(canonicalKey):
		decision.Rejection = ConfigMutationImmutable
	case operation == ConfigMutationSet && registeredKind == config.RegisteredConfigUnknown:
		decision.Rejection = ConfigMutationUnregistered
	case operation == ConfigMutationSet && manager.IsSensitive(canonicalKey):
		decision.Rejection = ConfigMutationSensitive
	case operation == ConfigMutationDelete && registeredKind == config.RegisteredConfigScalar && manager.IsSensitive(canonicalKey):
		decision.Rejection = ConfigMutationSensitive
	}
	return decision
}
