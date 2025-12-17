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

package sessionutil

import (
	"os"
	"strings"

	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// milvus server label rules:
// The server label of milvus will be injected into the session of milvus server,
// which can be found at SessionRaw.ServerLabels.
// The label is a envionment variable, the key must be prefixed with "MILVUS_SERVER_LABEL_".
// There are two types of labels:
//
// role-specific labels and non-role-specific labels.
// The key of role-specific labels have the format of "MILVUS_SERVER_LABEL_<ROLE>_<LABEL>",
// only the session of the role can see the label.
// The key of non-role-specific labels have the format of "MILVUS_SERVER_LABEL_<LABEL>",
// all sessions can see the label.
// e.g.
// export MILVUS_SERVER_LABEL_label1=value1, all roles can see the `label1:value1` in the server label of session.
// export MILVUS_SERVER_LABEL_querynode_label2=value2, only the session of querynode can see the `label2:value2` in the server label of session.
//
// the key of non-role-specific labels will be overwritten by the key of role-specific labels.
// e.g.
// export MILVUS_SERVER_LABEL_label1=value1, export MILVUS_SERVER_LABEL_querynode_label1=value2,
// the session of querynode will see the `label1:value2` in the server label of session because of the overwrite.
// the session of other roles will see the `label1:value1` in the server label of session.
const (
	SupportedLabelPrefix = "MILVUS_SERVER_LABEL_"

	// QueryNode
	LabelStreamingNodeEmbeddedQueryNode       = "STREAMING-EMBEDDED"
	LegacyLabelStreamingNodeEmbeddedQueryNode = "QUERYNODE_" + LabelStreamingNodeEmbeddedQueryNode

	// All Roles
	LabelStandalone    = "STANDALONE"
	LabelResourceGroup = "RESOURCE_GROUP"
)

// NewServerLabel creates a new server label with the given role and label.
func NewServerLabel(role string, label string) string {
	if role == "" {
		return SupportedLabelPrefix + strings.ToUpper(label)
	}
	return SupportedLabelPrefix + strings.ToUpper(role) + "_" + strings.ToUpper(label)
}

func getServerLabelsFromEnv(role string) map[string]string {
	labelsSpecifiedByRole := make(map[string]string)
	labels := make(map[string]string)
	for _, value := range os.Environ() {
		rs := []rune(value)
		in := strings.Index(value, "=")
		key := string(rs[0:in])
		value := string(rs[in+1:])

		roleFromEnv, label, ok := getRoleFromEnvKey(key)
		if !ok || (roleFromEnv != "" && roleFromEnv != role) {
			continue
		}
		if roleFromEnv == "" {
			labels[label] = value
		} else {
			labelsSpecifiedByRole[label] = value
		}
	}
	// use role specified labels to overwrite labels not specified by role.
	for label, value := range labelsSpecifiedByRole {
		labels[label] = value
	}
	return labels
}

func getRoleFromEnvKey(key string) (string, string, bool) {
	if !strings.HasPrefix(key, SupportedLabelPrefix) {
		return "", "", false
	}
	labelWithRole := strings.TrimPrefix(key, SupportedLabelPrefix)
	if len(labelWithRole) == 0 {
		return "", "", false
	}
	splits := strings.Split(labelWithRole, "_")
	var role string
	switch strings.ToLower(splits[0]) {
	case typeutil.MixCoordRole, "coord":
		role = typeutil.MixCoordRole
	case typeutil.QueryNodeRole, "qn":
		role = typeutil.QueryNodeRole
	case typeutil.DataNodeRole, "dn":
		role = typeutil.DataNodeRole
	case typeutil.StreamingNodeRole, "sn":
		role = typeutil.StreamingNodeRole
	case typeutil.ProxyRole:
		role = typeutil.ProxyRole
	default:
	}

	// if role is not found, the label can be seen session in all roles.
	if role == "" {
		return "", labelWithRole, true
	}
	// else, it's a selected role label, only can be seen in the selected role.
	return role, strings.Join(splits[1:], "_"), true
}
