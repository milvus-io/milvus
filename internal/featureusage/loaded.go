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

package featureusage

import (
	"strconv"

	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

// Names of GroupLoaded entries: what QueryCoord currently has loaded, as
// opposed to what collections declare. Computed from QueryCoord's in-memory
// load metadata on every call.
const (
	LoadedCollections          = "collections"
	LoadedFieldsSubset         = "load_fields_subset"
	LoadedCustomResourceGroups = "custom_resource_groups"
	// DistLoadedReplicaNumber is the effective replica count of loaded
	// collections, as opposed to the collection.replica.number property.
	DistLoadedReplicaNumber = "loaded_replica_number"
)

// LoadedCollection is what the loaded group needs about one loaded collection.
type LoadedCollection struct {
	ReplicaNumber    int32
	LoadFieldsSubset bool     // the load request named a subset of fields
	ResourceGroups   []string // resource groups its replicas are placed in
}

// ComputeLoadedEntries computes GroupLoaded and the loaded_replica_number
// distribution. Resource group names are user strings and are never emitted;
// only "placed outside the default resource group" is counted.
func ComputeLoadedEntries(cols []LoadedCollection) []*internalpb.FeatureEntry {
	c := newCollector()
	c.set(GroupLoaded, LoadedCollections, int64(len(cols)))
	c.ensure(GroupLoaded, LoadedFieldsSubset, "")
	c.ensure(GroupLoaded, LoadedCustomResourceGroups, "")
	for _, col := range cols {
		if col.LoadFieldsSubset {
			c.add(GroupLoaded, LoadedFieldsSubset, "")
		}
		for _, rg := range col.ResourceGroups {
			if rg != common.DefaultResourceGroupName {
				c.add(GroupLoaded, LoadedCustomResourceGroups, "")
				break
			}
		}
		replicas := int64(col.ReplicaNumber)
		if replicas < 1 {
			replicas = 1
		}
		c.add(GroupDist, DistLoadedReplicaNumber, replicaNumberBuckets.bucket(replicas))
	}
	return c.entries()
}

// BoolConfigEntry is one GroupConfig entry: a node-level boolean configuration
// item in effect on the reporting node. The key is an official configuration
// key (a constant in paramtable) and the value is rendered as true/false, so
// the entry name is drawn from a code-defined set.
func BoolConfigEntry(key string, value bool) *internalpb.FeatureEntry {
	return &internalpb.FeatureEntry{
		Group: GroupConfig,
		Name:  key + "=" + strconv.FormatBool(value),
		Value: 1,
	}
}
