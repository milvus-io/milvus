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

package datacoord

import (
	"github.com/milvus-io/milvus/internal/kv"
	"stathat.com/c/consistent"
)

// ChannelPolicyFactory is the abstract factory to create policies for channel manager
type ChannelPolicyFactory interface {
	// NewRegisterPolicy create a new register policy
	NewRegisterPolicy() RegisterPolicy
	// NewDeregisterPolicy create a new dereigster policy
	NewDeregisterPolicy() DeregisterPolicy
	// NewAssignPolicy create a new channel assign policy
	NewAssignPolicy() ChannelAssignPolicy
	// NewReassignPolicy create a new channel reassign policy
	NewReassignPolicy() ChannelReassignPolicy
	// NewBgChecker create a new bakcground checker
	NewBgChecker() ChannelBGChecker
}

// ChannelPolicyFactoryV1 equal to policy batch
type ChannelPolicyFactoryV1 struct {
	kv kv.TxnKV
}

// NewChannelPolicyFactoryV1 helper function creates a Channel policy factory v1 from kv
func NewChannelPolicyFactoryV1(kv kv.TxnKV) *ChannelPolicyFactoryV1 {
	return &ChannelPolicyFactoryV1{kv: kv}
}

// NewRegisterPolicy implementing ChannelPolicyFactory returns BufferChannelAssignPolicy
func (f *ChannelPolicyFactoryV1) NewRegisterPolicy() RegisterPolicy {
	return BufferChannelAssignPolicy
}

// NewDeregisterPolicy implementing ChannelPolicyFactory returns AvgAssignUnregisteredChannels
func (f *ChannelPolicyFactoryV1) NewDeregisterPolicy() DeregisterPolicy {
	return AvgAssignUnregisteredChannels
}

// NewAssignPolicy implementing ChannelPolicyFactory returns AverageAssignPolicy
func (f *ChannelPolicyFactoryV1) NewAssignPolicy() ChannelAssignPolicy {
	return AverageAssignPolicy
}

// NewReassignPolicy implementing ChannelPolicyFactory returns AvarageReassginPolicy
func (f *ChannelPolicyFactoryV1) NewReassignPolicy() ChannelReassignPolicy {
	return AverageReassignPolicy
}

// NewBgChecker implementing ChannelPolicyFactory
func (f *ChannelPolicyFactoryV1) NewBgChecker() ChannelBGChecker {
	return BgCheckWithMaxWatchDuration(f.kv)
}

// ConsistentHashChannelPolicyFactory use consistent hash to determine channel assignment
type ConsistentHashChannelPolicyFactory struct {
	hashring *consistent.Consistent
}

func NewConsistentHashChannelPolicyFactory(hashring *consistent.Consistent) *ConsistentHashChannelPolicyFactory {
	return &ConsistentHashChannelPolicyFactory{
		hashring: hashring,
	}
}

func (f *ConsistentHashChannelPolicyFactory) NewRegisterPolicy() RegisterPolicy {
	return ConsistentHashRegisterPolicy(f.hashring)
}

func (f *ConsistentHashChannelPolicyFactory) NewDeregisterPolicy() DeregisterPolicy {
	return ConsistentHashDeregisterPolicy(f.hashring)
}

func (f *ConsistentHashChannelPolicyFactory) NewAssignPolicy() ChannelAssignPolicy {
	return ConsistentHashChannelAssignPolicy(f.hashring)
}

func (f *ConsistentHashChannelPolicyFactory) NewReassignPolicy() ChannelReassignPolicy {
	return EmptyReassignPolicy
}

func (f *ConsistentHashChannelPolicyFactory) NewBgChecker() ChannelBGChecker {
	return EmptyBgChecker
}
