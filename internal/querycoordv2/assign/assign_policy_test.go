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

package assign

import (
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
)

// AssignPolicyTestSuite tests the assign policy interfaces and implementations
type AssignPolicyTestSuite struct {
	suite.Suite
	nodeManager *session.NodeManager
	dist        *meta.DistributionManager
	meta        *meta.Meta
}

func TestAssignPolicyTestSuite(t *testing.T) {
	suite.Run(t, new(AssignPolicyTestSuite))
}

// SetupSuite initializes the global factory before all tests
func (suite *AssignPolicyTestSuite) SetupSuite() {
	suite.nodeManager = session.NewNodeManager()
	suite.dist = meta.NewDistributionManager(suite.nodeManager)
	suite.meta = meta.NewMeta(nil, nil, suite.nodeManager)

	// Initialize global factory for tests
	InitGlobalAssignPolicyFactory(nil, suite.nodeManager, suite.dist, suite.meta, nil)
}

// TearDownSuite cleans up after all tests
func (suite *AssignPolicyTestSuite) TearDownSuite() {
	ResetGlobalAssignPolicyFactoryForTest()
}

// TestAssignPolicyInterface verifies that all policies implement the unified AssignPolicy interface
func (suite *AssignPolicyTestSuite) TestAssignPolicyInterface() {
	// This test verifies that our policies correctly implement the unified interface
	// It's a compile-time check that ensures interface compatibility

	var _ AssignPolicy = (*RoundRobinAssignPolicy)(nil)
	var _ AssignPolicy = (*RowCountBasedAssignPolicy)(nil)
	var _ AssignPolicy = (*ScoreBasedAssignPolicy)(nil)
}

// TestPolicyFactory tests the factory creates correct policy types
func (suite *AssignPolicyTestSuite) TestPolicyFactory() {
	factory := GetGlobalAssignPolicyFactory()
	suite.NotNil(factory, "Global factory should be initialized")

	// Test RoundRobin policy creation
	rrPolicy := factory.GetPolicy(PolicyTypeRoundRobin)
	suite.NotNil(rrPolicy, "Should create RoundRobin policy")
	_, ok := rrPolicy.(*RoundRobinAssignPolicy)
	suite.True(ok, "Should be RoundRobinAssignPolicy type")

	// Test RowCountBased policy creation
	rcPolicy := factory.GetPolicy(PolicyTypeRowCount)
	suite.NotNil(rcPolicy, "Should create RowCountBased policy")
	_, ok = rcPolicy.(*RowCountBasedAssignPolicy)
	suite.True(ok, "Should be RowCountBasedAssignPolicy type")

	// Test ScoreBased policy creation
	sbPolicy := factory.GetPolicy(PolicyTypeScoreBased)
	suite.NotNil(sbPolicy, "Should create ScoreBased policy")
	_, ok = sbPolicy.(*ScoreBasedAssignPolicy)
	suite.True(ok, "Should be ScoreBasedAssignPolicy type")

	// Test policy caching - should return same instance
	rrPolicy2 := factory.GetPolicy(PolicyTypeRoundRobin)
	suite.Same(rrPolicy, rrPolicy2, "Should return cached instance")
}

// TestSegmentAssignPlan_String tests the string representation of SegmentAssignPlan
func (suite *AssignPolicyTestSuite) TestSegmentAssignPlan_String() {
	replica := newTestReplica(1)

	plan := SegmentAssignPlan{
		Segment: &meta.Segment{
			SegmentInfo: &datapb.SegmentInfo{
				ID:           100,
				CollectionID: 200,
			},
		},
		Replica:      replica,
		From:         1,
		To:           2,
		FromScore:    1000,
		ToScore:      500,
		SegmentScore: 250,
	}

	str := plan.String()
	suite.Contains(str, "collectionID: 200")
	suite.Contains(str, "replicaID: 1")
	suite.Contains(str, "segmentID: 100")
	suite.Contains(str, "from: 1")
	suite.Contains(str, "to: 2")
	suite.Contains(str, "fromScore: 1000")
	suite.Contains(str, "toScore: 500")
	suite.Contains(str, "segmentScore: 250")
}

// TestChannelAssignPlan_String tests the string representation of ChannelAssignPlan
func (suite *AssignPolicyTestSuite) TestChannelAssignPlan_String() {
	replica := newTestReplica(1)

	plan := ChannelAssignPlan{
		Channel: &meta.DmChannel{
			VchannelInfo: &datapb.VchannelInfo{
				CollectionID: 200,
				ChannelName:  "test-channel",
			},
		},
		Replica:      replica,
		From:         1,
		To:           2,
		FromScore:    100,
		ToScore:      50,
		ChannelScore: 25,
	}

	str := plan.String()
	suite.Contains(str, "collectionID: 200")
	suite.Contains(str, "channel: test-channel")
	suite.Contains(str, "replicaID: 1")
	suite.Contains(str, "from: 1")
	suite.Contains(str, "to: 2")
}

// TestUnknownPolicyType tests that unknown policy type defaults to ScoreBased
func (suite *AssignPolicyTestSuite) TestUnknownPolicyType() {
	factory := GetGlobalAssignPolicyFactory()

	// Test unknown policy type defaults to ScoreBased
	defaultPolicy := factory.GetPolicy("unknown")
	suite.NotNil(defaultPolicy)
	_, ok := defaultPolicy.(*ScoreBasedAssignPolicy)
	suite.True(ok, "Unknown policy type should default to ScoreBased")
}

// TestAssignPolicyConfig tests AssignPolicyConfig struct
func (suite *AssignPolicyTestSuite) TestAssignPolicyConfig() {
	config := AssignPolicyConfig{
		BatchSize:          100,
		EnableBenefitCheck: true,
	}

	suite.Equal(100, config.BatchSize)
	suite.True(config.EnableBenefitCheck)
}

// Helper function to create test replica
func newTestReplica(replicaID int64) *meta.Replica {
	//nolint:staticcheck
	return meta.NewReplica(
		&querypb.Replica{
			ID: replicaID,
		},
		nil,
	)
}
