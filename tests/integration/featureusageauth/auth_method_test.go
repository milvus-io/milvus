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

// Package featureusageauth drives the two auth_method counters of the feature
// usage report.
//
// They live here rather than in tests/integration/featureusage because they
// only move when common.security.authorizationEnabled is on, and that flag
// makes every request in a suite need a credential. Turning it on for the main
// suite would mean rewriting nineteen unrelated test methods; a second cluster
// is the cheaper price.
package featureusageauth

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/featureusage"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/crypto"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
	"github.com/milvus-io/milvus/tests/integration"
)

const defaultAuth = "root:Milvus"

type Suite struct {
	integration.MiniClusterSuite
}

func (s *Suite) SetupSuite() {
	s.WithMilvusConfig(paramtable.Get().CommonCfg.FeatureUsageEnabled.Key, "true")
	s.WithMilvusConfig(paramtable.Get().CommonCfg.FeatureUsageCountersEnabled.Key, "true")
	s.WithMilvusConfig(paramtable.Get().CommonCfg.AuthorizationEnabled.Key, "true")
	s.MiniClusterSuite.SetupSuite()
}

// authContext attaches a credential the way an SDK does: base64 of the raw
// token in the authorization header.
func authContext(ctx context.Context, rawToken string) context.Context {
	md := metadata.New(map[string]string{
		strings.ToLower(util.HeaderAuthorize): crypto.Base64Encode(rawToken),
	})
	return metadata.NewOutgoingContext(ctx, md)
}

func (s *Suite) proxyCounters(ctx context.Context) map[string]int64 {
	resp, err := s.Cluster.MixCoordClient.GetFeatureUsage(ctx, &internalpb.GetFeatureUsageRequest{})
	s.Require().NoError(err)
	s.Require().NoError(merr.Error(resp.GetStatus()))

	out := map[string]int64{}
	proxies := 0
	for _, n := range resp.GetNodes() {
		if n.GetRole() != typeutil.ProxyRole {
			continue
		}
		s.Require().True(n.GetReachable(), "proxy %d unreachable: %s", n.GetNodeId(), n.GetError())
		proxies++
		for _, e := range n.GetEntries() {
			if e.GetGroup() == featureusage.GroupRequest {
				out[e.GetName()] += e.GetValue()
			}
		}
	}
	s.Require().NotZero(proxies, "the report must list at least one proxy")
	return out
}

// TestPasswordAuthIsCounted proves the username/password branch of the Proxy's
// authentication interceptor moves auth_method=password.
//
// The delta is asserted as "at least one" rather than "exactly one": the
// counter is per authenticated RPC on the Proxy, and this suite cannot promise
// that no other RPC reaches the same Proxy between the two reads. What the
// assertion does pin is that the counter belongs to the password branch and
// that the API key branch stays still, which is the part a wrong hook site
// would break.
func (s *Suite) TestPasswordAuthIsCounted() {
	ctx := context.Background()
	before := s.proxyCounters(ctx)

	authed := authContext(ctx, defaultAuth)
	resp, err := s.Cluster.MilvusClient.ListDatabases(authed, &milvuspb.ListDatabasesRequest{})
	s.Require().NoError(err)
	s.Require().NoError(merr.Error(resp.GetStatus()))

	after := s.proxyCounters(ctx)
	s.Greater(after["auth_method=password"], before["auth_method=password"],
		"an authenticated request must move the password counter")
	s.Equal(before["auth_method=api_key"], after["auth_method=api_key"],
		"a username/password request must not touch the API key counter")
}

// TestRejectedCredentialIsNotCounted pins the other half of the rule: the
// counters sit after verification, so a wrong password moves nothing. Counting
// before the check would turn the report into a brute-force log.
func (s *Suite) TestRejectedCredentialIsNotCounted() {
	ctx := context.Background()
	before := s.proxyCounters(ctx)

	_, err := s.Cluster.MilvusClient.ListDatabases(authContext(ctx, "root:not-the-password"), &milvuspb.ListDatabasesRequest{})
	s.Require().Error(err, "a wrong password must be rejected at the interceptor")

	after := s.proxyCounters(ctx)
	s.Equal(before["auth_method=password"], after["auth_method=password"])
	s.Equal(before["auth_method=api_key"], after["auth_method=api_key"])
}

// TestAPIKeyIsNotCountedWithoutAHook records why auth_method=api_key cannot be
// driven to a non-zero value in this tree: VerifyAPIKey delegates to the hook
// extension, and the built-in DefaultHook rejects every key. A token with no
// separator therefore reaches the API key branch and fails there.
//
// If a future change makes OSS able to verify an API key, this test fails and
// the counter should move out of notDrivable in the main suite's coverage gate.
func (s *Suite) TestAPIKeyIsNotCountedWithoutAHook() {
	ctx := context.Background()
	before := s.proxyCounters(ctx)

	_, err := s.Cluster.MilvusClient.ListDatabases(authContext(ctx, "an-api-key-without-a-separator"), &milvuspb.ListDatabasesRequest{})
	s.Require().Error(err, "the default hook cannot verify an API key")

	after := s.proxyCounters(ctx)
	s.Equal(before["auth_method=api_key"], after["auth_method=api_key"])
	s.Equal(before["auth_method=password"], after["auth_method=password"])
}

func TestFeatureUsageAuth(t *testing.T) {
	suite.Run(t, new(Suite))
}
