// Copyright 2016 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package member

import (
	"strings"
	"time"

	"github.com/czs007/suvlim/master/election"

	"github.com/czs007/suvlim/pkg/pdpb"

	"github.com/czs007/suvlim/master/config"

	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
)

const (
	// The timeout to wait transfer etcd leader to complete.
	moveLeaderTimeout = 5 * time.Second
)

// Member is used for the election related logic.
type Member struct {
	// etcd and cluster information.
	leadership *election.Leadership
	etcd       *embed.Etcd
	client     *clientv3.Client
	id         uint64       // etcd server id.
	member     *pdpb.Member // current PD's info.
	// memberValue is the serialized string of `member`. It will be save in
	// etcd leader key when the PD node is successfully elected as the PD leader
	// of the cluster. Every write will use it to check PD leadership.
	memberValue string
}

// NewMember create a new Member.
func NewMember(etcd *embed.Etcd, client *clientv3.Client, id uint64) *Member {
	return &Member{
		etcd:   etcd,
		client: client,
		id:     id,
	}
}

// ID returns the unique etcd ID for this server in etcd cluster.
func (m *Member) ID() uint64 {
	return m.id
}

// MemberValue returns the member value.
func (m *Member) MemberValue() string {
	return m.memberValue
}

// Member returns the member.
func (m *Member) Member() *pdpb.Member {
	return m.member
}

// Etcd returns etcd related information.
func (m *Member) Etcd() *embed.Etcd {
	return m.etcd
}

// GetEtcdLeader returns the etcd leader ID.
func (m *Member) GetEtcdLeader() uint64 {
	return m.etcd.Server.Lead()
}

// GetLeadership returns the leadership of the PD member.
func (m *Member) GetLeadership() *election.Leadership {
	return m.leadership
}

// MemberInfo initializes the member info.
func (m *Member) MemberInfo(cfg *config.Config, name string) {
	leader := &pdpb.Member{
		Name:       name,
		MemberId:   m.ID(),
		ClientUrls: strings.Split(cfg.AdvertiseClientUrls, ","),
		PeerUrls:   strings.Split(cfg.AdvertisePeerUrls, ","),
	}
	m.member = leader
}

// Close gracefully shuts down all servers/listeners.
func (m *Member) Close() {
	m.Etcd().Close()
}
