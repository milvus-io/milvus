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

package meta

import (
	"math/big"
	"sort"
)

// assignGroupQuotas uses exact arithmetic: neither summing many collections nor
// multiplying a collection row count by the node count can overflow int64.
func assignGroupQuotas(members []*groupReplicaAssignment, nodes int) {
	if len(members) == 0 {
		return
	}
	total := new(big.Int)
	for _, member := range members {
		total.Add(total, big.NewInt(member.rows))
	}
	empty := total.Sign() == 0
	if empty {
		total.SetInt64(int64(len(members)))
	}
	type fraction struct {
		member    *groupReplicaAssignment
		remainder *big.Int
	}
	fractions := make([]fraction, 0, len(members))
	remaining := nodes
	for _, member := range members {
		rows := member.rows
		if empty {
			rows = 1
		}
		numerator := new(big.Int).Mul(big.NewInt(rows), big.NewInt(int64(nodes)))
		quotient, remainder := new(big.Int), new(big.Int)
		quotient.QuoRem(numerator, total, remainder)
		member.quota = int(quotient.Int64())
		remaining -= member.quota
		fractions = append(fractions, fraction{member, remainder})
	}
	sort.SliceStable(fractions, func(i, j int) bool {
		a, b := fractions[i], fractions[j]
		if cmp := a.remainder.Cmp(b.remainder); cmp != 0 {
			return cmp > 0
		}
		aRetain := a.member.replica.RWNodesCount() > a.member.quota
		bRetain := b.member.replica.RWNodesCount() > b.member.quota
		if aRetain != bRetain {
			return aRetain
		}
		return a.member.replica.GetID() < b.member.replica.GetID()
	})
	for i := 0; i < remaining; i++ {
		fractions[i].member.quota++
	}
	for _, member := range members {
		member.quota = max(1, member.quota)
	}
}
