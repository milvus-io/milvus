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
package routing

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// The residue of a primary key is its routing hash modulo the routing modulus.
// The hash is the one typeutil.HashPK2Channels uses, so on a collection that
// was never split the residue is the channel index HashPK2Channels returns.

// PKResidueInt64 returns the residue of an int64 primary key.
func PKResidueInt64(pk int64, modulus uint64) (uint64, error) {
	if err := checkModulus(modulus); err != nil {
		return 0, err
	}
	return int64Residue(pk, modulus)
}

// PKResidueVarChar returns the residue of a varchar primary key.
func PKResidueVarChar(pk string, modulus uint64) (uint64, error) {
	if err := checkModulus(modulus); err != nil {
		return 0, err
	}
	return varCharResidue(pk, modulus), nil
}

// PKResidues returns the residue of every key of a batch, in order. A batch
// with no id field yields no residues, as HashPK2Channels does.
func PKResidues(pks *schemapb.IDs, modulus uint64) ([]uint64, error) {
	if err := checkModulus(modulus); err != nil {
		return nil, err
	}
	switch pks.GetIdField().(type) {
	case *schemapb.IDs_IntId:
		data := pks.GetIntId().GetData()
		residues := make([]uint64, len(data))
		for i, pk := range data {
			residue, err := int64Residue(pk, modulus)
			if err != nil {
				return nil, err
			}
			residues[i] = residue
		}
		return residues, nil
	case *schemapb.IDs_StrId:
		data := pks.GetStrId().GetData()
		residues := make([]uint64, len(data))
		for i, pk := range data {
			residues[i] = varCharResidue(pk, modulus)
		}
		return residues, nil
	default:
		return nil, nil
	}
}

// VChannelsOfPKs returns the vchannel that owns each key of a batch, in order.
func (t *Table) VChannelsOfPKs(pks *schemapb.IDs) ([]string, error) {
	residues, err := PKResidues(pks, t.modulus)
	if err != nil {
		return nil, err
	}
	vchannels := make([]string, len(residues))
	for i, residue := range residues {
		vchannels[i] = t.slots[residue]
	}
	return vchannels, nil
}

func int64Residue(pk int64, modulus uint64) (uint64, error) {
	hash, err := typeutil.Hash32Int64(pk)
	if err != nil {
		return 0, err
	}
	return uint64(hash) % modulus, nil
}

func varCharResidue(pk string, modulus uint64) uint64 {
	return uint64(typeutil.HashString2Uint32(pk)) % modulus
}

func checkModulus(modulus uint64) error {
	if modulus == 0 {
		return merr.WrapErrServiceInternal("routing modulus must be positive")
	}
	if modulus > MaxModulus {
		return merr.WrapErrServiceInternalMsg("routing modulus %d exceeds the bound %d", modulus, MaxModulus)
	}
	return nil
}
