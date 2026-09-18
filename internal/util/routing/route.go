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

package routing

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// The residue of a primary key is its routing hash modulo the collection's
// routing modulus. The hash is exactly the one typeutil.HashPK2Channels uses
// (Hash32Int64 for int64 keys, HashString2Uint32 for varchar keys), so for a
// never-split N-shard collection PKResidue(pk, N) is the HashPK2Channels index,
// and the legacy table sends every row where it has always gone.

// PKResidueInt64 is the residue of an int64 primary key modulo modulus.
func PKResidueInt64(pk int64, modulus uint64) (uint64, error) {
	if err := checkModulus(modulus); err != nil {
		return 0, err
	}
	return int64Residue(pk, modulus)
}

// PKResidueVarChar is the residue of a varchar primary key modulo modulus.
func PKResidueVarChar(pk string, modulus uint64) (uint64, error) {
	if err := checkModulus(modulus); err != nil {
		return 0, err
	}
	return varCharResidue(pk, modulus), nil
}

// int64Residue and varCharResidue are the one copy of each hash; the callers
// have already checked the modulus.
func int64Residue(pk int64, modulus uint64) (uint64, error) {
	h, err := typeutil.Hash32Int64(pk)
	if err != nil {
		return 0, err
	}
	return uint64(h) % modulus, nil
}

func varCharResidue(pk string, modulus uint64) uint64 {
	return uint64(typeutil.HashString2Uint32(pk)) % modulus
}

// PKResidue is the residue of a primary key, an int64 or a string, modulo
// modulus. Any other type is a caller bug: a primary key is one of the two.
func PKResidue(pk any, modulus uint64) (uint64, error) {
	switch v := pk.(type) {
	case int64:
		return PKResidueInt64(v, modulus)
	case string:
		return PKResidueVarChar(v, modulus)
	default:
		return 0, merr.WrapErrServiceInternalMsg("primary key of type %T has no routing residue", pk)
	}
}

// PKResidues is the residue of every key of a primary key batch, in order. A
// batch with no key field set yields no residues, as HashPK2Channels does.
func PKResidues(pks *schemapb.IDs, modulus uint64) ([]uint64, error) {
	if err := checkModulus(modulus); err != nil {
		return nil, err
	}
	switch pks.GetIdField().(type) {
	case *schemapb.IDs_IntId:
		data := pks.GetIntId().GetData()
		residues := make([]uint64, len(data))
		for i, pk := range data {
			r, err := int64Residue(pk, modulus)
			if err != nil {
				return nil, err
			}
			residues[i] = r
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

// Modulus is the table's routing modulus M.
func (t *ResidueTable) Modulus() uint64 {
	if t == nil {
		return 0
	}
	return t.modulus
}

// Lookup is the vchannel owning residue. It reports false for a residue not
// below the modulus, and for a nil table.
func (t *ResidueTable) Lookup(residue uint64) (string, bool) {
	if t == nil || residue >= t.modulus {
		return "", false
	}
	return t.slots[residue], true
}

// VChannelOfPK is the vchannel owning a primary key, an int64 or a string.
func (t *ResidueTable) VChannelOfPK(pk any) (string, error) {
	if t == nil {
		return "", merr.WrapErrServiceInternal("no routing table to place the primary key")
	}
	r, err := PKResidue(pk, t.modulus)
	if err != nil {
		return "", err
	}
	vchannel, _ := t.Lookup(r)
	return vchannel, nil
}

// TableFromMeta builds a collection's residue table from its routing meta: its
// vchannel list, its per-shard infos (parallel to the list, or empty) and its
// routing modulus. It is ShardsFromMeta followed by Derive, so a never-split
// collection (modulus 0, no residues) gets the legacy table: modulus
// len(vchannels), shard i owning residue i. A non-zero modulus with no residues
// is refused, as Derive refuses it.
func TableFromMeta(vchannels []string, infos []*schemapb.CollectionShardInfo, modulus uint64) (*ResidueTable, error) {
	shards, err := ShardsFromMeta(vchannels, infos)
	if err != nil {
		return nil, err
	}
	return Derive(modulus, vchannels, shards)
}
