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

package datacoord

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Pages are immutable and content-addressed. Only the small root is published;
// an interrupted prepare leaves unreachable pages, never a partial value.
const (
	metadataPageBytes = 256 << 10
	metadataRootBytes = 1 << 20
	pagedValueMagic   = "milvus-paged-v1:"
)

type pagedValueRoot struct {
	Hash         string
	Bytes        int
	Pages        int
	CollectionID int64
}

func (kc *Catalog) preparePagedValue(ctx context.Context, collectionID int64, value []byte) (string, error) {
	if len(value) <= metadataPageBytes {
		return string(value), nil
	}
	sum := sha256.Sum256(value)
	hash := hex.EncodeToString(sum[:])
	count := (len(value) + metadataPageBytes - 1) / metadataPageBytes
	root, err := json.Marshal(pagedValueRoot{Hash: hash, Bytes: len(value), Pages: count, CollectionID: collectionID})
	if err != nil {
		return "", err
	}
	if len(root)+len(pagedValueMagic) > metadataRootBytes {
		return "", merr.Wrapf(merr.ErrServiceResourceInsufficient, "metadata page root exceeds storage budget")
	}
	for i := 0; i < count; i++ {
		end := min(len(value), (i+1)*metadataPageBytes)
		if err := kc.MetaKv.Save(ctx, fmt.Sprintf("datacoord-meta/pages/%d/%s/%d", collectionID, hash, i), string(value[i*metadataPageBytes:end])); err != nil {
			return "", err
		}
	}
	return pagedValueMagic + string(root), nil
}

func (kc *Catalog) readPagedValue(ctx context.Context, value string) ([]byte, error) {
	if !strings.HasPrefix(value, pagedValueMagic) {
		return []byte(value), nil
	}
	var root pagedValueRoot
	if err := json.Unmarshal([]byte(strings.TrimPrefix(value, pagedValueMagic)), &root); err != nil {
		return nil, merr.WrapErrDataIntegrityMsg("invalid paged metadata root")
	}
	if root.CollectionID <= 0 || root.Pages <= 0 || root.Bytes <= metadataPageBytes || root.Pages != 1+(root.Bytes-1)/metadataPageBytes || len(root.Hash) != 64 {
		return nil, merr.WrapErrDataIntegrityMsg("invalid paged metadata dimensions")
	}
	if _, err := hex.DecodeString(root.Hash); err != nil {
		return nil, merr.WrapErrDataIntegrityMsg("invalid metadata page hash")
	}
	var out []byte
	for i := 0; i < root.Pages; i++ {
		page, err := kc.MetaKv.Load(ctx, fmt.Sprintf("datacoord-meta/pages/%d/%s/%d", root.CollectionID, root.Hash, i))
		if err != nil {
			return nil, err
		}
		expected := min(metadataPageBytes, root.Bytes-i*metadataPageBytes)
		if len(page) != expected {
			return nil, merr.WrapErrDataIntegrityMsg("metadata page %d has wrong size", i)
		}
		out = append(out, page...)
	}
	sum := sha256.Sum256(out)
	if hex.EncodeToString(sum[:]) != root.Hash {
		return nil, merr.WrapErrDataIntegrityMsg("metadata pages checksum mismatch")
	}
	return out, nil
}

// Never-split collections preserve their existing on-disk format. A durable
// split epoch is the local capability boundary for later flush/manifest views.
func (kc *Catalog) prepareDataViewValue(ctx context.Context, view *viewpb.DataViewOfCollection, encoded []byte) (string, error) {
	if view.GetShardSetEpoch() < 2 {
		return string(encoded), nil
	}
	return kc.preparePagedValue(ctx, view.GetCollectionId(), encoded)
}
