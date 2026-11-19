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

	"google.golang.org/protobuf/proto"

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
	Version      *viewpb.DataVersion
}

func dataViewPagesPrefix(collectionID int64, version *viewpb.DataVersion) string {
	return fmt.Sprintf("datacoord-meta/pages/%d/views/%d/%d/", collectionID, version.GetStreamingVersion(), version.GetCompactVersion())
}

func (root pagedValueRoot) pageKey(i int) string {
	if root.Version != nil {
		return fmt.Sprintf("%s%s/%d", dataViewPagesPrefix(root.CollectionID, root.Version), root.Hash, i)
	}
	return fmt.Sprintf("datacoord-meta/pages/%d/%s/%d", root.CollectionID, root.Hash, i)
}

func (kc *Catalog) preparePagedValue(ctx context.Context, collectionID int64, version *viewpb.DataVersion, value []byte) (string, error) {
	if len(value) <= metadataPageBytes {
		return string(value), nil
	}
	sum := sha256.Sum256(value)
	hash := hex.EncodeToString(sum[:])
	count := (len(value) + metadataPageBytes - 1) / metadataPageBytes
	descriptor := pagedValueRoot{Hash: hash, Bytes: len(value), Pages: count, CollectionID: collectionID, Version: version}
	root, err := json.Marshal(descriptor)
	if err != nil {
		return "", err
	}
	if len(root)+len(pagedValueMagic) > metadataRootBytes {
		return "", merr.Wrapf(merr.ErrServiceResourceInsufficient, "metadata page root exceeds storage budget")
	}
	for i := 0; i < count; i++ {
		end := min(len(value), (i+1)*metadataPageBytes)
		if err := kc.MetaKv.Save(ctx, descriptor.pageKey(i), string(value[i*metadataPageBytes:end])); err != nil {
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
		page, err := kc.MetaKv.Load(ctx, root.pageKey(i))
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
func (kc *Catalog) prepareDataView(ctx context.Context, view *viewpb.DataViewOfCollection) (key, value string, err error) {
	encoded, err := proto.Marshal(view)
	if err != nil {
		return "", "", err
	}
	version := view.GetDataVersion()
	key = buildDataViewVersionKey(view.GetCollectionId(), version.GetStreamingVersion(), version.GetCompactVersion())
	if view.GetShardSetEpoch() < 2 {
		return key, string(encoded), nil
	}
	// A directory boundary makes a range deletion of compact version 1
	// disjoint from version 10. Page blobs stay outside the root scan.
	value, err = kc.preparePagedValue(ctx, view.GetCollectionId(), version, encoded)
	return key + "/root", value, err
}
