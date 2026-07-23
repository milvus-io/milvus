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

package storage

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"hash"
	"reflect"
	"slices"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const snapshotFingerprintVersion = "milvus-snapshot-fingerprint-v1"

// SnapshotFingerprint returns a stable content fingerprint for the snapshot
// state consumed by restore. Top-level sets are sorted before hashing so the
// value does not depend on manifest or in-memory iteration order.
func SnapshotFingerprint(snapshot *SnapshotData) (string, error) {
	if snapshot == nil {
		return "", merr.WrapErrDataIntegrityMsg("snapshot cannot be nil")
	}
	if snapshot.SnapshotInfo == nil {
		return "", merr.WrapErrDataIntegrityMsg("snapshot info cannot be nil")
	}
	if snapshot.Collection == nil {
		return "", merr.WrapErrDataIntegrityMsg("snapshot collection cannot be nil")
	}

	hasher := sha256.New()
	writeFingerprintBytes(hasher, []byte(snapshotFingerprintVersion))
	writeFingerprintUint64(hasher, uint64(snapshot.Layout))
	if err := writeFingerprintProto(hasher, snapshot.SnapshotInfo); err != nil {
		return "", merr.Wrap(err, "failed to fingerprint snapshot info")
	}
	if err := writeFingerprintProto(hasher, snapshot.Collection); err != nil {
		return "", merr.Wrap(err, "failed to fingerprint collection")
	}

	manifestPaths := append([]string(nil), snapshot.ManifestPaths...)
	for i := range manifestPaths {
		manifestPaths[i] = NormalizeSnapshotObjectPath(manifestPaths[i])
	}
	slices.Sort(manifestPaths)
	for _, manifestPath := range manifestPaths {
		writeFingerprintBytes(hasher, []byte(manifestPath))
	}

	segmentIDs := append([]int64(nil), snapshot.SegmentIDs...)
	slices.Sort(segmentIDs)
	for _, segmentID := range segmentIDs {
		writeFingerprintUint64(hasher, uint64(segmentID))
	}
	buildIDs := append([]int64(nil), snapshot.BuildIDs...)
	slices.Sort(buildIDs)
	for _, buildID := range buildIDs {
		writeFingerprintUint64(hasher, uint64(buildID))
	}

	if err := writeFingerprintProtoSet(hasher, snapshot.Segments); err != nil {
		return "", merr.Wrap(err, "failed to fingerprint snapshot segments")
	}
	if err := writeFingerprintProtoSet(hasher, snapshot.Indexes); err != nil {
		return "", merr.Wrap(err, "failed to fingerprint snapshot indexes")
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func writeFingerprintProtoSet[T proto.Message](hasher hash.Hash, messages []T) error {
	encoded := make([][]byte, 0, len(messages))
	for index, message := range messages {
		value := reflect.ValueOf(message)
		if !value.IsValid() || (value.Kind() == reflect.Ptr && value.IsNil()) {
			return merr.WrapErrDataIntegrityMsg("snapshot proto entry at index %d cannot be nil", index)
		}
		data, err := proto.MarshalOptions{Deterministic: true}.Marshal(message)
		if err != nil {
			return err
		}
		encoded = append(encoded, data)
	}
	slices.SortFunc(encoded, bytes.Compare)
	for _, data := range encoded {
		writeFingerprintBytes(hasher, data)
	}
	return nil
}

func writeFingerprintProto(hasher hash.Hash, message proto.Message) error {
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(message)
	if err != nil {
		return err
	}
	writeFingerprintBytes(hasher, data)
	return nil
}

func writeFingerprintBytes(hasher hash.Hash, data []byte) {
	writeFingerprintUint64(hasher, uint64(len(data)))
	_, _ = hasher.Write(data)
}

func writeFingerprintUint64(hasher hash.Hash, value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	_, _ = hasher.Write(encoded[:])
}
