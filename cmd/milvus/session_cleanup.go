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

package milvus

import (
	"context"
	"encoding/json"
	"path"
	"strconv"
	"strings"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// sessionCleanupTarget keeps the revision from the same read that verified ownership.
// A later read must never replace it: the key may already belong to a successor.
type sessionCleanupTarget struct {
	key         string
	modRevision int64
}

func collectSessionCleanupTargets(ctx context.Context, kv clientv3.KV, prefix string, suffixes []string) ([]sessionCleanupTarget, error) {
	targets := make([]sessionCleanupTarget, 0, len(suffixes))
	seen := make(map[string]struct{})
	add := func(suffix string, serverID int64) error {
		key := path.Join(prefix, suffix)
		if _, ok := seen[key]; ok {
			return nil
		}
		target, found, err := readSessionCleanupTarget(ctx, kv, key, serverID)
		if err != nil {
			return err
		}
		if found {
			seen[key] = struct{}{}
			targets = append(targets, target)
		}
		return nil
	}

	for _, suffix := range suffixes {
		role, id, ok := strings.Cut(suffix, "-")
		if !ok || role == "" {
			continue
		}
		serverID, err := strconv.ParseInt(id, 10, 64)
		if err != nil {
			continue
		}
		if err := add(suffix, serverID); err != nil {
			return nil, err
		}
		switch role {
		case typeutil.MixCoordRole, typeutil.QueryCoordRole, typeutil.DataCoordRole:
			if err := add(role, serverID); err != nil {
				return nil, err
			}
		}
		if role == typeutil.MixCoordRole {
			// MixCoord retains the legacy keys to protect mixed-version failover.
			// Check each independently even if the active MixCoord key is gone
			// or belongs to a successor.
			for _, legacyRole := range []string{typeutil.RootCoordRole, typeutil.DataCoordRole, typeutil.QueryCoordRole} {
				if err := add(legacyRole, serverID); err != nil {
					return nil, err
				}
			}
		}
	}
	return targets, nil
}

func readSessionCleanupTarget(ctx context.Context, kv clientv3.KV, key string, serverID int64) (sessionCleanupTarget, bool, error) {
	resp, err := kv.Get(ctx, key)
	if err != nil {
		return sessionCleanupTarget{}, false, err
	}
	if len(resp.Kvs) == 0 {
		return sessionCleanupTarget{}, false, nil
	}
	// Only the owner is needed here. A pointer distinguishes a missing ServerID
	// (including a JSON null session) from a registered server whose ID is zero.
	var session struct {
		ServerID *int64
	}
	if err := json.Unmarshal(resp.Kvs[0].Value, &session); err != nil || session.ServerID == nil || *session.ServerID != serverID {
		return sessionCleanupTarget{}, false, nil
	}
	return sessionCleanupTarget{key: key, modRevision: resp.Kvs[0].ModRevision}, true, nil
}

func deleteSessionCleanupTargets(ctx context.Context, kv clientv3.KV, targets []sessionCleanupTarget) error {
	for _, target := range targets {
		_, err := kv.Txn(ctx).
			If(clientv3.Compare(clientv3.ModRevision(target.key), "=", target.modRevision)).
			Then(clientv3.OpDelete(target.key)).
			Commit()
		if err != nil {
			return err
		}
		// A failed comparison means the key changed after ownership was checked.
		// Leave it alone; retrying with a fresh revision could delete a successor.
	}
	return nil
}
