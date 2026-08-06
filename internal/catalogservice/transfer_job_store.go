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

package catalogservice

import (
	"context"
	"encoding/json"
	"path"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type kvTransferJobStore struct {
	kv     kv.BaseKV
	prefix string
}

func NewKVTransferJobStore(kv kv.BaseKV, prefix string) TransferJobStore {
	return &kvTransferJobStore{
		kv:     kv,
		prefix: prefix,
	}
}

func (s *kvTransferJobStore) Get(ctx context.Context, transferID string) (*TransferJob, error) {
	if err := ValidateCatalogPathSegment("transfer id", transferID); err != nil {
		return nil, err
	}
	value, err := s.kv.Load(ctx, s.key(transferID))
	if err != nil {
		if errors.Is(err, merr.ErrIoKeyNotFound) {
			return nil, nil
		}
		return nil, err
	}
	job := &TransferJob{}
	if err := json.Unmarshal([]byte(value), job); err != nil {
		return nil, err
	}
	job.storeValue = value
	return job, nil
}

func (s *kvTransferJobStore) Save(ctx context.Context, job *TransferJob) error {
	if job == nil {
		return merr.WrapErrParameterInvalidMsg("transfer job is required")
	}
	if err := ValidateCatalogPathSegment("transfer id", job.TransferID); err != nil {
		return err
	}
	if job.Version == 0 {
		job.Version = 1
	}
	value, err := json.Marshal(job)
	if err != nil {
		return err
	}
	job.storeValue = string(value)
	return s.kv.Save(ctx, s.key(job.TransferID), string(value))
}

func (s *kvTransferJobStore) CompareAndSave(ctx context.Context, expected *TransferJob, job *TransferJob) error {
	if job == nil {
		return merr.WrapErrParameterInvalidMsg("transfer job is required")
	}
	if err := ValidateCatalogPathSegment("transfer id", job.TransferID); err != nil {
		return err
	}
	if expected == nil {
		job.Version = 1
	} else {
		job.Version = expected.Version + 1
	}
	value, err := json.Marshal(job)
	if err != nil {
		return err
	}
	key := s.key(job.TransferID)
	txn, ok := s.kv.(kv.TxnKV)
	if !ok {
		return errors.Wrap(merr.ErrServiceUnimplemented, "transfer job store requires compare-and-save support")
	}
	if expected == nil {
		if err := txn.MultiSaveAndRemove(ctx, map[string]string{key: string(value)}, nil, predicates.KeyNotExists(key)); err != nil {
			return err
		}
		job.storeValue = string(value)
		return nil
	}
	if expected.storeValue == "" {
		return merr.WrapErrServiceInternalMsg("transfer job expected value is empty")
	}
	if err := txn.MultiSaveAndRemove(ctx, map[string]string{key: string(value)}, nil, predicates.ValueEqual(key, expected.storeValue)); err != nil {
		return err
	}
	job.storeValue = string(value)
	return nil
}

func (s *kvTransferJobStore) key(transferID string) string {
	return path.Join(s.prefix, transferID)
}
