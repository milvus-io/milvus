// Copyright 2023 Zilliz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package manifest

import (
	"github.com/milvus-io/milvus/internal/storagev2/common/errors"
	"github.com/milvus-io/milvus/internal/storagev2/file/blob"
	"github.com/milvus-io/milvus/internal/storagev2/file/fragment"
)

type ManifestCommitOp interface {
	commit(manifest *Manifest) error
}

type AddScalarFragmentOp struct {
	ScalarFragment fragment.Fragment
}

func (op AddScalarFragmentOp) commit(manifest *Manifest) error {
	op.ScalarFragment.SetFragmentId(manifest.Version())
	manifest.AddScalarFragment(op.ScalarFragment)
	return nil
}

type AddVectorFragmentOp struct {
	VectorFragment fragment.Fragment
}

func (op AddVectorFragmentOp) commit(manifest *Manifest) error {
	op.VectorFragment.SetFragmentId(manifest.Version())
	manifest.AddVectorFragment(op.VectorFragment)
	return nil
}

type AddDeleteFragmentOp struct {
	DeleteFragment fragment.Fragment
}

func (op AddDeleteFragmentOp) commit(manifest *Manifest) error {
	op.DeleteFragment.SetFragmentId(manifest.Version())
	manifest.AddDeleteFragment(op.DeleteFragment)
	return nil
}

type AddBlobOp struct {
	Replace bool
	Blob    blob.Blob
}

func (op AddBlobOp) commit(manifest *Manifest) error {
	if !op.Replace && manifest.HasBlob(op.Blob.Name) {
		return errors.ErrBlobAlreadyExist
	}
	manifest.AddBlob(op.Blob)
	return nil
}
