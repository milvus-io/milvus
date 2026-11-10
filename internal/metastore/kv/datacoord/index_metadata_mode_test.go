// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datacoord

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type indexModeTestKV struct {
	kv.MetaKv
	value     string
	occupied  bool
	loadError error
}

func (k *indexModeTestKV) Load(context.Context, string) (string, error) {
	if k.loadError != nil {
		return "", k.loadError
	}
	if k.value == "" {
		return "", merr.ErrIoKeyNotFound
	}
	return k.value, nil
}
func (k *indexModeTestKV) HasPrefix(context.Context, string) (bool, error) { return k.occupied, nil }
func (k *indexModeTestKV) CompareVersionAndSwap(_ context.Context, _ string, _ int64, value string) (bool, error) {
	if k.value != "" {
		return false, nil
	}
	k.value = value
	return true, nil
}
func TestIndexMetadataModeIsImmutable(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		k := &indexModeTestKV{}
		c := &Catalog{MetaKv: k}
		require.NoError(t, c.EnsureIndexMetadataMode(t.Context(), enabled, false))
		require.NoError(t, c.EnsureIndexMetadataMode(t.Context(), enabled, true))
		require.ErrorIs(t, c.EnsureIndexMetadataMode(t.Context(), !enabled, false), merr.ErrServiceUnimplemented)
	}
}
func TestIndexMetadataModeRejectsExistingCluster(t *testing.T) {
	for _, collections := range []bool{false, true} {
		k := &indexModeTestKV{occupied: !collections}
		c := &Catalog{MetaKv: k}
		require.ErrorIs(t, c.EnsureIndexMetadataMode(t.Context(), true, collections), merr.ErrServiceUnimplemented)
		require.Empty(t, k.value)
	}
	k := &indexModeTestKV{loadError: merr.ErrServiceUnavailable}
	require.ErrorIs(t, (&Catalog{MetaKv: k}).EnsureIndexMetadataMode(t.Context(), true, false), merr.ErrServiceUnavailable)
	require.Empty(t, k.value)
}
