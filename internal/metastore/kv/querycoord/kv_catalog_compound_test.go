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

package querycoord

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

func TestCatalog_SaveAndReleaseReplicas(t *testing.T) {
	saves := []*querypb.Replica{{ID: 100, CollectionID: 1}, {ID: 101, CollectionID: 1}}
	releases := []int64{7, 8}

	mockey.PatchConvey("success saves replicas before releasing", t, func() {
		kc := Catalog{}
		var calls []string
		mockey.Mock(Catalog.SaveReplica).To(func(_ Catalog, _ context.Context, replicas ...*querypb.Replica) error {
			assert.Equal(t, saves, replicas)
			calls = append(calls, "SaveReplica")
			return nil
		}).Build()
		mockey.Mock(Catalog.ReleaseReplica).To(func(_ Catalog, _ context.Context, collection int64, replicas ...int64) error {
			assert.Equal(t, int64(1), collection)
			assert.Equal(t, releases, replicas)
			calls = append(calls, "ReleaseReplica")
			return nil
		}).Build()

		err := kc.SaveAndReleaseReplicas(context.TODO(), 1, saves, releases)
		assert.NoError(t, err)
		assert.Equal(t, []string{"SaveReplica", "ReleaseReplica"}, calls)
	})

	mockey.PatchConvey("save failure aborts before release", t, func() {
		kc := Catalog{}
		var calls []string
		mockey.Mock(Catalog.SaveReplica).To(func(_ Catalog, _ context.Context, _ ...*querypb.Replica) error {
			calls = append(calls, "SaveReplica")
			return errors.New("save failed")
		}).Build()
		mockey.Mock(Catalog.ReleaseReplica).To(func(_ Catalog, _ context.Context, _ int64, _ ...int64) error {
			calls = append(calls, "ReleaseReplica")
			return nil
		}).Build()

		err := kc.SaveAndReleaseReplicas(context.TODO(), 1, saves, releases)
		assert.Error(t, err)
		assert.Equal(t, []string{"SaveReplica"}, calls)
	})

	mockey.PatchConvey("release failure is returned", t, func() {
		kc := Catalog{}
		mockey.Mock(Catalog.SaveReplica).To(func(_ Catalog, _ context.Context, _ ...*querypb.Replica) error {
			return nil
		}).Build()
		mockey.Mock(Catalog.ReleaseReplica).To(func(_ Catalog, _ context.Context, _ int64, _ ...int64) error {
			return errors.New("release failed")
		}).Build()

		err := kc.SaveAndReleaseReplicas(context.TODO(), 1, saves, releases)
		assert.Error(t, err)
	})

	mockey.PatchConvey("empty saves and releases are skipped", t, func() {
		kc := Catalog{}
		var calls []string
		mockey.Mock(Catalog.SaveReplica).To(func(_ Catalog, _ context.Context, _ ...*querypb.Replica) error {
			calls = append(calls, "SaveReplica")
			return nil
		}).Build()
		mockey.Mock(Catalog.ReleaseReplica).To(func(_ Catalog, _ context.Context, _ int64, _ ...int64) error {
			calls = append(calls, "ReleaseReplica")
			return nil
		}).Build()

		err := kc.SaveAndReleaseReplicas(context.TODO(), 1, nil, nil)
		assert.NoError(t, err)
		assert.Empty(t, calls)

		err = kc.SaveAndReleaseReplicas(context.TODO(), 1, saves, nil)
		assert.NoError(t, err)
		assert.Equal(t, []string{"SaveReplica"}, calls)
	})
}
