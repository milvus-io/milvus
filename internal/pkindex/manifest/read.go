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

package manifest

import (
	"context"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	pkroot "github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/pkg/v3/proto/pkindexpb"
)

// ObjectReader reads a whole object from object storage. A missing object
// returns an error marked pkroot.ErrCorrupted, any other failure one marked
// pkroot.ErrUnavailable. internal/pkindex/objstore.Store implements it.
type ObjectReader interface {
	ReadObject(ctx context.Context, key string) ([]byte, error)
}

// ReadBody reads the manifest body at bodyKey, the BodyKey of a
// RecoveryManifest. A body that does not decode returns an error marked
// pkroot.ErrCorrupted, and so does a body that does not exist.
func ReadBody(ctx context.Context, r ObjectReader, bodyKey string) (View, error) {
	body, err := r.ReadObject(ctx, bodyKey)
	if err != nil {
		return View{}, errors.Wrapf(err, "read manifest body %s", bodyKey)
	}
	var m pkindexpb.Manifest
	if err := proto.Unmarshal(body, &m); err != nil {
		return View{}, errors.Mark(errors.Wrapf(err, "decode manifest body %s", bodyKey), pkroot.ErrCorrupted)
	}
	return ViewOf(&m), nil
}
