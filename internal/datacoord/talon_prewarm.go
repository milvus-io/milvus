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
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type talonPrewarmAdapter struct{ *packed.TalonClient }

func (c *talonPrewarmAdapter) BatchLoad(ctx context.Context, requests []TalonPrewarmRequest) error {
	native := make([]packed.TalonLoadRequest, len(requests))
	for i, request := range requests {
		native[i] = packed.TalonLoadRequest{URI: request.URI, Size: request.Size, Version: request.Version}
	}
	_, err := c.TalonClient.BatchLoad(ctx, native)
	return err
}

func (s *Server) initTalonPrewarm() error {
	inspector, err := newTalonPrewarm(s.ctx, Params, s.GetPublishedManifestPaths, func(config *indexpb.StorageConfig, concurrency int) (TalonPrewarmClient, error) {
		client, err := packed.NewTalonClient(config, concurrency)
		if err != nil {
			return nil, err
		}
		return &talonPrewarmAdapter{client}, nil
	})
	if err != nil {
		return err
	}
	s.talonPrewarmInspector = inspector
	return nil
}

func newTalonPrewarm(ctx context.Context, p *paramtable.ComponentParam, supplier func(context.Context) ([]string, error), factory func(*indexpb.StorageConfig, int) (TalonPrewarmClient, error)) (*TalonPrewarmInspector, error) {
	if !p.DataCoordCfg.TalonPrewarmEnabled.GetAsBool() || p.CommonCfg.StorageType.GetValue() == "local" {
		return nil, nil
	}
	interval, err := strconv.ParseInt(p.DataCoordCfg.TalonPrewarmInterval.GetValue(), 10, 64)
	if err != nil || interval < 1 || interval > 86400 {
		return nil, merr.WrapErrServiceInternalMsg("dataCoord.talonPrewarm.interval must be in [1, 86400] seconds")
	}
	batchSize, err := strconv.ParseInt(p.DataCoordCfg.TalonPrewarmBatchSize.GetValue(), 10, 32)
	if err != nil || batchSize < 1 || batchSize > 4096 {
		return nil, merr.WrapErrServiceInternalMsg("dataCoord.talonPrewarm.batchSize must be in [1, 4096]")
	}
	if supplier == nil {
		return nil, merr.WrapErrServiceInternalMsg("Talon prewarm requires a DataCoord manifest supplier")
	}
	coordinator := p.CommonCfg.StorageTalonCoordinator.GetValue()
	if strings.TrimSpace(coordinator) == "" {
		return nil, merr.WrapErrServiceInternalMsg("Talon prewarm requires common.storage.talon.coordinator")
	}
	resolve, err := manifestPrewarmResolver(p.MinioCfg.CloudProvider.GetValue(), p.MinioCfg.BucketName.GetValue())
	if err != nil {
		return nil, err
	}
	client, err := factory(&indexpb.StorageConfig{
		BucketName:          p.MinioCfg.BucketName.GetValue(),
		CloudProvider:       p.MinioCfg.CloudProvider.GetValue(),
		TalonCoordinator:    coordinator,
		TalonBlockSize:      p.CommonCfg.StorageTalonBlockSize.GetAsUint32(),
		TalonMaxIdlePerAddr: p.CommonCfg.StorageTalonMaxIdlePerAddr.GetAsUint32(),
	}, 1)
	if err != nil {
		return nil, merr.Wrap(err, "initialize Talon prewarm")
	}
	return NewTalonPrewarmInspector(ctx, supplier, client, resolve, time.Duration(interval)*time.Second, int(batchSize)), nil
}

// manifestPrewarmResolver returns the Talon URI of a descriptor's manifest
// object and the object size the descriptor records, 0 when it records none.
func manifestPrewarmResolver(provider, bucket string) (func(string) (string, uint64, error), error) {
	scheme := ""
	switch provider {
	case objectstorage.CloudProviderAWS, objectstorage.CloudProviderAliyun,
		objectstorage.CloudProviderTencent, objectstorage.CloudProviderHuawei,
		objectstorage.CloudProviderGCP:
		scheme = packed.TalonSchemeS3
	case objectstorage.CloudProviderAzure:
		scheme = packed.TalonSchemeAzure
	default:
		return nil, merr.WrapErrServiceInternalMsg("Talon prewarm does not support cloud provider %q", provider)
	}
	bucket = strings.TrimRight(bucket, "/")
	if bucket == "" {
		return nil, merr.WrapErrServiceInternalMsg("Talon prewarm requires a bucket")
	}
	prefix := (&url.URL{Scheme: scheme, Host: bucket, Path: "/"}).String()
	return func(descriptor string) (string, uint64, error) {
		manifest, err := packed.ParseManifestPath(descriptor)
		// Descriptors are internal persisted metadata; malformed JSON is not user
		// input. ParseManifestPath's validation error must be classified here.
		if err != nil {
			return "", 0, merr.WrapErrDataIntegrity(err, "invalid manifest prewarm descriptor")
		}
		// Growing V3 segments start at version 0 before publishing their first file.
		if manifest.ManifestVersion <= 0 {
			return "", 0, nil
		}
		key, err := packed.ManifestFilePath(descriptor)
		if err != nil {
			return "", 0, merr.Wrap(err, "resolve manifest prewarm object key")
		}
		// Talon treats the key as literal bytes; URL escaping or path cleaning
		// would change the object identity used by the normal storage reader.
		return prefix + key, uint64(max(manifest.Size, 0)), nil
	}, nil
}
