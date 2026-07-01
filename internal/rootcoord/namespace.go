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

package rootcoord

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	defaultNamespacePageSize = 100
	maxNamespacePageSize     = 1000
)

func (c *Core) CreateNamespace(ctx context.Context, in *milvuspb.CreateNamespaceRequest) (*rootcoordpb.CreateNamespaceResponse, error) {
	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return &rootcoordpb.CreateNamespaceResponse{Status: merr.Status(err)}, nil
	}
	if err := validateNamespaceName(in.GetNamespaceName()); err != nil {
		return &rootcoordpb.CreateNamespaceResponse{Status: merr.Status(err)}, nil
	}
	if _, err := c.getNamespaceCollection(ctx, in.GetDbName(), in.GetCollectionName()); err != nil {
		return &rootcoordpb.CreateNamespaceResponse{Status: merr.Status(err)}, nil
	}

	partitionID, err := c.broadcastCreatePartition(ctx, &milvuspb.CreatePartitionRequest{
		Base:           in.GetBase(),
		DbName:         in.GetDbName(),
		CollectionName: in.GetCollectionName(),
		PartitionName:  in.GetNamespaceName(),
	})
	if err != nil {
		if errors.Is(err, errIgnoerdCreatePartition) {
			err = merr.WrapErrNamespaceAlreadyExists(in.GetNamespaceName())
		}
		return &rootcoordpb.CreateNamespaceResponse{Status: merr.Status(err)}, nil
	}

	info, err := c.getNamespaceInfo(ctx, in.GetDbName(), in.GetCollectionName(), in.GetNamespaceName())
	if err != nil {
		return &rootcoordpb.CreateNamespaceResponse{Status: merr.Status(err), PartitionID: partitionID}, nil
	}
	return &rootcoordpb.CreateNamespaceResponse{
		Status:      merr.Success(),
		Namespace:   info,
		PartitionID: partitionID,
	}, nil
}

func (c *Core) DescribeNamespace(ctx context.Context, in *milvuspb.DescribeNamespaceRequest) (*milvuspb.DescribeNamespaceResponse, error) {
	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return &milvuspb.DescribeNamespaceResponse{Status: merr.Status(err)}, nil
	}
	if err := validateNamespaceName(in.GetNamespaceName()); err != nil {
		return &milvuspb.DescribeNamespaceResponse{Status: merr.Status(err)}, nil
	}
	info, err := c.getNamespaceInfo(ctx, in.GetDbName(), in.GetCollectionName(), in.GetNamespaceName())
	if err != nil {
		return &milvuspb.DescribeNamespaceResponse{Status: merr.Status(err)}, nil
	}
	return &milvuspb.DescribeNamespaceResponse{
		Status:    merr.Success(),
		Namespace: info,
	}, nil
}

func (c *Core) ListNamespaces(ctx context.Context, in *milvuspb.ListNamespacesRequest) (*milvuspb.ListNamespacesResponse, error) {
	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return &milvuspb.ListNamespacesResponse{Status: merr.Status(err)}, nil
	}
	coll, err := c.getNamespaceCollection(ctx, in.GetDbName(), in.GetCollectionName())
	if err != nil {
		return &milvuspb.ListNamespacesResponse{Status: merr.Status(err)}, nil
	}

	namespaces := make([]*milvuspb.NamespaceInfo, 0, len(coll.Partitions))
	for _, partition := range coll.Partitions {
		if !isActiveNamespacePartition(partition) {
			continue
		}
		if in.GetPrefix() != "" && !strings.HasPrefix(partition.PartitionName, in.GetPrefix()) {
			continue
		}
		namespaces = append(namespaces, namespaceInfoFromPartition(coll.Name, partition))
	}
	sort.Slice(namespaces, func(i, j int) bool {
		return namespaces[i].GetNamespaceName() < namespaces[j].GetNamespaceName()
	})

	start, err := namespacePageStart(in.GetPageToken(), len(namespaces))
	if err != nil {
		return &milvuspb.ListNamespacesResponse{Status: merr.Status(err)}, nil
	}
	pageSize, err := namespacePageSize(in.GetPageSize())
	if err != nil {
		return &milvuspb.ListNamespacesResponse{Status: merr.Status(err)}, nil
	}

	end := start + pageSize
	if end > len(namespaces) {
		end = len(namespaces)
	}
	nextPageToken := ""
	if end < len(namespaces) {
		nextPageToken = strconv.Itoa(end)
	}

	return &milvuspb.ListNamespacesResponse{
		Status:        merr.Success(),
		Namespaces:    namespaces[start:end],
		NextPageToken: nextPageToken,
	}, nil
}

func (c *Core) DropNamespace(ctx context.Context, in *milvuspb.DropNamespaceRequest) (*milvuspb.DropNamespaceResponse, error) {
	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return &milvuspb.DropNamespaceResponse{Status: merr.Status(err)}, nil
	}
	if err := validateNamespaceName(in.GetNamespaceName()); err != nil {
		return &milvuspb.DropNamespaceResponse{Status: merr.Status(err)}, nil
	}

	info, err := c.getNamespaceInfo(ctx, in.GetDbName(), in.GetCollectionName(), in.GetNamespaceName())
	if err != nil {
		return &milvuspb.DropNamespaceResponse{Status: merr.Status(err)}, nil
	}
	err = c.broadcastDropPartition(ctx, &milvuspb.DropPartitionRequest{
		Base:           in.GetBase(),
		DbName:         in.GetDbName(),
		CollectionName: in.GetCollectionName(),
		PartitionName:  in.GetNamespaceName(),
	})
	if err != nil {
		if errors.Is(err, errIgnoredDropPartition) {
			err = merr.WrapErrNamespaceNotFound(in.GetNamespaceName())
		}
		return &milvuspb.DropNamespaceResponse{Status: merr.Status(err)}, nil
	}

	return &milvuspb.DropNamespaceResponse{
		Status:    merr.Success(),
		Namespace: info,
	}, nil
}

func (c *Core) HasNamespace(ctx context.Context, in *milvuspb.HasNamespaceRequest) (*milvuspb.HasNamespaceResponse, error) {
	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return &milvuspb.HasNamespaceResponse{Status: merr.Status(err)}, nil
	}
	if err := validateNamespaceName(in.GetNamespaceName()); err != nil {
		return &milvuspb.HasNamespaceResponse{Status: merr.Status(err)}, nil
	}
	coll, err := c.getNamespaceCollection(ctx, in.GetDbName(), in.GetCollectionName())
	if err != nil {
		return &milvuspb.HasNamespaceResponse{Status: merr.Status(err)}, nil
	}

	for _, partition := range coll.Partitions {
		if isActiveNamespacePartition(partition) && partition.PartitionName == in.GetNamespaceName() {
			return &milvuspb.HasNamespaceResponse{Status: merr.Success(), Value: true}, nil
		}
	}
	return &milvuspb.HasNamespaceResponse{Status: merr.Success(), Value: false}, nil
}

func (c *Core) getNamespaceInfo(ctx context.Context, dbName, collectionName, namespaceName string) (*milvuspb.NamespaceInfo, error) {
	coll, err := c.getNamespaceCollection(ctx, dbName, collectionName)
	if err != nil {
		return nil, err
	}
	for _, partition := range coll.Partitions {
		if isActiveNamespacePartition(partition) && partition.PartitionName == namespaceName {
			return namespaceInfoFromPartition(coll.Name, partition), nil
		}
	}
	return nil, merr.WrapErrNamespaceNotFound(namespaceName)
}

func (c *Core) getNamespaceCollection(ctx context.Context, dbName, collectionName string) (*model.Collection, error) {
	coll, err := c.meta.GetCollectionByName(ctx, dbName, collectionName, typeutil.MaxTimestamp, false)
	if err != nil {
		return nil, err
	}
	if !coll.EnableNamespace || !common.IsNamespaceModePartition(coll.Properties...) {
		return nil, merr.WrapErrParameterInvalidMsg("namespace APIs require collection with namespace enabled and namespace.mode=partition")
	}
	return coll, nil
}

func namespaceInfoFromPartition(collectionName string, partition *model.Partition) *milvuspb.NamespaceInfo {
	physical, _ := tsoutil.ParseHybridTs(partition.PartitionCreatedTimestamp)
	return &milvuspb.NamespaceInfo{
		CollectionName:      collectionName,
		NamespaceName:       partition.PartitionName,
		CreatedTimestamp:    partition.PartitionCreatedTimestamp,
		CreatedUtcTimestamp: uint64(physical),
		State:               "Ready",
	}
}

func isActiveNamespacePartition(partition *model.Partition) bool {
	return partition.Available() && partition.PartitionName != Params.CommonCfg.DefaultPartitionName.GetValue()
}

func namespacePageStart(pageToken string, total int) (int, error) {
	if pageToken == "" {
		return 0, nil
	}
	offset, err := strconv.ParseInt(pageToken, 10, 64)
	if err != nil || offset < 0 {
		return 0, merr.WrapErrParameterInvalidMsg("invalid namespace page token: %q", pageToken)
	}
	if offset > int64(total) {
		return total, nil
	}
	return int(offset), nil
}

func namespacePageSize(pageSize int64) (int, error) {
	if pageSize < 0 {
		return 0, merr.WrapErrParameterInvalidMsg("namespace page size must be non-negative")
	}
	if pageSize == 0 {
		return defaultNamespacePageSize, nil
	}
	if pageSize > maxNamespacePageSize {
		return maxNamespacePageSize, nil
	}
	return int(pageSize), nil
}

func validateNamespaceName(namespaceName string) error {
	return common.ValidatePartitionName(namespaceName, Params.ProxyCfg.MaxNameLength.GetAsInt(), Params.ProxyCfg.MaxNameLength.GetValue(), true)
}
