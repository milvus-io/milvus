package message

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// NewResourceKeyFromProto creates a ResourceKey from proto.
func NewResourceKeyFromProto(proto *messagespb.ResourceKey) ResourceKey {
	return ResourceKey{
		Domain: proto.Domain,
		Key:    proto.Key,
		Shared: proto.Shared,
	}
}

// newProtoFromResourceKey creates a set of proto from ResourceKey.
func newProtoFromResourceKey(keys ...ResourceKey) []*messagespb.ResourceKey {
	deduplicated := typeutil.NewSet(keys...)
	protos := make([]*messagespb.ResourceKey, 0, len(keys))
	for key := range deduplicated {
		protos = append(protos, &messagespb.ResourceKey{
			Domain: key.Domain,
			Key:    key.Key,
			Shared: key.Shared,
		})
	}
	return protos
}

type ResourceKey struct {
	Domain messagespb.ResourceDomain
	Key    string
	Shared bool
}

func (r ResourceKey) String() string {
	domain, _ := strings.CutPrefix(r.Domain.String(), "ResourceDomain")
	if r.Shared {
		return fmt.Sprintf("%s:%s@R", domain, r.Key)
	}
	return fmt.Sprintf("%s:%s@X", domain, r.Key)
}

// NewSharedClusterResourceKey creates a shared cluster resource key.
func NewSharedClusterResourceKey() ResourceKey {
	return ResourceKey{
		Domain: messagespb.ResourceDomain_ResourceDomainCluster,
		Key:    "",
		Shared: true,
	}
}

// NewExclusiveClusterResourceKey creates an exclusive cluster resource key.
func NewExclusiveClusterResourceKey() ResourceKey {
	return ResourceKey{
		Domain: messagespb.ResourceDomain_ResourceDomainCluster,
		Key:    "",
		Shared: false,
	}
}

// NewSharedCollectionNameResourceKey creates a shared collection name resource key.
func NewSharedCollectionNameResourceKey(dbName string, collectionName string) ResourceKey {
	return ResourceKey{
		Domain: messagespb.ResourceDomain_ResourceDomainCollectionName,
		Key:    fmt.Sprintf("%s:%s", dbName, collectionName),
		Shared: true,
	}
}

// NewExclusiveCollectionNameResourceKey creates an exclusive collection name resource key.
func NewExclusiveCollectionNameResourceKey(dbName string, collectionName string) ResourceKey {
	return ResourceKey{
		Domain: messagespb.ResourceDomain_ResourceDomainCollectionName,
		Key:    fmt.Sprintf("%s:%s", dbName, collectionName),
		Shared: false,
	}
}

// NewSharedDBNameResourceKey creates a shared db name resource key.
func NewSharedDBNameResourceKey(dbName string) ResourceKey {
	return ResourceKey{
		Domain: messagespb.ResourceDomain_ResourceDomainDBName,
		Key:    dbName,
		Shared: true,
	}
}

// NewExclusiveDBNameResourceKey creates an exclusive db name resource key.
func NewExclusiveDBNameResourceKey(dbName string) ResourceKey {
	return ResourceKey{
		Domain: messagespb.ResourceDomain_ResourceDomainDBName,
		Key:    dbName,
		Shared: false,
	}
}

// NewExclusivePrivilegeResourceKey creates an exclusive privilege resource key.
func NewExclusivePrivilegeResourceKey() ResourceKey {
	return ResourceKey{
		Domain: messagespb.ResourceDomain_ResourceDomainPrivilege,
		Key:    "",
		Shared: false,
	}
}

// Deprecated: NewImportJobIDResourceKey creates a key for import job resource.
func NewImportJobIDResourceKey(importJobID int64) ResourceKey {
	return ResourceKey{
		Domain: messagespb.ResourceDomain_ResourceDomainImportJobID,
		Key:    strconv.FormatInt(importJobID, 10),
	}
}

// Deprecated: NewCollectionNameResourceKey creates a key for collection name resource.
func NewCollectionNameResourceKey(collectionName string) ResourceKey {
	return ResourceKey{
		Domain: messagespb.ResourceDomain_ResourceDomainCollectionName,
		Key:    collectionName,
	}
}
