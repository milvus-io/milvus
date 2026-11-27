package storage

import "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"

// Azure's flat listing exposes HNS directories as zero-byte entries. The pinned
// azblob v1.1.0 omits ResourceType, so recursive walks also skip ordinary empty
// files, including during prefix deletion. Once the SDK exposes ResourceType,
// skip only entries marked "directory".
func azureZeroByteBlob(item *container.BlobItem) bool {
	return item != nil &&
		item.Properties != nil &&
		item.Properties.ContentLength != nil &&
		*item.Properties.ContentLength == 0
}
