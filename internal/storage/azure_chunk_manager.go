package storage

import (
	"context"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/retry"
	"go.uber.org/zap"
	"golang.org/x/exp/mmap"
	"golang.org/x/sync/errgroup"
	"io"
	"net/http"
	"strings"
	"time"
)

var _ ChunkManager = (*AzureChunkManager)(nil)

var (
	// default tenantId, clientId https://github.com/Azure/azure-sdk-for-go/blob/HEAD/sdk/azidentity/azidentity.go#L41
	defaultClientId = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
	defaultTenantId = "organizations"
)

type AzureChunkManager struct {
	cli                     *azblob.Client
	containerName, rootPath string
}

func newAzureChunkManager(cli *azblob.Client, containerName, rootPath string) *AzureChunkManager {
	return &AzureChunkManager{
		cli:           cli,
		rootPath:      rootPath,
		containerName: containerName,
	}
}

func newAzureChunkManagerWithConfig(ctx context.Context, c *config) (*AzureChunkManager, error) {
	var (
		azureClient *azblob.Client
		accountName string
		err         error
	)
	accountName = c.accessKeyID // azure storage account name

	serverUrl := fmt.Sprintf("https://%s.blob.core.windows.net/", accountName)
	if c.useIAM {
		// 1. userName and password ?
		//credential, err := azidentity.NewUsernamePasswordCredential(defaultTenantId, defaultClientId, c.accessKeyID, c.secretAccessKeyID, nil)
		//if err != nil {
		//	return nil, err
		//}
		// 2. user default?
		credential, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			return nil, err
		}

		azureClient, err = azblob.NewClient(serverUrl, credential, nil)
		if err != nil {
			return nil, err
		}
	} else {
		// accountName and sharded Key
		credential, err := azblob.NewSharedKeyCredential(accountName, c.secretAccessKeyID)
		if err != nil {
			return nil, err
		}
		azureClient, err = azblob.NewClientWithSharedKeyCredential(serverUrl, credential, nil)
	}

	// options nil or invalid formatted endpoint, don't need to retry
	if err != nil {
		return nil, err
	}
	var bucketExists bool
	// check valid in first query
	checkBucketFn := func() error {
		_, err = azureClient.ServiceClient().NewContainerClient(c.bucketName).GetProperties(ctx, nil)
		if err != nil {
			err, ok := err.(*azcore.ResponseError)
			if !ok || err.StatusCode != http.StatusNotFound {
				log.Warn("failed to check blob bucket exist", zap.String("bucket", c.bucketName), zap.Error(err))
				return err
			}
			bucketExists = true
		}
		if !bucketExists {
			if c.createBucket {
				log.Info("blob bucket not exist, create bucket.", zap.Any("bucket name", c.bucketName))
				//err := minIOClient.MakeBucket(ctx, c.bucketName, minio.MakeBucketOptions{})
				if err != nil {
					log.Warn("failed to create blob bucket", zap.String("bucket", c.bucketName), zap.Error(err))
					return err
				}
			} else {
				return fmt.Errorf("bucket %s not Existed", c.bucketName)
			}
		}
		return nil
	}
	err = retry.Do(ctx, checkBucketFn, retry.Attempts(CheckBucketRetryAttempts))
	if err != nil {
		return nil, err
	}

	//todo: handle rootpath
	rootPath := c.rootPath
	log.Info("azure chunk manager init success.", zap.String("bucketname", c.bucketName), zap.String("root", rootPath))

	return newAzureChunkManager(azureClient, c.bucketName, rootPath), nil
}

func (i *AzureChunkManager) Mmap(ctx context.Context, filePath string) (*mmap.ReaderAt, error) {
	return nil, errors.New("azure blob storage not support mmap")
}

func (i *AzureChunkManager) RootPath() string {
	return i.rootPath
}

func (i *AzureChunkManager) getProperties(ctx context.Context, blobName string) (*blob.GetPropertiesResponse, error) {
	client := i.cli.ServiceClient().NewContainerClient(i.containerName).NewBlobClient(blobName)
	resp, err := client.GetProperties(ctx, nil)
	if err != nil {
		if err, ok := err.(*azcore.ResponseError); ok {
			if err.StatusCode == http.StatusNotFound {
				return nil, errors.New("azure not found this blob")
			}
		}
		log.Warn("failed to stat object", zap.String("bucket", i.containerName), zap.String("path", blobName), zap.Error(err))
		return nil, err
	}
	return &resp, nil
}

func (i *AzureChunkManager) Path(ctx context.Context, filePath string) (string, error) {
	if _, err := i.getProperties(ctx, filePath); err != nil {
		return "", err
	}
	return filePath, nil
}

func (i *AzureChunkManager) Size(ctx context.Context, blobName string) (int64, error) {
	resp, err := i.getProperties(ctx, blobName)
	if err != nil {
		return 0, err
	}
	if resp.ContentLength != nil {
		return *resp.ContentLength, nil
	}
	return 0, nil
}

func (i *AzureChunkManager) Write(ctx context.Context, filePath string, content []byte) error {
	_, err := i.cli.UploadBuffer(ctx, i.containerName, filePath, content, nil)
	if err != nil {
		log.Warn("failed to put object", zap.String("bucket", i.containerName), zap.String("path", filePath), zap.Error(err))
		return err
	}
	return nil
}

func (i *AzureChunkManager) MultiWrite(ctx context.Context, kvs map[string][]byte) error {
	var el error
	for key, value := range kvs {
		err := i.Write(ctx, key, value)
		if err != nil {
			el = err
			el = merr.Combine(el, errors.Wrapf(err, "failed to write %s", key))
		}
	}
	return el
}

func (i *AzureChunkManager) Exist(ctx context.Context, blobName string) (bool, error) {
	if _, err := i.getProperties(ctx, blobName); err != nil {
		return false, err
	}
	return true, nil
}

func (i *AzureChunkManager) Read(ctx context.Context, filePath string) ([]byte, error) {
	resp, err := i.cli.DownloadStream(ctx, i.containerName, filePath, nil)
	if err != nil {
		return nil, err
	}
	var ans []byte
	if resp.ContentLength != nil {
		ans = make([]byte, *resp.ContentLength)
	}
	defer resp.Body.Close()
	n, err := resp.Body.Read(ans)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	return ans[:n], nil
}

func (i *AzureChunkManager) Reader(ctx context.Context, filePath string) (FileReader, error) {
	resp, err := i.cli.DownloadStream(ctx, i.containerName, filePath, nil)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

func (i *AzureChunkManager) MultiRead(ctx context.Context, keys []string) ([][]byte, error) {
	var el error
	var objectsValues [][]byte
	for _, key := range keys {
		objectValue, err := i.Read(ctx, key)
		if err != nil {
			el = err
			el = merr.Combine(el, errors.Wrapf(err, "failed to read %s", key))
		}
		objectsValues = append(objectsValues, objectValue)
	}

	return objectsValues, el
}

func (i *AzureChunkManager) ListWithPrefix(ctx context.Context, prefix string, recursive bool) ([]string, []time.Time, error) {
	pager := i.cli.NewListBlobsFlatPager(i.containerName, &azblob.ListBlobsFlatOptions{ // should add root path? ex. prefix = rootPath + prefix
		Prefix: &prefix, // this sdk don't have delimiter option
	})

	var blobNames []string
	var blobTimes []time.Time

	for pager.More() {
		resp, err := pager.NextPage(ctx)
		if err != nil {
			return nil, nil, err
		}
		for _, v := range resp.Segment.BlobItems {
			name := *v.Name
			if !recursive && strings.Contains(strings.Trim(name, prefix), "/") { // folder
				continue
			}
			blobNames = append(blobNames, name)
			blobTimes = append(blobTimes, *v.Properties.LastModified)
		}
	}
	return blobNames, blobTimes, nil
}

func (i *AzureChunkManager) ReadWithPrefix(ctx context.Context, prefix string) ([]string, [][]byte, error) {
	blobNames, _, err := i.ListWithPrefix(ctx, prefix, true)
	if err != nil {
		return nil, nil, err
	}
	blobValues, err := i.MultiRead(ctx, blobNames)
	if err != nil {
		return nil, nil, err
	}
	return blobNames, blobValues, nil
}

func (i *AzureChunkManager) ReadAt(ctx context.Context, filePath string, off int64, length int64) (p []byte, err error) {
	if off < 0 || length < 0 {
		return nil, io.EOF
	}
	// off = 0
	size, err := i.Size(ctx, filePath)
	if err != nil {
		return nil, err
	}
	if length == blob.CountToEnd { // length == 0, mean get all
		length = size - off
	}

	if size <= off+length { // exceed ex. [size = 10, off = 5, length = 10] -> length = 5 (5~9)
		length = size - off
	}

	p = make([]byte, length)
	_, err = i.cli.DownloadBuffer(ctx, i.containerName, filePath, p, &azblob.DownloadBufferOptions{
		Range: azblob.HTTPRange{
			Offset: off,
			Count:  length,
		},
	})
	return p, err
}

func (i *AzureChunkManager) Remove(ctx context.Context, filePath string) error {
	if _, err := i.cli.DeleteBlob(ctx, i.containerName, filePath, nil); err != nil {
		return err
	}
	return nil
}

func (i *AzureChunkManager) MultiRemove(ctx context.Context, keys []string) error {
	var el error
	for _, key := range keys {
		err := i.Remove(ctx, key)
		if err != nil {
			el = err
			el = merr.Combine(el, errors.Wrapf(err, "failed to remove %s", key))
		}
	}
	return el
}

func (i *AzureChunkManager) RemoveWithPrefix(ctx context.Context, prefix string) error {
	blobNames, _, err := i.ListWithPrefix(ctx, prefix, true)
	if err != nil {
		return err
	}

	maxGoroutine := 10
	for idx := 0; idx < len(blobNames); {
		runningGroup, groupCtx := errgroup.WithContext(ctx)
		for j := 0; j < maxGoroutine && idx < len(blobNames); j++ {
			key := blobNames[idx]
			runningGroup.Go(func() error {
				err := i.Remove(groupCtx, key)
				if err != nil {
					log.Warn("failed to remove object", zap.String("path", key), zap.Error(err))
					return err
				}
				return nil
			})
			idx++
		}
		if err := runningGroup.Wait(); err != nil {
			return err
		}
	}
	return nil
}
