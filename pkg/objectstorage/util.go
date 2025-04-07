package objectstorage

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/cockroachdb/errors"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.uber.org/zap"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/objectstorage/aliyun"
	"github.com/milvus-io/milvus/pkg/v2/objectstorage/gcp"
	"github.com/milvus-io/milvus/pkg/v2/objectstorage/tencent"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/retry"
)

const (
	CloudProviderGCP       = "gcp"
	CloudProviderGCPNative = "gcpnative"
	CloudProviderAWS       = "aws"
	CloudProviderAliyun    = "aliyun"
	CloudProviderAzure     = "azure"
	CloudProviderTencent   = "tencent"
)

var CheckBucketRetryAttempts uint = 20

func NewMinioClient(ctx context.Context, c *Config) (*minio.Client, error) {
	var creds *credentials.Credentials
	newMinioFn := minio.New
	bucketLookupType := minio.BucketLookupAuto

	if c.UseVirtualHost {
		bucketLookupType = minio.BucketLookupDNS
	}

	matchedDefault := false
	switch c.CloudProvider {
	case CloudProviderAliyun:
		// auto doesn't work for aliyun, so we set to dns deliberately
		bucketLookupType = minio.BucketLookupDNS
		if c.UseIAM {
			newMinioFn = aliyun.NewMinioClient
		} else {
			creds = credentials.NewStaticV4(c.AccessKeyID, c.SecretAccessKeyID, "")
		}
	case CloudProviderGCP:
		newMinioFn = gcp.NewMinioClient
		if !c.UseIAM {
			creds = credentials.NewStaticV2(c.AccessKeyID, c.SecretAccessKeyID, "")
		}
	case CloudProviderTencent:
		bucketLookupType = minio.BucketLookupDNS
		newMinioFn = tencent.NewMinioClient
		if !c.UseIAM {
			creds = credentials.NewStaticV4(c.AccessKeyID, c.SecretAccessKeyID, "")
		}

	default: // aws, minio
		matchedDefault = true
	}

	// Compatibility logic. If the cloud provider is not specified in the request,
	// it shall be inferred based on the service address.
	if matchedDefault {
		matchedDefault = false
		switch {
		case strings.Contains(c.Address, gcp.GcsDefaultAddress):
			newMinioFn = gcp.NewMinioClient
			if !c.UseIAM {
				creds = credentials.NewStaticV2(c.AccessKeyID, c.SecretAccessKeyID, "")
			}
		case strings.Contains(c.Address, aliyun.OSSAddressFeatureString):
			// auto doesn't work for aliyun, so we set to dns deliberately
			bucketLookupType = minio.BucketLookupDNS
			if c.UseIAM {
				newMinioFn = aliyun.NewMinioClient
			} else {
				creds = credentials.NewStaticV4(c.AccessKeyID, c.SecretAccessKeyID, "")
			}
		default:
			matchedDefault = true
		}
	}

	if matchedDefault {
		// aws, minio
		if c.UseIAM {
			creds = credentials.NewIAM("")
		} else {
			creds = credentials.NewStaticV4(c.AccessKeyID, c.SecretAccessKeyID, "")
		}
	}

	// We must set the cert path by os environment variable "SSL_CERT_FILE",
	// because the minio.DefaultTransport() need this path to read the file content,
	// we shouldn't read this file by ourself.
	if c.UseSSL && len(c.SslCACert) > 0 {
		err := os.Setenv("SSL_CERT_FILE", c.SslCACert)
		if err != nil {
			return nil, err
		}
	}

	minioOpts := &minio.Options{
		BucketLookup: bucketLookupType,
		Creds:        creds,
		Secure:       c.UseSSL,
		Region:       c.Region,
	}
	minIOClient, err := newMinioFn(c.Address, minioOpts)
	// options nil or invalid formatted endpoint, don't need to retry
	if err != nil {
		return nil, err
	}
	var bucketExists bool
	// check valid in first query
	checkBucketFn := func() error {
		bucketExists, err = minIOClient.BucketExists(ctx, c.BucketName)
		if err != nil {
			log.Warn("failed to check blob bucket exist", zap.String("bucket", c.BucketName), zap.Error(err))
			return err
		}
		if !bucketExists {
			if c.CreateBucket {
				log.Info("blob bucket not exist, create bucket.", zap.String("bucket name", c.BucketName))
				err := minIOClient.MakeBucket(ctx, c.BucketName, minio.MakeBucketOptions{})
				if err != nil {
					log.Warn("failed to create blob bucket", zap.String("bucket", c.BucketName), zap.Error(err))
					return err
				}
			} else {
				return fmt.Errorf("bucket %s not Existed", c.BucketName)
			}
		}
		return nil
	}
	err = retry.Do(ctx, checkBucketFn, retry.Attempts(CheckBucketRetryAttempts))
	if err != nil {
		return nil, err
	}
	return minIOClient, nil
}

func NewAzureObjectStorageClient(ctx context.Context, c *Config) (*service.Client, error) {
	var client *service.Client
	var err error
	if c.UseIAM {
		cred, credErr := azidentity.NewWorkloadIdentityCredential(&azidentity.WorkloadIdentityCredentialOptions{
			ClientID:      os.Getenv("AZURE_CLIENT_ID"),
			TenantID:      os.Getenv("AZURE_TENANT_ID"),
			TokenFilePath: os.Getenv("AZURE_FEDERATED_TOKEN_FILE"),
		})
		if credErr != nil {
			return nil, credErr
		}
		client, err = service.NewClient("https://"+c.AccessKeyID+".blob."+c.Address+"/", cred, &service.ClientOptions{})
	} else {
		connectionString := os.Getenv("AZURE_STORAGE_CONNECTION_STRING")
		if connectionString == "" {
			connectionString = "DefaultEndpointsProtocol=https;AccountName=" + c.AccessKeyID +
				";AccountKey=" + c.SecretAccessKeyID + ";EndpointSuffix=" + c.Address
		}
		client, err = service.NewClientFromConnectionString(connectionString, &service.ClientOptions{})
	}
	if err != nil {
		return nil, err
	}
	if c.BucketName == "" {
		return nil, merr.WrapErrParameterInvalidMsg("invalid empty bucket name")
	}
	// check valid in first query
	checkBucketFn := func() error {
		_, err := client.NewContainerClient(c.BucketName).GetProperties(ctx, &container.GetPropertiesOptions{})
		if err != nil {
			switch err := err.(type) {
			case *azcore.ResponseError:
				if c.CreateBucket && err.ErrorCode == string(bloberror.ContainerNotFound) {
					_, createErr := client.NewContainerClient(c.BucketName).Create(ctx, &azblob.CreateContainerOptions{})
					if createErr != nil {
						return createErr
					}
					return nil
				}
			}
		}
		return err
	}
	err = retry.Do(ctx, checkBucketFn, retry.Attempts(CheckBucketRetryAttempts))
	if err != nil {
		return nil, err
	}
	return client, nil
}

func NewGcpObjectStorageClient(ctx context.Context, c *Config) (*storage.Client, error) {
	var err error

	var opts []option.ClientOption
	var projectId string
	if c.Address != "" {
		completeAddress := "http://"
		if c.UseSSL {
			completeAddress = "https://"
		}
		completeAddress = completeAddress + c.Address + "/storage/v1/"
		opts = append(opts, option.WithEndpoint(completeAddress))
	}
	if c.GcpNativeWithoutAuth {
		opts = append(opts, option.WithoutAuthentication())
	} else {
		creds, err := google.CredentialsFromJSON(ctx, []byte(c.GcpCredentialJSON), storage.ScopeReadWrite)
		if err != nil {
			return nil, err
		}
		projectId, err = getProjectId(c.GcpCredentialJSON)
		if err != nil {
			return nil, err
		}
		opts = append(opts, option.WithCredentials(creds))
	}

	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, err
	}

	if c.BucketName == "" {
		return nil, merr.WrapErrParameterInvalidMsg("invalid empty bucket name")
	}
	// Check bucket validity
	checkBucketFn := func() error {
		bucket := client.Bucket(c.BucketName)
		_, err = bucket.Attrs(ctx)
		if errors.Is(err, storage.ErrBucketNotExist) && c.CreateBucket {
			log.Info("gcs bucket does not exist, create bucket.", zap.String("bucket name", c.BucketName))
			err = client.Bucket(c.BucketName).Create(ctx, projectId, nil)
			if err != nil {
				return err
			}
			return nil
		}
		return err
	}
	err = retry.Do(ctx, checkBucketFn, retry.Attempts(CheckBucketRetryAttempts))
	if err != nil {
		return nil, err
	}
	return client, nil
}

func getProjectId(gcpCredentialJSON string) (string, error) {
	if gcpCredentialJSON == "" {
		return "", errors.New("the JSON string is empty")
	}
	var data map[string]interface{}
	if err := json.Unmarshal([]byte(gcpCredentialJSON), &data); err != nil {
		return "", errors.New("failed to parse Google Cloud credentials as JSON")
	}
	propertyValue, ok := data["project_id"]
	projectId := fmt.Sprintf("%v", propertyValue)
	if !ok {
		return "", errors.New("projectId doesn't exist")
	}
	return projectId, nil
}
