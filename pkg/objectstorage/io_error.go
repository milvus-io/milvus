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

package objectstorage

import (
	"io"
	"net/http"
	"syscall"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/cockroachdb/errors"
	"github.com/minio/minio-go/v7"
	"google.golang.org/api/googleapi"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Error code constants for cloud provider error mapping.
const (
	// Azure Blob Storage error codes
	azureBlobNotFound      = "BlobNotFound"
	azureServerBusy        = "ServerBusy"
	azureAuthFailed        = "AuthenticationFailed"
	azureAuthFailure       = "AuthorizationFailure"
	azureContainerNotFound = "ContainerNotFound"
	azureInvalidParam      = "InvalidParameterValue"
	azureInvalidRange      = "InvalidRange"
	azureRequestBodyTooLg  = "RequestBodyTooLarge"

	// S3-compatible error codes (AWS S3, MinIO, Aliyun OSS, Tencent COS, etc.)
	minioNoSuchKey      = "NoSuchKey"
	minioSlowDown       = "SlowDown"
	minioTooMany        = "TooManyRequestsException"
	minioAccessDenied   = "AccessDenied"
	minioInvalidKeyId   = "InvalidAccessKeyId"
	minioSigMismatch    = "SignatureDoesNotMatch"
	minioNoSuchBucket   = "NoSuchBucket"
	minioInvalidToken   = "InvalidToken"
	minioExpiredToken   = "ExpiredToken"
	minioInvalidArg     = "InvalidArgument"
	minioInvalidRequest = "InvalidRequest"
	minioInvalidRange   = "InvalidRange"
	minioEntityTooLarge = "EntityTooLarge"
	minioMaxMessage     = "MaxMessageLengthExceeded"
	// Aliyun OSS specific error codes (S3-compatible, via MinIO client)
	ossSecurityTokenExpired = "SecurityTokenExpired"
	ossInvalidAccessKeyId   = "InvalidAccessKeyId.Inactive"
)

// ToMilvusIoError preserves typed and context errors and classifies provider errors.
func ToMilvusIoError(fileName string, err error) error {
	if err == nil {
		return nil
	}

	// If error is already a Milvus error, return it as-is to avoid double-wrapping
	if merr.IsMilvusError(err) {
		return err
	}

	// Preserve context termination so callers can stop retries and classify
	// canceled operations correctly.
	if merr.IsCanceledOrTimeout(err) {
		return err
	}

	switch err := err.(type) {
	case *azcore.ResponseError:
		switch err.ErrorCode {
		case azureBlobNotFound:
			return merr.WrapErrIoKeyNotFound(fileName, err.Error())
		case azureServerBusy:
			return merr.WrapErrIoTooManyRequests(fileName, err)
		case azureAuthFailed, azureAuthFailure:
			return merr.WrapErrIoPermissionDenied(fileName, err)
		case azureContainerNotFound:
			return merr.WrapErrIoBucketNotFound(fileName, err)
		case azureInvalidParam:
			return merr.WrapErrIoInvalidArgument(fileName, err)
		case azureInvalidRange:
			return merr.WrapErrIoInvalidRange(fileName, err)
		case azureRequestBodyTooLg:
			return merr.WrapErrIoEntityTooLarge(fileName, err)
		default:
			return merr.WrapErrIoFailed(fileName, err)
		}
	case minio.ErrorResponse:
		switch err.Code {
		case minioNoSuchKey:
			return merr.WrapErrIoKeyNotFound(fileName, err.Error())
		case minioSlowDown, minioTooMany:
			return merr.WrapErrIoTooManyRequests(fileName, err)
		case minioAccessDenied, minioInvalidKeyId, minioSigMismatch, ossInvalidAccessKeyId:
			return merr.WrapErrIoPermissionDenied(fileName, err)
		case minioNoSuchBucket:
			return merr.WrapErrIoBucketNotFound(fileName, err)
		case minioInvalidToken, minioExpiredToken, ossSecurityTokenExpired:
			return merr.WrapErrIoInvalidCredentials(fileName, err)
		case minioInvalidArg, minioInvalidRequest:
			return merr.WrapErrIoInvalidArgument(fileName, err)
		case minioInvalidRange:
			return merr.WrapErrIoInvalidRange(fileName, err)
		case minioEntityTooLarge, minioMaxMessage:
			return merr.WrapErrIoEntityTooLarge(fileName, err)
		default:
			return merr.WrapErrIoFailed(fileName, err)
		}
	case *googleapi.Error:
		switch err.Code {
		case http.StatusNotFound:
			return merr.WrapErrIoKeyNotFound(fileName, err.Error())
		case http.StatusTooManyRequests:
			return merr.WrapErrIoTooManyRequests(fileName, err)
		case http.StatusForbidden:
			return merr.WrapErrIoPermissionDenied(fileName, err)
		case http.StatusUnauthorized:
			return merr.WrapErrIoInvalidCredentials(fileName, err)
		case http.StatusBadRequest:
			return merr.WrapErrIoInvalidArgument(fileName, err)
		case http.StatusRequestedRangeNotSatisfiable:
			return merr.WrapErrIoInvalidRange(fileName, err)
		case http.StatusRequestEntityTooLarge:
			return merr.WrapErrIoEntityTooLarge(fileName, err)
		default:
			return merr.WrapErrIoFailed(fileName, err)
		}
	}

	// GCP cloud.google.com/go/storage sentinel errors (not *googleapi.Error).
	// The library wraps them with a multi-%w, so plain errors.Is misses them.
	if IsGcsObjectNotExist(err) {
		return merr.WrapErrIoKeyNotFound(fileName, err.Error())
	}
	if IsGcsBucketNotExist(err) {
		return merr.WrapErrIoBucketNotFound(fileName, err)
	}

	// syscall.ECONNRESET is typically triggered by rate limiting
	if errors.Is(err, syscall.ECONNRESET) {
		return merr.WrapErrIoTooManyRequests(fileName, err)
	}
	if err == io.ErrUnexpectedEOF {
		return merr.WrapErrIoUnexpectEOF(fileName, err)
	}
	return merr.WrapErrIoFailed(fileName, err)
}
