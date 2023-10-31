package storage

import (
	"context"
	"io"
	"time"

	"golang.org/x/exp/mmap"
)

var CheckBucketRetryAttempts uint = 20

const (
	CloudProviderGCP    = "gcp"
	CloudProviderAWS    = "aws"
	CloudProviderAliyun = "aliyun"

	CloudProviderAzure = "azure"
)

type Config struct {
	Address           string
	BucketName        string
	AccessKeyID       string
	SecretAccessKeyID string
	UseSSL            bool
	CreateBucket      bool
	RootPath          string
	UseIAM            bool
	CloudProvider     string
	IamEndpoint       string
	UseVirtualHost    bool
	Region            string
	RequestTimeoutMs  int64
}

type FileReader interface {
	io.Reader
	io.Closer
}

type ObjectStorage interface {
	GetObject(ctx context.Context, bucketName, objectName string, offset int64, size int64) (FileReader, error)
	PutObject(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64) error
	StatObject(ctx context.Context, bucketName, objectName string) (int64, error)
	ListObjects(ctx context.Context, bucketName string, prefix string, recursive bool) (map[string]time.Time, error)
	RemoveObject(ctx context.Context, bucketName, objectName string) error
}

type ChunkManager interface {
	// RootPath returns current root path.
	RootPath() string
	// Path returns path of @filePath.
	Path(ctx context.Context, filePath string) (string, error)
	// Size returns path of @filePath.
	Size(ctx context.Context, filePath string) (int64, error)
	// Write writes @content to @filePath.
	Write(ctx context.Context, filePath string, content []byte) error
	// MultiWrite writes multi @content to @filePath.
	MultiWrite(ctx context.Context, contents map[string][]byte) error
	// Exist returns true if @filePath exists.
	Exist(ctx context.Context, filePath string) (bool, error)
	// Read reads @filePath and returns content.
	Read(ctx context.Context, filePath string) ([]byte, error)
	// Reader return a reader for @filePath
	Reader(ctx context.Context, filePath string) (FileReader, error)
	// MultiRead reads @filePath and returns content.
	MultiRead(ctx context.Context, filePaths []string) ([][]byte, error)
	ListWithPrefix(ctx context.Context, prefix string, recursive bool) ([]string, []time.Time, error)
	// ReadWithPrefix reads files with same @prefix and returns contents.
	ReadWithPrefix(ctx context.Context, prefix string) ([]string, [][]byte, error)
	Mmap(ctx context.Context, filePath string) (*mmap.ReaderAt, error)
	// ReadAt reads @filePath by offset @off, content stored in @p, return @n as the number of bytes read.
	// if all bytes are read, @err is io.EOF.
	// return other error if read failed.
	ReadAt(ctx context.Context, filePath string, off int64, length int64) (p []byte, err error)
	// Remove delete @filePath.
	Remove(ctx context.Context, filePath string) error
	// MultiRemove delete @filePaths.
	MultiRemove(ctx context.Context, filePaths []string) error
	// RemoveWithPrefix remove files with same @prefix.
	RemoveWithPrefix(ctx context.Context, prefix string) error
}
