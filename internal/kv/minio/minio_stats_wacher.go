package miniokv

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/zilliztech/milvus-distributed/internal/log"
	"go.uber.org/zap"
)

type MinioStatsWatcher struct {
	mu            sync.RWMutex
	client        *minio.Client
	objCreateSize int64
	startTime     time.Time
	bucketName    string
	helper        MinioStatsWatcherHelper
}

type MinioStatsWatcherHelper struct {
	eventAfterStartWatch func()
	eventAfterNotify     func()
}

func defaultStatsHelper() MinioStatsWatcherHelper {
	return MinioStatsWatcherHelper{
		eventAfterStartWatch: func() {},
		eventAfterNotify:     func() {},
	}
}

func NewMinioStatsWatcher(client *minio.Client, bucketName string) *MinioStatsWatcher {
	return &MinioStatsWatcher{
		client:     client,
		bucketName: bucketName,
		helper:     defaultStatsHelper(),
	}
}

func NewMinioStatsWatcherWithHelper(client *minio.Client, bucketName string, helper MinioStatsWatcherHelper) *MinioStatsWatcher {
	stats := NewMinioStatsWatcher(client, bucketName)
	stats.helper = helper
	return stats
}

func (s *MinioStatsWatcher) StartBackground(ctx context.Context) {
	s.mu.Lock()
	s.startTime = time.Now()
	s.mu.Unlock()
	ch := s.client.ListenBucketNotification(ctx, s.bucketName, "", "", []string{"s3:ObjectCreated:*"})

	s.helper.eventAfterStartWatch()
	for {
		select {
		case <-ctx.Done():
			log.Debug("minio stats shutdown")
			return
		case info := <-ch:
			if info.Err != nil {
				log.Error("minio receive wrong notification", zap.Error(info.Err))
				continue
			}
			var size int64
			for _, record := range info.Records {
				size += record.S3.Object.Size
			}
			s.mu.Lock()
			s.objCreateSize += size
			s.mu.Unlock()
			s.helper.eventAfterNotify()
		}
	}
}

func (s *MinioStatsWatcher) GetObjectCreateSize() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.objCreateSize
}

func (s *MinioStatsWatcher) GetStartTime() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.startTime
}

func (s *MinioStatsWatcher) String() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	duration := time.Since(s.startTime).Seconds()
	return fmt.Sprintf("object create %d bytes in %f seconds, avg: %f", s.objCreateSize, duration, float64(s.objCreateSize)/duration)
}

func (s *MinioStatsWatcher) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.objCreateSize = 0
	s.startTime = time.Now()
}
