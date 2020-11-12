package miniodriver

import (
	"context"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/zilliztech/milvus-distributed/internal/storage/internal/minio/codec"
	storageType "github.com/zilliztech/milvus-distributed/internal/storage/type"
)

type MinioDriver struct {
	driver *minioStore
}

func NewMinioDriver(ctx context.Context) (*MinioDriver, error) {
	// to-do read conf
	var endPoint = "localhost:9000"
	var accessKeyID = "testminio"
	var secretAccessKey = "testminio"
	var useSSL = false

	minioClient, err := minio.New(endPoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: useSSL,
	})

	if err != nil {
		return nil, err
	}

	bucketExists, err := minioClient.BucketExists(ctx, bucketName)
	if err != nil {
		return nil, err
	}

	if !bucketExists {
		err = minioClient.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{})
		if err != nil {
			return nil, err
		}
	}
	return &MinioDriver{
		&minioStore{
			client: minioClient,
		},
	}, nil
}

func (s *MinioDriver) put(ctx context.Context, key storageType.Key, value storageType.Value, timestamp storageType.Timestamp, suffix string) error {
	minioKey, err := codec.MvccEncode(key, timestamp, suffix)
	if err != nil {
		return err
	}

	err = s.driver.Put(ctx, minioKey, value)
	return err
}

func (s *MinioDriver) scanLE(ctx context.Context, key storageType.Key, timestamp storageType.Timestamp, keyOnly bool) ([]storageType.Timestamp, []storageType.Key, []storageType.Value, error) {
	keyEnd, err := codec.MvccEncode(key, timestamp, "")
	if err != nil {
		return nil, nil, nil, err
	}

	keys, values, err := s.driver.Scan(ctx, key, []byte(keyEnd), -1, keyOnly)
	if err != nil {
		return nil, nil, nil, err
	}

	var timestamps []storageType.Timestamp
	for _, key := range keys {
		_, timestamp, _, _ := codec.MvccDecode(key)
		timestamps = append(timestamps, timestamp)
	}

	return timestamps, keys, values, nil
}

func (s *MinioDriver) scanGE(ctx context.Context, key storageType.Key, timestamp storageType.Timestamp, keyOnly bool) ([]storageType.Timestamp, []storageType.Key, []storageType.Value, error) {
	keyStart, err := codec.MvccEncode(key, timestamp, "")
	if err != nil {
		return nil, nil, nil, err
	}

	keys, values, err := s.driver.Scan(ctx, key, keyStart, -1, keyOnly)
	if err != nil {
		return nil, nil, nil, err
	}

	var timestamps []storageType.Timestamp
	for _, key := range keys {
		_, timestamp, _, _ := codec.MvccDecode(key)
		timestamps = append(timestamps, timestamp)
	}

	return timestamps, keys, values, nil
}

//scan(ctx context.Context, key storageType.Key, start storageType.Timestamp, end storageType.Timestamp, withValue bool) ([]storageType.Timestamp, []storageType.Key, []storageType.Value, error)
func (s *MinioDriver) deleteLE(ctx context.Context, key storageType.Key, timestamp storageType.Timestamp) error {
	keyEnd, err := codec.MvccEncode(key, timestamp, "delete")
	if err != nil {
		return err
	}
	err = s.driver.DeleteRange(ctx, key, keyEnd)
	return err
}
func (s *MinioDriver) deleteGE(ctx context.Context, key storageType.Key, timestamp storageType.Timestamp) error {
	keys, _, err := s.driver.GetByPrefix(ctx, key, true)
	if err != nil {
		return err
	}
	keyStart, err := codec.MvccEncode(key, timestamp, "")
	if err != nil {
		panic(err)
	}
	err = s.driver.DeleteRange(ctx, keyStart, keys[len(keys)-1])
	if err != nil {
		panic(err)
	}
	return nil
}
func (s *MinioDriver) deleteRange(ctx context.Context, key storageType.Key, start storageType.Timestamp, end storageType.Timestamp) error {
	keyStart, err := codec.MvccEncode(key, start, "")
	if err != nil {
		return err
	}
	keyEnd, err := codec.MvccEncode(key, end, "")
	if err != nil {
		return err
	}
	err = s.driver.DeleteRange(ctx, keyStart, keyEnd)
	return err
}

func (s *MinioDriver) GetRow(ctx context.Context, key storageType.Key, timestamp storageType.Timestamp) (storageType.Value, error) {
	minioKey, err := codec.MvccEncode(key, timestamp, "")
	if err != nil {
		return nil, err
	}

	keys, values, err := s.driver.Scan(ctx, append(key, byte('_')), minioKey, 1, false)
	if values == nil || keys == nil {
		return nil, err
	}

	_, _, suffix, err := codec.MvccDecode(keys[0])
	if err != nil {
		return nil, err
	}
	if suffix == "delete" {
		return nil, nil
	}

	return values[0], err
}
func (s *MinioDriver) GetRows(ctx context.Context, keys []storageType.Key, timestamps []storageType.Timestamp) ([]storageType.Value, error) {
	var values []storageType.Value
	for i, key := range keys {
		value, err := s.GetRow(ctx, key, timestamps[i])
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, nil
}

func (s *MinioDriver) PutRow(ctx context.Context, key storageType.Key, value storageType.Value, segment string, timestamp storageType.Timestamp) error {
	minioKey, err := codec.MvccEncode(key, timestamp, segment)
	if err != nil {
		return err
	}
	err = s.driver.Put(ctx, minioKey, value)
	return err
}
func (s *MinioDriver) PutRows(ctx context.Context, keys []storageType.Key, values []storageType.Value, segments []string, timestamps []storageType.Timestamp) error {
	maxThread := 100
	batchSize := 1
	keysLength := len(keys)

	if keysLength/batchSize > maxThread {
		batchSize = keysLength / maxThread
	}

	batchNums := keysLength / batchSize

	if keysLength%batchSize != 0 {
		batchNums = keysLength/batchSize + 1
	}

	errCh := make(chan error)
	f := func(ctx2 context.Context, keys2 []storageType.Key, values2 []storageType.Value, segments2 []string, timestamps2 []storageType.Timestamp) {
		for i := 0; i < len(keys2); i++ {
			err := s.PutRow(ctx2, keys2[i], values2[i], segments2[i], timestamps2[i])
			errCh <- err
		}
	}
	for i := 0; i < batchNums; i++ {
		j := i
		go func() {
			start, end := j*batchSize, (j+1)*batchSize
			if len(keys) < end {
				end = len(keys)
			}
			f(ctx, keys[start:end], values[start:end], segments[start:end], timestamps[start:end])
		}()
	}

	for i := 0; i < len(keys); i++ {
		if err := <-errCh; err != nil {
			return err
		}
	}
	return nil
}

func (s *MinioDriver) GetSegments(ctx context.Context, key storageType.Key, timestamp storageType.Timestamp) ([]string, error) {
	keyEnd, err := codec.MvccEncode(key, timestamp, "")
	if err != nil {
		return nil, err
	}
	keys, _, err := s.driver.Scan(ctx, append(key, byte('_')), keyEnd, -1, true)
	if err != nil {
		return nil, err
	}
	segmentsSet := map[string]bool{}
	for _, key := range keys {
		_, _, segment, err := codec.MvccDecode(key)
		if err != nil {
			panic("must no error")
		}
		if segment != "delete" {
			segmentsSet[segment] = true
		}
	}

	var segments []string
	for k, v := range segmentsSet {
		if v {
			segments = append(segments, k)
		}
	}
	return segments, err
}

func (s *MinioDriver) DeleteRow(ctx context.Context, key storageType.Key, timestamp storageType.Timestamp) error {
	minioKey, err := codec.MvccEncode(key, timestamp, "delete")
	if err != nil {
		return err
	}
	value := []byte("0")
	err = s.driver.Put(ctx, minioKey, value)
	return err
}

func (s *MinioDriver) DeleteRows(ctx context.Context, keys []storageType.Key, timestamps []storageType.Timestamp) error {
	maxThread := 100
	batchSize := 1
	keysLength := len(keys)

	if keysLength/batchSize > maxThread {
		batchSize = keysLength / maxThread
	}

	batchNums := keysLength / batchSize

	if keysLength%batchSize != 0 {
		batchNums = keysLength/batchSize + 1
	}

	errCh := make(chan error)
	f := func(ctx2 context.Context, keys2 []storageType.Key, timestamps2 []storageType.Timestamp) {
		for i := 0; i < len(keys2); i++ {
			err := s.DeleteRow(ctx2, keys2[i], timestamps2[i])
			errCh <- err
		}
	}
	for i := 0; i < batchNums; i++ {
		j := i
		go func() {
			start, end := j*batchSize, (j+1)*batchSize
			if len(keys) < end {
				end = len(keys)
			}
			f(ctx, keys[start:end], timestamps[start:end])
		}()
	}

	for i := 0; i < len(keys); i++ {
		if err := <-errCh; err != nil {
			return err
		}
	}
	return nil
}

func (s *MinioDriver) PutLog(ctx context.Context, key storageType.Key, value storageType.Value, timestamp storageType.Timestamp, channel int) error {
	logKey := codec.LogEncode(key, timestamp, channel)
	err := s.driver.Put(ctx, logKey, value)
	return err
}

func (s *MinioDriver) GetLog(ctx context.Context, start storageType.Timestamp, end storageType.Timestamp, channels []int) ([]storageType.Value, error) {
	keys, values, err := s.driver.GetByPrefix(ctx, []byte("log_"), false)
	if err != nil {
		return nil, err
	}

	var resultValues []storageType.Value
	for i, key := range keys {
		_, ts, channel, err := codec.LogDecode(string(key))
		if err != nil {
			return nil, err
		}
		if ts >= start && ts <= end {
			for j := 0; j < len(channels); j++ {
				if channel == channels[j] {
					resultValues = append(resultValues, values[i])
				}
			}
		}
	}

	return resultValues, nil
}

func (s *MinioDriver) GetSegmentIndex(ctx context.Context, segment string) (storageType.SegmentIndex, error) {

	return s.driver.Get(ctx, codec.SegmentEncode(segment, "index"))
}

func (s *MinioDriver) PutSegmentIndex(ctx context.Context, segment string, index storageType.SegmentIndex) error {

	return s.driver.Put(ctx, codec.SegmentEncode(segment, "index"), index)
}

func (s *MinioDriver) DeleteSegmentIndex(ctx context.Context, segment string) error {

	return s.driver.Delete(ctx, codec.SegmentEncode(segment, "index"))
}

func (s *MinioDriver) GetSegmentDL(ctx context.Context, segment string) (storageType.SegmentDL, error) {

	return s.driver.Get(ctx, codec.SegmentEncode(segment, "DL"))
}

func (s *MinioDriver) PutSegmentDL(ctx context.Context, segment string, log storageType.SegmentDL) error {

	return s.driver.Put(ctx, codec.SegmentEncode(segment, "DL"), log)
}

func (s *MinioDriver) DeleteSegmentDL(ctx context.Context, segment string) error {

	return s.driver.Delete(ctx, codec.SegmentEncode(segment, "DL"))
}
