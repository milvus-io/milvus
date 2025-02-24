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

package accesslog

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

const megabyte = 1024 * 1024

var (
	CheckBucketRetryAttempts uint = 20
	timeNameFormat                = ".2006-01-02T15-04-05.000"
)

type CacheWriter struct {
	mu     sync.Mutex
	writer *bufio.Writer
	closer io.Closer

	// interval of auto flush
	flushInterval time.Duration

	closed    bool
	closeOnce sync.Once
	closeCh   chan struct{}
	closeWg   sync.WaitGroup
}

func NewCacheWriter(writer io.Writer, cacheSize int, flushInterval time.Duration) *CacheWriter {
	c := &CacheWriter{
		writer:        bufio.NewWriterSize(writer, cacheSize),
		flushInterval: flushInterval,
		closeCh:       make(chan struct{}),
	}
	c.Start()
	return c
}

func NewCacheWriterWithCloser(writer io.Writer, closer io.Closer, cacheSize int, flushInterval time.Duration) *CacheWriter {
	c := &CacheWriter{
		writer:        bufio.NewWriterSize(writer, cacheSize),
		flushInterval: flushInterval,
		closer:        closer,
		closeCh:       make(chan struct{}),
	}
	c.Start()
	return c
}

func (l *CacheWriter) Write(p []byte) (n int, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return 0, fmt.Errorf("write to closed writer")
	}

	return l.writer.Write(p)
}

func (l *CacheWriter) Flush() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.writer.Flush()
}

func (l *CacheWriter) Start() {
	l.closeWg.Add(1)
	go func() {
		defer l.closeWg.Done()
		if l.flushInterval == 0 {
			return
		}
		ticker := time.NewTicker(l.flushInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				l.Flush()
			case <-l.closeCh:
				return
			}
		}
	}()
}

func (l *CacheWriter) Close() {
	l.closeOnce.Do(func() {
		// close auto flush
		close(l.closeCh)
		l.closeWg.Wait()

		l.mu.Lock()
		defer l.mu.Unlock()
		l.closed = true

		// flush remaining bytes
		l.writer.Flush()

		if l.closer != nil {
			l.closer.Close()
		}
	})
}

// a rotated file writer
type RotateWriter struct {
	// local path is the path to save log before update to minIO
	// use os.TempDir()/accesslog if empty
	localPath string
	fileName  string
	// the time interval of rotate and update log to minIO
	rotatedTime int64
	// the max size(MB) of log file
	// if local file large than maxSize will update immediately
	// close if empty(zero)
	maxSize int
	// MaxBackups is the maximum number of old log files to retain
	// close retention limit if empty(zero)
	maxBackups int

	handler *minioHandler

	size int64
	file *os.File
	mu   sync.Mutex

	millCh chan bool

	closed    bool
	closeCh   chan struct{}
	closeWg   sync.WaitGroup
	closeOnce sync.Once
}

func NewRotateWriter(logCfg *paramtable.AccessLogConfig, minioCfg *paramtable.MinioConfig) (*RotateWriter, error) {
	logger := &RotateWriter{
		localPath:   logCfg.LocalPath.GetValue(),
		fileName:    logCfg.Filename.GetValue(),
		rotatedTime: logCfg.RotatedTime.GetAsInt64(),
		maxSize:     logCfg.MaxSize.GetAsInt(),
		maxBackups:  logCfg.MaxBackups.GetAsInt(),
		closeCh:     make(chan struct{}),
	}
	log.Info("Access log save to " + logger.dir())
	if logCfg.MinioEnable.GetAsBool() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		log.Info("Access log will backup files to minio", zap.String("remote", logCfg.RemotePath.GetValue()), zap.String("maxBackups", logCfg.MaxBackups.GetValue()))
		handler, err := NewMinioHandler(ctx, minioCfg, logCfg.RemotePath.GetValue(), logCfg.MaxBackups.GetAsInt())
		if err != nil {
			return nil, err
		}
		prefix, ext := logger.prefixAndExt()
		if logCfg.RemoteMaxTime.GetAsInt() > 0 {
			handler.retentionPolicy = getTimeRetentionFunc(logCfg.RemoteMaxTime.GetAsInt(), prefix, ext)
		}

		logger.handler = handler
	}

	logger.start()
	return logger, nil
}

func (l *RotateWriter) Write(p []byte) (n int, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return 0, fmt.Errorf("write to closed writer")
	}

	writeLen := int64(len(p))
	if writeLen > l.max() {
		return 0, fmt.Errorf(
			"write length %d exceeds maximum file size %d", writeLen, l.max(),
		)
	}

	if l.file == nil {
		if err = l.openFileExistingOrNew(); err != nil {
			return 0, err
		}
	}

	if l.size+writeLen > l.max() {
		if err := l.rotate(); err != nil {
			return 0, err
		}
	}

	n, err = l.file.Write(p)
	l.size += int64(n)
	return n, err
}

func (l *RotateWriter) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.closeOnce.Do(func() {
		close(l.closeCh)
		if l.handler != nil {
			l.handler.Close()
		}

		l.closeWg.Wait()
		l.closed = true
	})

	return l.closeFile()
}

func (l *RotateWriter) Rotate() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.rotate()
}

func (l *RotateWriter) rotate() error {
	if l.size == 0 {
		return nil
	}

	if err := l.closeFile(); err != nil {
		return err
	}
	if err := l.openNewFile(); err != nil {
		return err
	}
	l.mill()
	return nil
}

func (l *RotateWriter) openFileExistingOrNew() error {
	l.mill()
	filename := l.filename()
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return l.openNewFile()
	}
	if err != nil {
		return fmt.Errorf("file to get log file info: %s", err)
	}

	file, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return l.openNewFile()
	}

	l.file = file
	l.size = info.Size()
	return nil
}

func (l *RotateWriter) openNewFile() error {
	err := os.MkdirAll(l.dir(), 0o744)
	if err != nil {
		return fmt.Errorf("make directories for new log file filed: %s", err)
	}

	name := l.filename()
	mode := os.FileMode(0o644)
	info, err := os.Stat(name)
	if err == nil {
		mode = info.Mode()
		newName := l.newBackupName()
		if err := os.Rename(name, newName); err != nil {
			return fmt.Errorf("can't rename log file: %s", err)
		}
		log.Info("seal old log to: " + newName)
		if l.handler != nil {
			l.handler.Update(newName, path.Base(newName))
		}

		// for linux
		if err := chown(name, info); err != nil {
			return err
		}
	}

	f, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return fmt.Errorf("can't open new logfile: %s", err)
	}
	l.file = f
	l.size = 0
	return nil
}

func (l *RotateWriter) closeFile() error {
	if l.file == nil {
		return nil
	}
	err := l.file.Close()
	l.file = nil
	return err
}

// Remove old log when log num over maxBackups
func (l *RotateWriter) millRunOnce() error {
	files, err := l.oldLogFiles()
	if err != nil {
		return err
	}

	if l.maxBackups >= 0 && l.maxBackups < len(files) {
		for _, f := range files[:len(files)-l.maxBackups] {
			errRemove := os.Remove(path.Join(l.dir(), f.fileName))
			if err == nil && errRemove != nil {
				err = errRemove
			}
		}
	}

	return err
}

// millRun runs in a goroutine to remove old log files out of limit.
func (l *RotateWriter) millRun() {
	defer l.closeWg.Done()
	for {
		select {
		case <-l.closeCh:
			log.Warn("close Access log mill")
			return
		case <-l.millCh:
			_ = l.millRunOnce()
		}
	}
}

func (l *RotateWriter) mill() {
	select {
	case l.millCh <- true:
	default:
	}
}

func (l *RotateWriter) timeRotating() {
	ticker := time.NewTicker(time.Duration(l.rotatedTime * int64(time.Second)))
	log.Info("start time rotating of access log")
	defer ticker.Stop()
	defer l.closeWg.Done()

	for {
		select {
		case <-l.closeCh:
			log.Warn("close Access file logger")
			return
		case <-ticker.C:
			l.Rotate()
		}
	}
}

// start rotate log file by time
func (l *RotateWriter) start() {
	if l.rotatedTime > 0 {
		l.closeWg.Add(1)
		go l.timeRotating()
	}

	if l.maxBackups > 0 {
		l.closeWg.Add(1)
		l.millCh = make(chan bool, 1)
		go l.millRun()
	}
}

func (l *RotateWriter) max() int64 {
	return int64(l.maxSize) * int64(megabyte)
}

func (l *RotateWriter) dir() string {
	if l.localPath == "" {
		l.localPath = path.Join(os.TempDir(), "milvus_accesslog")
	}
	return l.localPath
}

func (l *RotateWriter) filename() string {
	return path.Join(l.dir(), l.fileName)
}

func (l *RotateWriter) prefixAndExt() (string, string) {
	ext := path.Ext(l.fileName)
	prefix := l.fileName[:len(l.fileName)-len(ext)]
	return prefix, ext
}

func (l *RotateWriter) newBackupName() string {
	t := time.Now()
	timestamp := t.Format(timeNameFormat)
	prefix, ext := l.prefixAndExt()
	return path.Join(l.dir(), prefix+timestamp+ext)
}

func (l *RotateWriter) oldLogFiles() ([]logInfo, error) {
	files, err := os.ReadDir(l.dir())
	if err != nil {
		return nil, fmt.Errorf("can't read log file directory: %s", err)
	}

	logFiles := []logInfo{}
	prefix, ext := l.prefixAndExt()

	for _, f := range files {
		if f.IsDir() {
			continue
		}
		if t, err := timeFromName(f.Name(), prefix, ext); err == nil {
			logFiles = append(logFiles, logInfo{t, f.Name()})
		}
	}

	return logFiles, nil
}

type logInfo struct {
	timestamp time.Time
	fileName  string
}
