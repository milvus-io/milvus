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
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"go.uber.org/zap"
)

const megabyte = 1024 * 1024

var CheckBucketRetryAttempts uint = 20
var timeFormat = ".2006-01-02T15-04-05.000"

// a rotated file logger for zap.log and could upload sealed log file to minIO
type RotateLogger struct {
	//local path is the path to save log before update to minIO
	//use os.TempDir()/accesslog if empty
	localPath string
	fileName  string
	//the interval time of update log to minIO
	rotatedTime int64
	//the max size(Mb) of log file
	//if local file large than maxSize will update immediately
	//close if empty(zero)
	maxSize int
	//MaxBackups is the maximum number of old log files to retain
	//close retention limit if empty(zero)
	maxBackups int

	handler *minioHandler

	size int64
	file *os.File
	mu   sync.Mutex

	millCh    chan bool
	closeCh   chan struct{}
	closeWg   sync.WaitGroup
	closeOnce sync.Once
}

func NewRotateLogger(logCfg *paramtable.AccessLogConfig, minioCfg *paramtable.MinioConfig) (*RotateLogger, error) {
	logger := &RotateLogger{
		localPath:   logCfg.LocalPath.GetValue(),
		fileName:    logCfg.Filename.GetValue(),
		rotatedTime: logCfg.RotatedTime.GetAsInt64(),
		maxSize:     logCfg.MaxSize.GetAsInt(),
		maxBackups:  logCfg.MaxBackups.GetAsInt(),
	}
	log.Info("Access log save to " + logger.dir())
	if logCfg.MinioEnable.GetAsBool() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		log.Debug("remtepath", zap.Any("remote", logCfg.RemotePath.GetValue()))
		log.Debug("maxBackups", zap.Any("maxBackups", logCfg.MaxBackups.GetValue()))
		handler, err := NewMinioHandler(ctx, minioCfg, logCfg.RemotePath.GetValue(), logCfg.MaxBackups.GetAsInt())
		if err != nil {
			return nil, err
		}
		prefix, ext := logger.prefixAndExt()
		handler.retentionPolicy = getTimeRetentionFunc(logCfg.RemoteMaxTime.GetAsInt(), prefix, ext)
		logger.handler = handler
	}

	logger.start()

	return logger, nil
}

func (l *RotateLogger) Write(p []byte) (n int, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()

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

func (l *RotateLogger) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.closeOnce.Do(func() {
		close(l.closeCh)
		if l.handler != nil {
			l.handler.Close()
		}

		l.closeWg.Wait()
	})

	return l.closeFile()
}

func (l *RotateLogger) Rotate() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.rotate()
}

func (l *RotateLogger) rotate() error {
	if err := l.closeFile(); err != nil {
		return err
	}
	if err := l.openNewFile(); err != nil {
		return err
	}
	l.mill()
	return nil
}

func (l *RotateLogger) openFileExistingOrNew() error {
	l.mill()
	filename := l.filename()
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return l.openNewFile()
	}
	if err != nil {
		return fmt.Errorf("file to get log file info: %s", err)
	}

	file, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return l.openNewFile()
	}

	l.file = file
	l.size = info.Size()
	return nil
}

func (l *RotateLogger) openNewFile() error {
	err := os.MkdirAll(l.dir(), 0744)
	if err != nil {
		return fmt.Errorf("make directories for new log file filed: %s", err)
	}

	name := l.filename()
	mode := os.FileMode(0644)
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

func (l *RotateLogger) closeFile() error {
	if l.file == nil {
		return nil
	}
	err := l.file.Close()
	l.file = nil
	return err
}

func (l *RotateLogger) millRunOnce() error {
	files, err := l.oldLogFiles()
	if err != nil {
		return err
	}

	if l.maxBackups > 0 && l.maxBackups < len(files) {
		for _, f := range files[l.maxBackups:] {
			errRemove := os.Remove(path.Join(l.dir(), f.fileName))
			if err == nil && errRemove != nil {
				err = errRemove
			}
		}
	}

	return err
}

// millRun runs in a goroutine to remove old log files out of limit.
func (l *RotateLogger) millRun() {
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

func (l *RotateLogger) mill() {
	select {
	case l.millCh <- true:
	default:
	}
}

func (l *RotateLogger) timeRotating() {
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
func (l *RotateLogger) start() {
	l.closeCh = make(chan struct{})
	l.closeWg = sync.WaitGroup{}
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

func (l *RotateLogger) max() int64 {
	return int64(l.maxSize) * int64(megabyte)
}

func (l *RotateLogger) dir() string {
	if l.localPath == "" {
		l.localPath = path.Join(os.TempDir(), "milvus_accesslog")
	}
	return l.localPath
}

func (l *RotateLogger) filename() string {
	return path.Join(l.dir(), l.fileName)
}

func (l *RotateLogger) prefixAndExt() (string, string) {
	ext := path.Ext(l.fileName)
	prefix := l.fileName[:len(l.fileName)-len(ext)]
	return prefix, ext
}

func (l *RotateLogger) newBackupName() string {
	t := time.Now()
	timestamp := t.Format(timeFormat)
	prefix, ext := l.prefixAndExt()
	return path.Join(l.dir(), prefix+timestamp+ext)
}

func (l *RotateLogger) oldLogFiles() ([]logInfo, error) {
	files, err := ioutil.ReadDir(l.dir())
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
			continue
		}
	}

	return logFiles, nil
}

type logInfo struct {
	timestamp time.Time
	fileName  string
}
