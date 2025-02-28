package components

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime/pprof"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/conc"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

var errStopTimeout = errors.New("stop timeout")

// exitWhenStopTimeout stops a component with timeout and exit progress when timeout.
func exitWhenStopTimeout(stop func() error, timeout time.Duration) error {
	err := dumpPprof(func() error { return stopWithTimeout(stop, timeout) })
	if errors.Is(err, errStopTimeout) {
		log.Info("stop progress timeout, force exit")
		os.Exit(1)
	}
	return err
}

// stopWithTimeout stops a component with timeout.
func stopWithTimeout(stop func() error, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	future := conc.Go(func() (struct{}, error) {
		return struct{}{}, dumpPprof(stop)
	})
	select {
	case <-future.Inner():
		return errors.Wrap(future.Err(), "failed to stop component")
	case <-ctx.Done():
		return errStopTimeout
	}
}

// profileType defines the structure for each type of profile to be collected
type profileType struct {
	name     string               // Name of the profile type
	filename string               // File path for the profile
	dump     func(*os.File) error // Function to dump the profile data
}

// dumpPprof wraps the execution of a function with pprof profiling
// It collects various performance profiles only if the execution fails
func dumpPprof(exec func() error) error {
	// Get pprof directory from configuration
	pprofDir := paramtable.Get().ServiceParam.ProfileCfg.PprofPath.GetValue()
	if err := os.MkdirAll(pprofDir, 0o755); err != nil {
		log.Error("failed to create pprof directory", zap.Error(err))
		return exec()
	}

	// Generate base file path with timestamp
	baseFilePath := filepath.Join(
		pprofDir,
		fmt.Sprintf("%s_pprof_%s",
			paramtable.GetRole(),
			time.Now().Format("20060102_150405"),
		),
	)

	// Define all profile types to be collected
	profiles := []profileType{
		{
			name:     "cpu",
			filename: baseFilePath + "_cpu.prof",
			dump: func(f *os.File) error {
				// Ensure no other CPU profiling is active before starting a new one.
				// This prevents the "cpu profiling already in use" error.
				pprof.StopCPUProfile()
				return pprof.StartCPUProfile(f)
			},
		},
		{
			name:     "goroutine",
			filename: baseFilePath + "_goroutine.prof",
			dump: func(f *os.File) error {
				return pprof.Lookup("goroutine").WriteTo(f, 0)
			},
		},
		{
			name:     "heap",
			filename: baseFilePath + "_heap.prof",
			dump: func(f *os.File) error {
				return pprof.WriteHeapProfile(f)
			},
		},
		{
			name:     "block",
			filename: baseFilePath + "_block.prof",
			dump: func(f *os.File) error {
				return pprof.Lookup("block").WriteTo(f, 0)
			},
		},
		{
			name:     "mutex",
			filename: baseFilePath + "_mutex.prof",
			dump: func(f *os.File) error {
				return pprof.Lookup("mutex").WriteTo(f, 0)
			},
		},
	}

	// Create all profile files and store file handles
	files := make(map[string]*os.File)
	for _, p := range profiles {
		f, err := os.Create(p.filename)
		if err != nil {
			log.Error("could not create profile file",
				zap.String("profile", p.name),
				zap.Error(err))
			for filename, f := range files {
				f.Close()
				os.Remove(filename)
			}
			return exec()
		}
		files[p.filename] = f
	}
	// Ensure all files are closed when function returns
	defer func() {
		for _, f := range files {
			f.Close()
		}
	}()

	// Start CPU profiling
	cpuProfile := profiles[0]
	if err := cpuProfile.dump(files[cpuProfile.filename]); err != nil {
		log.Error("could not start CPU profiling", zap.Error(err))
		return exec()
	}
	defer pprof.StopCPUProfile()

	// Execute the target function
	execErr := exec()

	// Only save profiles and collect additional data if execution fails
	if execErr != nil {
		// Start from index 1 to skip CPU profile (already running)
		for _, p := range profiles[1:] {
			if err := p.dump(files[p.filename]); err != nil {
				log.Error("could not write profile",
					zap.String("profile", p.name),
					zap.Error(err))
			}
		}
	} else {
		// Remove all files if execution succeeds
		for _, p := range profiles {
			os.Remove(p.filename)
		}
	}

	return execErr
}
