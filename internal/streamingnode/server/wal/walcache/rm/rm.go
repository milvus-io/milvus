package rm

import (
	"fmt"
	"os"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walcache"
)

func Init(
	memoryLimit int64,
	diskLimit int64,
	diskPath string,
) {
	Memory = newRM(memoryLimit, func(bo walcache.BlockOperator) {
		bo.AsyncEvictMemory()
	})
	Memory.StartEventLoop()

	if diskLimit > 0 && diskPath != "" {
		Disk = newDiskResourceManager(diskPath, diskLimit)
		Disk.StartEventLoop()
	}
}

var (
	Memory *ResourceManager
	Disk   *DiskResourceManager
)

// allocateReq is the request of allocate.
type allocateReq struct {
	r    walcache.BlockOperator
	resp chan struct{}
}

// freeReq is the request of free.
type freeReq struct {
	bytes int64
}

func newRM(thresholdBytes int64, evictor func(walcache.BlockOperator)) *ResourceManager {
	return &ResourceManager{
		thresholdBytes:    thresholdBytes,
		allocatedBytes:    0,
		onEvcitedBytes:    0,
		pendingAllocating: make([]allocateReq, 0),
		allocateReq:       make(chan allocateReq),
		freeReq:           make(chan freeReq),
		evictOrder:        make([]walcache.BlockOperator, 0),
		evictor:           evictor,
	}
}

// newDiskResourceManager creates a new disk cache.
func newDiskResourceManager(rootPath string, limit int64) *DiskResourceManager {
	if err := os.RemoveAll(rootPath); err != nil {
		panic(err)
	}
	if err := os.MkdirAll(rootPath, os.ModePerm); err != nil {
		panic(err)
	}
	return &DiskResourceManager{
		ResourceManager: newRM(limit, func(bo walcache.BlockOperator) {
			bo.AsyncEvictDisk()
		}),
		rootPath: rootPath,
	}
}

// DiskResourceManager is a disk resource manager.
type DiskResourceManager struct {
	*ResourceManager
	rootPath string
}

// DiskRM creates a disk resource manager.
func (drm *DiskResourceManager) GetPath(blockID int64) string {
	return fmt.Sprintf("%s/%d.cacheblock", drm.rootPath, blockID)
}

// ResourceManager is the resource manager.
type ResourceManager struct {
	thresholdBytes    int64
	allocatedBytes    int64
	onEvcitedBytes    int64
	onAllocating      int64
	pendingAllocating []allocateReq
	allocateReq       chan allocateReq
	freeReq           chan freeReq
	evictOrder        []walcache.BlockOperator
	// We are only support fifo evictor now.
	// TODO: maybe we need a channel-fair evictor in future.
	evictor func(walcache.BlockOperator)
}

// BlockAndAllocate blocks until the resource is enough.
func (rm *ResourceManager) BlockAndAllocate(r walcache.BlockOperator) {
	resp := make(chan struct{})
	rm.allocateReq <- allocateReq{
		r:    r,
		resp: resp,
	}
	<-resp
}

// Free frees the resource.
func (rm *ResourceManager) Free(bytes int64) {
	rm.freeReq <- freeReq{
		bytes: bytes,
	}
}

// StartEventLoop start the event loop.
func (rm *ResourceManager) StartEventLoop() {
	go func() {
		for {
			rm.eventLoop()
		}
	}()
}

// eventLoop is the main loop of the resource manager.
func (rm *ResourceManager) eventLoop() {
	select {
	case req := <-rm.allocateReq:
		rm.handleAllocate(req)
	case req := <-rm.freeReq:
		rm.handleFree(req)
	}
	rm.consumePendingAllocating()
	rm.consumeUnallocateBytes()
}

// handleAllocate handles the allocate request.
func (rm *ResourceManager) handleAllocate(req allocateReq) {
	rm.pendingAllocating = append(rm.pendingAllocating, req)
	rm.onAllocating += req.r.Bytes()
}

// handleFree handles the free request.
func (rm *ResourceManager) handleFree(req freeReq) {
	rm.allocatedBytes -= req.bytes
	rm.onEvcitedBytes -= req.bytes // The onEvictingBytes may be negative if the flush failed.
	// the block is still located in evictor list, so we don't need to do anything here.
	// After evict that block from evictor list, the onEvictingBytes will be increase to keep 0.
}

// consumeUnallocateBytes consumes all unallocated bytes.
func (rm *ResourceManager) consumeUnallocateBytes() {
	for rm.allocatedBytes+rm.onAllocating-rm.onEvcitedBytes > rm.thresholdBytes && len(rm.evictOrder) > 0 {
		b := rm.evictOrder[0]
		rm.onEvcitedBytes += b.Bytes()
		rm.evictor(b)
		rm.evictOrder[0] = nil
		rm.evictOrder = rm.evictOrder[1:]
	}
}

// consumePendingAllocating consumes all pending allocating requests.
func (rm *ResourceManager) consumePendingAllocating() {
	for len(rm.pendingAllocating) > 0 {
		b := rm.pendingAllocating[0]
		if b.r.Bytes()+rm.allocatedBytes < rm.thresholdBytes {
			rm.allocatedBytes += b.r.Bytes()
			rm.onAllocating -= b.r.Bytes()
			rm.pendingAllocating[0] = allocateReq{}
			rm.pendingAllocating = rm.pendingAllocating[1:]
			rm.evictOrder = append(rm.evictOrder, b.r)
			close(b.resp)
		} else {
			break
		}
	}
}
