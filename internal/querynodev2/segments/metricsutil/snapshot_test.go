package metricsutil

import "testing"

func TestCacheObserverSnapshot(t *testing.T) {
	snapshot := CacheObserverSnapshot{}
	snapshot.Add(CacheObserverSnapshot{
		LoadTotal:    1,
		LoadDuration: 1,
	})
	if snapshot.LoadTotal != 1 || snapshot.LoadDuration != 1 {
		t.Error("cacheObserverSnapshot Add failed")
	}
}

func TestAccessObserverSnapshot(t *testing.T) {
	snapshot := AccessObserverSnapshot{}
	snapshot.Add(AccessObserverSnapshot{
		Total:            1,
		Duration:         1,
		WaitLoadTotal:    1,
		WaitLoadDuration: 1,
	})
	if snapshot.Total != 1 || snapshot.Duration != 1 || snapshot.WaitLoadTotal != 1 || snapshot.WaitLoadDuration != 1 {
		t.Error("accessObserverSnapshot Add failed")
	}
}
