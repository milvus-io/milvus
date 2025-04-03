package policy

import (
	"testing"
	"time"
)

func TestPolicyPartitionNotFound(t *testing.T) {
	policy := PolicyParitionNotFound()
	if policy.Policy != PolicyNamePartitionNotFound {
		t.Errorf("expected policy name %s, got %s", PolicyNamePartitionNotFound, policy.Policy)
	}
	if policy.Extra != nil {
		t.Errorf("expected extra to be nil, got %v", policy.Extra)
	}
}

func TestPolicyPartitionRemoved(t *testing.T) {
	policy := PolicyPartitionRemoved()
	if policy.Policy != PolicyNamePartitionRemoved {
		t.Errorf("expected policy name %s, got %s", PolicyNamePartitionRemoved, policy.Policy)
	}
	if policy.Extra != nil {
		t.Errorf("expected extra to be nil, got %v", policy.Extra)
	}
}

func TestPolicyCollectionRemoved(t *testing.T) {
	policy := PolicyCollectionRemoved()
	if policy.Policy != PolicyNameCollectionRemoved {
		t.Errorf("expected policy name %s, got %s", PolicyNameCollectionRemoved, policy.Policy)
	}
	if policy.Extra != nil {
		t.Errorf("expected extra to be nil, got %v", policy.Extra)
	}
}

func TestPolicyRecover(t *testing.T) {
	policy := PolicyRecover()
	if policy.Policy != PolicyNameRecover {
		t.Errorf("expected policy name %s, got %s", PolicyNameRecover, policy.Policy)
	}
	if policy.Extra != nil {
		t.Errorf("expected extra to be nil, got %v", policy.Extra)
	}
}

func TestPolicyFenced(t *testing.T) {
	timetick := uint64(12345)
	policy := PolicyFenced(timetick)
	if policy.Policy != PolicyNameFenced {
		t.Errorf("expected policy name %s, got %s", PolicyNameFenced, policy.Policy)
	}
	extra, ok := policy.Extra.(sealFenced)
	if !ok {
		t.Fatalf("expected extra to be of type sealFenced, got %T", policy.Extra)
	}
	if extra.TimeTick != timetick {
		t.Errorf("expected timetick %d, got %d", timetick, extra.TimeTick)
	}
}

func TestPolicyCapacity(t *testing.T) {
	policy := PolicyCapacity()
	if policy.Policy != PolicyNameCapacity {
		t.Errorf("expected policy name %s, got %s", PolicyNameCapacity, policy.Policy)
	}
	if policy.Extra != nil {
		t.Errorf("expected extra to be nil, got %v", policy.Extra)
	}
}

func TestPolicyBinlogNumber(t *testing.T) {
	binlogNumberLimit := uint64(10)
	policy := PolicyBinlogNumber(binlogNumberLimit)
	if policy.Policy != PolicyNameBinlogNumber {
		t.Errorf("expected policy name %s, got %s", PolicyNameBinlogNumber, policy.Policy)
	}
	extra, ok := policy.Extra.(sealByBinlogFileExtraInfo)
	if !ok {
		t.Fatalf("expected extra to be of type sealByBinlogFileExtraInfo, got %T", policy.Extra)
	}
	if extra.BinLogNumberLimit != binlogNumberLimit {
		t.Errorf("expected binlog number limit %d, got %d", binlogNumberLimit, extra.BinLogNumberLimit)
	}
}

func TestPolicyLifetime(t *testing.T) {
	maxLifetime := 2 * time.Hour
	policy := PolicyLifetime(maxLifetime)
	if policy.Policy != PolicyNameLifetime {
		t.Errorf("expected policy name %s, got %s", PolicyNameLifetime, policy.Policy)
	}
	extra, ok := policy.Extra.(sealByLifetimeExtraInfo)
	if !ok {
		t.Fatalf("expected extra to be of type sealByLifetimeExtraInfo, got %T", policy.Extra)
	}
	if extra.MaxLifetime != maxLifetime {
		t.Errorf("expected max lifetime %v, got %v", maxLifetime, extra.MaxLifetime)
	}
}

func TestPolicyIdle(t *testing.T) {
	idleTime := 30 * time.Minute
	minimalSize := uint64(1024)
	policy := PolicyIdle(idleTime, minimalSize)
	if policy.Policy != PolicyNameIdle {
		t.Errorf("expected policy name %s, got %s", PolicyNameIdle, policy.Policy)
	}
	extra, ok := policy.Extra.(sealByIdleTimeExtraInfo)
	if !ok {
		t.Fatalf("expected extra to be of type sealByIdleTimeExtraInfo, got %T", policy.Extra)
	}
	if extra.IdleTime != idleTime {
		t.Errorf("expected idle time %v, got %v", idleTime, extra.IdleTime)
	}
	if extra.MinimalSize != minimalSize {
		t.Errorf("expected minimal size %d, got %d", minimalSize, extra.MinimalSize)
	}
}

func TestPolicyGrowingSegmentBytesHWM(t *testing.T) {
	totalBytes := uint64(2048)
	policy := PolicyGrowingSegmentBytesHWM(totalBytes)
	if policy.Policy != PolicyNameGrowingSegmentBytesHWM {
		t.Errorf("expected policy name %s, got %s", PolicyNameGrowingSegmentBytesHWM, policy.Policy)
	}
	extra, ok := policy.Extra.(sealByGrowingSegmentBytesHWM)
	if !ok {
		t.Fatalf("expected extra to be of type sealByGrowingSegmentBytesHWM, got %T", policy.Extra)
	}
	if extra.TotalBytes != totalBytes {
		t.Errorf("expected total bytes %d, got %d", totalBytes, extra.TotalBytes)
	}
}

func TestPolicyNodeMemory(t *testing.T) {
	usedRatio := 0.75
	policy := PolicyNodeMemory(usedRatio)
	if policy.Policy != PolicyNameNodeMemory {
		t.Errorf("expected policy name %s, got %s", PolicyNameNodeMemory, policy.Policy)
	}
	extra, ok := policy.Extra.(nodeMemory)
	if !ok {
		t.Fatalf("expected extra to be of type nodeMemory, got %T", policy.Extra)
	}
	if extra.UsedRatio != usedRatio {
		t.Errorf("expected used ratio %f, got %f", usedRatio, extra.UsedRatio)
	}
}
