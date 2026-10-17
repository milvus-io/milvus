package requestbudget

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestResolveAbsoluteDeadline(t *testing.T) {
	start := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	p := Policy{OverallTimeoutBudget: 30 * time.Second, ReadHeaderTimeout: 5 * time.Second}
	tests := []struct {
		name, header string
		parent       time.Duration
		want         time.Duration
	}{
		{"server default", "", 0, 30 * time.Second},
		{"shorter client", "10", 0, 10 * time.Second},
		{"longer client capped", "120", 0, 30 * time.Second},
		{"parent earlier", "20", 8 * time.Second, 8 * time.Second},
		{"client budget uses the same post-header start", "4", 0, 4 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parent := context.Background()
			if tt.parent != 0 {
				var cancel context.CancelFunc
				parent, cancel = context.WithDeadline(parent, start.Add(tt.parent))
				defer cancel()
			}
			got, err := p.Resolve(parent, start, tt.header)
			if err != nil {
				t.Fatal(err)
			}
			if want := start.Add(tt.want); !got.Equal(want) {
				t.Fatalf("deadline = %v, want %v", got, want)
			}
		})
	}
}

func TestResolveRejectsInvalidClientSeconds(t *testing.T) {
	p := Policy{OverallTimeoutBudget: 30 * time.Second, ReadHeaderTimeout: 5 * time.Second}
	for _, value := range []string{"0", "-1", "1.5", "abc", "9223372037", "9223372036854775808"} {
		t.Run(value, func(t *testing.T) {
			_, err := p.Resolve(context.Background(), time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC), value)
			if !errors.Is(err, merr.ErrParameterInvalid) {
				t.Fatalf("error = %v, want parameter invalid", err)
			}
			if !strings.Contains(err.Error(), value) {
				t.Fatalf("error = %v, want invalid header value %q", err, value)
			}
		})
	}
}

func TestResolveTruncatesInvalidHeaderEcho(t *testing.T) {
	p := Policy{OverallTimeoutBudget: 30 * time.Second, ReadHeaderTimeout: 5 * time.Second}
	_, err := p.Resolve(context.Background(), time.Now(), strings.Repeat("x", 1024))
	if !errors.Is(err, merr.ErrParameterInvalid) || len(err.Error()) >= 256 {
		t.Fatalf("unbounded invalid header error: %v", err)
	}
}

func TestResolveRejectsInvalidServerPolicy(t *testing.T) {
	for _, p := range []Policy{
		{ReadHeaderTimeout: time.Second},
		{OverallTimeoutBudget: time.Second},
		{OverallTimeoutBudget: time.Second, ReadHeaderTimeout: -time.Second},
		{OverallTimeoutBudget: time.Second, ReadHeaderTimeout: time.Second, MaxConnectionIdleInterval: -time.Second},
	} {
		_, err := p.Resolve(context.Background(), time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC), "1")
		if !errors.Is(err, merr.ErrServiceInternal) {
			t.Fatalf("policy %+v: error = %v, want service internal", p, err)
		}
	}
}

func TestResolveAllowsOverallShorterThanIndependentHeaderGuard(t *testing.T) {
	start := time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC)
	p := Policy{OverallTimeoutBudget: 4 * time.Second, ReadHeaderTimeout: 5 * time.Second}
	got, err := p.Resolve(context.Background(), start, "")
	if err != nil {
		t.Fatal(err)
	}
	if want := start.Add(4 * time.Second); !got.Equal(want) {
		t.Fatalf("deadline = %v, want %v", got, want)
	}
}
