// Package statusprobe provides throttled health probing for components whose
// clients have no zero-cost state query (redis, mongodb, nsq ...), so that
// polling ConnectionStatus does not hammer the remote server.
package statusprobe

import (
	"context"
	"sync"
	"time"

	"github.com/rulego/rulego/api/types"
)

const (
	defaultInterval = 5 * time.Second
	defaultTimeout  = time.Second
)

// Throttled 限频探测并缓存结果
type Throttled struct {
	mu       sync.Mutex
	interval time.Duration
	timeout  time.Duration
	last     time.Time
	cached   types.StatusInfo
}

func New() *Throttled {
	return &Throttled{interval: defaultInterval, timeout: defaultTimeout}
}

// Status returns the cached probe result, refreshing it at most once per interval.
func (t *Throttled) Status(probe func(ctx context.Context) error) types.StatusInfo {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.last.IsZero() && time.Since(t.last) < t.interval {
		return t.cached
	}
	ctx, cancel := context.WithTimeout(context.Background(), t.timeout)
	defer cancel()
	if err := probe(ctx); err != nil {
		t.cached = types.StatusInfo{Status: types.StatusReconnecting, Message: err.Error()}
	} else {
		t.cached = types.StatusInfo{Status: types.StatusConnected}
	}
	t.last = time.Now()
	return t.cached
}
