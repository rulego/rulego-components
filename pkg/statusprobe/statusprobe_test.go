package statusprobe

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/test/assert"
)

func newTestProbe(interval time.Duration) *Throttled {
	return &Throttled{interval: interval, timeout: time.Second}
}

func TestStatusProbeConnected(t *testing.T) {
	p := newTestProbe(time.Minute)
	calls := 0
	info := p.Status(func(ctx context.Context) error {
		calls++
		return nil
	})
	assert.Equal(t, types.StatusConnected, info.Status)
	assert.Equal(t, 1, calls)
}

func TestStatusProbeThrottled(t *testing.T) {
	p := newTestProbe(50 * time.Millisecond)
	calls := 0
	probe := func(ctx context.Context) error {
		calls++
		return nil
	}
	p.Status(probe)
	p.Status(probe)
	p.Status(probe)
	assert.Equal(t, 1, calls, "probe should be cached within interval")

	time.Sleep(60 * time.Millisecond)
	info := p.Status(probe)
	assert.Equal(t, 2, calls, "probe should refresh after interval")
	assert.Equal(t, types.StatusConnected, info.Status)
}

func TestStatusProbeErrorCached(t *testing.T) {
	p := newTestProbe(time.Minute)
	info := p.Status(func(ctx context.Context) error {
		return errors.New("dial tcp: connection refused")
	})
	assert.Equal(t, types.StatusReconnecting, info.Status)
	assert.Equal(t, "dial tcp: connection refused", info.Message)

	// interval 内复用缓存，probe 不再执行
	calls := 0
	info = p.Status(func(ctx context.Context) error {
		calls++
		return nil
	})
	assert.Equal(t, 0, calls)
	assert.Equal(t, types.StatusReconnecting, info.Status)
	assert.Equal(t, "dial tcp: connection refused", info.Message)
}

func TestStatusProbeRecovers(t *testing.T) {
	p := newTestProbe(10 * time.Millisecond)
	fail := true
	probe := func(ctx context.Context) error {
		if fail {
			return errors.New("server down")
		}
		return nil
	}
	info := p.Status(probe)
	assert.Equal(t, types.StatusReconnecting, info.Status)

	time.Sleep(15 * time.Millisecond)
	fail = false
	info = p.Status(probe)
	assert.Equal(t, types.StatusConnected, info.Status)
}
