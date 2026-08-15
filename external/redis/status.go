package redis

import (
	"context"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rulego/rulego/api/types"
)

// pingProbe 限频探测并缓存结果，避免前端轮询状态时高频 Ping 服务器
type pingProbe struct {
	mu       sync.Mutex
	interval time.Duration
	last     time.Time
	cached   types.StatusInfo
}

func newPingProbe() *pingProbe {
	return &pingProbe{interval: 5 * time.Second}
}

func (p *pingProbe) status(client *redis.Client) types.StatusInfo {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.last.IsZero() && time.Since(p.last) < p.interval {
		return p.cached
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		p.cached = types.StatusInfo{Status: types.StatusReconnecting, Message: err.Error()}
	} else {
		p.cached = types.StatusInfo{Status: types.StatusConnected}
	}
	p.last = time.Now()
	return p.cached
}

// ConnectionStatus reports the live server state of the redis client.
func (x *ClientNode) ConnectionStatus() types.StatusInfo {
	if client, ok := x.SharedNode.Instance(); ok {
		return x.probe.status(client)
	}
	return x.SharedNode.ConnectionStatus()
}

// ConnectionStatus reports the live server state of the redis client.
func (x *PublisherNode) ConnectionStatus() types.StatusInfo {
	if client, ok := x.SharedNode.Instance(); ok {
		return x.probe.status(client)
	}
	return x.SharedNode.ConnectionStatus()
}
