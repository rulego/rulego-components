/*
 * Copyright 2023 The RuleGo Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package locker

import (
	"context"
	"strings"
	"time"

	"github.com/rulego/rulego/api/types"
)

// prefixedLocker 为底层 Locker 的所有键加统一前缀，
// 多套环境共用同一 Redis 时用前缀隔离锁键。
type prefixedLocker struct {
	inner  types.Locker
	prefix string
}

var _ types.Locker = (*prefixedLocker)(nil)

// NewPrefixedLocker 用 prefix 包装 inner。prefix 末尾不带 ":" 时自动补上。
func NewPrefixedLocker(inner types.Locker, prefix string) types.Locker {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		return inner
	}
	if !strings.HasSuffix(prefix, ":") {
		prefix += ":"
	}
	return &prefixedLocker{inner: inner, prefix: prefix}
}

func (p *prefixedLocker) Lock(ctx context.Context, key string, expiration time.Duration) (string, error) {
	return p.inner.Lock(ctx, p.prefix+key, expiration)
}

func (p *prefixedLocker) Unlock(ctx context.Context, key, token string) error {
	return p.inner.Unlock(ctx, p.prefix+key, token)
}

func (p *prefixedLocker) TryLock(ctx context.Context, key string, expiration time.Duration) (string, bool, error) {
	return p.inner.TryLock(ctx, p.prefix+key, expiration)
}

func (p *prefixedLocker) LockWithRetry(ctx context.Context, key string, expiration time.Duration, retryInterval time.Duration, maxRetries int) (string, error) {
	return p.inner.LockWithRetry(ctx, p.prefix+key, expiration, retryInterval, maxRetries)
}

// Renew 透传 Renew 能力；底层实现不支持续约时返回不支持错误。
func (p *prefixedLocker) Renew(ctx context.Context, key, token string, expiration time.Duration) (bool, error) {
	if renewer, ok := p.inner.(types.LeaseRenewer); ok {
		return renewer.Renew(ctx, p.prefix+key, token, expiration)
	}
	return false, types.ErrLeaseRenewUnsupported
}
