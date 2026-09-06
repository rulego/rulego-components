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

// Package locker provides a Redis implementation of the rulego types.Locker
// interface, for hosts and components that need at-most-once semantics across
// replicas, such as deduplicating schedule endpoint ticks.
//
// Package locker 提供 rulego types.Locker 接口的 Redis 实现，
// 供需要跨副本至多一次语义的宿主与组件使用，例如定时端点的跨副本去重。
package locker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/rulego/rulego/api/types"
)

// ErrLockExists 锁已被其他持有者占用（Lock/TryLock 竞争失败时返回）。
var ErrLockExists = errors.New("lock already exists")

// unlockScript 校验凭证后删除，保证不误删其他持有者的锁。
const unlockScript = `if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("del", KEYS[1]) else return 0 end`

// RedisLocker 基于 Redis 的键锁实现（SET NX EX 加锁 + Lua 脚本释放）。
type RedisLocker struct {
	client redis.UniversalClient
}

var _ types.Locker = (*RedisLocker)(nil)

// NewRedisLocker 创建 Redis 键锁。client 接受单机、哨兵与集群客户端。
func NewRedisLocker(client redis.UniversalClient) *RedisLocker {
	return &RedisLocker{client: client}
}

// Lock 单次尝试获取锁，返回持有凭证；锁被其他持有者占用时返回 ErrLockExists。
func (r *RedisLocker) Lock(ctx context.Context, key string, expiration time.Duration) (string, error) {
	value := newLockValue()
	result := r.client.SetNX(ctx, key, value, expiration)
	if err := result.Err(); err != nil {
		return "", fmt.Errorf("failed to acquire lock: %w", err)
	}
	if !result.Val() {
		return "", ErrLockExists
	}
	return value, nil
}

// Unlock 释放锁；凭证不匹配返回错误。
func (r *RedisLocker) Unlock(ctx context.Context, key, token string) error {
	result := r.client.Eval(ctx, unlockScript, []string{key}, token)
	if err := result.Err(); err != nil {
		return fmt.Errorf("failed to release lock: %w", err)
	}
	if result.Val().(int64) == 0 {
		return fmt.Errorf("lock not found or token mismatch: %s", key)
	}
	return nil
}

// TryLock 非阻塞获取；acquired=false 表示锁被占用，不视为错误。
func (r *RedisLocker) TryLock(ctx context.Context, key string, expiration time.Duration) (string, bool, error) {
	value, err := r.Lock(ctx, key, expiration)
	if err != nil {
		if errors.Is(err, ErrLockExists) {
			return "", false, nil
		}
		return "", false, err
	}
	return value, true, nil
}

// LockWithRetry 先立即尝试一次，之后每隔 retryInterval 重试，最多重试 maxRetries 次。
func (r *RedisLocker) LockWithRetry(ctx context.Context, key string, expiration time.Duration, retryInterval time.Duration, maxRetries int) (string, error) {
	for i := 0; i <= maxRetries; i++ {
		value, acquired, err := r.TryLock(ctx, key, expiration)
		if err != nil {
			return "", err
		}
		if acquired {
			return value, nil
		}
		if i < maxRetries {
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-time.After(retryInterval):
			}
		}
	}
	return "", fmt.Errorf("lock %s not acquired after %d retries", key, maxRetries)
}

// newLockValue 生成随机持有凭证。
func newLockValue() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
