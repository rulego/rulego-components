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
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestLocker(t *testing.T) (*RedisLocker, *miniredis.Miniredis) {
	t.Helper()
	srv := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return NewRedisLocker(client), srv
}

func TestRedisLockerAcquireRelease(t *testing.T) {
	locker, _ := newTestLocker(t)
	ctx := context.Background()

	token, ok, err := locker.TryLock(ctx, "k1", time.Minute)
	if err != nil || !ok || token == "" {
		t.Fatalf("first TryLock should succeed, token=%s ok=%v err=%v", token, ok, err)
	}
	if _, ok, _ := locker.TryLock(ctx, "k1", time.Minute); ok {
		t.Fatal("second TryLock on a held key should fail")
	}
	if err := locker.Unlock(ctx, "k1", "wrong-token"); err == nil {
		t.Fatal("Unlock with a wrong token should fail")
	}
	if err := locker.Unlock(ctx, "k1", token); err != nil {
		t.Fatalf("Unlock with the holding token should succeed: %v", err)
	}
	if token2, ok, _ := locker.TryLock(ctx, "k1", time.Minute); !ok || token2 == token {
		t.Fatal("key should be re-acquirable with a new token after release")
	}
}

func TestRedisLockerExpiration(t *testing.T) {
	locker, srv := newTestLocker(t)
	ctx := context.Background()

	if _, ok, _ := locker.TryLock(ctx, "k1", 100*time.Millisecond); !ok {
		t.Fatal("initial TryLock should succeed")
	}
	srv.FastForward(150 * time.Millisecond)
	if _, ok, _ := locker.TryLock(ctx, "k1", time.Minute); !ok {
		t.Fatal("expired lock should be acquirable by the next holder")
	}
}

func TestRedisLockerLockWithRetry(t *testing.T) {
	locker, _ := newTestLocker(t)
	ctx := context.Background()

	if _, err := locker.LockWithRetry(ctx, "free", time.Minute, time.Millisecond, 0); err != nil {
		t.Fatalf("single attempt on a free key should succeed: %v", err)
	}
	if _, err := locker.LockWithRetry(ctx, "free", time.Minute, 5*time.Millisecond, 2); err == nil {
		t.Fatal("LockWithRetry on a held key should fail after retries are exhausted")
	}
}

func TestRedisLockerConcurrent(t *testing.T) {
	locker, _ := newTestLocker(t)
	ctx := context.Background()

	const goroutines = 20
	var success = make(chan string, goroutines)
	done := make(chan struct{})
	for i := 0; i < goroutines; i++ {
		go func() {
			if token, ok, _ := locker.TryLock(ctx, "same-key", time.Minute); ok {
				success <- token
			}
			done <- struct{}{}
		}()
	}
	for i := 0; i < goroutines; i++ {
		<-done
	}
	close(success)
	winners := 0
	for range success {
		winners++
	}
	if winners != 1 {
		t.Fatalf("exactly one goroutine should win, got %d", winners)
	}
}

func TestRedisLockerRenew(t *testing.T) {
	locker, srv := newTestLocker(t)
	ctx := context.Background()

	token, ok, err := locker.TryLock(ctx, "k1", 200*time.Millisecond)
	if err != nil || !ok {
		t.Fatalf("TryLock: %v %v", ok, err)
	}
	if ok, err := locker.Renew(ctx, "k1", token, time.Minute); err != nil || !ok {
		t.Fatalf("Renew own lock: %v %v", ok, err)
	}
	srv.FastForward(300 * time.Millisecond)
	if _, ok, _ := locker.TryLock(ctx, "k1", time.Minute); ok {
		t.Fatal("lock should still be held after renew extended the TTL")
	}
	if ok, err := locker.Renew(ctx, "k1", "wrong-token", time.Minute); ok || err != nil {
		t.Fatalf("Renew with wrong token should be false, nil: %v %v", ok, err)
	}
}
