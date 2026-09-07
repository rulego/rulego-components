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
)

func TestNewPrefixedLockerAddsColon(t *testing.T) {
	inner, _ := newTestLocker(t)
	if NewPrefixedLocker(inner, "") != inner {
		t.Fatal("empty prefix should return inner unchanged")
	}
	p := NewPrefixedLocker(inner, "prod1").(*prefixedLocker)
	if p.prefix != "prod1:" {
		t.Fatalf("prefix = %q, want prod1:", p.prefix)
	}
}

func TestPrefixedLockerKeysUsePrefix(t *testing.T) {
	inner, _ := newTestLocker(t)
	p := NewPrefixedLocker(inner, "env1")
	ctx := context.Background()

	token, ok, err := p.TryLock(ctx, "rulego:once:s", time.Minute)
	if err != nil || !ok {
		t.Fatalf("try lock: %v %v", ok, err)
	}
	// 原锁未加前缀键不受影响
	if _, ok, _ := inner.TryLock(ctx, "rulego:once:s", time.Minute); !ok {
		t.Fatal("inner key should be free; prefix not applied")
	}
	// 前缀键被占
	if _, ok, _ := inner.TryLock(ctx, "env1:rulego:once:s", time.Minute); ok {
		t.Fatal("prefixed key should be held")
	}
	if err := p.Unlock(ctx, "rulego:once:s", token); err != nil {
		t.Fatalf("unlock: %v", err)
	}
}

func TestPrefixedLockerRenew(t *testing.T) {
	inner, _ := newTestLocker(t)
	p := NewPrefixedLocker(inner, "env1").(*prefixedLocker)
	ctx := context.Background()

	token, ok, err := p.TryLock(ctx, "k", time.Minute)
	if err != nil || !ok {
		t.Fatalf("try lock: %v %v", ok, err)
	}
	if ok, err := p.Renew(ctx, "k", token, time.Minute); err != nil || !ok {
		t.Fatalf("renew through prefix wrapper: %v %v", ok, err)
	}
}
