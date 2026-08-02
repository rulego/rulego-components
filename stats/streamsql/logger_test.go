/*
 * Copyright 2025 The RuleGo Authors.
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

package streamsql

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rulego/rulego/test/assert"
	"github.com/rulego/streamsql"
)

// captureRulegoLogger 实现 types.Logger，按级别记录格式化后的消息，便于断言。
type captureRulegoLogger struct {
	mu    sync.Mutex
	debug []string
	info  []string
	warn  []string
	errs  []string
}

func (c *captureRulegoLogger) Printf(format string, v ...interface{}) { c.Infof(format, v...) }
func (c *captureRulegoLogger) Debugf(format string, v ...interface{}) {
	c.mu.Lock()
	c.debug = append(c.debug, fmt.Sprintf(format, v...))
	c.mu.Unlock()
}
func (c *captureRulegoLogger) Infof(format string, v ...interface{}) {
	c.mu.Lock()
	c.info = append(c.info, fmt.Sprintf(format, v...))
	c.mu.Unlock()
}
func (c *captureRulegoLogger) Warnf(format string, v ...interface{}) {
	c.mu.Lock()
	c.warn = append(c.warn, fmt.Sprintf(format, v...))
	c.mu.Unlock()
}
func (c *captureRulegoLogger) Errorf(format string, v ...interface{}) {
	c.mu.Lock()
	c.errs = append(c.errs, fmt.Sprintf(format, v...))
	c.mu.Unlock()
}

// TestRulegoLoggerAdapter 验证适配器把 streamsql 的 Debug/Info/Warn/Error 委派给 rulego logger。
func TestRulegoLoggerAdapter(t *testing.T) {
	cap := &captureRulegoLogger{}
	a := &rulegoLogger{l: cap}
	a.Debug("d %s", "x")
	a.Info("i %d", 1)
	a.Warn("w")
	a.Error("e %v", 2)

	assert.Equal(t, 1, len(cap.debug))
	assert.Equal(t, "d x", cap.debug[0])
	assert.Equal(t, "i 1", cap.info[0])
	assert.Equal(t, "w", cap.warn[0])
	assert.Equal(t, "e 2", cap.errs[0])
}

// TestNewRulegoLogger_NilFallback 验证 nil 回退到默认 logger 不 panic。
func TestNewRulegoLogger_NilFallback(t *testing.T) {
	l := newRulegoLogger(nil)
	assert.NotNil(t, l)
	// 调一下不 panic 即可
	l.Info("init")
}

// TestNodeLoggerWiring_RoutesViaWithLogger 验证 WithLogger 按实例注入后，引擎内部
// 日志会路由到 rulego logger（适配器）。WithLogger 是按实例的，不走包级全局 logger，
// 故用异步路径触发一条内部日志：JOIN 查询但表未注册 → enrichJoin 报错 → s.log.Error。
func TestNodeLoggerWiring_RoutesViaWithLogger(t *testing.T) {
	cap := &captureRulegoLogger{}
	ssql := streamsql.New(streamsql.WithLogger(newRulegoLogger(cap)))
	defer ssql.Stop()

	if err := ssql.Execute("SELECT m.x FROM stream JOIN meta m ON id = m.id"); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	ssql.Emit(map[string]interface{}{"id": 1}) // 异步 → enrichJoin 报错 → s.log.Error

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && len(cap.errs) == 0 {
		time.Sleep(20 * time.Millisecond)
	}

	cap.mu.Lock()
	defer cap.mu.Unlock()
	if len(cap.errs) == 0 {
		t.Fatalf("期望引擎内部错误日志路由到 rulego logger，实际无")
	}
	assert.True(t, strings.Contains(cap.errs[0], "join"), "日志应是 join enrichment error, got %s", cap.errs[0])
}
