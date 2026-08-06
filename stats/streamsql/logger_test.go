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

// captureRulegoLogger implements types.Logger, recording formatted messages per level for assertion.
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

// errCount reads the error log count under lock, for tests to poll while waiting for async writes from background goroutines.
func (c *captureRulegoLogger) errCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.errs)
}

// TestRulegoLoggerAdapter verifies the adapter delegates streamsql's Debug/Info/Warn/Error to the rulego logger.
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

// TestNewRulegoLogger_NilFallback verifies that nil falls back to the default logger without panic.
func TestNewRulegoLogger_NilFallback(t *testing.T) {
	l := newRulegoLogger(nil)
	assert.NotNil(t, l)
	// Just call it once to ensure no panic
	l.Info("init")
}

// TestNodeLoggerWiring_RoutesViaWithLogger verifies that after WithLogger is injected per instance,
// engine internal logs are routed to the rulego logger (adapter). WithLogger is per instance and does not
// use the package-level global logger, so an async path is used to trigger an internal log:
// a JOIN query with an unregistered table -> enrichJoin errors -> s.log.Error.
func TestNodeLoggerWiring_RoutesViaWithLogger(t *testing.T) {
	cap := &captureRulegoLogger{}
	ssql := streamsql.New(streamsql.WithLogger(newRulegoLogger(cap)))
	defer ssql.Stop()

	if err := ssql.Execute("SELECT m.x FROM stream JOIN meta m ON id = m.id"); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	ssql.Emit(map[string]interface{}{"id": 1}) // async -> enrichJoin errors -> s.log.Error

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && cap.errCount() == 0 {
		time.Sleep(20 * time.Millisecond)
	}

	cap.mu.Lock()
	defer cap.mu.Unlock()
	if len(cap.errs) == 0 {
		t.Fatalf("expected engine internal error logs to be routed to rulego logger, got none")
	}
	assert.True(t, strings.Contains(cap.errs[0], "join"), "log should be a join enrichment error, got %s", cap.errs[0])
}
