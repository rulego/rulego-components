/*
 * Copyright 2026 The RuleGo Authors.
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

package pythonEngine

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rulego/rulego/api/types"
)

// checkPythonAvailable skips the test if python is not available.
func checkPythonAvailable(t *testing.T) {
	t.Helper()
	if _, err := ResolvePythonPath(""); err != nil {
		t.Skipf("python not available, skipping test: %v", err)
	}
}

func pythonPath() string {
	if p := os.Getenv("PYTHON_PATH"); p != "" {
		return p
	}
	p, _ := ResolvePythonPath("")
	if p != "" {
		return p
	}
	return "python3"
}

// resolveTestdataPath returns an absolute path to a file in the local testdata/ directory.
func resolveTestdataPath(t *testing.T, filename string) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("failed to determine test source file path")
	}
	dir := filepath.Dir(thisFile)
	p := filepath.Join(dir, "testdata", filename)
	absPath, err := filepath.Abs(p)
	if err != nil {
		t.Fatalf("failed to resolve testdata path: %v", err)
	}
	if _, err := os.Stat(absPath); err != nil {
		t.Fatalf("testdata file not found: %s", absPath)
	}
	return absPath
}

// ---------- Unit tests (no subprocess needed) ----------

func TestIndentLines(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		prefix string
		want   string
	}{
		{"Simple", "a\nb", "  ", "  a\n  b"},
		{"Empty line preserved", "a\n\nb", "  ", "  a\n\n  b"},
		{"Single line", "hello", "    ", "    hello"},
		{"Empty input", "", "  ", ""},
		{"Only whitespace line", "a\n  \nb", "  ", "  a\n  \n  b"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := indentLines(tt.input, tt.prefix)
			if got != tt.want {
				t.Errorf("indentLines(%q, %q) = %q, want %q", tt.input, tt.prefix, got, tt.want)
			}
		})
	}
}

func TestBuildInlineScript(t *testing.T) {
	script := buildInlineScript("Process", "return msg")
	if !strings.Contains(script, "def Process(") {
		t.Error("script should contain function definition")
	}
	if !strings.Contains(script, "_call_func = Process") {
		t.Error("script should set _call_func")
	}
	if !strings.Contains(script, "import inspect") {
		t.Error("script should import inspect at top level")
	}
	if !strings.Contains(script, "def main():") {
		t.Error("script should contain main()")
	}
	// User code should be indented inside the function
	if !strings.Contains(script, "    return msg") {
		t.Errorf("user code should be indented, got:\n%s", script)
	}
}

func TestBuildFileScript(t *testing.T) {
	script := buildFileScript("/path/to/script.py", "Process")
	if !strings.Contains(script, `spec_from_file_location("_user_module",`) {
		t.Error("script should load module via importlib")
	}
	if !strings.Contains(script, `_call_func = getattr(mod, "Process")`) {
		t.Error("script should set _call_func via getattr")
	}
	if !strings.Contains(script, "import inspect") {
		t.Error("script should import inspect at top level")
	}
}

func TestTryParseJSON(t *testing.T) {
	tests := []struct {
		name  string
		input string
		check func(t *testing.T, result interface{})
	}{
		{
			"Valid JSON object",
			`{"key":"value"}`,
			func(t *testing.T, result interface{}) {
				m, ok := result.(map[string]interface{})
				if !ok {
					t.Fatalf("expected map, got %T", result)
				}
				if m["key"] != "value" {
					t.Errorf("expected value, got %v", m["key"])
				}
			},
		},
		{
			"Valid JSON array",
			`[1,2,3]`,
			func(t *testing.T, result interface{}) {
				arr, ok := result.([]interface{})
				if !ok {
					t.Fatalf("expected slice, got %T", result)
				}
				if len(arr) != 3 {
					t.Errorf("expected len 3, got %d", len(arr))
				}
			},
		},
		{
			"Valid JSON number",
			`42`,
			func(t *testing.T, result interface{}) {
				if result != float64(42) {
					t.Errorf("expected 42.0, got %v", result)
				}
			},
		},
		{
			"Invalid JSON returns string",
			`not json`,
			func(t *testing.T, result interface{}) {
				s, ok := result.(string)
				if !ok {
					t.Fatalf("expected string, got %T", result)
				}
				if s != "not json" {
					t.Errorf("expected original string, got %q", s)
				}
			},
		},
		{
			"Empty string",
			``,
			func(t *testing.T, result interface{}) {
				s, ok := result.(string)
				if !ok || s != "" {
					t.Errorf("expected empty string, got %v", result)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tryParseJSON(tt.input)
			tt.check(t, result)
		})
	}
}

func TestScriptContentCached(t *testing.T) {
	pool := NewStringProcessPool(types.NewConfig(), "Process", "return msg", pythonPath(), 0, 10, nil)
	if pool.scriptContent == "" {
		t.Error("scriptContent should be pre-built at construction time")
	}
	if !strings.Contains(pool.scriptContent, "def Process(") {
		t.Error("cached script should contain function definition")
	}
}

// ---------- Integration tests (require python) ----------

func TestExecute_PassThrough(t *testing.T) {
	checkPythonAvailable(t)
	pool := NewStringProcessPool(types.NewConfig(), "Process", "return msg, metadata, msgType", pythonPath(), 5*time.Second, 10, nil)
	defer pool.Shutdown()

	result, err := pool.Execute(`{"temperature":35}`, map[string]string{"device": "sensor01"}, "TEST", "JSON")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result["msg"] == nil {
		t.Error("expected msg in result")
	}
	if result["msgType"] != "TEST" {
		t.Errorf("expected msgType=TEST, got %v", result["msgType"])
	}
}

func TestExecute_SingleValue(t *testing.T) {
	checkPythonAvailable(t)
	pool := NewStringProcessPool(types.NewConfig(), "Process", `return "hello world"`, pythonPath(), 5*time.Second, 10, nil)
	defer pool.Shutdown()

	result, err := pool.Execute("test", nil, "TEST", "TEXT")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result["msg"] != "hello world" {
		t.Errorf("expected msg='hello world', got %v", result["msg"])
	}
}

func TestExecute_TupleReturn(t *testing.T) {
	checkPythonAvailable(t)
	script := `return "newMsg", {"k": "v"}, "NEW_TYPE"`
	pool := NewStringProcessPool(types.NewConfig(), "Process", script, pythonPath(), 5*time.Second, 10, nil)
	defer pool.Shutdown()

	result, err := pool.Execute("test", nil, "TEST", "TEXT")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result["msg"] != "newMsg" {
		t.Errorf("expected msg='newMsg', got %v", result["msg"])
	}
	if result["msgType"] != "NEW_TYPE" {
		t.Errorf("expected msgType='NEW_TYPE', got %v", result["msgType"])
	}
	meta, ok := result["metadata"].(map[string]interface{})
	if !ok || meta["k"] != "v" {
		t.Errorf("expected metadata with k=v, got %v", result["metadata"])
	}
}

func TestExecute_DictReturn(t *testing.T) {
	checkPythonAvailable(t)
	script := `return {"msg": "dictMsg", "msgType": "DICT"}`
	pool := NewStringProcessPool(types.NewConfig(), "Process", script, pythonPath(), 5*time.Second, 10, nil)
	defer pool.Shutdown()

	result, err := pool.Execute("test", nil, "TEST", "TEXT")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result["msg"] != "dictMsg" {
		t.Errorf("expected msg='dictMsg', got %v", result["msg"])
	}
	if result["msgType"] != "DICT" {
		t.Errorf("expected msgType='DICT', got %v", result["msgType"])
	}
}

func TestExecute_DictWithoutKeys(t *testing.T) {
	checkPythonAvailable(t)
	// dict without msg/metadata/msgType keys should be wrapped as {"msg": result}
	script := `return {"data": 123}`
	pool := NewStringProcessPool(types.NewConfig(), "Process", script, pythonPath(), 5*time.Second, 10, nil)
	defer pool.Shutdown()

	result, err := pool.Execute("test", nil, "TEST", "TEXT")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	inner, ok := result["msg"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected msg to be a map, got %T", result["msg"])
	}
	if inner["data"] != float64(123) {
		t.Errorf("expected inner data=123, got %v", inner["data"])
	}
}

func TestExecute_ScriptError(t *testing.T) {
	checkPythonAvailable(t)
	pool := NewStringProcessPool(types.NewConfig(), "Process", `raise ValueError("boom")`, pythonPath(), 5*time.Second, 10, nil)
	defer pool.Shutdown()

	_, err := pool.Execute("test", nil, "TEST", "TEXT")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "boom") {
		t.Errorf("error should contain 'boom', got: %v", err)
	}
}

func TestExecute_SyntaxError(t *testing.T) {
	checkPythonAvailable(t)
	pool := NewStringProcessPool(types.NewConfig(), "Process", `this is not valid python !!!`, pythonPath(), 5*time.Second, 10, nil)
	defer pool.Shutdown()

	_, err := pool.Execute("test", nil, "TEST", "TEXT")
	if err == nil {
		t.Fatal("expected error for syntax error, got nil")
	}
}

func TestExecute_Timeout(t *testing.T) {
	checkPythonAvailable(t)
	pool := NewStringProcessPool(types.NewConfig(), "Process", `import time; time.sleep(10); return msg`, pythonPath(), 500*time.Millisecond, 10, nil)
	defer pool.Shutdown()

	_, err := pool.Execute("test", nil, "TEST", "TEXT")
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
	if !strings.Contains(err.Error(), "timed out") {
		t.Errorf("expected timeout error, got: %v", err)
	}
}

func TestExecute_InvalidPythonPath(t *testing.T) {
	pool := NewStringProcessPool(types.NewConfig(), "Process", "return msg", "/nonexistent/python3", 5*time.Second, 10, nil)
	defer pool.Shutdown()

	_, err := pool.Execute("test", nil, "TEST", "TEXT")
	if err == nil {
		t.Fatal("expected error for invalid python path, got nil")
	}
}

func TestExecute_WithVars(t *testing.T) {
	checkPythonAvailable(t)
	script := `return vars.get("host", "missing")`
	configuration := types.Configuration{
		types.Vars: map[string]string{
			"host": "192.168.1.1",
		},
	}
	pool := NewStringProcessPool(types.NewConfig(), "Process", script, pythonPath(), 5*time.Second, 10, configuration)
	defer pool.Shutdown()

	result, err := pool.Execute("test", nil, "TEST", "TEXT")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result["msg"] != "192.168.1.1" {
		t.Errorf("expected msg='192.168.1.1', got %v", result["msg"])
	}
}

func TestExecute_WithGlobalProps(t *testing.T) {
	checkPythonAvailable(t)
	script := `return globalProps.get("env", "missing")`
	config := types.NewConfig()
	config.Properties = types.Properties{
		"env": "production",
	}
	pool := NewStringProcessPool(config, "Process", script, pythonPath(), 5*time.Second, 10, nil)
	defer pool.Shutdown()

	result, err := pool.Execute("test", nil, "TEST", "TEXT")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result["msg"] != "production" {
		t.Errorf("expected msg='production', got %v", result["msg"])
	}
}

func TestExecute_6ParamFunction(t *testing.T) {
	checkPythonAvailable(t)
	script := `return vars.get("k", "none") + "_" + globalProps.get("g", "none")`
	configuration := types.Configuration{
		types.Vars: map[string]string{
			"k": "v1",
		},
	}
	config := types.NewConfig()
	config.Properties = types.Properties{
		"g": "g1",
	}
	pool := NewStringProcessPool(config, "Process", script, pythonPath(), 5*time.Second, 10, configuration)
	defer pool.Shutdown()

	result, err := pool.Execute("test", nil, "TEST", "TEXT")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result["msg"] != "v1_g1" {
		t.Errorf("expected msg='v1_g1', got %v", result["msg"])
	}
}

func TestExecute_FileMode(t *testing.T) {
	checkPythonAvailable(t)
	testdataPath := resolveTestdataPath(t, "process_basic.py")
	pool := NewFileProcessPool(types.NewConfig(), "Process", testdataPath, pythonPath(), 5*time.Second, 10, nil)
	defer pool.Shutdown()

	result, err := pool.Execute(`{"temperature":35}`, map[string]string{}, "TEST", "JSON")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// process_basic.py parses JSON, adds "processed": true, returns as tuple
	msg, ok := result["msg"].(string)
	if !ok {
		t.Fatalf("expected msg to be string, got %T", result["msg"])
	}
	if !strings.Contains(msg, "processed") {
		t.Errorf("expected processed flag in msg, got: %s", msg)
	}
}

func TestExecute_FileModeSingleValue(t *testing.T) {
	checkPythonAvailable(t)
	testdataPath := resolveTestdataPath(t, "process_single_value.py")
	pool := NewFileProcessPool(types.NewConfig(), "Process", testdataPath, pythonPath(), 5*time.Second, 10, nil)
	defer pool.Shutdown()

	result, err := pool.Execute("test", map[string]string{}, "TEST", "TEXT")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result["msg"] != "single value result" {
		t.Errorf("expected msg='single value result', got %v", result["msg"])
	}
}

func TestExecute_Concurrent(t *testing.T) {
	checkPythonAvailable(t)
	pool := NewStringProcessPool(types.NewConfig(), "Process", "return msg, metadata, msgType", pythonPath(), 10*time.Second, 3, nil)
	defer pool.Shutdown()

	var wg sync.WaitGroup
	errCh := make(chan error, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := pool.Execute(`{"i":%d}`, nil, "TEST", "JSON")
			if err != nil {
				errCh <- err
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Errorf("concurrent execution failed: %v", err)
	}
}

func TestResolvePythonPath(t *testing.T) {
	t.Run("ExplicitValidPath", func(t *testing.T) {
		checkPythonAvailable(t)
		pp := pythonPath()
		resolved, err := ResolvePythonPath(pp)
		if err != nil {
			t.Errorf("expected resolved path, got error: %v", err)
		}
		if resolved != pp {
			t.Errorf("expected %q, got %q", pp, resolved)
		}
	})

	t.Run("EmptyAutoResolve", func(t *testing.T) {
		checkPythonAvailable(t)
		resolved, err := ResolvePythonPath("")
		if err != nil {
			t.Errorf("expected auto-resolve to work, got error: %v", err)
		}
		if resolved == "" {
			t.Error("expected non-empty resolved path")
		}
	})

	t.Run("InvalidPath", func(t *testing.T) {
		_, err := ResolvePythonPath("/nonexistent/python999")
		if err == nil {
			t.Error("expected error for invalid path, got nil")
		}
	})
}

func TestValidatePython(t *testing.T) {
	t.Run("ValidPath", func(t *testing.T) {
		checkPythonAvailable(t)
		if err := ValidatePython(pythonPath()); err != nil {
			t.Errorf("expected valid, got: %v", err)
		}
	})

	t.Run("InvalidPath", func(t *testing.T) {
		if err := ValidatePython("/nonexistent/python999"); err == nil {
			t.Error("expected error for invalid python path")
		}
	})
}

// TestExecute_SemaphoreTimeout tests that Execute returns an error when no
// ready worker is available within the configured timeout.
func TestExecute_SemaphoreTimeout(t *testing.T) {
	checkPythonAvailable(t)
	shortPool := NewStringProcessPool(types.NewConfig(), "Process",
		`return msg`,
		pythonPath(), 500*time.Millisecond, 1, nil)
	defer shortPool.Shutdown()

	// Drain the pre-warmed worker from ready channel directly (bypassing Execute).
	// Since we don't signal demand, warmLoop won't produce a replacement — ready stays empty.
	w := <-shortPool.ready
	defer w.kill()

	// Now Execute should timeout waiting for a ready worker.
	done := make(chan error, 1)
	go func() {
		_, err := shortPool.Execute("second", nil, "TEST", "TEXT")
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected timeout error when waiting for worker slot, got nil")
		}
		if !strings.Contains(err.Error(), "concurrency limit reached") {
			t.Errorf("expected concurrency limit error, got: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("test timed out waiting for Execute to return")
	}
}

// TestReapIdleShrinksToKeepAlive verifies that reapIdle shrinks the ready pool
// down to the keepAlive floor in a single pass and never below it.
func TestReapIdleShrinksToKeepAlive(t *testing.T) {
	checkPythonAvailable(t)
	const maxRunning = 6 // keepAlive = max(1, maxRunning/3) = 2
	pool := NewStringProcessPool(types.NewConfig(), "Process", "return msg", pythonPath(), 5*time.Second, maxRunning, nil)
	defer pool.Shutdown()

	// Wait for warm-up: ready fills to maxRunning.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if len(pool.ready) >= maxRunning {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := len(pool.ready); got < maxRunning {
		t.Fatalf("warm-up did not fill ready: got %d workers", got)
	}

	// A single reap should drop to keepAlive=2 in one pass.
	pool.reapIdle()
	if got := len(pool.ready); got != 2 {
		t.Errorf("expected ready shrunk to keepAlive=2, got %d", got)
	}
	// Reaping again must not drop below keepAlive.
	pool.reapIdle()
	if got := len(pool.ready); got != 2 {
		t.Errorf("expected ready to stay at keepAlive=2, got %d", got)
	}
}

// TestShutdown_WaitsForInFlight tests that Shutdown blocks until all executions complete.
func TestShutdown_WaitsForInFlight(t *testing.T) {
	checkPythonAvailable(t)
	pool := NewStringProcessPool(types.NewConfig(), "Process",
		`import time; time.sleep(1); return msg`,
		pythonPath(), 10*time.Second, 5, nil)

	// Start several executions
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pool.Execute("test", nil, "TEST", "TEXT")
		}()
	}

	// Give time for all goroutines to start executing
	time.Sleep(200 * time.Millisecond)

	// Shutdown should block until all 3 executions finish
	shutdownDone := make(chan struct{})
	go func() {
		pool.Shutdown()
		close(shutdownDone)
	}()

	select {
	case <-shutdownDone:
		// Shutdown completed — all in-flight executions finished
	case <-time.After(10 * time.Second):
		t.Fatal("Shutdown did not complete within expected time")
	}

	wg.Wait()
}

// TestShutdown_UnblocksWaitingExecute tests that Shutdown unblocks Execute calls
// that are waiting for a ready worker, instead of deadlocking.
func TestShutdown_UnblocksWaitingExecute(t *testing.T) {
	checkPythonAvailable(t)
	pool := NewStringProcessPool(types.NewConfig(), "Process",
		`return msg`,
		pythonPath(), 0, 1, nil) // no timeout, maxRunning=1
	defer pool.Shutdown()

	// Drain the pre-warmed worker from ready channel directly (bypassing Execute).
	// Since we don't signal demand, warmLoop won't produce a replacement — ready stays empty.
	w := <-pool.ready
	defer w.kill()

	// Start an Execute that will block waiting for a worker (no timeout, ready is empty).
	executeDone := make(chan error, 1)
	go func() {
		_, err := pool.Execute("test", nil, "TEST", "TEXT")
		executeDone <- err
	}()

	// Give the Execute goroutine time to block at <-p.ready.
	time.Sleep(200 * time.Millisecond)

	// Shutdown should NOT deadlock — it should unblock the waiting Execute via p.done.
	shutdownDone := make(chan struct{})
	go func() {
		pool.Shutdown()
		close(shutdownDone)
	}()

	select {
	case <-shutdownDone:
		// Shutdown completed successfully.
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown deadlocked — waiting Execute was not unblocked")
	}

	// The blocked Execute should have returned with a shutdown error.
	select {
	case err := <-executeDone:
		if err == nil {
			t.Fatal("expected shutdown error from blocked Execute")
		}
		if !strings.Contains(err.Error(), "shutting down") {
			t.Errorf("expected shutting down error, got: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("blocked Execute did not return after Shutdown")
	}
}
