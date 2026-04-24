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
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/components/base"
)

// DefaultPythonPath is the default python3 executable path
const DefaultPythonPath = "python3"

const (
	// componentPrefix is used in error messages
	componentPrefix = "x/python"

	// defaultIdleTimeout is how long an idle worker sits in the pool before being killed
	defaultIdleTimeout = 30 * time.Second
	// initialBackoff is the initial retry delay when starting a worker fails
	initialBackoff = 100 * time.Millisecond
	// maxBackoff is the maximum retry delay when starting a worker fails
	maxBackoff = 10 * time.Second

	// input data keys (protocol shared with Python script)
	inputKeyMsg      = "msg"
	inputKeyMetadata = "metadata"
	inputKeyMsgType  = "msgType"
	inputKeyDataType = "dataType"
	inputKeyVars     = "vars"
	inputKeyGlobal   = "global"

	// result key
	resultKeyError = "error"
)

// pythonCandidates is a list of python executable names to try, in order
var pythonCandidates = []string{"python3", "python"}

// ResolvePythonPath resolves the python executable path.
// If pythonPath is empty, it tries "python3" first, then "python".
func ResolvePythonPath(pythonPath string) (string, error) {
	if pythonPath != "" {
		if err := ValidatePython(pythonPath); err != nil {
			return "", err
		}
		return pythonPath, nil
	}
	for _, candidate := range pythonCandidates {
		if err := ValidatePython(candidate); err == nil {
			return candidate, nil
		}
	}
	return "", fmt.Errorf("no python executable found (tried: %v)", pythonCandidates)
}

// worker represents a pre-started Python process that is blocking at sys.stdin.read(),
// waiting for input. Once stdin is written and closed, the process reads input,
// executes the user function, writes JSON to stdout, and exits.
type worker struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout *bytes.Buffer
	stderr *bytes.Buffer
}

// kill closes the worker's stdin and kills the process.
func (w *worker) kill() {
	w.stdin.Close()
	w.cmd.Process.Kill()
	w.cmd.Wait()
}

// ProcessPool manages python3 subprocess invocations with concurrency control and timeout.
// It pre-starts Python processes that block at sys.stdin.read(), eliminating startup
// overhead from the request path. Each request consumes one worker (process exits after
// handling one request), and a replacement is started in the background.
type ProcessPool struct {
	config        types.Config
	pythonPath    string
	timeout       time.Duration
	maxRunning    int
	vars          map[string]interface{}
	scriptContent string // pre-built complete Python script

	ready       chan *worker    // pre-warmed workers (buffered to maxRunning)
	demand      chan struct{}   // signals warmLoop to produce a worker (buffered to maxRunning)
	done        chan struct{}   // closed when pool is shutting down
	stop        chan struct{}   // signals warmLoop/reaper to stop
	idleTimeout time.Duration   // idle workers are killed after this duration (0 = no eviction)
	warmWg      sync.WaitGroup  // tracks warmLoop goroutines
	wg          sync.WaitGroup  // tracks in-flight Execute() calls

	stateMu sync.Mutex
	stopped bool
}

// NewStringProcessPool creates a pool for inline script strings.
// The functionName is baked into the script at construction time.
// idleTimeout controls how long an idle worker sits in the pool before being killed (0 = no limit).
func NewStringProcessPool(config types.Config, functionName string, script string, pythonPath string, timeout time.Duration, maxRunning int, configuration types.Configuration) *ProcessPool {
	return newProcessPool(config, pythonPath, timeout, maxRunning, configuration,
		buildInlineScript(functionName, strings.TrimSpace(script)))
}

// NewFileProcessPool creates a pool for .py file paths.
// The functionName is baked into the script at construction time.
// idleTimeout controls how long an idle worker sits in the pool before being killed (0 = no limit).
func NewFileProcessPool(config types.Config, functionName string, path string, pythonPath string, timeout time.Duration, maxRunning int, configuration types.Configuration) *ProcessPool {
	return newProcessPool(config, pythonPath, timeout, maxRunning, configuration,
		buildFileScript(strings.TrimSpace(path), functionName))
}

// newProcessPool is the shared constructor for both inline and file modes.
func newProcessPool(config types.Config, pythonPath string, timeout time.Duration, maxRunning int, configuration types.Configuration, scriptContent string) *ProcessPool {
	p := &ProcessPool{
		config:        config,
		pythonPath:    pythonPath,
		timeout:       timeout,
		maxRunning:    maxRunning,
		vars:          base.NodeUtils.GetVars(configuration),
		scriptContent: scriptContent,
		ready:         make(chan *worker, maxRunning),
		demand:        make(chan struct{}, maxRunning),
		done:          make(chan struct{}),
		stop:          make(chan struct{}),
		idleTimeout:   defaultIdleTimeout,
	}
	// Pre-fill demand signals so warmLoops start workers eagerly at init.
	for i := 0; i < maxRunning; i++ {
		p.demand <- struct{}{}
	}
	for i := 0; i < maxRunning; i++ {
		p.warmWg.Add(1)
		go p.warmLoop()
	}
	p.warmWg.Add(1)
	go p.idleReaper()
	return p
}

// SetIdleTimeout sets the idle timeout for the pool. Workers that sit idle in the
// ready channel longer than this duration are killed and their slot is released.
// Set to 0 to disable idle eviction. Must be called before any Execute() calls.
func (p *ProcessPool) SetIdleTimeout(d time.Duration) {
	p.idleTimeout = d
}

// idleReaper periodically kills idle workers that have been sitting in the ready
// channel too long. It does NOT signal demand, so the pool naturally shrinks when
// idle. When Execute later needs workers, it signals demand and warmLoop produces
// them on the fly.
func (p *ProcessPool) idleReaper() {
	defer p.warmWg.Done()
	if p.idleTimeout <= 0 {
		return
	}
	ticker := time.NewTicker(p.idleTimeout)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			// Try to drain one idle worker per tick. Since we don't signal demand,
			// the pool shrinks by one. If load increases later, Execute will signal
			// demand and warmLoop will produce replacements.
			select {
			case w := <-p.ready:
				w.kill()
			default:
				// No idle workers — pool is already empty or busy.
			}
		case <-p.stop:
			return
		}
	}
}

// startWorker starts a new Python process with the pre-built script.
// After cmd.Start(), the process runs main() which blocks at sys.stdin.read().
func (p *ProcessPool) startWorker() (*worker, error) {
	cmd := exec.Command(p.pythonPath, "-c", p.scriptContent)
	stdinPipe, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("%s: failed to create stdin pipe: %w", componentPrefix, err)
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("%s: failed to start process: %w", componentPrefix, err)
	}
	return &worker{
		cmd:    cmd,
		stdin:  stdinPipe,
		stdout: &stdout,
		stderr: &stderr,
	}, nil
}

// warmLoop runs in a background goroutine, producing workers on demand.
// It waits for a demand signal before starting each worker, so the pool
// only grows when Execute needs workers or during initial warm-up.
func (p *ProcessPool) warmLoop() {
	defer p.warmWg.Done()
	backoff := initialBackoff
	for {
		// Wait for demand signal before starting a worker.
		select {
		case <-p.demand:
		case <-p.stop:
			return
		}

		w, err := p.startWorker()
		if err != nil {
			// Put demand back so we retry, then back off.
			p.demand <- struct{}{}
			if p.config.Logger != nil {
				p.config.Logger.Printf("%s: failed to start worker: %v, retrying in %v", componentPrefix, err, backoff)
			}
			select {
			case <-p.stop:
				return
			case <-time.After(backoff):
				backoff *= 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
				continue
			}
		}
		// Worker started successfully, reset backoff.
		backoff = initialBackoff
		select {
		case p.ready <- w:
			// Worker placed in ready channel.
		case <-p.stop:
			w.kill()
			return
		}
	}
}

// pythonMainBody is the shared main() body for both inline and file modes.
// It reads JSON from stdin, calls the user function, normalizes the return value,
// and writes JSON to stdout.
//
// Return value normalization:
//   - tuple  → {"msg": elem0, "metadata": elem1, "msgType": elem2}
//   - dict with "msg" or "metadata" key → treated as structured result
//   - other dict → wrapped as {"msg": result}
//   - single value → wrapped as {"msg": result}
const pythonMainBody = `
def _normalize_result(result):
    if isinstance(result, tuple):
        parts = list(result)
        out = {}
        if len(parts) > 0:
            out["msg"] = parts[0]
        if len(parts) > 1:
            out["metadata"] = parts[1]
        if len(parts) > 2:
            out["msgType"] = parts[2]
        return out
    if isinstance(result, dict) and ("msg" in result or "metadata" in result or "msgType" in result):
        return result
    return {"msg": result}

def main():
    try:
        raw = sys.stdin.read()
        input_data = json.loads(raw)
        msg = input_data["msg"]
        metadata = input_data["metadata"]
        msgType = input_data["msgType"]
        dataType = input_data["dataType"]
        vars_data = input_data.get("vars", {})
        global_data = input_data.get("global", {})
        sig = inspect.signature(_call_func)
        n = len(sig.parameters)
        if n >= 6:
            result = _call_func(msg, metadata, msgType, dataType, vars_data, global_data)
        elif n == 5:
            result = _call_func(msg, metadata, msgType, dataType, vars_data)
        else:
            result = _call_func(msg, metadata, msgType, dataType)
        json.dump(_normalize_result(result), sys.stdout, ensure_ascii=False)
    except Exception as e:
        json.dump({"error": str(e), "traceback": traceback.format_exc()}, sys.stderr, ensure_ascii=False)
        sys.exit(1)

if __name__ == "__main__":
    main()
`

// buildInlineScript builds a complete Python script from inline function body.
func buildInlineScript(functionName string, userScript string) string {
	return "import sys\nimport json\nimport traceback\nimport inspect\n\n" +
		fmt.Sprintf("def %s(msg, metadata, msgType, dataType, vars={}, globalProps={}):\n%s\n\n", functionName, indentLines(userScript, "    ")) +
		fmt.Sprintf("_call_func = %s\n", functionName) +
		pythonMainBody
}

// buildFileScript builds a Python script that imports the user's .py file.
func buildFileScript(filePath string, functionName string) string {
	return "import sys\nimport json\nimport importlib.util\nimport traceback\nimport inspect\n\n" +
		fmt.Sprintf("spec = importlib.util.spec_from_file_location(\"_user_module\", %q)\n", filePath) +
		"mod = importlib.util.module_from_spec(spec)\n" +
		"spec.loader.exec_module(mod)\n\n" +
		fmt.Sprintf("_call_func = getattr(mod, %q)\n\n", functionName) +
		pythonMainBody
}

func indentLines(s string, prefix string) string {
	lines := strings.Split(s, "\n")
	var b strings.Builder
	for i, line := range lines {
		if i > 0 {
			b.WriteByte('\n')
		}
		if strings.TrimSpace(line) == "" {
			b.WriteString(line)
		} else {
			b.WriteString(prefix + line)
		}
	}
	return b.String()
}

// Execute runs the python script in a pre-warmed subprocess, passing input as JSON
// via stdin and reading the result from stdout. stderr is captured and returned as error.
func (p *ProcessPool) Execute(msg string, metadata map[string]string, msgType string, dataType string) (map[string]interface{}, error) {
	p.stateMu.Lock()
	if p.stopped {
		p.stateMu.Unlock()
		return nil, fmt.Errorf("%s: pool is shutting down", componentPrefix)
	}
	p.wg.Add(1)
	p.stateMu.Unlock()
	defer p.wg.Done()

	// Phase 1: get a pre-warmed worker from the ready channel.
	var w *worker
	if p.timeout > 0 {
		select {
		case w = <-p.ready:
		case <-p.done:
			return nil, fmt.Errorf("%s: pool is shutting down", componentPrefix)
		case <-time.After(p.timeout):
			return nil, fmt.Errorf("%s: concurrency limit reached, timed out waiting for slot (maxRunning=%d)", componentPrefix, p.maxRunning)
		}
	} else {
		select {
		case w = <-p.ready:
		case <-p.done:
			return nil, fmt.Errorf("%s: pool is shutting down", componentPrefix)
		}
	}
	// Signal demand so warmLoop starts a replacement worker.
	p.demand <- struct{}{}

	// Phase 2: build input data.
	inputData := map[string]interface{}{
		inputKeyMsg:      tryParseJSON(msg),
		inputKeyMetadata: metadata,
		inputKeyMsgType:  msgType,
		inputKeyDataType: dataType,
	}
	if len(p.vars) > 0 {
		varsMap := make(map[string]string)
		for _, v := range p.vars {
			if m, ok := v.(map[string]string); ok {
				for mk, mv := range m {
					varsMap[mk] = mv
				}
			}
		}
		if len(varsMap) > 0 {
			inputData[inputKeyVars] = varsMap
		}
	}
	if p.config.Properties != nil && len(p.config.Properties.Values()) > 0 {
		globals := make(map[string]string, len(p.config.Properties))
		for k, v := range p.config.Properties {
			globals[k] = v
		}
		inputData[inputKeyGlobal] = globals
	}

	inputBytes, err := json.Marshal(inputData)
	if err != nil {
		w.stdin.Close()
		w.cmd.Process.Kill()
		w.cmd.Wait()
		return nil, fmt.Errorf("%s: failed to marshal input: %w", componentPrefix, err)
	}

	// Phase 3: write input to worker's stdin and close it (signals EOF).
	if _, err := w.stdin.Write(inputBytes); err != nil {
		w.stdin.Close()
		w.cmd.Process.Kill()
		w.cmd.Wait()
		return nil, fmt.Errorf("%s: failed to write to process stdin: %w", componentPrefix, err)
	}
	if err := w.stdin.Close(); err != nil {
		w.cmd.Process.Kill()
		w.cmd.Wait()
		return nil, fmt.Errorf("%s: failed to close process stdin: %w", componentPrefix, err)
	}

	// Phase 4: wait for the process to finish with timeout.
	waitErrCh := make(chan error, 1)
	go func() {
		waitErrCh <- w.cmd.Wait()
	}()

	var waitErr error
	if p.timeout > 0 {
		select {
		case waitErr = <-waitErrCh:
		case <-time.After(p.timeout):
			w.cmd.Process.Kill()
			<-waitErrCh
			return nil, fmt.Errorf("%s: script execution timed out after %s", componentPrefix, p.timeout)
		}
	} else {
		waitErr = <-waitErrCh
	}

	if waitErr != nil {
		stderrStr := w.stderr.String()
		if stderrStr != "" {
			return nil, fmt.Errorf("%s: %s", componentPrefix, stderrStr)
		}
		return nil, fmt.Errorf("%s: execution failed: %w", componentPrefix, waitErr)
	}

	// Phase 5: parse output.
	var result map[string]interface{}
	if err := json.Unmarshal(w.stdout.Bytes(), &result); err != nil {
		return nil, fmt.Errorf("%s: failed to parse output: %w, output: %s", componentPrefix, err, w.stdout.String())
	}

	if errMsg, ok := result[resultKeyError]; ok {
		return nil, fmt.Errorf("%s: %v", componentPrefix, errMsg)
	}

	return result, nil
}

// Shutdown stops the warmLoop, kills all idle workers, and waits for in-flight
// executions to complete.
func (p *ProcessPool) Shutdown() {
	p.stateMu.Lock()
	if p.stopped {
		p.stateMu.Unlock()
		return
	}
	p.stopped = true
	p.stateMu.Unlock()

	// Signal warmLoop goroutines to stop producing new workers.
	close(p.stop)
	// Wait for all warmLoop goroutines to finish (no more sends to ready channel).
	p.warmWg.Wait()
	// Signal all waiting Execute calls to return.
	close(p.done)
	// Drain and kill all idle workers.
	for {
		select {
		case w := <-p.ready:
			w.cmd.Process.Kill()
			w.cmd.Wait()
		default:
			goto drained
		}
	}
drained:
	p.wg.Wait()
}

// tryParseJSON attempts to parse a string as JSON.
// If valid, returns the parsed value; otherwise returns the original string.
func tryParseJSON(s string) interface{} {
	var v interface{}
	if err := json.Unmarshal([]byte(s), &v); err == nil {
		return v
	}
	return s
}

// ValidatePython checks if the python executable is available.
func ValidatePython(pythonPath string) error {
	cmd := exec.Command(pythonPath, "--version")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("python executable '%s' not found: %w", pythonPath, err)
	}
	return nil
}
