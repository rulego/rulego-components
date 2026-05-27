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

package python

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/rulego/rulego"
	pythonEngine "github.com/rulego/rulego-components/pkg/python_engine"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/utils/maps"
)

// init registers the component to rulego
func init() {
	_ = rulego.Registry.Register(&PythonNode{})
}

const (
	// nodeType is the component type identifier
	nodeType = "x/python"
	// defaultMaxRunning is the default maximum concurrent python processes
	defaultMaxRunning = 10

	// result keys (protocol shared with Python script)
	resultKeyMsg      = "msg"
	resultKeyMetadata = "metadata"
	resultKeyMsgType  = "msgType"
)

// FunctionNameProcess is the name of the function to be called in the script
const FunctionNameProcess = "Process"

// PythonNodeConfiguration node configuration
type PythonNodeConfiguration struct {
	Script     string `json:"script" label:"Script" desc:"Python script content or .py file path" required:"true"`
	PythonPath string `json:"pythonPath" label:"Python Path" desc:"Python interpreter path, uses system PATH by default"`
	Timeout    string `json:"timeout" label:"Timeout" desc:"Script execution timeout, e.g. 10s, 1m"`
	MaxRunning int    `json:"maxRunning" label:"Max Running" desc:"Max concurrent executions, default 1"`
}

// PythonNode is an action component that executes Python scripts.
// It spawns a python3 subprocess for each message, passes input via stdin JSON,
// and reads the result from stdout JSON.
//
// JSON rule chain DSL example:
//
//	{
//	  "id": "s1",
//	  "type": "x/python",
//	  "name": "Python处理",
//	  "configuration": {
//	    "script": "import json\ndata = json.loads(msg) if isinstance(msg, str) else msg\ndata['processed'] = True\nreturn json.dumps(data), metadata, msgType",
//	    "pythonPath": "python3",
//	    "timeout": "5s",
//	    "maxRunning": 10
//	  }
//	}
type PythonNode struct {
	Config PythonNodeConfiguration
	pool   *pythonEngine.ProcessPool
}

// New creates a new instance of PythonNode
func (x *PythonNode) New() types.Node {
	return &PythonNode{Config: PythonNodeConfiguration{
		Script:     "return msg, metadata, msgType",
		PythonPath: pythonEngine.DefaultPythonPath,
		Timeout:    "",
		MaxRunning: defaultMaxRunning,
	}}
}

// Type returns the type of the component
func (x *PythonNode) Type() string {
	return nodeType
}

// Init initializes the component
func (x *PythonNode) Init(ruleConfig types.Config, configuration types.Configuration) error {
	err := maps.Map2Struct(configuration, &x.Config)
	if err != nil {
		return err
	}

	if x.Config.PythonPath == "" {
		// auto-detect python executable
		resolved, err := pythonEngine.ResolvePythonPath("")
		if err != nil {
			return fmt.Errorf("%s: %w", nodeType, err)
		}
		x.Config.PythonPath = resolved
	} else {
		// validate user-specified path
		if err = pythonEngine.ValidatePython(x.Config.PythonPath); err != nil {
			return fmt.Errorf("%s: %w", nodeType, err)
		}
	}
	if x.Config.MaxRunning <= 0 {
		x.Config.MaxRunning = defaultMaxRunning
	}

	// parse timeout
	var timeout time.Duration
	if x.Config.Timeout != "" {
		timeout, err = parseTimeout(x.Config.Timeout)
		if err != nil {
			return fmt.Errorf("%s: invalid timeout '%s': %w", nodeType, x.Config.Timeout, err)
		}
	} else if ruleConfig.ScriptMaxExecutionTime > 0 {
		timeout = ruleConfig.ScriptMaxExecutionTime
	}

	if strings.HasSuffix(x.Config.Script, ".py") {
		x.pool = pythonEngine.NewFileProcessPool(
			ruleConfig, FunctionNameProcess, x.Config.Script,
			x.Config.PythonPath, timeout,
			x.Config.MaxRunning, configuration,
		)
	} else {
		// inline mode: detect if user mistakenly wrote a function definition
		if hasFunctionDef(x.Config.Script) {
			return fmt.Errorf("%s: inline script should only contain the function body, not a 'def' statement. Remove the 'def Process(...):' line and keep only the function body", nodeType)
		}
		x.pool = pythonEngine.NewStringProcessPool(
			ruleConfig, FunctionNameProcess, x.Config.Script,
			x.Config.PythonPath, timeout,
			x.Config.MaxRunning, configuration,
		)
	}

	return nil
}

// OnMsg handles the message by executing the Python script
func (x *PythonNode) OnMsg(ctx types.RuleContext, msg types.RuleMsg) {
	if x.pool == nil {
		ctx.TellFailure(msg, fmt.Errorf("%s: pool not initialized", nodeType))
		return
	}

	result, err := x.pool.Execute(
		msg.GetData(),
		msg.Metadata.GetReadOnlyValues(),
		msg.Type,
		string(msg.DataType),
	)
	if err != nil {
		ctx.TellFailure(msg, err)
		return
	}

	applyResult(&msg, result)
	ctx.TellSuccess(msg)
}

// applyResult writes the Python script result back to the RuleMsg.
// The result is always a map with optional keys: "msg", "metadata", "msgType".
func applyResult(msg *types.RuleMsg, result map[string]interface{}) {
	if v, ok := result[resultKeyMsg]; ok {
		msg.SetData(toJSONString(v))
	}
	if m, ok := result[resultKeyMetadata].(map[string]interface{}); ok {
		meta := make(map[string]string, len(m))
		for k, v := range m {
			meta[k] = fmt.Sprintf("%v", v)
		}
		msg.Metadata.ReplaceAll(meta)
	}
	if s, ok := result[resultKeyMsgType].(string); ok && s != "" {
		msg.Type = s
	}
}

// toJSONString converts a value to a JSON string.
// If the value is already a string, it is returned as-is.
func toJSONString(v interface{}) string {
	if s, ok := v.(string); ok {
		return s
	}
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("%v", v)
	}
	return string(b)
}

// Destroy releases the resources of the component
func (x *PythonNode) Destroy() {
	if x.pool != nil {
		x.pool.Shutdown()
	}
}

// Desc returns the component description
func (x *PythonNode) Desc() string {
	return "Execute Python script for message transformation. Script must return dict with msg, metadata, msgType. Routes to Success/Failure"
}

// hasFunctionDef checks if an inline script contains a top-level function definition
// (a line starting with "def " without indentation). Nested function definitions
// inside the function body (indented) are allowed and not detected.
// hasFunctionDef checks if the script contains a top-level function definition
// that looks like the main entry point (e.g., "def Process(").
// Helper functions like "def helper(x):" are allowed in inline mode because
// they will be indented inside the generated Process function body.
func hasFunctionDef(script string) bool {
	for _, line := range strings.Split(script, "\n") {
		trimmed := strings.TrimLeft(line, " \t")
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		// only check lines that are NOT indented (top-level in the script)
		if line == trimmed && strings.HasPrefix(trimmed, "def "+FunctionNameProcess+"(") {
			return true
		}
	}
	return false
}

// parseTimeout parses a timeout string.
// Supports Go duration format ("5s", "1000ms", "1m") or a plain number interpreted as seconds ("5" = 5s).
func parseTimeout(s string) (time.Duration, error) {
	if d, err := time.ParseDuration(s); err == nil {
		return d, nil
	}
	// try plain number as seconds
	if n, err := strconv.Atoi(s); err == nil {
		return time.Duration(n) * time.Second, nil
	}
	return 0, fmt.Errorf("invalid duration: %s", s)
}
