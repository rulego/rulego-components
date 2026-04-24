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
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pythonEngine "github.com/rulego/rulego-components/pkg/python_engine"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/test"
	"github.com/rulego/rulego/test/assert"
)

const targetNodeType = "x/python"

func getRegistry() *types.SafeComponentSlice {
	registry := &types.SafeComponentSlice{}
	registry.Add(&PythonNode{})
	return registry
}

// checkPythonAvailable skips the test if python is not available
func checkPythonAvailable(t *testing.T) {
	t.Helper()
	if _, err := pythonEngine.ResolvePythonPath(""); err != nil {
		t.Skipf("python not available, skipping test: %v", err)
	}
}

func pythonPath() string {
	if p := os.Getenv("PYTHON_PATH"); p != "" {
		return p
	}
	p, _ := pythonEngine.ResolvePythonPath("")
	if p != "" {
		return p
	}
	return "python3"
}

// resolveTestdataPath returns an absolute path to a file in pkg/python_engine/testdata/.
func resolveTestdataPath(t *testing.T, filename string) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("failed to determine test source file path")
	}
	dir := filepath.Dir(thisFile)
	p := filepath.Join(dir, "..", "..", "pkg", "python_engine", "testdata", filename)
	absPath, err := filepath.Abs(p)
	if err != nil {
		t.Fatalf("failed to resolve testdata path: %v", err)
	}
	if _, err := os.Stat(absPath); err != nil {
		t.Fatalf("testdata file not found: %s", absPath)
	}
	return absPath
}

// TestNodeNew tests Node.New() and Type()
func TestNodeNew(t *testing.T) {
	checkPythonAvailable(t)
	registry := getRegistry()
	test.NodeNew(t, targetNodeType, &PythonNode{}, types.Configuration{
		"script": "return msg, metadata, msgType",
	}, registry)
}

// TestNodeInit tests Node.Init()
func TestNodeInit(t *testing.T) {
	checkPythonAvailable(t)
	registry := getRegistry()

	t.Run("DefaultConfig", func(t *testing.T) {
		pp := pythonPath()
		test.NodeInit(t, targetNodeType, types.Configuration{
			"script":     "return msg, metadata, msgType",
			"pythonPath": pp,
		}, types.Configuration{
			"script":      "return msg, metadata, msgType",
			"pythonPath":  pp,
			"maxRunning":  10,
		}, registry)
	})

	t.Run("CustomConfig", func(t *testing.T) {
		pp := pythonPath()
		test.NodeInit(t, targetNodeType, types.Configuration{
			"script":     "return msg",
			"pythonPath": pp,
			"timeout":    "5s",
			"maxRunning": 5,
		}, types.Configuration{
			"script":     "return msg",
			"pythonPath": pp,
			"timeout":    "5s",
			"maxRunning": 5,
		}, registry)
	})
}

// TestOnMsg_PassThrough tests the simplest script that returns msg unchanged
func TestOnMsg_PassThrough(t *testing.T) {
	checkPythonAvailable(t)
	registry := getRegistry()

	node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
		"script":     "return msg, metadata, msgType",
		"pythonPath": pythonPath(),
	}, registry)
	assert.Nil(t, err)
	defer node.Destroy()

	metaData := types.BuildMetadata(make(map[string]string))
	metaData.PutValue("device", "sensor01")

	msgList := []test.Msg{
		{
			MetaData:   metaData,
			MsgType:    "TEST",
			Data:       `{"temperature":35}`,
			AfterSleep: time.Millisecond * 200,
		},
	}

	test.NodeOnMsgWithChildren(t, node, msgList, nil, func(msg types.RuleMsg, relationType string, err error) {
		assert.Equal(t, types.Success, relationType)
		assert.Nil(t, err)
		assert.True(t, strings.Contains(msg.GetData(), "temperature"))
	})
}

// TestOnMsg_TransformData tests a script that transforms the message data
func TestOnMsg_TransformData(t *testing.T) {
	checkPythonAvailable(t)
	registry := getRegistry()

	node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
		"script":     `data = msg if isinstance(msg, dict) else json.loads(msg); data["processed"] = True; return json.dumps(data), metadata, msgType`,
		"pythonPath": pythonPath(),
	}, registry)
	assert.Nil(t, err)
	defer node.Destroy()

	metaData := types.BuildMetadata(make(map[string]string))
	metaData.PutValue("device", "sensor01")

	msgList := []test.Msg{
		{
			MetaData:   metaData,
			MsgType:    "TEST",
			Data:       `{"temperature":35}`,
			AfterSleep: time.Millisecond * 200,
		},
	}

	test.NodeOnMsgWithChildren(t, node, msgList, nil, func(msg types.RuleMsg, relationType string, err error) {
		assert.Equal(t, types.Success, relationType)
		assert.Nil(t, err)
		// verify the "processed" field was added
		var data map[string]interface{}
		assert.Nil(t, json.Unmarshal([]byte(msg.GetData()), &data))
		assert.Equal(t, true, data["processed"])
		assert.Equal(t, float64(35), data["temperature"])
	})
}

// TestOnMsg_ModifyMetadata tests a script that modifies metadata
func TestOnMsg_ModifyMetadata(t *testing.T) {
	checkPythonAvailable(t)
	registry := getRegistry()

	node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
		"script":     `metadata["newKey"] = "newValue"; return msg, metadata, msgType`,
		"pythonPath": pythonPath(),
	}, registry)
	assert.Nil(t, err)
	defer node.Destroy()

	metaData := types.BuildMetadata(make(map[string]string))
	metaData.PutValue("device", "sensor01")

	msgList := []test.Msg{
		{
			MetaData:   metaData,
			MsgType:    "TEST",
			Data:       `{"temperature":35}`,
			AfterSleep: time.Millisecond * 200,
		},
	}

	test.NodeOnMsgWithChildren(t, node, msgList, nil, func(msg types.RuleMsg, relationType string, err error) {
		assert.Equal(t, types.Success, relationType)
		assert.Nil(t, err)
		assert.Equal(t, "newValue", msg.Metadata.GetValue("newKey"))
		assert.Equal(t, "sensor01", msg.Metadata.GetValue("device"))
	})
}

// TestOnMsg_ModifyMsgType tests a script that changes the message type
func TestOnMsg_ModifyMsgType(t *testing.T) {
	checkPythonAvailable(t)
	registry := getRegistry()

	node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
		"script":     `return msg, metadata, "PROCESSED"`,
		"pythonPath": pythonPath(),
	}, registry)
	assert.Nil(t, err)
	defer node.Destroy()

	metaData := types.BuildMetadata(make(map[string]string))

	msgList := []test.Msg{
		{
			MetaData:   metaData,
			MsgType:    "TEST",
			Data:       `{"temperature":35}`,
			AfterSleep: time.Millisecond * 200,
		},
	}

	test.NodeOnMsgWithChildren(t, node, msgList, nil, func(msg types.RuleMsg, relationType string, err error) {
		assert.Equal(t, types.Success, relationType)
		assert.Nil(t, err)
		assert.Equal(t, "PROCESSED", msg.Type)
	})
}

// TestOnMsg_DictResult tests a script that returns a dict instead of a tuple
func TestOnMsg_DictResult(t *testing.T) {
	checkPythonAvailable(t)
	registry := getRegistry()

	node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
		"script":     `return {"msg": "transformed", "msgType": "NEW_TYPE"}`,
		"pythonPath": pythonPath(),
	}, registry)
	assert.Nil(t, err)
	defer node.Destroy()

	metaData := types.BuildMetadata(make(map[string]string))

	msgList := []test.Msg{
		{
			MetaData:   metaData,
			MsgType:    "TEST",
			Data:       `hello`,
			AfterSleep: time.Millisecond * 200,
		},
	}

	test.NodeOnMsgWithChildren(t, node, msgList, nil, func(msg types.RuleMsg, relationType string, err error) {
		assert.Equal(t, types.Success, relationType)
		assert.Nil(t, err)
		assert.Equal(t, "transformed", msg.GetData())
		assert.Equal(t, "NEW_TYPE", msg.Type)
	})
}

// TestOnMsg_ScriptError tests that a script with an error goes to the failure branch
func TestOnMsg_ScriptError(t *testing.T) {
	checkPythonAvailable(t)
	registry := getRegistry()

	node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
		"script":     `raise ValueError("intentional error")`,
		"pythonPath": pythonPath(),
	}, registry)
	assert.Nil(t, err)
	defer node.Destroy()

	metaData := types.BuildMetadata(make(map[string]string))

	msgList := []test.Msg{
		{
			MetaData:   metaData,
			MsgType:    "TEST",
			Data:       `{"temperature":35}`,
			AfterSleep: time.Millisecond * 200,
		},
	}

	test.NodeOnMsgWithChildren(t, node, msgList, nil, func(msg types.RuleMsg, relationType string, err error) {
		assert.Equal(t, types.Failure, relationType)
		assert.NotNil(t, err)
		assert.True(t, strings.Contains(err.Error(), "intentional error"))
	})
}

// TestOnMsg_ScriptSyntaxError tests that a syntax error in the script goes to the failure branch
func TestOnMsg_ScriptSyntaxError(t *testing.T) {
	checkPythonAvailable(t)
	registry := getRegistry()

	node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
		"script":     `this is not valid python syntax !!!`,
		"pythonPath": pythonPath(),
	}, registry)
	assert.Nil(t, err)
	defer node.Destroy()

	metaData := types.BuildMetadata(make(map[string]string))

	msgList := []test.Msg{
		{
			MetaData:   metaData,
			MsgType:    "TEST",
			Data:       `{"temperature":35}`,
			AfterSleep: time.Millisecond * 200,
		},
	}

	test.NodeOnMsgWithChildren(t, node, msgList, nil, func(msg types.RuleMsg, relationType string, err error) {
		assert.Equal(t, types.Failure, relationType)
		assert.NotNil(t, err)
	})
}

// TestOnMsg_Timeout tests that a script exceeding the configured timeout goes to the failure branch
func TestOnMsg_Timeout(t *testing.T) {
	checkPythonAvailable(t)
	registry := getRegistry()

	node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
		"script":     `import time; time.sleep(10); return msg`,
		"pythonPath": pythonPath(),
		"timeout":    "1s",
	}, registry)
	assert.Nil(t, err)
	defer node.Destroy()

	metaData := types.BuildMetadata(make(map[string]string))

	msgList := []test.Msg{
		{
			MetaData:   metaData,
			MsgType:    "TEST",
			Data:       `{"temperature":35}`,
			AfterSleep: time.Second * 3,
		},
	}

	test.NodeOnMsgWithChildren(t, node, msgList, nil, func(msg types.RuleMsg, relationType string, err error) {
		assert.Equal(t, types.Failure, relationType)
		assert.NotNil(t, err)
		assert.True(t, strings.Contains(err.Error(), "timed out"), "expected timeout error, got: %v", err)
	})
}

// TestOnMsg_PrintToStderr tests that stderr output causes a failure
func TestOnMsg_PrintToStderr(t *testing.T) {
	checkPythonAvailable(t)
	registry := getRegistry()

	node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
		"script":     `import sys; print("error from script", file=sys.stderr); sys.exit(1)`,
		"pythonPath": pythonPath(),
	}, registry)
	assert.Nil(t, err)
	defer node.Destroy()

	metaData := types.BuildMetadata(make(map[string]string))

	msgList := []test.Msg{
		{
			MetaData:   metaData,
			MsgType:    "TEST",
			Data:       `{"temperature":35}`,
			AfterSleep: time.Millisecond * 200,
		},
	}

	test.NodeOnMsgWithChildren(t, node, msgList, nil, func(msg types.RuleMsg, relationType string, err error) {
		assert.Equal(t, types.Failure, relationType)
		assert.NotNil(t, err)
		assert.True(t, strings.Contains(err.Error(), "error from script"), "expected stderr in error, got: %v", err)
	})
}

// TestOnMsg_FileScript tests executing a .py file from testdata
func TestOnMsg_FileScript(t *testing.T) {
	checkPythonAvailable(t)
	registry := getRegistry()

	node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
		"script":     resolveTestdataPath(t, "process_basic.py"),
		"pythonPath": pythonPath(),
	}, registry)
	assert.Nil(t, err)
	defer node.Destroy()

	metaData := types.BuildMetadata(make(map[string]string))

	msgList := []test.Msg{
		{
			MetaData:   metaData,
			MsgType:    "TEST",
			Data:       `{"temperature":35}`,
			AfterSleep: time.Millisecond * 500,
		},
	}

	test.NodeOnMsgWithChildren(t, node, msgList, nil, func(msg types.RuleMsg, relationType string, err error) {
		assert.Equal(t, types.Success, relationType)
		assert.Nil(t, err)
		var data map[string]interface{}
		assert.Nil(t, json.Unmarshal([]byte(msg.GetData()), &data))
		assert.Equal(t, true, data["processed"])
	})
}

// TestOnMsg_Concurrent tests concurrent message processing
func TestOnMsg_Concurrent(t *testing.T) {
	checkPythonAvailable(t)
	registry := getRegistry()

	node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
		"script":     `return msg, metadata, msgType`,
		"pythonPath": pythonPath(),
		"maxRunning": 3,
	}, registry)
	assert.Nil(t, err)
	defer node.Destroy()

	metaData := types.BuildMetadata(make(map[string]string))

	msgList := make([]test.Msg, 5)
	for i := 0; i < 5; i++ {
		msgList[i] = test.Msg{
			MetaData:   metaData,
			MsgType:    "TEST",
			Data:       fmt.Sprintf(`{"index":%d}`, i),
			AfterSleep: time.Millisecond * 500,
		}
	}

	var successCount atomic.Int32
	test.NodeOnMsgWithChildren(t, node, msgList, nil, func(msg types.RuleMsg, relationType string, err error) {
		if relationType == types.Success {
			successCount.Add(1)
		}
	})

	assert.Equal(t, int32(5), successCount.Load())
}

// TestOnMsg_SingleReturnValue tests returning a single value (not a tuple)
func TestOnMsg_SingleReturnValue(t *testing.T) {
	checkPythonAvailable(t)
	registry := getRegistry()

	node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
		"script":     `return "plain string result"`,
		"pythonPath": pythonPath(),
	}, registry)
	assert.Nil(t, err)
	defer node.Destroy()

	metaData := types.BuildMetadata(make(map[string]string))

	msgList := []test.Msg{
		{
			MetaData:   metaData,
			MsgType:    "TEST",
			Data:       `{"temperature":35}`,
			AfterSleep: time.Millisecond * 200,
		},
	}

	test.NodeOnMsgWithChildren(t, node, msgList, nil, func(msg types.RuleMsg, relationType string, err error) {
		assert.Equal(t, types.Success, relationType)
		assert.Nil(t, err)
		assert.Equal(t, "plain string result", msg.GetData())
	})
}

// TestParseTimeout tests the parseTimeout helper function
func TestParseTimeout(t *testing.T) {
	tests := []struct {
		input    string
		expected time.Duration
		wantErr  bool
	}{
		{"5s", 5 * time.Second, false},
		{"1000ms", 1000 * time.Millisecond, false},
		{"1m", 1 * time.Minute, false},
		{"5", 5 * time.Second, false},
		{"30", 30 * time.Second, false},
		{"0", 0, false},
		{"", 0, true},
		{"abc", 0, true},
		{"1.5", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := parseTimeout(tt.input)
			if tt.wantErr {
				assert.NotNil(t, err)
			} else {
				assert.Nil(t, err)
				assert.Equal(t, tt.expected, got)
			}
		})
	}
}

// TestHasFunctionDef tests the hasFunctionDef helper
func TestHasFunctionDef(t *testing.T) {
	tests := []struct {
		name     string
		script   string
		expected bool
	}{
		{"Body only", `data = msg if isinstance(msg, dict) else json.loads(msg)
data["processed"] = True
return json.dumps(data), metadata, msgType`, false},
		{"Top-level def", `def Process(msg, metadata, msgType, dataType):
    return msg`, true},
		{"Nested def allowed", `def helper(x):
    return x * 2
return helper(msg)`, false},
		{"Indented def in block", `if True:
    def inner():
        pass
return msg`, false},
		{"Comment with def", `# def Process(msg):
return msg`, false},
		{"Other top-level def allowed", `import json

def myFunc(msg):
    return msg`, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, hasFunctionDef(tt.script))
		})
	}
}

// TestOnMsg_InlineDefError tests that inline mode rejects scripts with top-level function definitions
func TestOnMsg_InlineDefError(t *testing.T) {
	checkPythonAvailable(t)
	registry := getRegistry()

	_, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
		"script": `def Process(msg, metadata, msgType, dataType):
    return msg`,
		"pythonPath": pythonPath(),
	}, registry)
	assert.NotNil(t, err)
	assert.True(t, strings.Contains(err.Error(), "inline script should only contain the function body"))
}

// TestOnMsg_NumericTimeout tests that a plain number timeout string works (e.g. "2" means 2 seconds)
func TestOnMsg_NumericTimeout(t *testing.T) {
	checkPythonAvailable(t)
	registry := getRegistry()

	// script sleeps for 10 seconds, timeout is "1" (1 second) — should timeout
	node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
		"script":     `import time; time.sleep(10); return msg`,
		"pythonPath": pythonPath(),
		"timeout":    "1",
	}, registry)
	assert.Nil(t, err)
	defer node.Destroy()

	metaData := types.BuildMetadata(make(map[string]string))

	msgList := []test.Msg{
		{
			MetaData:   metaData,
			MsgType:    "TEST",
			Data:       `{"temperature":35}`,
			AfterSleep: time.Second * 3,
		},
	}

	test.NodeOnMsgWithChildren(t, node, msgList, nil, func(msg types.RuleMsg, relationType string, err error) {
		assert.Equal(t, types.Failure, relationType)
		assert.NotNil(t, err)
		assert.True(t, strings.Contains(err.Error(), "timed out"), "expected timeout error, got: %v", err)
	})
}

// TestOnMsg_VarsAccess tests that node configuration vars are accessible in the Python script
func TestOnMsg_VarsAccess(t *testing.T) {
	checkPythonAvailable(t)
	registry := getRegistry()

	node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
		"script":     `return vars.get("ip", "not found")`,
		"pythonPath": pythonPath(),
		"vars": map[string]string{
			"ip": "192.168.1.1",
		},
	}, registry)
	assert.Nil(t, err)
	defer node.Destroy()

	metaData := types.BuildMetadata(make(map[string]string))
	msgList := []test.Msg{
		{
			MetaData:   metaData,
			MsgType:    "TEST",
			Data:       `{"temperature":35}`,
			AfterSleep: time.Millisecond * 200,
		},
	}

	test.NodeOnMsgWithChildren(t, node, msgList, nil, func(msg types.RuleMsg, relationType string, err error) {
		assert.Equal(t, types.Success, relationType)
		assert.Nil(t, err)
		assert.Equal(t, "192.168.1.1", msg.GetData())
	})
}

// TestOnMsg_VarsInMetadata tests using vars to enrich metadata
func TestOnMsg_VarsInMetadata(t *testing.T) {
	checkPythonAvailable(t)
	registry := getRegistry()

	node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
		"script":     `metadata["server"] = vars.get("server", "unknown"); return msg, metadata, msgType`,
		"pythonPath": pythonPath(),
		"vars": map[string]string{
			"server": "prod-server-01",
		},
	}, registry)
	assert.Nil(t, err)
	defer node.Destroy()

	metaData := types.BuildMetadata(make(map[string]string))
	msgList := []test.Msg{
		{
			MetaData:   metaData,
			MsgType:    "TEST",
			Data:       `{"temperature":35}`,
			AfterSleep: time.Millisecond * 200,
		},
	}

	test.NodeOnMsgWithChildren(t, node, msgList, nil, func(msg types.RuleMsg, relationType string, err error) {
		assert.Equal(t, types.Success, relationType)
		assert.Nil(t, err)
		assert.Equal(t, "prod-server-01", msg.Metadata.GetValue("server"))
	})
}

// TestOnMsg_FileScriptWithVars tests vars access in file mode with 6-param function
func TestOnMsg_FileScriptWithVars(t *testing.T) {
	checkPythonAvailable(t)
	registry := getRegistry()

	node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
		"script":     resolveTestdataPath(t, "process_vars.py"),
		"pythonPath": pythonPath(),
		"vars": map[string]string{
			"server": "file-server-01",
			"env":    "staging",
		},
	}, registry)
	assert.Nil(t, err)
	defer node.Destroy()

	metaData := types.BuildMetadata(make(map[string]string))
	msgList := []test.Msg{
		{
			MetaData:   metaData,
			MsgType:    "TEST",
			Data:       `{"temperature":35}`,
			AfterSleep: time.Millisecond * 500,
		},
	}

	test.NodeOnMsgWithChildren(t, node, msgList, nil, func(msg types.RuleMsg, relationType string, err error) {
		assert.Equal(t, types.Success, relationType)
		assert.Nil(t, err)
		var data map[string]interface{}
		assert.Nil(t, json.Unmarshal([]byte(msg.GetData()), &data))
		assert.Equal(t, "file-server-01", data["server"])
		assert.Equal(t, "staging", data["env"])
		assert.Equal(t, "file-server-01", msg.Metadata.GetValue("processedBy"))
	})
}

// TestOnMsg_FileScriptBackwardCompatible tests that file scripts with only 4 params still work
// after the vars/globalProps parameter was added. process_basic.py uses 4-param signature.
func TestOnMsg_FileScriptBackwardCompatible(t *testing.T) {
	checkPythonAvailable(t)
	registry := getRegistry()

	node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
		"script":     resolveTestdataPath(t, "process_basic.py"),
		"pythonPath": pythonPath(),
	}, registry)
	assert.Nil(t, err)
	defer node.Destroy()

	metaData := types.BuildMetadata(make(map[string]string))
	msgList := []test.Msg{
		{
			MetaData:   metaData,
			MsgType:    "TEST",
			Data:       `{"temperature":35}`,
			AfterSleep: time.Millisecond * 500,
		},
	}

	test.NodeOnMsgWithChildren(t, node, msgList, nil, func(msg types.RuleMsg, relationType string, err error) {
		assert.Equal(t, types.Success, relationType)
		assert.Nil(t, err)
		var data map[string]interface{}
		assert.Nil(t, json.Unmarshal([]byte(msg.GetData()), &data))
		assert.Equal(t, true, data["processed"])
	})
}

// TestOnMsg_FileScriptDictReturn tests file script returning a dict result
func TestOnMsg_FileScriptDictReturn(t *testing.T) {
	checkPythonAvailable(t)
	registry := getRegistry()

	node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
		"script":     resolveTestdataPath(t, "process_dict_return.py"),
		"pythonPath": pythonPath(),
	}, registry)
	assert.Nil(t, err)
	defer node.Destroy()

	metaData := types.BuildMetadata(make(map[string]string))
	msgList := []test.Msg{
		{
			MetaData:   metaData,
			MsgType:    "TEST",
			Data:       `{"temperature":35}`,
			AfterSleep: time.Millisecond * 500,
		},
	}

	test.NodeOnMsgWithChildren(t, node, msgList, nil, func(msg types.RuleMsg, relationType string, err error) {
		assert.Equal(t, types.Success, relationType)
		assert.Nil(t, err)
		assert.Equal(t, "DICT_RESULT", msg.Type)
		var data map[string]interface{}
		assert.Nil(t, json.Unmarshal([]byte(msg.GetData()), &data))
		assert.Equal(t, "dict_return", data["source"])
	})
}

// TestOnMsg_FileScriptSingleValue tests file script returning a single value
func TestOnMsg_FileScriptSingleValue(t *testing.T) {
	checkPythonAvailable(t)
	registry := getRegistry()

	node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
		"script":     resolveTestdataPath(t, "process_single_value.py"),
		"pythonPath": pythonPath(),
	}, registry)
	assert.Nil(t, err)
	defer node.Destroy()

	metaData := types.BuildMetadata(make(map[string]string))
	msgList := []test.Msg{
		{
			MetaData:   metaData,
			MsgType:    "TEST",
			Data:       `{"temperature":35}`,
			AfterSleep: time.Millisecond * 500,
		},
	}

	test.NodeOnMsgWithChildren(t, node, msgList, nil, func(msg types.RuleMsg, relationType string, err error) {
		assert.Equal(t, types.Success, relationType)
		assert.Nil(t, err)
		assert.Equal(t, "single value result", msg.GetData())
	})
}

// TestOnMsg_ComprehensiveExample demonstrates a script using multiple features:
//   - access msg as dict (JSON auto-parsed)
//   - read and write metadata
//   - change msgType
//   - use vars
//   - return as tuple
func TestOnMsg_ComprehensiveExample(t *testing.T) {
	checkPythonAvailable(t)
	registry := getRegistry()

	node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
		"script": `import json
data = msg if isinstance(msg, dict) else json.loads(msg)
data["processed"] = True
data["threshold"] = int(vars.get("threshold", "50"))
metadata["processedAt"] = vars.get("env", "unknown")
return json.dumps(data), metadata, "PROCESSED"
`,
		"pythonPath": pythonPath(),
		"vars": map[string]string{
			"threshold": "30",
			"env":       "production",
		},
	}, registry)
	assert.Nil(t, err)
	defer node.Destroy()

	metaData := types.BuildMetadata(make(map[string]string))
	metaData.PutValue("device", "sensor01")

	msgList := []test.Msg{
		{
			MetaData:   metaData,
			MsgType:    "TEST",
			Data:       `{"temperature":35}`,
			AfterSleep: time.Millisecond * 300,
		},
	}

	test.NodeOnMsgWithChildren(t, node, msgList, nil, func(msg types.RuleMsg, relationType string, err error) {
		assert.Equal(t, types.Success, relationType)
		assert.Nil(t, err)
		assert.Equal(t, "PROCESSED", msg.Type)

		var data map[string]interface{}
		assert.Nil(t, json.Unmarshal([]byte(msg.GetData()), &data))
		assert.Equal(t, true, data["processed"])
		assert.Equal(t, float64(30), data["threshold"])
		assert.Equal(t, float64(35), data["temperature"])

		assert.Equal(t, "sensor01", msg.Metadata.GetValue("device"))
		assert.Equal(t, "production", msg.Metadata.GetValue("processedAt"))
	})
}
