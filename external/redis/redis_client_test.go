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

package redis

import (
	"strings"
	"testing"
	"time"

	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/test"
	"github.com/rulego/rulego/test/assert"
)

func TestRedisClientNodeOnMsg(t *testing.T) {
	testRedisClientSetFromMetadata(t)
	testRedisClientSetFromData(t)
	testRedisClientGetOnMsg(t)
	testRedisClientDelOnMsg(t)
	testRedisClientHMSet(t)
	testRedisClientHMGet(t)
	testRedisClientHMSetFromExpr(t)
	testRedisClientHMGetFromExpr(t)
	testRedisClientFlushDB(t)
	// The new el.Template function test
	testRedisClientTemplateSetWithCombinedCmd(t)
	testRedisClientTemplateWithParams(t)
	testRedisClientTemplateComplexCmd(t)
	testRedisClientBackwardCompatibility(t)
	// Comprehensive expression testing
	testRedisClientCmdExpression(t)
	testRedisClientCmdWithParamsExpression(t)
	testRedisClientParamsMixedExpression(t)
	testRedisClientCmdParamsCombo(t)
	testRedisClientEdgeCases(t)
}

// Test adding key/value
func testRedisClientSetFromMetadata(t *testing.T) {
	var node ClientNode
	var configuration = make(types.Configuration)
	configuration["Cmd"] = "SET"
	configuration["Params"] = []interface{}{"${metadata.key}", "${metadata.value}"}
	configuration["PoolSize"] = 10
	configuration["Server"] = "127.0.0.1:6379"
	config := types.NewConfig()
	err := node.Init(config, configuration)
	if err != nil {
		t.Errorf("err=%s", err)
	}
	ctx := test.NewRuleContext(config, func(msg types.RuleMsg, relationType string, err2 error) {
		assert.Equal(t, types.Success, relationType)
		// Check if the result is correct - HMSET command returns OK, indicating success
		assert.Equal(t, "OK", msg.GetData())
	})
	metaData := types.NewMetadata()
	// Add parameters to the metadata
	metaData.PutValue("key", "test")
	metaData.PutValue("value", `{"aa":"lala"}`)
	msg := ctx.NewMsg("TEST_MSG_TYPE_AA", metaData, "")
	node.OnMsg(ctx, msg)

	time.Sleep(time.Second * 1)

}

// Test adds key/value, and the value uses msg payload
func testRedisClientSetFromData(t *testing.T) {
	var node ClientNode
	var configuration = make(types.Configuration)
	configuration["Cmd"] = "SET"
	configuration["Params"] = []interface{}{"${metadata.key}", "${data}"}
	configuration["PoolSize"] = 10
	configuration["Server"] = "127.0.0.1:6379"
	config := types.NewConfig()
	err := node.Init(config, configuration)
	if err != nil {
		t.Errorf("err=%s", err)
	}
	ctx := test.NewRuleContext(config, func(msg types.RuleMsg, relationType string, err2 error) {
		assert.Equal(t, types.Success, relationType)
		// Check if the result is correct - HMSET command returns OK, indicating success
		assert.Equal(t, "OK", msg.GetData())
	})
	metaData := types.NewMetadata()
	// Add parameters to the metadata
	metaData.PutValue("key", "test")
	msg := ctx.NewMsg("TEST_MSG_TYPE_AA", metaData, `{"aa":"lala"}`)
	node.OnMsg(ctx, msg)

	time.Sleep(time.Second * 1)

}

// Test to obtain keys
func testRedisClientGetOnMsg(t *testing.T) {
	var node ClientNode
	var configuration = make(types.Configuration)
	configuration["Cmd"] = "GET"
	configuration["Params"] = []interface{}{"${metadata.key}"}
	configuration["PoolSize"] = 10
	configuration["Server"] = "127.0.0.1:6379"
	config := types.NewConfig()
	err := node.Init(config, configuration)
	if err != nil {
		t.Errorf("err=%s", err)
	}
	ctx := test.NewRuleContext(config, func(msg types.RuleMsg, relationType string, err2 error) {
		assert.Equal(t, types.Success, relationType)
		// Check if the results are correct
		assert.Equal(t, `{"aa":"lala"}`, msg.GetData())
	})
	metaData := types.NewMetadata()
	// Add parameters to the metadata
	metaData.PutValue("key", "test")
	msg := ctx.NewMsg("TEST_MSG_TYPE_AA", metaData, "")
	node.OnMsg(ctx, msg)

	time.Sleep(time.Second * 1)

}

// Test to delete key
func testRedisClientDelOnMsg(t *testing.T) {
	var node ClientNode
	var configuration = make(types.Configuration)
	configuration["Cmd"] = "DEL"
	configuration["Params"] = []interface{}{"${metadata.key}"}
	configuration["PoolSize"] = 10
	configuration["Server"] = "127.0.0.1:6379"
	config := types.NewConfig()
	err := node.Init(config, configuration)
	if err != nil {
		t.Errorf("err=%s", err)
	}
	ctx := test.NewRuleContext(config, func(msg types.RuleMsg, relationType string, err2 error) {
		assert.Equal(t, types.Success, relationType)
		// Check if the result is correct - DEL commands return the number of keys deleted
		assert.Equal(t, "1", msg.GetData())
	})
	metaData := types.NewMetadata()
	// Add parameters to the metadata
	metaData.PutValue("key", "test")
	msg := ctx.NewMsg("TEST_MSG_TYPE_AA", metaData, "")
	node.OnMsg(ctx, msg)

	time.Sleep(time.Second * 1)

}

func testRedisClientHMSet(t *testing.T) {
	var node ClientNode
	var configuration = make(types.Configuration)
	configuration["Cmd"] = "HMSET"
	configuration["Params"] = []interface{}{"myhash", "field1", "value1"}
	configuration["PoolSize"] = 10
	configuration["Server"] = "127.0.0.1:6379"
	config := types.NewConfig()
	err := node.Init(config, configuration)
	if err != nil {
		t.Errorf("err=%s", err)
	}
	ctx := test.NewRuleContext(config, func(msg types.RuleMsg, relationType string, err2 error) {
		assert.Equal(t, types.Success, relationType)
		// Check if the result is correct - HMSET command returns OK, indicating success
		assert.Equal(t, "OK", msg.GetData())
	})
	metaData := types.NewMetadata()
	// Add parameters to the metadata
	metaData.PutValue("key", "test")
	metaData.PutValue("value", `{"aa":"lala"}`)
	msg := ctx.NewMsg("TEST_MSG_TYPE_AA", metaData, "")
	node.OnMsg(ctx, msg)

	time.Sleep(time.Second * 1)
}

// testRedisClientParamsMixedExpression: The test params array contains both static parameters and expression parameters
func testRedisClientParamsMixedExpression(t *testing.T) {
	var node ClientNode
	var configuration = make(types.Configuration)
	// Use mixed parameters: static parameters and expression parameters
	configuration["Cmd"] = "HMSET"
	configuration["Params"] = []interface{}{"static_hash_key", "field1", "${metadata.value1}", "field2", "static_value", "field3", "${msg}"}
	configuration["PoolSize"] = 10
	configuration["Server"] = "127.0.0.1:6379"
	config := types.NewConfig()
	err := node.Init(config, configuration)
	if err != nil {
		t.Errorf("err=%s", err)
	}
	ctx := test.NewRuleContext(config, func(msg types.RuleMsg, relationType string, err2 error) {
		assert.Equal(t, types.Success, relationType)
		// Check if the result is correct - HMSET command returns OK, indicating success
		assert.Equal(t, "OK", msg.GetData())
	})
	metaData := types.NewMetadata()
	// Add dynamic parameters to the metadata
	metaData.PutValue("value1", "dynamic_value_from_metadata")
	msg := ctx.NewMsg("TEST_MSG_TYPE_AA", metaData, "dynamic_value_from_msg")
	node.OnMsg(ctx, msg)

	time.Sleep(time.Second * 1)
}

// testRedisClientCmdParamsCombo tests the combination of cmd and params expressions
func testRedisClientCmdParamsCombo(t *testing.T) {
	var node ClientNode
	var configuration = make(types.Configuration)
	// cmd uses expressions, and params also uses expressions
	configuration["Cmd"] = "${metadata.cmd_type}"
	configuration["Params"] = []interface{}{"${metadata.hash_name}", "${metadata.field_name}", "${msg}"}
	configuration["PoolSize"] = 10
	configuration["Server"] = "127.0.0.1:6379"
	config := types.NewConfig()
	err := node.Init(config, configuration)
	if err != nil {
		t.Errorf("err=%s", err)
	}
	ctx := test.NewRuleContext(config, func(msg types.RuleMsg, relationType string, err2 error) {
		assert.Equal(t, types.Success, relationType)
		// Check if the results are correct
		assert.Equal(t, "1", msg.GetData())
	})
	metaData := types.NewMetadata()
	// Add dynamic parameters for cmd and params to the metadata
	metaData.PutValue("cmd_type", "HSET")
	metaData.PutValue("hash_name", "combo_test_hash")
	metaData.PutValue("field_name", "combo_field")
	msg := ctx.NewMsg("TEST_MSG_TYPE_AA", metaData, "combo_value_from_msg")
	node.OnMsg(ctx, msg)

	time.Sleep(time.Second * 1)
}

// testRedisClientEdgeCases tests boundary conditions - initialization failures and boundary behavior
func testRedisClientEdgeCases(t *testing.T) {
	// Test 1: Initialization failure - empty cmd
	t.Run("InitFailureEmptyCmd", func(t *testing.T) {
		var node ClientNode
		var configuration = make(types.Configuration)
		configuration["Cmd"] = "" // An empty command should cause initialization failure
		configuration["Params"] = []interface{}{"test_key", "test_value"}
		configuration["PoolSize"] = 10
		configuration["Server"] = "127.0.0.1:6379"
		config := types.NewConfig()
		err := node.Init(config, configuration)
		// Initialization should fail
		if err == nil {
			t.Errorf("expected initialization to fail for empty cmd")
		}
		if err != nil && !strings.Contains(err.Error(), "cmd field cannot be empty") {
			t.Errorf("expected error message to contain 'cmd field cannot be empty', got: %s", err.Error())
		}
	})

	// Test 2: Initialization failed - nil parameters
	t.Run("InitFailureNilParam", func(t *testing.T) {
		var node ClientNode
		var configuration = make(types.Configuration)
		configuration["Cmd"] = "SET"
		configuration["Params"] = []interface{}{"test_key", nil} // The nil parameter should cause initialization failure
		configuration["PoolSize"] = 10
		configuration["Server"] = "127.0.0.1:6379"
		config := types.NewConfig()
		err := node.Init(config, configuration)
		// Initialization should fail
		if err == nil {
			t.Errorf("expected initialization to fail for nil param")
		}
		if err != nil && !strings.Contains(err.Error(), "param at index 1 is nil") {
			t.Errorf("expected error message to contain 'param at index 1 is nil', got: %s", err.Error())
		}
	})

	// Test 3: Successfully initialized but tested for metadata fields that do not exist
	t.Run("NonExistentMetadata", func(t *testing.T) {
		var node ClientNode
		var configuration = make(types.Configuration)
		configuration["Cmd"] = "SET"
		configuration["Params"] = []interface{}{"nonexistent_key", "${metadata.nonexistent_field}"}
		configuration["PoolSize"] = 10
		configuration["Server"] = "127.0.0.1:6379"
		config := types.NewConfig()
		err := node.Init(config, configuration)
		if err != nil {
			t.Errorf("err=%s", err)
		}
		ctx := test.NewRuleContext(config, func(msg types.RuleMsg, relationType string, err2 error) {
			// Fields that don't exist should be replaced with empty strings or left as is
			assert.Equal(t, types.Success, relationType)
		})
		metaData := types.NewMetadata()
		msg := ctx.NewMsg("TEST_MSG_TYPE_AA", metaData, "test_data")
		node.OnMsg(ctx, msg)
		time.Sleep(time.Millisecond * 500)
	})

	// Test 3: Complex nested expressions
	t.Run("ComplexNestedExpression", func(t *testing.T) {
		var node ClientNode
		var configuration = make(types.Configuration)
		configuration["Cmd"] = "SET"
		configuration["Params"] = []interface{}{"${metadata.prefix}_${metadata.suffix}", "${metadata.value}_${msg}"}
		configuration["PoolSize"] = 10
		configuration["Server"] = "127.0.0.1:6379"
		config := types.NewConfig()
		err := node.Init(config, configuration)
		if err != nil {
			t.Errorf("err=%s", err)
		}
		ctx := test.NewRuleContext(config, func(msg types.RuleMsg, relationType string, err2 error) {
			// Detailed output information is used for debugging
			t.Logf("RelationType: %s, Data: %s", relationType, msg.GetData())
			if err2 != nil {
				t.Logf("Error: %s", err2.Error())
			}
			// Strict assertion: Redis servers are available, and commands should be executed successfully
			if err2 != nil {
				t.Errorf("Unexpected error: %s", err2.Error())
			}
			assert.Equal(t, types.Success, relationType)
			assert.Equal(t, "OK", msg.GetData())
		})
		metaData := types.NewMetadata()
		metaData.PutValue("prefix", "test")
		metaData.PutValue("suffix", "key")
		metaData.PutValue("value", "test")
		msg := ctx.NewMsg("TEST_MSG_TYPE_AA", metaData, "nested")
		node.OnMsg(ctx, msg)
		time.Sleep(time.Millisecond * 500)
	})
}

// testRedisClientCmdExpression The test cmd field uses expressions to dynamically generate Redis commands
func testRedisClientCmdExpression(t *testing.T) {
	var node ClientNode
	var configuration = make(types.Configuration)
	// Use expressions to dynamically generate cmd commands
	configuration["Cmd"] = "${metadata.operation}"
	configuration["Params"] = []interface{}{"${metadata.key}", "${metadata.value}"}
	configuration["PoolSize"] = 10
	configuration["Server"] = "127.0.0.1:6379"
	config := types.NewConfig()
	err := node.Init(config, configuration)
	if err != nil {
		t.Errorf("err=%s", err)
	}
	ctx := test.NewRuleContext(config, func(msg types.RuleMsg, relationType string, err2 error) {
		assert.Equal(t, types.Success, relationType)
		// Check if the results are correct
		assert.Equal(t, "OK", msg.GetData())
	})
	metaData := types.NewMetadata()
	// Add dynamic command parameters to the metadata
	metaData.PutValue("operation", "SET")
	metaData.PutValue("key", "test_cmd_expr_key")
	metaData.PutValue("value", "test_cmd_expr_value")
	msg := ctx.NewMsg("TEST_MSG_TYPE_AA", metaData, "")
	node.OnMsg(ctx, msg)

	time.Sleep(time.Second * 1)
}

// testRedisClientCmdWithParamsExpression The test cmd field contains complex expressions for commands and parameters
func testRedisClientCmdWithParamsExpression(t *testing.T) {
	var node ClientNode
	var configuration = make(types.Configuration)
	// Use expressions to include commands and parameters in cmd
	configuration["Cmd"] = "SETEX"
	configuration["Params"] = []interface{}{"${metadata.key}", "${metadata.ttl}", "${metadata.value}"}
	configuration["PoolSize"] = 10
	configuration["Server"] = "127.0.0.1:6379"
	config := types.NewConfig()
	err := node.Init(config, configuration)
	if err != nil {
		t.Errorf("err=%s", err)
	}
	ctx := test.NewRuleContext(config, func(msg types.RuleMsg, relationType string, err2 error) {
		assert.Equal(t, types.Success, relationType)
		// Check if the results are correct
		assert.Equal(t, "OK", msg.GetData())
	})
	metaData := types.NewMetadata()
	// Add parameters to the metadata
	metaData.PutValue("key", "test_cmd_with_params_key")
	metaData.PutValue("value", "test_cmd_with_params_value")
	metaData.PutValue("ttl", "300")
	msg := ctx.NewMsg("TEST_MSG_TYPE_AA", metaData, "")
	node.OnMsg(ctx, msg)

	time.Sleep(time.Second * 1)
}

func testRedisClientHMGet(t *testing.T) {
	var node ClientNode
	var configuration = make(types.Configuration)
	configuration["Cmd"] = "HMGET"
	configuration["Params"] = []interface{}{"myhash", "field1"}
	configuration["PoolSize"] = 10
	configuration["Server"] = "127.0.0.1:6379"
	config := types.NewConfig()
	err := node.Init(config, configuration)
	if err != nil {
		t.Errorf("err=%s", err)
	}
	ctx := test.NewRuleContext(config, func(msg types.RuleMsg, relationType string, err2 error) {
		assert.Equal(t, types.Success, relationType)
		// Check if the results are correct
		assert.Equal(t, "[\"value1\"]", msg.GetData())
	})
	metaData := types.NewMetadata()
	// Add parameters to the metadata
	metaData.PutValue("key", "test")
	metaData.PutValue("value", `{"aa":"lala"}`)
	msg := ctx.NewMsg("TEST_MSG_TYPE_AA", metaData, "")
	node.OnMsg(ctx, msg)

	time.Sleep(time.Second * 1)
}

func testRedisClientHMSetFromExpr(t *testing.T) {
	var node ClientNode
	var configuration = make(types.Configuration)
	configuration["cmd"] = "HMSET"
	configuration["paramsExpr"] = "msg"
	configuration["poolSize"] = 10
	configuration["server"] = "127.0.0.1:6379"
	config := types.NewConfig()
	err := node.Init(config, configuration)
	if err != nil {
		t.Errorf("err=%s", err)
	}
	ctx := test.NewRuleContext(config, func(msg types.RuleMsg, relationType string, err2 error) {
		assert.Equal(t, types.Success, relationType)
		// Check if the results are correct
		assert.Equal(t, "OK", msg.GetData())
	})
	metaData := types.NewMetadata()
	msg := ctx.NewMsg("TEST_MSG_TYPE_AA", metaData, `["myhash2", "field1", "value1"]`)
	node.OnMsg(ctx, msg)

	time.Sleep(time.Second * 1)
}

func testRedisClientHMGetFromExpr(t *testing.T) {
	var node ClientNode
	var configuration = make(types.Configuration)
	configuration["cmd"] = "HMGET"
	configuration["paramsExpr"] = "msg"
	configuration["server"] = "127.0.0.1:6379"
	config := types.NewConfig()
	err := node.Init(config, configuration)
	if err != nil {
		t.Errorf("err=%s", err)
	}
	ctx := test.NewRuleContext(config, func(msg types.RuleMsg, relationType string, err2 error) {
		assert.Equal(t, types.Success, relationType)
		// Check if the results are correct
		assert.Equal(t, "[\"value1\"]", msg.GetData())
	})
	metaData := types.NewMetadata()
	msg := ctx.NewMsg("TEST_MSG_TYPE_AA", metaData, `["myhash", "field1"]`)
	node.OnMsg(ctx, msg)

	time.Sleep(time.Second * 1)
}

func testRedisClientFlushDB(t *testing.T) {
	var node ClientNode
	var configuration = make(types.Configuration)
	configuration["cmd"] = "FlushDB"
	configuration["server"] = "127.0.0.1:6379"
	config := types.NewConfig()
	err := node.Init(config, configuration)
	if err != nil {
		t.Errorf("err=%s", err)
	}
	ctx := test.NewRuleContext(config, func(msg types.RuleMsg, relationType string, err2 error) {
		assert.Equal(t, types.Success, relationType)
		// Check if the results are correct
		assert.Equal(t, "OK", msg.GetData())
	})
	metaData := types.NewMetadata()
	msg := ctx.NewMsg("TEST_MSG_TYPE_AA", metaData, ``)
	node.OnMsg(ctx, msg)

	time.Sleep(time.Second * 1)
}

// Test the new el.Template function - Provides commands and parameters together
func testRedisClientTemplateSetWithCombinedCmd(t *testing.T) {
	var node ClientNode
	var configuration = make(types.Configuration)
	// Using new template features, commands and parameters are provided together
	configuration["Cmd"] = "SET ${metadata.key} ${metadata.value}"
	configuration["PoolSize"] = 10
	configuration["Server"] = "127.0.0.1:6379"
	config := types.NewConfig()
	err := node.Init(config, configuration)
	if err != nil {
		t.Errorf("err=%s", err)
	}
	ctx := test.NewRuleContext(config, func(msg types.RuleMsg, relationType string, err2 error) {
		assert.Equal(t, types.Success, relationType)
		// Check if the results are correct
		assert.Equal(t, "OK", msg.GetData())
	})
	metaData := types.NewMetadata()
	// Add parameters to the metadata
	metaData.PutValue("key", "test_template_key")
	metaData.PutValue("value", "test_template_value")
	msg := ctx.NewMsg("TEST_MSG_TYPE_AA", metaData, "")
	node.OnMsg(ctx, msg)

	time.Sleep(time.Second * 1)
}

// Test the new el.Template feature - Use Params templates
func testRedisClientTemplateWithParams(t *testing.T) {
	var node ClientNode
	var configuration = make(types.Configuration)
	configuration["Cmd"] = "SET"
	configuration["Params"] = []interface{}{"${metadata.key}", "${msg}"}
	configuration["PoolSize"] = 10
	configuration["Server"] = "127.0.0.1:6379"
	config := types.NewConfig()
	err := node.Init(config, configuration)
	if err != nil {
		t.Errorf("err=%s", err)
	}
	ctx := test.NewRuleContext(config, func(msg types.RuleMsg, relationType string, err2 error) {
		assert.Equal(t, types.Success, relationType)
		// Check if the results are correct
		assert.Equal(t, "OK", msg.GetData())
	})
	metaData := types.NewMetadata()
	// Add parameters to the metadata
	metaData.PutValue("key", "test_params_template")
	msg := ctx.NewMsg("TEST_MSG_TYPE_AA", metaData, "template_message_data")
	node.OnMsg(ctx, msg)

	time.Sleep(time.Second * 1)
}

// Test complex Redis command templates
func testRedisClientTemplateComplexCmd(t *testing.T) {
	var node ClientNode
	var configuration = make(types.Configuration)
	// Testing complex Redis commands containing multiple parameters
	configuration["Cmd"] = "SETEX ${metadata.key} ${metadata.ttl} ${msg}"
	configuration["PoolSize"] = 10
	configuration["Server"] = "127.0.0.1:6379"
	config := types.NewConfig()
	err := node.Init(config, configuration)
	if err != nil {
		t.Errorf("err=%s", err)
	}
	ctx := test.NewRuleContext(config, func(msg types.RuleMsg, relationType string, err2 error) {
		assert.Equal(t, types.Success, relationType)
		// Check if the results are correct
		assert.Equal(t, "OK", msg.GetData())
	})
	metaData := types.NewMetadata()
	// Add parameters to the metadata
	metaData.PutValue("key", "test_complex_key")
	metaData.PutValue("ttl", "60")
	msg := ctx.NewMsg("TEST_MSG_TYPE_AA", metaData, "complex_value_with_expiry")
	node.OnMsg(ctx, msg)

	time.Sleep(time.Second * 1)
}

// Testing backward compatibility – ParamsExpr is still working
func testRedisClientBackwardCompatibility(t *testing.T) {
	var node ClientNode
	var configuration = make(types.Configuration)
	configuration["cmd"] = "HMSET"
	configuration["paramsExpr"] = "msg" // Use the old ParamsExpr
	configuration["poolSize"] = 10
	configuration["server"] = "127.0.0.1:6379"
	config := types.NewConfig()
	err := node.Init(config, configuration)
	if err != nil {
		t.Errorf("err=%s", err)
	}
	ctx := test.NewRuleContext(config, func(msg types.RuleMsg, relationType string, err2 error) {
		assert.Equal(t, types.Success, relationType)
		// Check if the results are correct
		assert.Equal(t, "OK", msg.GetData())
	})
	metaData := types.NewMetadata()
	msg := ctx.NewMsg("TEST_MSG_TYPE_AA", metaData, `["backward_compat_hash", "field1", "value1"]`)
	node.OnMsg(ctx, msg)

	time.Sleep(time.Second * 1)
}
