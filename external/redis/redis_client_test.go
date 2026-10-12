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

	"github.com/redis/go-redis/v9"
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
	// 新的el.Template功能测试
	testRedisClientTemplateSetWithCombinedCmd(t)
	testRedisClientTemplateWithParams(t)
	testRedisClientTemplateComplexCmd(t)
	testRedisClientBackwardCompatibility(t)
	// 完善的表达式测试
	testRedisClientCmdExpression(t)
	testRedisClientCmdWithParamsExpression(t)
	testRedisClientParamsMixedExpression(t)
	testRedisClientCmdParamsCombo(t)
	testRedisClientEdgeCases(t)
	// cmd 内联引号切分
	testRedisClientInlineCmdQuotes(t)
	// outputTo 输出位置
	testRedisClientOutputToMetadata(t)
}

// 测试添加key/value
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
		// 检查结果是否正确 - HMSET命令返回OK表示成功
		assert.Equal(t, "OK", msg.GetData())
	})
	metaData := types.NewMetadata()
	// 在元数据中添加参数
	metaData.PutValue("key", "test")
	metaData.PutValue("value", `{"aa":"lala"}`)
	msg := ctx.NewMsg("TEST_MSG_TYPE_AA", metaData, "")
	node.OnMsg(ctx, msg)

	time.Sleep(time.Second * 1)

}

// 测试添加key/value ,value使用msg payload
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
		// 检查结果是否正确 - HMSET命令返回OK表示成功
		assert.Equal(t, "OK", msg.GetData())
	})
	metaData := types.NewMetadata()
	// 在元数据中添加参数
	metaData.PutValue("key", "test")
	msg := ctx.NewMsg("TEST_MSG_TYPE_AA", metaData, `{"aa":"lala"}`)
	node.OnMsg(ctx, msg)

	time.Sleep(time.Second * 1)

}

// 测试获取key
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
		// 检查结果是否正确
		assert.Equal(t, `{"aa":"lala"}`, msg.GetData())
	})
	metaData := types.NewMetadata()
	// 在元数据中添加参数
	metaData.PutValue("key", "test")
	msg := ctx.NewMsg("TEST_MSG_TYPE_AA", metaData, "")
	node.OnMsg(ctx, msg)

	time.Sleep(time.Second * 1)

}

// 测试删除key
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
		// 检查结果是否正确 - DEL命令返回删除的键数量
		assert.Equal(t, "1", msg.GetData())
	})
	metaData := types.NewMetadata()
	// 在元数据中添加参数
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
		// 检查结果是否正确 - HMSET命令返回OK表示成功
		assert.Equal(t, "OK", msg.GetData())
	})
	metaData := types.NewMetadata()
	// 在元数据中添加参数
	metaData.PutValue("key", "test")
	metaData.PutValue("value", `{"aa":"lala"}`)
	msg := ctx.NewMsg("TEST_MSG_TYPE_AA", metaData, "")
	node.OnMsg(ctx, msg)

	time.Sleep(time.Second * 1)
}

// testRedisClientParamsMixedExpression 测试params数组中同时包含静态参数和表达式参数
func testRedisClientParamsMixedExpression(t *testing.T) {
	var node ClientNode
	var configuration = make(types.Configuration)
	// 使用混合参数：静态参数和表达式参数
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
		// 检查结果是否正确 - HMSET命令返回OK表示成功
		assert.Equal(t, "OK", msg.GetData())
	})
	metaData := types.NewMetadata()
	// 在元数据中添加动态参数
	metaData.PutValue("value1", "dynamic_value_from_metadata")
	msg := ctx.NewMsg("TEST_MSG_TYPE_AA", metaData, "dynamic_value_from_msg")
	node.OnMsg(ctx, msg)

	time.Sleep(time.Second * 1)
}

// testRedisClientCmdParamsCombo 测试cmd表达式与params表达式的组合使用
func testRedisClientCmdParamsCombo(t *testing.T) {
	var node ClientNode
	var configuration = make(types.Configuration)
	// cmd使用表达式，params也使用表达式
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
		// 检查结果是否正确
		assert.Equal(t, "1", msg.GetData())
	})
	metaData := types.NewMetadata()
	// 在元数据中添加cmd和params的动态参数
	metaData.PutValue("cmd_type", "HSET")
	metaData.PutValue("hash_name", "combo_test_hash")
	metaData.PutValue("field_name", "combo_field")
	msg := ctx.NewMsg("TEST_MSG_TYPE_AA", metaData, "combo_value_from_msg")
	node.OnMsg(ctx, msg)

	time.Sleep(time.Second * 1)
}

// testRedisClientEdgeCases 测试边界情况 - 初始化失败和边界行为
func testRedisClientEdgeCases(t *testing.T) {
	// 测试1: 初始化失败 - 空Cmd
	t.Run("InitFailureEmptyCmd", func(t *testing.T) {
		var node ClientNode
		var configuration = make(types.Configuration)
		configuration["Cmd"] = "" // 空命令应该导致初始化失败
		configuration["Params"] = []interface{}{"test_key", "test_value"}
		configuration["PoolSize"] = 10
		configuration["Server"] = "127.0.0.1:6379"
		config := types.NewConfig()
		err := node.Init(config, configuration)
		// 应该初始化失败
		if err == nil {
			t.Errorf("expected initialization to fail for empty cmd")
		}
		if err != nil && !strings.Contains(err.Error(), "cmd field cannot be empty") {
			t.Errorf("expected error message to contain 'cmd field cannot be empty', got: %s", err.Error())
		}
	})

	// 测试2: 初始化失败 - nil参数
	t.Run("InitFailureNilParam", func(t *testing.T) {
		var node ClientNode
		var configuration = make(types.Configuration)
		configuration["Cmd"] = "SET"
		configuration["Params"] = []interface{}{"test_key", nil} // nil参数应该导致初始化失败
		configuration["PoolSize"] = 10
		configuration["Server"] = "127.0.0.1:6379"
		config := types.NewConfig()
		err := node.Init(config, configuration)
		// 应该初始化失败
		if err == nil {
			t.Errorf("expected initialization to fail for nil param")
		}
		if err != nil && !strings.Contains(err.Error(), "param at index 1 is nil") {
			t.Errorf("expected error message to contain 'param at index 1 is nil', got: %s", err.Error())
		}
	})

	// 测试3: 成功初始化但测试不存在的元数据字段
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
			// 不存在的字段应该被替换为空字符串或保持原样
			assert.Equal(t, types.Success, relationType)
		})
		metaData := types.NewMetadata()
		msg := ctx.NewMsg("TEST_MSG_TYPE_AA", metaData, "test_data")
		node.OnMsg(ctx, msg)
		time.Sleep(time.Millisecond * 500)
	})

	// 测试3: 复杂嵌套表达式
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
			// 输出详细信息用于调试
			t.Logf("RelationType: %s, Data: %s", relationType, msg.GetData())
			if err2 != nil {
				t.Logf("Error: %s", err2.Error())
			}
			// 严格断言：Redis服务器可用，命令应该成功执行
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

// testRedisClientCmdExpression 测试cmd字段使用表达式动态生成Redis命令
func testRedisClientCmdExpression(t *testing.T) {
	var node ClientNode
	var configuration = make(types.Configuration)
	// 使用表达式动态生成cmd命令
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
		// 检查结果是否正确
		assert.Equal(t, "OK", msg.GetData())
	})
	metaData := types.NewMetadata()
	// 在元数据中添加动态命令参数
	metaData.PutValue("operation", "SET")
	metaData.PutValue("key", "test_cmd_expr_key")
	metaData.PutValue("value", "test_cmd_expr_value")
	msg := ctx.NewMsg("TEST_MSG_TYPE_AA", metaData, "")
	node.OnMsg(ctx, msg)

	time.Sleep(time.Second * 1)
}

// testRedisClientCmdWithParamsExpression 测试cmd字段包含命令和参数的复杂表达式
func testRedisClientCmdWithParamsExpression(t *testing.T) {
	var node ClientNode
	var configuration = make(types.Configuration)
	// 使用表达式在cmd中包含命令和参数
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
		// 检查结果是否正确
		assert.Equal(t, "OK", msg.GetData())
	})
	metaData := types.NewMetadata()
	// 在元数据中添加参数
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
		// 检查结果是否正确
		assert.Equal(t, "[\"value1\"]", msg.GetData())
	})
	metaData := types.NewMetadata()
	// 在元数据中添加参数
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
		// 检查结果是否正确
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
		// 检查结果是否正确
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
		// 检查结果是否正确
		assert.Equal(t, "OK", msg.GetData())
	})
	metaData := types.NewMetadata()
	msg := ctx.NewMsg("TEST_MSG_TYPE_AA", metaData, ``)
	node.OnMsg(ctx, msg)

	time.Sleep(time.Second * 1)
}

// 测试新的el.Template功能 - 命令和参数一起提供
func testRedisClientTemplateSetWithCombinedCmd(t *testing.T) {
	var node ClientNode
	var configuration = make(types.Configuration)
	// 使用新的模板功能，命令和参数一起提供
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
		// 检查结果是否正确
		assert.Equal(t, "OK", msg.GetData())
	})
	metaData := types.NewMetadata()
	// 在元数据中添加参数
	metaData.PutValue("key", "test_template_key")
	metaData.PutValue("value", "test_template_value")
	msg := ctx.NewMsg("TEST_MSG_TYPE_AA", metaData, "")
	node.OnMsg(ctx, msg)

	time.Sleep(time.Second * 1)
}

// 测试新的el.Template功能 - 使用Params模板
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
		// 检查结果是否正确
		assert.Equal(t, "OK", msg.GetData())
	})
	metaData := types.NewMetadata()
	// 在元数据中添加参数
	metaData.PutValue("key", "test_params_template")
	msg := ctx.NewMsg("TEST_MSG_TYPE_AA", metaData, "template_message_data")
	node.OnMsg(ctx, msg)

	time.Sleep(time.Second * 1)
}

// 测试复杂的Redis命令模板
func testRedisClientTemplateComplexCmd(t *testing.T) {
	var node ClientNode
	var configuration = make(types.Configuration)
	// 测试复杂的Redis命令，包含多个参数
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
		// 检查结果是否正确
		assert.Equal(t, "OK", msg.GetData())
	})
	metaData := types.NewMetadata()
	// 在元数据中添加参数
	metaData.PutValue("key", "test_complex_key")
	metaData.PutValue("ttl", "60")
	msg := ctx.NewMsg("TEST_MSG_TYPE_AA", metaData, "complex_value_with_expiry")
	node.OnMsg(ctx, msg)

	time.Sleep(time.Second * 1)
}

// 测试向后兼容性 - ParamsExpr仍然工作
func testRedisClientBackwardCompatibility(t *testing.T) {
	var node ClientNode
	var configuration = make(types.Configuration)
	configuration["cmd"] = "HMSET"
	configuration["paramsExpr"] = "msg" // 使用旧的ParamsExpr
	configuration["poolSize"] = 10
	configuration["server"] = "127.0.0.1:6379"
	config := types.NewConfig()
	err := node.Init(config, configuration)
	if err != nil {
		t.Errorf("err=%s", err)
	}
	ctx := test.NewRuleContext(config, func(msg types.RuleMsg, relationType string, err2 error) {
		assert.Equal(t, types.Success, relationType)
		// 检查结果是否正确
		assert.Equal(t, "OK", msg.GetData())
	})
	metaData := types.NewMetadata()
	msg := ctx.NewMsg("TEST_MSG_TYPE_AA", metaData, `["backward_compat_hash", "field1", "value1"]`)
	node.OnMsg(ctx, msg)

	time.Sleep(time.Second * 1)
}

func TestClientNodeConnectionStatus(t *testing.T) {
	var node ClientNode
	config := types.NewConfig()
	err := node.Init(config, types.Configuration{
		"Server": "127.0.0.1:6379",
		"Cmd":    "GET",
		"Params": []interface{}{"k"},
	})
	assert.Nil(t, err)
	if _, err := node.SharedNode.GetSafely(); err != nil {
		t.Skipf("redis server not available: %v", err)
	}
	assert.Equal(t, types.StatusConnected, node.ConnectionStatus().Status)
	node.Destroy()
	assert.Equal(t, types.StatusDisconnected, node.ConnectionStatus().Status)
}

func TestClientNodeConnectionStatusUnreachable(t *testing.T) {
	var node ClientNode
	config := types.NewConfig()
	err := node.Init(config, types.Configuration{
		"Server": "127.0.0.1:1",
		"Cmd":    "GET",
		"Params": []interface{}{"k"},
	})
	assert.Nil(t, err)
	_, err = node.SharedNode.GetSafely()
	assert.NotNil(t, err)
	info := node.ConnectionStatus()
	assert.Equal(t, types.StatusReconnecting, info.Status)
	assert.True(t, info.Message != "")
}

// 测试outputTo=metadata：SET 结果进 metadata，msg.Data 保留原负荷
func testRedisClientOutputToMetadata(t *testing.T) {
	var node ClientNode
	var configuration = make(types.Configuration)
	configuration["Cmd"] = "SET"
	configuration["Params"] = []interface{}{"${metadata.key}", "${data}"}
	configuration["OutputTo"] = OutputMetadata
	configuration["PoolSize"] = 10
	configuration["Server"] = "127.0.0.1:6379"
	config := types.NewConfig()
	err := node.Init(config, configuration)
	if err != nil {
		t.Errorf("err=%s", err)
	}
	ctx := test.NewRuleContext(config, func(msg types.RuleMsg, relationType string, err2 error) {
		assert.Equal(t, types.Success, relationType)
		assert.Equal(t, `{"aa":"lala"}`, msg.GetData())
		assert.Equal(t, "OK", msg.Metadata.GetValue(KeyResult))
	})
	metaData := types.NewMetadata()
	metaData.PutValue("key", "test_output_to")
	msg := ctx.NewMsg("TEST_MSG_TYPE_AA", metaData, `{"aa":"lala"}`)
	node.OnMsg(ctx, msg)

	time.Sleep(time.Second * 1)
}

// 测试outputTo非法值：Init 报错，不静默回落到覆盖 data 的行为
func TestClientNodeOutputToInvalid(t *testing.T) {
	var node ClientNode
	config := types.NewConfig()
	err := node.Init(config, types.Configuration{
		"Server":   "127.0.0.1:6379",
		"Cmd":      "GET",
		"Params":   []interface{}{"k"},
		"OutputTo": "metdata",
	})
	assert.NotNil(t, err)
}

// 测试cmd内联引号切分：成对引号是空串、引号内空格不切分、裸JSON原样
func testRedisClientInlineCmdQuotes(t *testing.T) {
	var node ClientNode
	config := types.NewConfig()
	ctx := test.NewRuleContext(config, func(msg types.RuleMsg, relationType string, err2 error) {
		assert.Equal(t, types.Success, relationType)
	})
	run := func(cmd string) {
		var configuration = make(types.Configuration)
		configuration["Server"] = "127.0.0.1:6379"
		configuration["Cmd"] = cmd
		if err := node.Init(config, configuration); err != nil {
			t.Fatalf("err=%s", err)
		}
		node.OnMsg(ctx, ctx.NewMsg("TEST_MSG_TYPE_AA", types.NewMetadata(), ""))
		time.Sleep(time.Second)
	}

	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	defer func() {
		_ = client.Close()
	}()
	get := func(key string) string {
		v, err := client.Get(t.Context(), key).Result()
		if err != nil {
			t.Fatalf("get %s err=%s", key, err)
		}
		return v
	}

	run(`SET inline:quotes:empty ""`)
	assert.Equal(t, "", get("inline:quotes:empty"))
	run(`SET inline:quotes:spaced "a b"`)
	assert.Equal(t, "a b", get("inline:quotes:spaced"))
	run(`SET inline:quotes:json {"a":1}`)
	assert.Equal(t, `{"a":1}`, get("inline:quotes:json"))
}

func TestSplitCmdTokens(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want []string
	}{
		{"普通命令", "SET key value", []string{"SET", "key", "value"}},
		{"成对双引号是空串", `SET mes:line:current ""`, []string{"SET", "mes:line:current", ""}},
		{"成对单引号是空串", `SET k ''`, []string{"SET", "k", ""}},
		{"双引号剥壳", `SET k "abc"`, []string{"SET", "k", "abc"}},
		{"单引号剥壳", `SET k 'abc'`, []string{"SET", "k", "abc"}},
		{"引号内空格不切分", `SET k "a b c"`, []string{"SET", "k", "a b c"}},
		{"单引号包裹带空格JSON", `JSON.SET k $ '{"a": 1}'`, []string{"JSON.SET", "k", "$", `{"a": 1}`}},
		{"裸紧凑JSON不受影响", `SET k {"a":1}`, []string{"SET", "k", `{"a":1}`}},
		{"token中间引号是字面量", `SET k {"a":1}b`, []string{"SET", "k", `{"a":1}b`}},
		{"反斜杠是字面量", `SET k C:\dir\file`, []string{"SET", "k", `C:\dir\file`}},
		{"嵌套异类引号剥外层", `SET k "'x'"`, []string{"SET", "k", `'x'`}},
		{"未闭合引号原样保留", `SET k "abc`, []string{"SET", "k", `"abc`}},
		{"部分包裹不剥壳", `SET k "a"b`, []string{"SET", "k", `"a"b`}},
		{"多个引号参数各自剥壳", `SET k "a" 'b'`, []string{"SET", "k", "a", "b"}},
		{"首尾空白", "  SET key value  ", []string{"SET", "key", "value"}},
		{"连续空白", "SET\tkey\n\rvalue", []string{"SET", "key", "value"}},
		{"空白输入", "   ", nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := splitCmdTokens(c.in)
			assert.Equal(t, len(c.want), len(got))
			for i := range c.want {
				assert.Equal(t, c.want[i], got[i])
			}
		})
	}
}
