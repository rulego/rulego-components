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
