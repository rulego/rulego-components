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
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/test"
	"github.com/rulego/rulego/test/assert"
	"testing"
	"time"
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
		// 检查结果是否正确
		assert.Equal(t, "OK", msg.Data)
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
		// 检查结果是否正确
		assert.Equal(t, "OK", msg.Data)
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
		assert.Equal(t, `{"aa":"lala"}`, msg.Data)
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
		// 检查结果是否正确
		assert.Equal(t, "1", msg.Data)
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
		// 检查结果是否正确
		assert.Equal(t, "OK", msg.Data)
	})
	metaData := types.NewMetadata()
	// 在元数据中添加参数
	metaData.PutValue("key", "test")
	metaData.PutValue("value", `{"aa":"lala"}`)
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
		assert.Equal(t, "[\"value1\"]", msg.Data)
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
		assert.Equal(t, "OK", msg.Data)
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
		assert.Equal(t, "[\"value1\"]", msg.Data)
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
		assert.Equal(t, "OK", msg.Data)
	})
	metaData := types.NewMetadata()
	msg := ctx.NewMsg("TEST_MSG_TYPE_AA", metaData, ``)
	node.OnMsg(ctx, msg)

	time.Sleep(time.Second * 1)
}
