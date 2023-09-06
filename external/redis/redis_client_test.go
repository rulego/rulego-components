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
	TestRedisClientSetFromMetadata(t)
	TestRedisClientSetFromData(t)
	TestRedisClientGetOnMsg(t)
	TestRedisClientDelOnMsg(t)
}

//测试添加key/value
func TestRedisClientSetFromMetadata(t *testing.T) {
	var node ClientNode
	var configuration = make(types.Configuration)
	configuration["Cmd"] = "SET"
	configuration["Params"] = []interface{}{"${key}", "${value}"}
	configuration["PoolSize"] = 10
	configuration["Server"] = "192.168.82.82:6379"
	config := types.NewConfig()
	err := node.Init(config, configuration)
	if err != nil {
		t.Errorf("err=%s", err)
	}
	ctx := test.NewRuleContext(config, func(msg types.RuleMsg, relationType string) {
		assert.Equal(t, types.Success, relationType)
		// 检查结果是否正确
		assert.Equal(t, "OK", msg.Data)
	})
	metaData := types.NewMetadata()
	// 在元数据中添加参数
	metaData.PutValue("key", "test")
	metaData.PutValue("value", `{"aa":"lala"}`)
	msg := ctx.NewMsg("TEST_MSG_TYPE_AA", metaData, "")
	err = node.OnMsg(ctx, msg)
	if err != nil {
		t.Errorf("err=%s", err)
	}

	time.Sleep(time.Second * 1)

}

//测试添加key/value ,value使用msg payload
func TestRedisClientSetFromData(t *testing.T) {
	var node ClientNode
	var configuration = make(types.Configuration)
	configuration["Cmd"] = "SET"
	configuration["Params"] = []interface{}{"${key}", "$data"}
	configuration["PoolSize"] = 10
	configuration["Server"] = "192.168.82.82:6379"
	config := types.NewConfig()
	err := node.Init(config, configuration)
	if err != nil {
		t.Errorf("err=%s", err)
	}
	ctx := test.NewRuleContext(config, func(msg types.RuleMsg, relationType string) {
		assert.Equal(t, types.Success, relationType)
		// 检查结果是否正确
		assert.Equal(t, "OK", msg.Data)
	})
	metaData := types.NewMetadata()
	// 在元数据中添加参数
	metaData.PutValue("key", "test")
	msg := ctx.NewMsg("TEST_MSG_TYPE_AA", metaData, `{"aa":"lala"}`)
	err = node.OnMsg(ctx, msg)
	if err != nil {
		t.Errorf("err=%s", err)
	}

	time.Sleep(time.Second * 1)

}

//测试获取key
func TestRedisClientGetOnMsg(t *testing.T) {
	var node ClientNode
	var configuration = make(types.Configuration)
	configuration["Cmd"] = "GET"
	configuration["Params"] = []interface{}{"${key}"}
	configuration["PoolSize"] = 10
	configuration["Server"] = "192.168.82.82:6379"
	config := types.NewConfig()
	err := node.Init(config, configuration)
	if err != nil {
		t.Errorf("err=%s", err)
	}
	ctx := test.NewRuleContext(config, func(msg types.RuleMsg, relationType string) {
		assert.Equal(t, types.Success, relationType)
		// 检查结果是否正确
		assert.Equal(t, `{"aa":"lala"}`, msg.Data)
	})
	metaData := types.NewMetadata()
	// 在元数据中添加参数
	metaData.PutValue("key", "test")
	msg := ctx.NewMsg("TEST_MSG_TYPE_AA", metaData, "")
	err = node.OnMsg(ctx, msg)
	if err != nil {
		t.Errorf("err=%s", err)
	}

	time.Sleep(time.Second * 1)

}

//测试删除key
func TestRedisClientDelOnMsg(t *testing.T) {
	var node ClientNode
	var configuration = make(types.Configuration)
	configuration["Cmd"] = "DEL"
	configuration["Params"] = []interface{}{"${key}"}
	configuration["PoolSize"] = 10
	configuration["Server"] = "192.168.82.82:6379"
	config := types.NewConfig()
	err := node.Init(config, configuration)
	if err != nil {
		t.Errorf("err=%s", err)
	}
	ctx := test.NewRuleContext(config, func(msg types.RuleMsg, relationType string) {
		assert.Equal(t, types.Success, relationType)
		// 检查结果是否正确
		assert.Equal(t, "1", msg.Data)
	})
	metaData := types.NewMetadata()
	// 在元数据中添加参数
	metaData.PutValue("key", "test")
	msg := ctx.NewMsg("TEST_MSG_TYPE_AA", metaData, "")
	err = node.OnMsg(ctx, msg)
	if err != nil {
		t.Errorf("err=%s", err)
	}

	time.Sleep(time.Second * 1)

}
