/*
 * Copyright 2024 The RuleGo Authors.
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
	"context"
	"github.com/redis/go-redis/v9"
	"github.com/rulego/rulego"
	"github.com/rulego/rulego/test/assert"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rulego/rulego/api/types"
	endpointApi "github.com/rulego/rulego/api/types/endpoint"
	"github.com/rulego/rulego/endpoint"
)

var testdataFolder = "../../testdata"

func TestRedisEndpoint(t *testing.T) {
	buf, err := os.ReadFile(testdataFolder + "/chain_msg_type_switch.json")
	if err != nil {
		t.Fatal(err)
	}
	config := rulego.NewConfig(types.WithDefaultPool())
	// 注册规则链
	_, _ = rulego.New("default", buf, rulego.WithConfig(config))

	// 启动redis接收服务
	ep, err := endpoint.Registry.New(Type, config, Config{
		Server: "127.0.0.1:6379",
	})
	if err != nil {
		t.Fatal(err)
	}
	count := int32(0)
	// 路由1
	router1 := endpoint.NewRouter().SetId("router1").From("device.msg.request,device.msg.response").Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		atomic.AddInt32(&count, 1)
		if exchange.In.Headers().Get("topic") == "device.msg.response" {
			assert.Equal(t, "this is response", exchange.In.GetMsg().Data)
			return false
		}
		assert.Equal(t, "test message", exchange.In.GetMsg().Data)
		return true
	}).To("chain:default").Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		// 往指定主题发送数据，用于响应
		exchange.Out.Headers().Add("topic", "device.msg.response")
		exchange.Out.SetBody([]byte("this is response"))
		return true
	}).End()
	//重复路由，无法注册
	router2 := endpoint.NewRouter().SetId("router1").From("device.msg.request,device.msg.response").Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		atomic.AddInt32(&count, 1)
		if exchange.In.Headers().Get("topic") == "device.msg.response" {
			assert.Equal(t, "this is response", exchange.In.GetMsg().Data)
			return false
		}
		assert.Equal(t, "test message", exchange.In.GetMsg().Data)
		return true
	}).To("chain:default").Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		// 往指定主题发送数据，用于响应
		exchange.Out.Headers().Add("topic", "device.msg.response")
		exchange.Out.SetBody([]byte("this is response"))
		return true
	}).End()

	// 注册路由
	_, err = ep.AddRouter(router1)
	if err != nil {
		t.Fatal(err)
	}
	_, err = ep.AddRouter(router2)
	assert.NotNil(t, err)
	// 启动服务
	err = ep.Start()
	if err != nil {
		t.Fatal(err)
	}

	// 测试发布和订阅
	redisClient := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})
	err = redisClient.Ping(context.Background()).Err()
	assert.Nil(t, err)
	// 发布消息到device.msg.request
	redisClient.Publish(context.TODO(), "device.msg.request", "test message")
	// 等待消息处理
	time.Sleep(time.Millisecond * 200)
	assert.Equal(t, int32(2), count)
	atomic.StoreInt32(&count, 0)

	router3 := endpoint.NewRouter().SetId("router3").From("device.msg.request").Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		atomic.AddInt32(&count, 1)
		if exchange.In.Headers().Get("topic") == "device.msg.response" {
			assert.Equal(t, "this is response", exchange.In.GetMsg().Data)
			return false
		}
		assert.Equal(t, "test message", exchange.In.GetMsg().Data)
		return true
	}).To("chain:default").Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		// 往指定主题发送数据，用于响应
		exchange.Out.Headers().Add("topic", "device.msg.response")
		exchange.Out.SetBody([]byte("this is response"))
		return true
	}).End()

	_, err = ep.AddRouter(router3)
	if err != nil {
		t.Fatal(err)
	}
	// 发布消息到device.msg.request
	redisClient.Publish(context.TODO(), "device.msg.request", "test message")
	// 等待消息处理
	time.Sleep(time.Millisecond * 200)
	assert.Equal(t, int32(4), count)
	atomic.StoreInt32(&count, 0)

	_ = ep.RemoveRouter("router3")

	redisClient.Publish(context.TODO(), "device.msg.request", "test message")
	// 等待消息处理
	time.Sleep(time.Millisecond * 200)
	assert.Equal(t, int32(2), count)
	atomic.StoreInt32(&count, 0)

	_ = ep.RemoveRouter("router1")

	redisClient.Publish(context.TODO(), "device.msg.request", "test message")
	// 等待消息处理
	time.Sleep(time.Millisecond * 200)
	assert.Equal(t, int32(0), count)
	atomic.StoreInt32(&count, 0)

	_, _ = ep.AddRouter(router1)

	redisClient.Publish(context.TODO(), "device.msg.request", "test message")
	// 等待消息处理
	time.Sleep(time.Millisecond * 200)
	assert.Equal(t, int32(2), count)
	atomic.StoreInt32(&count, 0)
}
