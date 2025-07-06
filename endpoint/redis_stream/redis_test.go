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
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rulego/rulego"
	"github.com/rulego/rulego/test/assert"

	"github.com/rulego/rulego/api/types"
	endpointApi "github.com/rulego/rulego/api/types/endpoint"
	"github.com/rulego/rulego/endpoint"
)

var testdataFolder = "../../testdata"
var redisServer = "127.0.0.1:6379"

func Test2t(*testing.T) {
	// 测试发布和订阅
	redisClient := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})
	addData(redisClient, "device.msg.request")
}

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
			assert.Equal(t, "{\"value\":\"this is response\"}", exchange.In.GetMsg().GetData())
			return false
		}
		assert.Equal(t, "{\"field1\":\"value1\",\"field2\":\"42\"}", exchange.In.GetMsg().GetData())
		return true
	}).To("chain:default").Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		// 往指定主题发送数据，用于响应
		exchange.Out.Headers().Add(KeyResponseTopic, "device.msg.response")
		exchange.Out.SetBody([]byte("this is response"))
		return true
	}).End()
	//重复路由，无法注册
	router2 := endpoint.NewRouter().SetId("router1").From("device.msg.request,device.msg.response").Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		atomic.AddInt32(&count, 1)
		if exchange.In.Headers().Get("topic") == "device.msg.response" {
			assert.Equal(t, "{\"value\":\"this is response\"}", exchange.In.GetMsg().GetData())
			return false
		}
		assert.Equal(t, "{\"field1\":\"value1\",\"field2\":\"42\"}", exchange.In.GetMsg().GetData())
		return true
	}).To("chain:default").Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		// 往指定主题发送数据，用于响应
		exchange.Out.Headers().Add(KeyResponseTopic, "device.msg.response")
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
	// 发布数据
	addData(redisClient, "device.msg.request")
	// 等待消息处理
	time.Sleep(time.Millisecond * 500)

	assert.Equal(t, int32(2), atomic.LoadInt32(&count))
	atomic.StoreInt32(&count, 0)

	ep.RemoveRouter("router1")
	// 发布数据
	addData(redisClient, "device.msg.request")
	// 等待消息处理
	time.Sleep(time.Millisecond * 500)
	assert.Equal(t, int32(0), atomic.LoadInt32(&count))
	atomic.StoreInt32(&count, 0)
}

func addData(redisClient *redis.Client, streamName string) {
	// 定义要写入的消息，这里是一个简单的 key-value 形式
	// Redis 会自动为每个消息生成一个唯一的 ID
	message := map[string]interface{}{
		"field1": "value1",
		"field2": 42,
	}

	// 向 Stream 写入数据
	redisClient.XAdd(context.Background(), &redis.XAddArgs{
		Stream: streamName,
		ID:     "*", // 使用 "*" 表示使用 Redis 自动生成的 ID
		Values: message,
	}).Result()
}

// TestEndpoint is a placeholder to demonstrate basic endpoint functionality
func TestEndpoint(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr: redisServer,
	})
	err := client.Ping(context.Background()).Err()
	if err != nil {
		t.Skip("redis not available, skipping test")
	}
	_ = client.FlushDB(context.Background()).Err()
	defer client.Close()

	config := types.NewConfig()
	ep := &Endpoint{}
	err = ep.Init(config, types.Configuration{
		"server":  redisServer,
		"groupId": "test_group",
	})
	assert.Nil(t, err)
	assert.Equal(t, Type, ep.Type())

	router := endpoint.NewRouter().From("test_stream").End()
	routerId, err := ep.AddRouter(router)
	assert.Nil(t, err)
	assert.NotEqual(t, "", routerId)

	err = ep.RemoveRouter(routerId)
	assert.Nil(t, err)
	ep.Destroy()
}

// TestRedisStreamEndpointLifecycle tests that the redis stream endpoint can start, receive a message,
// be destroyed, and not receive any more messages.
func TestRedisStreamEndpointLifecycle(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr: redisServer,
	})
	err := client.Ping(context.Background()).Err()
	if err != nil {
		t.Skip("redis not available, skipping test")
	}
	_ = client.FlushDB(context.Background()).Err()
	defer client.Close()

	config := types.NewConfig()

	ep := &Endpoint{}
	err = ep.Init(config, types.Configuration{
		"server":  redisServer,
		"groupId": "test_group_lifecycle",
	})
	assert.Nil(t, err)

	msgChan := make(chan bool, 1)
	streamName := "test_stream_lifecycle"

	router := endpoint.NewRouter().From(streamName).Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		if strings.Contains(string(exchange.In.Body()), "msg1") {
			msgChan <- true
		}
		return true
	}).End()

	_, err = ep.AddRouter(router)
	assert.Nil(t, err)

	// Publish first message, should be received
	_, err = client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: streamName,
		Values: map[string]interface{}{"data": "msg1"},
	}).Result()
	assert.Nil(t, err)

	select {
	case <-msgChan:
		// Message received, as expected
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for message before destroy")
	}

	// Destroy the endpoint
	ep.Destroy()
	// Wait a bit for graceful shutdown
	time.Sleep(200 * time.Millisecond)

	// Publish second message, should NOT be received
	_, err = client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: streamName,
		Values: map[string]interface{}{"data": "msg2"},
	}).Result()
	assert.Nil(t, err)

	select {
	case <-msgChan:
		t.Fatal("received message after endpoint was destroyed")
	case <-time.After(1 * time.Second):
		// No message received, as expected
	}
}
