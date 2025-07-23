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

package nsq

import (
	"io"
	"log"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nsqio/go-nsq"
	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
	endpointApi "github.com/rulego/rulego/api/types/endpoint"
	"github.com/rulego/rulego/endpoint"
	"github.com/rulego/rulego/test/assert"
)

var testdataFolder = "../../testdata"

func TestNsqEndpoint(t *testing.T) {
	// 检查是否有可用的 NSQ 服务器
	nsqdAddress := os.Getenv("NSQD_ADDRESS")
	if nsqdAddress == "" {
		nsqdAddress = "127.0.0.1:4150"
	}

	lookupdAddress := os.Getenv("LOOKUPD_ADDRESS")
	if lookupdAddress == "" {
		lookupdAddress = "127.0.0.1:4161"
	}

	// 如果设置了跳过 NSQ 测试，则跳过
	if os.Getenv("SKIP_NSQ_TESTS") == "true" {
		t.Skip("Skipping NSQ tests")
	}

	buf, err := os.ReadFile(testdataFolder + "/chain_msg_type_switch.json")
	if err != nil {
		t.Fatal(err)
	}
	config := rulego.NewConfig(types.WithDefaultPool())
	// 注册规则链
	_, _ = rulego.New("default", buf, rulego.WithConfig(config))

	// 启动NSQ接收服务
	nsqEndpoint, err := endpoint.Registry.New(Type, config, Config{
		Server: nsqdAddress,
	})
	if err != nil {
		t.Skipf("Failed to create NSQ endpoint (NSQ may not be available): %v", err)
		return
	}

	// 路由1
	router1 := endpoint.NewRouter().From("device_msg_request").Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		assert.Equal(t, "test message", exchange.In.GetMsg().GetData())
		return true
	}).To("chain:default").Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		// 往指定主题发送数据，用于响应
		exchange.Out.Headers().Add(KeyResponseTopic, "device_msg_response")
		exchange.Out.SetBody([]byte("this is response"))
		return true
	}).End()

	count := int32(0)
	// 模拟获取响应
	router2 := endpoint.NewRouter().SetId("router2").From("device_msg_response").Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		//fmt.Println("接收到数据：device_msg_response", exchange.In.GetMsg())
		assert.Equal(t, "this is response", exchange.In.GetMsg().GetData())
		atomic.AddInt32(&count, 1)
		return true
	}).End()

	// 模拟获取响应,相同主题
	router3 := endpoint.NewRouter().SetId("router3").From("device_msg_response").Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		//fmt.Println("接收到数据：device_msg_response", exchange.In.GetMsg())
		assert.Equal(t, "this is response", exchange.In.GetMsg().GetData())
		atomic.AddInt32(&count, 1)
		return true
	}).End()

	// 注册路由
	_, err = nsqEndpoint.AddRouter(router1, "channel1")
	if err != nil {
		t.Skipf("Failed to add router1 (NSQ server may not be available): %v", err)
		return
	}
	_, err = nsqEndpoint.AddRouter(router2, "channel2")
	if err != nil {
		t.Skipf("Failed to add router2 (NSQ server may not be available): %v", err)
		return
	}
	router3Id, err := nsqEndpoint.AddRouter(router3, "channel2")
	assert.Nil(t, err)

	// 测试重复添加相同路由ID，应该报错
	_, err = nsqEndpoint.AddRouter(router3, "channel2")
	assert.NotNil(t, err)
	if !strings.Contains(err.Error(), "already exists") {
		t.Errorf("Expected error to contain 'already exists', got: %v", err)
	}

	// 启动服务
	err = nsqEndpoint.Start()
	if err != nil {
		t.Skipf("Failed to start NSQ endpoint: %v", err)
		return
	}

	// 等待消费者连接
	time.Sleep(time.Second * 2)

	// 测试发布消息
	producerConfig := nsq.NewConfig()
	producer, err := nsq.NewProducer(nsqdAddress, producerConfig)
	if err != nil {
		t.Skipf("NSQ server not available: %v", err)
		return
	}
	// 禁用NSQ内部日志输出
	producer.SetLogger(log.New(io.Discard, "", 0), nsq.LogLevelError)
	defer producer.Stop()

	// 发布消息到device_msg_request
	err = producer.Publish("device_msg_request", []byte("test message"))
	if err != nil {
		t.Skipf("Failed to publish message: %v", err)
		return
	}
	// 等待消息处理
	time.Sleep(time.Second * 3)

	// 由于router2和router3监听不同的channel，只有router2会处理device_msg_response消息
	assert.Equal(t, int32(1), atomic.LoadInt32(&count))

	atomic.StoreInt32(&count, 0)
	//删除router3
	_ = nsqEndpoint.RemoveRouter(router3Id)
	// 发布消息到device_msg_request
	err = producer.Publish("device_msg_request", []byte("test message"))
	if err != nil {
		t.Skipf("Failed to publish second message: %v", err)
		return
	}
	// 等待消息处理
	time.Sleep(time.Second * 3)

	// 删除router3后，仍然只有router2处理消息
	assert.Equal(t, int32(1), atomic.LoadInt32(&count))

	// 清理
	nsqEndpoint.Destroy()
}

func TestNsqEndpointWithNsqd(t *testing.T) {
	// 检查是否有可用的 NSQ 服务器
	nsqdAddress := os.Getenv("NSQD_ADDRESS")
	if nsqdAddress == "" {
		nsqdAddress = "127.0.0.1:4150"
	}

	// 如果设置了跳过 NSQ 测试，则跳过
	if os.Getenv("SKIP_NSQ_TESTS") == "true" {
		t.Skip("Skipping NSQ tests")
	}

	buf, err := os.ReadFile(testdataFolder + "/chain_msg_type_switch.json")
	if err != nil {
		t.Fatal(err)
	}
	config := rulego.NewConfig(types.WithDefaultPool())
	// 注册规则链
	_, _ = rulego.New("default", buf, rulego.WithConfig(config))

	// 启动NSQ接收服务（仅使用nsqd）
	nsqEndpoint, err := endpoint.Registry.New(Type, config, Config{
		Server: nsqdAddress,
	})
	if err != nil {
		t.Skipf("Failed to create NSQ endpoint (NSQ may not be available): %v", err)
		return
	}

	count := int32(0)
	// 路由
	router := endpoint.NewRouter().From("test_topic").Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		assert.Equal(t, "test message", exchange.In.GetMsg().GetData())
		atomic.AddInt32(&count, 1)
		return true
	}).End()

	// 注册路由
	_, err = nsqEndpoint.AddRouter(router)
	if err != nil {
		t.Skipf("Failed to add router (NSQ server may not be available): %v", err)
		return
	}

	// 启动服务
	err = nsqEndpoint.Start()
	if err != nil {
		t.Skipf("Failed to start NSQ endpoint: %v", err)
		return
	}

	// 等待消费者连接
	time.Sleep(time.Second * 2)

	// 测试发布消息
	producerConfig := nsq.NewConfig()
	producer, err := nsq.NewProducer(nsqdAddress, producerConfig)
	if err != nil {
		t.Skipf("NSQ server not available: %v", err)
		return
	}
	// 禁用NSQ内部日志输出
	producer.SetLogger(log.New(io.Discard, "", 0), nsq.LogLevelError)
	defer producer.Stop()

	// 发布消息
	err = producer.Publish("test_topic", []byte("test message"))
	if err != nil {
		t.Skipf("Failed to publish message: %v", err)
		return
	}
	// 等待消息处理
	time.Sleep(time.Second * 3)

	assert.Equal(t, int32(1), atomic.LoadInt32(&count))

	// 清理
	nsqEndpoint.Destroy()
}
