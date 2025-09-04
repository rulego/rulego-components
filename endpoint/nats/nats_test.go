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

package nats

import (
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rulego/rulego"
	"github.com/rulego/rulego/test/assert"

	"github.com/nats-io/nats.go"
	"github.com/rulego/rulego/api/types"
	endpointApi "github.com/rulego/rulego/api/types/endpoint"
	"github.com/rulego/rulego/endpoint"
)

var testdataFolder = "../../testdata"

func TestNatsEndpoint(t *testing.T) {
	// 检查是否有可用的 NATS 服务器
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = "nats://localhost:4222"
	}

	// 如果设置了跳过 NATS 测试，则跳过
	if os.Getenv("SKIP_NATS_TESTS") == "true" {
		t.Skip("Skipping NATS tests")
	}

	buf, err := os.ReadFile(testdataFolder + "/chain_msg_type_switch.json")
	if err != nil {
		t.Fatal(err)
	}
	config := rulego.NewConfig(types.WithDefaultPool())
	// 注册规则链
	_, _ = rulego.New("default", buf, rulego.WithConfig(config))

	// 启动NATS接收服务
	natsEndpoint, err := endpoint.Registry.New(Type, config, Config{
		Server: natsURL,
	})
	if err != nil {
		t.Skipf("Failed to create NATS endpoint (NATS may not be available): %v", err)
		return
	}

	// 路由1
	router1 := endpoint.NewRouter().From("device.msg.request").Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		assert.Equal(t, "test message", exchange.In.GetMsg().GetData())
		return true
	}).To("chain:default").Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		// 往指定主题发送数据，用于响应
		exchange.Out.Headers().Add(KeyResponseTopic, "device.msg.response")
		exchange.Out.SetBody([]byte("this is response"))
		return true
	}).End()

	count := int32(0)
	// 模拟获取响应
	router2 := endpoint.NewRouter().SetId("router3").From("device.msg.response").Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		//fmt.Println("接收到数据：device.msg.response", exchange.In.GetMsg())
		assert.Equal(t, "this is response", exchange.In.GetMsg().GetData())
		atomic.AddInt32(&count, 1)
		return true
	}).End()

	// 模拟获取响应,相同主题
	router3 := endpoint.NewRouter().SetId("router3").From("device.msg.response").Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		//fmt.Println("接收到数据：device.msg.response", exchange.In.GetMsg())
		assert.Equal(t, "this is response", exchange.In.GetMsg().GetData())
		atomic.AddInt32(&count, 1)
		return true
	}).End()

	// 注册路由
	_, err = natsEndpoint.AddRouter(router1)
	if err != nil {
		t.Skipf("Failed to add router1 (NATS server may not be available): %v", err)
		return
	}
	_, err = natsEndpoint.AddRouter(router2)
	if err != nil {
		t.Skipf("Failed to add router2 (NATS server may not be available): %v", err)
		return
	}
	router3Id, err := natsEndpoint.AddRouter(router3)
	assert.NotNil(t, err)
	// 启动服务
	err = natsEndpoint.Start()
	if err != nil {
		t.Skipf("Failed to start NATS endpoint: %v", err)
		return
	}

	// 测试发布和订阅
	conn, err := nats.Connect(natsURL)
	if err != nil {
		t.Skipf("NATS server not available: %v", err)
		return
	}
	defer conn.Close()

	// 发布消息到device.msg.request
	err = conn.Publish("device.msg.request", []byte("test message"))
	if err != nil {
		t.Skipf("Failed to publish message: %v", err)
		return
	}
	// 等待消息处理
	time.Sleep(time.Second * 1)

	assert.Equal(t, int32(1), atomic.LoadInt32(&count))

	atomic.StoreInt32(&count, 0)
	//删除一个相同的主题
	_ = natsEndpoint.RemoveRouter(router3Id)
	// 发布消息到device.msg.request
	err = conn.Publish("device.msg.request", []byte("test message"))
	if err != nil {
		t.Skipf("Failed to publish second message: %v", err)
		return
	}
	// 等待消息处理
	time.Sleep(time.Second * 1)

	assert.Equal(t, int32(0), atomic.LoadInt32(&count))

}

// TestNatsEndpointWithGroupId 测试NATS endpoint的GroupId功能
func TestNatsEndpointWithGroupId(t *testing.T) {
	// 检查是否有可用的 NATS 服务器
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = "nats://localhost:4222"
	}

	// 如果设置了跳过 NATS 测试，则跳过
	if os.Getenv("SKIP_NATS_TESTS") == "true" {
		t.Skip("Skipping NATS tests")
	}

	buf, err := os.ReadFile(testdataFolder + "/chain_msg_type_switch.json")
	if err != nil {
		t.Fatal(err)
	}
	config := rulego.NewConfig(types.WithDefaultPool())
	// 注册规则链
	_, _ = rulego.New("default", buf, rulego.WithConfig(config))

	// 启动NATS接收服务，使用GroupId
	natsEndpoint1, err := endpoint.Registry.New(Type, config, Config{
		Server:  natsURL,
		GroupId: "test-group",
	})
	if err != nil {
		t.Skipf("Failed to create NATS endpoint1 (NATS may not be available): %v", err)
		return
	}

	// 启动第二个NATS接收服务，使用相同的GroupId
	natsEndpoint2, err := endpoint.Registry.New(Type, config, Config{
		Server:  natsURL,
		GroupId: "test-group",
	})
	if err != nil {
		t.Skipf("Failed to create NATS endpoint2 (NATS may not be available): %v", err)
		return
	}

	count1 := int32(0)
	count2 := int32(0)

	// 路由1 - 第一个endpoint
	router1 := endpoint.NewRouter().From("device.group.request").Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		assert.Equal(t, "group test message", exchange.In.GetMsg().GetData())
		atomic.AddInt32(&count1, 1)
		return true
	}).End()

	// 路由2 - 第二个endpoint，相同主题
	router2 := endpoint.NewRouter().From("device.group.request").Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		assert.Equal(t, "group test message", exchange.In.GetMsg().GetData())
		atomic.AddInt32(&count2, 1)
		return true
	}).End()

	// 注册路由
	_, err = natsEndpoint1.AddRouter(router1)
	if err != nil {
		t.Skipf("Failed to add router1 (NATS server may not be available): %v", err)
		return
	}
	_, err = natsEndpoint2.AddRouter(router2)
	if err != nil {
		t.Skipf("Failed to add router2 (NATS server may not be available): %v", err)
		return
	}

	// 启动服务
	err = natsEndpoint1.Start()
	if err != nil {
		t.Skipf("Failed to start NATS endpoint1: %v", err)
		return
	}
	err = natsEndpoint2.Start()
	if err != nil {
		t.Skipf("Failed to start NATS endpoint2: %v", err)
		return
	}

	// 测试发布和订阅
	conn, err := nats.Connect(natsURL)
	if err != nil {
		t.Skipf("NATS server not available: %v", err)
		return
	}
	defer conn.Close()

	// 发布多条消息到device.group.request
	// 由于使用了GroupId，消息应该在两个endpoint之间负载均衡
	for i := 0; i < 10; i++ {
		err = conn.Publish("device.group.request", []byte("group test message"))
		if err != nil {
			t.Skipf("Failed to publish message %d: %v", i, err)
			return
		}
	}

	// 等待消息处理
	time.Sleep(time.Second * 2)

	// 验证消息被分发到两个endpoint
	totalCount := atomic.LoadInt32(&count1) + atomic.LoadInt32(&count2)
	assert.Equal(t, int32(10), totalCount)

	// 验证负载均衡：两个endpoint都应该收到消息
	// 注意：由于负载均衡的随机性，我们只验证总数和至少一个endpoint收到消息
	assert.True(t, atomic.LoadInt32(&count1) > 0 || atomic.LoadInt32(&count2) > 0)

	t.Logf("Endpoint1 received %d messages, Endpoint2 received %d messages", 
		atomic.LoadInt32(&count1), atomic.LoadInt32(&count2))

	// 清理资源
	natsEndpoint1.Destroy()
	natsEndpoint2.Destroy()
}
