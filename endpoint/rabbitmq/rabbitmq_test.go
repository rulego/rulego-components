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

package rabbitmq

import (
	"os"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rulego/rulego"
	"github.com/rulego/rulego/test/assert"

	"github.com/rulego/rulego/api/types"
	endpointApi "github.com/rulego/rulego/api/types/endpoint"
	"github.com/rulego/rulego/endpoint"
)

var testdataFolder = "../../testdata"

const (
	exchange      = "rulego.topic.test"
	topicRequest  = "device.msg.request"
	topicResponse = "device.msg.response"
)

func TestEndpoint(t *testing.T) {
	// 从环境变量获取RabbitMQ服务器地址
	server := os.Getenv("RABBITMQ_URL")
	if server == "" {
		server = "amqp://guest:guest@localhost:5672/"
	}

	// 如果设置了跳过 RabbitMQ 测试，则跳过
	if os.Getenv("SKIP_RABBITMQ_TESTS") == "true" {
		t.Skip("Skipping RabbitMQ tests")
	}

	buf, err := os.ReadFile(testdataFolder + "/chain_msg_type_switch.json")
	if err != nil {
		t.Fatal(err)
	}
	config := rulego.NewConfig(types.WithDefaultPool())
	// 注册规则链
	_, _ = rulego.New("default", buf, rulego.WithConfig(config))

	// 启动enpoint接收服务
	ep, err := endpoint.Registry.New(Type, config, Config{
		Server:   server,
		Exchange: exchange,
	})
	if err != nil {
		t.Skipf("Failed to create RabbitMQ endpoint (RabbitMQ may not be available): %v", err)
		return
	}

	// 路由1
	router1 := endpoint.NewRouter().From(topicRequest).Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		assert.Equal(t, "test message", exchange.In.GetMsg().GetData())
		return true
	}).To("chain:default").Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		// 往指定主题发送数据，用于响应
		exchange.Out.Headers().Add(KeyResponseTopic, topicResponse)
		exchange.Out.SetBody([]byte("this is response"))
		return true
	}).End()

	count := int32(0)
	// 模拟获取响应
	router2 := endpoint.NewRouter().SetId("router3").From(topicResponse).Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		//fmt.Println("接收到数据：device.msg.response", exchange.In.GetMsg())
		assert.Equal(t, "this is response", exchange.In.GetMsg().GetData())
		atomic.AddInt32(&count, 1)
		return true
	}).End()

	// 模拟获取响应,相同主题
	router3 := endpoint.NewRouter().SetId("router3").From(topicResponse).Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		assert.Equal(t, "this is response", exchange.In.GetMsg().GetData())
		atomic.AddInt32(&count, 1)
		return true
	}).End()

	// 注册路由
	_, err = ep.AddRouter(router1)
	if err != nil {
		t.Skipf("Failed to add router1 (RabbitMQ server may not be available): %v", err)
		return
	}
	_, err = ep.AddRouter(router2)
	if err != nil {
		t.Skipf("Failed to add router2 (RabbitMQ server may not be available): %v", err)
		return
	}
	router3Id, err := ep.AddRouter(router3)
	assert.NotNil(t, err)
	// 启动服务
	err = ep.Start()
	if err != nil {
		t.Skipf("Failed to start RabbitMQ endpoint: %v", err)
		return
	}

	// 测试发布和订阅
	conn, err := amqp.Dial(server)
	if err != nil {
		t.Skipf("RabbitMQ server not available: %v", err)
		return
	}
	defer conn.Close()
	channel, err := conn.Channel()
	if err != nil {
		t.Skipf("Failed to create channel: %v", err)
		return
	}
	defer channel.Close()

	// 发布消息到device.msg.request
	err = channel.Publish(
		exchange,     // 发布到的交换机
		topicRequest, // 路由键
		false,        // 表示是否要求消息必须被路由到至少一个队列
		false,        // 是否要求消息立即被消费者接收
		amqp.Publishing{
			ContentType:     ContentTypeJson,
			ContentEncoding: KeyUTF8,
			Body:            []byte("test message"),
		})
	if err != nil {
		t.Skipf("Failed to publish message: %v", err)
		return
	}
	// 等待消息处理
	time.Sleep(time.Second * 1)

	assert.Equal(t, int32(1), atomic.LoadInt32(&count))

	atomic.StoreInt32(&count, 0)
	//删除一个相同的主题
	_ = ep.RemoveRouter(router3Id)
	// 发布消息到device.msg.request
	err = channel.Publish(
		exchange,     // 发布到的交换机
		topicRequest, // 路由键
		false,        // 表示是否要求消息必须被路由到至少一个队列
		false,        // 是否要求消息立即被消费者接收
		amqp.Publishing{
			ContentType:     ContentTypeJson,
			ContentEncoding: KeyUTF8,
			Body:            []byte("test message"),
		})
	if err != nil {
		t.Skipf("Failed to publish second message: %v", err)
		return
	}
	// 等待消息处理
	time.Sleep(time.Second * 1)

	assert.Equal(t, int32(0), atomic.LoadInt32(&count))
}

func TestEndpointConnectionStatus(t *testing.T) {
	if os.Getenv("SKIP_RABBITMQ_TESTS") == "true" {
		t.Skip("Skipping RabbitMQ tests")
	}
	server := os.Getenv("RABBITMQ_URL")
	if server == "" {
		server = "amqp://guest:guest@localhost:5672/"
	}
	endpoint := &RabbitMQ{}
	config := types.NewConfig()
	err := endpoint.Init(config, types.Configuration{
		"Server": server,
	})
	assert.Nil(t, err)
	conn, err := endpoint.SharedNode.GetSafely()
	if err != nil {
		t.Skipf("rabbitmq server not available: %v", err)
	}
	assert.Equal(t, types.StatusConnected, endpoint.ConnectionStatus().Status)

	err = conn.Close()
	assert.Nil(t, err)
	info := endpoint.ConnectionStatus()
	assert.Equal(t, types.StatusReconnecting, info.Status)
}

// AddRouter 采用"先占位再建连"，建连窗口内 channels 会含 nil 占位项。
// 该窗口内并发 Close 遍历关闭 channel 时不得对 nil 调 Close 而 panic。
func TestCloseWithPendingPlaceholder(t *testing.T) {
	ep := (&RabbitMQ{}).New().(*RabbitMQ)
	// 模拟 AddRouter 已写入 nil 占位、channel 尚未建立
	ep.channels["pending-router"] = nil
	ep.gens["pending-router"] = 0

	assert.Nil(t, ep.Close())
	assert.Equal(t, 0, len(ep.channels))
}
